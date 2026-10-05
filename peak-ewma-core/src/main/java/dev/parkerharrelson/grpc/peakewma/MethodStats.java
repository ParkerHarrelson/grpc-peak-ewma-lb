package dev.parkerharrelson.grpc.peakewma;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-(subchannel, method) latency statistics used by the Peak-EWMA picker.
 *
 * <p>Maintains two EWMAs over observed RTTs:
 *
 * <ul>
 *   <li><b>fast</b> — Peak EWMA: {@code max(rtt, previous * decay)}. Reacts instantly to spikes so
 *       a slow response is penalised on the very next pick.
 *   <li><b>slow</b> — Standard EWMA: {@code rtt * (1 - decay) + previous * decay}. Smooths short
 *       bursts; the outlier detector's baseline.
 * </ul>
 *
 * Both half-lives come from the method's {@link MethodScale}: a number of samples of the fleet's
 * observed traffic (so they mean the same thing at any request rate and latency), with the peak's
 * further shortened for noisy methods so it doesn't lock onto noise spikes.
 *
 * <p>Mean and variance are exponentially weighted with the slow half-life (bias-corrected, so the
 * first samples count fully), so the coefficient of variation reflects recent behaviour rather than
 * everything since the stats were created.
 *
 * <p><b>Concurrency.</b> All state lives in one immutable {@link State} swapped with
 * compare-and-set: every completed RPC updates the stats of the (backend, method) it ran on, so
 * under load many threads hit the same object. A lock here parked threads for microseconds per RPC;
 * a CAS loser simply recomputes and retries, and readers always see a consistent snapshot.
 */
public final class MethodStats {

    /** Upper bound for the failure penalty recorded into the fast EWMA (10 s). */
    static final double MAX_PENALTY_MICROS = 10_000_000.0;

    /**
     * One consistent snapshot of the statistics. {@code lastUpdateNanos} is when the peak was last
     * written (any sample or failure); {@code slowUpdateNanos} is when the smoothed statistics
     * (slow EWMA, mean, variance) were. They differ because the smoothed statistics are thinned
     * (see {@link #minSmoothingIntervalNanos}) and failures update only the peak; each EWMA must
     * decay over the time since its own last write.
     */
    record State(
            double fastMicros,
            double slowMicros,
            long lastUpdateNanos,
            long slowUpdateNanos,
            long firstSampleNanos,
            int samples,
            double meanMicros,
            double varMicros,
            boolean fastIsSeed) {}

    private final AtomicReference<State> state;

    // The fleet's view of this method (half-lives, seed); replaced by the outlier tick.
    private volatile MethodScale scale = MethodScale.DEFAULT;

    /**
     * Creates a new {@code MethodStats} seeded from the initial RTT and timestamp.
     *
     * @param initialRttMicros seed RTT in microseconds; used for both fast and slow EWMAs
     * @param initNanos seed timestamp, normally {@code clocks.nanoTime()}
     */
    public MethodStats(long initialRttMicros, long initNanos) {
        this.state =
                new AtomicReference<>(
                        new State(
                                initialRttMicros,
                                initialRttMicros,
                                initNanos,
                                initNanos,
                                0L,
                                0,
                                0.0,
                                0.0,
                                true));
    }

    /**
     * Records one observed RTT and advances both EWMAs + variance. Thread-safe. Within {@link
     * #minSmoothingIntervalNanos} of the last smoothed write only the peak is updated (see {@link
     * #update(long, long, PeakEwmaConfig, boolean)}).
     *
     * @param nowNanos end-of-call timestamp from the shared clock
     * @param rttNanos observed RTT in nanoseconds; must be non-negative
     * @param cfg current Peak-EWMA config (tau fast/slow etc.)
     */
    public void update(long nowNanos, long rttNanos, PeakEwmaConfig cfg) {
        update(nowNanos, rttNanos, cfg, false);
    }

    /**
     * Records one completed call.
     *
     * <p>When {@code serverFailure} is true the call failed in a way that says the backend is
     * unhealthy (UNAVAILABLE, INTERNAL, ...). Its RTT is usually tiny, so recording it as-is would
     * make a failing backend look like the fastest one. Instead the fast (peak) EWMA takes a
     * penalty of at least twice the current estimate, capped at {@link #MAX_PENALTY_MICROS}; the
     * slow EWMA and the variance only ever see real successful latencies, so they remain a clean
     * baseline for outlier detection.
     *
     * <p>Successful samples always reach the peak (it must react to every spike). The smoothed
     * statistics take at most one sample per {@link #minSmoothingIntervalNanos}: the first to
     * arrive after the interval, whatever its value. Choosing by arrival time, not by value, keeps
     * them unbiased; letting only peak-raising samples through inside the interval inflated the
     * mean by 2-3x at tens of thousands of calls per second.
     *
     * @param nowNanos end-of-call timestamp from the shared clock
     * @param rttNanos observed RTT in nanoseconds; must be non-negative
     * @param cfg current Peak-EWMA config (tau fast/slow etc.)
     * @param serverFailure whether the call failed with a server-health status
     */
    public void update(long nowNanos, long rttNanos, PeakEwmaConfig cfg, boolean serverFailure) {
        double rttMicros = EwmaClocks.nanosToMicros(rttNanos);
        while (true) {
            State s = state.get();
            State next;
            if (serverFailure) {
                next = afterFailure(s, nowNanos, rttMicros);
            } else if (smoothingDue(s, nowNanos)) {
                next = afterSample(s, nowNanos, rttMicros);
            } else {
                // Exact to skip: readers decay the stored peak to now, which is what
                // max(rtt, decayed) would store.
                if (rttMicros <= decayedFast(s, nowNanos)) return;
                next = withPeak(s, nowNanos, rttMicros);
            }
            if (state.compareAndSet(s, next)) {
                return;
            }
        }
    }

    /**
     * Minimum spacing between writes to the smoothed statistics. At most one such write per
     * (backend, method) per millisecond still gives the baseline horizon thousands of samples, and
     * removes nearly all write contention at high request rates.
     */
    static volatile long minSmoothingIntervalNanos = 1_000_000L;

    private static boolean smoothingDue(State s, long now) {
        long interval = minSmoothingIntervalNanos;
        // Out-of-order completions (now < slowUpdate) count as "within the interval".
        return interval <= 0
                || s.fastIsSeed
                || s.samples == 0
                || now - s.slowUpdateNanos >= interval;
    }

    private double decayedFast(State s, long now) {
        // A seed is a prior, not an observation: it holds until the first real sample replaces
        // it. Decaying it would make a backend on probation (or never measured) look cheaper the
        // longer it waits, and pull a burst of traffic onto it when it returns (#106).
        if (s.fastIsSeed) return s.fastMicros;
        return s.fastMicros
                * EwmaClocks.decayFactor(
                        now, s.lastUpdateNanos, PeakEwmaTuner.tauFastMillis(coeffVar(s), scale));
    }

    /**
     * The peak EWMA decayed to {@code now}, computed from one snapshot so the peak, its timestamp
     * and the noise level that sets its half-life always belong together.
     */
    double decayedPeakMicros(long now) {
        return decayedFast(state.get(), now);
    }

    // Timestamps only move forward: an out-of-order completion (ended earlier, CAS won later)
    // must not rewind them, or readers would decay the peak over time that never passed.

    private State withPeak(State s, long now, double rttMicros) {
        return new State(
                rttMicros,
                s.slowMicros,
                Math.max(now, s.lastUpdateNanos),
                s.slowUpdateNanos,
                s.firstSampleNanos,
                s.samples,
                s.meanMicros,
                s.varMicros,
                false);
    }

    private State afterFailure(State s, long now, double rttMicros) {
        double decayed = decayedFast(s, now);
        double penalty =
                Math.min(
                        MAX_PENALTY_MICROS,
                        Math.max(rttMicros, 2.0 * Math.max(decayed, s.slowMicros)));
        return new State(
                Math.max(penalty, decayed),
                s.slowMicros,
                Math.max(now, s.lastUpdateNanos),
                s.slowUpdateNanos,
                s.firstSampleNanos,
                s.samples,
                s.meanMicros,
                s.varMicros,
                false);
    }

    private State afterSample(State s, long now, double rttMicros) {
        double cv = coeffVar(s);
        double decaySlow =
                EwmaClocks.decayFactor(
                        now, s.slowUpdateNanos, PeakEwmaTuner.tauSlowMillis(cv, scale));

        // The first real sample replaces the seed outright. Blending it with the seed using the
        // 30 s slow half-life pinned the baseline near initialRttMicros for ~a minute.
        double fast = s.fastIsSeed ? rttMicros : Math.max(rttMicros, decayedFast(s, now));

        // Exponentially weighted mean/variance on the slow horizon, bias-corrected: the weight of
        // a new sample is at least 1/(n+1), so the first samples behave like a plain average.
        double weight = Math.max(1 - decaySlow, 1.0 / (Math.min(s.samples, 1 << 20) + 1));
        double diff = rttMicros - s.meanMicros;
        double mean = s.meanMicros + weight * diff;
        double var = (1 - weight) * (s.varMicros + weight * diff * diff);
        // The slow EWMA is that same bias-corrected mean. A plain EWMA started from the first
        // sample kept ~half its weight on that one sample for a whole baseline half-life (30 s
        // before the first tick), skewing the outlier baseline and the fleet seed.
        double slow = mean;

        return new State(
                fast,
                slow,
                Math.max(now, s.lastUpdateNanos),
                Math.max(now, s.slowUpdateNanos),
                s.firstSampleNanos == 0L ? now : s.firstSampleNanos,
                // Saturating: only "at least minSamples" matters, and an int that wraps after
                // 2^31 calls would flip a busy peer back to "cold".
                s.samples == Integer.MAX_VALUE ? Integer.MAX_VALUE : s.samples + 1,
                mean,
                Math.max(0.0, var),
                false);
    }

    private static double coeffVar(State s) {
        return Math.sqrt(Math.max(0.0, s.varMicros)) / Math.max(1e-6, s.meanMicros);
    }

    /**
     * Puts this (backend, method) on probation after a latency ejection (#106): forgets everything
     * it observed, so it is judged again only on samples taken after it returns, and must re-warm
     * (see the balancer's {@code isWarm}) before it can be re-ejected. The peak is set to {@code
     * seedMicros}, the fleet's typical latency, and held there until the first real sample, so the
     * returning backend competes at par rather than as the cheapest peer.
     */
    void resetForProbation(double seedMicros, long nowNanos) {
        state.set(new State(seedMicros, seedMicros, nowNanos, nowNanos, 0L, 0, 0.0, 0.0, true));
    }

    MethodScale scale() {
        return scale;
    }

    void setScale(MethodScale scale) {
        this.scale = scale;
    }

    /** One consistent snapshot of all statistics. */
    State snapshot() {
        return state.get();
    }

    /**
     * @return current fast (peak) EWMA in microseconds
     */
    public double getEwmaFastMicros() {
        return state.get().fastMicros;
    }

    /**
     * @return current slow EWMA in microseconds
     */
    public double getEwmaSlowMicros() {
        return state.get().slowMicros;
    }

    /**
     * @return number of successful samples recorded (saturates at {@link Integer#MAX_VALUE})
     */
    public int getSamples() {
        return state.get().samples;
    }

    /**
     * @return nano-time of the first-ever sample on this entry, or 0 if none have been recorded
     */
    public long getFirstSampleNanos() {
        return state.get().firstSampleNanos;
    }

    /**
     * @return nano-time of the most recent sample; matches the construction timestamp if none
     */
    public long getLastUpdateNanos() {
        return state.get().lastUpdateNanos;
    }

    /**
     * @return exponentially weighted mean of observed RTTs in microseconds
     */
    public double getRttMeanMicros() {
        return state.get().meanMicros;
    }

    /**
     * @return exponentially weighted variance of observed RTTs in microseconds squared
     */
    public double getRttVarMicros() {
        return state.get().varMicros;
    }

    /** Test seam: overwrite selected fields (null = keep). Package-private, tests only. */
    void overrideForTest(
            Double fastMicros,
            Double slowMicros,
            Integer samples,
            Long firstSampleNanos,
            Long lastUpdateNanos) {
        State s = state.get();
        state.set(
                new State(
                        fastMicros != null ? fastMicros : s.fastMicros,
                        slowMicros != null ? slowMicros : s.slowMicros,
                        lastUpdateNanos != null ? lastUpdateNanos : s.lastUpdateNanos,
                        lastUpdateNanos != null ? lastUpdateNanos : s.slowUpdateNanos,
                        firstSampleNanos != null ? firstSampleNanos : s.firstSampleNanos,
                        samples != null ? samples : s.samples,
                        s.meanMicros,
                        s.varMicros,
                        s.fastIsSeed && fastMicros == null));
    }
}
