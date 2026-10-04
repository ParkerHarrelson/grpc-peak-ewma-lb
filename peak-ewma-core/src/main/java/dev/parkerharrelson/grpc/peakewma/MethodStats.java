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
 * Both decay half-lives (configurable via {@link PeakEwmaConfig#tauFastMillis} and {@link
 * PeakEwmaConfig#tauSlowMillis}) are further adapted by {@link PeakEwmaTuner} based on the observed
 * coefficient of variation, so noisy methods don't lock onto a stale peak.
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

    /** One consistent snapshot of the statistics. */
    record State(
            double fastMicros,
            double slowMicros,
            long lastUpdateNanos,
            long firstSampleNanos,
            int samples,
            double meanMicros,
            double varMicros,
            boolean fastIsSeed) {}

    private final AtomicReference<State> state;

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
                                0L,
                                0,
                                0.0,
                                0.0,
                                true));
    }

    /**
     * Records one observed RTT and advances both EWMAs + variance. Thread-safe. A sample that
     * neither raises the peak nor is due for the smoothed statistics is skipped (see {@link
     * #redundant}).
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
     * @param nowNanos end-of-call timestamp from the shared clock
     * @param rttNanos observed RTT in nanoseconds; must be non-negative
     * @param cfg current Peak-EWMA config (tau fast/slow etc.)
     * @param serverFailure whether the call failed with a server-health status
     */
    public void update(long nowNanos, long rttNanos, PeakEwmaConfig cfg, boolean serverFailure) {
        double rttMicros = EwmaClocks.nanosToMicros(rttNanos);
        while (true) {
            State s = state.get();
            if (!serverFailure && redundant(s, nowNanos, rttMicros, cfg)) {
                return;
            }
            State next =
                    serverFailure
                            ? afterFailure(s, nowNanos, rttMicros, cfg)
                            : afterSample(s, nowNanos, rttMicros, cfg);
            if (state.compareAndSet(s, next)) {
                return;
            }
        }
    }

    /**
     * Minimum spacing between writes that only feed the smoothed statistics. At most one such write
     * per (backend, method) per millisecond still gives the 30 s slow horizon tens of thousands of
     * samples, and removes nearly all write contention at high request rates.
     */
    static volatile long minSmoothingIntervalNanos = 1_000_000L;

    /**
     * A sample needs no write when it doesn't raise the peak and the smoothed statistics were
     * updated within {@link #minSmoothingIntervalNanos}. Skipping it is exact for the fast EWMA:
     * readers decay the stored peak to now, which is what {@code max(rtt, decayed)} would store.
     */
    private static boolean redundant(State s, long now, double rttMicros, PeakEwmaConfig cfg) {
        long interval = minSmoothingIntervalNanos;
        if (interval <= 0 || s.fastIsSeed || s.samples == 0) return false;
        // Out-of-order completions (now < lastUpdate) count as "within the interval".
        if (now - s.lastUpdateNanos >= interval) return false;
        double decayed =
                s.fastMicros
                        * EwmaClocks.decayFactor(
                                now,
                                s.lastUpdateNanos,
                                PeakEwmaTuner.tauFastMillis(coeffVar(s), cfg));
        return rttMicros <= decayed;
    }

    private static State afterFailure(State s, long now, double rttMicros, PeakEwmaConfig cfg) {
        double decayFast =
                EwmaClocks.decayFactor(
                        now, s.lastUpdateNanos, PeakEwmaTuner.tauFastMillis(coeffVar(s), cfg));
        double decayed = s.fastMicros * decayFast;
        double penalty =
                Math.min(
                        MAX_PENALTY_MICROS,
                        Math.max(rttMicros, 2.0 * Math.max(decayed, s.slowMicros)));
        return new State(
                Math.max(penalty, decayed),
                s.slowMicros,
                now,
                s.firstSampleNanos,
                s.samples,
                s.meanMicros,
                s.varMicros,
                false);
    }

    private static State afterSample(State s, long now, double rttMicros, PeakEwmaConfig cfg) {
        double cv = coeffVar(s);
        double decayFast =
                EwmaClocks.decayFactor(
                        now, s.lastUpdateNanos, PeakEwmaTuner.tauFastMillis(cv, cfg));
        double decaySlow =
                EwmaClocks.decayFactor(
                        now, s.lastUpdateNanos, PeakEwmaTuner.tauSlowMillis(cv, cfg));

        // The first real sample replaces the seed outright. Blending it with the seed using the
        // 30 s slow half-life pinned the baseline near initialRttMicros for ~a minute.
        double fast = s.fastIsSeed ? rttMicros : Math.max(rttMicros, s.fastMicros * decayFast);
        double slow =
                s.samples == 0 ? rttMicros : rttMicros * (1 - decaySlow) + s.slowMicros * decaySlow;

        // Exponentially weighted mean/variance on the slow horizon, bias-corrected: the weight of
        // a new sample is at least 1/(n+1), so the first samples behave like a plain average.
        double weight = Math.max(1 - decaySlow, 1.0 / (Math.min(s.samples, 1 << 20) + 1));
        double diff = rttMicros - s.meanMicros;
        double mean = s.meanMicros + weight * diff;
        double var = (1 - weight) * (s.varMicros + weight * diff * diff);

        return new State(
                fast,
                slow,
                now,
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
                        firstSampleNanos != null ? firstSampleNanos : s.firstSampleNanos,
                        samples != null ? samples : s.samples,
                        s.meanMicros,
                        s.varMicros,
                        s.fastIsSeed && fastMicros == null));
    }
}
