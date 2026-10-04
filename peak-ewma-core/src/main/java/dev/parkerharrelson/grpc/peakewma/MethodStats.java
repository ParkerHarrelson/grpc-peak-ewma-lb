package dev.parkerharrelson.grpc.peakewma;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-(subchannel, method) latency statistics used by the Peak-EWMA picker.
 *
 * <p>Maintains two EWMAs over observed RTTs:
 *
 * <ul>
 *   <li><b>fast</b> — Peak EWMA: {@code max(rtt, previous * decay)}. Reacts instantly to spikes so
 *       a slow response is penalised on the very next pick.
 *   <li><b>slow</b> — Standard EWMA: {@code rtt * (1 - decay) + previous * decay}. Smooths short
 *       bursts so the seed for new methods and the cold-start fallback don't flip on every tick.
 * </ul>
 *
 * Both decay half-lives (configurable via {@link PeakEwmaConfig#tauFastMillis} and {@link
 * PeakEwmaConfig#tauSlowMillis}) are further adapted by {@link PeakEwmaTuner} based on observed
 * coefficient of variation, so noisy methods don't lock onto a stale peak.
 *
 * <p>A Welford running variance of RTTs is maintained so {@code PeakEwmaTuner.coeffVarFromEwma} can
 * return a normalized noise level. Updates are serialized through a single lock to prevent lost
 * writes under concurrent samples; reads are lock-free volatile.
 */
public final class MethodStats {

    private volatile long lastUpdateNanos;
    // Fast (peak) half-life chosen by the outlier tick from this method's fleet-wide request
    // rate and latency; NaN until the first tick has seen traffic (then the config default).
    private volatile double adaptiveTauFastMillis = Double.NaN;
    private volatile double ewmaFastMicros;
    private volatile double ewmaSlowMicros;

    private final AtomicLong meanBits = new AtomicLong(Double.doubleToRawLongBits(0.0));
    private double m2Micros = 0.0;
    private long varN = 0;
    // Guarded by varLock: true until the first sample (success or failure) lands.
    private boolean fastIsSeed = true;

    /** Upper bound for the failure penalty recorded into the fast EWMA (10 s). */
    static final double MAX_PENALTY_MICROS = 10_000_000.0;

    private final ReentrantLock varLock = new ReentrantLock();
    private volatile double varMicros = 0.0;

    private final AtomicLong firstSampleNanos = new AtomicLong(0L);
    private final AtomicInteger samples = new AtomicInteger(0);

    /**
     * Creates a new {@code MethodStats} seeded from the initial RTT and timestamp.
     *
     * @param initialRttMicros seed RTT in microseconds; used for both fast and slow EWMAs
     * @param initNanos seed timestamp, normally {@code clocks.nanoTime()}
     */
    public MethodStats(long initialRttMicros, long initNanos) {
        this.ewmaFastMicros = initialRttMicros;
        this.ewmaSlowMicros = initialRttMicros;
        this.lastUpdateNanos = initNanos;
    }

    /**
     * Records one observed RTT and advances both EWMAs + variance. Thread-safe against concurrent
     * callers; the full write is serialized so no sample is lost.
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

        // Serialize the full EWMA + variance update so concurrent samples cannot lose each
        // other's writes. Readers stay lock-free via volatile fields — they may observe
        // slightly inconsistent fast/slow pairs between updates, which the picker tolerates,
        // but a single update is applied atomically relative to other updates.
        varLock.lock();
        try {
            if (serverFailure) {
                long tauFastEff = PeakEwmaTuner.tauFastMillis(this, cfg);
                double decayFast = EwmaClocks.decayFactor(nowNanos, lastUpdateNanos, tauFastEff);
                double penalty =
                        Math.min(
                                MAX_PENALTY_MICROS,
                                Math.max(
                                        rttMicros,
                                        2.0
                                                * Math.max(
                                                        ewmaFastMicros * decayFast,
                                                        ewmaSlowMicros)));
                ewmaFastMicros = Math.max(penalty, ewmaFastMicros * decayFast);
                fastIsSeed = false;
                lastUpdateNanos = nowNanos;
                return;
            }

            long tauFastEff = PeakEwmaTuner.tauFastMillis(this, cfg);
            long tauSlowEff = PeakEwmaTuner.tauSlowMillis(this, cfg);
            double decayFast = EwmaClocks.decayFactor(nowNanos, lastUpdateNanos, tauFastEff);
            double decaySlow = EwmaClocks.decayFactor(nowNanos, lastUpdateNanos, tauSlowEff);

            // The first real sample replaces the seed outright. Blending it with the seed using
            // the 30 s slow half-life pinned the baseline near initialRttMicros for ~a minute.
            ewmaFastMicros =
                    fastIsSeed ? rttMicros : Math.max(rttMicros, ewmaFastMicros * decayFast);
            ewmaSlowMicros =
                    (varN == 0)
                            ? rttMicros
                            : rttMicros * (1 - decaySlow) + ewmaSlowMicros * decaySlow;
            fastIsSeed = false;
            lastUpdateNanos = nowNanos;

            long n = ++varN;
            double curMean = Double.longBitsToDouble(meanBits.get());
            double newMean;
            double newM2;

            if (n == 1) {
                newMean = rttMicros;
                newM2 = 0.0;
            } else {
                double delta = rttMicros - curMean;
                newMean = curMean + delta / n;
                newM2 = m2Micros + delta * (rttMicros - newMean);
            }
            m2Micros = newM2;
            meanBits.set(Double.doubleToRawLongBits(newMean));

            varMicros = (n > 1) ? Math.max(0.0, newM2 / (n - 1)) : 0.0;

            // Saturating: only "at least minSamples" matters, and an int that wraps after 2^31
            // calls would flip a busy peer back to "cold".
            int s = samples.get();
            if (s == 0) {
                firstSampleNanos.compareAndSet(0L, nowNanos);
            }
            if (s < Integer.MAX_VALUE) {
                samples.set(s + 1);
            }
        } finally {
            varLock.unlock();
        }
    }

    void setAdaptiveTauFastMillis(double millis) {
        this.adaptiveTauFastMillis = millis;
    }

    double adaptiveTauFastMillis() {
        return adaptiveTauFastMillis;
    }

    /**
     * @return current fast (peak) EWMA in microseconds
     */
    public double getEwmaFastMicros() {
        return ewmaFastMicros;
    }

    /**
     * @return current slow EWMA in microseconds
     */
    public double getEwmaSlowMicros() {
        return ewmaSlowMicros;
    }

    /**
     * @return total number of samples recorded since construction
     */
    public int getSamples() {
        return samples.get();
    }

    /**
     * @return nano-time of the first-ever sample on this entry, or 0 if none have been recorded
     */
    public long getFirstSampleNanos() {
        return firstSampleNanos.get();
    }

    /**
     * @return nano-time of the most recent sample; matches the construction timestamp if none
     */
    public long getLastUpdateNanos() {
        return lastUpdateNanos;
    }

    /**
     * @return Welford running mean of observed RTTs in microseconds
     */
    public double getRttMeanMicros() {
        return Double.longBitsToDouble(meanBits.get());
    }

    /**
     * @return Welford sample variance of observed RTTs in microseconds squared
     */
    public double getRttVarMicros() {
        return varMicros;
    }
}
