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
    private volatile double ewmaFastMicros;
    private volatile double ewmaSlowMicros;

    private final AtomicLong meanBits = new AtomicLong(Double.doubleToRawLongBits(0.0));
    private double m2Micros = 0.0;
    private int varN = 0;

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
        double rttMicros = EwmaClocks.nanosToMicros(rttNanos);

        // Serialize the full EWMA + variance update so concurrent samples cannot lose each
        // other's writes. Readers stay lock-free via volatile fields — they may observe
        // slightly inconsistent fast/slow pairs between updates, which the picker tolerates,
        // but a single update is applied atomically relative to other updates.
        varLock.lock();
        try {
            long tauFastEff = PeakEwmaTuner.tauFastMillis(this, cfg);
            long tauSlowEff = PeakEwmaTuner.tauSlowMillis(this, cfg);

            double decayFast = EwmaClocks.decayFactor(nowNanos, lastUpdateNanos, tauFastEff);
            double decaySlow = EwmaClocks.decayFactor(nowNanos, lastUpdateNanos, tauSlowEff);

            double fast = Math.max(rttMicros, ewmaFastMicros * decayFast);
            double slow = rttMicros * (1 - decaySlow) + ewmaSlowMicros * decaySlow;

            ewmaFastMicros = fast;
            ewmaSlowMicros = slow;
            lastUpdateNanos = nowNanos;

            int n = ++varN;
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
        } finally {
            varLock.unlock();
        }

        if (samples.getAndIncrement() == 0) {
            firstSampleNanos.compareAndSet(0L, nowNanos);
        }
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
