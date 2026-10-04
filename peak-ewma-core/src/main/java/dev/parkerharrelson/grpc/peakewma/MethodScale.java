package dev.parkerharrelson.grpc.peakewma;

/**
 * What the fleet has observed about one method, recomputed by the outlier tick and shared by every
 * backend's stats for that method.
 *
 * <p>This is what makes the defaults scale-free: memory lengths are expressed in <b>samples</b> of
 * a fair share of the method's traffic (converted to time with the observed request rate) and in
 * <b>RTTs</b>, never in fixed milliseconds. A 2 ms method at 2,000 rps and a 1 s method at 5 rps
 * get the same behaviour per sample, so one configuration fits both.
 *
 * @param tauFastMillis peak-EWMA half-life: ~{@link #PEAK_MEMORY_SAMPLES} fair-share samples, at
 *     least 2 RTTs, capped at {@link #MAX_TAU_FAST_MILLIS}
 * @param tauSlowMillis smoothed-EWMA half-life: ~{@link #BASELINE_MEMORY_SAMPLES} samples, at least
 *     20 RTTs, within [1 s, 5 min]
 * @param seedMicros fleet median latency, used as the prior for a backend that hasn't served the
 *     method yet
 */
record MethodScale(double tauFastMillis, double tauSlowMillis, double seedMicros) {

    /**
     * How many samples a peak is remembered for. Long enough to keep a bad backend out of rotation
     * for a meaningful stretch, short enough that it's re-probed and can recover. Dimensionless, so
     * it means the same thing at every request rate (validated across 2 ms..1 s and 5..2,000 rps in
     * AdversarialScaleTest).
     */
    static final double PEAK_MEMORY_SAMPLES = 20;

    /** How many samples the smoothed baseline (outlier detection) averages over. */
    static final double BASELINE_MEMORY_SAMPLES = 600;

    /**
     * Upper bound on the peak half-life. At very low traffic, 20 samples could span minutes; this
     * caps how long one stale observation can keep a backend out ("never trust old data longer than
     * this"), not a tuning value.
     */
    static final double MAX_TAU_FAST_MILLIS = 60_000;

    /** Used until the first tick has observed traffic for a method. */
    static final MethodScale DEFAULT = new MethodScale(1_000, 30_000, Double.NaN);

    /**
     * @param fairSharePerSec the method's request rate per backend (fleet rate / backends)
     * @param rttMicros fleet median latency of the method, or NaN if unknown
     */
    static MethodScale of(double fairSharePerSec, double rttMicros) {
        double rttMs = Double.isNaN(rttMicros) ? 0.0 : rttMicros / 1000.0;
        double perSampleMs = 1000.0 / fairSharePerSec;
        double fast =
                clamp(
                        Math.max(PEAK_MEMORY_SAMPLES * perSampleMs, 2 * rttMs),
                        10,
                        MAX_TAU_FAST_MILLIS);
        double slow =
                clamp(Math.max(BASELINE_MEMORY_SAMPLES * perSampleMs, 20 * rttMs), 1_000, 300_000);
        return new MethodScale(fast, slow, rttMicros);
    }

    private static double clamp(double v, double lo, double hi) {
        return Math.max(lo, Math.min(hi, v));
    }
}
