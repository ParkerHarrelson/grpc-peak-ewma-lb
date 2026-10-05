package dev.parkerharrelson.grpc.peakewma;

import dev.parkerharrelson.grpc.peakewma.outlier.ErrorWindow;
import java.util.ArrayList;
import java.util.List;

final class PeakEwmaTuner {
    private PeakEwmaTuner() {}

    static long seededInitialMicros(MethodTable table, PeakEwmaConfig cfg) {
        List<Double> vals = new ArrayList<>();
        for (String m : table.methodKeys()) {
            vals.add(table.statsFor(m).getEwmaSlowMicros());
        }
        if (vals.isEmpty()) return Math.max(2_000L, cfg.initialRttMicros);
        vals.sort(Double::compareTo);
        double med =
                (vals.size() & 1) == 1
                        ? vals.get(vals.size() / 2)
                        : 0.5 * (vals.get(vals.size() / 2 - 1) + vals.get(vals.size() / 2));
        return Math.max(2_000L, (long) Math.rint(med));
    }

    static double methodRatePerSec(ErrorWindow win, long nowNanos) {
        var snap = win.snapshot(nowNanos);
        // Divide by the time the buckets really cover, not the current configured window: the
        // window is resized every tick, and old buckets keep the width they were opened with.
        double coveredSec = Math.max(0.001, win.coveredNanos(nowNanos) / 1e9);
        return snap.total / coveredSec;
    }

    static double coeffVarFromEwma(MethodStats ms) {
        double mean = Math.max(1e-6, ms.getRttMeanMicros());
        double std = Math.sqrt(Math.max(0.0, ms.getRttVarMicros()));
        return std / mean;
    }

    static long tauFastMillis(MethodStats ms, PeakEwmaConfig cfg) {
        return tauFastMillis(coeffVarFromEwma(ms), ms.scale());
    }

    /**
     * Peak half-life: the method's sample-based half-life ({@link MethodScale#tauFastMillis}),
     * shortened for noisy methods, whose peaks are mostly noise spikes, down to 30% at high CV.
     */
    static long tauFastMillis(double cv, MethodScale scale) {
        double noise = clamp(1.0 / (1.0 + 1.5 * cv), 0.3, 1.0);
        return Math.max(1L, (long) Math.rint(scale.tauFastMillis() * noise));
    }

    static long tauSlowMillis(MethodStats ms, PeakEwmaConfig cfg) {
        return tauSlowMillis(coeffVarFromEwma(ms), ms.scale());
    }

    static long tauSlowMillis(double cv, MethodScale scale) {
        double noise = clamp(1.0 / (1.0 + 0.5 * cv), 0.5, 1.0);
        return Math.max(1L, (long) Math.rint(scale.tauSlowMillis() * noise));
    }

    /**
     * Samples before a peer's EWMAs are trusted for outlier decisions: half the peak memory ({@link
     * MethodScale#PEAK_MEMORY_SAMPLES}), enough for the smoothed value to reflect the peer rather
     * than its first few calls. Dimensionless, so the same at any request rate.
     */
    static final int WARM_SAMPLES = (int) (MethodScale.PEAK_MEMORY_SAMPLES / 2);

    static int minSamplesForRatioEff(double lambdaPerSec) {
        return WARM_SAMPLES;
    }

    /** No separate time gate: counting samples already says how much evidence there is. */
    static long minWarmupMillisForRatioEff(double lambdaPerSec) {
        return 0L;
    }

    /**
     * Cost multiplier per outstanding unary call: cost = latency x (inflight + 1), the standard
     * Peak-EWMA load term (Finagle, tower). Expected wait behind {@code inflight} queued calls on a
     * backend serving them at the observed latency; there is no workload-specific weight to tune.
     */
    static final double INFLIGHT_WEIGHT = 1.0;

    static double inflightWeightEff(int readyCount, int medianInflight, PeakEwmaConfig cfg) {
        return INFLIGHT_WEIGHT;
    }

    /**
     * Error-window length: long enough to hold ~200 calls of this method (the larger of its own
     * rate and half the fleet's), never shorter than two outlier ticks (the window is only read
     * once per tick), and at most 5 minutes.
     */
    static long windowMillisEff(double lambdaMethod, double lambdaFleet, long tickMillis) {
        double targetSamples = 200.0;
        double denominator = Math.max(1e-3, Math.max(lambdaMethod, 0.5 * lambdaFleet));
        return (long)
                Math.rint(clamp(1000.0 * targetSamples / denominator, 2.0 * tickMillis, 300_000.0));
    }

    /** Fewest calls in the window before an error rate is judged at all. */
    static final int MIN_ERROR_EVIDENCE = 5;

    /**
     * Relative error ejection (#108): a backend is also ejected when the 95% lower bound on its
     * error rate is at least this, and at least {@link #RELATIVE_ERROR_FACTOR} times the rest of
     * the fleet's rate for the method. 1% keeps ordinary noise from ejecting anyone; with ~200
     * calls in the window, a backend failing 5% of calls clears it.
     */
    static final double RELATIVE_ERROR_FLOOR = 0.01;

    /**
     * How many times worse than the other backends a backend's error rate must be (on its 95% lower
     * bound). When every backend fails alike, e.g. a shared dependency is down, none is an outlier
     * and nothing is ejected.
     */
    static final double RELATIVE_ERROR_FACTOR = 3.0;

    /**
     * 95% Wilson lower bound on the true error rate given {@code errors} of {@code total}. A
     * backend is ejected when even this pessimistic-for-ejection estimate exceeds the threshold: 10
     * failures of 10 calls is conclusive (bound 0.72), 3 of 20 is not (0.05). Replaces a fixed
     * ~40-call volume gate that a backend drained to ~2% of traffic never reached.
     */
    static double errorRateLowerBound(long errors, long total) {
        if (total <= 0) return 0.0;
        double z = 1.96;
        double n = total;
        double p = errors / n;
        double z2 = z * z;
        double centre = p + z2 / (2 * n);
        double margin = z * Math.sqrt(p * (1 - p) / n + z2 / (4 * n * n));
        return Math.max(0.0, (centre - margin) / (1 + z2 / n));
    }

    /**
     * At most a fifth of the fleet may be ejected at once (but always at least one peer, see {@link
     * #maxEjectedCountEff}), so outlier detection can never take out most of the capacity even if
     * many peers look bad together, e.g. during a shared dependency outage.
     */
    static final int MAX_EJECTION_PERCENT = 20;

    static int maxEjectionPercentEff(int readyCount) {
        return MAX_EJECTION_PERCENT;
    }

    /**
     * Maximum peers that may be ejected at once: the percentage cap, but never less than one.
     * Without the floor, 1 of n exceeds the cap for every n < 5, so small fleets could never eject
     * anything (grpc's outlier_detection likewise always allows the first ejection).
     */
    static int maxEjectedCountEff(int readyCount) {
        if (readyCount < 2) return 0;
        return Math.max(
                1, (int) Math.floor(readyCount * maxEjectionPercentEff(readyCount) / 100.0));
    }

    /** Peers that must stay in rotation after an ejection; never more than readyCount - 1. */
    /** Peers that stay in rotation: whatever the ejection cap leaves (never fewer than one). */
    static int minReadyAfterEjectEff(int readyCount) {
        return Math.max(1, readyCount - maxEjectedCountEff(readyCount));
    }

    static double minReadyFractionAfterEjectEff(int readyCount) {
        return readyCount <= 0 ? 1.0 : (double) minReadyAfterEjectEff(readyCount) / readyCount;
    }

    /**
     * How many times slower than the fleet median a peer must be to be ejected for latency: the
     * configured multiplier, raised for noisy methods so the threshold always sits beyond two
     * standard deviations of normal variation (1 + 2 x CV).
     */
    static double latencyMultiplierEff(double coeffVar, PeakEwmaConfig cfg) {
        return Math.max(cfg.outlierLatencyMultiplier, 1.0 + 2.0 * coeffVar);
    }

    private static double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }

    private static long clamp(long v) {
        return Math.max(250L, Math.min(2000L, v));
    }

    private static int clamp(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private static double log2(double v) {
        return Math.log(v) / Math.log(2.0);
    }
}
