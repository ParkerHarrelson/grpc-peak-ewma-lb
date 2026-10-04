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

    static int minSamplesForRatioEff(double lambdaPerSec) {
        int v = (int) Math.ceil(6.0 * log2(lambdaPerSec + 1.0));
        return clamp(v, 2, 16);
    }

    static long minWarmupMillisForRatioEff(double lambdaPerSec) {
        long v = (long) Math.rint(1200.0 / Math.max(0.05, lambdaPerSec));
        return clamp(v);
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

    static int maxEjectionPercentEff(int readyCount) {
        int v = (int) Math.round(10 + 5 * log2(Math.max(1.0, readyCount)));
        return clamp(v, 10, 50);
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
    static int minReadyAfterEjectEff(int readyCount) {
        int v = (int) Math.ceil(minReadyFractionAfterEjectEff(readyCount) * readyCount);
        return Math.min(v, Math.max(1, readyCount - 1));
    }

    static double minReadyFractionAfterEjectEff(int readyCount) {
        double v = 0.7 - 0.05 * log2(Math.max(1.0, readyCount));
        return clamp(v, 0.4, 0.7);
    }

    static int minTotalForErrorEjectEff(int readyCount) {
        int v = (int) Math.round(30 + 5 * log2(Math.max(1.0, readyCount)));
        return clamp(v, 20, 100);
    }

    static double latencyMultiplierEff(double coeffVar, PeakEwmaConfig cfg) {
        if (coeffVar < 0.20) return Math.max(2.0, cfg.outlierLatencyMultiplier - 0.5);
        if (coeffVar > 0.50) return Math.min(3.5, cfg.outlierLatencyMultiplier + 1.0);
        return cfg.outlierLatencyMultiplier;
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
