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
        double windowSec = Math.max(0.001, win.currentWindowMillis() / 1000.0);
        return snap.total / windowSec;
    }

    static double coeffVarFromEwma(MethodStats ms) {
        double mean = Math.max(1e-6, ms.getRttMeanMicros());
        double std = Math.sqrt(Math.max(0.0, ms.getRttVarMicros()));
        return std / mean;
    }

    static long tauFastMillis(MethodStats ms, PeakEwmaConfig cfg) {
        double cv = coeffVarFromEwma(ms);
        double base = cfg.tauFastMillis;
        double min = 300.0;
        double max = 2000.0;
        double scale = 1.0 / (1.0 + 1.5 * cv);
        return (long) Math.rint(clamp(base * scale, min, max));
    }

    static long tauSlowMillis(MethodStats ms, PeakEwmaConfig cfg) {
        double cv = coeffVarFromEwma(ms);
        double base = cfg.tauSlowMillis;
        double min = 15_000.0;
        double max = 60_000.0;
        double scale = 1.0 / (1.0 + 0.5 * cv);
        return (long) Math.rint(clamp(base * scale, min, max));
    }

    static int minSamplesForRatioEff(double lambdaPerSec) {
        int v = (int) Math.ceil(6.0 * log2(lambdaPerSec + 1.0));
        return clamp(v, 2, 16);
    }

    static long minWarmupMillisForRatioEff(double lambdaPerSec) {
        long v = (long) Math.rint(1200.0 / Math.max(0.05, lambdaPerSec));
        return clamp(v);
    }

    static double inflightWeightEff(int readyCount, int medianInflight, PeakEwmaConfig cfg) {
        double base = cfg.inflightWeight;
        double factor = Math.sqrt(Math.max(1.0, readyCount) / Math.max(1.0, medianInflight));
        return clamp(base * factor, 0.05, 0.30);
    }

    static long warmupMillisEff(MethodTable table) {
        // Use the cached seed on MethodTable rather than recomputing the median of all methods
        // on every pick; the cache is refreshed on pruneStale (periodic) so this value is at
        // most one tick behind the true median, which is well within the tuner's 300–3000 ms
        // clamp.
        long rttSeed = table.cachedSeedMicros();
        return (long) Math.rint(clamp(0.5 * rttSeed * 150, 300.0, 3000.0));
    }

    static long windowMillisEff(double lambdaMethod, double lambdaFleet) {
        double targetSamples = 200.0;
        double denominator = Math.max(0.05, Math.max(lambdaMethod, 0.5 * lambdaFleet));
        return (long) Math.rint(clamp(1000.0 * targetSamples / denominator, 8000.0, 45_000.0));
    }

    static int maxEjectionPercentEff(int readyCount) {
        int v = (int) Math.round(10 + 5 * log2(Math.max(1.0, readyCount)));
        return clamp(v, 10, 50);
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
