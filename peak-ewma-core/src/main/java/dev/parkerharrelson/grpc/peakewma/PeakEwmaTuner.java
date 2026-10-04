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

    /** Prototype knob: how many fair-share samples a peak should be remembered for. */
    static volatile double PEAK_MEMORY_SAMPLES =
            Double.parseDouble(System.getProperty("peakewma.peakMemorySamples", "NaN"));

    /**
     * Scale-free fast half-life: remember a peak for ~K samples of a fair share of this method's
     * traffic, and never for less than 2 RTTs. Returns NaN when disabled or rate unknown.
     */
    static double sampleBasedTauFastMillis(double fairSharePerSec, double rttMillis) {
        double k = PEAK_MEMORY_SAMPLES;
        if (Double.isNaN(k) || fairSharePerSec <= 0) return Double.NaN;
        return clamp(Math.max(1000.0 * k / fairSharePerSec, 2.0 * rttMillis), 20.0, 300_000.0);
    }

    static long tauFastMillis(MethodStats ms, PeakEwmaConfig cfg) {
        double adaptive = ms.adaptiveTauFastMillis();
        if (!Double.isNaN(adaptive)) return Math.max(1L, (long) adaptive);
        double cv = coeffVarFromEwma(ms);
        // Adapt around the CONFIGURED half-life (clamps are relative to it); with the default
        // 1000 ms this is the same [300, 2000] ms range as before.
        double base = cfg.tauFastMillis;
        double min = 0.3 * base;
        double max = 2.0 * base;
        double scale = 1.0 / (1.0 + 1.5 * cv);
        return (long) Math.rint(clamp(base * scale, min, max));
    }

    static long tauSlowMillis(MethodStats ms, PeakEwmaConfig cfg) {
        double cv = coeffVarFromEwma(ms);
        // Relative to the configured value; default 30 s gives the previous [15, 60] s range.
        double base = cfg.tauSlowMillis;
        double min = 0.5 * base;
        double max = 2.0 * base;
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
        // Relative to the configured weight (default 0.15 gives the previous [0.05, 0.30]), so
        // inflightWeight=0 disables the penalty and larger values are honoured.
        double base = cfg.inflightWeight;
        double factor = Math.sqrt(Math.max(1.0, readyCount) / Math.max(1.0, medianInflight));
        return clamp(base * factor, base / 3.0, base * 2.0);
    }

    static long warmupMillisEff(MethodTable table) {
        // Use the cached seed on MethodTable rather than recomputing the median of all methods
        // on every pick; the cache is refreshed on pruneStale (periodic) so this value is at
        // most one tick behind the true median, which is well within the tuner's 300–3000 ms
        // clamp.
        // 75 typical RTTs of warmup, in milliseconds. The seed is in MICROseconds; the old
        // formula used it as milliseconds, so every result clamped to 3000 ms.
        double rttSeedMillis = table.cachedSeedMicros() / 1000.0;
        return (long) Math.rint(clamp(75.0 * rttSeedMillis, 300.0, 3000.0));
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
