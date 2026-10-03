package dev.parkerharrelson.grpc.peakewma;

public final class PeakEwmaConfigKeys {
    private PeakEwmaConfigKeys() {}

    public static final String POLICY_NAME = "peak_ewma_p2c";

    public static final String TAU_FAST_MILLIS = "tauFastMillis";
    public static final String TAU_SLOW_MILLIS = "tauSlowMillis";
    public static final String INFLIGHT_WEIGHT = "inflightWeight";
    public static final String INITIAL_RTT_MICROS = "initialRttMicros";
    public static final String OUTLIER_ENABLED = "outlierEnabled";
    public static final String OUTLIER_WINDOW_MILLIS = "outlierWindowMillis";
    public static final String OUTLIER_ERROR_RATE = "outlierErrorRate";
    public static final String OUTLIER_EJECT_MILLIS = "outlierEjectMillis";
    public static final String OUTLIER_LATENCY_MULTIPLIER = "outlierLatencyMultiplier";
    public static final String STALE_MILLIS_FOR_RATIO = "staleMillisForRatio";
    public static final String OUTLIER_REENTRY_COOLDOWN_MILLIS = "outlierReentryCooldownMillis";
    public static final String OUTLIER_TICK_INTERVAL_MILLIS = "outlierTickIntervalMillis";
    public static final String METHOD_MAX_ENTRIES = "methodMaxEntries";
    public static final String METHOD_PRUNE_STALE_AFTER_MILLIS = "methodPruneStaleAfterMillis";
}
