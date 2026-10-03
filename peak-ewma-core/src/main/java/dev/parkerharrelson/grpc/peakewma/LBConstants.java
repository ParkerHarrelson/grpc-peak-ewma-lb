package dev.parkerharrelson.grpc.peakewma;

/** String constants shared by the Peak-EWMA load balancer, its picker, and its metrics emitter. */
public final class LBConstants {

    // Metrics Constants
    public static final String METHOD = "method";
    public static final String OUTCOME = "outcome";
    public static final String SUBCHANNEL = "subchannel";
    public static final String REASON = "reason";
    public static final String KEY = "key";
    public static final String UNKNOWN = "unknown";
    public static final String WINDOW_MILLIS_EFF = "windowMillisEff";
    public static final String LATENCY_MULTIPLIER_EFF = "latencyMultiplierEff";
    public static final String INFLIGHT_WEIGHT_EFF = "inflightWeightEff";
    public static final String OUTLIER_ERROR_RATE = "outlierErrorRate";
    public static final String LATENCY = "latency";
    public static final String ERRORS = "errors";
    public static final String NO_READY = "no_ready";
    public static final String ALL_EJECTED = "all_ejected";
    public static final String OK_FALLBACK = "ok_fallback";
    public static final String OK = "ok";
    public static final String NO_TABLE = "no_table";

    private LBConstants() {
        // intentional: utility class
    }
}
