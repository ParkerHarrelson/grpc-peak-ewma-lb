package dev.parkerharrelson.grpc.peakewma;

import java.util.Map;
import java.util.Objects;

/** Immutable config with defaults and simple partial Map overrides. */
public final class PeakEwmaConfig {

    private static final long DEF_TAU_FAST_MS = 1_000;
    private static final long DEF_TAU_SLOW_MS = 30_000;

    private static final double DEF_INFLIGHT_WEIGHT = 0.15;
    private static final long DEF_INITIAL_RTT_US = 50_000;

    private static final boolean DEF_OUTLIER_ENABLED = true;
    private static final long DEF_OUTLIER_WINDOW_MS = 15_000;
    private static final double DEF_OUTLIER_ERROR_RATE = 0.20;
    private static final long DEF_OUTLIER_EJECT_MS = 15_000;
    private static final double DEF_OUTLIER_LAT_MULT = 2.5;
    private static final long DEF_STALE_MS_FOR_RATIO = 30_000;

    private static final long DEF_REENTRY_COOLDOWN_MS = 5_000;
    private static final long DEF_OUTLIER_TICK_INTERVAL_MS = 1_000;

    private static final int DEF_METHOD_MAX_ENTRIES = 512;
    private static final long DEF_METHOD_PRUNE_STALE_MS = 120_000;

    public static final PeakEwmaConfig DEFAULTS = builder().build();

    public final long tauFastMillis;
    public final long tauSlowMillis;
    public final double inflightWeight;
    public final long initialRttMicros;
    public final boolean outlierEnabled;
    public final long outlierWindowMillis;
    public final double outlierErrorRate;
    public final long outlierEjectMillis;
    public final double outlierLatencyMultiplier;
    public final long staleMillisForRatio;
    public final long outlierReentryCooldownMillis;
    public final long outlierTickIntervalMillis;
    public final int methodMaxEntries;
    public final long methodPruneStaleAfterMillis;

    private PeakEwmaConfig(Builder b) {
        this.tauFastMillis = b.tauFastMillis;
        this.tauSlowMillis = b.tauSlowMillis;
        this.inflightWeight = b.inflightWeight;
        this.initialRttMicros = b.initialRttMicros;

        this.outlierEnabled = b.outlierEnabled;
        this.outlierWindowMillis = b.outlierWindowMillis;
        this.outlierErrorRate = b.outlierErrorRate;
        this.outlierEjectMillis = b.outlierEjectMillis;
        this.outlierLatencyMultiplier = b.outlierLatencyMultiplier;

        this.staleMillisForRatio = b.staleMillisForRatio;

        this.outlierReentryCooldownMillis = b.outlierReentryCooldownMillis;
        this.outlierTickIntervalMillis = b.outlierTickIntervalMillis;

        this.methodMaxEntries = b.methodMaxEntries;
        this.methodPruneStaleAfterMillis = b.methodPruneStaleAfterMillis;

        validate();
    }

    private void validate() {
        if (tauFastMillis < 1) throw new IllegalArgumentException("tauFastMillis >= 1");
        if (tauSlowMillis < 1) throw new IllegalArgumentException("tauSlowMillis >= 1");
        if (inflightWeight < 0) throw new IllegalArgumentException("inflightWeight >= 0");
        if (initialRttMicros < 1) throw new IllegalArgumentException("initialRttMicros >= 1");
        if (outlierWindowMillis < 0) throw new IllegalArgumentException("outlierWindowMillis >= 0");
        if (outlierErrorRate < 0 || outlierErrorRate > 1)
            throw new IllegalArgumentException("outlierErrorRate [0..1]");
        if (outlierEjectMillis < 0) throw new IllegalArgumentException("outlierEjectMillis >= 0");
        if (outlierLatencyMultiplier < 1.0)
            throw new IllegalArgumentException("outlierLatencyMultiplier >= 1.0");
        if (staleMillisForRatio < 0) throw new IllegalArgumentException("staleMillisForRatio >= 0");
        if (outlierTickIntervalMillis < 100)
            throw new IllegalArgumentException("outlierTickIntervalMillis >= 100");
        if (methodMaxEntries < 1) throw new IllegalArgumentException("methodMaxEntries >= 1");
    }

    public static final class Builder {
        private long tauFastMillis = DEF_TAU_FAST_MS;
        private long tauSlowMillis = DEF_TAU_SLOW_MS;
        private double inflightWeight = DEF_INFLIGHT_WEIGHT;
        private long initialRttMicros = DEF_INITIAL_RTT_US;

        private boolean outlierEnabled = DEF_OUTLIER_ENABLED;
        private long outlierWindowMillis = DEF_OUTLIER_WINDOW_MS;
        private double outlierErrorRate = DEF_OUTLIER_ERROR_RATE;
        private long outlierEjectMillis = DEF_OUTLIER_EJECT_MS;
        private double outlierLatencyMultiplier = DEF_OUTLIER_LAT_MULT;

        private long staleMillisForRatio = DEF_STALE_MS_FOR_RATIO;

        private long outlierReentryCooldownMillis = DEF_REENTRY_COOLDOWN_MS;
        private long outlierTickIntervalMillis = DEF_OUTLIER_TICK_INTERVAL_MS;

        private int methodMaxEntries = DEF_METHOD_MAX_ENTRIES;
        private long methodPruneStaleAfterMillis = DEF_METHOD_PRUNE_STALE_MS;

        /**
         * No-arg constructor intentionally left empty. All builder fields are pre-initialized with
         * defaults above.
         */
        public Builder() {
            // intentional no-op
        }

        public Builder tauFastMillis(long v) {
            this.tauFastMillis = v;
            return this;
        }

        public Builder tauSlowMillis(long v) {
            this.tauSlowMillis = v;
            return this;
        }

        public Builder inflightWeight(double v) {
            this.inflightWeight = v;
            return this;
        }

        public Builder initialRttMicros(long v) {
            this.initialRttMicros = v;
            return this;
        }

        public Builder outlierEnabled(boolean v) {
            this.outlierEnabled = v;
            return this;
        }

        public Builder outlierWindowMillis(long v) {
            this.outlierWindowMillis = v;
            return this;
        }

        public Builder outlierErrorRate(double v) {
            this.outlierErrorRate = v;
            return this;
        }

        public Builder outlierEjectMillis(long v) {
            this.outlierEjectMillis = v;
            return this;
        }

        public Builder outlierLatencyMultiplier(double v) {
            this.outlierLatencyMultiplier = v;
            return this;
        }

        public Builder staleMillisForRatio(long v) {
            this.staleMillisForRatio = v;
            return this;
        }

        public Builder outlierReentryCooldownMillis(long v) {
            this.outlierReentryCooldownMillis = v;
            return this;
        }

        public Builder outlierTickIntervalMillis(long v) {
            this.outlierTickIntervalMillis = v;
            return this;
        }

        public Builder methodMaxEntries(int v) {
            this.methodMaxEntries = v;
            return this;
        }

        public Builder methodPruneStaleAfterMillis(long v) {
            this.methodPruneStaleAfterMillis = v;
            return this;
        }

        public PeakEwmaConfig build() {
            return new PeakEwmaConfig(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public PeakEwmaConfig merge(Map<String, Object> m) {
        return merge(this, m);
    }

    public static PeakEwmaConfig fromMap(Map<String, Object> m) {
        return merge(DEFAULTS, m);
    }

    public static PeakEwmaConfig merge(PeakEwmaConfig base, Map<String, Object> m) {
        if (m == null || m.isEmpty()) return base;
        Builder b =
                builder()
                        .tauFastMillis(
                                getLongOr(
                                        base.tauFastMillis, m, PeakEwmaConfigKeys.TAU_FAST_MILLIS))
                        .tauSlowMillis(
                                getLongOr(
                                        base.tauSlowMillis, m, PeakEwmaConfigKeys.TAU_SLOW_MILLIS))
                        .inflightWeight(
                                getDoubleOr(
                                        base.inflightWeight, m, PeakEwmaConfigKeys.INFLIGHT_WEIGHT))
                        .initialRttMicros(
                                getLongOr(
                                        base.initialRttMicros,
                                        m,
                                        PeakEwmaConfigKeys.INITIAL_RTT_MICROS))
                        .outlierEnabled(getBoolOr(base.outlierEnabled, m))
                        .outlierWindowMillis(
                                getLongOr(
                                        base.outlierWindowMillis,
                                        m,
                                        PeakEwmaConfigKeys.OUTLIER_WINDOW_MILLIS))
                        .outlierErrorRate(
                                getDoubleOr(
                                        base.outlierErrorRate,
                                        m,
                                        PeakEwmaConfigKeys.OUTLIER_ERROR_RATE))
                        .outlierEjectMillis(
                                getLongOr(
                                        base.outlierEjectMillis,
                                        m,
                                        PeakEwmaConfigKeys.OUTLIER_EJECT_MILLIS))
                        .outlierLatencyMultiplier(
                                getDoubleOr(
                                        base.outlierLatencyMultiplier,
                                        m,
                                        PeakEwmaConfigKeys.OUTLIER_LATENCY_MULTIPLIER))
                        .staleMillisForRatio(
                                getLongOr(
                                        base.staleMillisForRatio,
                                        m,
                                        PeakEwmaConfigKeys.STALE_MILLIS_FOR_RATIO))
                        .outlierReentryCooldownMillis(
                                getLongOr(
                                        base.outlierReentryCooldownMillis,
                                        m,
                                        PeakEwmaConfigKeys.OUTLIER_REENTRY_COOLDOWN_MILLIS))
                        .outlierTickIntervalMillis(
                                getLongOr(
                                        base.outlierTickIntervalMillis,
                                        m,
                                        PeakEwmaConfigKeys.OUTLIER_TICK_INTERVAL_MILLIS))
                        .methodMaxEntries(getIntOr(base.methodMaxEntries, m))
                        .methodPruneStaleAfterMillis(
                                getLongOr(
                                        base.methodPruneStaleAfterMillis,
                                        m,
                                        PeakEwmaConfigKeys.METHOD_PRUNE_STALE_AFTER_MILLIS));

        return b.build();
    }

    private static long getLongOr(long base, Map<String, Object> m, String k) {
        Object v = m.get(k);
        if (v == null) return base;
        if (v instanceof Number n) return n.longValue();
        if (v instanceof String s) return Long.parseLong(s);
        throw new IllegalArgumentException(k + ": expected number or string");
    }

    private static int getIntOr(int base, Map<String, Object> m) {
        Object v = m.get(PeakEwmaConfigKeys.METHOD_MAX_ENTRIES);
        if (v == null) return base;
        if (v instanceof Number n) return n.intValue();
        if (v instanceof String s) return Integer.parseInt(s);
        throw new IllegalArgumentException(
                PeakEwmaConfigKeys.METHOD_MAX_ENTRIES + ": expected number or string");
    }

    private static double getDoubleOr(double base, Map<String, Object> m, String k) {
        Object v = m.get(k);
        if (v == null) return base;
        if (v instanceof Number n) return n.doubleValue();
        if (v instanceof String s) return Double.parseDouble(s);
        throw new IllegalArgumentException(k + ": expected number or string");
    }

    private static boolean getBoolOr(boolean base, Map<String, Object> m) {
        Object v = m.get(PeakEwmaConfigKeys.OUTLIER_ENABLED);
        if (v == null) return base;
        if (v instanceof Boolean b) return b;
        if (v instanceof String s) return Boolean.parseBoolean(s);
        throw new IllegalArgumentException(
                PeakEwmaConfigKeys.OUTLIER_ENABLED + ": expected boolean or string");
    }

    @Override
    public String toString() {
        /* keep your existing toString if you like */
        return "PeakEwmaConfig{"
                + "tauFastMillis="
                + tauFastMillis
                + ", tauSlowMillis="
                + tauSlowMillis
                + ", inflightWeight="
                + inflightWeight
                + ", initialRttMicros="
                + initialRttMicros
                + ", outlierEnabled="
                + outlierEnabled
                + ", outlierWindowMillis="
                + outlierWindowMillis
                + ", outlierErrorRate="
                + outlierErrorRate
                + ", outlierEjectMillis="
                + outlierEjectMillis
                + ", outlierLatencyMultiplier="
                + outlierLatencyMultiplier
                + ", staleMillisForRatio="
                + staleMillisForRatio
                + ", outlierReentryCooldownMillis="
                + outlierReentryCooldownMillis
                + ", outlierTickIntervalMillis="
                + outlierTickIntervalMillis
                + ", methodMaxEntries="
                + methodMaxEntries
                + ", methodPruneStaleAfterMillis="
                + methodPruneStaleAfterMillis
                + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tauFastMillis,
                tauSlowMillis,
                inflightWeight,
                initialRttMicros,
                outlierEnabled,
                outlierWindowMillis,
                outlierErrorRate,
                outlierEjectMillis,
                outlierLatencyMultiplier,
                staleMillisForRatio,
                outlierReentryCooldownMillis,
                outlierTickIntervalMillis,
                methodMaxEntries,
                methodPruneStaleAfterMillis);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PeakEwmaConfig that)) return false;
        return tauFastMillis == that.tauFastMillis
                && tauSlowMillis == that.tauSlowMillis
                && Double.compare(that.inflightWeight, inflightWeight) == 0
                && initialRttMicros == that.initialRttMicros
                && outlierEnabled == that.outlierEnabled
                && outlierWindowMillis == that.outlierWindowMillis
                && Double.compare(that.outlierErrorRate, outlierErrorRate) == 0
                && outlierEjectMillis == that.outlierEjectMillis
                && Double.compare(that.outlierLatencyMultiplier, outlierLatencyMultiplier) == 0
                && staleMillisForRatio == that.staleMillisForRatio
                && outlierReentryCooldownMillis == that.outlierReentryCooldownMillis
                && outlierTickIntervalMillis == that.outlierTickIntervalMillis
                && methodMaxEntries == that.methodMaxEntries
                && methodPruneStaleAfterMillis == that.methodPruneStaleAfterMillis;
    }
}
