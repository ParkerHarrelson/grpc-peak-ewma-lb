package dev.parkerharrelson.grpc.peakewma.metrics;

/**
 * Sink for Peak-EWMA load balancer observability signals.
 *
 * <p>The core module has no metrics-library dependency. It ships {@link NoopLbMetrics} (the
 * zero-overhead default used by the SPI provider); separate adapter modules bridge this interface
 * to Micrometer ({@code peak-ewma-micrometer}) and OpenTelemetry ({@code peak-ewma-opentelemetry}).
 * Any other backend can be supported by implementing this interface and passing it to {@link
 * dev.parkerharrelson.grpc.peakewma.PeakEwmaP2CProvider#register(LbMetrics)}.
 *
 * <p>Implementations are called from gRPC's synchronization context and from the pick path, so they
 * must be thread-safe and cheap.
 */
public interface LbMetrics {
    /** Records a single pick outcome (ok, fallback, no_ready, …). */
    void recordPick(String outcome);

    /** Sets the current inflight stream count for the given subchannel. */
    void setInflight(String subchannelId, int inflight);

    /** Sets the computed picker cost for the given (subchannel, method) pair. */
    void setCost(String subchannelId, String method, double cost);

    /** Records a decision to eject a subchannel due to outlier behaviour. */
    void recordOutlierEjection(
            String subchannelId, String reason, double errorRate, double latencyRatio);

    /** Sets the number of ready subchannels. */
    void setReadySubchannelCount(int readyCount);

    /** Sets the number of currently-ejected subchannels. */
    void setEjectedSubchannelCount(int ejectedCount);

    /** Sets an adaptively-tuned value (e.g. effective inflight weight, effective window). */
    void setAdaptiveTuning(String key, double value);

    /** Publishes the current slow and fast EWMAs (microseconds) for a method. */
    void setMethodLatencyEwma(String method, double slowEwmaMicros, double fastEwmaMicros);

    /** Drops all metric state for the given subchannel; called when gRPC removes it. */
    void removeSubchannel(String subchannelId);

    /** Sets the current per-second call rate for a method. */
    void setMethodRate(String method, double ratePerSec);

    /** Sets the current error rate for a method (0.0–1.0). */
    void setMethodErrorRate(String method, double errorRate);

    /**
     * Records one observed RTT sample for a given method. Implementations typically expose this as
     * a timer/histogram so dashboards can show p50/p95/p99 alongside the EWMA gauges.
     *
     * @param method full gRPC method name
     * @param rttNanos observed stream RTT in nanoseconds
     */
    default void recordObservedRtt(String method, long rttNanos) {
        // default no-op so existing implementations need not be updated
    }

    /**
     * Publishes the fleet-derived scale of a method, recomputed every outlier tick: the peak and
     * baseline half-lives before the per-peer noise adjustment, and the seed latency a backend that
     * hasn't served the method yet is scored at ({@code NaN} until known).
     */
    default void setMethodScale(
            String method,
            double peakHalfLifeMillis,
            double baselineHalfLifeMillis,
            double seedMicros) {
        // default no-op so existing implementations need not be updated
    }

    /** Records how long one outlier tick took on the synchronization context. */
    default void recordOutlierTick(long durationNanos) {
        // default no-op so existing implementations need not be updated
    }

    /**
     * Records one sampled {@code pickSubchannel} duration. Only called when {@link SampledTimers}
     * is enabled, for about one pick in {@link SampledTimers#EVERY}.
     */
    default void recordPickNanos(long durationNanos) {
        // default no-op so existing implementations need not be updated
    }

    /**
     * Records one sampled stream-tracer callback duration; {@code callback} is {@code
     * streamCreated} or {@code streamClosed}. Only called when {@link SampledTimers} is enabled.
     */
    default void recordTracerNanos(String callback, long durationNanos) {
        // default no-op so existing implementations need not be updated
    }
}
