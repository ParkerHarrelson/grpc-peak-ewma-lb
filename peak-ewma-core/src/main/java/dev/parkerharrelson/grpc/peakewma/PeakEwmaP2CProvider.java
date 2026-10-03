package dev.parkerharrelson.grpc.peakewma;

import static dev.parkerharrelson.grpc.peakewma.PeakEwmaConfigKeys.POLICY_NAME;

import dev.parkerharrelson.grpc.peakewma.metrics.LbMetrics;
import dev.parkerharrelson.grpc.peakewma.metrics.NoopLbMetrics;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancerProvider;
import io.grpc.LoadBalancerRegistry;
import io.grpc.NameResolver.ConfigOrError;
import io.grpc.Status;
import java.util.Map;
import java.util.Objects;

/**
 * {@link LoadBalancerProvider} for the Peak-EWMA Power-of-Two-Choices load balancer.
 *
 * <p>Registered via SPI ({@code META-INF/services/io.grpc.LoadBalancerProvider}) at {@link
 * #DEFAULT_PRIORITY} with a no-op metrics sink, so the policy is usable with zero code. To publish
 * metrics, call {@link #register(LbMetrics)} once at startup: it registers a second instance at
 * {@link #METRICS_PRIORITY}, which gRPC prefers over the SPI default for the same policy name.
 * Policy name is {@value PeakEwmaConfigKeys#POLICY_NAME}.
 */
public class PeakEwmaP2CProvider extends LoadBalancerProvider {

    /** Priority of the SPI-loaded, no-metrics instance. */
    public static final int DEFAULT_PRIORITY = 10;

    /** Priority used by {@link #register(LbMetrics)} so it wins over the SPI default. */
    public static final int METRICS_PRIORITY = 15;

    private final LbMetrics metrics;
    private final int priority;

    /** Zero-arg ctor used by the SPI loader; publishes metrics to a no-op sink. */
    public PeakEwmaP2CProvider() {
        this(NoopLbMetrics.INSTANCE);
    }

    /**
     * Creates a provider at {@link #DEFAULT_PRIORITY} that publishes to the given sink.
     *
     * @param metrics metrics sink; null is treated as {@link NoopLbMetrics#INSTANCE}
     */
    public PeakEwmaP2CProvider(LbMetrics metrics) {
        this(metrics, DEFAULT_PRIORITY);
    }

    /**
     * Creates a provider with an explicit priority. When several providers share a policy name,
     * gRPC uses the one with the highest priority.
     *
     * @param metrics metrics sink; null is treated as {@link NoopLbMetrics#INSTANCE}
     * @param priority gRPC provider priority
     */
    public PeakEwmaP2CProvider(LbMetrics metrics, int priority) {
        this.metrics = (metrics != null) ? metrics : NoopLbMetrics.INSTANCE;
        this.priority = priority;
    }

    /**
     * Registers a metrics-enabled provider with the default {@link LoadBalancerRegistry}.
     *
     * <p>Call once at application startup, before building channels that use {@value
     * PeakEwmaConfigKeys#POLICY_NAME}. Keep the returned provider if you need to {@linkplain
     * LoadBalancerRegistry#deregister(LoadBalancerProvider) deregister} it later (e.g. in tests).
     *
     * @param metrics metrics sink, e.g. a Micrometer or OpenTelemetry adapter
     * @return the registered provider
     */
    public static PeakEwmaP2CProvider register(LbMetrics metrics) {
        return register(LoadBalancerRegistry.getDefaultRegistry(), metrics);
    }

    /**
     * Registers a metrics-enabled provider with the given registry at {@link #METRICS_PRIORITY}.
     *
     * @param registry target registry
     * @param metrics metrics sink
     * @return the registered provider
     */
    public static PeakEwmaP2CProvider register(LoadBalancerRegistry registry, LbMetrics metrics) {
        Objects.requireNonNull(registry, "registry");
        Objects.requireNonNull(metrics, "metrics");
        PeakEwmaP2CProvider provider = new PeakEwmaP2CProvider(metrics, METRICS_PRIORITY);
        registry.register(provider);
        return provider;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public String getPolicyName() {
        return POLICY_NAME;
    }

    /**
     * Parses the {@code peak_ewma_p2c} entry of a service config's {@code loadBalancingConfig} into
     * a validated {@link PeakEwmaConfig}. Unspecified keys take their defaults.
     */
    @Override
    public ConfigOrError parseLoadBalancingPolicyConfig(Map<String, ?> rawConfig) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> config = (Map<String, Object>) rawConfig;
            return ConfigOrError.fromConfig(PeakEwmaConfig.fromMap(config));
        } catch (RuntimeException e) {
            return ConfigOrError.fromError(
                    Status.UNAVAILABLE
                            .withDescription(
                                    "Invalid " + POLICY_NAME + " config: " + e.getMessage())
                            .withCause(e));
        }
    }

    @Override
    public LoadBalancer newLoadBalancer(LoadBalancer.Helper helper) {
        return new PeakEwmaP2CBalancer(helper, PeakEwmaConfig.DEFAULTS, metrics);
    }
}
