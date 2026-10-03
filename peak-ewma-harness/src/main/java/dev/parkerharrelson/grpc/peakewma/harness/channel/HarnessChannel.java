package dev.parkerharrelson.grpc.peakewma.harness.channel;

import dev.parkerharrelson.grpc.peakewma.PeakEwmaP2CProvider;
import dev.parkerharrelson.grpc.peakewma.harness.server.HarnessBackend;
import dev.parkerharrelson.grpc.peakewma.metrics.LbMetrics;
import dev.parkerharrelson.grpc.peakewma.metrics.NoopLbMetrics;
import dev.parkerharrelson.grpc.peakewma.micrometer.MicrometerLbMetrics;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancerProvider;
import io.grpc.LoadBalancerRegistry;
import io.grpc.ManagedChannel;
import io.grpc.NameResolverRegistry;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Construction helper for the single {@link ManagedChannel} used by the harness.
 *
 * <p>Binds together a {@link StaticAddressesNameResolverProvider} registration, a Netty channel
 * builder, the {@code peak_ewma_p2c} load balancing policy, and a metrics sink.
 *
 * <p>Only one harness channel should be active per JVM at a time — the resolver provider and LB
 * provider are registered into gRPC's global registries.
 */
public final class HarnessChannel implements AutoCloseable {

    static {
        NameResolverRegistry.getDefaultRegistry()
                .register(new StaticAddressesNameResolverProvider());
    }

    private final String resolverName;
    private final ManagedChannel channel;
    private final PrometheusMeterRegistry meterRegistry;
    private final LoadBalancerProvider peakEwmaProvider;

    public HarnessChannel(
            List<HarnessBackend> backends, String policy, PrometheusMeterRegistry meterRegistry) {
        Objects.requireNonNull(backends, "backends");
        if (backends.isEmpty()) throw new IllegalArgumentException("need at least one backend");

        this.meterRegistry = meterRegistry;
        this.resolverName = "harness-" + UUID.randomUUID();

        StaticAddressesNameResolverProvider.register(
                resolverName,
                backends.stream().map(b -> new EquivalentAddressGroup(b.address())).toList());

        // Register the Peak-EWMA provider with a concrete metrics sink so we can read real
        // Micrometer state during the run; register() uses a priority that beats the SPI default.
        // Retain a reference so close() can deregister it and not leak providers across repeated
        // harness runs in the same JVM (e.g. scenario JUnit classes).
        LbMetrics lbMetrics =
                meterRegistry != null
                        ? new MicrometerLbMetrics(meterRegistry)
                        : NoopLbMetrics.INSTANCE;
        this.peakEwmaProvider = PeakEwmaP2CProvider.register(lbMetrics);

        this.channel =
                NettyChannelBuilder.forTarget(
                                StaticAddressesNameResolverProvider.SCHEME + ":///" + resolverName)
                        .usePlaintext()
                        .defaultLoadBalancingPolicy(policy)
                        .disableServiceConfigLookUp()
                        .build();
    }

    public ManagedChannel channel() {
        return channel;
    }

    public PrometheusMeterRegistry meterRegistry() {
        return meterRegistry;
    }

    /**
     * Convenience constructor: defaults to {@code peak_ewma_p2c} with a fresh
     * PrometheusMeterRegistry so {@link dev.parkerharrelson.grpc.peakewma.harness.report.Reporter}
     * can emit real Prometheus scrape text.
     */
    public static HarnessChannel peakEwma(List<HarnessBackend> backends) {
        return new HarnessChannel(backends, "peak_ewma_p2c", newPrometheusRegistry());
    }

    /** Convenience constructor: {@code round_robin} baseline. */
    public static HarnessChannel roundRobin(List<HarnessBackend> backends) {
        return new HarnessChannel(backends, "round_robin", newPrometheusRegistry());
    }

    /** Fresh PrometheusMeterRegistry seeded with the default {@link PrometheusConfig}. */
    public static PrometheusMeterRegistry newPrometheusRegistry() {
        return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    }

    @Override
    public void close() {
        channel.shutdownNow();
        try {
            channel.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        StaticAddressesNameResolverProvider.deregister(resolverName);
        LoadBalancerRegistry.getDefaultRegistry().deregister(peakEwmaProvider);
    }
}
