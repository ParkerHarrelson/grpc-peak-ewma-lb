package dev.parkerharrelson.grpc.peakewma.harness;

import static org.assertj.core.api.Assertions.assertThat;

import dev.parkerharrelson.grpc.peakewma.LBConstants;
import dev.parkerharrelson.grpc.peakewma.PeakEwmaConfigKeys;
import dev.parkerharrelson.grpc.peakewma.PeakEwmaP2CProvider;
import dev.parkerharrelson.grpc.peakewma.harness.channel.StaticAddressesNameResolverProvider;
import dev.parkerharrelson.grpc.peakewma.harness.server.HarnessBackend;
import dev.parkerharrelson.grpc.peakewma.harness.server.InjectableService;
import dev.parkerharrelson.grpc.peakewma.metrics.LbMetrics;
import dev.parkerharrelson.grpc.peakewma.metrics.NoopLbMetrics;
import io.grpc.CallOptions;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancerRegistry;
import io.grpc.ManagedChannel;
import io.grpc.NameResolverRegistry;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * End-to-end check that tuning supplied through a real channel's service config reaches the
 * balancer, i.e. gRPC calls {@code parseLoadBalancingPolicyConfig} and the balancer accepts the
 * parsed result.
 */
class ServiceConfigWiringTest {

    @Test
    void defaultServiceConfig_withOutlierErrorRate_isAppliedByBalancer() throws Exception {
        // The outlier tick reports the configured error-rate threshold as a tuning gauge. (This
        // used inflightWeight, which is now derived rather than configured.)
        List<Double> errorRate = new CopyOnWriteArrayList<>();
        LbMetrics recording =
                (LbMetrics)
                        Proxy.newProxyInstance(
                                LbMetrics.class.getClassLoader(),
                                new Class<?>[] {LbMetrics.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("setAdaptiveTuning")
                                            && LBConstants.OUTLIER_ERROR_RATE.equals(args[0])) {
                                        errorRate.add((Double) args[1]);
                                    }
                                    return method.invoke(NoopLbMetrics.INSTANCE, args);
                                });

        PeakEwmaP2CProvider provider = PeakEwmaP2CProvider.register(recording);
        StaticAddressesNameResolverProvider resolverProvider =
                new StaticAddressesNameResolverProvider();
        NameResolverRegistry.getDefaultRegistry().register(resolverProvider);
        String resolverName = "svc-config-" + UUID.randomUUID();
        ManagedChannel channel = null;
        try (HarnessBackend backend = new HarnessBackend("b0")) {
            StaticAddressesNameResolverProvider.register(
                    resolverName, List.of(new EquivalentAddressGroup(backend.address())));

            Map<String, ?> serviceConfig =
                    Map.of(
                            "loadBalancingConfig",
                            List.of(
                                    Map.of(
                                            PeakEwmaConfigKeys.POLICY_NAME,
                                            Map.of(
                                                    PeakEwmaConfigKeys.OUTLIER_ERROR_RATE,
                                                    0.5,
                                                    PeakEwmaConfigKeys.OUTLIER_TICK_INTERVAL_MILLIS,
                                                    200.0))));

            channel =
                    NettyChannelBuilder.forTarget(
                                    StaticAddressesNameResolverProvider.SCHEME
                                            + ":///"
                                            + resolverName)
                            .usePlaintext()
                            .defaultServiceConfig(serviceConfig)
                            .disableServiceConfigLookUp()
                            .build();

            ClientCalls.blockingUnaryCall(
                    channel,
                    InjectableService.fastDescriptor(),
                    CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS),
                    new byte[0]);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (errorRate.isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(50); // the 200 ms tick (also from the service config) reports it
            }
            assertThat(errorRate).isNotEmpty();
            assertThat(errorRate.get(errorRate.size() - 1)).isEqualTo(0.5);
        } finally {
            if (channel != null) {
                channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
            }
            StaticAddressesNameResolverProvider.deregister(resolverName);
            LoadBalancerRegistry.getDefaultRegistry().deregister(provider);
            NameResolverRegistry.getDefaultRegistry().deregister(resolverProvider);
        }
    }
}
