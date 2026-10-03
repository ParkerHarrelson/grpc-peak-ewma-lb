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
    void defaultServiceConfig_withInflightWeight_isAppliedByBalancer() throws Exception {
        // With one ready backend and nothing inflight, inflightWeightEff == configured base weight.
        List<Double> inflightWeightEff = new CopyOnWriteArrayList<>();
        LbMetrics recording =
                (LbMetrics)
                        Proxy.newProxyInstance(
                                LbMetrics.class.getClassLoader(),
                                new Class<?>[] {LbMetrics.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("setAdaptiveTuning")
                                            && LBConstants.INFLIGHT_WEIGHT_EFF.equals(args[0])) {
                                        inflightWeightEff.add((Double) args[1]);
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
                                            Map.of(PeakEwmaConfigKeys.INFLIGHT_WEIGHT, 0.25))));

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

            assertThat(inflightWeightEff).isNotEmpty();
            assertThat(inflightWeightEff.get(inflightWeightEff.size() - 1)).isEqualTo(0.25);
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
