package dev.parkerharrelson.grpc.peakewma;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.parkerharrelson.grpc.peakewma.metrics.NoopLbMetrics;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancerRegistry;
import io.grpc.NameResolver.ConfigOrError;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PeakEwmaP2CProviderTest {

    @Test
    void basics() {
        PeakEwmaP2CProvider p = new PeakEwmaP2CProvider();
        assertThat(p.isAvailable()).isTrue();
        assertThat(p.getPriority()).isEqualTo(PeakEwmaP2CProvider.DEFAULT_PRIORITY);
        assertThat(p.getPolicyName()).isEqualTo(PeakEwmaConfigKeys.POLICY_NAME);
    }

    @Test
    void newLoadBalancer_returnsBalancer() {
        LoadBalancer.Helper helper = org.mockito.Mockito.mock(LoadBalancer.Helper.class);
        LoadBalancer lb = new PeakEwmaP2CProvider().newLoadBalancer(helper);
        assertThat(lb).isInstanceOf(PeakEwmaP2CBalancer.class);
    }

    @Test
    void defaultRegistry_withJarOnClasspath_discoversProviderViaSpi() {
        assertThat(
                        LoadBalancerRegistry.getDefaultRegistry()
                                .getProvider(PeakEwmaConfigKeys.POLICY_NAME))
                .isInstanceOf(PeakEwmaP2CProvider.class);
    }

    @Test
    void register_withMetrics_winsOverSpiDefault() {
        LoadBalancerRegistry registry = new LoadBalancerRegistry();
        PeakEwmaP2CProvider spiDefault = new PeakEwmaP2CProvider();
        registry.register(spiDefault);

        PeakEwmaP2CProvider registered =
                PeakEwmaP2CProvider.register(registry, NoopLbMetrics.INSTANCE);

        assertThat(registered.getPriority()).isEqualTo(PeakEwmaP2CProvider.METRICS_PRIORITY);
        assertThat(registry.getProvider(PeakEwmaConfigKeys.POLICY_NAME)).isSameAs(registered);

        registry.deregister(registered);
        assertThat(registry.getProvider(PeakEwmaConfigKeys.POLICY_NAME)).isSameAs(spiDefault);
    }

    @Test
    void register_withNullMetrics_throws() {
        assertThatThrownBy(() -> PeakEwmaP2CProvider.register(new LoadBalancerRegistry(), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void parseLoadBalancingPolicyConfig_withOverrides_returnsMergedConfig() {
        ConfigOrError parsed =
                new PeakEwmaP2CProvider()
                        .parseLoadBalancingPolicyConfig(
                                Map.of(
                                        PeakEwmaConfigKeys.INFLIGHT_WEIGHT, 0.25,
                                        // JSON numbers arrive as doubles
                                        PeakEwmaConfigKeys.TAU_FAST_MILLIS, 1500.0));

        assertThat(parsed.getError()).isNull();
        assertThat(parsed.getConfig()).isInstanceOf(PeakEwmaConfig.class);
        PeakEwmaConfig cfg = (PeakEwmaConfig) parsed.getConfig();
        assertThat(cfg.inflightWeight).isEqualTo(0.25);
        assertThat(cfg.tauFastMillis).isEqualTo(1500L);
        assertThat(cfg.tauSlowMillis).isEqualTo(PeakEwmaConfig.DEFAULTS.tauSlowMillis);
    }

    @Test
    void parseLoadBalancingPolicyConfig_withEmptyMap_returnsDefaults() {
        ConfigOrError parsed = new PeakEwmaP2CProvider().parseLoadBalancingPolicyConfig(Map.of());

        assertThat(parsed.getConfig()).isSameAs(PeakEwmaConfig.DEFAULTS);
    }

    @Test
    void parseLoadBalancingPolicyConfig_withInvalidValue_returnsError() {
        ConfigOrError parsed =
                new PeakEwmaP2CProvider()
                        .parseLoadBalancingPolicyConfig(
                                Map.of(PeakEwmaConfigKeys.INFLIGHT_WEIGHT, -1.0));

        assertThat(parsed.getConfig()).isNull();
        assertThat(parsed.getError().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(parsed.getError().getDescription()).contains("inflightWeight");
    }

    @Test
    void parseLoadBalancingPolicyConfig_withWrongType_returnsError() {
        ConfigOrError parsed =
                new PeakEwmaP2CProvider()
                        .parseLoadBalancingPolicyConfig(
                                Map.of(PeakEwmaConfigKeys.TAU_FAST_MILLIS, List.of(1)));

        assertThat(parsed.getError()).isNotNull();
    }
}
