package dev.parkerharrelson.grpc.peakewma.bench;

import io.grpc.ConnectivityState;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancerProvider;
import io.grpc.LoadBalancerRegistry;
import io.grpc.NameResolver.ConfigOrError;
import io.grpc.util.ForwardingLoadBalancerHelper;
import java.util.Map;

/**
 * Registers under the SAME policy name as a real provider, at a higher priority, and delegates to
 * it — only wrapping the Helper so the latest picker the real policy publishes can be read. Lets
 * the JMH picker benchmark drive any grpc-java policy's real picker, over real (in-process)
 * subchannels, without re-implementing a Helper per policy.
 */
final class CapturingLoadBalancerProvider extends LoadBalancerProvider {

    private final LoadBalancerProvider delegate;
    private volatile LoadBalancer.SubchannelPicker latestPicker;
    private volatile ConnectivityState latestState;

    private CapturingLoadBalancerProvider(LoadBalancerProvider delegate) {
        this.delegate = delegate;
    }

    static CapturingLoadBalancerProvider install(String policy) {
        LoadBalancerProvider real = LoadBalancerRegistry.getDefaultRegistry().getProvider(policy);
        if (real == null) {
            throw new IllegalStateException("no LoadBalancerProvider registered for " + policy);
        }
        if (real instanceof CapturingLoadBalancerProvider c) {
            return c;
        }
        CapturingLoadBalancerProvider p = new CapturingLoadBalancerProvider(real);
        LoadBalancerRegistry.getDefaultRegistry().register(p);
        return p;
    }

    void uninstall() {
        LoadBalancerRegistry.getDefaultRegistry().deregister(this);
    }

    LoadBalancer.SubchannelPicker latestPicker() {
        return latestPicker;
    }

    ConnectivityState latestState() {
        return latestState;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public int getPriority() {
        return 1_000;
    }

    @Override
    public String getPolicyName() {
        return delegate.getPolicyName();
    }

    @Override
    public ConfigOrError parseLoadBalancingPolicyConfig(Map<String, ?> rawConfig) {
        return delegate.parseLoadBalancingPolicyConfig(rawConfig);
    }

    @Override
    public LoadBalancer newLoadBalancer(LoadBalancer.Helper helper) {
        return delegate.newLoadBalancer(
                new ForwardingLoadBalancerHelper() {
                    @Override
                    protected LoadBalancer.Helper delegate() {
                        return helper;
                    }

                    @Override
                    public void updateBalancingState(
                            ConnectivityState state, LoadBalancer.SubchannelPicker picker) {
                        latestState = state;
                        latestPicker = picker;
                        helper.updateBalancingState(state, picker);
                    }
                });
    }
}
