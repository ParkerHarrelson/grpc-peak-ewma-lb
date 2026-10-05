package dev.parkerharrelson.grpc.peakewma.loadtest;

import io.grpc.ConnectivityState;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancerProvider;
import io.grpc.LoadBalancerRegistry;
import io.grpc.NameResolver.ConfigOrError;
import io.grpc.util.ForwardingLoadBalancer;
import io.grpc.util.ForwardingLoadBalancerHelper;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.HdrHistogram.Recorder;

/**
 * {@code timed_<policy>}: the named policy, unchanged, with its pickers wrapped so that about one
 * pick in {@link #EVERY} is timed into {@link #PICK_NANOS}. Identical for every policy, so the pick
 * times compare like for like (the core's own sampled timer covers only peak_ewma_p2c).
 */
final class TimedPolicyProvider extends LoadBalancerProvider {
    static final int EVERY = 1000;
    static final Recorder PICK_NANOS = new Recorder(3);

    private final String child;

    TimedPolicyProvider(String child) {
        this.child = child;
    }

    static String register(String child) {
        LoadBalancerRegistry.getDefaultRegistry().register(new TimedPolicyProvider(child));
        return "timed_" + child;
    }

    private LoadBalancerProvider childProvider() {
        LoadBalancerProvider p = LoadBalancerRegistry.getDefaultRegistry().getProvider(child);
        if (p == null) throw new IllegalStateException("no LB policy " + child);
        return p;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public int getPriority() {
        return 5;
    }

    @Override
    public String getPolicyName() {
        return "timed_" + child;
    }

    @Override
    public ConfigOrError parseLoadBalancingPolicyConfig(Map<String, ?> rawConfig) {
        return childProvider().parseLoadBalancingPolicyConfig(rawConfig);
    }

    @Override
    public LoadBalancer newLoadBalancer(LoadBalancer.Helper helper) {
        LoadBalancer delegate =
                childProvider()
                        .newLoadBalancer(
                                new ForwardingLoadBalancerHelper() {
                                    @Override
                                    protected LoadBalancer.Helper delegate() {
                                        return helper;
                                    }

                                    @Override
                                    public void updateBalancingState(
                                            ConnectivityState state,
                                            LoadBalancer.SubchannelPicker picker) {
                                        helper.updateBalancingState(state, new TimedPicker(picker));
                                    }
                                });
        return new ForwardingLoadBalancer() {
            @Override
            protected LoadBalancer delegate() {
                return delegate;
            }
        };
    }

    private static final class TimedPicker extends LoadBalancer.SubchannelPicker {
        private final LoadBalancer.SubchannelPicker delegate;

        TimedPicker(LoadBalancer.SubchannelPicker delegate) {
            this.delegate = delegate;
        }

        @Override
        public LoadBalancer.PickResult pickSubchannel(LoadBalancer.PickSubchannelArgs args) {
            if (ThreadLocalRandom.current().nextInt(EVERY) != 0) {
                return delegate.pickSubchannel(args);
            }
            long t0 = System.nanoTime();
            LoadBalancer.PickResult r = delegate.pickSubchannel(args);
            PICK_NANOS.recordValue(Math.max(1, System.nanoTime() - t0));
            return r;
        }
    }
}
