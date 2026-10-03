package dev.parkerharrelson.grpc.peakewma.bench;

import dev.parkerharrelson.grpc.peakewma.harness.server.BackendBehaviour;
import dev.parkerharrelson.grpc.peakewma.harness.server.InjectableService;
import io.grpc.CallOptions;
import io.grpc.ConnectivityState;
import io.grpc.EquivalentAddressGroup;
import io.grpc.ManagedChannel;
import io.grpc.NameResolverRegistry;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.inprocess.InProcessSocketAddress;
import io.grpc.stub.ClientCalls;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * N in-process backends plus one channel using a given LB policy.
 *
 * <p>In-process transport keeps network/TLS/Netty cost out of the measurement, so differences
 * between policies are LB overhead. Backends run with a direct executor and (by default) zero
 * latency, so server-side CPU is small and identical across policies.
 */
final class BenchCluster implements AutoCloseable {

    /** grpc-java policies compared against peak_ewma_p2c. */
    static final List<String> POLICIES =
            List.of(
                    "pick_first",
                    "round_robin",
                    "least_request_experimental",
                    "weighted_round_robin",
                    "peak_ewma_p2c");

    static {
        NameResolverRegistry.getDefaultRegistry().register(new InProcessResolverProvider());
    }

    final List<Server> servers = new ArrayList<>();
    final List<BackendBehaviour> behaviours = new ArrayList<>();
    final List<InjectableService> services = new ArrayList<>();
    private final ScheduledExecutorService latencyScheduler;
    private final String resolverName = "bench-" + UUID.randomUUID();
    private ManagedChannel channel;
    private CapturingLoadBalancerProvider capture;

    /** Starts {@code n} healthy backends with the given fixed latency (ZERO = respond inline). */
    BenchCluster(int n, Duration latency) throws IOException {
        latencyScheduler =
                Executors.newScheduledThreadPool(
                        4,
                        r -> {
                            Thread t = new Thread(r, "bench-latency");
                            t.setDaemon(true);
                            return t;
                        });
        List<EquivalentAddressGroup> eags = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            BackendBehaviour b = new BackendBehaviour();
            b.setHealthy(latency, latency.isZero() ? 0.0 : 0.2);
            InjectableService svc = new InjectableService(b, latencyScheduler);
            String name = resolverName + "-b" + i;
            servers.add(
                    InProcessServerBuilder.forName(name)
                            .directExecutor()
                            .addService(svc.bindService())
                            .build()
                            .start());
            behaviours.add(b);
            services.add(svc);
            eags.add(new EquivalentAddressGroup(new InProcessSocketAddress(name)));
        }
        InProcessResolverProvider.register(resolverName, eags);
    }

    /** Builds the channel. With {@code capturePicker} the policy's live picker is observable. */
    ManagedChannel connect(String policy, boolean capturePicker) throws InterruptedException {
        if (capturePicker) {
            capture = CapturingLoadBalancerProvider.install(policy);
        }
        channel =
                InProcessChannelBuilder.forTarget(
                                InProcessResolverProvider.SCHEME + ":///" + resolverName)
                        .defaultServiceConfig(serviceConfig(policy))
                        .disableServiceConfigLookUp()
                        .build();
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (channel.getState(true) != ConnectivityState.READY && System.nanoTime() < end) {
            Thread.sleep(5);
        }
        if (channel.getState(false) != ConnectivityState.READY) {
            throw new IllegalStateException(policy + ": channel never became READY");
        }
        Thread.sleep(200); // let every subchannel finish connecting
        return channel;
    }

    static Map<String, ?> serviceConfig(String policy) {
        Map<String, ?> cfg =
                switch (policy) {
                    case "least_request_experimental" -> Map.of("choiceCount", 2.0);
                    default -> Map.of();
                };
        return Map.of("loadBalancingConfig", List.of(Map.of(policy, cfg)));
    }

    /** Sequential warm-up RPCs (JIT + per-backend LB state). Errors are ignored. */
    void warm(int rpcs) {
        for (int i = 0; i < rpcs; i++) {
            call(2_000);
        }
    }

    boolean call(long deadlineMs) {
        try {
            ClientCalls.blockingUnaryCall(
                    channel,
                    InjectableService.fastDescriptor(),
                    CallOptions.DEFAULT.withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS),
                    PAYLOAD);
            return true;
        } catch (StatusRuntimeException e) {
            return false;
        }
    }

    private static final byte[] PAYLOAD = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};

    ManagedChannel channel() {
        return channel;
    }

    io.grpc.LoadBalancer.SubchannelPicker picker() {
        if (capture == null) throw new IllegalStateException("connect(policy, true) first");
        return capture.latestPicker();
    }

    long[] servedPerBackend() {
        long[] out = new long[services.size()];
        for (int i = 0; i < out.length; i++) out[i] = services.get(i).fastCalls();
        return out;
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.shutdownNow();
            try {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (capture != null) capture.uninstall();
        for (Server s : servers) s.shutdownNow();
        latencyScheduler.shutdownNow();
        InProcessResolverProvider.deregister(resolverName);
    }
}
