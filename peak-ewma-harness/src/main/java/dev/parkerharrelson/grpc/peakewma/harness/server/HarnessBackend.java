package dev.parkerharrelson.grpc.peakewma.harness.server;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * One simulated backend: a real Netty gRPC server bound to an ephemeral localhost port, serving an
 * {@link InjectableService} whose response profile can be flipped at runtime via its attached
 * {@link BackendBehaviour}.
 *
 * <p>The harness uses real sockets (rather than {@code InProcessServerBuilder}) so the load
 * balancer under test goes through actual subchannel connect/ready transitions and Netty I/O, which
 * is the code path we care about validating.
 */
public final class HarnessBackend implements AutoCloseable {

    /**
     * Harness backends always bind to the loopback interface — this module is a non-deployed local
     * test harness. Never use this class on a reachable interface; the gRPC server it exposes has
     * no authentication and will execute every request its simulated behaviour snapshot describes.
     */
    private static final String LOOPBACK_HOST = "127.0.0.1";

    private final String id;
    private final BackendBehaviour behaviour;
    private final InjectableService service;
    private final ScheduledExecutorService latencyScheduler;
    private final Server server;

    public HarnessBackend(String id) throws IOException {
        this(id, new BackendBehaviour());
    }

    public HarnessBackend(String id, BackendBehaviour behaviour) throws IOException {
        this.id = id;
        this.behaviour = behaviour;
        // One small scheduler per backend so a slow / outage backend doesn't block the others.
        this.latencyScheduler =
                Executors.newScheduledThreadPool(
                        2,
                        r -> {
                            Thread t = new Thread(r, "harness-latency-" + id);
                            t.setDaemon(true);
                            return t;
                        });
        this.service = new InjectableService(behaviour, latencyScheduler);

        this.server =
                NettyServerBuilder.forAddress(new InetSocketAddress(LOOPBACK_HOST, 0))
                        .addService(service.bindService())
                        .build()
                        .start();
    }

    public String id() {
        return id;
    }

    public int port() {
        return server.getPort();
    }

    public InetSocketAddress address() {
        return new InetSocketAddress(LOOPBACK_HOST, port());
    }

    public BackendBehaviour behaviour() {
        return behaviour;
    }

    public InjectableService service() {
        return service;
    }

    @Override
    public void close() {
        server.shutdownNow();
        latencyScheduler.shutdownNow();
        try {
            server.awaitTermination(2, TimeUnit.SECONDS);
            latencyScheduler.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return "HarnessBackend{id=" + id + ", port=" + port() + "}";
    }
}
