package dev.parkerharrelson.grpc.peakewma.harness.workload;

import dev.parkerharrelson.grpc.peakewma.harness.server.InjectableService;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Open-loop workload driver: schedules requests at a target QPS onto a virtual-thread executor,
 * records each outcome into a {@link CallStats}, and stops cleanly when the requested duration
 * elapses.
 *
 * <p>Open-loop means "request arrivals are independent of response times"; the driver does NOT wait
 * for the previous call to finish before dispatching the next one, which is the workload pattern
 * that actually exercises the load balancer.
 *
 * <p>Uses Java 21 virtual threads so tens of thousands of in-flight calls cost little more than a
 * few MB of heap — no need to tune a thread pool size per scenario.
 */
public final class WorkloadDriver implements AutoCloseable {

    private final ManagedChannel channel;
    private final CallStats stats;
    private final double fastRatio;
    private final Map<Integer, String> portToBackendId;

    private final ScheduledExecutorService arrivalScheduler;
    private final ExecutorService callerPool;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * @param channel the managed channel under test
     * @param stats output sink
     * @param fastRatio fraction of requests that hit the {@code Fast} method (rest hit {@code
     *     Slow})
     * @param portToBackendId localhost port → backend id map, used to attribute each call
     */
    public WorkloadDriver(
            ManagedChannel channel,
            CallStats stats,
            double fastRatio,
            Map<Integer, String> portToBackendId) {
        this.channel = channel;
        this.stats = stats;
        this.fastRatio = fastRatio;
        this.portToBackendId = Map.copyOf(portToBackendId);
        this.arrivalScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "harness-arrival");
                            t.setDaemon(true);
                            return t;
                        });
        this.callerPool = Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Runs an open-loop workload for {@code duration} at {@code targetQps}.
     *
     * <p>Blocks until {@code duration} elapses, then drains in-flight calls for up to 3 seconds.
     * Any requests still outstanding after that drain are counted as failures by their own 5-second
     * per-call timeout rather than being waited on indefinitely — this prevents a hung backend from
     * blocking the whole run.
     */
    public void run(int targetQps, Duration duration) throws InterruptedException {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("driver already running");
        }

        long periodNanos = Math.max(1L, TimeUnit.SECONDS.toNanos(1) / Math.max(1, targetQps));
        long endNanos = System.nanoTime() + duration.toNanos();
        AtomicInteger outstanding = new AtomicInteger(0);

        Runnable scheduleOne =
                () -> {
                    if (System.nanoTime() >= endNanos) return;
                    outstanding.incrementAndGet();
                    callerPool.submit(
                            () -> {
                                try {
                                    issueCall();
                                } finally {
                                    outstanding.decrementAndGet();
                                }
                            });
                };

        arrivalScheduler.scheduleAtFixedRate(scheduleOne, 0, periodNanos, TimeUnit.NANOSECONDS);

        long remaining = endNanos - System.nanoTime();
        if (remaining > 0) {
            Thread.sleep(remaining / 1_000_000L, (int) (remaining % 1_000_000L));
        }

        arrivalScheduler.shutdownNow();
        long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (outstanding.get() > 0 && System.nanoTime() < waitUntil) {
            Thread.sleep(20);
        }
        running.set(false);
    }

    private void issueCall() {
        MethodDescriptor<byte[], byte[]> method =
                ThreadLocalRandom.current().nextDouble() < fastRatio
                        ? InjectableService.fastDescriptor()
                        : InjectableService.slowDescriptor();

        ClientCall<byte[], byte[]> call = channel.newCall(method, CallOptions.DEFAULT);
        final String[] observedBackend = new String[] {"unknown"};
        long start = System.nanoTime();
        CompletableFuture<Status> done = new CompletableFuture<>();

        call.start(
                new ClientCall.Listener<byte[]>() {
                    @Override
                    public void onHeaders(Metadata headers) {
                        SocketAddress remote =
                                call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                        if (remote instanceof InetSocketAddress isa) {
                            String id = portToBackendId.get(isa.getPort());
                            if (id != null) observedBackend[0] = id;
                        }
                    }

                    @Override
                    public void onMessage(byte[] message) {
                        // ignore body
                    }

                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        done.complete(status);
                    }
                },
                new Metadata());
        call.sendMessage(new byte[] {1});
        call.halfClose();
        call.request(1);

        try {
            Status status = done.get(5, TimeUnit.SECONDS);
            long elapsed = System.nanoTime() - start;
            stats.recordCall(observedBackend[0], status.isOk(), elapsed);
        } catch (Exception e) {
            // Cancel the still-running call so we don't leak in-flight RPCs on timeout. gRPC's
            // ClientCall.cancel is idempotent and safe to invoke even if onClose already fired.
            try {
                call.cancel("WorkloadDriver timeout/interrupt", e);
            } catch (RuntimeException ignored) {
                // best-effort cancel
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            long elapsed = System.nanoTime() - start;
            stats.recordCall(observedBackend[0], false, elapsed);
        }
    }

    @Override
    public void close() {
        arrivalScheduler.shutdownNow();
        callerPool.shutdownNow();
    }
}
