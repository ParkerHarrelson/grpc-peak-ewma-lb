package dev.parkerharrelson.grpc.peakewma.harness.scenario;

import dev.parkerharrelson.grpc.peakewma.harness.channel.HarnessChannel;
import dev.parkerharrelson.grpc.peakewma.harness.server.HarnessBackend;
import dev.parkerharrelson.grpc.peakewma.harness.workload.CallStats;
import dev.parkerharrelson.grpc.peakewma.harness.workload.WorkloadDriver;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Shared lifecycle for a harness run: stands up N backends, builds a channel with the named policy,
 * drives traffic, and returns collected stats. Callers mutate backend behaviour in-between via the
 * returned {@link Handle}.
 */
public final class Scenario {

    /**
     * How long {@link #start(int, String)} will wait for the underlying channel to reach READY
     * before giving up. Slow CI runners can take well over 300 ms for the first localhost gRPC
     * handshake; the previous fixed sleep occasionally let the workload start while the channel was
     * still CONNECTING. The timeout only bounds the wait — once READY fires the start returns
     * immediately.
     */
    private static final long CHANNEL_READY_TIMEOUT_MILLIS = 10_000L;

    private Scenario() {}

    /** Runtime handle returned from {@link #start(int, String)}. */
    public record Handle(
            List<HarnessBackend> backends, HarnessChannel harnessChannel, CallStats stats) {}

    /**
     * Spins up {@code backendCount} healthy backends on localhost, builds a channel with the given
     * {@code policy}, and returns a handle the caller can drive.
     *
     * <p>The workload's fast/slow method mix is a per-run setting supplied to {@link
     * #driveFor(Handle, int, double, Duration)}, not a property of the scenario lifecycle.
     */
    public static Handle start(int backendCount, String policy) throws IOException {
        List<HarnessBackend> backends = new ArrayList<>(backendCount);
        for (int i = 0; i < backendCount; i++) {
            backends.add(new HarnessBackend("b" + i));
        }

        HarnessChannel harnessChannel =
                new HarnessChannel(backends, policy, HarnessChannel.newPrometheusRegistry());

        // Wait for the channel to reach READY rather than relying on a fixed sleep — slow CI
        // runners take longer than the previous 300 ms grace and would let the workload start
        // while the load balancer was still in CONNECTING.
        awaitChannelReady(harnessChannel.channel(), CHANNEL_READY_TIMEOUT_MILLIS);

        return new Handle(backends, harnessChannel, new CallStats());
    }

    /**
     * Polls {@link ManagedChannel#getState(boolean)} and parks on {@link
     * ManagedChannel#notifyWhenStateChanged} until the channel is READY or {@code timeoutMillis}
     * elapses. Returns immediately on READY; the timeout is a hard upper bound rather than a normal
     * expectation.
     */
    private static void awaitChannelReady(ManagedChannel channel, long timeoutMillis) {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            ConnectivityState state = channel.getState(/* requestConnection= */ true);
            if (state == ConnectivityState.READY) {
                return;
            }
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0L) {
                return;
            }
            CountDownLatch latch = new CountDownLatch(1);
            channel.notifyWhenStateChanged(state, latch::countDown);
            try {
                if (!latch.await(remainingNanos, TimeUnit.NANOSECONDS)) {
                    // Deadline elapsed before the channel left its previous state; give up and
                    // let the caller proceed against whatever state it ended up in.
                    return;
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** Drives an open-loop workload for {@code duration} at {@code qps}. */
    public static void driveFor(Handle h, int qps, double fastRatio, Duration duration)
            throws InterruptedException {
        Map<Integer, String> portToId = new HashMap<>();
        for (HarnessBackend b : h.backends) {
            portToId.put(b.port(), b.id());
        }
        try (WorkloadDriver d =
                new WorkloadDriver(h.harnessChannel.channel(), h.stats, fastRatio, portToId)) {
            d.run(qps, duration);
        }
    }

    /** Tears everything down in reverse order. */
    public static void stop(Handle h) {
        h.harnessChannel.close();
        for (HarnessBackend b : h.backends) {
            b.close();
        }
    }
}
