package dev.parkerharrelson.grpc.peakewma;

import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.METHOD_A;
import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Attributes;
import io.grpc.ClientStreamTracer;
import io.grpc.LoadBalancer.PickResult;
import io.grpc.LoadBalancer.PickSubchannelArgs;
import io.grpc.LoadBalancer.SubchannelPicker;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * CPU / allocation / scalability measurements. Not JMH-grade, but the effects measured here are
 * orders of magnitude, not percent. Results are printed as [perf] lines.
 */
@Tag("adversarial")
class AdversarialPerformanceTest {

    private static final PeakEwmaConfig CFG = PeakEwmaConfig.DEFAULTS;

    private static AdversarialFixture warmFleet(int n, MethodDescriptor<?, ?>... methods) {
        AdversarialFixture f = new AdversarialFixture(CFG);
        int[] ports = new int[n];
        for (int i = 0; i < n; i++) {
            ports[i] = 20_000 + i;
            f.models.put(ports[i], AdversarialFixture.jittered(5.0, 0.1, i));
        }
        f.resolveAndReady(ports);
        f.run(Math.max(500, n * 20), 5_000, methods);
        f.drain();
        return f;
    }

    /** ns per pickSubchannel() call, single thread, fake clock advancing 1us per pick. */
    private static double nsPerPick(AdversarialFixture f, int iters) {
        SubchannelPicker p = f.latest().picker();
        PickSubchannelArgs args = AdversarialFixture.args(METHOD_A);
        long sink = 0;
        for (int i = 0; i < iters / 4; i++) { // warmup
            f.now.addAndGet(1_000);
            sink += p.pickSubchannel(args).hashCode();
        }
        long t0 = System.nanoTime();
        for (int i = 0; i < iters; i++) {
            f.now.addAndGet(1_000);
            sink += p.pickSubchannel(args).hashCode();
        }
        long el = System.nanoTime() - t0;
        if (sink == 42) System.out.print("");
        return (double) el / iters;
    }

    /**
     * P2C's selling point is O(1) picks: sample two, compare two. This picker scores EVERY ready
     * backend on every pick (cost() for i in 0..n, each doing 2 map lookups, 2 computeIfAbsent, an
     * ErrorWindow snapshot summing 20 LongAdders, and two Math.log calls), so pick cost grows
     * linearly with fleet size.
     */
    @Test
    void pickCost_isIndependentOfFleetSize() {
        int[] sizes = {2, 10, 100, 500};
        double[] ns = new double[sizes.length];
        for (int i = 0; i < sizes.length; i++) {
            AdversarialFixture f = warmFleet(sizes[i], METHOD_A);
            int iters = sizes[i] >= 100 ? 20_000 : 200_000;
            ns[i] = nsPerPick(f, iters);
            System.out.printf("[perf] pick n=%-4d %,10.0f ns/pick%n", sizes[i], ns[i]);
        }
        assertThat(ns[3] / ns[1])
                .as("pick cost at 500 backends / at 10 backends (O(1) would be ~1x)")
                .isLessThan(3.0);
    }

    /** Bytes allocated per pick (two n-sized arrays + tracer factory + lambdas per pick). */
    @Test
    void pickAllocation_isSmallAndConstant() {
        var mx = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long tid = Thread.currentThread().threadId();
        double[] perPick = new double[2];
        int[] sizes = {10, 500};
        for (int k = 0; k < sizes.length; k++) {
            AdversarialFixture f = warmFleet(sizes[k], METHOD_A);
            SubchannelPicker p = f.latest().picker();
            PickSubchannelArgs args = AdversarialFixture.args(METHOD_A);
            for (int i = 0; i < 5_000; i++) p.pickSubchannel(args);
            int iters = 20_000;
            long b0 = mx.getThreadAllocatedBytes(tid);
            for (int i = 0; i < iters; i++) p.pickSubchannel(args);
            perPick[k] = (double) (mx.getThreadAllocatedBytes(tid) - b0) / iters;
            System.out.printf("[perf] alloc n=%-4d %,8.0f bytes/pick%n", sizes[k], perPick[k]);
        }
        assertThat(perPick[1]).as("bytes allocated per pick with 500 backends").isLessThan(512);
    }

    /**
     * Full call path (pick + tracer create + streamCreated + streamClosed) on a hot method, 1
     * thread vs many. Shared hot spots: MethodStats.varLock per (backend, method), ErrorWindow ring
     * lock on bucket rotation, the AtomicInteger inflight counter, and CHM bins.
     */
    @Test
    void fullCallPath_scalesAcrossThreads() throws Exception {
        // Real System.nanoTime clock here, so the fake clock's AtomicLong isn't the bottleneck.
        AdversarialFixture f = new AdversarialFixture(CFG);
        AdversarialFixture.setField(f.balancer, "clocks", new EwmaClocks());
        int[] ports = new int[20];
        for (int i = 0; i < ports.length; i++) ports[i] = 21_000 + i;
        f.resolveAndReady(ports);
        throughput(f, 4, 1_000); // warm JIT + stats
        int cpus = Runtime.getRuntime().availableProcessors();
        int many = Math.max(2, Math.min(16, cpus));
        double one = throughput(f, 1, 1_500);
        double par = throughput(f, many, 1_500);
        System.out.printf(
                "[perf] full call path n=20: 1 thread %,.0f calls/s, %d threads %,.0f calls/s"
                        + " (%.1fx)%n",
                one, many, par, par / one);
        assertThat(par / one)
                .as("speed-up from 1 to %d threads (cpus=%d)", many, cpus)
                .isGreaterThan(many * 0.35);
    }

    private static double throughput(AdversarialFixture f, int threads, long millis)
            throws Exception {
        LongAdder calls = new LongAdder();
        long end = System.nanoTime() + millis * 1_000_000L;
        CountDownLatch done = new CountDownLatch(threads);
        SubchannelPicker p = f.latest().picker();
        for (int t = 0; t < threads; t++) {
            new Thread(
                            () -> {
                                PickSubchannelArgs args = AdversarialFixture.args(METHOD_A);
                                ClientStreamTracer.StreamInfo info =
                                        ClientStreamTracer.StreamInfo.newBuilder().build();
                                Metadata md = new Metadata();
                                while (System.nanoTime() < end) {
                                    for (int i = 0; i < 256; i++) {
                                        PickResult pr = p.pickSubchannel(args);
                                        ClientStreamTracer tr =
                                                pr.getStreamTracerFactory()
                                                        .newClientStreamTracer(info, md);
                                        tr.streamCreated(Attributes.EMPTY, md);
                                        tr.streamClosed(Status.OK);
                                    }
                                    calls.add(256);
                                }
                                done.countDown();
                            })
                    .start();
        }
        done.await();
        return calls.sum() * 1000.0 / millis;
    }

    /**
     * The outlier tick is O(backends^2 x methods): for every (backend, method) it calls
     * fleetRateForMethod, which snapshots that method's window on every other ready backend (and
     * creates windows for methods those backends never served) — twice. It runs every second on the
     * channel's shared scheduler.
     */
    @Test
    void outlierTick_isCheapForLargeFleets() {
        List<MethodDescriptor<byte[], byte[]>> ms = new ArrayList<>();
        for (int i = 0; i < 20; i++) ms.add(AdversarialFixture.method("svc/M" + i));
        int[] sizes = {50, 200, 400};
        double[] tickMs = new double[sizes.length];
        for (int k = 0; k < sizes.length; k++) {
            AdversarialFixture f = warmFleet(sizes[k], ms.toArray(new MethodDescriptor<?, ?>[0]));
            for (int i = 0; i < 2; i++) f.tick(); // warm
            int reps = 3;
            long t0 = System.nanoTime();
            for (int i = 0; i < reps; i++) {
                f.advanceMs(1000);
                f.tick();
            }
            tickMs[k] = (System.nanoTime() - t0) / 1e6 / reps;
            System.out.printf(
                    "[perf] outlier tick n=%-4d methods=20 %,8.1f ms/tick%n", sizes[k], tickMs[k]);
        }
        assertThat(tickMs[2]).as("ms per outlier tick, 400 backends x 20 methods").isLessThan(50.0);
    }
}
