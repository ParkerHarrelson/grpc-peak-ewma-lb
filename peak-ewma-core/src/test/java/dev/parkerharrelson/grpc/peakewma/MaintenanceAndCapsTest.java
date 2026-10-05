package dev.parkerharrelson.grpc.peakewma;

import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.METHOD_A;
import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.METHOD_B;
import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.parkerharrelson.grpc.peakewma.AdversarialFixture.Response;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancer.ResolvedAddresses;
import io.grpc.LoadBalancer.Subchannel;
import io.grpc.Status;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;

/** #99 (maintenance without outlier detection), #101 (ejection caps, n = 2), #102 (IDs). */
class MaintenanceAndCapsTest {

    private static AdversarialFixture fleet(PeakEwmaConfig cfg, int n, double latencyMs) {
        AdversarialFixture f = new AdversarialFixture(cfg);
        return populate(f, n, latencyMs);
    }

    private static AdversarialFixture populate(AdversarialFixture f, int n, double latencyMs) {
        int[] ports = new int[n];
        for (int i = 0; i < n; i++) {
            ports[i] = 7000 + i;
            f.models.put(ports[i], AdversarialFixture.jittered(latencyMs, 0.1, i));
        }
        f.resolveAndReady(ports);
        return f;
    }

    @SuppressWarnings("unchecked")
    private static SubchannelState state(AdversarialFixture f, int port) {
        var states =
                (Map<Subchannel, SubchannelState>)
                        AdversarialFixture.getField(f.balancer, "states");
        return states.get(f.byPort.get(port));
    }

    /**
     * #99: with outlier detection off, the tick must still adapt half-lives to the fleet and prune
     * idle methods; it just never ejects.
     */
    @Test
    void outlierDisabled_stillPrunesAndAdapts_butNeverEjects() {
        PeakEwmaConfig cfg = PeakEwmaConfig.builder().outlierEnabled(false).build();
        AdversarialFixture f = fleet(cfg, 3, 1.0);
        f.models.put(7000, (t, m) -> new Response(MS, Status.UNAVAILABLE));

        f.run(2_000, 5_000, METHOD_A, METHOD_B);
        f.drain();

        MethodStats a =
                AdversarialSimulationTest.table(f, 7001).peekStats(METHOD_A.getFullMethodName());
        assertNotNull(a);
        assertNotEquals(MethodScale.DEFAULT, a.scale(), "half-lives adapted to the fleet");
        assertFalse(state(f, 7000).isEjected(f.now.get()), "no ejection when disabled");

        // svc/B goes idle; svc/A keeps flowing. After the prune age, B is gone.
        f.run(20, cfg.methodPruneStaleAfterMillis + 5_000, METHOD_A);
        f.drain();
        assertNull(
                AdversarialSimulationTest.table(f, 7001).peekStats(METHOD_B.getFullMethodName()),
                "idle method pruned");
    }

    /**
     * #101: while one of three backends is ejected for errors, no second backend may be ejected for
     * latency on a method it serves: that would leave a single server for the method.
     */
    @Test
    void methodEjection_countsWholeBackendEjections() {
        AdversarialFixture f = fleet(PeakEwmaConfig.DEFAULTS, 3, 1.0);
        AtomicLong calls = new AtomicLong();
        // Half its calls fail, at normal latency (so it is warm and in the latency median).
        f.models.put(
                7000,
                (t, m) ->
                        new Response(
                                MS,
                                calls.incrementAndGet() % 2 == 0 ? Status.UNAVAILABLE : Status.OK));
        f.run(2_000, 3_000, METHOD_A);
        assertTrue(state(f, 7000).isEjected(f.now.get()), "error ejection happened");

        // Now 7001 turns 10x slower than the fleet.
        f.models.put(7001, AdversarialFixture.jittered(10.0, 0.1, 1));
        MethodTable t1 = AdversarialSimulationTest.table(f, 7001);
        String m = METHOD_A.getFullMethodName();
        for (int s = 0; s < 30; s++) {
            f.run(2_000, 1_000, METHOD_A); // one tick per second
            boolean e0 = state(f, 7000).isEjected(f.now.get());
            boolean m1 = t1.isMethodEjected(m, f.now.get());
            // 7000 keeps failing and returning, so this covers both orders: error ejection
            // first, and error ejection while 7001 is already out for latency.
            assertFalse(e0 && m1, "two of three peers out for " + m + " (second " + s + ")");
        }

        // Control: once 7000 is healthy again, the cap allows ejecting 7001 for latency.
        f.models.put(7000, AdversarialFixture.jittered(1.0, 0.1, 0));
        boolean latencyEjected = false;
        for (int s = 0; s < 30 && !latencyEjected; s++) {
            f.run(2_000, 1_000, METHOD_A);
            latencyEjected = t1.isMethodEjected(m, f.now.get());
        }
        assertTrue(latencyEjected, "latency ejection still happens when the cap allows");
    }

    /**
     * #101: with two peers and one ejected, every pick must go to the survivor on the fast path,
     * not burn 8 resamples on the same ejected peer and fall into the allocating scan.
     */
    @Test
    void twoPeers_oneEjected_picksSurvivorWithoutFallback() {
        Map<String, LongAdder> outcomes = new ConcurrentHashMap<>();
        AdversarialFixture f =
                new AdversarialFixture(
                        PeakEwmaConfig.DEFAULTS,
                        new AdversarialFixture.RecordingMetrics() {
                            @Override
                            public void recordPick(String outcome) {
                                outcomes.computeIfAbsent(outcome, k -> new LongAdder()).increment();
                            }
                        });
        populate(f, 2, 1.0);
        state(f, 7000).ejectUntil(f.now.get() + 60_000 * MS);
        outcomes.clear();

        for (int i = 0; i < 1_000; i++) {
            assertSame(f.byPort.get(7001), f.pick(METHOD_A).getSubchannel());
        }
        assertEquals(1_000, outcomes.get("ok").sum(), "all picks on the fast path: " + outcomes);

        // ... and cost no more than an ordinary pick: the old path resampled the same ejected
        // peer 8 times and then scanned, allocating two n-sized arrays per pick.
        long healthy = bytesPerPick(f, 7001, 7000, false);
        long oneEjected = bytesPerPick(f, 7001, 7000, true);
        assertTrue(
                oneEjected <= healthy + 16,
                "allocation per pick: " + oneEjected + " B with one ejected vs " + healthy + " B");
    }

    private static long bytesPerPick(AdversarialFixture f, int live, int other, boolean eject) {
        state(f, other).ejectUntil(eject ? f.now.get() + 60_000 * MS : f.now.get());
        var mx =
                (com.sun.management.ThreadMXBean)
                        java.lang.management.ManagementFactory.getThreadMXBean();
        long tid = Thread.currentThread().getId();
        for (int i = 0; i < 20_000; i++) f.pick(METHOD_A); // warm up the JIT
        long before = mx.getThreadAllocatedBytes(tid);
        int picks = 20_000;
        for (int i = 0; i < picks; i++) f.pick(METHOD_A);
        return (mx.getThreadAllocatedBytes(tid) - before) / picks;
    }

    /** #102: subchannel IDs use every address, like endpoint identity does. */
    @Test
    void subchannelIds_distinguishEndpointsSharingAFirstAddress() {
        AdversarialFixture f = new AdversarialFixture(PeakEwmaConfig.DEFAULTS);
        var a = new InetSocketAddress("127.0.0.1", 7000);
        var eag1 = new EquivalentAddressGroup(List.of(a, new InetSocketAddress("127.0.0.1", 7001)));
        var eag2 = new EquivalentAddressGroup(List.of(a, new InetSocketAddress("127.0.0.1", 7002)));
        f.helper.syncCtx.execute(
                () ->
                        f.balancer.handleResolvedAddresses(
                                ResolvedAddresses.newBuilder()
                                        .setAddresses(List.of(eag1, eag2))
                                        .build()));
        var ids = (Map<?, ?>) AdversarialFixture.getField(f.balancer, "subchannelIds");
        assertEquals(2, ids.size());
        assertEquals(2, ids.values().stream().distinct().count(), "distinct IDs: " + ids);
    }
}
