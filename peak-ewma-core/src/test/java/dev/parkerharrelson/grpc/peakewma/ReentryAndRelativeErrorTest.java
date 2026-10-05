package dev.parkerharrelson.grpc.peakewma;

import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.METHOD_A;
import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.parkerharrelson.grpc.peakewma.AdversarialFixture.Response;
import io.grpc.Status;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * #106 (a healed backend comes back promptly; a still-slow one is re-ejected on fresh evidence),
 * #108 (a backend failing a few percent of calls is ejected relative to the fleet) and #107 (no
 * per-method cost work for a sink that drops costs).
 */
class ReentryAndRelativeErrorTest {

    private static final int N = 10;
    private static final String M = METHOD_A.getFullMethodName();

    private static AdversarialFixture fleet(AdversarialFixture.RecordingMetrics metrics) {
        AdversarialFixture f = new AdversarialFixture(PeakEwmaConfig.DEFAULTS, metrics);
        int[] ports = new int[N];
        for (int i = 0; i < N; i++) {
            ports[i] = 7000 + i;
            f.models.put(ports[i], AdversarialFixture.jittered(2.0, 0.1, i));
        }
        f.resolveAndReady(ports);
        return f;
    }

    private static long count(AdversarialFixture.RecordingMetrics m, String needle) {
        return m.ejections.stream().filter(s -> s.contains(needle)).count();
    }

    /** Runs one simulated second at 2,000 rps and returns 7000's share of it. */
    private static double second(AdversarialFixture f) {
        f.resetCounters();
        f.run(2_000, 1_000, METHOD_A);
        return f.share(7000);
    }

    @Test
    void healedBackend_returnsToFairShare_andIsNotReEjected() {
        AdversarialFixture.RecordingMetrics m = new AdversarialFixture.RecordingMetrics();
        AdversarialFixture f = fleet(m);
        f.run(2_000, 5_000, METHOD_A); // warm

        // 7000 turns 10x slower for 20 s: ejected, returns, re-ejected while still slow.
        f.models.put(7000, AdversarialFixture.jittered(20.0, 0.1, 99));
        for (int s = 0; s < 20; s++) second(f);
        assertTrue(
                count(m, ":7000 reason=latency") >= 2, "ejected, and re-ejected: " + m.ejections);

        // Heal. Whatever ejection is already running (backed off: it was ejected repeatedly)
        // finishes; then it must be back within a few seconds on probation, not re-ejected on its
        // stale fault-era baseline. Before #106 it was still out 30 s after healing.
        f.models.put(7000, AdversarialFixture.jittered(2.0, 0.1, 0));
        MethodTable t = AdversarialSimulationTest.table(f, 7000);
        // The first tick after the heal may still act on probation samples taken while it was
        // slow (legitimately re-ejecting it); judge recovery from there.
        second(f);
        long remainingS =
                Math.max(0, (t.methodEjectedUntilNanos(M) - f.now.get()) / 1_000_000_000L);
        long ejectionsAtHeal = count(m, ":7000 reason=latency");
        int recoveredAt = -1;
        int streak = 0;
        for (int s = 1; s <= 60 && recoveredAt < 0; s++) {
            double share = second(f);
            streak = share >= 0.7 / N ? streak + 1 : 0;
            if (streak == 2) recoveredAt = s;
        }
        assertTrue(
                recoveredAt > 0 && recoveredAt <= remainingS + 4,
                "back to >= 70% of fair at "
                        + recoveredAt
                        + " s; ejection had "
                        + remainingS
                        + " s left");
        assertEquals(
                ejectionsAtHeal, count(m, ":7000 reason=latency"), "never re-ejected once healthy");
        long afterRecovery = count(m, ":7000 reason=latency");
        for (int s = 0; s < 10; s++) second(f);
        assertEquals(afterRecovery, count(m, ":7000 reason=latency"), "no flapping once healthy");
        assertTrue(second(f) >= 0.7 / N, "keeps its share");
    }

    @Test
    void stillSlowBackend_isReEjectedOnFreshEvidence_withBackoff() {
        AdversarialFixture.RecordingMetrics m = new AdversarialFixture.RecordingMetrics();
        AdversarialFixture f = fleet(m);
        f.run(2_000, 5_000, METHOD_A);

        f.models.put(7000, AdversarialFixture.jittered(20.0, 0.1, 99));
        f.resetCounters();
        MethodTable t = AdversarialSimulationTest.table(f, 7000);
        java.util.List<Long> durations = new java.util.ArrayList<>();
        long lastEnd = Long.MIN_VALUE;
        for (int s = 0; s < 40; s++) {
            long before = t.methodEjectedUntilNanos(M);
            f.run(2_000, 1_000, METHOD_A);
            long end = t.methodEjectedUntilNanos(M);
            if (end != before && end > lastEnd) {
                durations.add((end - f.now.get()) / 1_000_000_000L);
                lastEnd = end;
            }
        }
        // Returned on probation, re-ejected on fresh evidence each time, for longer each time.
        assertTrue(durations.size() >= 3, "re-ejected after each probation: " + durations);
        for (int i = 1; i < durations.size(); i++) {
            assertTrue(durations.get(i) > durations.get(i - 1), "backs off: " + durations);
        }
        assertTrue(f.share(7000) < 0.02, "still-slow backend mostly avoided: " + f.share(7000));
    }

    @Test
    void lowRateFailures_areEjectedRelativeToAHealthyFleet() {
        AdversarialFixture.RecordingMetrics m = new AdversarialFixture.RecordingMetrics();
        AdversarialFixture f = fleet(m);
        f.run(2_000, 5_000, METHOD_A);

        // 7000 fails 5% of calls: far below the 20% absolute threshold.
        Random r = new Random(7);
        f.models.put(
                7000,
                (t, md) ->
                        new Response(
                                2 * MS, r.nextDouble() < 0.05 ? Status.UNAVAILABLE : Status.OK));
        f.run(2_000, 10_000, METHOD_A);
        assertTrue(count(m, ":7000 reason=errors") >= 1, "relative error ejection: " + m.ejections);
    }

    @Test
    void uniformFailures_ejectNobody() {
        AdversarialFixture.RecordingMetrics m = new AdversarialFixture.RecordingMetrics();
        AdversarialFixture f = fleet(m);
        f.run(2_000, 5_000, METHOD_A);

        // A shared dependency: every backend fails 5% of calls. No backend is an outlier.
        for (int i = 0; i < N; i++) {
            Random r = new Random(i);
            f.models.put(
                    7000 + i,
                    (t, md) ->
                            new Response(
                                    2 * MS,
                                    r.nextDouble() < 0.05 ? Status.UNAVAILABLE : Status.OK));
        }
        f.run(2_000, 15_000, METHOD_A);
        assertEquals(0, count(m, "reason=errors"), "no error ejections: " + m.ejections);
    }

    static final class CostCounting extends AdversarialFixture.RecordingMetrics {
        final boolean wants;
        final AtomicLong costs = new AtomicLong();

        CostCounting(boolean wants) {
            this.wants = wants;
        }

        @Override
        public boolean recordsCosts() {
            return wants;
        }

        @Override
        public void setCost(String subchannelId, String method, double cost) {
            costs.incrementAndGet();
        }
    }

    @Test
    void costGauges_skippedForSinksThatDropThem() {
        CostCounting off = new CostCounting(false);
        fleet(off).run(1_000, 3_000, METHOD_A);
        assertEquals(0, off.costs.get());

        CostCounting on = new CostCounting(true);
        fleet(on).run(1_000, 3_000, METHOD_A);
        assertTrue(on.costs.get() >= N, "costs still emitted when wanted: " + on.costs.get());
    }
}
