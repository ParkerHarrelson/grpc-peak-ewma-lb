package dev.parkerharrelson.grpc.peakewma;

import static org.junit.jupiter.api.Assertions.*;

import dev.parkerharrelson.grpc.peakewma.outlier.ErrorWindow;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MethodTableTest {

    static final class TimeHarness {
        final AtomicLong nowNanos = new AtomicLong(TimeUnit.MILLISECONDS.toNanos(1_000));
        final EwmaClocks clocks = new EwmaClocks(nowNanos::get);

        long now() {
            return nowNanos.get();
        }

        void advanceMillis(long ms) {
            nowNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(ms));
        }
    }

    private TimeHarness t;
    private PeakEwmaConfig cfg;

    @BeforeEach
    void setUp() {
        t = new TimeHarness();
        cfg =
                PeakEwmaConfig.builder()
                        .initialRttMicros(3_000)
                        .outlierWindowMillis(15_000)
                        .tauFastMillis(1_000)
                        .tauSlowMillis(30_000)
                        .inflightWeight(0.1)
                        .staleMillisForRatio(30_000)
                        .outlierEnabled(true)
                        .outlierErrorRate(0.2)
                        .outlierLatencyMultiplier(2.5)
                        .outlierEjectMillis(10_000)
                        .methodMaxEntries(512)
                        .methodPruneStaleAfterMillis(120_000)
                        .build();
    }

    private static void sample(
            TimeHarness t, MethodTable table, String method, PeakEwmaConfig cfg, long rttMicros) {
        MethodStats ms = table.statsFor(method);
        long start = t.now();
        long rttNanos = TimeUnit.MICROSECONDS.toNanos(rttMicros);
        ms.update(start + rttNanos, rttNanos, cfg);
    }

    @Test
    void statsFor_isMemoized_andSeededFromInitialRtt() {
        MethodTable table = new MethodTable(cfg, t.clocks);

        MethodStats a1 = table.statsFor("svc/A");
        MethodStats a2 = table.statsFor("svc/A");
        assertSame(a1, a2, "statsFor should memoize per method");

        assertEquals(3_000.0, a1.getEwmaSlowMicros(), 1e-9);
        assertEquals(3_000.0, a1.getEwmaFastMicros(), 1e-9);
        assertEquals(t.now(), a1.getLastUpdateNanos());
    }

    @Test
    void windowFor_isMemoized_andHasConfiguredWindowMillis() {
        MethodTable table = new MethodTable(cfg, t.clocks);

        ErrorWindow w1 = table.windowFor("svc/B");
        ErrorWindow w2 = table.windowFor("svc/B");
        assertSame(w1, w2, "windowFor should memoize per method");
        assertEquals(
                cfg.outlierWindowMillis,
                w1.currentWindowMillis(),
                "window millis should come from cfg");
    }

    @Test
    void methodKeys_containsOnlyMethodsWithStats() {
        MethodTable table = new MethodTable(cfg, t.clocks);
        assertTrue(table.methodKeys().isEmpty());

        table.statsFor("svc/A");
        table.statsFor("svc/B");
        Set<String> keys = table.methodKeys();
        assertEquals(2, keys.size());
        assertTrue(keys.contains("svc/A"));
        assertTrue(keys.contains("svc/B"));
    }

    @Test
    void ejectionUntil_and_isMethodEjected_transitions() {
        MethodTable table = new MethodTable(cfg, t.clocks);
        String m = "svc/Eject";

        table.statsFor(m);
        table.windowFor(m);

        long until = t.now() + TimeUnit.SECONDS.toNanos(5);
        table.ejectMethodUntil(m, until);
        assertTrue(table.isMethodEjected(m, t.now()), "should be ejected before deadline");

        t.advanceMillis(5_000);
        assertFalse(
                table.isMethodEjected(m, t.now()), "should no longer be ejected after deadline");
    }

    @Test
    void inflight_increments_and_floorsAtZero_onDecrement() {
        MethodTable table = new MethodTable(cfg, t.clocks);

        assertEquals(0, table.getInflight());
        table.decrementInflight();
        assertEquals(0, table.getInflight());

        table.incrementInflight();
        table.incrementInflight();
        assertEquals(2, table.getInflight());

        table.decrementInflight();
        assertEquals(1, table.getInflight());

        table.decrementInflight();
        assertEquals(0, table.getInflight());

        table.decrementInflight();
        assertEquals(0, table.getInflight());
    }

    @Test
    void pruneStale_timeBased_removesEntries_andAlsoRemovesWindowsAndEjections() {
        MethodTable table = new MethodTable(cfg, t.clocks);

        String oldM = "svc/Old";
        String keepM = "svc/Keep";

        sample(t, table, oldM, cfg, 10_000);
        sample(t, table, keepM, cfg, 12_000);
        table.windowFor(oldM);
        table.windowFor(keepM);

        long until = t.now() + TimeUnit.SECONDS.toNanos(30);
        table.ejectMethodUntil(oldM, until);

        long pruneAfterMillis = 1_000;
        t.advanceMillis(pruneAfterMillis + 10);
        sample(t, table, keepM, cfg, 12_000);

        table.pruneStale(t.now(), pruneAfterMillis, 1_000);

        Set<String> keys = table.methodKeys();
        assertFalse(keys.contains(oldM), "oldM should be pruned by time");
        assertTrue(keys.contains(keepM), "keepM should remain");

        ErrorWindow winOld = table.windowFor(oldM);
        assertEquals(cfg.outlierWindowMillis, winOld.currentWindowMillis());

        assertFalse(table.isMethodEjected(oldM, t.now()));
    }

    @Test
    void pruneStale_sizeCap_removesOldestEntries_andCleansWindowsAndEjections() {
        MethodTable table = new MethodTable(cfg, t.clocks);

        String[] methods = {"svc/M0", "svc/M1", "svc/M2", "svc/M3", "svc/M4"};
        for (int i = 0; i < methods.length; i++) {
            sample(t, table, methods[i], cfg, 10_000 + i * 1_000);
            table.windowFor(methods[i]);
            table.ejectMethodUntil(methods[i], t.now() + TimeUnit.SECONDS.toNanos(60));
            t.advanceMillis(10);
        }

        table.pruneStale(t.now(), 60_000, 3);

        Set<String> keys = table.methodKeys();
        assertEquals(3, keys.size(), "should keep 3 newest entries");
        assertFalse(keys.contains("svc/M0"));
        assertFalse(keys.contains("svc/M1"));
        assertTrue(keys.contains("svc/M2"));
        assertTrue(keys.contains("svc/M3"));
        assertTrue(keys.contains("svc/M4"));

        ErrorWindow newWinM0 = table.windowFor("svc/M0");
        assertEquals(cfg.outlierWindowMillis, newWinM0.currentWindowMillis());
        assertFalse(table.isMethodEjected("svc/M0", t.now()));
    }
}
