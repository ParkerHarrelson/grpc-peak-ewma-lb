package dev.parkerharrelson.grpc.peakewma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class MethodTableSeedTest {

    private static PeakEwmaConfig cfg() {
        return PeakEwmaConfig.builder().initialRttMicros(50_000).build();
    }

    @Test
    void initialSeed_usesConfigInitialRtt() {
        EwmaClocks clocks = new EwmaClocks(() -> 1_000_000L);
        MethodTable table = new MethodTable(cfg(), clocks);

        assertEquals(50_000L, table.cachedSeedMicros());

        MethodStats first = table.statsFor("svc/A");
        assertEquals(50_000.0, first.getEwmaSlowMicros(), 1e-9);
        assertEquals(50_000.0, first.getEwmaFastMicros(), 1e-9);
    }

    @Test
    void pruneStale_refreshesCachedSeedToMedianOfLiveMethods() {
        PeakEwmaConfig c = cfg();
        AtomicLong now = new AtomicLong(1_000_000L);
        EwmaClocks clocks = new EwmaClocks(now::get);
        MethodTable table = new MethodTable(c, clocks);

        MethodStats a = table.statsFor("svc/A");
        MethodStats b = table.statsFor("svc/B");
        MethodStats d = table.statsFor("svc/D");

        long tBase = now.get();
        long sixtySec = TimeUnit.SECONDS.toNanos(60);
        a.update(tBase + sixtySec + 1_000, 30_000L * 1_000L, c);
        b.update(tBase + sixtySec + 2_000, 10_000L * 1_000L, c);
        d.update(tBase + sixtySec + 3_000, 70_000L * 1_000L, c);

        // Still using the initial seed until pruneStale runs.
        assertEquals(50_000L, table.cachedSeedMicros());

        // Advance time past first-sample timestamps but keep all methods "fresh" for the prune
        // threshold, so pruneStale will refresh the seed without removing anything.
        now.set(tBase + sixtySec + 10_000_000L);
        table.pruneStale(now.get(), TimeUnit.MINUTES.toMillis(5), 512);

        long refreshed = table.cachedSeedMicros();
        long slowMedian =
                (long)
                        Math.rint(
                                median(
                                        a.getEwmaSlowMicros(),
                                        b.getEwmaSlowMicros(),
                                        d.getEwmaSlowMicros()));
        assertEquals(Math.max(2_000L, slowMedian), refreshed);
    }

    @Test
    void statsForNewMethod_inheritsMedianSeed_afterPrune() {
        PeakEwmaConfig c = cfg();
        AtomicLong now = new AtomicLong(1_000_000L);
        EwmaClocks clocks = new EwmaClocks(now::get);
        MethodTable table = new MethodTable(c, clocks);

        long tBase = now.get();
        long sixtySec = TimeUnit.SECONDS.toNanos(60);

        table.statsFor("svc/A").update(tBase + sixtySec + 1_000, 5_000L * 1_000L, c);
        table.statsFor("svc/B").update(tBase + sixtySec + 2_000, 15_000L * 1_000L, c);
        table.statsFor("svc/C").update(tBase + sixtySec + 3_000, 25_000L * 1_000L, c);

        now.set(tBase + sixtySec + 10_000_000L);
        table.pruneStale(now.get(), TimeUnit.MINUTES.toMillis(5), 512);

        MethodStats fresh = table.statsFor("svc/NEW");
        double seed = fresh.getEwmaSlowMicros();
        assertTrue(
                seed > 5_000.0 && seed < 25_000.0,
                "new method should seed near median of existing methods, got " + seed);
    }

    private static double median(double... vals) {
        double[] c = vals.clone();
        java.util.Arrays.sort(c);
        int n = c.length;
        return (n & 1) == 1 ? c[n / 2] : 0.5 * (c[n / 2 - 1] + c[n / 2]);
    }
}
