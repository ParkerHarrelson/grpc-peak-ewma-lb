package dev.parkerharrelson.grpc.peakewma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.parkerharrelson.grpc.peakewma.outlier.ErrorWindow;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class PeakEwmaTunerTest {

    private static PeakEwmaConfig cfg() {
        return PeakEwmaConfig.builder()
                .tauFastMillis(1000)
                .tauSlowMillis(30000)
                .inflightWeight(0.15)
                .initialRttMicros(50_000)
                .outlierLatencyMultiplier(2.5)
                .build();
    }

    @Test
    void seededInitialMicros_usesCfgSeedWhenEmpty_andUsesMedianOtherwise() {
        PeakEwmaConfig c = cfg();
        EwmaClocks clocks = new EwmaClocks(() -> 1_000_000_000L);
        MethodTable table = new MethodTable(c, clocks);

        long seedEmpty = PeakEwmaTuner.seededInitialMicros(table, c);
        assertEquals(50_000, seedEmpty);

        MethodStats a = table.statsFor("svc/A");
        MethodStats b = table.statsFor("svc/B");
        MethodStats d = table.statsFor("svc/D");

        long tBase = 1_000_000_000L;
        long sixtySec = TimeUnit.SECONDS.toNanos(60);

        a.update(tBase + sixtySec + 1_000, 30_000L * 1_000L, c);
        b.update(tBase + sixtySec + 2_000, 10_000L * 1_000L, c);
        d.update(tBase + sixtySec + 3_000, 70_000L * 1_000L, c);

        double[] slows = {a.getEwmaSlowMicros(), b.getEwmaSlowMicros(), d.getEwmaSlowMicros()};
        java.util.Arrays.sort(slows);
        long expected = Math.max(2_000L, Math.round(slows[1]));

        long seed = PeakEwmaTuner.seededInitialMicros(table, c);
        assertEquals(expected, seed);
    }

    @Test
    void methodRatePerSec_usesSnapshotTotals_andWindowSecondsWithClamp() {
        ErrorWindow win = new ErrorWindow(10, 1000);
        long base = 2_000_000_000L;

        for (int i = 0; i < 6; i++) win.recordResult(true, base + i);
        double rate = PeakEwmaTuner.methodRatePerSec(win, base + 999_999_999L);
        assertTrue(rate >= 5.9 && rate <= 6.1, "rate ~ 6/s but was " + rate);

        win.setWindowMillis(0);
        double rateTiny = PeakEwmaTuner.methodRatePerSec(win, base + 2_000_000_000L);
        assertTrue(rateTiny >= 0.0);
    }

    @Test
    void coeffVarFromEwma_handlesLowMeanClamp_andReasonableVariance() {
        PeakEwmaConfig c = cfg();
        MethodStats ms = new MethodStats(c.initialRttMicros, 0L);

        long t = 5_000_000_000L;
        ms.update(t + 1_000, 20_000L * 1_000L, c);
        ms.update(t + 2_000, 22_000L * 1_000L, c);
        ms.update(t + 3_000, 18_000L * 1_000L, c);

        double cv = PeakEwmaTuner.coeffVarFromEwma(ms);
        assertTrue(cv > 0.0 && cv < 0.5, "expected smallish CV, got " + cv);
    }

    @Test
    void tauFastMillis_and_tauSlowMillis_scaleWithCv_andClamp() {
        PeakEwmaConfig c = cfg();
        MethodStats stable = new MethodStats(c.initialRttMicros, 0L);
        MethodStats noisy = new MethodStats(c.initialRttMicros, 0L);
        long t = 10_000_000_000L;

        for (int i = 0; i < 5; i++) stable.update(t + i, 20_000L * 1_000L, c);

        for (int i = 0; i < 10; i++) {
            long rttMicros = (i % 2 == 0) ? 5_000 : 80_000;
            noisy.update(t + i, rttMicros * 1_000L, c);
        }

        long tfStable = PeakEwmaTuner.tauFastMillis(stable, c);
        long tfNoisy = PeakEwmaTuner.tauFastMillis(noisy, c);
        long tsStable = PeakEwmaTuner.tauSlowMillis(stable, c);
        long tsNoisy = PeakEwmaTuner.tauSlowMillis(noisy, c);

        assertTrue(tfNoisy <= c.tauFastMillis && tfNoisy >= 300, "tfNoisy clamped range");
        assertTrue(tfStable >= 300 && tfStable <= 2000, "tfStable clamp");

        assertTrue(tsNoisy <= c.tauSlowMillis && tsNoisy >= 15_000, "tsNoisy clamp");
        assertTrue(tsStable >= 15_000 && tsStable <= 60_000, "tsStable clamp");
    }

    @Test
    void minSamplesForRatioEff_and_minWarmupMillisForRatioEff_hitBounds() {
        assertEquals(2, PeakEwmaTuner.minSamplesForRatioEff(0.0));
        assertTrue(PeakEwmaTuner.minWarmupMillisForRatioEff(0.0) >= 250);
        assertTrue(PeakEwmaTuner.minWarmupMillisForRatioEff(0.0) <= 2000);

        assertEquals(16, PeakEwmaTuner.minSamplesForRatioEff(10_000.0));
        long warmHigh = PeakEwmaTuner.minWarmupMillisForRatioEff(10_000.0);
        assertTrue(warmHigh >= 250 && warmHigh <= 2000);
    }

    @Test
    void inflightWeightEff_scalesWithReadyAndMedian_andClamps() {
        PeakEwmaConfig c = cfg();
        double hi = PeakEwmaTuner.inflightWeightEff(200, 1, c);
        assertEquals(0.30, hi, 1e-9);

        double lo = PeakEwmaTuner.inflightWeightEff(1, 10_000, c);
        assertEquals(0.05, lo, 1e-9);

        double mid = PeakEwmaTuner.inflightWeightEff(10, 5, c);
        assertTrue(mid >= 0.05 && mid <= 0.30);
    }

    @Test
    void warmupMillisEff_dependsOnSeededInitialMicros_andClamps() {
        PeakEwmaConfig c = cfg();
        EwmaClocks clocks = new EwmaClocks(() -> 42L);
        MethodTable table = new MethodTable(c, clocks);

        long warm1 = PeakEwmaTuner.warmupMillisEff(table);
        assertTrue(warm1 >= 300 && warm1 <= 3000);

        table.statsFor("svc/A").update(1000L, 3_000L * 1_000L, c);
        long warm2 = PeakEwmaTuner.warmupMillisEff(table);
        assertTrue(warm2 >= 300 && warm2 <= 3000);
    }

    @Test
    void windowMillisEff_targetsSampleCount_andClamps() {
        long big = PeakEwmaTuner.windowMillisEff(0.01, 0.01);
        assertEquals(45_000L, big);

        long small = PeakEwmaTuner.windowMillisEff(5000, 5000);
        assertEquals(8_000L, small);

        long mid = PeakEwmaTuner.windowMillisEff(5.0, 10.0);
        assertTrue(mid > 8_000 && mid < 45_000, "mid=" + mid);
    }

    @Test
    void ejectionGuards_andLatencyMultiplierEff_coverBounds() {
        PeakEwmaConfig c = cfg();

        assertTrue(PeakEwmaTuner.maxEjectionPercentEff(1) >= 10);
        assertTrue(PeakEwmaTuner.maxEjectionPercentEff(10) <= 50);

        double minFrac = PeakEwmaTuner.minReadyFractionAfterEjectEff(1);
        double maxFrac = PeakEwmaTuner.minReadyFractionAfterEjectEff(1000);
        assertTrue(minFrac >= 0.4 && minFrac <= 0.7);
        assertTrue(maxFrac >= 0.4 && maxFrac <= 0.7);

        int minTotalSmall = PeakEwmaTuner.minTotalForErrorEjectEff(1);
        int minTotalLarge = PeakEwmaTuner.minTotalForErrorEjectEff(10_000);
        assertTrue(minTotalSmall >= 20 && minTotalSmall <= 100);
        assertTrue(minTotalLarge >= 20 && minTotalLarge <= 100);

        double low = PeakEwmaTuner.latencyMultiplierEff(0.10, c);
        double mid = PeakEwmaTuner.latencyMultiplierEff(0.30, c);
        double high = PeakEwmaTuner.latencyMultiplierEff(0.70, c);

        assertTrue(low <= c.outlierLatencyMultiplier && low >= 2.0);
        assertEquals(c.outlierLatencyMultiplier, mid, 1e-12);
        assertTrue(high >= c.outlierLatencyMultiplier && high <= 3.5);
    }
}
