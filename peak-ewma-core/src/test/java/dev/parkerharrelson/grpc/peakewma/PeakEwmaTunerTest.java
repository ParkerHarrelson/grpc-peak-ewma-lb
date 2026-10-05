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
        // Spaced past the 1 ms smoothing interval so each sample reaches the variance.
        ms.update(t + 2_000_000, 20_000L * 1_000L, c);
        ms.update(t + 4_000_000, 22_000L * 1_000L, c);
        ms.update(t + 6_000_000, 18_000L * 1_000L, c);

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
    void warmGate_isSampleBased_andRateIndependent() {
        assertEquals(10, PeakEwmaTuner.minSamplesForRatioEff(0.0));
        assertEquals(10, PeakEwmaTuner.minSamplesForRatioEff(10_000.0));
        assertEquals(0L, PeakEwmaTuner.minWarmupMillisForRatioEff(5.0));
    }

    @Test
    void inflightWeightEff_isTheStandardInflightPlusOneTerm() {
        PeakEwmaConfig c = cfg();
        assertEquals(1.0, PeakEwmaTuner.inflightWeightEff(200, 1, c), 1e-12);
        assertEquals(1.0, PeakEwmaTuner.inflightWeightEff(1, 10_000, c), 1e-12);
    }

    @Test
    void windowMillisEff_targetsSampleCount_andClamps() {
        // ~200 calls: 5/s -> 40 s.
        assertEquals(40_000L, PeakEwmaTuner.windowMillisEff(5.0, 5.0, 1_000));
        // Never shorter than two ticks, however hot the method.
        assertEquals(2_000L, PeakEwmaTuner.windowMillisEff(5_000, 5_000, 1_000));
        // At most 5 minutes, however cold.
        assertEquals(300_000L, PeakEwmaTuner.windowMillisEff(0.01, 0.01, 1_000));
    }

    @Test
    void ejectionGuards_andLatencyMultiplierEff_coverBounds() {
        PeakEwmaConfig c = cfg();
        // At most 20% of the fleet, at least one peer, never the last one.
        assertEquals(1, PeakEwmaTuner.maxEjectedCountEff(2));
        assertEquals(1, PeakEwmaTuner.minReadyAfterEjectEff(2));
        assertEquals(2, PeakEwmaTuner.maxEjectedCountEff(10));
        assertEquals(8, PeakEwmaTuner.minReadyAfterEjectEff(10));
        assertEquals(0, PeakEwmaTuner.maxEjectedCountEff(1));
        // Configured multiplier, raised for noisy methods (1 + 2 x CV).
        assertEquals(c.outlierLatencyMultiplier, PeakEwmaTuner.latencyMultiplierEff(0.1, c), 1e-9);
        assertEquals(3.0, PeakEwmaTuner.latencyMultiplierEff(1.0, c), 1e-9);
        // Error ejection is statistical: conclusive with few calls, not with noisy many.
        assertTrue(PeakEwmaTuner.errorRateLowerBound(10, 10) > 0.7);
        assertTrue(PeakEwmaTuner.errorRateLowerBound(3, 20) < 0.1);
    }
}
