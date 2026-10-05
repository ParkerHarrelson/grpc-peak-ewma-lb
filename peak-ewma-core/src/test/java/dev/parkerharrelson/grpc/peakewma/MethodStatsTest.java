package dev.parkerharrelson.grpc.peakewma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class MethodStatsTest {

    @Test
    void constructor_seedsEwmas_andSetsLastUpdate() {
        long seedUs = 50_000L;
        long initNanos = 123_456_789L;
        MethodStats ms = new MethodStats(seedUs, initNanos);

        assertEquals(seedUs, ms.getEwmaFastMicros(), 1e-12);
        assertEquals(seedUs, ms.getEwmaSlowMicros(), 1e-12);
        assertEquals(initNanos, ms.getLastUpdateNanos());
        assertEquals(0, ms.getSamples());
        assertEquals(0L, ms.getFirstSampleNanos());
    }

    @Test
    void firstUpdate_withLastUpdateZero_behavesAsHardSetToRtt() {
        long seedUs = 10_000L;
        MethodStats ms = new MethodStats(seedUs, 0L);
        PeakEwmaConfig cfg = PeakEwmaConfig.DEFAULTS;

        long start = 1_000_000_000L;
        long rttNanos = 2_000_000L;
        long end = start + rttNanos;

        ms.update(end, rttNanos, cfg);

        assertEquals(2_000.0, ms.getEwmaFastMicros(), 1e-6);
        assertEquals(2_000.0, ms.getEwmaSlowMicros(), 1e-6);
        assertEquals(end, ms.getLastUpdateNanos());
        assertEquals(1, ms.getSamples());
        assertEquals(end, ms.getFirstSampleNanos());
    }

    @Test
    void subsequentUpdates_applyPeakAndDecay() {
        MethodStats ms = new MethodStats(5_000L, 1_000_000L);
        PeakEwmaConfig cfg = PeakEwmaConfig.DEFAULTS;

        long t1 = 10_000_000L;
        long rtt1 = 4_000_000L;
        // The first real sample replaces the 5 ms seed outright (it is not blended with it).
        double expectedFast1 = 4000.0;
        double expectedSlow1 = 4000.0;

        ms.update(t1, rtt1, cfg);
        assertEquals(expectedFast1, ms.getEwmaFastMicros(), 1e-6);
        assertEquals(expectedSlow1, ms.getEwmaSlowMicros(), 1e-6);
        assertEquals(1, ms.getSamples());
        assertEquals(t1, ms.getLastUpdateNanos());
        assertEquals(t1, ms.getFirstSampleNanos());

        long t2 = t1 + 1_000_000_000L;
        long rtt2 = 2_000_000L;
        double df2 = EwmaClocks.decayFactor(t2, t1, cfg.tauFastMillis);
        double ds2 = EwmaClocks.decayFactor(t2, t1, cfg.tauSlowMillis);
        double expectedFast2 = Math.max(2000.0, expectedFast1 * df2);
        // The slow EWMA is bias-corrected: with two samples a sample's weight is at least 1/2,
        // so it is the plain average here (an uncorrected EWMA would still sit near the first).
        double expectedSlow2 = 2000.0 * Math.max(1 - ds2, 0.5) + expectedSlow1 * Math.min(ds2, 0.5);

        ms.update(t2, rtt2, cfg);
        assertEquals(expectedFast2, ms.getEwmaFastMicros(), 1e-6);
        assertEquals(expectedSlow2, ms.getEwmaSlowMicros(), 1e-6);

        assertTrue(ms.getEwmaFastMicros() >= 2000.0 - 1e-3);
        assertTrue(ms.getEwmaSlowMicros() < expectedSlow1 + 1e-6);
        assertEquals(2, ms.getSamples());
        assertEquals(t2, ms.getLastUpdateNanos());
        assertEquals(t1, ms.getFirstSampleNanos());
    }

    @Test
    void largeRtt_spike_raisesFastImmediately_andRaisesSlowGradually() {
        MethodStats ms = new MethodStats(1_000L, 0L);
        PeakEwmaConfig cfg = PeakEwmaConfig.DEFAULTS;

        long t1 = 1_000_000L;
        long rtt1 = 1_000_000L;
        ms.update(t1, rtt1, cfg);

        long t2 = t1 + 10_000_000L;
        long rtt2 = 50_000_000L;
        ms.update(t2, rtt2, cfg);

        double fastAfter = ms.getEwmaFastMicros();
        double slowAfter = ms.getEwmaSlowMicros();

        assertEquals(50_000.0, fastAfter, 1e-6, "peak ewma should jump to spike immediately");
        assertTrue(slowAfter > 1_000.0, "slow should increase");
        assertTrue(slowAfter < 50_000.0, "slow should not jump fully to spike");
    }

    @Test
    void nanosToMicros_conversion_isConsistentForIntuitiveChecks() {
        assertEquals(1000.0, EwmaClocks.nanosToMicros(1_000_000L), 1e-12);
        assertEquals(0.5, EwmaClocks.nanosToMicros(500L), 1e-12);
    }

    /**
     * Regression guard for the previously unserialised EWMA and variance update path. Eight threads
     * each apply ten thousand RTT samples; every sample must be reflected in {@code getSamples()}.
     * If a future change drops the lock back out for performance, this assertion starts losing
     * writes.
     */
    @Test
    void redundantSamples_areSkipped_butPeakRaisingSamplesAreAlwaysApplied() {
        PeakEwmaConfig cfg = PeakEwmaConfig.DEFAULTS;
        MethodStats ms = new MethodStats(10_000L, 0L);
        long t = 1_000_000_000L;
        ms.update(t, 5_000_000L, cfg); // first sample: 5 ms
        int samples = ms.getSamples();

        // 100 us later, a faster call: doesn't raise the peak and the smoothed stats were just
        // written -> skipped (exact for the peak: readers decay the stored value to now).
        ms.update(t + 100_000L, 4_000_000L, cfg);
        assertEquals(samples, ms.getSamples());
        assertEquals(t, ms.getLastUpdateNanos());

        // 100 us later again, a SLOWER call: raises the peak -> applied to the peak only. The
        // smoothed stats are not due yet, and must not see it: letting through only the samples
        // that raise the peak would bias them upward (#96).
        double slow = ms.getEwmaSlowMicros();
        double mean = ms.getRttMeanMicros();
        ms.update(t + 200_000L, 9_000_000L, cfg);
        assertEquals(9_000.0, ms.getEwmaFastMicros(), 1e-6);
        assertEquals(samples, ms.getSamples());
        assertEquals(slow, ms.getEwmaSlowMicros(), 1e-9);
        assertEquals(mean, ms.getRttMeanMicros(), 1e-9);
        assertEquals(t + 200_000L, ms.getLastUpdateNanos());

        // >= 1 ms after the last SMOOTHED write, any call (here a fast one) feeds them.
        ms.update(t + MethodStats.minSmoothingIntervalNanos, 4_000_000L, cfg);
        assertEquals(samples + 1, ms.getSamples());
    }

    /**
     * #96: write thinning must not bias the smoothed statistics. Exponential RTTs (mean 1 ms, CV 1)
     * arriving every 20 us (50k/s per backend and method): the old thinning, which let through only
     * samples that raised the peak, drove the mean to ~3x the truth.
     */
    @Test
    void thinning_keepsSmoothedStatsUnbiased_atHighRates() {
        MethodStats thinned = runExponential(MethodStats.minSmoothingIntervalNanos);
        MethodStats exact = runExponential(0L);
        assertEquals(1_000.0, thinned.getRttMeanMicros(), 100.0, "mean vs truth");
        assertEquals(exact.getRttMeanMicros(), thinned.getRttMeanMicros(), 100.0, "mean");
        assertEquals(exact.getEwmaSlowMicros(), thinned.getEwmaSlowMicros(), 150.0, "slow EWMA");
        double cv = Math.sqrt(thinned.getRttVarMicros()) / thinned.getRttMeanMicros();
        assertEquals(1.0, cv, 0.2, "coefficient of variation");
    }

    private static MethodStats runExponential(long thinningNanos) {
        long saved = MethodStats.minSmoothingIntervalNanos;
        MethodStats.minSmoothingIntervalNanos = thinningNanos;
        try {
            java.util.Random rnd = new java.util.Random(42);
            MethodStats ms = new MethodStats(1_000L, 0L);
            long t = 1_000_000_000L;
            for (int i = 0; i < 600_000; i++) { // 12 s of traffic
                t += 20_000L;
                long rtt = (long) (-Math.log(1 - rnd.nextDouble()) * 1_000_000L);
                ms.update(t, Math.max(1L, rtt), PeakEwmaConfig.DEFAULTS);
            }
            return ms;
        } finally {
            MethodStats.minSmoothingIntervalNanos = saved;
        }
    }

    /** #100: a completion that ended earlier but wins its CAS later must not rewind time. */
    @Test
    void outOfOrderCompletion_doesNotMoveTimestampsBackwards() {
        PeakEwmaConfig cfg = PeakEwmaConfig.DEFAULTS;
        MethodStats ms = new MethodStats(10_000L, 0L);
        long t = 1_000_000_000L;
        ms.update(t, 5_000_000L, cfg);
        ms.update(t + 100_000_000L, 50_000_000L, cfg); // spike at t+100 ms
        ms.update(t + 90_000_000L, 1_000_000L, cfg); // ended at t+90 ms, applied later
        ms.update(t + 95_000_000L, 1_000_000L, cfg, true); // a failure, also out of order
        assertEquals(t + 100_000_000L, ms.getLastUpdateNanos());

        // The peak is decayed from t+100 ms, not from the earlier end times.
        long tau = PeakEwmaTuner.tauFastMillis(ms, cfg);
        double expected =
                ms.getEwmaFastMicros()
                        * EwmaClocks.decayFactor(t + 110_000_000L, t + 100_000_000L, tau);
        assertEquals(expected, ms.decayedPeakMicros(t + 110_000_000L), 1e-9);
    }

    @Test
    void update_underHighConcurrency_doesNotLoseSamples() throws Exception {
        final int threads = 8;
        final int samplesPerThread = 10_000;
        final int totalSamples = threads * samplesPerThread;

        MethodStats ms = new MethodStats(10_000L, 0L);
        PeakEwmaConfig cfg = PeakEwmaConfig.DEFAULTS;
        // This test checks the CAS loop never loses a write; disable write thinning.
        long thinning = MethodStats.minSmoothingIntervalNanos;
        MethodStats.minSmoothingIntervalNanos = 0L;

        ExecutorService exec = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicLong tickerNanos = new AtomicLong(1L);

        try {
            for (int t = 0; t < threads; t++) {
                exec.submit(
                        () -> {
                            ready.countDown();
                            try {
                                start.await();
                                for (int i = 0; i < samplesPerThread; i++) {
                                    long now = tickerNanos.incrementAndGet();
                                    long rttNanos = 1_000_000L + (i % 1024);
                                    ms.update(now, rttNanos, cfg);
                                }
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            } finally {
                                done.countDown();
                            }
                        });
            }

            ready.await();
            start.countDown();
            assertTrue(
                    done.await(30, TimeUnit.SECONDS),
                    "concurrent updates did not complete within timeout");
        } finally {
            MethodStats.minSmoothingIntervalNanos = thinning;
            exec.shutdownNow();
        }

        assertEquals(
                totalSamples,
                ms.getSamples(),
                "concurrent update() must serialise so every sample is counted");
        assertTrue(ms.getEwmaFastMicros() > 0.0, "fast EWMA should have advanced");
        assertTrue(ms.getEwmaSlowMicros() > 0.0, "slow EWMA should have advanced");
        assertTrue(
                ms.getRttVarMicros() >= 0.0,
                "Welford variance must remain non-negative under concurrent input");
    }
}
