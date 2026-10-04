package dev.parkerharrelson.grpc.peakewma;

import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.MS;
import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.T0;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import dev.parkerharrelson.grpc.peakewma.outlier.ErrorWindow;
import org.junit.jupiter.api.Test;

/** Numerical / formula bugs in the EWMA, tuner and error window. */
class AdversarialMathTest {

    private static final PeakEwmaConfig CFG = PeakEwmaConfig.DEFAULTS;

    /**
     * EwmaClocks.decayFactor documents "an uninitialized last-update returns 0 so the first sample
     * overrides the seed cleanly", but MethodStats initialises lastUpdateNanos to the creation
     * time, so the first sample is blended with the 50 ms seed using the 30 s slow half-life — i.e.
     * it's ignored.
     */
    @Test
    void firstSample_overridesSeed() {
        MethodStats ms = new MethodStats(50_000, T0); // default initialRttMicros = 50 ms
        ms.update(T0 + MS, 200 * MS, CFG); // a 200 ms call completes 1 ms later
        assertThat(ms.getEwmaSlowMicros())
                .as("slow EWMA after the first 200 ms sample (seed was 50 ms)")
                .isCloseTo(200_000, within(20_000.0));
    }

    /**
     * Consequence of the above: for a service whose real latency is far from initialRttMicros the
     * slow EWMA (outlier baseline, cold-peer score, new-method seed) takes tens of seconds to
     * converge even under steady traffic.
     */
    @Test
    void slowEwma_convergesWithinFiveSecondsOfSteadyTraffic() {
        MethodStats ms = new MethodStats(50_000, T0);
        long t = T0;
        for (int i = 0; i < 500; i++) { // 100 rps of 200 ms calls for 5 s
            t += 10 * MS;
            ms.update(t, 200 * MS, CFG);
        }
        double fast = ms.getEwmaFastMicros();
        double slow = ms.getEwmaSlowMicros();
        assertThat(slow)
                .as("slow EWMA after 500 identical 200 ms samples (fast=%.0fus)", fast)
                .isCloseTo(200_000, within(40_000.0));
        // fast/slow is what the outlier detector compares against latencyMultiplierEff (2.0 here,
        // because CV≈0). Identical healthy samples should never look like an outlier.
        assertThat(fast / slow).as("fast/slow ratio on perfectly steady traffic").isLessThan(2.0);
    }

    /**
     * The Peak-EWMA cost is not decayed at read time. A backend that served one slow response is
     * scored at that peak until it receives another sample — which P2C ensures it never does.
     */
    @Test
    void peakCost_decaysWithTime_evenWithoutNewSamples() {
        java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong(T0);
        EwmaClocks clocks = new EwmaClocks(now::get);
        MethodTable mt = new MethodTable(CFG, clocks);
        SubchannelState st = new SubchannelState();
        st.markReady(T0);
        MethodStats ms = mt.statsFor("m");
        long t = T0;
        for (int i = 0; i < 200; i++) {
            t += 10 * MS;
            ms.update(t, 5 * MS, CFG);
        }
        t += 10 * MS;
        ms.update(t, 1000 * MS, CFG); // one 1 s response
        now.set(t + 10_000 * MS); // 10 s later, no new samples (tauFast ≤ 2 s → >5 half-lives)
        double cost = P2CPicker.peakEwmaCost(mt, ms, st, "m", now.get(), 0.15, CFG);
        assertThat(cost / 1000.0)
                .as("cost (ms) 10 s after a single 1 s spike on a 5 ms backend")
                .isLessThan(100.0);
    }

    /**
     * `samples` is an int. After 2^31 calls on one (subchannel, method) it goes negative and the
     * peer is classified "cold" for the next 2^31 calls (scored on the slow EWMA instead of the
     * peak EWMA). At 25k rps per backend-method that is ~1 day of uptime.
     */
    @Test
    void sampleCounterOverflow_doesNotFlipPeerToCold() {
        java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong(T0);
        MethodTable mt = new MethodTable(CFG, new EwmaClocks(now::get));
        MethodStats ms = mt.statsFor("m");
        long t = T0;
        for (int i = 0; i < 100; i++) {
            t += MS;
            ms.update(t, 5 * MS, CFG);
            mt.windowFor("m").recordResult(true, t);
        }
        now.set(t + 5_000 * MS);

        ms.overrideForTest(null, null, Integer.MAX_VALUE, null, null);
        ms.update(now.get(), 5 * MS, CFG);
        assertThat(ms.getSamples()).as("sample counter after 2^31 calls").isPositive();
    }

    /**
     * After an idle gap longer than the window, rotation advances at most `buckets` steps and moves
     * startNanos forward by only one window — not to now. Every subsequent call within the next
     * (gap / window) rotations wipes the entire ring again, so freshly recorded results disappear.
     */
    /**
     * Memory is measured in samples, not milliseconds: the same method at 2,000 rps and at 5 rps
     * remembers a peak for the same number of samples (and never less than 2 RTTs).
     */
    @Test
    void peakMemory_isMeasuredInSamples_notMilliseconds() {
        MethodScale hot = MethodScale.of(2_000 / 10.0, 2_000); // 2 ms, 2,000 rps over 10 peers
        MethodScale cold = MethodScale.of(5 / 10.0, 2_000); // 2 ms, 5 rps over 10 peers
        double hotSamples = hot.tauFastMillis() / 1000.0 * (2_000 / 10.0);
        double coldSamples =
                Math.min(cold.tauFastMillis(), MethodScale.MAX_TAU_FAST_MILLIS)
                        / 1000.0
                        * (5 / 10.0);
        assertThat(hotSamples).isCloseTo(MethodScale.PEAK_MEMORY_SAMPLES, within(0.5));
        assertThat(coldSamples).isCloseTo(MethodScale.PEAK_MEMORY_SAMPLES, within(0.5));

        MethodScale slow = MethodScale.of(1_000, 1_000_000); // 1 s latency, very high rate
        assertThat(slow.tauFastMillis())
                .as("never shorter than 2 RTTs")
                .isGreaterThanOrEqualTo(2_000);
    }

    /** cost = latency x (inflight + 1): doubling the queued calls doubles the cost. */
    @Test
    void cost_scalesWithInflightPlusOne() {
        java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong(T0);
        MethodTable mt = new MethodTable(CFG, new EwmaClocks(now::get));
        SubchannelState st = new SubchannelState();
        st.markReady(T0);
        MethodStats ms = mt.statsFor("m");
        ms.update(T0 + MS, 10 * MS, CFG);
        double w = PeakEwmaTuner.inflightWeightEff(10, 1, CFG);
        double idle = P2CPicker.peakEwmaCost(mt, ms, st, "m", T0 + MS, w, CFG);
        mt.incrementInflight();
        double one = P2CPicker.peakEwmaCost(mt, ms, st, "m", T0 + MS, w, CFG);
        mt.incrementInflight();
        mt.incrementInflight();
        double three = P2CPicker.peakEwmaCost(mt, ms, st, "m", T0 + MS, w, CFG);
        assertThat(one / idle).isCloseTo(2.0, within(1e-9));
        assertThat(three / idle).isCloseTo(4.0, within(1e-9));
    }

    /** Statistical knobs still parse (no broken service configs) but no longer change anything. */
    @Test
    void deprecatedStatisticalKeys_parse_butAreIgnored() {
        PeakEwmaConfig custom =
                PeakEwmaConfig.fromMap(
                        java.util.Map.of(
                                PeakEwmaConfigKeys.TAU_FAST_MILLIS,
                                10_000,
                                PeakEwmaConfigKeys.INFLIGHT_WEIGHT,
                                0.0));
        MethodStats ms = new MethodStats(5_000, T0);
        assertThat(PeakEwmaTuner.tauFastMillis(ms, custom))
                .isEqualTo(PeakEwmaTuner.tauFastMillis(ms, CFG));
        assertThat(PeakEwmaTuner.inflightWeightEff(10, 1, custom)).isEqualTo(1.0);
    }

    @Test
    void errorWindow_keepsResults_afterIdleGap() {
        ErrorWindow w = new ErrorWindow(10_000); // 10 x 1 s buckets
        w.recordResult(true, T0);
        long t = T0 + 120_000 * MS; // 2 minutes idle
        w.recordResult(false, t);
        w.recordResult(false, t + MS);
        w.recordResult(false, t + 2 * MS);
        assertThat(w.snapshot(t + 3 * MS).total)
                .as("results recorded in the last 3 ms after an idle gap")
                .isEqualTo(3);
    }

    /**
     * The tuner resizes every window on every tick (8 s..45 s) without rescaling the buckets. The
     * rate estimate total/currentWindow then mixes old-width buckets with the new width, so a
     * resize from 45 s → 8 s reports a ~5x inflated call rate (which feeds back into the next
     * window size, minSamples, and warmup thresholds).
     */
    @Test
    void errorWindow_rateEstimate_survivesResize() {
        ErrorWindow w = new ErrorWindow(45_000);
        long t = T0;
        for (int i = 0; i < 600; i++) { // 10 rps for 60 s
            t += 100 * MS;
            w.recordResult(true, t);
        }
        w.setWindowMillis(8_000);
        double rate = PeakEwmaTuner.methodRatePerSec(w, t);
        assertThat(rate)
                .as("estimated rate (true rate 10/s) right after a resize")
                .isCloseTo(10.0, within(3.0));
    }

    /**
     * Outlier ejection caps: maxEjectionPercentEff(n) = 10 + 5*log2(n). Ejecting even ONE backend
     * needs 100/n <= that, which first holds at n = 5. With 2, 3 or 4 backends — very common for
     * small services — no backend can ever be ejected, no matter how broken.
     */
    @Test
    void oneBackend_canBeEjected_inSmallFleets() {
        for (int n = 2; n <= 4; n++) {
            assertThat(PeakEwmaTuner.maxEjectedCountEff(n))
                    .as("max ejected peers with %d backends", n)
                    .isGreaterThanOrEqualTo(1);
            assertThat(n - 1)
                    .as("peers left after ejecting one of %d vs min-ready", n)
                    .isGreaterThanOrEqualTo(PeakEwmaTuner.minReadyAfterEjectEff(n));
        }
    }
}
