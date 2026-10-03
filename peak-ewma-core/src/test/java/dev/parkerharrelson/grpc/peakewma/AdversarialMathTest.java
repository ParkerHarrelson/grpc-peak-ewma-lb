package dev.parkerharrelson.grpc.peakewma;

import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.MS;
import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.T0;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import dev.parkerharrelson.grpc.peakewma.outlier.ErrorWindow;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Numerical / formula bugs in the EWMA, tuner and error window. */
@Tag("adversarial")
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

        ((AtomicInteger) AdversarialFixture.getField(ms, "samples")).set(Integer.MAX_VALUE);
        ms.update(now.get(), 5 * MS, CFG);
        assertThat(ms.getSamples()).as("sample counter after 2^31 calls").isPositive();
    }

    /**
     * warmupMillisEff = clamp(0.5 * seedMicros * 150, 300, 3000). seedMicros is clamped to ≥ 2000,
     * so the expression is ≥ 150 000 and always clamps to 3000 — the "adaptive" warmup is a
     * constant (unit mismatch: micros treated as millis).
     */
    @Test
    void warmupDuration_actuallyAdaptsToLatency() {
        java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong(T0);
        EwmaClocks clocks = new EwmaClocks(now::get);
        MethodTable fast =
                new MethodTable(PeakEwmaConfig.builder().initialRttMicros(2_000).build(), clocks);
        MethodTable slow =
                new MethodTable(
                        PeakEwmaConfig.builder().initialRttMicros(2_000_000).build(), clocks);
        assertThat(PeakEwmaTuner.warmupMillisEff(fast))
                .as("warmup for a 2 ms service vs a 2 s service")
                .isNotEqualTo(PeakEwmaTuner.warmupMillisEff(slow));
    }

    /** Documented knobs that are silently overridden by hard-coded clamps. */
    @Test
    void inflightWeight_zero_disablesInflightPenalty() {
        PeakEwmaConfig cfg = PeakEwmaConfig.builder().inflightWeight(0.0).build();
        assertThat(PeakEwmaTuner.inflightWeightEff(10, 1, cfg))
                .as("inflightWeightEff when the user configured inflightWeight=0")
                .isZero();
    }

    @Test
    void inflightWeight_large_isHonoured() {
        PeakEwmaConfig cfg = PeakEwmaConfig.builder().inflightWeight(1.0).build();
        assertThat(PeakEwmaTuner.inflightWeightEff(1, 1, cfg))
                .as("inflightWeightEff when the user configured inflightWeight=1.0")
                .isGreaterThanOrEqualTo(1.0);
    }

    @Test
    void tauFastMillis_isHonoured() {
        PeakEwmaConfig cfg = PeakEwmaConfig.builder().tauFastMillis(10_000).build();
        MethodStats ms = new MethodStats(5_000, T0);
        assertThat(PeakEwmaTuner.tauFastMillis(ms, cfg))
                .as("effective fast half-life when the user configured 10 s")
                .isEqualTo(10_000);
    }

    /**
     * After an idle gap longer than the window, rotation advances at most `buckets` steps and moves
     * startNanos forward by only one window — not to now. Every subsequent call within the next
     * (gap / window) rotations wipes the entire ring again, so freshly recorded results disappear.
     */
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
            int pct = (int) Math.round(100.0 / n);
            assertThat(pct)
                    .as("ejecting 1 of %d backends (%d%%) vs maxEjectionPercentEff", n, pct)
                    .isLessThanOrEqualTo(PeakEwmaTuner.maxEjectionPercentEff(n));
        }
    }
}
