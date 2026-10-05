package dev.parkerharrelson.grpc.peakewma;

import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.METHOD_A;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** #94 instrumentation: MethodScale gauges and the outlier-tick timer. */
class InstrumentationTest {

    static final class Recording extends AdversarialFixture.RecordingMetrics {
        final Map<String, double[]> scales = new ConcurrentHashMap<>();
        final AtomicLong ticks = new AtomicLong();

        @Override
        public void setMethodScale(String method, double peak, double baseline, double seed) {
            scales.put(method, new double[] {peak, baseline, seed});
        }

        @Override
        public void recordOutlierTick(long durationNanos) {
            assertTrue(durationNanos >= 0);
            ticks.incrementAndGet();
        }
    }

    @Test
    void tick_publishesMethodScaleAndItsOwnDuration() {
        Recording m = new Recording();
        AdversarialFixture f = new AdversarialFixture(PeakEwmaConfig.DEFAULTS, m);
        int[] ports = {7000, 7001, 7002};
        for (int i = 0; i < ports.length; i++) {
            f.models.put(ports[i], AdversarialFixture.jittered(2.0, 0.1, i));
        }
        f.resolveAndReady(ports);

        f.run(1_000, 5_000, METHOD_A);
        f.drain();

        assertTrue(m.ticks.get() >= 4, "one timed tick per second: " + m.ticks.get());
        double[] s = m.scales.get(METHOD_A.getFullMethodName());
        assertNotNull(s, "scale published for the method");
        MethodScale live =
                AdversarialSimulationTest.table(f, 7000)
                        .peekStats(METHOD_A.getFullMethodName())
                        .scale();
        assertEquals(live.tauFastMillis(), s[0], 1e-9, "peak half-life matches what stats use");
        assertEquals(live.tauSlowMillis(), s[1], 1e-9, "baseline half-life matches");
        assertEquals(2_000, s[2], 400, "seed is the fleet's typical latency (~2 ms)");
    }
}
