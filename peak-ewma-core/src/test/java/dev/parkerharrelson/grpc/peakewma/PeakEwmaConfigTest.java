package dev.parkerharrelson.grpc.peakewma;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PeakEwmaConfigTest {

    private static PeakEwmaConfig defaults() {
        return PeakEwmaConfig.DEFAULTS;
    }

    @Test
    void defaults_areAsExpected() throws Exception {
        PeakEwmaConfig d = defaults();
        assertEquals(getPrivateStaticLong("DEF_TAU_FAST_MS"), d.tauFastMillis);
        assertEquals(getPrivateStaticLong("DEF_TAU_SLOW_MS"), d.tauSlowMillis);
        assertEquals(getPrivateStaticDouble("DEF_INFLIGHT_WEIGHT"), d.inflightWeight, 0.0);
        assertEquals(getPrivateStaticLong("DEF_INITIAL_RTT_US"), d.initialRttMicros);

        assertEquals(getPrivateStaticBoolean(), d.outlierEnabled);
        assertEquals(getPrivateStaticLong("DEF_OUTLIER_WINDOW_MS"), d.outlierWindowMillis);
        assertEquals(getPrivateStaticDouble("DEF_OUTLIER_ERROR_RATE"), d.outlierErrorRate, 0.0);
        assertEquals(getPrivateStaticLong("DEF_OUTLIER_EJECT_MS"), d.outlierEjectMillis);
        assertEquals(
                getPrivateStaticDouble("DEF_OUTLIER_LAT_MULT"), d.outlierLatencyMultiplier, 0.0);
        assertEquals(getPrivateStaticLong("DEF_STALE_MS_FOR_RATIO"), d.staleMillisForRatio);

        assertEquals(
                getPrivateStaticLong("DEF_REENTRY_COOLDOWN_MS"), d.outlierReentryCooldownMillis);
        assertEquals(
                getPrivateStaticLong("DEF_OUTLIER_TICK_INTERVAL_MS"), d.outlierTickIntervalMillis);
        assertEquals(getPrivateStaticInt(), d.methodMaxEntries);
        assertEquals(
                getPrivateStaticLong("DEF_METHOD_PRUNE_STALE_MS"), d.methodPruneStaleAfterMillis);

        assertEquals(d, PeakEwmaConfig.builder().build());
        assertEquals(d.hashCode(), PeakEwmaConfig.builder().build().hashCode());

        String s = d.toString();
        assertTrue(s.contains("PeakEwmaConfig{"));
        assertTrue(s.contains("tauFastMillis=" + d.tauFastMillis));
        assertTrue(s.contains("outlierEnabled=" + d.outlierEnabled));
    }

    @Test
    void builder_customValues_buildsAndSupportsEqualsHashCode() {
        PeakEwmaConfig c1 =
                PeakEwmaConfig.builder()
                        .tauFastMillis(1500)
                        .tauSlowMillis(45_000)
                        .inflightWeight(0.2)
                        .initialRttMicros(12_345)
                        .outlierEnabled(false)
                        .outlierWindowMillis(9_999)
                        .outlierErrorRate(0.33)
                        .outlierEjectMillis(7_777)
                        .outlierLatencyMultiplier(3.1)
                        .staleMillisForRatio(55_555)
                        .outlierReentryCooldownMillis(4_321)
                        .outlierTickIntervalMillis(750)
                        .methodMaxEntries(1024)
                        .methodPruneStaleAfterMillis(33_333)
                        .build();

        PeakEwmaConfig c2 =
                PeakEwmaConfig.builder()
                        .tauFastMillis(1500)
                        .tauSlowMillis(45_000)
                        .inflightWeight(0.2)
                        .initialRttMicros(12_345)
                        .outlierEnabled(false)
                        .outlierWindowMillis(9_999)
                        .outlierErrorRate(0.33)
                        .outlierEjectMillis(7_777)
                        .outlierLatencyMultiplier(3.1)
                        .staleMillisForRatio(55_555)
                        .outlierReentryCooldownMillis(4_321)
                        .outlierTickIntervalMillis(750)
                        .methodMaxEntries(1024)
                        .methodPruneStaleAfterMillis(33_333)
                        .build();

        assertEquals(c1, c2);
        assertEquals(c1.hashCode(), c2.hashCode());
        assertNotEquals(c1, defaults());
    }

    @Test
    void validation_guardsThrowOnInvalidValues() {

        assertThrows(
                IllegalArgumentException.class,
                () -> PeakEwmaConfig.builder().tauFastMillis(0).build());

        assertThrows(
                IllegalArgumentException.class,
                () -> PeakEwmaConfig.builder().tauSlowMillis(0).build());

        assertThrows(
                IllegalArgumentException.class,
                () -> PeakEwmaConfig.builder().inflightWeight(-0.01).build());

        assertThrows(
                IllegalArgumentException.class,
                () -> PeakEwmaConfig.builder().initialRttMicros(0).build());

        assertThrows(
                IllegalArgumentException.class,
                () -> PeakEwmaConfig.builder().outlierWindowMillis(-1).build());

        assertThrows(
                IllegalArgumentException.class,
                () -> PeakEwmaConfig.builder().outlierErrorRate(-0.01).build());
        assertThrows(
                IllegalArgumentException.class,
                () -> PeakEwmaConfig.builder().outlierErrorRate(1.01).build());

        assertThrows(
                IllegalArgumentException.class,
                () -> PeakEwmaConfig.builder().outlierEjectMillis(-5).build());

        assertThrows(
                IllegalArgumentException.class,
                () -> PeakEwmaConfig.builder().outlierLatencyMultiplier(0.99).build());

        assertThrows(
                IllegalArgumentException.class,
                () -> PeakEwmaConfig.builder().staleMillisForRatio(-1).build());

        assertThrows(
                IllegalArgumentException.class,
                () -> PeakEwmaConfig.builder().methodMaxEntries(0).build());

        PeakEwmaConfig.Builder tickBuilder = PeakEwmaConfig.builder().outlierTickIntervalMillis(50);
        assertThrows(
                IllegalArgumentException.class,
                tickBuilder::build,
                "outlierTickIntervalMillis below 100 should be rejected");
    }

    @Test
    void merge_returnsBaseWhenNullOrEmpty() {
        PeakEwmaConfig base = defaults();
        assertSame(base, PeakEwmaConfig.merge(base, null));
        assertSame(base, PeakEwmaConfig.merge(base, Map.of()));
        assertSame(base, base.merge(null));
        assertSame(base, base.merge(Map.of()));
    }

    @Test
    void fromMap_and_merge_acceptNumbers() {
        Map<String, Object> m = new HashMap<>();
        m.put(PeakEwmaConfigKeys.TAU_FAST_MILLIS, 2345L);
        m.put(PeakEwmaConfigKeys.TAU_SLOW_MILLIS, 46_000);
        m.put(PeakEwmaConfigKeys.INFLIGHT_WEIGHT, 0.23);
        m.put(PeakEwmaConfigKeys.INITIAL_RTT_MICROS, 11_111L);
        m.put(PeakEwmaConfigKeys.OUTLIER_ENABLED, true);
        m.put(PeakEwmaConfigKeys.OUTLIER_WINDOW_MILLIS, 12_345L);
        m.put(PeakEwmaConfigKeys.OUTLIER_ERROR_RATE, 0.44);
        m.put(PeakEwmaConfigKeys.OUTLIER_EJECT_MILLIS, 9_876L);
        m.put(PeakEwmaConfigKeys.OUTLIER_LATENCY_MULTIPLIER, 2.7);
        m.put(PeakEwmaConfigKeys.STALE_MILLIS_FOR_RATIO, 66_666L);
        m.put(PeakEwmaConfigKeys.OUTLIER_REENTRY_COOLDOWN_MILLIS, 4_444L);
        m.put(PeakEwmaConfigKeys.OUTLIER_TICK_INTERVAL_MILLIS, 2_500L);
        m.put(PeakEwmaConfigKeys.METHOD_MAX_ENTRIES, 2048);
        m.put(PeakEwmaConfigKeys.METHOD_PRUNE_STALE_AFTER_MILLIS, 222_222L);

        PeakEwmaConfig c = PeakEwmaConfig.fromMap(m);

        assertEquals(2345, c.tauFastMillis);
        assertEquals(46_000, c.tauSlowMillis);
        assertEquals(0.23, c.inflightWeight, 0.0);
        assertEquals(11_111, c.initialRttMicros);
        assertTrue(c.outlierEnabled);
        assertEquals(12_345, c.outlierWindowMillis);
        assertEquals(0.44, c.outlierErrorRate, 0.0);
        assertEquals(9_876, c.outlierEjectMillis);
        assertEquals(2.7, c.outlierLatencyMultiplier, 0.0);
        assertEquals(66_666, c.staleMillisForRatio);
        assertEquals(4_444, c.outlierReentryCooldownMillis);
        assertEquals(2_500, c.outlierTickIntervalMillis);
        assertEquals(2048, c.methodMaxEntries);
        assertEquals(222_222, c.methodPruneStaleAfterMillis);
    }

    @Test
    void fromMap_acceptsStringsAndCoerces() {
        Map<String, Object> m = new HashMap<>();
        m.put(PeakEwmaConfigKeys.TAU_FAST_MILLIS, "3333");
        m.put(PeakEwmaConfigKeys.INFLIGHT_WEIGHT, "0.19");
        m.put(PeakEwmaConfigKeys.OUTLIER_ENABLED, "false");
        m.put(PeakEwmaConfigKeys.METHOD_MAX_ENTRIES, "777");

        PeakEwmaConfig c = PeakEwmaConfig.fromMap(m);
        assertEquals(3333, c.tauFastMillis);
        assertEquals(0.19, c.inflightWeight, 0.0);
        assertFalse(c.outlierEnabled);
        assertEquals(777, c.methodMaxEntries);
    }

    @Test
    void merge_throwsHelpfulErrorsOnWrongTypes() {
        PeakEwmaConfig base = defaults();

        Map<String, Object> badLong = Map.of(PeakEwmaConfigKeys.TAU_FAST_MILLIS, new Object());
        IllegalArgumentException e1 =
                assertThrows(
                        IllegalArgumentException.class, () -> PeakEwmaConfig.merge(base, badLong));
        assertTrue(e1.getMessage().contains(PeakEwmaConfigKeys.TAU_FAST_MILLIS));
        assertTrue(e1.getMessage().contains("expected number or string"));

        Map<String, Object> badDouble = Map.of(PeakEwmaConfigKeys.OUTLIER_ERROR_RATE, new Object());
        IllegalArgumentException e2 =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> PeakEwmaConfig.merge(base, badDouble));
        assertTrue(e2.getMessage().contains(PeakEwmaConfigKeys.OUTLIER_ERROR_RATE));

        Map<String, Object> badInt = Map.of(PeakEwmaConfigKeys.METHOD_MAX_ENTRIES, new Object());
        IllegalArgumentException e3 =
                assertThrows(
                        IllegalArgumentException.class, () -> PeakEwmaConfig.merge(base, badInt));
        assertTrue(e3.getMessage().contains(PeakEwmaConfigKeys.METHOD_MAX_ENTRIES));

        Map<String, Object> badBool = Map.of(PeakEwmaConfigKeys.OUTLIER_ENABLED, new Object());
        IllegalArgumentException e4 =
                assertThrows(
                        IllegalArgumentException.class, () -> PeakEwmaConfig.merge(base, badBool));
        assertTrue(e4.getMessage().contains(PeakEwmaConfigKeys.OUTLIER_ENABLED));
    }

    private static long getPrivateStaticLong(String name) throws Exception {
        Field f = PeakEwmaConfig.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.getLong(null);
    }

    private static int getPrivateStaticInt() throws Exception {
        Field f = PeakEwmaConfig.class.getDeclaredField("DEF_METHOD_MAX_ENTRIES");
        f.setAccessible(true);
        return f.getInt(null);
    }

    private static double getPrivateStaticDouble(String name) throws Exception {
        Field f = PeakEwmaConfig.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.getDouble(null);
    }

    private static boolean getPrivateStaticBoolean() throws Exception {
        Field f = PeakEwmaConfig.class.getDeclaredField("DEF_OUTLIER_ENABLED");
        f.setAccessible(true);
        return f.getBoolean(null);
    }
}
