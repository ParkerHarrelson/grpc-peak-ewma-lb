package dev.parkerharrelson.grpc.peakewma;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

class PeakEwmaConfigKeysTest {

    @Test
    void constants_existAndAreNonNull() {
        assertEquals("peak_ewma_p2c", PeakEwmaConfigKeys.POLICY_NAME);

        assertNotNull(PeakEwmaConfigKeys.TAU_FAST_MILLIS);
        assertNotNull(PeakEwmaConfigKeys.TAU_SLOW_MILLIS);
        assertNotNull(PeakEwmaConfigKeys.INFLIGHT_WEIGHT);
        assertNotNull(PeakEwmaConfigKeys.INITIAL_RTT_MICROS);

        assertNotNull(PeakEwmaConfigKeys.OUTLIER_ENABLED);
        assertNotNull(PeakEwmaConfigKeys.OUTLIER_WINDOW_MILLIS);
        assertNotNull(PeakEwmaConfigKeys.OUTLIER_ERROR_RATE);
        assertNotNull(PeakEwmaConfigKeys.OUTLIER_EJECT_MILLIS);
        assertNotNull(PeakEwmaConfigKeys.OUTLIER_LATENCY_MULTIPLIER);
        assertNotNull(PeakEwmaConfigKeys.STALE_MILLIS_FOR_RATIO);
        assertNotNull(PeakEwmaConfigKeys.OUTLIER_REENTRY_COOLDOWN_MILLIS);
        assertNotNull(PeakEwmaConfigKeys.OUTLIER_TICK_INTERVAL_MILLIS);
        assertNotNull(PeakEwmaConfigKeys.METHOD_MAX_ENTRIES);
        assertNotNull(PeakEwmaConfigKeys.METHOD_PRUNE_STALE_AFTER_MILLIS);
    }

    @Test
    void class_isUtilityWithPrivateCtor() throws Exception {
        Constructor<?>[] ctors = PeakEwmaConfigKeys.class.getDeclaredConstructors();
        assertEquals(1, ctors.length);
        Constructor<?> c = ctors[0];
        assertTrue(Modifier.isPrivate(c.getModifiers()));

        c.setAccessible(true);
        Object instance = c.newInstance();
        assertNotNull(instance);
        assertEquals(PeakEwmaConfigKeys.class, instance.getClass());
    }
}
