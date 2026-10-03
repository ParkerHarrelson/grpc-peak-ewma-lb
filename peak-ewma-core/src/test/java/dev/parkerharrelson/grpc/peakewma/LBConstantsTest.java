package dev.parkerharrelson.grpc.peakewma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

class LBConstantsTest {

    @Test
    void classIsFinal_andConstructorIsPrivate() throws Exception {
        assertTrue(
                Modifier.isFinal(LBConstants.class.getModifiers()),
                "LBConstants should be final to prevent subclassing");

        Constructor<?>[] ctors = LBConstants.class.getDeclaredConstructors();
        assertEquals(1, ctors.length, "exactly one constructor expected");
        Constructor<?> c = ctors[0];
        assertTrue(
                Modifier.isPrivate(c.getModifiers()), "utility class constructor must be private");

        c.setAccessible(true);
        Object instance = c.newInstance();
        assertNotNull(instance);
    }

    @Test
    void metricConstants_nonNull_andHaveExpectedValues() {
        assertEquals("method", LBConstants.METHOD);
        assertEquals("outcome", LBConstants.OUTCOME);
        assertEquals("subchannel", LBConstants.SUBCHANNEL);
        assertEquals("reason", LBConstants.REASON);
        assertEquals("key", LBConstants.KEY);
        assertEquals("unknown", LBConstants.UNKNOWN);
        assertEquals("latency", LBConstants.LATENCY);
        assertEquals("errors", LBConstants.ERRORS);
        assertEquals("no_ready", LBConstants.NO_READY);
        assertEquals("all_ejected", LBConstants.ALL_EJECTED);
        assertEquals("ok_fallback", LBConstants.OK_FALLBACK);
        assertEquals("ok", LBConstants.OK);
        assertEquals("no_table", LBConstants.NO_TABLE);
    }
}
