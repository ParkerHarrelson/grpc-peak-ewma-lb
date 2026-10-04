package dev.parkerharrelson.grpc.peakewma;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class EjectionBackoffTest {

    private static final long BASE = 5_000_000_000L; // 5 s

    @Test
    void repeatEjections_lastLonger_andHealthyTimeForgives() {
        EjectionBackoff b = new EjectionBackoff();
        long t = 1_000_000_000L;

        long end1 = b.nextEjectionEnd(t, BASE);
        assertEquals(BASE, end1 - t, "first ejection: one base period");

        // Re-ejected right after coming back: two base periods.
        long end2 = b.nextEjectionEnd(end1, BASE);
        assertEquals(2 * BASE, end2 - end1);

        // Healthy for one base period after the ejection ends: forgiven one level.
        b.maybeForgive(end2 + BASE, BASE, false);
        assertEquals(1, b.level());
        long end3 = b.nextEjectionEnd(end2 + BASE, BASE);
        assertEquals(2 * BASE, end3 - (end2 + BASE), "back to level 2, not 3");

        // No forgiveness while still ejected.
        b.maybeForgive(end3 - 1, BASE, true);
        assertEquals(2, b.level());
    }

    @Test
    void duration_isCappedAtFiveMinutes() {
        EjectionBackoff b = new EjectionBackoff();
        long t = 0;
        long d = 0;
        for (int i = 0; i < 100; i++) {
            long end = b.nextEjectionEnd(t, 60_000_000_000L);
            d = end - t;
            t = end;
        }
        assertEquals(EjectionBackoff.MAX_EJECTION_NANOS, d);
    }
}
