package dev.parkerharrelson.grpc.peakewma;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

class EwmaClocksTest {

    @Test
    void nanoTime_usesInjectedSupplier() {
        AtomicLong now = new AtomicLong(123);
        LongSupplier sup = now::get;
        EwmaClocks clocks = new EwmaClocks(sup);
        assertThat(clocks.nanoTime()).isEqualTo(123L);
        now.set(999L);
        assertThat(clocks.nanoTime()).isEqualTo(999L);
    }

    @Test
    void decayFactor_lastUpdateZero_isZero() {
        double df = EwmaClocks.decayFactor(10, 0, 1500);
        assertThat(df).isEqualTo(0.0);
    }

    @Test
    void decayFactor_halfLifeNonPositive_isZero() {
        double df = EwmaClocks.decayFactor(10, 5, 0);
        assertThat(df).isEqualTo(0.0);
        df = EwmaClocks.decayFactor(10, 5, -1);
        assertThat(df).isEqualTo(0.0);
    }

    @Test
    void decayFactor_negativeDeltaClampedToZero_returnsOne() {
        double df = EwmaClocks.decayFactor(5, 10, 1500);
        assertThat(df).isEqualTo(1.0);
    }

    @Test
    void decayFactor_normalCase_decaysExponentially() {
        long halfLifeMs = 1000;
        long last;
        long now;
        last = 1_000_000_000L;
        now = 3_000_000_000L;
        double df = EwmaClocks.decayFactor(now, last, halfLifeMs);
        assertThat(df).isLessThan(1.0).isGreaterThan(0.0);
    }

    @Test
    void millisToNanos_and_nanosToMicros_roundTripEnough() {
        long ms = 1234L;
        long ns = EwmaClocks.millisToNanos(ms);
        assertThat(ns).isEqualTo(1_234_000_000L);
        assertThat(EwmaClocks.nanosToMicros(2_500L)).isEqualTo(2_500.0 / 1_000.0);
    }
}
