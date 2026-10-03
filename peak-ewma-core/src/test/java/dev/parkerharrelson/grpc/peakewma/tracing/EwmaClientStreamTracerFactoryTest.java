package dev.parkerharrelson.grpc.peakewma.tracing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.parkerharrelson.grpc.peakewma.EwmaClocks;
import dev.parkerharrelson.grpc.peakewma.MethodTable;
import dev.parkerharrelson.grpc.peakewma.PeakEwmaConfig;
import io.grpc.ClientStreamTracer;
import io.grpc.Metadata;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

class EwmaClientStreamTracerFactoryTest {

    static final class ManualClock implements LongSupplier {
        private long now;

        ManualClock(long start) {
            this.now = start;
        }

        @Override
        public long getAsLong() {
            return now;
        }

        void advanceNanos(long delta) {
            this.now += delta;
        }

        long now() {
            return now;
        }
    }

    private PeakEwmaConfig cfg() {
        return PeakEwmaConfig.builder()
                .tauFastMillis(1000)
                .tauSlowMillis(30_000)
                .initialRttMicros(10_000)
                .build();
    }

    @Test
    void factory_createsTracer_thatIncrementsOnStreamCreated_andDecrementsOnStreamClosed() {
        ManualClock mc = new ManualClock(10_000_000_000L);
        EwmaClocks clocks = new EwmaClocks(mc);
        PeakEwmaConfig c = cfg();
        MethodTable table = new MethodTable(c, clocks);

        AtomicInteger inc = new AtomicInteger();
        AtomicInteger dec = new AtomicInteger();

        EwmaClientStreamTracerFactory fac =
                new EwmaClientStreamTracerFactory(
                        table, c, clocks, "svc/Foo", inc::incrementAndGet, dec::incrementAndGet);

        ClientStreamTracer tracer = fac.newClientStreamTracer(null, new Metadata());

        assertEquals(0, inc.get(), "inc does not fire at tracer construction");
        assertEquals(0, dec.get());

        tracer.streamCreated(null, new Metadata());
        assertEquals(1, inc.get(), "inc fires on streamCreated");

        mc.advanceNanos(2_000_000L);
        tracer.streamClosed(io.grpc.Status.OK);

        assertEquals(1, dec.get(), "dec fires on streamClosed after streamCreated");

        assertEquals(1, table.statsFor("svc/Foo").getSamples());
        assertEquals(1, table.windowFor("svc/Foo").snapshot(mc.now()).total);
    }

    @Test
    void factory_withNullMethod_usesEmptyKey_andStillWorks() {
        ManualClock mc = new ManualClock(20_000_000_000L);
        EwmaClocks clocks = new EwmaClocks(mc);
        PeakEwmaConfig c = cfg();
        MethodTable table = new MethodTable(c, clocks);

        EwmaClientStreamTracerFactory fac =
                new EwmaClientStreamTracerFactory(table, c, clocks, null, null, null);

        ClientStreamTracer tracer = fac.newClientStreamTracer(null, new Metadata());

        tracer.streamCreated(null, new Metadata());
        mc.advanceNanos(1_000_000L);
        tracer.streamClosed(io.grpc.Status.INTERNAL);

        // Server failures are penalties, not latency samples.
        assertEquals(0, table.statsFor("").getSamples());
        assertEquals(2.0 * c.initialRttMicros, table.statsFor("").getEwmaFastMicros(), 1e-6);
        assertEquals(1, table.windowFor("").snapshot(mc.now()).total);
    }
}
