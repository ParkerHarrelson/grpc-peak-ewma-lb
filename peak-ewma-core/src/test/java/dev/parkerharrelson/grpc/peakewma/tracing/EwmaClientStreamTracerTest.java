package dev.parkerharrelson.grpc.peakewma.tracing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.parkerharrelson.grpc.peakewma.EwmaClocks;
import dev.parkerharrelson.grpc.peakewma.MethodStats;
import dev.parkerharrelson.grpc.peakewma.PeakEwmaConfig;
import dev.parkerharrelson.grpc.peakewma.metrics.NoopLbMetrics;
import dev.parkerharrelson.grpc.peakewma.outlier.ErrorWindow;
import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

class EwmaClientStreamTracerTest {

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
                .initialRttMicros(20_000)
                .build();
    }

    @Test
    void constructor_doesNotIncOrDec_untilStreamLifecycleFires() {
        ManualClock mc = new ManualClock(1_000_000_000L);
        EwmaClocks clocks = new EwmaClocks(mc);
        PeakEwmaConfig c = cfg();

        MethodStats ms = new MethodStats(c.initialRttMicros, 0L);
        ErrorWindow win = new ErrorWindow(10, 1000);

        AtomicInteger inc = new AtomicInteger();
        AtomicInteger dec = new AtomicInteger();

        new EwmaClientStreamTracer(
                ms,
                c,
                clocks,
                inc::incrementAndGet,
                dec::incrementAndGet,
                win,
                NoopLbMetrics.INSTANCE,
                "svc/Method");

        assertEquals(0, inc.get(), "inflight should not be incremented at tracer construction");
        assertEquals(0, dec.get());
    }

    @Test
    void streamClosed_withoutStreamCreated_stillRecordsOutcome_butDoesNotDec() {
        ManualClock mc = new ManualClock(1_000_000_000L);
        EwmaClocks clocks = new EwmaClocks(mc);
        PeakEwmaConfig c = cfg();

        MethodStats ms = new MethodStats(c.initialRttMicros, 0L);
        ErrorWindow win = new ErrorWindow(10, 1000);

        AtomicInteger inc = new AtomicInteger();
        AtomicInteger dec = new AtomicInteger();

        EwmaClientStreamTracer tracer =
                new EwmaClientStreamTracer(
                        ms,
                        c,
                        clocks,
                        inc::incrementAndGet,
                        dec::incrementAndGet,
                        win,
                        NoopLbMetrics.INSTANCE,
                        "svc/Method");

        tracer.streamClosed(Status.OK);

        assertEquals(0, inc.get(), "inc should not fire when streamCreated was skipped");
        assertEquals(0, dec.get(), "dec should not fire when streamCreated was skipped");
        assertEquals(0, ms.getSamples(), "no RTT to record without streamCreated");

        ErrorWindow.Snapshot snap = win.snapshot(mc.now());
        assertEquals(1, snap.total);
        assertEquals(1, snap.successes);
        assertEquals(0, snap.errors);
    }

    @Test
    void streamCreated_incrementsInflight_then_streamClosed_decrements_andRecordsRtt() {
        ManualClock mc = new ManualClock(2_000_000_000L);
        EwmaClocks clocks = new EwmaClocks(mc);
        PeakEwmaConfig c = cfg();

        MethodStats ms = new MethodStats(c.initialRttMicros, 0L);
        ErrorWindow win = new ErrorWindow(10, 1000);

        AtomicInteger inc = new AtomicInteger();
        AtomicInteger dec = new AtomicInteger();

        EwmaClientStreamTracer tracer =
                new EwmaClientStreamTracer(
                        ms,
                        c,
                        clocks,
                        inc::incrementAndGet,
                        dec::incrementAndGet,
                        win,
                        NoopLbMetrics.INSTANCE,
                        "svc/Method");

        tracer.streamCreated(Attributes.EMPTY, new Metadata());
        assertEquals(1, inc.get(), "inc fires on streamCreated");
        assertEquals(0, dec.get());

        mc.advanceNanos(5_000_000L);
        tracer.streamClosed(Status.OK);

        assertEquals(1, dec.get(), "dec fires on streamClosed after streamCreated");
        assertEquals(1, ms.getSamples(), "stats updated exactly once");
        ErrorWindow.Snapshot snap = win.snapshot(mc.now());
        assertEquals(1, snap.total);
        assertEquals(1, snap.successes);
        assertEquals(0, snap.errors);
        assertEquals(0.0, snap.errorRate, 0.0);
    }

    @Test
    void streamClosed_errorRecordsError_inWindow_andStillUpdatesDec() {
        ManualClock mc = new ManualClock(3_000_000_000L);
        EwmaClocks clocks = new EwmaClocks(mc);
        PeakEwmaConfig c = cfg();

        MethodStats ms = new MethodStats(c.initialRttMicros, 0L);
        ErrorWindow win = new ErrorWindow(10, 1000);

        AtomicInteger dec = new AtomicInteger();

        EwmaClientStreamTracer tracer =
                new EwmaClientStreamTracer(
                        ms,
                        c,
                        clocks,
                        null,
                        dec::incrementAndGet,
                        win,
                        NoopLbMetrics.INSTANCE,
                        "svc/Method");

        tracer.streamCreated(Attributes.EMPTY, new Metadata());
        mc.advanceNanos(1_000_000L);
        tracer.streamClosed(Status.INTERNAL);

        assertEquals(1, dec.get());
        assertEquals(1, ms.getSamples());
        ErrorWindow.Snapshot snap = win.snapshot(mc.now());
        assertEquals(1, snap.total);
        assertEquals(0, snap.successes);
        assertEquals(1, snap.errors);
        assertEquals(1.0, snap.errorRate, 0.0);
    }

    @Test
    void nullCallbacks_areTreatedAsNoOps_andDoNotThrow() {
        ManualClock mc = new ManualClock(4_000_000_000L);
        EwmaClocks clocks = new EwmaClocks(mc);
        PeakEwmaConfig c = cfg();

        MethodStats ms = new MethodStats(c.initialRttMicros, 0L);
        ErrorWindow win = new ErrorWindow(10, 1000);

        EwmaClientStreamTracer tracer =
                new EwmaClientStreamTracer(
                        ms, c, clocks, null, null, win, NoopLbMetrics.INSTANCE, "svc/Method");

        tracer.streamCreated(Attributes.EMPTY, new Metadata());
        tracer.streamClosed(Status.OK);
        assertEquals(0, ms.getSamples(), "no RTT because clock did not advance");
        assertEquals(1, win.snapshot(mc.now()).total);
    }
}
