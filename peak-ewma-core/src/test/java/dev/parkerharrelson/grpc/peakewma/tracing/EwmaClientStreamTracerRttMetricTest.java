package dev.parkerharrelson.grpc.peakewma.tracing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.parkerharrelson.grpc.peakewma.EwmaClocks;
import dev.parkerharrelson.grpc.peakewma.MethodTable;
import dev.parkerharrelson.grpc.peakewma.PeakEwmaConfig;
import dev.parkerharrelson.grpc.peakewma.metrics.LbMetrics;
import io.grpc.Attributes;
import io.grpc.ClientStreamTracer;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

class EwmaClientStreamTracerRttMetricTest {

    static final class RecordingLbMetrics implements LbMetrics {
        final List<long[]> rtt = new ArrayList<>();
        final List<String> methods = new ArrayList<>();

        @Override
        public void recordPick(String outcome) {
            /* no-op sink for this recording test */
        }

        @Override
        public void setInflight(String subchannelId, int inflight) {
            /* no-op sink for this recording test */
        }

        @Override
        public void setCost(String subchannelId, String method, double cost) {
            /* no-op sink for this recording test */
        }

        @Override
        public void recordOutlierEjection(
                String subchannelId, String reason, double errorRate, double latencyRatio) {
            /* no-op sink for this recording test */
        }

        @Override
        public void setReadySubchannelCount(int readyCount) {
            /* no-op sink for this recording test */
        }

        @Override
        public void setEjectedSubchannelCount(int ejectedCount) {
            /* no-op sink for this recording test */
        }

        @Override
        public void setAdaptiveTuning(String key, double value) {
            /* no-op sink for this recording test */
        }

        @Override
        public void setMethodLatencyEwma(
                String method, double slowEwmaMicros, double fastEwmaMicros) {
            /* no-op sink for this recording test */
        }

        @Override
        public void removeSubchannel(String subchannelId) {
            /* no-op sink for this recording test */
        }

        @Override
        public void setMethodRate(String method, double ratePerSec) {
            /* no-op sink for this recording test */
        }

        @Override
        public void setMethodErrorRate(String method, double errorRate) {
            /* no-op sink for this recording test */
        }

        @Override
        public void recordObservedRtt(String method, long rttNanos) {
            methods.add(method);
            rtt.add(new long[] {rttNanos});
        }
    }

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
    }

    @Test
    void factory_passes_metrics_through_to_tracer_and_observes_rtt_on_streamClosed() {
        ManualClock mc = new ManualClock(1_000_000_000L);
        EwmaClocks clocks = new EwmaClocks(mc);
        PeakEwmaConfig cfg = PeakEwmaConfig.builder().initialRttMicros(5_000).build();
        MethodTable table = new MethodTable(cfg, clocks);

        RecordingLbMetrics metrics = new RecordingLbMetrics();
        AtomicInteger inc = new AtomicInteger();
        AtomicInteger dec = new AtomicInteger();

        EwmaClientStreamTracerFactory factory =
                new EwmaClientStreamTracerFactory(
                        table,
                        cfg,
                        clocks,
                        "svc/Foo",
                        inc::incrementAndGet,
                        dec::incrementAndGet,
                        metrics);

        ClientStreamTracer tracer = factory.newClientStreamTracer(null, new Metadata());
        tracer.streamCreated(Attributes.EMPTY, new Metadata());

        long rttNanos = 3_500_000L;
        mc.advanceNanos(rttNanos);
        tracer.streamClosed(Status.OK);

        assertEquals(1, metrics.methods.size(), "one RTT sample published");
        assertEquals("svc/Foo", metrics.methods.get(0));
        assertTrue(
                Math.abs(metrics.rtt.get(0)[0] - rttNanos) <= 1,
                "published RTT should match elapsed nanos");
    }

    @Test
    void tracer_does_not_publish_rtt_when_streamCreated_was_skipped() {
        ManualClock mc = new ManualClock(1_000_000_000L);
        EwmaClocks clocks = new EwmaClocks(mc);
        PeakEwmaConfig cfg = PeakEwmaConfig.builder().initialRttMicros(5_000).build();
        MethodTable table = new MethodTable(cfg, clocks);

        RecordingLbMetrics metrics = new RecordingLbMetrics();

        EwmaClientStreamTracerFactory factory =
                new EwmaClientStreamTracerFactory(
                        table, cfg, clocks, "svc/Foo", () -> {}, () -> {}, metrics);

        ClientStreamTracer tracer = factory.newClientStreamTracer(null, new Metadata());
        tracer.streamClosed(Status.OK);

        assertEquals(0, metrics.methods.size(), "no RTT to publish without streamCreated");
    }
}
