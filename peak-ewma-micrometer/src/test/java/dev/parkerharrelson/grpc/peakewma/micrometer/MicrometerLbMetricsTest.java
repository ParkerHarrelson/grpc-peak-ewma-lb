package dev.parkerharrelson.grpc.peakewma.micrometer;

import static dev.parkerharrelson.grpc.peakewma.LBConstants.*;
import static org.junit.jupiter.api.Assertions.*;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MicrometerLbMetricsTest {

    private SimpleMeterRegistry registry;
    private MicrometerLbMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new MicrometerLbMetrics(registry);
    }

    @Test
    void constructorRegistersReadyAndEjectedGauges() {
        Gauge readyGauge = registry.get("lb.ready.subchannels").gauge();
        Gauge ejectedGauge = registry.get("lb.ejected.subchannels").gauge();

        assertNotNull(readyGauge);
        assertNotNull(ejectedGauge);

        assertEquals(0.0, readyGauge.value(), 1e-9);
        assertEquals(0.0, ejectedGauge.value(), 1e-9);
    }

    @Test
    void recordPickIncrementsCountersPerOutcome() {
        metrics.recordPick("ok");
        metrics.recordPick("ok");
        metrics.recordPick("no_ready");

        Counter okCounter = registry.get("lb.pick.total").tag(OUTCOME, "ok").counter();
        Counter noReadyCounter = registry.get("lb.pick.total").tag(OUTCOME, "no_ready").counter();

        assertEquals(2.0, okCounter.count(), 1e-9);
        assertEquals(1.0, noReadyCounter.count(), 1e-9);
    }

    @Test
    void setReadyAndEjectedSubchannelCountUpdateGauges() {
        metrics.setReadySubchannelCount(5);
        metrics.setEjectedSubchannelCount(2);

        Gauge readyGauge = registry.get("lb.ready.subchannels").gauge();
        Gauge ejectedGauge = registry.get("lb.ejected.subchannels").gauge();

        assertEquals(5.0, readyGauge.value(), 1e-9);
        assertEquals(2.0, ejectedGauge.value(), 1e-9);
    }

    @Test
    void setInflightCreatesAndUpdatesGaugePerSubchannel() {
        metrics.setInflight("sc-1", 3);

        Gauge g = registry.get("lb.subchannel.inflight").tag(SUBCHANNEL, "sc-1").gauge();
        assertNotNull(g);
        assertEquals(3.0, g.value(), 1e-9);

        metrics.setInflight("sc-1", 7);
        assertEquals(7.0, g.value(), 1e-9);
    }

    @Test
    void setInflightWithNullSubchannelDoesNothing() {
        metrics.setInflight(null, 5);

        assertTrue(
                registry.find("lb.subchannel.inflight").meters().isEmpty(),
                "No inflight gauges should be registered for null subchannel");
    }

    @Test
    void setCostCreatesAndUpdatesGaugePerSubchannelAndMethod() {
        metrics.setCost("sc-1", "methodA", 10.5);

        Gauge g =
                registry.get("lb.subchannel.method.cost")
                        .tag(SUBCHANNEL, "sc-1")
                        .tag(METHOD, "methodA")
                        .gauge();
        assertNotNull(g);
        assertEquals(10.5, g.value(), 1e-9);

        metrics.setCost("sc-1", "methodA", 42.0);
        assertEquals(42.0, g.value(), 1e-9);
    }

    @Test
    void setCostWithNullArgumentsDoesNothing() {
        metrics.setCost(null, "methodA", 1.0);
        metrics.setCost("sc-1", null, 1.0);

        assertTrue(
                registry.find("lb.subchannel.method.cost").meters().isEmpty(),
                "No cost gauges should be registered when subchannel or method is null");
    }

    @Test
    void recordOutlierEjectionUpdatesCounterAndOutlierGauges() {
        metrics.recordOutlierEjection("sc-1", "errors", 0.3, 2.0);
        metrics.recordOutlierEjection("sc-1", "latency", 0.4, 1.5);

        Counter errorsCounter =
                registry.get("lb.outlier.ejections")
                        .tag(SUBCHANNEL, "sc-1")
                        .tag(REASON, "errors")
                        .counter();
        Counter latencyCounter =
                registry.get("lb.outlier.ejections")
                        .tag(SUBCHANNEL, "sc-1")
                        .tag(REASON, "latency")
                        .counter();

        assertEquals(1.0, errorsCounter.count(), 1e-9);
        assertEquals(1.0, latencyCounter.count(), 1e-9);

        Gauge lastErr = registry.get("lb.outlier.last_error_rate").tag(SUBCHANNEL, "sc-1").gauge();
        Gauge lastLat =
                registry.get("lb.outlier.last_latency_ratio").tag(SUBCHANNEL, "sc-1").gauge();

        assertNotNull(lastErr);
        assertNotNull(lastLat);

        assertEquals(0.4, lastErr.value(), 1e-9);
        assertEquals(1.5, lastLat.value(), 1e-9);
    }

    @Test
    void recordOutlierEjectionWithNullSubchannelDoesNothing() {
        metrics.recordOutlierEjection(null, "errors", 0.2, 1.0);

        assertTrue(
                registry.find("lb.outlier.ejections").meters().isEmpty(),
                "No outlier metrics should be registered for null subchannel");
        assertTrue(
                registry.find("lb.outlier.last_error_rate").meters().isEmpty(),
                "No outlier error-rate gauges should be registered for null subchannel");
        assertTrue(
                registry.find("lb.outlier.last_latency_ratio").meters().isEmpty(),
                "No outlier latency-ratio gauges should be registered for null subchannel");
    }

    @Test
    void setAdaptiveTuningCreatesAndUpdatesGaugePerKey() {
        metrics.setAdaptiveTuning(WINDOW_MILLIS_EFF, 1000.0);
        Gauge g = registry.get("lb.tuning.value").tag(KEY, WINDOW_MILLIS_EFF).gauge();

        assertNotNull(g);
        assertEquals(1000.0, g.value(), 1e-9);

        metrics.setAdaptiveTuning(WINDOW_MILLIS_EFF, 2000.0);
        assertEquals(2000.0, g.value(), 1e-9);
    }

    @Test
    void setAdaptiveTuningWithNullKeyDoesNothing() {
        metrics.setAdaptiveTuning(null, 123.0);

        assertTrue(
                registry.find("lb.tuning.value").meters().isEmpty(),
                "No tuning metrics should be registered when key is null");
    }

    @Test
    void setMethodLatencyEwmaCreatesAndUpdatesGaugesPerMethod() {
        metrics.setMethodLatencyEwma("methodA", 100.0, 50.0);

        Gauge slow =
                registry.get("lb.method.latency_ewma_slow_micros").tag(METHOD, "methodA").gauge();
        Gauge fast =
                registry.get("lb.method.latency_ewma_fast_micros").tag(METHOD, "methodA").gauge();

        assertNotNull(slow);
        assertNotNull(fast);

        assertEquals(100.0, slow.value(), 1e-9);
        assertEquals(50.0, fast.value(), 1e-9);

        metrics.setMethodLatencyEwma("methodA", 200.0, 80.0);
        assertEquals(200.0, slow.value(), 1e-9);
        assertEquals(80.0, fast.value(), 1e-9);
    }

    @Test
    void setMethodLatencyEwmaWithNullMethodDoesNothing() {
        metrics.setMethodLatencyEwma(null, 100.0, 50.0);

        assertTrue(
                registry.find("lb.method.latency_ewma_slow_micros").meters().isEmpty(),
                "No slow EWMA metrics should be registered for null method");
        assertTrue(
                registry.find("lb.method.latency_ewma_fast_micros").meters().isEmpty(),
                "No fast EWMA metrics should be registered for null method");
    }

    @Test
    void setMethodRateCreatesAndUpdatesGaugePerMethod() {
        metrics.setMethodRate("methodA", 12.5);

        Gauge rate = registry.get("lb.method.rate_per_sec").tag(METHOD, "methodA").gauge();
        assertNotNull(rate);
        assertEquals(12.5, rate.value(), 1e-9);

        metrics.setMethodRate("methodA", 20.0);
        assertEquals(20.0, rate.value(), 1e-9);
    }

    @Test
    void setMethodRateWithNullMethodDoesNothing() {
        metrics.setMethodRate(null, 5.0);

        assertTrue(
                registry.find("lb.method.rate_per_sec").meters().isEmpty(),
                "No method rate metrics should be registered for null method");
    }

    @Test
    void setMethodErrorRateCreatesAndUpdatesGaugePerMethod() {
        metrics.setMethodErrorRate("methodA", 0.1);

        Gauge errRate = registry.get("lb.method.error_rate").tag(METHOD, "methodA").gauge();
        assertNotNull(errRate);
        assertEquals(0.1, errRate.value(), 1e-9);

        metrics.setMethodErrorRate("methodA", 0.25);
        assertEquals(0.25, errRate.value(), 1e-9);
    }

    @Test
    void setMethodErrorRateWithNullMethodDoesNothing() {
        metrics.setMethodErrorRate(null, 0.3);

        assertTrue(
                registry.find("lb.method.error_rate").meters().isEmpty(),
                "No method error-rate metrics should be registered for null method");
    }

    @Test
    void removeSubchannelRemovesMetersAndStateForThatSubchannel() {
        metrics.setInflight("sc-1", 5);
        metrics.setCost("sc-1", "methodA", 10.0);
        metrics.recordOutlierEjection("sc-1", "errors", 0.3, 1.7);

        assertFalse(
                registry.find("lb.subchannel.inflight").tag(SUBCHANNEL, "sc-1").gauges().isEmpty());
        assertFalse(
                registry.find("lb.subchannel.method.cost")
                        .tag(SUBCHANNEL, "sc-1")
                        .gauges()
                        .isEmpty());
        assertFalse(
                registry.find("lb.outlier.last_error_rate")
                        .tag(SUBCHANNEL, "sc-1")
                        .gauges()
                        .isEmpty());
        assertFalse(
                registry.find("lb.outlier.last_latency_ratio")
                        .tag(SUBCHANNEL, "sc-1")
                        .gauges()
                        .isEmpty());

        // Remove
        metrics.removeSubchannel("sc-1");

        assertTrue(
                registry.find("lb.subchannel.inflight").tag(SUBCHANNEL, "sc-1").gauges().isEmpty());
        assertTrue(
                registry.find("lb.subchannel.method.cost")
                        .tag(SUBCHANNEL, "sc-1")
                        .gauges()
                        .isEmpty());
        assertTrue(
                registry.find("lb.outlier.last_error_rate")
                        .tag(SUBCHANNEL, "sc-1")
                        .gauges()
                        .isEmpty());
        assertTrue(
                registry.find("lb.outlier.last_latency_ratio")
                        .tag(SUBCHANNEL, "sc-1")
                        .gauges()
                        .isEmpty());
    }

    @Test
    void removeSubchannelWithNullDoesNothing() {
        metrics.removeSubchannel(null);

        assertNotNull(registry.get("lb.ready.subchannels").gauge());
        assertNotNull(registry.get("lb.ejected.subchannels").gauge());
    }

    @Test
    void recordObservedRttRegistersTimerAndRecordsSample() {
        metrics.recordObservedRtt("svc/Foo", TimeUnit.MILLISECONDS.toNanos(25));
        metrics.recordObservedRtt("svc/Foo", TimeUnit.MILLISECONDS.toNanos(75));

        Timer t = registry.get("lb.stream.rtt").tag(METHOD, "svc/Foo").timer();
        assertNotNull(t, "lb.stream.rtt timer should be registered per method");
        assertEquals(2L, t.count(), "two RTT samples should be recorded");
        assertTrue(
                t.totalTime(TimeUnit.MILLISECONDS) >= 99.0,
                "total time should sum the samples (~100 ms)");
    }

    @Test
    void recordObservedRttIgnoresNullMethodAndNonPositiveRtt() {
        metrics.recordObservedRtt(null, 1_000_000L);
        metrics.recordObservedRtt("svc/Bar", 0L);
        metrics.recordObservedRtt("svc/Bar", -5L);

        assertTrue(
                registry.find("lb.stream.rtt").timers().isEmpty(),
                "invalid inputs must not register a timer");
    }
}
