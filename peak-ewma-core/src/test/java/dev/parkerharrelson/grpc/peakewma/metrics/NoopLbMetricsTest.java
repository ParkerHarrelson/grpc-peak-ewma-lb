package dev.parkerharrelson.grpc.peakewma.metrics;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

class NoopLbMetricsTest {

    @Test
    void allMethodsAreNoopAndDoNotThrow() {
        NoopLbMetrics noop = NoopLbMetrics.INSTANCE;

        assertDoesNotThrow(() -> noop.recordPick("ok"));
        assertDoesNotThrow(() -> noop.recordPick(null));

        assertDoesNotThrow(() -> noop.setInflight("sc-1", 10));
        assertDoesNotThrow(() -> noop.setInflight(null, 10));

        assertDoesNotThrow(() -> noop.setCost("sc-1", "/svc/Foo", 123.4));
        assertDoesNotThrow(() -> noop.setCost(null, null, 0.0));

        assertDoesNotThrow(() -> noop.recordOutlierEjection("sc-1", "errors", 0.5, 2.0));
        assertDoesNotThrow(() -> noop.recordOutlierEjection(null, null, 0.0, 0.0));

        assertDoesNotThrow(() -> noop.setReadySubchannelCount(5));
        assertDoesNotThrow(() -> noop.setEjectedSubchannelCount(2));

        assertDoesNotThrow(() -> noop.setAdaptiveTuning("inflightWeightEff", 0.42));
        assertDoesNotThrow(() -> noop.setAdaptiveTuning(null, 0.0));

        assertDoesNotThrow(() -> noop.setMethodLatencyEwma("MyRpc", 10.0, 5.0));
        assertDoesNotThrow(() -> noop.setMethodLatencyEwma(null, 0.0, 0.0));

        assertDoesNotThrow(() -> noop.recordObservedRtt("MyRpc", 1_000_000L));
        assertDoesNotThrow(() -> noop.recordObservedRtt(null, 0L));
    }
}
