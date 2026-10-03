package dev.parkerharrelson.grpc.peakewma.metrics;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class LbMetricsTest {

    @Test
    void noopImplIsSingletonAndIsAnLbMetrics() {
        LbMetrics n = NoopLbMetrics.INSTANCE;
        assertNotNull(n);
        assertInstanceOf(LbMetrics.class, n);
    }
}
