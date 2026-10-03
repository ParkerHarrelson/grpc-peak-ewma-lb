package dev.parkerharrelson.grpc.peakewma.harness;

import static org.assertj.core.api.Assertions.assertThat;

import dev.parkerharrelson.grpc.peakewma.harness.scenario.Scenario;
import dev.parkerharrelson.grpc.peakewma.harness.workload.CallStats;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Verifies outlier ejection: when one backend returns UNAVAILABLE/INTERNAL at high rate, the
 * Peak-EWMA load balancer ejects it and the remaining backends absorb the traffic.
 *
 * <p>Tagged {@code perf} for the same reason as {@link BrownoutScenarioTest}: the end-to-end
 * error-rate and share thresholds depend on the outlier ticker firing within the test window, which
 * is sensitive to scheduler pressure on shared runners.
 */
@Tag("perf")
class OutageScenarioTest {

    @Test
    void peak_ewma_ejects_errors_backend_and_preserves_end_to_end_success_rate() throws Exception {
        Scenario.Handle h = Scenario.start(4, "peak_ewma_p2c");
        try {
            // Warm baseline.
            Scenario.driveFor(h, 300, 1.0, Duration.ofSeconds(2));

            // Backend b0 returns 60% errors — well above the default 20% outlier threshold.
            h.backends()
                    .get(0)
                    .behaviour()
                    .set(
                            new dev.parkerharrelson.grpc.peakewma.harness.server.BackendBehaviour
                                    .Snapshot(
                                    Duration.ofMillis(10), 0.2, 0.60, false, Duration.ZERO, 0.0));

            CallStats during = new CallStats();
            Scenario.Handle under = new Scenario.Handle(h.backends(), h.harnessChannel(), during);
            Scenario.driveFor(under, 400, 1.0, Duration.ofSeconds(7));

            Map<String, CallStats.BackendSummary> per = during.perBackend();
            long total = during.aggregate().totalRequests();

            // End-to-end error rate should be significantly below the 60% at-source rate,
            // because the LB ejects the bad backend within a few outlier ticks.
            double errRate = total == 0 ? 0.0 : (double) during.aggregate().totalErrors() / total;
            assertThat(errRate)
                    .as(
                            "end-to-end error rate should be well below the 60%% bad-backend"
                                    + " rate (observed %.3f)",
                            errRate)
                    .isLessThan(0.25);

            // The bad backend should have received a minority of the total requests over
            // the full run (it gets ejected pretty quickly).
            double badShare =
                    total == 0 ? 0.0 : 100.0 * per.getOrDefault("b0", zero()).requests() / total;
            assertThat(badShare)
                    .as("errors-only backend share after ejection (observed %.2f%%)", badShare)
                    .isLessThan(30.0);
        } finally {
            Scenario.stop(h);
        }
    }

    private static CallStats.BackendSummary zero() {
        return new CallStats.BackendSummary("b0", 0, 0, 0, 0, 0, 0);
    }
}
