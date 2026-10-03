package dev.parkerharrelson.grpc.peakewma.harness;

import static org.assertj.core.api.Assertions.assertThat;

import dev.parkerharrelson.grpc.peakewma.harness.scenario.Scenario;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Smoke test: ensures the harness can bring up backends, open the channel, and drive a small
 * open-loop workload successfully end-to-end. Cheap and fast — gate for CI.
 */
class HarnessSmokeTest {

    @Test
    void end_to_end_workload_runs_and_records_successes() throws Exception {
        Scenario.Handle h = Scenario.start(3, "peak_ewma_p2c");
        try {
            Scenario.driveFor(h, 200, 1.0, Duration.ofSeconds(2));

            var agg = h.stats().aggregate();
            assertThat(agg.totalRequests()).isGreaterThan(100);
            assertThat(agg.totalSuccesses()).isGreaterThan(100);
            assertThat(agg.totalErrors()).isLessThan(agg.totalRequests() / 2);
        } finally {
            Scenario.stop(h);
        }
    }

    @Test
    void all_healthy_backends_see_traffic_under_peak_ewma() throws Exception {
        Scenario.Handle h = Scenario.start(4, "peak_ewma_p2c");
        try {
            Scenario.driveFor(h, 400, 1.0, Duration.ofSeconds(3));

            var per = h.stats().perBackend();
            // Every backend should have received at least a handful of requests.
            long minShare = per.values().stream().mapToLong(v -> v.requests()).min().orElse(0);
            assertThat(minShare)
                    .as("each backend should see some traffic with all-healthy workload")
                    .isGreaterThan(5);
        } finally {
            Scenario.stop(h);
        }
    }
}
