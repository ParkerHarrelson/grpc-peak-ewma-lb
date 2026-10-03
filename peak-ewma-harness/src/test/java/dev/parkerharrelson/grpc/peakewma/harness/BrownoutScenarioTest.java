package dev.parkerharrelson.grpc.peakewma.harness;

import static org.assertj.core.api.Assertions.assertThat;

import dev.parkerharrelson.grpc.peakewma.harness.scenario.Scenario;
import dev.parkerharrelson.grpc.peakewma.harness.workload.CallStats;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Verifies that when one backend brown-outs (latency jumps ~10x), the Peak-EWMA load balancer
 * steers a strong majority of traffic away from it within the steady-state portion of the run.
 *
 * <p>Tagged {@code perf} because the share threshold depends on scheduler timing and is
 * occasionally sensitive to runner load. Excluded from the default Surefire build; runnable via
 * {@code mvn -pl internal/grpc-client-loadbalancer-harness test -Pperf}. The deterministic smoke
 * tests that CI depends on live in {@link HarnessSmokeTest} and {@code LoadBalancerVerifications}.
 */
@Tag("perf")
class BrownoutScenarioTest {

    @Test
    void peak_ewma_reduces_share_of_slow_backend_to_below_15pct() throws Exception {
        Scenario.Handle h = Scenario.start(4, "peak_ewma_p2c");
        try {
            // Warm the EWMAs with a healthy run — EVERY backend should look equally good.
            Scenario.driveFor(h, 400, 1.0, Duration.ofSeconds(2));

            // Brown out backend b0: 10x base latency, no errors.
            h.backends().get(0).behaviour().setSlow(Duration.ofMillis(100), 0.15);

            // Drive enough traffic that the outlier ticker and fast-EWMA both converge.
            CallStats during = new CallStats();
            Scenario.Handle under = new Scenario.Handle(h.backends(), h.harnessChannel(), during);
            Scenario.driveFor(under, 400, 1.0, Duration.ofSeconds(6));

            Map<String, CallStats.BackendSummary> per = during.perBackend();
            long total = during.aggregate().totalRequests();
            long brownoutShare = per.getOrDefault("b0", zero()).requests();
            double pct = total == 0 ? 0.0 : (100.0 * brownoutShare / total);

            assertThat(pct)
                    .as(
                            "peak_ewma_p2c should drop the brown-out backend's share "
                                    + "below 15%% after convergence (observed %.2f%%)",
                            pct)
                    .isLessThan(15.0);
        } finally {
            Scenario.stop(h);
        }
    }

    private static CallStats.BackendSummary zero() {
        return new CallStats.BackendSummary("b0", 0, 0, 0, 0, 0, 0);
    }
}
