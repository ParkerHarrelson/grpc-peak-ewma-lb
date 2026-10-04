package dev.parkerharrelson.grpc.peakewma.harness;

import static org.assertj.core.api.Assertions.assertThat;

import dev.parkerharrelson.grpc.peakewma.harness.scenario.Scenario;
import dev.parkerharrelson.grpc.peakewma.harness.workload.CallStats;
import java.time.Duration;
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
    void peak_ewma_isolates_errors_backend_and_preserves_end_to_end_success_rate()
            throws Exception {
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

            long[] before = served(h);
            CallStats during = new CallStats();
            Scenario.Handle under = new Scenario.Handle(h.backends(), h.harnessChannel(), during);
            Scenario.driveFor(under, 400, 1.0, Duration.ofSeconds(7));
            long[] after = served(h);

            // Attribute by SERVER-side counters: failed calls are trailers-only, so the client
            // never learns which backend served them (the old client-side share counted only
            // b0's successes).
            long total = 0;
            for (int i = 0; i < after.length; i++) total += after[i] - before[i];
            double badShare = total == 0 ? 0.0 : 100.0 * (after[0] - before[0]) / total;
            long requests = during.aggregate().totalRequests();
            double errRate =
                    requests == 0 ? 0.0 : (double) during.aggregate().totalErrors() / requests;
            double ejections =
                    h
                            .harnessChannel()
                            .meterRegistry()
                            .find("lb.outlier.ejections")
                            .counters()
                            .stream()
                            .mapToDouble(io.micrometer.core.instrument.Counter::count)
                            .sum();
            System.out.printf(
                    "[outage] b0 server share=%.1f%% client error rate=%.1f%% ejections=%.0f%n",
                    badShare, 100 * errRate, ejections);

            // round_robin would give b0 25%% of traffic and a 15%% end-to-end error rate
            // (60%% x 25%%). The LB must do much better, via ejection, the failure penalty, or
            // both.
            assertThat(errRate)
                    .as("end-to-end error rate (round_robin: 15%%; ejections=%.0f)", ejections)
                    .isLessThan(0.05);
            assertThat(badShare)
                    .as("server-side share of the 60%%-error backend (round_robin: 25%%)")
                    .isLessThan(10.0);
        } finally {
            Scenario.stop(h);
        }
    }

    private static long[] served(Scenario.Handle h) {
        long[] out = new long[h.backends().size()];
        for (int i = 0; i < out.length; i++) out[i] = h.backends().get(i).service().fastCalls();
        return out;
    }
}
