package dev.parkerharrelson.grpc.peakewma.harness.report;

import dev.parkerharrelson.grpc.peakewma.harness.workload.CallStats;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.Locale;
import java.util.Map;

/**
 * Formats a human-readable summary of a harness run and exports the meter registry as Prometheus
 * scrape text so scenarios can grep for specific values (e.g. {@code lb_pick_total}).
 */
public final class Reporter {

    private Reporter() {}

    /** Produces a multi-line text summary of call stats, with a per-backend breakdown. */
    public static String summarize(String scenarioName, String policy, CallStats stats) {
        StringBuilder sb = new StringBuilder();
        sb.append("==============================================================\n");
        sb.append("Scenario: ").append(scenarioName).append("\n");
        sb.append("Policy:   ").append(policy).append("\n");
        sb.append("--------------------------------------------------------------\n");

        CallStats.AggregateSummary agg = stats.aggregate();
        sb.append(
                String.format(
                        Locale.ROOT,
                        "Total: %d requests (%d ok, %d err, err-rate %.2f%%)%n",
                        agg.totalRequests(),
                        agg.totalSuccesses(),
                        agg.totalErrors(),
                        agg.totalRequests() == 0
                                ? 0.0
                                : 100.0 * agg.totalErrors() / agg.totalRequests()));
        sb.append(
                String.format(
                        Locale.ROOT,
                        "Aggregate RTT micros: p50=%d p95=%d p99=%d%n",
                        agg.p50Micros(),
                        agg.p95Micros(),
                        agg.p99Micros()));
        sb.append("--------------------------------------------------------------\n");
        sb.append("Per-backend share and latency:\n");

        Map<String, CallStats.BackendSummary> perBackend = stats.perBackend();
        for (Map.Entry<String, CallStats.BackendSummary> e : perBackend.entrySet()) {
            CallStats.BackendSummary bs = e.getValue();
            double share =
                    agg.totalRequests() == 0 ? 0.0 : (100.0 * bs.requests() / agg.totalRequests());
            sb.append(
                    String.format(
                            Locale.ROOT,
                            "  %-12s %6d req (%5.1f%% share, %5.1f%% err)  p50=%-6d p95=%-6d"
                                    + " p99=%-6d%n",
                            bs.id(),
                            bs.requests(),
                            share,
                            bs.requests() == 0 ? 0.0 : 100.0 * bs.errors() / bs.requests(),
                            bs.p50Micros(),
                            bs.p95Micros(),
                            bs.p99Micros()));
        }
        sb.append("==============================================================\n");
        return sb.toString();
    }

    /**
     * Dumps the registry as Prometheus scrape text. Delegates to {@link
     * PrometheusMeterRegistry#scrape()} rather than hand-rolling the format so the output matches
     * what a real scraper would receive (escaping rules, HELP/TYPE lines, histogram bucket layout).
     */
    public static String dumpMeters(PrometheusMeterRegistry registry) {
        return registry.scrape();
    }
}
