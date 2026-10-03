package dev.parkerharrelson.grpc.peakewma.harness.scenario;

import dev.parkerharrelson.grpc.peakewma.harness.report.Reporter;
import dev.parkerharrelson.grpc.peakewma.harness.server.BackendBehaviour;
import java.time.Duration;

/**
 * CLI entry point for the load balancer harness.
 *
 * <p>Usage: {@code java ... LoadBalancerHarnessMain [--scenario=<name>] [--backends=N] [--qps=N]
 * [--duration=Ns] [--policy=peak_ewma_p2c|round_robin]}.
 *
 * <p>Scenarios:
 *
 * <ul>
 *   <li>{@code baseline} — all backends healthy, no injection; sanity check
 *   <li>{@code brownout} — one backend slows to ~10x base latency mid-run
 *   <li>{@code outage} — one backend returns UNAVAILABLE mid-run, then recovers
 *   <li>{@code errors} — one backend returns 40% errors
 * </ul>
 */
@SuppressWarnings("java:S106")
public final class LoadBalancerHarnessMain {

    public static void main(String[] args) throws Exception {
        String scenario = arg(args, "--scenario", "baseline");
        int backends = Integer.parseInt(arg(args, "--backends", "5"));
        int qps = Integer.parseInt(arg(args, "--qps", "500"));
        String policy = arg(args, "--policy", "peak_ewma_p2c");
        int durationSeconds = Integer.parseInt(arg(args, "--duration", "15").replace("s", ""));
        double fastRatio = Double.parseDouble(arg(args, "--fastRatio", "0.7"));

        System.out.printf(
                "Running scenario=%s backends=%d qps=%d duration=%ds policy=%s fastRatio=%.2f%n",
                scenario, backends, qps, durationSeconds, policy, fastRatio);

        Scenario.Handle h = Scenario.start(backends, policy);
        try {
            runScenarioBody(scenario, h, qps, fastRatio, Duration.ofSeconds(durationSeconds));
        } finally {
            Scenario.stop(h);
        }

        System.out.println(Reporter.summarize(scenario, policy, h.stats()));
        System.out.println("--- selected meters ----------------------------------------------");
        // PrometheusMeterRegistry rewrites "." to "_" in metric names, so filter against the
        // scrape-text form rather than the Micrometer-side dotted names.
        String meters = Reporter.dumpMeters(h.harnessChannel().meterRegistry());
        meters.lines()
                .filter(
                        line ->
                                line.startsWith("lb_pick_total")
                                        || line.startsWith("lb_ejected_subchannels")
                                        || line.startsWith("lb_ready_subchannels")
                                        || line.startsWith("lb_outlier_ejections")
                                        || line.startsWith("lb_subchannel_inflight"))
                .forEach(System.out::println);
    }

    private static void runScenarioBody(
            String name, Scenario.Handle h, int qps, double fastRatio, Duration duration)
            throws InterruptedException {
        // Start backends healthy.
        for (var b : h.backends()) {
            b.behaviour().setHealthy(Duration.ofMillis(10), 0.2);
        }

        switch (name) {
            case "baseline" -> Scenario.driveFor(h, qps, fastRatio, duration);

            case "brownout" -> {
                // 1/3 of the run healthy, then one backend slows 10x, then healthy again.
                long slice = duration.toMillis() / 3;
                Scenario.driveFor(h, qps, fastRatio, Duration.ofMillis(slice));
                h.backends().get(0).behaviour().setSlow(Duration.ofMillis(100), 0.15);
                Scenario.driveFor(h, qps, fastRatio, Duration.ofMillis(slice));
                h.backends().get(0).behaviour().setHealthy(Duration.ofMillis(10), 0.2);
                Scenario.driveFor(h, qps, fastRatio, Duration.ofMillis(slice));
            }

            case "outage" -> {
                long slice = duration.toMillis() / 3;
                Scenario.driveFor(h, qps, fastRatio, Duration.ofMillis(slice));
                h.backends().get(0).behaviour().setOutage();
                Scenario.driveFor(h, qps, fastRatio, Duration.ofMillis(slice));
                h.backends().get(0).behaviour().setHealthy(Duration.ofMillis(10), 0.2);
                Scenario.driveFor(h, qps, fastRatio, Duration.ofMillis(slice));
            }

            case "errors" -> {
                long slice = duration.toMillis() / 2;
                Scenario.driveFor(h, qps, fastRatio, Duration.ofMillis(slice));
                h.backends()
                        .get(0)
                        .behaviour()
                        .set(
                                new BackendBehaviour.Snapshot(
                                        Duration.ofMillis(10),
                                        0.2,
                                        0.40,
                                        false,
                                        Duration.ZERO,
                                        0.0));
                Scenario.driveFor(h, qps, fastRatio, Duration.ofMillis(slice));
            }

            default -> throw new IllegalArgumentException("unknown scenario: " + name);
        }
    }

    private static String arg(String[] args, String key, String dflt) {
        for (String a : args) {
            if (a.startsWith(key + "=")) return a.substring(key.length() + 1);
        }
        return dflt;
    }

    private LoadBalancerHarnessMain() {}
}
