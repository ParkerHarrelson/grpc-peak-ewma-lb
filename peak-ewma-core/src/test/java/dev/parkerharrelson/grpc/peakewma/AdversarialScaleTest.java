package dev.parkerharrelson.grpc.peakewma;

import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.METHOD_A;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Does one default configuration behave the same across workloads? The same failure (one of 10
 * backends becomes 3x slower, later recovers) is replayed at very different latency scales and
 * request rates. A scale-free policy gives similar results in every row; fixed wall-clock constants
 * (e.g. a 1 s decay half-life) do not.
 */
class AdversarialScaleTest {

    record Workload(String name, double latencyMs, double rps) {}

    record Result(Workload w, double brownoutShare, double recoverySec, double healthyShare) {}

    static final List<Workload> WORKLOADS =
            List.of(
                    new Workload("fast+hot   2 ms, 2000 rps", 2, 2000),
                    new Workload("fast+cold  2 ms,    5 rps", 2, 5),
                    new Workload("mid       20 ms,  200 rps", 20, 200),
                    new Workload("slow+hot 500 ms,  400 rps", 500, 400),
                    new Workload("slow+cold  1 s,   10 rps", 1000, 10));

    static Result run(Workload w, PeakEwmaConfig cfg) {
        AdversarialFixture f = new AdversarialFixture(cfg);
        int[] ports = new int[10];
        for (int i = 0; i < 10; i++) {
            ports[i] = 7100 + i;
            f.models.put(ports[i], AdversarialFixture.jittered(w.latencyMs, 0.1, i));
        }
        f.resolveAndReady(ports);
        // Scale the timeline to the workload: ~40 RTTs or 400 calls per backend, whichever longer.
        long phaseMs = (long) Math.max(40 * w.latencyMs, 400_000.0 / w.rps * 10 / 10);
        phaseMs = Math.max(phaseMs, 20_000);
        f.run(w.rps, phaseMs, METHOD_A); // warm

        var healthy = f.models.get(7100);
        f.models.put(7100, AdversarialFixture.jittered(3 * w.latencyMs, 0.1, 99));
        f.run(w.rps, phaseMs / 2, METHOD_A); // let it converge
        f.resetCounters();
        f.run(w.rps, phaseMs, METHOD_A);
        double brownout = f.share(7100);

        // Recovery: b0 is healthy again; how long until it gets >= 70% of a fair share (7%)?
        f.models.put(7100, healthy);
        double stepMs = Math.max(250, phaseMs / 80.0);
        double recovered = Double.NaN;
        for (double t = 0; t < 4 * phaseMs; t += stepMs) {
            f.resetCounters();
            f.run(w.rps, (long) stepMs, METHOD_A);
            if (f.totalServed() > 0 && f.share(7100) >= 0.07) {
                recovered = (t + stepMs) / 1000.0;
                break;
            }
        }
        f.resetCounters();
        f.run(w.rps, phaseMs, METHOD_A);
        return new Result(w, brownout, recovered, f.share(7100));
    }

    static String table(List<Result> rs) {
        StringBuilder sb = new StringBuilder();
        sb.append(
                String.format(
                        "%-28s %14s %16s %16s%n",
                        "workload", "3x-slow share", "recovery (s)", "healed share"));
        for (Result r : rs) {
            sb.append(
                    String.format(
                            "%-28s %13.1f%% %16s %15.1f%%%n",
                            r.w.name,
                            100 * r.brownoutShare,
                            Double.isNaN(r.recoverySec)
                                    ? "never"
                                    : String.format("%.1f", r.recoverySec),
                            100 * r.healthyShare));
        }
        return sb.toString();
    }

    @Test
    void sameFailure_sameBehaviour_acrossLatencyScalesAndRates() {
        List<Result> rs = new ArrayList<>();
        for (Workload w : WORKLOADS) rs.add(run(w, PeakEwmaConfig.DEFAULTS));
        System.out.println("[scale] default config (self-tuning)\n" + table(rs));
        for (Result r : rs) {
            assertThat(r.brownoutShare).as("3x-slow backend share, %s", r.w.name).isLessThan(0.03);
            assertThat(r.healthyShare).as("healed backend share, %s", r.w.name).isGreaterThan(0.06);
        }
    }
}
