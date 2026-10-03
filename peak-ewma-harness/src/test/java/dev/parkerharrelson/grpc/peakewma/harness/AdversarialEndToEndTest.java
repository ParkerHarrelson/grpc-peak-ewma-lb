package dev.parkerharrelson.grpc.peakewma.harness;

import static org.assertj.core.api.Assertions.assertThat;

import dev.parkerharrelson.grpc.peakewma.harness.channel.HarnessChannel;
import dev.parkerharrelson.grpc.peakewma.harness.channel.StaticAddressesNameResolverProvider;
import dev.parkerharrelson.grpc.peakewma.harness.server.BackendBehaviour;
import dev.parkerharrelson.grpc.peakewma.harness.server.HarnessBackend;
import dev.parkerharrelson.grpc.peakewma.harness.server.InjectableService;
import dev.parkerharrelson.grpc.peakewma.harness.workload.CallStats;
import dev.parkerharrelson.grpc.peakewma.harness.workload.WorkloadDriver;
import io.grpc.CallOptions;
import io.grpc.ConnectivityState;
import io.grpc.EquivalentAddressGroup;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.micrometer.core.instrument.Counter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * End-to-end adversarial scenarios over real Netty sockets. Every scenario runs against both {@code
 * peak_ewma_p2c} and grpc's built-in {@code round_robin}; the assertion is on peak_ewma_p2c, and
 * the round_robin numbers are printed as the baseline.
 *
 * <p>Run: {@code ./mvnw -pl peak-ewma-harness -am test -Padversarial}
 */
@Tag("adversarial")
class AdversarialEndToEndTest {

    private static final String PEAK = "peak_ewma_p2c";
    private static final String RR = "round_robin";

    /** N backends + one channel. */
    static final class Rig implements AutoCloseable {
        final List<HarnessBackend> backends = new ArrayList<>();
        final HarnessChannel ch;

        Rig(String policy, int n, UnaryOperator<NettyServerBuilder> customizer) throws Exception {
            this(
                    policy,
                    n,
                    (java.util.function.IntFunction<UnaryOperator<NettyServerBuilder>>)
                            i -> customizer);
        }

        static Rig perBackend(
                String policy,
                int n,
                java.util.function.IntFunction<UnaryOperator<NettyServerBuilder>> f)
                throws Exception {
            return new Rig(policy, n, f);
        }

        private Rig(
                String policy,
                int n,
                java.util.function.IntFunction<UnaryOperator<NettyServerBuilder>> perBackend)
                throws Exception {
            for (int i = 0; i < n; i++) {
                backends.add(
                        new HarnessBackend("b" + i, new BackendBehaviour(), perBackend.apply(i)));
            }
            ch = new HarnessChannel(backends, policy, HarnessChannel.newPrometheusRegistry());
            awaitReady(ch.channel(), 10_000);
        }

        Rig(String policy, int n) throws Exception {
            this(policy, n, UnaryOperator.<NettyServerBuilder>identity());
        }

        ManagedChannel channel() {
            return ch.channel();
        }

        void addBackend(HarnessBackend b) {
            backends.add(b);
            List<EquivalentAddressGroup> eags = new ArrayList<>();
            for (HarnessBackend x : backends) eags.add(new EquivalentAddressGroup(x.address()));
            StaticAddressesNameResolverProvider.register(ch.resolverName(), eags);
        }

        CallStats drive(int qps, Duration d) throws InterruptedException {
            CallStats stats = new CallStats();
            Map<Integer, String> portToId = new HashMap<>();
            for (HarnessBackend b : backends) portToId.put(b.port(), b.id());
            try (WorkloadDriver w = new WorkloadDriver(ch.channel(), stats, 1.0, portToId)) {
                w.run(qps, d);
            }
            return stats;
        }

        long[] served() {
            long[] out = new long[backends.size()];
            for (int i = 0; i < out.length; i++) out[i] = backends.get(i).service().fastCalls();
            return out;
        }

        double ejections() {
            return ch.meterRegistry().find("lb.outlier.ejections").counters().stream()
                    .mapToDouble(Counter::count)
                    .sum();
        }

        @Override
        public void close() {
            ch.close();
            for (HarnessBackend b : backends) b.close();
        }
    }

    static void awaitReady(ManagedChannel ch, long ms) throws InterruptedException {
        long end = System.currentTimeMillis() + ms;
        while (ch.getState(true) != ConnectivityState.READY && System.currentTimeMillis() < end) {
            Thread.sleep(20);
        }
    }

    static long[] delta(long[] after, long[] before) {
        long[] d = new long[after.length];
        for (int i = 0; i < d.length; i++) d[i] = after[i] - (i < before.length ? before[i] : 0);
        return d;
    }

    static double share(long[] d, int i) {
        long t = 0;
        for (long x : d) t += x;
        return t == 0 ? 0 : (double) d[i] / t;
    }

    static String fmt(long[] d) {
        long t = 0;
        for (long x : d) t += x;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < d.length; i++)
            sb.append(String.format("b%d=%.1f%% ", i, t == 0 ? 0 : 100.0 * d[i] / t));
        return sb.toString().trim();
    }

    static byte[] unary(ManagedChannel ch, long deadlineMs) {
        return ClientCalls.blockingUnaryCall(
                ch,
                InjectableService.fastDescriptor(),
                CallOptions.DEFAULT.withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS),
                new byte[] {1});
    }

    // ------------------------------------------------------------------------------------------

    /**
     * One backend's server has maxConnectionAge (how you rebalance long-lived gRPC connections
     * behind k8s Services / L4 LBs; in a real fleet connection ages are never in sync). After its
     * GOAWAY the subchannel goes IDLE and the LB must call requestConnection(). peak_ewma only
     * reconnects when ZERO subchannels are READY, so that backend drops out permanently.
     */
    @Test
    void maxConnectionAge_goaway_backendKeepsReceivingTraffic() throws Exception {
        Map<String, Double> b0Share = new HashMap<>();
        for (String policy : List.of(RR, PEAK)) {
            UnaryOperator<NettyServerBuilder> plain = b -> b;
            UnaryOperator<NettyServerBuilder> aging =
                    b ->
                            b.maxConnectionAge(2, TimeUnit.SECONDS)
                                    .maxConnectionAgeGrace(500, TimeUnit.MILLISECONDS);
            try (Rig rig = Rig.perBackend(policy, 4, i -> i == 0 ? aging : plain)) {
                rig.drive(300, Duration.ofSeconds(4)); // b0's first connection ages out in here
                long[] before = rig.served();
                rig.drive(300, Duration.ofSeconds(10));
                long[] d = delta(rig.served(), before);
                b0Share.put(policy, share(d, 0));
                System.out.printf(
                        "[e2e goaway] %-14s share over 10 s after b0's first GOAWAY: %s%n",
                        policy, fmt(d));
            }
        }
        assertThat(b0Share.get(PEAK))
                .as(
                        "share of the backend that periodically GOAWAYs (round_robin: %.1f%%)",
                        100 * b0Share.get(RR))
                .isGreaterThan(0.15);
    }

    /**
     * All backends down. A non-wait-for-ready RPC must fail fast with UNAVAILABLE (that is what
     * TRANSIENT_FAILURE is for). peak_ewma reports CONNECTING forever, so the RPC waits for its
     * full deadline — and an RPC without a deadline hangs indefinitely.
     */
    @Test
    void allBackendsDown_rpcFailsFast() throws Exception {
        Map<String, String> outcome = new HashMap<>();
        Map<String, Long> elapsed = new HashMap<>();
        for (String policy : List.of(RR, PEAK)) {
            try (Rig rig = new Rig(policy, 2)) {
                unary(rig.channel(), 2_000);
                for (HarnessBackend b : rig.backends) b.close();
                Thread.sleep(3_000); // let the channel notice: connections drop, reconnects fail
                long t0 = System.nanoTime();
                Status.Code code;
                try {
                    unary(rig.channel(), 10_000);
                    code = Status.Code.OK;
                } catch (StatusRuntimeException e) {
                    code = e.getStatus().getCode();
                }
                long ms = (System.nanoTime() - t0) / 1_000_000;
                outcome.put(policy, code.name());
                elapsed.put(policy, ms);
                System.out.printf("[e2e all-down] %-14s -> %s after %d ms%n", policy, code, ms);
            }
        }
        assertThat(elapsed.get(PEAK))
                .as(
                        "time to fail an RPC with every backend down (round_robin: %s in %d ms;"
                                + " peak: %s)",
                        outcome.get(RR), elapsed.get(RR), outcome.get(PEAK))
                .isLessThan(5_000);
    }

    /**
     * Resolver hiccup: one failed resolution while every backend is healthy, then the resolver
     * recovers with the same addresses.
     */
    @Test
    void resolverError_thenRecovery_rpcsSucceed() throws Exception {
        Map<String, Integer> ok = new HashMap<>();
        for (String policy : List.of(RR, PEAK)) {
            try (Rig rig = new Rig(policy, 3)) {
                unary(rig.channel(), 2_000);
                StaticAddressesNameResolverProvider.fail(
                        rig.ch.resolverName(), Status.UNAVAILABLE.withDescription("DNS SERVFAIL"));
                Thread.sleep(200);
                StaticAddressesNameResolverProvider.refresh(rig.ch.resolverName()); // recovered
                Thread.sleep(2_000);
                int succeeded = 0;
                Status last = Status.OK;
                for (int i = 0; i < 50; i++) {
                    try {
                        unary(rig.channel(), 1_000);
                        succeeded++;
                    } catch (StatusRuntimeException e) {
                        last = e.getStatus();
                    }
                }
                ok.put(policy, succeeded);
                System.out.printf(
                        "[e2e resolver-blip] %-14s %d/50 RPCs ok after recovery (last error: %s)%n",
                        policy, succeeded, last);
            }
        }
        assertThat(ok.get(PEAK))
                .as("successful RPCs after resolver recovered (round_robin: %d/50)", ok.get(RR))
                .isEqualTo(50);
    }

    /**
     * One backend fails every RPC instantly (crash-looping app behind a healthy listener, bad
     * deploy, auth misconfig). round_robin gives it 1/3 of traffic. A latency-aware LB should do
     * better; peak_ewma does WORSE because the instant failures make it look fastest.
     */
    @Test
    void instantlyFailingBackend_doesNotAttractTraffic() throws Exception {
        Map<String, Double> errRate = new HashMap<>();
        Map<String, Double> badShare = new HashMap<>();
        for (String policy : List.of(RR, PEAK)) {
            try (Rig rig = new Rig(policy, 3)) {
                rig.drive(300, Duration.ofSeconds(3));
                rig.backends.get(0).behaviour().setOutage();
                long[] before = rig.served();
                CallStats s = rig.drive(300, Duration.ofSeconds(10));
                long[] d = delta(rig.served(), before);
                double er =
                        (double) s.aggregate().totalErrors()
                                / Math.max(1, s.aggregate().totalRequests());
                errRate.put(policy, er);
                badShare.put(policy, share(d, 0));
                System.out.printf(
                        "[e2e black-hole] %-14s server share: %s  client error rate: %.1f%%%n",
                        policy, fmt(d), 100 * er);
            }
        }
        assertThat(errRate.get(PEAK))
                .as(
                        "client error rate with 1/3 backends failing instantly (round_robin:"
                                + " %.1f%%)",
                        100 * errRate.get(RR))
                .isLessThanOrEqualTo(errRate.get(RR));
    }

    /** Autoscaling adds a 4th identical backend to a warm fleet. Does it get any traffic? */
    @Test
    void scaleUp_newBackendReceivesTraffic() throws Exception {
        Map<String, Double> newShare = new HashMap<>();
        for (String policy : List.of(RR, PEAK)) {
            try (Rig rig = new Rig(policy, 3)) {
                for (HarnessBackend b : rig.backends)
                    b.behaviour().setHealthy(Duration.ofMillis(3), 0.1);
                rig.drive(300, Duration.ofSeconds(5));
                HarnessBackend fresh = new HarnessBackend("b3");
                fresh.behaviour().setHealthy(Duration.ofMillis(3), 0.1);
                rig.addBackend(fresh);
                long[] before = rig.served();
                rig.drive(300, Duration.ofSeconds(15));
                long[] d = delta(rig.served(), before);
                newShare.put(policy, share(d, 3));
                System.out.printf(
                        "[e2e scale-up] %-14s 15 s after adding b3: %s%n", policy, fmt(d));
            }
        }
        assertThat(newShare.get(PEAK))
                .as(
                        "share of the newly added backend (round_robin: %.1f%%)",
                        100 * newShare.get(RR))
                .isGreaterThan(0.10);
    }

    /** Ten identical, healthy, 200 ms backends. Nothing should ever be ejected. */
    @Test
    void uniformHealthySlowService_noEjections() throws Exception {
        try (Rig rig = new Rig(PEAK, 10)) {
            for (HarnessBackend b : rig.backends)
                b.behaviour().setHealthy(Duration.ofMillis(200), 0.05);
            long[] before = rig.served();
            rig.drive(200, Duration.ofSeconds(25));
            long[] d = delta(rig.served(), before);
            System.out.printf(
                    "[e2e false-eject] peak_ewma_p2c ejections=%.0f share: %s%n",
                    rig.ejections(), fmt(d));
            assertThat(rig.ejections()).as("outlier ejections on a healthy uniform fleet").isZero();
        }
    }
}
