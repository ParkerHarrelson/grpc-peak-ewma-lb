package dev.parkerharrelson.grpc.peakewma;

import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.METHOD_A;
import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.METHOD_B;
import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.MS;
import static org.assertj.core.api.Assertions.assertThat;

import dev.parkerharrelson.grpc.peakewma.AdversarialFixture.Response;
import io.grpc.LoadBalancer.Subchannel;
import io.grpc.Status;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Discrete-event simulations through the real balancer, picker and stream tracers on a fake clock.
 * Deterministic, and minutes of traffic run in milliseconds.
 */
class AdversarialSimulationTest {

    private static final PeakEwmaConfig CFG = PeakEwmaConfig.DEFAULTS;

    private static AdversarialFixture fleet(int n, double latencyMs) {
        AdversarialFixture f = new AdversarialFixture(CFG);
        int[] ports = new int[n];
        for (int i = 0; i < n; i++) {
            ports[i] = 7000 + i;
            f.models.put(ports[i], AdversarialFixture.jittered(latencyMs, 0.1, i));
        }
        f.resolveAndReady(ports);
        return f;
    }

    @SuppressWarnings("unchecked")
    static MethodTable table(AdversarialFixture f, int port) {
        var tables =
                (java.util.Map<Subchannel, MethodTable>)
                        AdversarialFixture.getField(f.balancer, "tables");
        return tables.get(f.byPort.get(port));
    }

    /** The backend with the most samples for {@code method} — i.e. one that is definitely warm. */
    static int warmestPort(AdversarialFixture f, String method) {
        return f.byPort.keySet().stream()
                .max(
                        java.util.Comparator.comparingInt(
                                p -> table(f, p).statsFor(method).getSamples()))
                .orElseThrow();
    }

    private static String shares(AdversarialFixture f) {
        StringBuilder sb = new StringBuilder();
        f.served.keySet().stream()
                .sorted()
                .forEach(p -> sb.append(String.format("%d=%.1f%% ", p, 100 * f.share(p))));
        return sb.toString();
    }

    /**
     * Cold-start starvation inside a homogeneous fleet. Cold peers (fewer than minSamples, which is
     * up to 16) are scored on the slow EWMA, still ≈ the 50 ms seed; warm peers are scored on the
     * fast EWMA ≈ real latency. Whichever backends warm up first win every P2C comparison; the rest
     * never collect enough samples to warm up. Identical backends, identical traffic, yet some get
     * ~nothing — per method.
     */
    @Test
    void identicalBackends_allReceiveTraffic_forEveryMethod() {
        AdversarialFixture f = fleet(10, 5.0);
        f.run(1000, 30_000, METHOD_A, METHOD_B);
        StringBuilder report = new StringBuilder();
        int starved = 0;
        for (String m : new String[] {METHOD_A.getFullMethodName(), METHOD_B.getFullMethodName()}) {
            for (int p = 7000; p < 7010; p++) {
                int n = table(f, p).statsFor(m).getSamples();
                report.append(String.format("%s@%d=%d ", m, p, n));
                if (n < 30_000 * 1.0 / 2 / 10 * 0.25) starved++; // < 25 % of a fair share
            }
        }
        assertThat(starved)
                .as(
                        "(backend, method) pairs with <25%% of fair share on identical backends:"
                                + " %s",
                        report)
                .isZero();
    }

    /**
     * Scale-up: a new backend joins a warm fleet whose latency is below initialRttMicros (50 ms).
     * It is scored on its 50 ms seed (x2 warmup) and loses every P2C comparison, so it never gets a
     * sample and never leaves the seed: permanently starved. Autoscaling adds capacity that
     * receives no traffic.
     */
    @Test
    void newBackend_receivesTraffic_afterScaleUp() {
        AdversarialFixture f = fleet(3, 2.0);
        f.run(300, 30_000, METHOD_A);

        f.models.put(7003, AdversarialFixture.jittered(2.0, 0.1, 3));
        f.resolve(7000, 7001, 7002, 7003);
        f.drive(7003, io.grpc.ConnectivityState.READY);
        f.resetCounters();
        f.run(300, 60_000, METHOD_A);

        assertThat(f.share(7003))
                .as("share of the new (identical) backend over 60 s after joining: %s", shares(f))
                .isGreaterThan(0.10);
    }

    /**
     * A single slow response blacks out a backend. With 2 backends P2C always compares both, the
     * peak EWMA isn't decayed at read time, so the spiked backend gets zero traffic until its stats
     * go stale (staleMillisForRatio = 30 s) — halving capacity for half a minute.
     */
    @Test
    void singleSlowResponse_doesNotBlackOutBackend() {
        AdversarialFixture f = fleet(2, 5.0);
        f.run(200, 10_000, METHOD_A);

        AtomicBoolean spiked = new AtomicBoolean();
        var healthy = f.models.get(7000);
        f.models.put(
                7000,
                (t, m) ->
                        spiked.compareAndSet(false, true)
                                ? Response.ok(800 * MS)
                                : healthy.respond(t, m));
        f.run(200, 2_000, METHOD_A); // the spike lands and completes
        f.resetCounters();
        f.run(200, 10_000, METHOD_A);

        assertThat(f.share(7000))
                .as("share of backend 7000 in the 10 s after ONE 800 ms response: %s", shares(f))
                .isGreaterThan(0.25);
    }

    /**
     * Fast-failing backend ("black hole"). Errors are recorded with their (tiny) RTT, so a backend
     * that rejects instantly looks like the FASTEST backend and P2C sends it more traffic than
     * anyone else. With fewer than 5 backends ejection can't kick in (see AdversarialMathTest), so
     * nothing stops it. round_robin would send it 33 %.
     */
    @Test
    void fastFailingBackend_doesNotAttractTraffic() {
        AdversarialFixture f = fleet(3, 10.0);
        f.run(300, 10_000, METHOD_A);

        f.models.put(7000, (t, m) -> new Response(MS / 5, Status.UNAVAILABLE));
        f.resetCounters();
        f.run(300, 30_000, METHOD_A);

        double successRate = (double) f.totalOk() / f.totalServed();
        assertThat(f.share(7000))
                .as(
                        "share of the instantly-failing backend (%s) success=%.1f%%",
                        shares(f), 100 * successRate)
                .isLessThan(0.34);
    }

    /** Same black hole with 10 backends, where ejection is allowed. Still, how bad does it get? */
    @Test
    void fastFailingBackend_inLargeFleet_endToEndSuccessStaysHigh() {
        AdversarialFixture f = fleet(10, 10.0);
        f.run(500, 10_000, METHOD_A);

        f.models.put(7000, (t, m) -> new Response(MS / 5, Status.UNAVAILABLE));
        f.resetCounters();
        f.run(500, 60_000, METHOD_A);

        double successRate = (double) f.totalOk() / f.totalServed();
        assertThat(successRate)
                .as(
                        "end-to-end success with 1/10 instantly failing (%s) ejections=%s",
                        shares(f), f.metrics.ejections.size())
                .isGreaterThan(0.97);
    }

    /**
     * A broken backend in a 4-node fleet is never ejected (cap) even at 100 % errors, while the
     * README promises outlier ejection "if a backend crosses outlierErrorRate".
     */
    @Test
    @Tag("adversarial") // still failing: tracked issue open
    void erroringBackend_isEjected_inFourNodeFleet() {
        AdversarialFixture f = fleet(4, 10.0);
        f.run(400, 10_000, METHOD_A);
        // same latency as peers, so this is purely about error-rate ejection
        f.models.put(7000, (t, m) -> new Response(10 * MS, Status.INTERNAL));
        f.run(400, 30_000, METHOD_A);
        assertThat(f.metrics.ejections).as("ejections of a 100%%-error backend").isNotEmpty();
    }

    /**
     * False-positive ejection on a perfectly healthy, uniform fleet whose latency is well above the
     * 50 ms seed. The slow EWMA is pinned near the seed for ~20 s (see AdversarialMathTest), so
     * fast/slow > latencyMultiplierEff on EVERY backend and the detector ejects as many as its caps
     * allow.
     */
    @Test
    void uniformHealthyFleet_withHighLatency_hasNoEjections() {
        AdversarialFixture f = fleet(10, 200.0);
        f.run(200, 60_000, METHOD_A);
        assertThat(f.metrics.ejections)
                .as("ejections on 10 identical healthy 200 ms backends over 60 s")
                .isEmpty();
    }

    /**
     * One slow call on ONE method ejects the WHOLE backend, for every method, for 15 s. The
     * per-method design is defeated because tryEjectForMethod sets subchannelState.ejectUntil.
     */
    @Test
    @Tag("adversarial") // still failing: tracked issue open
    void singleSlowCallOnOneMethod_doesNotEjectBackendForOtherMethods() {
        AdversarialFixture f = fleet(10, 5.0);
        f.run(1000, 120_000, METHOD_A, METHOD_B); // long enough for the slow EWMA to converge
        f.metrics.ejections.clear();

        int target = warmestPort(f, METHOD_B.getFullMethodName());
        AtomicBoolean spiked = new AtomicBoolean();
        var healthy = f.models.get(target);
        f.models.put(
                target,
                (t, m) ->
                        m.equals(METHOD_B.getFullMethodName()) && spiked.compareAndSet(false, true)
                                ? Response.ok(
                                        60 * MS) // 12x a normal call; happens on any real service
                                : healthy.respond(t, m));
        f.run(1000, 2_000, METHOD_A, METHOD_B);

        assertThat(spiked).as("spike was served").isTrue();
        assertThat(f.metrics.ejections)
                .as("ejections caused by a single 60 ms call on svc/B")
                .isEmpty();
    }

    /**
     * Streaming RPCs: RTT is measured from streamCreated to streamClosed, i.e. the whole stream
     * lifetime. One 5-minute watch stream closing poisons that method's EWMA with 300 s and the
     * fast/slow ratio ejects the whole backend.
     */
    @Test
    @Tag("adversarial") // still failing: tracked issue open
    void longLivedStream_doesNotEjectBackend() {
        AdversarialFixture f = fleet(10, 5.0);
        f.run(2000, 10_000, METHOD_A, METHOD_B);
        int target = warmestPort(f, METHOD_B.getFullMethodName());
        io.grpc.LoadBalancer.PickResult watch = null;
        for (int i = 0; i < 10_000 && watch == null; i++) {
            var pr = f.pick(METHOD_B);
            if (AdversarialFixture.portOf(pr.getSubchannel()) == target) watch = pr;
        }
        assertThat(watch).as("got a pick on the warm backend").isNotNull();
        var tracer =
                watch.getStreamTracerFactory()
                        .newClientStreamTracer(
                                io.grpc.ClientStreamTracer.StreamInfo.newBuilder().build(),
                                new io.grpc.Metadata());
        tracer.streamCreated(io.grpc.Attributes.EMPTY, new io.grpc.Metadata());
        // Some short svc/B calls too, so the method has history on every backend.
        f.run(2000, 60_000, METHOD_A, METHOD_B);
        f.metrics.ejections.clear();
        tracer.streamClosed(Status.OK); // stream ends normally after ~60 s
        f.run(2000, 3_000, METHOD_A, METHOD_B);
        assertThat(f.metrics.ejections)
                .as("ejections after a normal 60 s server-stream closed")
                .isEmpty();
    }

    /**
     * Picker hot path materialises MethodStats + ErrorWindow for EVERY ready subchannel for every
     * pick (statsFor/windowFor are computeIfAbsent). Not a leak per se, but it means each pick
     * allocates per-backend state for methods the backend never served.
     */
    @Test
    @Tag("adversarial") // still failing: tracked issue open
    void pick_doesNotCreateStateOnUnpickedBackends() {
        AdversarialFixture f = fleet(50, 5.0);
        f.pick(AdversarialFixture.method("svc/NeverCalledBefore"));
        long created =
                f.byPort.values().stream()
                        .map(sc -> (Subchannel) sc)
                        .filter(
                                sc -> {
                                    @SuppressWarnings("unchecked")
                                    var tables =
                                            (java.util.Map<Subchannel, MethodTable>)
                                                    AdversarialFixture.getField(
                                                            f.balancer, "tables");
                                    return tables.get(sc)
                                            .methodKeys()
                                            .contains("svc/NeverCalledBefore");
                                })
                        .count();
        assertThat(created)
                .as("backends that got per-method state from ONE pick")
                .isLessThanOrEqualTo(2);
    }
}
