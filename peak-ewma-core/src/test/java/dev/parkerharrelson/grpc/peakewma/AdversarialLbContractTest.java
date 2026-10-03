package dev.parkerharrelson.grpc.peakewma;

import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.METHOD_A;
import static org.assertj.core.api.Assertions.assertThat;

import dev.parkerharrelson.grpc.peakewma.AdversarialFixture.FakeSubchannel;
import io.grpc.ConnectivityState;
import io.grpc.ConnectivityStateInfo;
import io.grpc.LoadBalancer.PickResult;
import io.grpc.Status;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Checks the balancer against grpc-java's LoadBalancer contract — the rules every built-in policy
 * (pick_first, round_robin, least_request, ...) follows. Each test asserts the CORRECT behaviour; a
 * failure means the bug is present.
 */
@Tag("adversarial")
class AdversarialLbContractTest {

    private static final PeakEwmaConfig CFG = PeakEwmaConfig.DEFAULTS;

    /**
     * A transient resolver error while subchannels are READY should not take the channel down — and
     * must certainly be undone once the resolver recovers. The balancer publishes an error picker,
     * then on recovery sees "ready hash unchanged" and never republishes the READY picker, so every
     * RPC fails until the ready set happens to change.
     */
    @Test
    void resolverError_thenRecovery_restoresReadyPicker() {
        AdversarialFixture f = new AdversarialFixture(CFG);
        f.resolveAndReady(5001, 5002);
        assertThat(f.latest().state()).isEqualTo(ConnectivityState.READY);

        f.nameResolutionError(Status.UNAVAILABLE.withDescription("dns blip"));

        // Resolver recovers with the exact same addresses, and an outlier tick runs.
        f.resolve(5001, 5002);
        f.advanceMs(1000);
        f.tick();

        PickResult pr = f.pick(METHOD_A);
        assertThat(f.latest().state())
                .as("channel state after resolver recovered")
                .isEqualTo(ConnectivityState.READY);
        assertThat(pr.getSubchannel())
                .as("pick after resolver recovered (status=%s)", pr.getStatus())
                .isNotNull();
    }

    /** Same thing but without the recovery step: a DNS blip should not fail RPCs at all. */
    @Test
    void resolverError_whileReady_keepsServing() {
        AdversarialFixture f = new AdversarialFixture(CFG);
        f.resolveAndReady(5001, 5002);
        f.nameResolutionError(Status.UNAVAILABLE.withDescription("dns blip"));
        PickResult pr = f.pick(METHOD_A);
        assertThat(pr.getSubchannel())
                .as("pick after transient resolver error (status=%s)", pr.getStatus())
                .isNotNull();
    }

    /**
     * Since grpc-java 1.4x subchannels do not reconnect on their own: when a connection drops
     * (server GOAWAY, max-connection-age, idle timeout) the subchannel goes IDLE and the LB must
     * call requestConnection(). This balancer only reconnects when zero subchannels are READY, so
     * the fleet silently shrinks one backend at a time.
     */
    @Test
    void idleSubchannel_isReconnected_whileOthersAreReady() {
        AdversarialFixture f = new AdversarialFixture(CFG);
        f.resolveAndReady(5001, 5002, 5003);
        FakeSubchannel a = f.byPort.get(5001);
        int before = a.requestConnections.get();

        f.drive(5001, ConnectivityState.IDLE); // e.g. server sent GOAWAY (max connection age)
        for (int i = 0; i < 5; i++) {
            f.advanceMs(1000);
            f.tick();
        }

        assertThat(a.requestConnections.get())
                .as("requestConnection() calls after subchannel went IDLE")
                .isGreaterThan(before);
    }

    /**
     * When every subchannel is in TRANSIENT_FAILURE the policy must report TRANSIENT_FAILURE with a
     * picker that fails non-wait-for-ready RPCs. Reporting CONNECTING + a no-result picker makes
     * every RPC queue until its deadline — or forever if the caller set none.
     */
    @Test
    void allSubchannelsTransientFailure_reportsTransientFailure_andFailsFast() {
        AdversarialFixture f = new AdversarialFixture(CFG);
        f.resolve(5001, 5002);
        f.helper.syncCtx.execute(
                () -> {
                    for (int p : new int[] {5001, 5002}) {
                        f.byPort
                                .get(p)
                                .deliver(
                                        ConnectivityStateInfo.forTransientFailure(
                                                Status.UNAVAILABLE.withDescription(
                                                        "connection refused")));
                    }
                });
        f.advanceMs(1000);
        f.tick();

        assertThat(f.latest().state()).isEqualTo(ConnectivityState.TRANSIENT_FAILURE);
        PickResult pr = f.pick(METHOD_A);
        assertThat(pr.getStatus().isOk())
                .as("pick should carry an UNAVAILABLE error, not 'no result' (queue forever)")
                .isFalse();
    }

    /**
     * An empty resolution must be rejected (non-OK status, so the channel re-resolves) and, when
     * nothing is READY, surface as TRANSIENT_FAILURE rather than CONNECTING forever. (Same as
     * grpc's MultiChildLoadBalancer: while READY, existing subchannels keep serving.)
     */
    @Test
    void emptyAddressList_isRejected_andReportsTransientFailure() {
        AdversarialFixture f = new AdversarialFixture(CFG);
        Status[] result = new Status[1];
        f.helper.syncCtx.execute(
                () ->
                        result[0] =
                                f.balancer.acceptResolvedAddresses(
                                        io.grpc.LoadBalancer.ResolvedAddresses.newBuilder()
                                                .setAddresses(java.util.List.of())
                                                .build()));
        assertThat(result[0].isOk()).as("acceptResolvedAddresses([]) status").isFalse();
        assertThat(f.latest().state()).isEqualTo(ConnectivityState.TRANSIENT_FAILURE);
    }

    /**
     * The outlier ticker runs on helper.getScheduledExecutorService(), not the sync context, but
     * calls publishPicker() which can call Subchannel.requestConnection(). In real grpc-java that
     * throws IllegalStateException("Not called from the SynchronizationContext"). We make the
     * window deterministic: the ready set collapses between the tick's ready snapshot and its
     * publishPicker() call.
     */
    @Test
    void outlierTick_neverTouchesSubchannelsOffSyncContext() {
        AdversarialFixture[] holder = new AdversarialFixture[1];
        AdversarialFixture.RecordingMetrics racing =
                new AdversarialFixture.RecordingMetrics() {
                    boolean fired;

                    @Override
                    public void setReadySubchannelCount(int readyCount) {
                        // Called by outlierTick right before publishPicker().
                        if (fired) return;
                        fired = true;
                        AdversarialFixture fx = holder[0];
                        fx.drive(5001, ConnectivityState.TRANSIENT_FAILURE);
                        fx.drive(5002, ConnectivityState.TRANSIENT_FAILURE);
                        fx.advanceMs(2000); // past the 1s reconnect debounce
                    }
                };
        AdversarialFixture f = new AdversarialFixture(CFG, racing);
        holder[0] = f;
        f.resolveAndReady(5001, 5002);
        f.advanceMs(1000);
        f.tick();

        int violations = f.byPort.values().stream().mapToInt(s -> s.syncCtxViolations.get()).sum();
        assertThat(violations)
                .as("Subchannel calls made off the SynchronizationContext by the outlier tick")
                .isZero();
    }

    /**
     * After a subchannel is removed gRPC still delivers its SHUTDOWN state to the listener. The
     * listener does subchannelConn.put(...) unconditionally, re-inserting the dead subchannel. With
     * pod churn (rolling deploys, autoscaling) the map grows forever and pins every dead Subchannel
     * (and its transport graph) in memory.
     */
    @Test
    void removedSubchannels_areNotRetained() {
        AdversarialFixture f = new AdversarialFixture(CFG);
        int churn = 2_000;
        for (int i = 0; i < churn; i++) {
            int port = 10_000 + i;
            int prev = port - 1;
            f.resolveAndReady(port);
            if (i > 0) {
                FakeSubchannel dead = f.byPort.get(prev);
                f.helper.syncCtx.execute(
                        () ->
                                dead.deliver(
                                        ConnectivityStateInfo.forNonError(
                                                ConnectivityState.SHUTDOWN)));
            }
        }
        Map<?, ?> conn = (Map<?, ?>) AdversarialFixture.getField(f.balancer, "subchannelConn");
        assertThat(conn.size())
                .as("subchannelConn entries after %d pod replacements (1 live)", churn)
                .isLessThanOrEqualTo(1);
    }

    /**
     * ensureOutlierTicker() cancels and re-schedules the ticker on EVERY resolver update with a
     * full initial delay. A resolver that pushes updates faster than the tick interval (xDS/EDS
     * churn, a chatty custom resolver) starves the ticker: no ejection, no pruning, ever.
     */
    @Test
    void frequentResolverUpdates_doNotStarveOutlierTicker() {
        AdversarialFixture f = new AdversarialFixture(CFG);
        f.resolveAndReady(5001, 5002);
        int scheduled = f.helper.scheduler.scheduleCount.get();
        for (int i = 0; i < 100; i++) f.resolve(5001, 5002); // identical updates
        assertThat(f.helper.scheduler.scheduleCount.get() - scheduled)
                .as("times the outlier ticker was cancelled + rescheduled by no-op updates")
                .isZero();
    }
}
