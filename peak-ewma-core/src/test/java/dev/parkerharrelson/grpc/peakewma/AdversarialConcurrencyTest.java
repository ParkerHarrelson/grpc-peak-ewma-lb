package dev.parkerharrelson.grpc.peakewma;

import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.METHOD_A;
import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.METHOD_B;
import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.MS;
import static dev.parkerharrelson.grpc.peakewma.AdversarialFixture.T0;
import static org.assertj.core.api.Assertions.assertThat;

import dev.parkerharrelson.grpc.peakewma.outlier.ErrorWindow;
import io.grpc.Attributes;
import io.grpc.ClientStreamTracer;
import io.grpc.ConnectivityState;
import io.grpc.LoadBalancer.PickResult;
import io.grpc.LoadBalancer.Subchannel;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** Multi-threaded stress against the real balancer and its shared mutable state. */
class AdversarialConcurrencyTest {

    private static final PeakEwmaConfig CFG = PeakEwmaConfig.DEFAULTS;

    /**
     * Ticks enqueue onto the sync context. A ticker spinning with no pause can enqueue faster than
     * the draining thread runs them, so the drain never returns (the real scheduler fires once per
     * tick interval). A short pause keeps the race window busy without that livelock.
     */
    private static void pace() {
        java.util.concurrent.locks.LockSupport.parkNanos(20_000);
    }

    @SuppressWarnings("unchecked")
    private static Set<Integer> actuallyReady(AdversarialFixture f) {
        Map<Subchannel, ConnectivityState> conn =
                (Map<Subchannel, ConnectivityState>)
                        AdversarialFixture.getField(f.balancer, "subchannelConn");
        Map<Subchannel, MethodTable> tables =
                (Map<Subchannel, MethodTable>) AdversarialFixture.getField(f.balancer, "tables");
        return tables.keySet().stream()
                .filter(sc -> conn.get(sc) == ConnectivityState.READY)
                .map(AdversarialFixture::portOf)
                .collect(Collectors.toSet());
    }

    private static Set<Integer> publishedReady(AdversarialFixture f) {
        return AdversarialFixture.readyPoolOf(f.latest().picker()).stream()
                .map(AdversarialFixture::portOf)
                .collect(Collectors.toSet());
    }

    /**
     * publishPicker() runs both on the sync context (state changes) and on the scheduler thread
     * (outlier tick). Its check-then-act on lastReadyHash and the enqueue of the new picker are not
     * atomic, so a tick can enqueue a picker built from a stale ready set AFTER the sync context
     * published the fresh one — while lastReadyHash already says "up to date". Every later publish
     * then short-circuits on the matching hash and the stale picker lives on: a READY backend gets
     * no traffic, or a dead one keeps being picked.
     */
    @Test
    void outlierTickRacingStateChanges_neverLeavesStalePickerPublished() throws Exception {
        int trials = 300;
        int permanentStale = 0;
        int transientStale = 0;
        int violations = 0;
        for (int trial = 0; trial < trials; trial++) {
            AdversarialFixture f = new AdversarialFixture(CFG);
            f.resolveAndReady(8001, 8002, 8003, 8004);

            AtomicBoolean stop = new AtomicBoolean();
            CyclicBarrier go = new CyclicBarrier(2);
            Thread ticker =
                    new Thread(
                            () -> {
                                try {
                                    go.await();
                                } catch (Exception e) {
                                    return;
                                }
                                while (!stop.get()) {
                                    f.tick();
                                    pace();
                                }
                            });
            ticker.start();
            go.await();
            for (int i = 0; i < 400; i++) {
                f.drive(
                        8004,
                        (i & 1) == 0
                                ? ConnectivityState.TRANSIENT_FAILURE
                                : ConnectivityState.READY);
                f.drive(
                        8003,
                        (i & 1) == 0 ? ConnectivityState.READY : ConnectivityState.CONNECTING);
            }
            stop.set(true);
            ticker.join();

            violations += f.byPort.values().stream().mapToInt(s -> s.syncCtxViolations.get()).sum();
            if (!publishedReady(f).equals(actuallyReady(f))) {
                transientStale++;
                f.tick(); // a clean, un-raced tick: does the balancer notice?
                if (!publishedReady(f).equals(actuallyReady(f))) permanentStale++;
            }
        }
        System.out.printf(
                "[race] trials=%d staleAfterQuiesce=%d staleEvenAfterCleanTick=%d offCtxCalls=%d%n",
                trials, transientStale, permanentStale, violations);
        assertThat(transientStale)
                .as("trials where the published picker disagreed with the real READY set")
                .isZero();
    }

    /**
     * Deterministic version of the race above. Interleaving (every step is something the real
     * sync-context thread can do while the scheduler thread is inside outlierTick):
     *
     * <ol>
     *   <li>sync ctx: backend 4 reports READY — subchannelConn updated, publishPicker not yet run
     *   <li>tick: sees {1,2,3,4}, builds picker P4, writes lastReadyHash = H4
     *   <li>sync ctx: finishes the READY handling (hash H4 == lastReadyHash → no publish), then
     *       backend 4 drops to TRANSIENT_FAILURE → publishes P3, lastReadyHash = H3
     *   <li>tick: enqueues P4 → P4 is live, but lastReadyHash = H3 matches reality
     * </ol>
     *
     * From then on every publish short-circuits on the matching hash; P4 (with a dead backend in
     * its pool) stays live until the ready set changes again. RPCs picked onto backend 4 sit in
     * grpc's delayed transport until their deadline.
     */
    @Test
    void tickPublishRace_leavesDeadBackendInPickerPermanently() {
        AdversarialFixture f = new AdversarialFixture(CFG);
        f.resolveAndReady(8001, 8002, 8003);
        f.resolve(8001, 8002, 8003, 8004); // 8004 created, IDLE

        // (1) sync thread is inside ScListener.onSubchannelState(READY) for 8004: the map write
        //     has happened, publishPicker() has not run yet.
        @SuppressWarnings("unchecked")
        Map<Subchannel, ConnectivityState> conn =
                (Map<Subchannel, ConnectivityState>)
                        AdversarialFixture.getField(f.balancer, "subchannelConn");
        conn.put(f.byPort.get(8004), ConnectivityState.READY);

        // (3) runs inside the tick, between "lastReadyHash = H4" and "enqueue P4".
        f.onTickGetsSyncContext =
                () -> {
                    f.drive(8004, ConnectivityState.READY); // the rest of step (1)
                    f.drive(8004, ConnectivityState.TRANSIENT_FAILURE);
                };
        f.advanceMs(1000);
        f.tick(); // (2) + (4)

        // Give the balancer every chance to notice: more ticks and a no-op resolver refresh.
        for (int i = 0; i < 10; i++) {
            f.advanceMs(1000);
            f.tick();
        }
        f.resolve(8001, 8002, 8003, 8004);

        assertThat(publishedReady(f))
                .as("ready pool of the LIVE picker vs actual READY set %s", actuallyReady(f))
                .isEqualTo(actuallyReady(f));
    }

    /**
     * Inflight accounting under heavy concurrency: 16 threads pick + run tracers while another
     * thread ticks (pruning) and the resolver churns one backend in and out. At quiescence every
     * table's inflight must be 0 and costs must be finite.
     */
    @Test
    void inflightIsConserved_underConcurrentPicksTicksAndChurn() throws Exception {
        AdversarialFixture f = new AdversarialFixture(CFG);
        f.resolveAndReady(9001, 9002, 9003, 9004);
        int threads = 16;
        int perThread = 50_000;
        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger errors = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(threads);
        Thread ticker =
                new Thread(
                        () -> {
                            while (!stop.get()) {
                                try {
                                    f.tick();
                                } catch (Throwable t) {
                                    errors.incrementAndGet();
                                }
                                pace();
                            }
                        });
        Thread churn =
                new Thread(
                        () -> {
                            int i = 0;
                            while (!stop.get()) {
                                if ((i++ & 1) == 0) f.resolve(9001, 9002, 9003);
                                else {
                                    f.resolve(9001, 9002, 9003, 9004);
                                    f.drive(9004, ConnectivityState.READY);
                                }
                            }
                        });
        ticker.start();
        churn.start();
        for (int t = 0; t < threads; t++) {
            new Thread(
                            () -> {
                                ThreadLocalRandom r = ThreadLocalRandom.current();
                                try {
                                    for (int i = 0; i < perThread; i++) {
                                        PickResult pr =
                                                f.pick(r.nextBoolean() ? METHOD_A : METHOD_B);
                                        if (pr.getSubchannel() == null) continue;
                                        ClientStreamTracer tr =
                                                pr.getStreamTracerFactory()
                                                        .newClientStreamTracer(
                                                                ClientStreamTracer.StreamInfo
                                                                        .newBuilder()
                                                                        .build(),
                                                                new Metadata());
                                        tr.streamCreated(Attributes.EMPTY, new Metadata());
                                        f.now.addAndGet(r.nextLong(1000, 50_000));
                                        tr.streamClosed(
                                                r.nextInt(20) == 0
                                                        ? Status.UNAVAILABLE
                                                        : Status.OK);
                                    }
                                } catch (Throwable e) {
                                    e.printStackTrace();
                                    errors.incrementAndGet();
                                } finally {
                                    done.countDown();
                                }
                            })
                    .start();
        }
        done.await();
        stop.set(true);
        ticker.join();
        churn.join();

        @SuppressWarnings("unchecked")
        Map<Subchannel, MethodTable> tables =
                (Map<Subchannel, MethodTable>) AdversarialFixture.getField(f.balancer, "tables");
        int leaked = tables.values().stream().mapToInt(MethodTable::getInflight).sum();
        boolean nonFinite =
                tables.values().stream()
                        .flatMap(mt -> mt.methodKeys().stream().map(mt::statsFor))
                        .anyMatch(
                                ms ->
                                        !Double.isFinite(ms.getEwmaFastMicros())
                                                || !Double.isFinite(ms.getEwmaSlowMicros()));
        System.out.printf(
                "[inflight] leaked=%d errors=%d syncCtxErrors=%d nonFinite=%s%n",
                leaked, errors.get(), f.syncCtxErrors.size(), nonFinite);
        assertThat(errors.get()).isZero();
        assertThat(f.syncCtxErrors).isEmpty();
        assertThat(leaked).isZero();
        assertThat(nonFinite).isFalse();
    }

    /** MethodStats under 16 concurrent writers: no lost samples, no NaN. */
    @Test
    void methodStats_concurrentUpdates_loseNothing() throws Exception {
        MethodStats ms = new MethodStats(5_000, T0);
        long thinning = MethodStats.minSmoothingIntervalNanos;
        MethodStats.minSmoothingIntervalNanos = 0L; // test the CAS loop itself: no thinning
        AtomicLong clock = new AtomicLong(T0);
        int threads = 16, per = 100_000;
        Thread[] ts = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            ts[t] =
                    new Thread(
                            () -> {
                                ThreadLocalRandom r = ThreadLocalRandom.current();
                                for (int i = 0; i < per; i++)
                                    ms.update(clock.addAndGet(1000), r.nextLong(1, 20) * MS, CFG);
                            });
            ts[t].start();
        }
        for (Thread t : ts) t.join();
        MethodStats.minSmoothingIntervalNanos = thinning;
        assertThat(ms.getSamples()).isEqualTo(threads * per);
        assertThat(Double.isFinite(ms.getEwmaFastMicros()) && Double.isFinite(ms.getRttVarMicros()))
                .isTrue();
    }

    /**
     * ErrorWindow under concurrent writers crossing many bucket boundaries; all writes land inside
     * the window so the snapshot must account for every one.
     */
    @Test
    void errorWindow_concurrentWritesAcrossRotations_loseNothing() throws Exception {
        ErrorWindow w = new ErrorWindow(10, 60_000); // 6 s buckets
        AtomicLong clock = new AtomicLong(T0);
        int threads = 16, per = 50_000;
        Thread[] ts = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            ts[t] =
                    new Thread(
                            () -> {
                                for (int i = 0; i < per; i++)
                                    w.recordResult(
                                            i % 3 != 0, clock.addAndGet(20_000)); // 20us steps
                            });
            ts[t].start();
        }
        for (Thread t : ts) t.join();
        // 800k writes * 20us = 16 s of simulated time < 60 s window
        assertThat(w.snapshot(clock.get()).total).isEqualTo((long) threads * per);
    }

    /** Sanity: no subchannel API call is ever made off the sync context in a busy run. */
    @Test
    void busyRun_makesNoOffContextSubchannelCalls() throws Exception {
        AdversarialFixture f = new AdversarialFixture(CFG);
        f.resolveAndReady(9101, 9102, 9103);
        AtomicBoolean stop = new AtomicBoolean();
        Thread ticker =
                new Thread(
                        () -> {
                            while (!stop.get()) {
                                f.tick();
                                pace();
                            }
                        });
        ticker.start();
        for (int i = 0; i < 20_000; i++) {
            ConnectivityState s =
                    List.of(
                                    ConnectivityState.READY,
                                    ConnectivityState.TRANSIENT_FAILURE,
                                    ConnectivityState.IDLE)
                            .get(i % 3);
            for (int p : new int[] {9101, 9102, 9103}) f.drive(p, s);
            if (i % 50 == 0) f.advanceMs(1500);
        }
        stop.set(true);
        ticker.join();
        Set<Integer> bad = new HashSet<>();
        int total = 0;
        for (var e : f.byPort.entrySet()) {
            int v = e.getValue().syncCtxViolations.get();
            total += v;
            if (v > 0) bad.add(e.getKey());
        }
        System.out.printf(
                "[offctx] requestConnection/shutdown calls off sync context: %d (on %s)%n",
                total, bad);
        assertThat(total).isZero();
    }
}
