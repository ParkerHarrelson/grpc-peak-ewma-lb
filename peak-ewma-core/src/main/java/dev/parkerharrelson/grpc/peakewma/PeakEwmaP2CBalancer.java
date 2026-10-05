package dev.parkerharrelson.grpc.peakewma;

import static dev.parkerharrelson.grpc.peakewma.LBConstants.*;
import static dev.parkerharrelson.grpc.peakewma.PeakEwmaConfigKeys.POLICY_NAME;
import static io.grpc.ConnectivityState.CONNECTING;
import static io.grpc.ConnectivityState.TRANSIENT_FAILURE;

import dev.parkerharrelson.grpc.peakewma.metrics.LbMetrics;
import dev.parkerharrelson.grpc.peakewma.metrics.NoopLbMetrics;
import dev.parkerharrelson.grpc.peakewma.outlier.ErrorWindow;
import io.grpc.ConnectivityState;
import io.grpc.ConnectivityStateInfo;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancer;
import io.grpc.Status;
import io.grpc.SynchronizationContext;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages Subchannels and publishes P2C pickers with method-aware Peak-EWMA scoring. Works with
 * headless-services (dns:/// target) and should not require external config to behave well.
 */
final class PeakEwmaP2CBalancer extends LoadBalancer {

    private static final Logger logger = LoggerFactory.getLogger(PeakEwmaP2CBalancer.class);

    private final Helper helper;
    private final AtomicReference<PeakEwmaConfig> cfg = new AtomicReference<>();
    private final LbMetrics metrics;

    private final EwmaClocks clocks = new EwmaClocks();

    private static final long RECONNECT_DEBOUNCE_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final Map<Subchannel, MethodTable> tables = new ConcurrentHashMap<>();
    // Per-method view of the whole fleet (sample-based half-lives, typical latency), recomputed
    // every outlier tick and shared with every backend's MethodTable.
    private final ConcurrentHashMap<String, MethodScale> fleetScales = new ConcurrentHashMap<>();
    private final Map<Subchannel, SubchannelState> states = new ConcurrentHashMap<>();
    private final Map<Subchannel, String> subchannelIds = new ConcurrentHashMap<>();
    private final Map<Subchannel, ConnectivityState> subchannelConn = new ConcurrentHashMap<>();
    private final Map<Subchannel, Long> lastReconnectRequestNanos = new ConcurrentHashMap<>();

    private final Set<String> currentAddressKeys = ConcurrentHashMap.newKeySet();
    private final Map<String, Subchannel> keyToSubchannel = new ConcurrentHashMap<>();
    private final Map<String, EquivalentAddressGroup> currentEags = new ConcurrentHashMap<>();

    // Guarded by outlierLock; a plain field is enough since all access goes through the lock.
    private ScheduledFuture<?> outlierTask;
    private long outlierTaskPeriodMs;

    // Most recent TRANSIENT_FAILURE status reported by any subchannel; surfaced by the error
    // picker when every subchannel is in TRANSIENT_FAILURE.
    private volatile Status lastTransientFailure = Status.UNAVAILABLE;
    private volatile boolean shutdown;

    private volatile String lastReadyHash = "";
    private volatile boolean lastWasReady = false;
    private volatile ConnectivityState lastPublishedState = null;
    // The inflightWeightEff the currently-published picker captured at construction. Reused by
    // emitReadySubchannelMetrics on hash-unchanged publishes so the cost gauges track the value
    // the live picker is actually scoring against, not a freshly computed snapshot that can drift.
    private volatile double livePickerInflightWeightEff = 0.0;

    private final AtomicInteger idSeq = new AtomicInteger(1);

    private final ReentrantLock outlierLock = new ReentrantLock();

    PeakEwmaP2CBalancer(Helper helper, PeakEwmaConfig initialCfg, LbMetrics metrics) {
        this.helper = Objects.requireNonNull(helper, "helper");
        this.cfg.set(Objects.requireNonNull(initialCfg, "initialCfg"));
        this.metrics = (metrics != null) ? metrics : NoopLbMetrics.INSTANCE;
    }

    /**
     * Mirrors grpc's built-in policies: a resolver error while we have READY subchannels is ignored
     * (we keep serving from the current picker); otherwise the channel goes TRANSIENT_FAILURE with
     * the error.
     */
    @Override
    public void handleNameResolutionError(Status error) {
        if (lastPublishedState == ConnectivityState.READY) {
            logger.warn("name resolution failed, keeping current READY picker: {}", error);
            return;
        }
        publishNotReady(ConnectivityState.TRANSIENT_FAILURE, errorPicker(error));
    }

    @Override
    public Status acceptResolvedAddresses(ResolvedAddresses resolvedAddresses) {
        if (resolvedAddresses.getAddresses().isEmpty()) {
            Status unavailable =
                    Status.UNAVAILABLE.withDescription(
                            "NameResolver returned no usable address. " + resolvedAddresses);
            handleNameResolutionError(unavailable);
            return unavailable;
        }
        Object policyConfig = resolvedAddresses.getLoadBalancingPolicyConfig();
        if (policyConfig instanceof PeakEwmaConfig parsed) {
            // Service-config path: gRPC hands back what the provider's
            // parseLoadBalancingPolicyConfig produced.
            this.cfg.set(parsed);
        } else if (policyConfig instanceof Map<?, ?> map) {
            Object inner = map.get(POLICY_NAME);
            if (inner instanceof Map<?, ?> innerMap) {
                @SuppressWarnings("unchecked")
                Map<String, Object> objectMap = (Map<String, Object>) innerMap;
                this.cfg.updateAndGet(prev -> PeakEwmaConfig.merge(prev, objectMap));
            }
        }

        List<EquivalentAddressGroup> addressGroups = resolvedAddresses.getAddresses();
        reconcileSubchannels(addressGroups);

        ensureOutlierTicker();

        publishPicker();
        return Status.OK;
    }

    @Override
    public void shutdown() {
        shutdown = true;
        stopOutlierTicker();
        for (Subchannel sc : tables.keySet()) {
            try {
                String scId = subchannelIds.get(sc);
                if (scId != null) {
                    metrics.removeSubchannel(scId);
                }

                sc.shutdown();
            } catch (Exception e) {
                logger.error("subchannel shutdown failed during balancer shutdown", e);
            }
        }

        tables.clear();
        states.clear();
        subchannelIds.clear();
        subchannelConn.clear();
        keyToSubchannel.clear();
        currentAddressKeys.clear();
        lastReconnectRequestNanos.clear();
    }

    /**
     * Schedules the periodic tick. It runs even with outlier detection disabled: besides ejection
     * it prunes idle methods and re-derives every method's half-lives and seed from the fleet's
     * traffic, and without it the per-method maps grow without bound and the half-lives stay at
     * their pre-traffic defaults.
     */
    private void ensureOutlierTicker() {
        var config = cfg.get();
        outlierLock.lock();
        try {
            long periodMs = Math.max(100L, config.outlierTickIntervalMillis);
            if (outlierTask != null && !outlierTask.isDone() && outlierTaskPeriodMs == periodMs) {
                return; // already running at this period; rescheduling would reset its delay
            }
            cancelOutlierTaskLocked();
            outlierTaskPeriodMs = periodMs;
            // The timer only fires; the tick itself runs on the synchronization context, like
            // every other mutation of balancer state and every Subchannel call.
            SynchronizationContext syncContext = helper.getSynchronizationContext();
            outlierTask =
                    helper.getScheduledExecutorService()
                            .scheduleAtFixedRate(
                                    () -> syncContext.execute(this::outlierTick),
                                    periodMs,
                                    periodMs,
                                    TimeUnit.MILLISECONDS);
        } finally {
            outlierLock.unlock();
        }
    }

    private void stopOutlierTicker() {
        outlierLock.lock();
        try {
            cancelOutlierTaskLocked();
        } finally {
            outlierLock.unlock();
        }
    }

    private void cancelOutlierTaskLocked() {
        ScheduledFuture<?> prev = outlierTask;
        outlierTask = null;
        if (prev != null && !prev.isCancelled()) {
            prev.cancel(false);
        }
    }

    /**
     * Outlier evaluation, once per tick on the sync context. Single pass over (peer, method): each
     * error window is snapshotted once and aggregated per method, so the tick is O(peers x
     * methods). (It used to compute the fleet rate for every (peer, method) by re-scanning every
     * peer: O(peers^2 x methods), ~0.5 s per tick at 400 x 20.)
     *
     * <ul>
     *   <li><b>Error rate</b> over the window ejects the whole subchannel: the backend is failing.
     *   <li><b>Latency</b> ejects only the affected method, and compares the peer's smoothed (slow)
     *       EWMA with the fleet median for that method. Comparing the peak EWMA against the peer's
     *       own history ejected whole backends on a single slow call or a closing stream.
     *   <li>At least one peer may always be ejected (as grpc's outlier_detection does), while at
     *       least one stays in rotation; the percentage caps used to forbid any ejection with fewer
     *       than five backends.
     * </ul>
     */
    private void outlierTick() {
        if (shutdown) {
            return;
        }
        long tickStart = System.nanoTime();
        try {
            outlierTickBody();
        } finally {
            metrics.recordOutlierTick(System.nanoTime() - tickStart);
        }
    }

    private void outlierTickBody() {
        try {
            PeakEwmaConfig c = cfg.get();
            final long now = clocks.nanoTime();

            List<Subchannel> ready = new ArrayList<>();
            for (Map.Entry<Subchannel, MethodTable> e : tables.entrySet()) {
                Subchannel sc = e.getKey();
                if (subchannelConn.getOrDefault(sc, CONNECTING) != ConnectivityState.READY
                        || states.get(sc) == null) {
                    continue;
                }
                e.getValue().pruneStale(now, c.methodPruneStaleAfterMillis, c.methodMaxEntries);
                ready.add(sc);
            }
            final int n = ready.size();
            if (n == 0) {
                return;
            }

            // One pass: per-method view of every ready peer that has served the method. Each
            // peer's state and method ejection are resolved here once, not per check below.
            Map<String, List<PeerMethod>> byMethod = new HashMap<>();
            for (Subchannel sc : ready) {
                MethodTable mt = tables.get(sc);
                SubchannelState st = states.get(sc);
                for (String method : mt.methodKeyView()) {
                    MethodStats ms = mt.peekStats(method);
                    if (ms == null) {
                        continue;
                    }
                    ErrorWindow w = mt.windowFor(method);
                    ErrorWindow.Snapshot snap = w.snapshot(now);
                    double rate = snap.total / Math.max(1e-3, w.coveredNanos(now) / 1e9);
                    byMethod.computeIfAbsent(method, k -> new ArrayList<>())
                            .add(
                                    new PeerMethod(
                                            sc,
                                            mt,
                                            st,
                                            ms,
                                            w,
                                            snap,
                                            rate,
                                            isWarm(ms, rate, now, c),
                                            mt.methodEjectedUntilNanos(method)));
                }
            }

            final long baseEject = EwmaClocks.millisToNanos(c.outlierEjectMillis);
            final long cooldown = EwmaClocks.millisToNanos(c.outlierReentryCooldownMillis);
            final int maxEjected = PeakEwmaTuner.maxEjectedCountEff(n);
            final int minReady = PeakEwmaTuner.minReadyAfterEjectEff(n);
            final long tickMillis = Math.max(100L, c.outlierTickIntervalMillis);
            int ejectedSubchannels = 0;
            for (Subchannel sc : ready) {
                SubchannelState st0 = states.get(sc);
                st0.backoff().maybeForgive(now, baseEject, st0.isEjected(now));
                if (st0.isEjected(now)) ejectedSubchannels++;
            }
            long lastWindowMillis = -1;

            // Pass 1, per method: adapt half-lives and error windows, and eject whole backends
            // for errors. All error ejections are decided before any latency ejection, and take
            // priority over them: a failing backend is worse than a slow one.
            for (Map.Entry<String, List<PeerMethod>> e : byMethod.entrySet()) {
                String method = e.getKey();
                List<PeerMethod> peers = e.getValue();

                double rateSum = 0;
                long total = 0;
                long errors = 0;
                double[] warmSlow = new double[peers.size()];
                double[] warmFast = new double[peers.size()];
                int warm = 0;
                for (PeerMethod pm : peers) {
                    rateSum += pm.rate;
                    total += pm.snap.total;
                    errors += pm.snap.errors;
                    if (pm.warm) {
                        warmSlow[warm] = pm.stats.getEwmaSlowMicros();
                        warmFast[warm] = pm.stats.getEwmaFastMicros();
                        warm++;
                    }
                }
                double fleetRate = rateSum / n;
                double medianSlow = warm > 0 ? median(warmSlow, warm) : Double.NaN;

                // Re-derive the method's memory lengths from what the fleet actually sees: K
                // samples of a fair share of its traffic, never less than a few RTTs. Uses the
                // FLEET rate, not each peer's own: an avoided peer's own rate falls, which would
                // lengthen its memory and keep it locked out.
                if (fleetRate > 0) {
                    MethodScale sc = MethodScale.of(fleetRate, medianSlow);
                    fleetScales.put(method, sc);
                    metrics.setMethodScale(
                            method, sc.tauFastMillis(), sc.tauSlowMillis(), sc.seedMicros());
                    for (PeerMethod pm : peers) pm.stats.setScale(sc);
                }

                for (PeerMethod pm : peers) {
                    long winMsEff = PeakEwmaTuner.windowMillisEff(pm.rate, fleetRate, tickMillis);
                    pm.window.setWindowMillis(winMsEff);
                    lastWindowMillis = winMsEff;
                    pm.table.backoffFor(method).maybeForgive(now, baseEject, pm.methodEjected(now));
                    if (!c.outlierEnabled) {
                        continue; // maintenance only: pruning, scales, windows
                    }

                    SubchannelState st = pm.state;
                    if (highErrorRate(pm, total, errors, c)
                            && !st.isEjected(now)
                            && now >= st.lastEjectEndNanos() + cooldown
                            && ejectedSubchannels < maxEjected
                            && n - (ejectedSubchannels + 1) >= minReady) {
                        st.ejectUntil(st.backoff().nextEjectionEnd(now, baseEject));
                        pm.table.resetErrorWindows(); // judge it on fresh evidence when it returns
                        ejectedSubchannels++;
                        metrics.recordOutlierEjection(
                                subchannelIds.getOrDefault(pm.sc, UNKNOWN),
                                ERRORS,
                                pm.snap.errorRate,
                                1.0);
                    }
                }

                // Fleet-level gauges, aggregated across peers (previously each peer overwrote
                // the same method-keyed gauge and the last one iterated won).
                metrics.setMethodRate(method, rateSum);
                metrics.setMethodErrorRate(method, total == 0 ? 0.0 : (double) errors / total);
                if (warm > 0) {
                    metrics.setMethodLatencyEwma(method, medianSlow, median(warmFast, warm));
                }
            }
            if (lastWindowMillis >= 0) {
                metrics.setAdaptiveTuning(WINDOW_MILLIS_EFF, lastWindowMillis);
            }

            // Pass 2, per method: latency ejections. A peer is unavailable for a method if the
            // method OR the whole backend is ejected, and one cap covers both: counting them
            // separately let them compound (with 3 peers, one ejected for errors and one for
            // latency left a single server for the method).
            for (Map.Entry<String, List<PeerMethod>> e : byMethod.entrySet()) {
                if (!c.outlierEnabled) {
                    break;
                }
                String method = e.getKey();
                List<PeerMethod> peers = e.getValue();
                final int p = peers.size();
                final int keep = Math.min(minReady, p - 1); // peers that must stay available

                int unavailable = 0;
                double[] warmSlow = new double[p];
                int warm = 0;
                for (PeerMethod pm : peers) {
                    if (pm.methodEjected(now) || pm.state.isEjected(now)) {
                        unavailable++;
                    }
                    if (pm.warm) warmSlow[warm++] = pm.stats.getEwmaSlowMicros();
                }

                // A new error ejection can push the method over the cap: return latency-ejected
                // peers to rotation (soonest-ending first) until it holds again.
                if (unavailable > maxEjected || p - unavailable < keep) {
                    List<PeerMethod> methodOnly = new ArrayList<>();
                    for (PeerMethod pm : peers) {
                        if (pm.methodEjected(now) && !pm.state.isEjected(now)) {
                            methodOnly.add(pm);
                        }
                    }
                    methodOnly.sort(Comparator.comparingLong(pm -> pm.ejectedUntil));
                    for (PeerMethod pm : methodOnly) {
                        if (unavailable <= maxEjected && p - unavailable >= keep) break;
                        pm.table.ejectMethodUntil(method, now);
                        pm.ejectedUntil = now;
                        unavailable--;
                    }
                }

                // Latency is judged against the fleet: needs >= 3 warm peers for a meaningful
                // median (with 2 there is no telling which one is the outlier).
                double medianSlow = warm >= 3 ? median(warmSlow, warm) : Double.NaN;
                if (Double.isNaN(medianSlow) || medianSlow <= 0) {
                    continue;
                }
                for (PeerMethod pm : peers) {
                    if (!pm.warm) {
                        continue;
                    }
                    double ratio = pm.stats.getEwmaSlowMicros() / medianSlow;
                    double multiplier =
                            PeakEwmaTuner.latencyMultiplierEff(
                                    PeakEwmaTuner.coeffVarFromEwma(pm.stats), c);
                    if (ratio >= multiplier
                            && !pm.state.isEjected(now)
                            && !pm.methodEjected(now)
                            // fresh evidence: a sample taken after the previous ejection ended
                            && pm.stats.getLastUpdateNanos() > pm.ejectedUntil
                            && now >= pm.ejectedUntil + cooldown
                            && unavailable < maxEjected
                            && p - (unavailable + 1) >= keep) {
                        // Repeat ejections back off (one more base period each, forgiven by
                        // healthy time, at most 5 min): a backend that keeps being slow is kept
                        // out progressively longer instead of being re-probed every few seconds.
                        long until = pm.table.backoffFor(method).nextEjectionEnd(now, baseEject);
                        pm.table.ejectMethodUntil(method, until);
                        pm.ejectedUntil = until;
                        // Probation (#106): when it returns, judge it only on what it does after
                        // returning. Its baseline is cleared (it must re-warm on fresh samples
                        // before it can be re-ejected; stale fault-era stats used to re-eject a
                        // healed backend on its first call) and its peak is held at the fleet's
                        // typical latency, so it returns at par, not as everyone's cheapest peer.
                        pm.stats.resetForProbation(medianSlow, now);
                        unavailable++;
                        metrics.recordOutlierEjection(
                                subchannelIds.getOrDefault(pm.sc, UNKNOWN),
                                LATENCY,
                                pm.snap.errorRate,
                                ratio);
                        metrics.setAdaptiveTuning(LATENCY_MULTIPLIER_EFF, multiplier);
                    }
                }
            }

            fleetScales.keySet().retainAll(byMethod.keySet());
            metrics.setAdaptiveTuning(OUTLIER_ERROR_RATE, c.outlierErrorRate);
            metrics.setReadySubchannelCount(n);
            metrics.setEjectedSubchannelCount(ejectedSubchannels);

            publishPicker();
        } catch (Exception e) {
            logger.error("outlier tick failed; load balancer continues with last picker", e);
        }
    }

    /** One ready peer's view of one method, captured once per tick. */
    private static final class PeerMethod {
        final Subchannel sc;
        final MethodTable table;
        final SubchannelState state;
        final MethodStats stats;
        final ErrorWindow window;
        final ErrorWindow.Snapshot snap;
        final double rate;
        final boolean warm;
        long ejectedUntil; // this method's ejection end on this peer; updated as the tick ejects

        PeerMethod(
                Subchannel sc,
                MethodTable table,
                SubchannelState state,
                MethodStats stats,
                ErrorWindow window,
                ErrorWindow.Snapshot snap,
                double rate,
                boolean warm,
                long ejectedUntil) {
            this.sc = sc;
            this.table = table;
            this.state = state;
            this.stats = stats;
            this.window = window;
            this.snap = snap;
            this.rate = rate;
            this.warm = warm;
            this.ejectedUntil = ejectedUntil;
        }

        boolean methodEjected(long now) {
            return now < ejectedUntil;
        }
    }

    /**
     * Whether a peer is failing badly enough to eject: statistically conclusive (the 95% lower
     * bound on its error rate), not volume-gated. Either absolutely, above {@code
     * outlierErrorRate}, or relative to the rest of the fleet for the method: at least {@link
     * PeakEwmaTuner#RELATIVE_ERROR_FLOOR} and {@link PeakEwmaTuner#RELATIVE_ERROR_FACTOR} times
     * what the other peers see. The relative check catches a backend failing a few percent of calls
     * in a healthy fleet (#108) without ejecting anyone when every backend fails alike (a shared
     * dependency).
     */
    private static boolean highErrorRate(
            PeerMethod pm, long fleetTotal, long fleetErrors, PeakEwmaConfig c) {
        if (c.outlierErrorRate <= 0.0 || pm.snap.total < PeakEwmaTuner.MIN_ERROR_EVIDENCE) {
            return false;
        }
        double lower = PeakEwmaTuner.errorRateLowerBound(pm.snap.errors, pm.snap.total);
        if (lower >= c.outlierErrorRate) {
            return true;
        }
        long othersTotal = fleetTotal - pm.snap.total;
        if (othersTotal < PeakEwmaTuner.MIN_ERROR_EVIDENCE) {
            return false; // nothing to compare against
        }
        double othersRate = (double) (fleetErrors - pm.snap.errors) / othersTotal;
        return lower
                >= Math.max(
                        PeakEwmaTuner.RELATIVE_ERROR_FLOOR,
                        PeakEwmaTuner.RELATIVE_ERROR_FACTOR * othersRate);
    }

    /** Enough recent samples, over enough time, to trust this peer's EWMAs. */
    private static boolean isWarm(MethodStats ms, double ratePerSec, long now, PeakEwmaConfig c) {
        int minSamples = PeakEwmaTuner.minSamplesForRatioEff(ratePerSec);
        long minWarmMs = PeakEwmaTuner.minWarmupMillisForRatioEff(ratePerSec);
        long first = ms.getFirstSampleNanos();
        long sinceFirst = (first == 0L) ? 0L : Math.max(0L, now - first);
        long sinceLast = Math.max(0L, now - ms.getLastUpdateNanos());
        return ms.getSamples() >= minSamples
                && sinceFirst >= EwmaClocks.millisToNanos(minWarmMs)
                // Stale = no sample for two baseline half-lives (sample-based, not a fixed time).
                && sinceLast < EwmaClocks.millisToNanos(2 * PeakEwmaTuner.tauSlowMillis(ms, c));
    }

    /** Median of the first {@code k} values; sorts them in place. */
    private static double median(double[] values, int k) {
        java.util.Arrays.sort(values, 0, k);
        return (k & 1) == 1 ? values[k / 2] : 0.5 * (values[k / 2 - 1] + values[k / 2]);
    }

    private void publishPicker() {
        List<Subchannel> ready = collectReadySubchannels();

        if (ready.isEmpty()) {
            handleNoReadySubchannels();
            return;
        }

        String hash = computeReadyHash(ready);
        boolean reuseLivePicker = lastWasReady && hash.equals(lastReadyHash);
        PeakEwmaConfig cfgSnap = cfg.get();

        // Pre-compute the inflightWeightEff that the new picker (if any) will capture so we can
        // share the exact same value with the metric-emission path. On hash-unchanged publishes
        // we reuse the live picker's captured value so the cost gauges keep tracking what the
        // picker is actually scoring against.
        double inflightWeightEffForEmit;
        P2CPicker newPicker = null;
        if (reuseLivePicker) {
            inflightWeightEffForEmit = livePickerInflightWeightEff;
        } else {
            List<Subchannel> readySnap = Collections.unmodifiableList(ready);
            double inflightWeightEff =
                    P2CPicker.computeInflightWeightEff(readySnap, tables, cfgSnap);
            newPicker =
                    new P2CPicker(
                            readySnap, tables, states, cfgSnap, clocks, metrics, inflightWeightEff);
            inflightWeightEffForEmit = inflightWeightEff;
        }

        emitReadySubchannelMetrics(ready, cfgSnap, inflightWeightEffForEmit);

        if (reuseLivePicker) {
            return;
        }

        livePickerInflightWeightEff = inflightWeightEffForEmit;
        lastReadyHash = hash;
        lastWasReady = true;
        lastPublishedState = ConnectivityState.READY;
        safeUpdate(ConnectivityState.READY, newPicker);
    }

    private List<Subchannel> collectReadySubchannels() {
        List<Subchannel> ready = new ArrayList<>();
        for (Subchannel sc : tables.keySet()) {
            ConnectivityState cs = subchannelConn.getOrDefault(sc, CONNECTING);
            if (cs == ConnectivityState.READY) {
                ready.add(sc);
            }
        }
        return ready;
    }

    private boolean shouldSkipReconnect(Subchannel sc, long now) {
        ConnectivityState cs = subchannelConn.getOrDefault(sc, CONNECTING);
        boolean reconnectEligibleState =
                cs == ConnectivityState.IDLE || cs == CONNECTING || cs == TRANSIENT_FAILURE;
        if (!reconnectEligibleState) {
            return true;
        }
        long last = lastReconnectRequestNanos.getOrDefault(sc, 0L);
        return now - last < RECONNECT_DEBOUNCE_NANOS;
    }

    private void handleNoReadySubchannels() {
        // Throttle the per-subchannel requestConnection burst. When the picker churns through
        // state changes we can otherwise end up calling requestConnection() dozens of times per
        // second on every subchannel while gRPC is still in backoff — harmless but noisy and
        // wasteful. RECONNECT_DEBOUNCE_NANOS caps it to ~once per second per subchannel.
        long now = clocks.nanoTime();
        for (Subchannel sc : tables.keySet()) {
            if (shouldSkipReconnect(sc, now)) {
                continue;
            }
            lastReconnectRequestNanos.put(sc, now);
            try {
                sc.requestConnection();
            } catch (Exception e) {
                logger.error(
                        "requestConnection failed for subchannel {}", subchannelIds.get(sc), e);
            }
        }

        // Aggregate like round_robin: CONNECTING while any subchannel may still connect,
        // otherwise TRANSIENT_FAILURE with a picker that fails non-wait-for-ready RPCs fast.
        boolean anyConnecting = false;
        for (Subchannel sc : tables.keySet()) {
            ConnectivityState cs = subchannelConn.getOrDefault(sc, ConnectivityState.IDLE);
            if (cs == ConnectivityState.CONNECTING || cs == ConnectivityState.IDLE) {
                anyConnecting = true;
                break;
            }
        }
        if (anyConnecting) {
            if (lastPublishedState != ConnectivityState.CONNECTING) {
                publishNotReady(ConnectivityState.CONNECTING, new NoResultPicker());
            }
        } else if (lastPublishedState != TRANSIENT_FAILURE) {
            publishNotReady(TRANSIENT_FAILURE, errorPicker(lastTransientFailure));
        }
    }

    /** Publishes a non-READY state and forgets the ready-set hash so READY gets republished. */
    private void publishNotReady(ConnectivityState state, SubchannelPicker picker) {
        lastReadyHash = "";
        lastWasReady = false;
        lastPublishedState = state;
        safeUpdate(state, picker);
    }

    private void emitReadySubchannelMetrics(
            List<Subchannel> ready, PeakEwmaConfig cfgSnap, double inflightWeightEff) {
        if (ready.isEmpty()) {
            return;
        }

        // The per-(backend, method) cost loop is O(backends x methods) on every publish; skip it
        // when the sink discards costs (the default no-op sink does).
        boolean costs = metrics.recordsCosts();
        long now = clocks.nanoTime();
        for (Subchannel sc : ready) {
            MethodTable tbl = tables.get(sc);
            SubchannelState st = (tbl == null) ? null : states.get(sc);
            if (tbl == null || st == null) {
                continue;
            }

            String scId = subchannelIds.getOrDefault(sc, UNKNOWN);
            metrics.setInflight(scId, tbl.getInflight());

            if (!costs) {
                continue;
            }
            for (String methodKey : tbl.methodKeyView()) {
                MethodStats ms = tbl.peekStats(methodKey);
                if (ms == null) {
                    continue;
                }
                // Delegate to the shared Peak-EWMA cost helper so this gauge tracks the picker
                // 1:1 — including cold/warm selection, inflight weighting, and the warmup
                // multiplier. Previously this method computed a simplified fast*busy cost and
                // diverged from the picker on cold subchannels and during the warmup window.
                double c =
                        P2CPicker.peakEwmaCost(
                                tbl, ms, st, methodKey, now, inflightWeightEff, cfgSnap);
                metrics.setCost(scId, methodKey, c);
            }
        }
    }

    private String computeReadyHash(List<Subchannel> ready) {
        return ready.stream()
                .map(sc -> subchannelIds.getOrDefault(sc, ""))
                .sorted()
                .collect(Collectors.joining("|"));
    }

    private void reconcileSubchannels(List<EquivalentAddressGroup> addressGroups) {
        Set<String> incomingKeys = new HashSet<>();
        Map<String, EquivalentAddressGroup> keyToEag = new HashMap<>();
        for (EquivalentAddressGroup eag : addressGroups) {
            String key = keyOf(eag);
            if (incomingKeys.add(key)) {
                keyToEag.put(key, eag);
            }
        }

        Set<String> toAdd = new HashSet<>(incomingKeys);
        toAdd.removeAll(currentAddressKeys);

        // Same endpoint, changed EAG (attributes, e.g. locality or weights): update in place
        // instead of ignoring it, which left existing subchannels on stale address info.
        for (Map.Entry<String, EquivalentAddressGroup> e : keyToEag.entrySet()) {
            Subchannel existing = keyToSubchannel.get(e.getKey());
            EquivalentAddressGroup previous = currentEags.get(e.getKey());
            if (existing != null && previous != null && !previous.equals(e.getValue())) {
                existing.updateAddresses(List.of(e.getValue()));
            }
        }
        currentEags.keySet().retainAll(incomingKeys);
        currentEags.putAll(keyToEag);

        Set<String> toRemove = new HashSet<>(currentAddressKeys);
        toRemove.removeAll(incomingKeys);

        if (!toRemove.isEmpty()) {
            for (String key : toRemove) {
                Subchannel sc = keyToSubchannel.remove(key);
                if (sc != null) {
                    String scId = subchannelIds.remove(sc);
                    tables.remove(sc);
                    SubchannelState removedState = states.remove(sc);
                    if (removedState != null) removedState.markRemoved();
                    subchannelConn.remove(sc);
                    lastReconnectRequestNanos.remove(sc);
                    try {
                        sc.shutdown();
                    } catch (Exception e) {
                        logger.error("subchannel shutdown failed for {}", scId, e);
                    }

                    if (scId != null) {
                        metrics.removeSubchannel(scId);
                    }
                }
            }
            currentAddressKeys.removeAll(toRemove);
        }

        for (String key : toAdd) {
            EquivalentAddressGroup equivalentAddressGroup = keyToEag.get(key);
            String scId = subchannelIdFor(equivalentAddressGroup);

            Subchannel subchannel =
                    helper.createSubchannel(
                            CreateSubchannelArgs.newBuilder()
                                    .setAddresses(equivalentAddressGroup)
                                    .build());

            PeakEwmaConfig cfgSnap = cfg.get();
            MethodTable methodTable = new MethodTable(cfgSnap, clocks, fleetScales);
            SubchannelState subchannelState = new SubchannelState();

            tables.put(subchannel, methodTable);
            states.put(subchannel, subchannelState);
            subchannelIds.put(subchannel, scId);
            subchannelConn.put(subchannel, ConnectivityState.IDLE);

            keyToSubchannel.put(key, subchannel);
            currentAddressKeys.add(key);

            subchannel.start(new ScListener(subchannel));
            subchannel.requestConnection();
        }
    }

    private String subchannelIdFor(EquivalentAddressGroup eag) {
        // Same identity as keyOf (all addresses): IDs from the first address alone collided for
        // endpoints like [A,B] and [A,C], and removing one wiped the other's metrics.
        return inetHostPort(eag).isPresent() ? keyOf(eag) : "sc-" + idSeq.getAndIncrement();
    }

    private final class ScListener implements SubchannelStateListener {
        private final Subchannel subchannel;

        ScListener(Subchannel subchannel) {
            this.subchannel = subchannel;
        }

        @Override
        public void onSubchannelState(ConnectivityStateInfo stateInfo) {
            ConnectivityState connectivityState = stateInfo.getState();
            // Removed (or balancer shut down): gRPC still delivers the final SHUTDOWN state.
            // Recording it would re-insert the dead subchannel into subchannelConn forever.
            if (shutdown
                    || connectivityState == ConnectivityState.SHUTDOWN
                    || !tables.containsKey(subchannel)) {
                return;
            }

            ConnectivityState previous = subchannelConn.get(subchannel);
            if (connectivityState == TRANSIENT_FAILURE) {
                lastTransientFailure = stateInfo.getStatus();
                helper.refreshNameResolution();
            }
            // Sticky TRANSIENT_FAILURE (as pick_first does): a subchannel retrying after backoff
            // reports CONNECTING, but it stays "failed" for aggregation until it is READY again.
            ConnectivityState effective =
                    (previous == TRANSIENT_FAILURE && connectivityState == CONNECTING)
                            ? TRANSIENT_FAILURE
                            : connectivityState;
            subchannelConn.put(subchannel, effective);

            SubchannelState subchannelState = states.get(subchannel);
            // Restart the warmup penalty only for a backend that is plausibly cold: its first
            // READY, or READY after a connection failure (likely a restart). A reconnect after
            // IDLE (GOAWAY / max connection age) goes back to the same warm server; re-warming
            // there would keep a periodically-GOAWAYing backend permanently penalised.
            if (subchannelState != null
                    && connectivityState == ConnectivityState.READY
                    && (subchannelState.readySinceNanos() == 0L || previous == TRANSIENT_FAILURE)) {
                subchannelState.markReady(clocks.nanoTime());
            }
            if (connectivityState == ConnectivityState.IDLE) {
                // Subchannels don't reconnect on their own once a connection closes (GOAWAY,
                // max connection age, idle timeout); the policy must ask.
                helper.refreshNameResolution();
                subchannel.requestConnection();
            }
            publishPicker();
        }
    }

    private void safeUpdate(
            ConnectivityState connectivityState, SubchannelPicker subchannelPicker) {
        helper.getSynchronizationContext()
                .execute(() -> helper.updateBalancingState(connectivityState, subchannelPicker));
    }

    /**
     * Identity of an endpoint: ALL of its addresses (dual-stack endpoints have several), not just
     * the first, and never a hashCode (which can collide). Attributes are not part of the identity:
     * a change there updates the existing subchannel instead of replacing it.
     */
    private static String keyOf(EquivalentAddressGroup equivalentAddressGroup) {
        StringBuilder key = new StringBuilder();
        for (SocketAddress address : equivalentAddressGroup.getAddresses()) {
            if (key.length() > 0) key.append(',');
            key.append(
                    address instanceof InetSocketAddress inet
                            ? formatInet(inet)
                            : address.getClass().getName() + ":" + address);
        }
        return key.toString();
    }

    private static String formatInet(InetSocketAddress inet) {
        String host =
                inet.getAddress() != null
                        ? inet.getAddress().getHostAddress()
                        : inet.getHostString();
        if (host.indexOf(':') >= 0) host = "[" + host + "]";
        return host + ":" + inet.getPort();
    }

    private static Optional<String> inetHostPort(EquivalentAddressGroup equivalentAddressGroup) {
        try {
            SocketAddress socketAddress = equivalentAddressGroup.getAddresses().get(0);
            if (socketAddress instanceof InetSocketAddress inetSocketAddress) {
                String host =
                        (inetSocketAddress.getAddress() != null)
                                ? inetSocketAddress.getAddress().getHostAddress()
                                : inetSocketAddress.getHostString();
                if (host.indexOf(':') >= 0) host = "[" + host + "]";
                return Optional.of(host + ":" + inetSocketAddress.getPort());
            }
        } catch (Exception e) {
            logger.error("failed to extract host:port from EAG {}", equivalentAddressGroup, e);
        }
        return Optional.empty();
    }

    private static final class NoResultPicker extends SubchannelPicker {
        @Override
        public PickResult pickSubchannel(PickSubchannelArgs args) {
            return PickResult.withNoResult();
        }
    }

    private static SubchannelPicker errorPicker(Status error) {
        return new SubchannelPicker() {
            @Override
            public PickResult pickSubchannel(PickSubchannelArgs args) {
                return PickResult.withError(error);
            }
        };
    }
}
