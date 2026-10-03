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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
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
    private final Map<Subchannel, SubchannelState> states = new ConcurrentHashMap<>();
    private final Map<Subchannel, String> subchannelIds = new ConcurrentHashMap<>();
    private final Map<Subchannel, ConnectivityState> subchannelConn = new ConcurrentHashMap<>();
    private final Map<Subchannel, Long> lastReconnectRequestNanos = new ConcurrentHashMap<>();

    private final Set<String> currentAddressKeys = ConcurrentHashMap.newKeySet();
    private final Map<String, Subchannel> keyToSubchannel = new ConcurrentHashMap<>();

    // Guarded by outlierLock; a plain field is enough since all access goes through the lock.
    private ScheduledFuture<?> outlierTask;

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

    @Override
    public void handleNameResolutionError(Status error) {
        safeUpdate(ConnectivityState.TRANSIENT_FAILURE, errorPicker(error));
    }

    @Override
    public void handleResolvedAddresses(ResolvedAddresses resolvedAddresses) {
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
    }

    @Override
    public void shutdown() {
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

    private void ensureOutlierTicker() {
        var config = cfg.get();
        boolean enabled = config.outlierEnabled && config.outlierWindowMillis > 0;
        outlierLock.lock();
        try {
            if (!enabled) {
                cancelOutlierTaskLocked();
                return;
            }
            long periodMs = Math.max(100L, config.outlierTickIntervalMillis);
            cancelOutlierTaskLocked();
            outlierTask =
                    helper.getScheduledExecutorService()
                            .scheduleAtFixedRate(
                                    this::outlierTick, periodMs, periodMs, TimeUnit.MILLISECONDS);
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

    private void outlierTick() {
        try {
            PeakEwmaConfig ewmaConfig = cfg.get();
            if (!ewmaConfig.outlierEnabled) {
                return;
            }

            final long now = clocks.nanoTime();
            final long cooldownNanos =
                    EwmaClocks.millisToNanos(ewmaConfig.outlierReentryCooldownMillis);

            ReadyContext readyCtx = collectReadyContext(now);
            if (readyCtx.readyBefore == 0) {
                return;
            }

            int ejectedNow = readyCtx.initialEjected;

            for (Map.Entry<Subchannel, MethodTable> entry : tables.entrySet()) {
                Subchannel subchannel = entry.getKey();
                MethodTable methodTable = entry.getValue();
                SubchannelState subchannelState = states.get(subchannel);

                if (!shouldEvaluateSubchannel(
                        subchannel, methodTable, subchannelState, now, ewmaConfig)) {
                    continue;
                }

                if (maybeEjectMethodsForSubchannel(
                        methodTable,
                        subchannelState,
                        readyCtx,
                        now,
                        cooldownNanos,
                        ewmaConfig,
                        ejectedNow)) {

                    ejectedNow++;
                }

                for (String methodKey : methodTable.methodKeys()) {
                    MethodStats ms = methodTable.statsFor(methodKey);
                    if (ms != null) {
                        metrics.setMethodLatencyEwma(
                                methodKey, ms.getEwmaSlowMicros(), ms.getEwmaFastMicros());
                    }

                    ErrorWindow w = methodTable.windowFor(methodKey);
                    double lambdaMethod = PeakEwmaTuner.methodRatePerSec(w, now);
                    double lambdaFleet = fleetRateForMethod(methodKey, readyCtx.readyEntries, now);
                    long winMsEff = PeakEwmaTuner.windowMillisEff(lambdaMethod, lambdaFleet);
                    metrics.setAdaptiveTuning(WINDOW_MILLIS_EFF, winMsEff);
                }
            }

            metrics.setReadySubchannelCount(readyCtx.readyBefore);
            metrics.setEjectedSubchannelCount(ejectedNow);

            publishPicker();
        } catch (Exception e) {
            logger.error("outlier tick failed; load balancer continues with last picker", e);
        }
    }

    private ReadyContext collectReadyContext(long now) {
        List<Map.Entry<Subchannel, MethodTable>> readyEntries = new ArrayList<>();
        int ejectedNow = 0;

        for (Map.Entry<Subchannel, MethodTable> entry : tables.entrySet()) {
            Subchannel sc = entry.getKey();
            MethodTable mt = entry.getValue();
            if (mt == null) {
                continue;
            }
            if (subchannelConn.getOrDefault(sc, CONNECTING) == ConnectivityState.READY) {
                readyEntries.add(entry);
                SubchannelState st = states.get(sc);
                if (st != null && st.isEjected(now)) {
                    ejectedNow++;
                }
            }
        }

        return new ReadyContext(readyEntries, readyEntries.size(), ejectedNow);
    }

    private boolean shouldEvaluateSubchannel(
            Subchannel subchannel,
            MethodTable methodTable,
            SubchannelState subchannelState,
            long now,
            PeakEwmaConfig ewmaConfig) {

        if (methodTable == null || subchannelState == null) {
            return false;
        }

        methodTable.pruneStale(
                now, ewmaConfig.methodPruneStaleAfterMillis, ewmaConfig.methodMaxEntries);

        return subchannelConn.getOrDefault(subchannel, CONNECTING) == ConnectivityState.READY;
    }

    private boolean maybeEjectMethodsForSubchannel(
            MethodTable methodTable,
            SubchannelState subchannelState,
            ReadyContext readyCtx,
            long now,
            long cooldownNanos,
            PeakEwmaConfig ewmaConfig,
            int ejectedSoFar) {

        MethodEjectContext ctx =
                new MethodEjectContext(
                        readyCtx, now, cooldownNanos, ewmaConfig, ejectedSoFar, subchannelState);

        for (String method : methodTable.methodKeys()) {
            MethodStats methodStats = methodTable.statsFor(method);
            if (methodStats == null) {
                continue;
            }

            if (tryEjectForMethod(method, methodStats, methodTable, ctx)) {
                return true;
            }
        }

        return false;
    }

    private boolean tryEjectForMethod(
            String method,
            MethodStats methodStats,
            MethodTable methodTable,
            MethodEjectContext ctx) {

        long now = ctx.now();
        PeakEwmaConfig ewmaConfig = ctx.cfg();
        SubchannelState subchannelState = ctx.subchannelState();

        ErrorWindow window = methodTable.windowFor(method);
        double lambdaMethod = PeakEwmaTuner.methodRatePerSec(window, now);
        double lambdaFleet = fleetRateForMethod(method, ctx.readyCtx().readyEntries(), now);

        long winMsEff = PeakEwmaTuner.windowMillisEff(lambdaMethod, lambdaFleet);
        window.setWindowMillis(winMsEff);

        ErrorWindow.Snapshot snapshot = window.snapshot(now);
        if (snapshot.total == 0L) {
            metrics.setMethodRate(method, lambdaMethod);
            metrics.setMethodErrorRate(method, snapshot.errorRate);
            metrics.setAdaptiveTuning(WINDOW_MILLIS_EFF, winMsEff);
            return false;
        }

        metrics.setMethodRate(method, lambdaMethod);
        metrics.setMethodErrorRate(method, snapshot.errorRate);
        metrics.setAdaptiveTuning(WINDOW_MILLIS_EFF, winMsEff);

        EjectionDecision decision =
                evaluateEjectionDecision(
                        methodStats,
                        window,
                        snapshot,
                        new EjectionContext(
                                ctx.readyCtx().readyBefore(),
                                ctx.ejectedSoFar(),
                                subchannelState,
                                now,
                                ctx.cooldownNanos(),
                                ewmaConfig));

        if (!decision.shouldEject) {
            return false;
        }

        long until = now + EwmaClocks.millisToNanos(ewmaConfig.outlierEjectMillis);

        methodTable.ejectMethodUntil(method, until);

        boolean wasEjected = subchannelState.isEjected(now);
        subchannelState.ejectUntil(until);

        String scIdForMetrics = subchannelIdForState(subchannelState);

        double fast = Math.max(1e-6, methodStats.getEwmaFastMicros());
        double slow = Math.max(1e-6, methodStats.getEwmaSlowMicros());
        double coeffVar = PeakEwmaTuner.coeffVarFromEwma(methodStats);
        double latencyMultiplierEff = PeakEwmaTuner.latencyMultiplierEff(coeffVar, ewmaConfig);

        boolean gate = adaptiveGate(methodStats, window, now, ewmaConfig);
        double latencyRatio = gate ? (fast / slow) : 1.0;

        boolean highErr =
                (ewmaConfig.outlierErrorRate > 0.0
                        && snapshot.errorRate >= ewmaConfig.outlierErrorRate);
        boolean highLat = (latencyMultiplierEff > 1.0 && latencyRatio >= latencyMultiplierEff);

        String reason = buildOutlierReason(highErr, highLat);

        metrics.recordOutlierEjection(scIdForMetrics, reason, snapshot.errorRate, latencyRatio);

        metrics.setAdaptiveTuning(LATENCY_MULTIPLIER_EFF, latencyMultiplierEff);
        metrics.setAdaptiveTuning(OUTLIER_ERROR_RATE, ewmaConfig.outlierErrorRate);

        return !wasEjected;
    }

    private EjectionDecision evaluateEjectionDecision(
            MethodStats methodStats,
            ErrorWindow window,
            ErrorWindow.Snapshot snapshot,
            EjectionContext ctx) {

        boolean gate = adaptiveGate(methodStats, window, ctx.now(), ctx.cfg());

        double coeffVarFromEwma = PeakEwmaTuner.coeffVarFromEwma(methodStats);
        double latencyMultiplierEff =
                PeakEwmaTuner.latencyMultiplierEff(coeffVarFromEwma, ctx.cfg());

        int maxPctEff = PeakEwmaTuner.maxEjectionPercentEff(ctx.readyBefore());
        double minReadyFracEff = PeakEwmaTuner.minReadyFractionAfterEjectEff(ctx.readyBefore());
        int minTotalEff = PeakEwmaTuner.minTotalForErrorEjectEff(ctx.readyBefore());

        if (snapshot.total < minTotalEff) {
            return new EjectionDecision(false);
        }

        double errorRate = snapshot.errorRate;
        double fast = Math.max(1e-6, methodStats.getEwmaFastMicros());
        double slow = Math.max(1e-6, methodStats.getEwmaSlowMicros());
        double latencyRatio = gate ? (fast / slow) : 1.0;

        boolean ejectOnErr =
                (ctx.cfg().outlierErrorRate > 0.0 && errorRate >= ctx.cfg().outlierErrorRate);
        boolean ejectOnLat = (latencyMultiplierEff > 1.0 && latencyRatio >= latencyMultiplierEff);

        if (!(ejectOnErr || ejectOnLat)) {
            return new EjectionDecision(false);
        }

        if (ctx.cfg().outlierReentryCooldownMillis > 0) {
            long nextAllowed = ctx.subchannelState().lastEjectEndNanos() + ctx.cooldownNanos();
            if (ctx.now() < nextAllowed) {
                return new EjectionDecision(false);
            }
        }

        int wouldBeEjectedGlobal =
                ctx.ejectedSoFar() + (ctx.subchannelState().isEjected(ctx.now()) ? 0 : 1);

        int percent = (int) Math.round(100.0 * wouldBeEjectedGlobal / ctx.readyBefore());
        if (percent > maxPctEff) {
            return new EjectionDecision(false);
        }

        int minReady = (int) Math.ceil(minReadyFracEff * ctx.readyBefore());
        if ((ctx.readyBefore() - wouldBeEjectedGlobal) < minReady) {
            return new EjectionDecision(false);
        }

        return new EjectionDecision(true);
    }

    private record EjectionDecision(boolean shouldEject) {}

    private record EjectionContext(
            int readyBefore,
            int ejectedSoFar,
            SubchannelState subchannelState,
            long now,
            long cooldownNanos,
            PeakEwmaConfig cfg) {}

    private record MethodEjectContext(
            ReadyContext readyCtx,
            long now,
            long cooldownNanos,
            PeakEwmaConfig cfg,
            int ejectedSoFar,
            SubchannelState subchannelState) {}

    private String buildOutlierReason(boolean highErr, boolean highLat) {
        if (highErr && highLat) {
            return ERRORS + "+" + LATENCY;
        } else if (highErr) {
            return ERRORS;
        } else {
            return LATENCY;
        }
    }

    private static boolean adaptiveGate(
            MethodStats methodStats,
            ErrorWindow errorWindow,
            long now,
            PeakEwmaConfig peakEwmaConfig) {
        double lambda = PeakEwmaTuner.methodRatePerSec(errorWindow, now);
        int minSamples = PeakEwmaTuner.minSamplesForRatioEff(lambda);
        long minWarmMs = PeakEwmaTuner.minWarmupMillisForRatioEff(lambda);

        int numSamples = methodStats.getSamples();
        long first = methodStats.getFirstSampleNanos();
        long sinceFirst = (first == 0L) ? 0L : Math.max(0L, now - first);
        long sinceLast = Math.max(0L, now - methodStats.getLastUpdateNanos());

        boolean warm =
                numSamples >= minSamples && sinceFirst >= EwmaClocks.millisToNanos(minWarmMs);
        boolean stale = sinceLast >= EwmaClocks.millisToNanos(peakEwmaConfig.staleMillisForRatio);
        return warm && !stale;
    }

    private double fleetRateForMethod(
            String method, List<Map.Entry<Subchannel, MethodTable>> readyEntries, long now) {

        double lambdaFleet = 0.0;
        for (Map.Entry<Subchannel, MethodTable> rEntry : readyEntries) {
            MethodTable rmt = rEntry.getValue();
            if (rmt == null) {
                continue;
            }
            lambdaFleet += PeakEwmaTuner.methodRatePerSec(rmt.windowFor(method), now);
        }
        int readyCount = readyEntries.size();
        return readyCount == 0 ? 0.0 : (lambdaFleet / readyCount);
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

        if (lastPublishedState != ConnectivityState.CONNECTING) {
            lastReadyHash = "";
            lastWasReady = false;
            lastPublishedState = ConnectivityState.CONNECTING;
            safeUpdate(ConnectivityState.CONNECTING, new NoResultPicker());
        }
    }

    private void emitReadySubchannelMetrics(
            List<Subchannel> ready, PeakEwmaConfig cfgSnap, double inflightWeightEff) {
        if (ready.isEmpty()) {
            return;
        }

        long now = clocks.nanoTime();
        for (Subchannel sc : ready) {
            MethodTable tbl = tables.get(sc);
            SubchannelState st = (tbl == null) ? null : states.get(sc);
            if (tbl == null || st == null) {
                continue;
            }

            String scId = subchannelIds.getOrDefault(sc, UNKNOWN);
            metrics.setInflight(scId, tbl.getInflight());

            for (String methodKey : tbl.methodKeys()) {
                MethodStats ms = tbl.statsFor(methodKey);
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

        Set<String> toRemove = new HashSet<>(currentAddressKeys);
        toRemove.removeAll(incomingKeys);

        if (!toRemove.isEmpty()) {
            for (String key : toRemove) {
                Subchannel sc = keyToSubchannel.remove(key);
                if (sc != null) {
                    String scId = subchannelIds.remove(sc);
                    tables.remove(sc);
                    states.remove(sc);
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
            MethodTable methodTable = new MethodTable(cfgSnap, clocks);
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
        return inetHostPort(eag).orElse("sc-" + idSeq.getAndIncrement());
    }

    private String subchannelIdForState(SubchannelState targetState) {
        for (Map.Entry<Subchannel, SubchannelState> e : states.entrySet()) {
            if (e.getValue() == targetState) {
                return subchannelIds.getOrDefault(e.getKey(), UNKNOWN);
            }
        }
        return UNKNOWN;
    }

    private final class ScListener implements SubchannelStateListener {
        private final Subchannel subchannel;

        ScListener(Subchannel subchannel) {
            this.subchannel = subchannel;
        }

        @Override
        public void onSubchannelState(ConnectivityStateInfo stateInfo) {
            ConnectivityState connectivityState = stateInfo.getState();
            subchannelConn.put(subchannel, connectivityState);

            SubchannelState subchannelState = states.get(subchannel);
            if (subchannelState != null && connectivityState == ConnectivityState.READY) {
                subchannelState.markReady(clocks.nanoTime());
            }
            publishPicker();
        }
    }

    private void safeUpdate(
            ConnectivityState connectivityState, SubchannelPicker subchannelPicker) {
        helper.getSynchronizationContext()
                .execute(() -> helper.updateBalancingState(connectivityState, subchannelPicker));
    }

    private static String keyOf(EquivalentAddressGroup equivalentAddressGroup) {
        return inetHostPort(equivalentAddressGroup)
                .orElse(String.valueOf(equivalentAddressGroup.getAddresses().hashCode()));
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

    private record ReadyContext(
            List<Map.Entry<Subchannel, MethodTable>> readyEntries,
            int readyBefore,
            int initialEjected) {}
}
