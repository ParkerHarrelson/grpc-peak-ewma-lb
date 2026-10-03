package dev.parkerharrelson.grpc.peakewma;

import static dev.parkerharrelson.grpc.peakewma.LBConstants.*;
import static java.util.Arrays.sort;

import dev.parkerharrelson.grpc.peakewma.metrics.LbMetrics;
import dev.parkerharrelson.grpc.peakewma.tracing.EwmaClientStreamTracerFactory;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancer.PickResult;
import io.grpc.LoadBalancer.SubchannelPicker;
import io.grpc.MethodDescriptor;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/** Power-of-Two-Choices picker with method-aware Peak-EWMA cost. */
public final class P2CPicker extends SubchannelPicker {
    private static final double EPS = 1e-6;

    private final List<LoadBalancer.Subchannel> readyPool;
    private final Map<LoadBalancer.Subchannel, MethodTable> tables;
    private final Map<LoadBalancer.Subchannel, SubchannelState> states;
    private final PeakEwmaConfig ewmaConfig;
    private final EwmaClocks clocks;

    private final LbMetrics metrics;

    private final double inflightWeightEff;

    P2CPicker(
            List<LoadBalancer.Subchannel> readyPool,
            Map<LoadBalancer.Subchannel, MethodTable> tables,
            Map<LoadBalancer.Subchannel, SubchannelState> states,
            PeakEwmaConfig ewmaConfig,
            EwmaClocks clocks,
            LbMetrics metrics) {
        this(
                readyPool,
                tables,
                states,
                ewmaConfig,
                clocks,
                metrics,
                computeInflightWeightEff(readyPool, tables, ewmaConfig));
    }

    P2CPicker(
            List<LoadBalancer.Subchannel> readyPool,
            Map<LoadBalancer.Subchannel, MethodTable> tables,
            Map<LoadBalancer.Subchannel, SubchannelState> states,
            PeakEwmaConfig ewmaConfig,
            EwmaClocks clocks,
            LbMetrics metrics,
            double inflightWeightEff) {
        this.readyPool = Objects.requireNonNull(readyPool, "readyPool");
        this.tables = Objects.requireNonNull(tables, "tables");
        this.states = Objects.requireNonNull(states, "states");
        this.ewmaConfig = Objects.requireNonNull(ewmaConfig, "cfg");
        this.clocks = Objects.requireNonNull(clocks, "clocks");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.inflightWeightEff = inflightWeightEff;

        metrics.setAdaptiveTuning(INFLIGHT_WEIGHT_EFF, inflightWeightEff);
    }

    /**
     * Snapshots the median inflight across {@code readyPool} and resolves it to the
     * configured-aware {@code inflightWeightEff} the picker uses to score every pick. Exposed so
     * the balancer can pre-compute the same value once per publish and pass it both to the picker
     * (which captures it for its lifetime) and to the metric-emission path (which would otherwise
     * recompute against a slightly newer snapshot and silently diverge).
     */
    static double computeInflightWeightEff(
            List<LoadBalancer.Subchannel> readyPool,
            Map<LoadBalancer.Subchannel, MethodTable> tables,
            PeakEwmaConfig ewmaConfig) {
        int readyCount = readyPool.size();
        if (readyCount == 0) {
            return PeakEwmaTuner.inflightWeightEff(0, 1, ewmaConfig);
        }
        int[] inflights = new int[readyCount];
        for (int i = 0; i < readyCount; i++) {
            MethodTable t = tables.get(readyPool.get(i));
            inflights[i] = (t != null) ? t.getInflight() : 0;
        }
        sort(inflights);
        int medianInflight = inflights[readyCount / 2];
        return PeakEwmaTuner.inflightWeightEff(readyCount, Math.max(1, medianInflight), ewmaConfig);
    }

    /** Returns the {@code inflightWeightEff} this picker captured at construction. */
    double getInflightWeightEff() {
        return inflightWeightEff;
    }

    @Override
    public PickResult pickSubchannel(LoadBalancer.PickSubchannelArgs args) {
        if (readyPool.isEmpty()) {
            metrics.recordPick(NO_READY);
            return PickResult.withNoResult();
        }

        final String method = methodName(args.getMethodDescriptor());
        final long now = clocks.nanoTime();

        final int n = readyPool.size();
        int[] idx = new int[n];
        double[] costs = new double[n];
        int m = 0;

        for (int i = 0; i < n; i++) {
            LoadBalancer.Subchannel sc = readyPool.get(i);
            double c = cost(sc, method, now);
            if (!Double.isInfinite(c)) {
                idx[m] = i;
                costs[m] = c;
                m++;
            }
        }

        if (m == 0) {
            LoadBalancer.Subchannel best = pickBestByBaseline(method);
            if (best == null) {
                metrics.recordPick(ALL_EJECTED);
                return PickResult.withNoResult();
            }
            metrics.recordPick(OK_FALLBACK);
            return buildPickResult(best, method);
        }

        final LoadBalancer.Subchannel chosen;
        if (m == 1) {
            chosen = readyPool.get(idx[0]);
        } else {
            // Unbiased "pick two distinct from m" — pick i1 uniformly in the full range,
            // then i2 uniformly in the range-minus-one and shift past i1. This gives a uniform
            // distribution over all ordered pairs with distinct indices; the naive approach of
            // re-sampling to the neighbour on collision biases the second pick toward that
            // neighbour.
            int i1 = ThreadLocalRandom.current().nextInt(m);
            int i2 = ThreadLocalRandom.current().nextInt(m - 1);
            if (i2 >= i1) i2++;

            int aIdx = idx[i1];
            int bIdx = idx[i2];
            // Break exact ties (identical latency histories) with sub-promille jitter so we
            // don't lock-step onto one subchannel under perfectly uniform steady state.
            double c1 = costs[i1] * (1.0 + 1e-4 * ThreadLocalRandom.current().nextDouble());
            double c2 = costs[i2] * (1.0 + 1e-4 * ThreadLocalRandom.current().nextDouble());
            chosen = (c1 <= c2) ? readyPool.get(aIdx) : readyPool.get(bIdx);
        }

        metrics.recordPick(OK);
        return buildPickResult(chosen, method);
    }

    private PickResult buildPickResult(LoadBalancer.Subchannel sc, String method) {
        MethodTable methodTable = tables.get(sc);
        if (methodTable == null) {
            metrics.recordPick(NO_TABLE);
            return PickResult.withNoResult();
        }

        Runnable inc = methodTable::incrementInflight;
        Runnable dec = methodTable::decrementInflight;

        EwmaClientStreamTracerFactory tracerFactory =
                new EwmaClientStreamTracerFactory(
                        methodTable, ewmaConfig, clocks, method, inc, dec, metrics);

        return PickResult.withSubchannel(sc, tracerFactory);
    }

    private LoadBalancer.Subchannel pickBestByBaseline(String method) {
        LoadBalancer.Subchannel best = null;
        double bestCost = Double.POSITIVE_INFINITY;
        long bestEjectEnd = Long.MAX_VALUE;

        for (LoadBalancer.Subchannel subchannel : readyPool) {
            MethodTable methodTable = tables.get(subchannel);
            SubchannelState subchannelState = states.get(subchannel);
            if (methodTable == null || subchannelState == null) {
                continue;
            }

            MethodStats methodStats = methodTable.statsFor(method);
            double cost = costIgnoringEjection(methodStats, methodTable);
            long lastEnd = subchannelState.lastEjectEndNanos();

            if (cost < bestCost || (cost == bestCost && lastEnd < bestEjectEnd)) {
                best = subchannel;
                bestCost = cost;
                bestEjectEnd = lastEnd;
            }
        }
        return best;
    }

    private double costIgnoringEjection(MethodStats ms, MethodTable mt) {
        // Baseline fallback: used when every live peer has been ejected. We purposely ignore
        // warmup, since the goal is simply to find the least-bad option until real ejections
        // expire. Score on the peak EWMA (fast) so the same "best observed latency" metric
        // drives both the main and fallback picks.
        double score = Math.max(EPS, ms.getEwmaFastMicros());
        double busy = 1.0 + inflightWeightEff * Math.max(0, mt.getInflight());
        return score * busy;
    }

    private double cost(LoadBalancer.Subchannel subchannel, String method, long now) {
        MethodTable methodTable = tables.get(subchannel);
        if (methodTable == null) return Double.POSITIVE_INFINITY;

        MethodStats methodStats = methodTable.statsFor(method);
        SubchannelState subchannelState = states.get(subchannel);
        if (subchannelState == null) return Double.POSITIVE_INFINITY;

        if (subchannelState.isEjected(now) || methodTable.isMethodEjected(method, now))
            return Double.POSITIVE_INFINITY;

        return peakEwmaCost(
                methodTable,
                methodStats,
                subchannelState,
                method,
                now,
                inflightWeightEff,
                ewmaConfig);
    }

    /**
     * Shared Peak-EWMA cost computation. Used by {@link #cost} (picker hot path) and by the
     * balancer's metric emission path so that the {@code lb.subchannel.method.cost} gauge always
     * matches the value the picker would actually score against.
     *
     * <p>Warm peers score on the fast (peak) EWMA so a slow spike is penalised immediately. Cold
     * peers (too few samples or stale) fall back to the slow EWMA, which is seeded from config or
     * historical methods and is more stable before fast has real data. Multiplied by a busy factor
     * (inflight weight) and a warmup factor that decays from 2.0 → 1.0 over the first warmupMsEff
     * milliseconds of readiness.
     *
     * <p>Does not apply ejection — callers decide whether to treat an ejected subchannel as
     * infinity (picker) or just show the would-be cost (metrics).
     */
    static double peakEwmaCost(
            MethodTable mt,
            MethodStats ms,
            SubchannelState st,
            String method,
            long now,
            double inflightWeightEff,
            PeakEwmaConfig cfg) {
        boolean cold = isColdAdaptive(mt, ms, method, now, cfg);
        double score =
                cold
                        ? Math.max(EPS, ms.getEwmaSlowMicros())
                        : Math.max(EPS, ms.getEwmaFastMicros());

        double busy = 1.0 + inflightWeightEff * Math.max(0, mt.getInflight());

        long warmupMsEff = PeakEwmaTuner.warmupMillisEff(mt);
        long ageNanos = Math.max(0L, now - st.readySinceNanos());
        double warm =
                (warmupMsEff <= 0)
                        ? 1.0
                        : 2.0
                                - Math.min(
                                        1.0,
                                        ageNanos / (double) EwmaClocks.millisToNanos(warmupMsEff));

        return score * busy * warm;
    }

    static boolean isColdAdaptive(
            MethodTable mt, MethodStats ms, String method, long now, PeakEwmaConfig cfg) {
        int numSamples = ms.getSamples();

        var window = mt.windowFor(method);
        double lambda = PeakEwmaTuner.methodRatePerSec(window, now);

        int minSamples = PeakEwmaTuner.minSamplesForRatioEff(lambda);
        long minWarmMs = PeakEwmaTuner.minWarmupMillisForRatioEff(lambda);

        boolean enoughSamples = numSamples >= minSamples;

        long first = ms.getFirstSampleNanos();
        long sinceFirst = (first == 0L) ? Long.MAX_VALUE : (now - first);
        boolean enoughTime = sinceFirst >= EwmaClocks.millisToNanos(minWarmMs);

        long sinceLastUpdate = now - ms.getLastUpdateNanos();
        final long STALE_NANOS = EwmaClocks.millisToNanos(cfg.staleMillisForRatio);
        boolean stale = sinceLastUpdate >= STALE_NANOS;

        return !(enoughSamples && enoughTime) || stale;
    }

    private static String methodName(MethodDescriptor<?, ?> methodDescriptor) {
        return methodDescriptor == null ? "" : methodDescriptor.getFullMethodName();
    }
}
