package dev.parkerharrelson.grpc.peakewma;

import static dev.parkerharrelson.grpc.peakewma.LBConstants.*;
import static java.util.Arrays.sort;

import dev.parkerharrelson.grpc.peakewma.metrics.LbMetrics;
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

    /**
     * Resamples allowed to replace an ejected peer before falling back to a full scan. Ejection is
     * capped at 50% of the fleet, so the fallback runs for at most ~0.5^8 = 0.4% of picks.
     */
    private static final int MAX_RESAMPLES = 8;

    private final List<LoadBalancer.Subchannel> readyPool;
    private final MethodTable[] tableAt;
    private final SubchannelState[] stateAt;
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
        // Resolve each peer's state once per picker (a picker lives until the ready set changes)
        // instead of two map lookups per scored peer on every pick.
        this.tableAt = new MethodTable[readyPool.size()];
        this.stateAt = new SubchannelState[readyPool.size()];
        for (int i = 0; i < readyPool.size(); i++) {
            tableAt[i] = tables.get(readyPool.get(i));
            stateAt[i] = states.get(readyPool.get(i));
        }
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
        // Only unary calls are latency samples; a streaming call's duration is its lifetime.
        final boolean recordLatency =
                args.getMethodDescriptor() == null
                        || args.getMethodDescriptor().getType()
                                == MethodDescriptor.MethodType.UNARY;
        final long now = clocks.nanoTime();
        final int n = readyPool.size();

        // Fast path, O(1): sample two distinct peers and score only those. An ejected sample is
        // replaced by resampling just that slot, so every pick is still a best-of-two
        // comparison between live peers. Accepting the surviving peer unopposed would turn
        // ~2e of picks into a plain random choice when a fraction e of the fleet is ejected.
        // Only when resampling keeps missing (very dense ejection, or tiny pools) do we fall
        // back to scanning the whole pool.
        if (n == 1) {
            LoadBalancer.Subchannel only = readyPool.get(0);
            if (!Double.isInfinite(cost(only, method, now))) {
                metrics.recordPick(OK);
                return buildPickResult(only, method, recordLatency);
            }
        } else {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            // Uniform over ordered pairs of distinct indices.
            int i1 = rnd.nextInt(n);
            int i2 = rnd.nextInt(n - 1);
            if (i2 >= i1) i2++;
            double ca = costAt(i1, method, now);
            double cb = costAt(i2, method, now);
            // With two peers there is nothing else to resample (it would draw the same ejected
            // peer every time): the survivor, if any, is taken below.
            int maxResamples = n > 2 ? MAX_RESAMPLES : 0;
            for (int attempt = 0;
                    attempt < maxResamples && (Double.isInfinite(ca) || Double.isInfinite(cb));
                    attempt++) {
                if (Double.isInfinite(ca)) {
                    i1 = otherIndex(rnd, n, i2);
                    ca = costAt(i1, method, now);
                } else {
                    i2 = otherIndex(rnd, n, i1);
                    cb = costAt(i2, method, now);
                }
            }
            if (!Double.isInfinite(ca) && !Double.isInfinite(cb)) {
                // Sub-promille jitter breaks exact ties so identical peers don't lock-step
                // onto one subchannel.
                LoadBalancer.Subchannel chosen =
                        ca * (1.0 + 1e-4 * rnd.nextDouble()) <= cb * (1.0 + 1e-4 * rnd.nextDouble())
                                ? readyPool.get(i1)
                                : readyPool.get(i2);
                metrics.recordPick(OK);
                return buildPickResult(chosen, method, recordLatency);
            }
            if (n == 2 && (!Double.isInfinite(ca) || !Double.isInfinite(cb))) {
                metrics.recordPick(OK);
                return buildPickResult(
                        readyPool.get(Double.isInfinite(ca) ? i2 : i1), method, recordLatency);
            }
        }
        return pickByScan(method, now, n, recordLatency);
    }

    /** A uniformly random index in [0, n) other than {@code exclude}; requires n >= 2. */
    private static int otherIndex(ThreadLocalRandom rnd, int n, int exclude) {
        int i = rnd.nextInt(n - 1);
        return i >= exclude ? i + 1 : i;
    }

    /**
     * Slow path: score every peer, then best-of-two among the live ones. Only reached when
     * resampling could not find two live peers.
     */
    private PickResult pickByScan(String method, long now, int n, boolean recordLatency) {
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
            return buildPickResult(best, method, recordLatency);
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
        return buildPickResult(chosen, method, recordLatency);
    }

    private PickResult buildPickResult(
            LoadBalancer.Subchannel sc, String method, boolean recordLatency) {
        MethodTable methodTable = tables.get(sc);
        if (methodTable == null) {
            metrics.recordPick(NO_TABLE);
            return PickResult.withNoResult();
        }

        // Cached per (backend, method): the factory is immutable, so allocating one (plus two
        // lambdas) on every pick was pure garbage.
        return PickResult.withSubchannel(
                sc, methodTable.tracerFactory(method, recordLatency, ewmaConfig, clocks, metrics));
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

            MethodStats methodStats = methodTable.peekStats(method);
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
        double score = Math.max(EPS, ms != null ? ms.getEwmaFastMicros() : mt.cachedSeedMicros());
        double busy = 1.0 + inflightWeightEff * Math.max(0, mt.getInflight());
        return score * busy;
    }

    /** Fast-path cost of the peer at {@code i}, using the per-picker resolved state. */
    private double costAt(int i, String method, long now) {
        MethodTable mt = tableAt[i];
        SubchannelState st = stateAt[i];
        if (mt == null || st == null || st.isRemoved()) return Double.POSITIVE_INFINITY;
        if (st.isEjected(now) || mt.isMethodEjected(method, now)) return Double.POSITIVE_INFINITY;
        return peakEwmaCost(
                mt, mt.peekStats(method), st, method, now, inflightWeightEff, ewmaConfig);
    }

    private double cost(LoadBalancer.Subchannel subchannel, String method, long now) {
        MethodTable methodTable = tables.get(subchannel);
        if (methodTable == null) return Double.POSITIVE_INFINITY;

        // Read-only lookup: scoring must not materialise per-method state on peers that are not
        // picked (the tracer creates it for the peer that actually serves the call).
        MethodStats methodStats = methodTable.peekStats(method);
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
     * <p>Every peer scores on its fast (peak) EWMA decayed to {@code now}, so a slow spike is
     * penalised immediately and wears off with time, and unmeasured peers (still on their seed)
     * become cheap enough to be probed within a few half-lives. Multiplied by the busy factor
     * (inflight + 1).
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
        double score =
                Math.max(
                        EPS,
                        ms != null
                                ? decayedPeakMicros(ms, now, cfg)
                                // Never served this method: assume the fleet's typical latency
                                // for it, so the peer competes at par and its own first
                                // responses decide (this replaces the old fixed warmup ramp).
                                : mt.seedMicros(method));

        // latency x (inflight + 1): the expected wait behind the calls already queued there.
        double busy = 1.0 + inflightWeightEff * Math.max(0, mt.getInflight());
        return score * busy;
    }

    /**
     * The peak EWMA decayed to {@code now}. Decay is applied at read time, not only when a sample
     * arrives: P2C stops sending traffic to a backend whose score spiked, so without read-time
     * decay the spike would never wear off (and a never-picked backend would never be probed).
     * Decaying toward zero is the standard Peak-EWMA behaviour (Finagle, tower): an idle backend's
     * cost shrinks until it gets picked again and re-measured.
     */
    static double decayedPeakMicros(MethodStats ms, long now, PeakEwmaConfig cfg) {
        return ms.decayedPeakMicros(now);
    }

    private static String methodName(MethodDescriptor<?, ?> methodDescriptor) {
        return methodDescriptor == null ? "" : methodDescriptor.getFullMethodName();
    }
}
