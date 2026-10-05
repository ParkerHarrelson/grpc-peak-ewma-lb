package dev.parkerharrelson.grpc.peakewma.loadtest;

import dev.parkerharrelson.grpc.peakewma.metrics.LbMetrics;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Recorder;

/**
 * {@link LbMetrics} sink that keeps what the load-test report needs: pick outcomes, ejections by
 * reason, the fleet-derived scale per method, cost gauges, and the outlier-tick durations. The
 * loadgen drains it once per interval.
 */
final class RecordingLbMetrics implements LbMetrics {

    record Ejection(String subchannel, String reason, double errorRate, double latencyRatio) {}

    final ConcurrentHashMap<String, LongAdder> picks = new ConcurrentHashMap<>();
    final ConcurrentLinkedQueue<Ejection> ejections = new ConcurrentLinkedQueue<>();
    final ConcurrentHashMap<String, double[]> scales = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Double> costs = new ConcurrentHashMap<>(); // "sc|method"
    final ConcurrentHashMap<String, Double> tuning = new ConcurrentHashMap<>();
    final Recorder tickNanos = new Recorder(3);
    final LongAdder tickBusyNanos = new LongAdder();
    volatile int ready;
    volatile int ejected;
    final boolean keepCosts;

    RecordingLbMetrics(boolean keepCosts) {
        this.keepCosts = keepCosts;
    }

    @Override
    public void recordPick(String outcome) {
        LongAdder a = picks.get(outcome);
        (a != null ? a : picks.computeIfAbsent(outcome, k -> new LongAdder())).increment();
    }

    @Override
    public void setInflight(String subchannelId, int inflight) {}

    @Override
    public void setCost(String subchannelId, String method, double cost) {
        if (keepCosts) costs.put(subchannelId + "|" + method, cost);
    }

    @Override
    public void recordOutlierEjection(
            String subchannelId, String reason, double errorRate, double latencyRatio) {
        ejections.add(new Ejection(subchannelId, reason, errorRate, latencyRatio));
    }

    @Override
    public void setReadySubchannelCount(int readyCount) {
        ready = readyCount;
    }

    @Override
    public void setEjectedSubchannelCount(int ejectedCount) {
        ejected = ejectedCount;
    }

    @Override
    public void setAdaptiveTuning(String key, double value) {
        tuning.put(key, value);
    }

    @Override
    public void setMethodLatencyEwma(String method, double slowEwmaMicros, double fastEwmaMicros) {}

    @Override
    public void removeSubchannel(String subchannelId) {
        if (keepCosts) costs.keySet().removeIf(k -> k.startsWith(subchannelId + "|"));
    }

    @Override
    public void setMethodRate(String method, double ratePerSec) {}

    @Override
    public void setMethodErrorRate(String method, double errorRate) {}

    @Override
    public void setMethodScale(
            String method,
            double peakHalfLifeMillis,
            double baselineHalfLifeMillis,
            double seedMicros) {
        scales.put(method, new double[] {peakHalfLifeMillis, baselineHalfLifeMillis, seedMicros});
    }

    @Override
    public void recordOutlierTick(long durationNanos) {
        tickNanos.recordValue(Math.max(1, durationNanos));
        tickBusyNanos.add(durationNanos);
    }

    Map<String, Long> drainPicks() {
        Map<String, Long> out = new java.util.TreeMap<>();
        picks.forEach((k, v) -> out.put(k, v.sumThenReset()));
        return out;
    }
}
