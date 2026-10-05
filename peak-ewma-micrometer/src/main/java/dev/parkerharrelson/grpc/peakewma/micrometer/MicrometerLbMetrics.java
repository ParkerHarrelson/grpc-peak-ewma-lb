package dev.parkerharrelson.grpc.peakewma.micrometer;

import static dev.parkerharrelson.grpc.peakewma.LBConstants.*;

import dev.parkerharrelson.grpc.peakewma.metrics.LbMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Micrometer-backed {@link LbMetrics} implementation.
 *
 * <p>Works with any Micrometer {@link MeterRegistry} (Prometheus, Datadog, OTLP, JMX, ...). Per
 * subchannel gauges (inflight, cost, outlier state) are tracked by subchannel id, and {@link
 * #removeSubchannel(String)} unregisters them so backends stop reporting dead subchannels.
 *
 * <pre>{@code
 * PeakEwmaP2CProvider.register(new MicrometerLbMetrics(meterRegistry));
 * }</pre>
 */
public final class MicrometerLbMetrics implements LbMetrics {

    private final MeterRegistry registry;
    private final AtomicInteger readyCountGauge = new AtomicInteger(0);
    private final AtomicInteger ejectedCountGauge = new AtomicInteger(0);
    private final Map<String, AtomicInteger> inflightBySub = new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<Double>> costBySubMethod = new ConcurrentHashMap<>();
    private final Map<String, CopyOnWriteArrayList<Meter>> metersBySub = new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<Double>> slowByMethod = new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<Double>> fastByMethod = new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<Double>> tuningVals = new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<Double>> rateByMethod = new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<Double>> errorRateByMethod =
            new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<Double>> outlierErrorRateBySub =
            new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<Double>> outlierLatencyRatioBySub =
            new ConcurrentHashMap<>();
    private final Map<String, Timer> rttTimerByMethod = new ConcurrentHashMap<>();
    private final Map<String, AtomicReference<Double>> scaleValues = new ConcurrentHashMap<>();
    private volatile Timer outlierTickTimer;

    public MicrometerLbMetrics(MeterRegistry registry) {
        this.registry = registry;

        Gauge.builder("lb.ready.subchannels", readyCountGauge, AtomicInteger::get)
                .register(registry);

        Gauge.builder("lb.ejected.subchannels", ejectedCountGauge, AtomicInteger::get)
                .register(registry);
    }

    @Override
    public void recordPick(String outcome) {
        Counter.builder("lb.pick.total").tag(OUTCOME, outcome).register(registry).increment();
    }

    @Override
    public void setInflight(String subchannelId, int inflight) {
        if (subchannelId == null) return;

        AtomicInteger holder =
                inflightBySub.computeIfAbsent(
                        subchannelId,
                        id -> {
                            AtomicInteger h = new AtomicInteger(inflight);
                            Gauge g =
                                    Gauge.builder("lb.subchannel.inflight", h, AtomicInteger::get)
                                            .tag(SUBCHANNEL, id)
                                            .register(registry);
                            metersBySub
                                    .computeIfAbsent(id, k -> new CopyOnWriteArrayList<>())
                                    .add(g);
                            return h;
                        });

        holder.set(inflight);
    }

    @Override
    public void setCost(String subchannelId, String method, double cost) {
        if (subchannelId == null || method == null) return;

        final String key = subchannelId + "|" + method;

        AtomicReference<Double> ref =
                costBySubMethod.computeIfAbsent(
                        key,
                        k -> {
                            AtomicReference<Double> r = new AtomicReference<>(cost);
                            Gauge g =
                                    Gauge.builder(
                                                    "lb.subchannel.method.cost",
                                                    r,
                                                    AtomicReference::get)
                                            .tag(SUBCHANNEL, subchannelId)
                                            .tag(METHOD, method)
                                            .register(registry);
                            metersBySub
                                    .computeIfAbsent(
                                            subchannelId, sid -> new CopyOnWriteArrayList<>())
                                    .add(g);
                            return r;
                        });

        ref.set(cost);
    }

    @Override
    public void recordOutlierEjection(
            String subchannelId, String reason, double errorRate, double latencyRatio) {
        if (subchannelId == null) {
            return;
        }

        Counter.builder("lb.outlier.ejections")
                .tag(SUBCHANNEL, subchannelId)
                .tag(REASON, reason)
                .register(registry)
                .increment();

        AtomicReference<Double> errRef =
                outlierErrorRateBySub.computeIfAbsent(
                        subchannelId,
                        id -> {
                            AtomicReference<Double> r = new AtomicReference<>(errorRate);
                            Gauge g =
                                    Gauge.builder(
                                                    "lb.outlier.last_error_rate",
                                                    r,
                                                    AtomicReference::get)
                                            .tag(SUBCHANNEL, id)
                                            .register(registry);
                            metersBySub
                                    .computeIfAbsent(id, k -> new CopyOnWriteArrayList<>())
                                    .add(g);
                            return r;
                        });
        errRef.set(errorRate);

        AtomicReference<Double> latRef =
                outlierLatencyRatioBySub.computeIfAbsent(
                        subchannelId,
                        id -> {
                            AtomicReference<Double> r = new AtomicReference<>(latencyRatio);
                            Gauge g =
                                    Gauge.builder(
                                                    "lb.outlier.last_latency_ratio",
                                                    r,
                                                    AtomicReference::get)
                                            .tag(SUBCHANNEL, id)
                                            .register(registry);
                            metersBySub
                                    .computeIfAbsent(id, k -> new CopyOnWriteArrayList<>())
                                    .add(g);
                            return r;
                        });
        latRef.set(latencyRatio);
    }

    @Override
    public void setReadySubchannelCount(int readyCount) {
        readyCountGauge.set(readyCount);
    }

    @Override
    public void setEjectedSubchannelCount(int ejectedCount) {
        ejectedCountGauge.set(ejectedCount);
    }

    @Override
    public void setAdaptiveTuning(String key, double value) {
        if (key == null) return;

        AtomicReference<Double> ref =
                tuningVals.computeIfAbsent(
                        key,
                        k -> {
                            AtomicReference<Double> r = new AtomicReference<>(value);
                            Gauge.builder("lb.tuning.value", r, AtomicReference::get)
                                    .tag(KEY, k)
                                    .register(registry);
                            return r;
                        });
        ref.set(value);
    }

    @Override
    public void setMethodLatencyEwma(String method, double slowEwmaMicros, double fastEwmaMicros) {
        if (method == null) return;

        AtomicReference<Double> slowRef =
                slowByMethod.computeIfAbsent(
                        method,
                        m -> {
                            AtomicReference<Double> r = new AtomicReference<>(slowEwmaMicros);
                            Gauge.builder(
                                            "lb.method.latency_ewma_slow_micros",
                                            r,
                                            AtomicReference::get)
                                    .tag(METHOD, method)
                                    .register(registry);
                            return r;
                        });
        slowRef.set(slowEwmaMicros);

        AtomicReference<Double> fastRef =
                fastByMethod.computeIfAbsent(
                        method,
                        m -> {
                            AtomicReference<Double> r = new AtomicReference<>(fastEwmaMicros);
                            Gauge.builder(
                                            "lb.method.latency_ewma_fast_micros",
                                            r,
                                            AtomicReference::get)
                                    .tag(METHOD, method)
                                    .register(registry);
                            return r;
                        });
        fastRef.set(fastEwmaMicros);
    }

    @Override
    public void removeSubchannel(String subchannelId) {
        if (subchannelId == null) return;
        List<Meter> meters = metersBySub.remove(subchannelId);
        if (meters != null) {
            for (Meter m : meters) {
                try {
                    registry.remove(m);
                } catch (Exception ignored) {
                    // ignore
                }
            }
        }

        inflightBySub.remove(subchannelId);
        costBySubMethod.keySet().removeIf(k -> k.startsWith(subchannelId + "|"));
    }

    @Override
    public void setMethodRate(String method, double ratePerSec) {
        if (method == null) return;

        AtomicReference<Double> ref =
                rateByMethod.computeIfAbsent(
                        method,
                        m -> {
                            AtomicReference<Double> r = new AtomicReference<>(ratePerSec);
                            Gauge.builder("lb.method.rate_per_sec", r, AtomicReference::get)
                                    .tag(METHOD, m)
                                    .register(registry);
                            return r;
                        });
        ref.set(ratePerSec);
    }

    @Override
    public void setMethodErrorRate(String method, double errorRate) {
        if (method == null) return;

        AtomicReference<Double> ref =
                errorRateByMethod.computeIfAbsent(
                        method,
                        m -> {
                            AtomicReference<Double> r = new AtomicReference<>(errorRate);
                            Gauge.builder("lb.method.error_rate", r, AtomicReference::get)
                                    .tag(METHOD, m)
                                    .register(registry);
                            return r;
                        });
        ref.set(errorRate);
    }

    @Override
    public void recordObservedRtt(String method, long rttNanos) {
        if (method == null || rttNanos <= 0L) return;

        Timer timer =
                rttTimerByMethod.computeIfAbsent(
                        method,
                        m ->
                                Timer.builder("lb.stream.rtt")
                                        .tag(METHOD, m)
                                        .description(
                                                "Observed per-stream RTT fed into the Peak-EWMA"
                                                    + " picker; paired with"
                                                    + " lb.method.latency_ewma_* gauges so"
                                                    + " dashboards can correlate EWMA state with"
                                                    + " real quantiles.")
                                        .publishPercentiles(0.5, 0.95, 0.99)
                                        .publishPercentileHistogram()
                                        .minimumExpectedValue(Duration.ofMillis(1))
                                        .maximumExpectedValue(Duration.ofSeconds(30))
                                        .register(registry));
        timer.record(rttNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void setMethodScale(
            String method,
            double peakHalfLifeMillis,
            double baselineHalfLifeMillis,
            double seedMicros) {
        if (method == null) return;
        scaleGauge("lb.method.peak_half_life_millis", method, peakHalfLifeMillis);
        scaleGauge("lb.method.baseline_half_life_millis", method, baselineHalfLifeMillis);
        scaleGauge("lb.method.seed_micros", method, seedMicros);
    }

    private void scaleGauge(String name, String method, double value) {
        scaleValues
                .computeIfAbsent(
                        name + "|" + method,
                        k -> {
                            AtomicReference<Double> r = new AtomicReference<>(value);
                            Gauge.builder(name, r, AtomicReference::get)
                                    .tag(METHOD, method)
                                    .register(registry);
                            return r;
                        })
                .set(value);
    }

    @Override
    public void recordOutlierTick(long durationNanos) {
        Timer t = outlierTickTimer;
        if (t == null) {
            t =
                    Timer.builder("lb.outlier.tick")
                            .description("Outlier tick duration on the synchronization context.")
                            .publishPercentiles(0.5, 0.99)
                            .register(registry);
            outlierTickTimer = t;
        }
        t.record(durationNanos, TimeUnit.NANOSECONDS);
    }
}
