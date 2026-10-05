package dev.parkerharrelson.grpc.peakewma.opentelemetry;

import static dev.parkerharrelson.grpc.peakewma.LBConstants.KEY;
import static dev.parkerharrelson.grpc.peakewma.LBConstants.METHOD;
import static dev.parkerharrelson.grpc.peakewma.LBConstants.OUTCOME;
import static dev.parkerharrelson.grpc.peakewma.LBConstants.REASON;
import static dev.parkerharrelson.grpc.peakewma.LBConstants.SUBCHANNEL;

import dev.parkerharrelson.grpc.peakewma.metrics.LbMetrics;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * OpenTelemetry-backed {@link LbMetrics} implementation.
 *
 * <p>Uses only the OpenTelemetry API, so it works with whatever SDK and exporter the application
 * configures (OTLP, Prometheus, logging, ...), including the Java agent's {@code
 * GlobalOpenTelemetry}. Counters and the RTT histogram are recorded synchronously; every gauge is
 * an asynchronous instrument whose callback reads the latest value at collection time, so the pick
 * path only writes a {@code volatile double}.
 *
 * <p>Instrument names mirror the Micrometer adapter (minus its {@code .total} counter suffix, which
 * OpenTelemetry leaves to the exporter), so both produce the same Prometheus series, e.g. {@code
 * lb_pick_total} and {@code lb_stream_rtt_seconds}.
 *
 * <pre>{@code
 * PeakEwmaP2CProvider.register(new OpenTelemetryLbMetrics(GlobalOpenTelemetry.get()));
 * }</pre>
 */
public final class OpenTelemetryLbMetrics implements LbMetrics, AutoCloseable {

    /** Instrumentation scope name used when created from an {@link OpenTelemetry} instance. */
    public static final String INSTRUMENTATION_SCOPE = "dev.parkerharrelson.grpc.peakewma";

    private static final AttributeKey<String> SUBCHANNEL_KEY = AttributeKey.stringKey(SUBCHANNEL);
    private static final AttributeKey<String> METHOD_KEY = AttributeKey.stringKey(METHOD);
    private static final AttributeKey<String> OUTCOME_KEY = AttributeKey.stringKey(OUTCOME);
    private static final AttributeKey<String> REASON_KEY = AttributeKey.stringKey(REASON);
    private static final AttributeKey<String> TUNING_KEY = AttributeKey.stringKey(KEY);

    private static final List<Double> RTT_BUCKETS_SECONDS =
            List.of(
                    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                    30.0);

    private final LongCounter picks;
    private final LongCounter ejections;
    private final DoubleHistogram rtt;
    private final DoubleHistogram outlierTick;

    private final GaugeCell readyCount = new GaugeCell(Attributes.empty());
    private final GaugeCell ejectedCount = new GaugeCell(Attributes.empty());
    private final Map<String, GaugeCell> inflightBySub = new ConcurrentHashMap<>();
    private final Map<SubMethod, GaugeCell> costBySubMethod = new ConcurrentHashMap<>();
    private final Map<String, GaugeCell> outlierErrorRateBySub = new ConcurrentHashMap<>();
    private final Map<String, GaugeCell> outlierLatencyRatioBySub = new ConcurrentHashMap<>();
    private final Map<String, GaugeCell> tuningVals = new ConcurrentHashMap<>();
    private final Map<String, GaugeCell> slowByMethod = new ConcurrentHashMap<>();
    private final Map<String, GaugeCell> fastByMethod = new ConcurrentHashMap<>();
    private final Map<String, GaugeCell> rateByMethod = new ConcurrentHashMap<>();
    private final Map<String, GaugeCell> errorRateByMethod = new ConcurrentHashMap<>();
    private final Map<String, GaugeCell> peakHalfLifeByMethod = new ConcurrentHashMap<>();
    private final Map<String, GaugeCell> baselineHalfLifeByMethod = new ConcurrentHashMap<>();
    private final Map<String, GaugeCell> seedByMethod = new ConcurrentHashMap<>();

    // Attribute sets for the synchronous instruments, cached so the hot path does not allocate.
    private final Map<String, Attributes> outcomeAttrs = new ConcurrentHashMap<>();
    private final Map<String, Attributes> methodAttrs = new ConcurrentHashMap<>();

    private final List<AutoCloseable> observables = new CopyOnWriteArrayList<>();

    /** Creates metrics using a meter from the given OpenTelemetry instance. */
    public OpenTelemetryLbMetrics(OpenTelemetry openTelemetry) {
        this(
                Objects.requireNonNull(openTelemetry, "openTelemetry")
                        .getMeter(INSTRUMENTATION_SCOPE));
    }

    /** Creates metrics on the given meter. */
    public OpenTelemetryLbMetrics(Meter meter) {
        Objects.requireNonNull(meter, "meter");

        this.picks =
                meter.counterBuilder("lb.pick")
                        .setDescription("Picker decisions by outcome.")
                        .setUnit("{pick}")
                        .build();
        this.ejections =
                meter.counterBuilder("lb.outlier.ejections")
                        .setDescription("Subchannels ejected for outlier behaviour.")
                        .setUnit("{ejection}")
                        .build();
        this.rtt =
                meter.histogramBuilder("lb.stream.rtt")
                        .setDescription("Observed per-stream RTT fed into the Peak-EWMA picker.")
                        .setUnit("s")
                        .setExplicitBucketBoundariesAdvice(RTT_BUCKETS_SECONDS)
                        .build();

        gauge(meter, "lb.ready.subchannels", "Ready subchannels.", "{subchannel}", readyCount);
        gauge(
                meter,
                "lb.ejected.subchannels",
                "Ejected subchannels.",
                "{subchannel}",
                ejectedCount);
        gauge(meter, "lb.subchannel.inflight", "Inflight streams.", "{stream}", inflightBySub);
        gauge(meter, "lb.subchannel.method.cost", "Picker cost.", "1", costBySubMethod);
        gauge(
                meter,
                "lb.outlier.last_error_rate",
                "Error rate at the last ejection.",
                "1",
                outlierErrorRateBySub);
        gauge(
                meter,
                "lb.outlier.last_latency_ratio",
                "Latency ratio at the last ejection.",
                "1",
                outlierLatencyRatioBySub);
        gauge(meter, "lb.tuning.value", "Adaptively tuned values.", "1", tuningVals);
        gauge(
                meter,
                "lb.method.latency_ewma_slow_micros",
                "Slow latency EWMA.",
                "us",
                slowByMethod);
        gauge(
                meter,
                "lb.method.latency_ewma_fast_micros",
                "Fast latency EWMA.",
                "us",
                fastByMethod);
        gauge(meter, "lb.method.rate_per_sec", "Call rate.", "{call}/s", rateByMethod);
        gauge(meter, "lb.method.error_rate", "Error rate.", "1", errorRateByMethod);
        gauge(
                meter,
                "lb.method.peak_half_life_millis",
                "Fleet-derived peak EWMA half-life.",
                "ms",
                peakHalfLifeByMethod);
        gauge(
                meter,
                "lb.method.baseline_half_life_millis",
                "Fleet-derived baseline EWMA half-life.",
                "ms",
                baselineHalfLifeByMethod);
        gauge(meter, "lb.method.seed_micros", "Seed latency for new backends.", "us", seedByMethod);
        this.outlierTick =
                meter.histogramBuilder("lb.outlier.tick")
                        .setDescription("Outlier tick duration on the synchronization context.")
                        .setUnit("s")
                        .build();
    }

    @Override
    public void setMethodScale(
            String method,
            double peakHalfLifeMillis,
            double baselineHalfLifeMillis,
            double seedMicros) {
        if (method == null) return;
        cell(peakHalfLifeByMethod, method, this::methodAttrs).value = peakHalfLifeMillis;
        cell(baselineHalfLifeByMethod, method, this::methodAttrs).value = baselineHalfLifeMillis;
        cell(seedByMethod, method, this::methodAttrs).value = seedMicros;
    }

    @Override
    public void recordOutlierTick(long durationNanos) {
        outlierTick.record(durationNanos / 1_000_000_000.0);
    }

    @Override
    public void recordPick(String outcome) {
        if (outcome == null) return;
        picks.add(1, outcomeAttrs.computeIfAbsent(outcome, o -> Attributes.of(OUTCOME_KEY, o)));
    }

    @Override
    public void setInflight(String subchannelId, int inflight) {
        if (subchannelId == null) return;
        cell(inflightBySub, subchannelId, OpenTelemetryLbMetrics::subchannelAttrs).value = inflight;
    }

    @Override
    public void setCost(String subchannelId, String method, double cost) {
        if (subchannelId == null || method == null) return;
        cell(
                                costBySubMethod,
                                new SubMethod(subchannelId, method),
                                k ->
                                        Attributes.of(
                                                SUBCHANNEL_KEY, k.subchannel, METHOD_KEY, k.method))
                        .value =
                cost;
    }

    @Override
    public void recordOutlierEjection(
            String subchannelId, String reason, double errorRate, double latencyRatio) {
        if (subchannelId == null) return;
        ejections.add(
                1,
                Attributes.of(
                        SUBCHANNEL_KEY,
                        subchannelId,
                        REASON_KEY,
                        reason != null ? reason : "unknown"));
        cell(outlierErrorRateBySub, subchannelId, OpenTelemetryLbMetrics::subchannelAttrs).value =
                errorRate;
        cell(outlierLatencyRatioBySub, subchannelId, OpenTelemetryLbMetrics::subchannelAttrs)
                        .value =
                latencyRatio;
    }

    @Override
    public void setReadySubchannelCount(int readyCount) {
        this.readyCount.value = readyCount;
    }

    @Override
    public void setEjectedSubchannelCount(int ejectedCount) {
        this.ejectedCount.value = ejectedCount;
    }

    @Override
    public void setAdaptiveTuning(String key, double value) {
        if (key == null) return;
        cell(tuningVals, key, k -> Attributes.of(TUNING_KEY, k)).value = value;
    }

    @Override
    public void setMethodLatencyEwma(String method, double slowEwmaMicros, double fastEwmaMicros) {
        if (method == null) return;
        cell(slowByMethod, method, this::methodAttrs).value = slowEwmaMicros;
        cell(fastByMethod, method, this::methodAttrs).value = fastEwmaMicros;
    }

    @Override
    public void removeSubchannel(String subchannelId) {
        if (subchannelId == null) return;
        // Async gauges stop reporting a series as soon as its cell is gone.
        inflightBySub.remove(subchannelId);
        outlierErrorRateBySub.remove(subchannelId);
        outlierLatencyRatioBySub.remove(subchannelId);
        costBySubMethod.keySet().removeIf(k -> k.subchannel.equals(subchannelId));
    }

    @Override
    public void setMethodRate(String method, double ratePerSec) {
        if (method == null) return;
        cell(rateByMethod, method, this::methodAttrs).value = ratePerSec;
    }

    @Override
    public void setMethodErrorRate(String method, double errorRate) {
        if (method == null) return;
        cell(errorRateByMethod, method, this::methodAttrs).value = errorRate;
    }

    @Override
    public void recordObservedRtt(String method, long rttNanos) {
        if (method == null || rttNanos <= 0L) return;
        rtt.record(rttNanos / 1_000_000_000.0, methodAttrs(method));
    }

    /** Unregisters all asynchronous gauge callbacks. */
    @Override
    public void close() {
        for (AutoCloseable c : observables) {
            try {
                c.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
        observables.clear();
    }

    private Attributes methodAttrs(String method) {
        return methodAttrs.computeIfAbsent(method, m -> Attributes.of(METHOD_KEY, m));
    }

    private static Attributes subchannelAttrs(String subchannelId) {
        return Attributes.of(SUBCHANNEL_KEY, subchannelId);
    }

    private static <K> GaugeCell cell(
            Map<K, GaugeCell> cells, K key, Function<K, Attributes> attributes) {
        GaugeCell c = cells.get(key);
        return c != null ? c : cells.computeIfAbsent(key, k -> new GaugeCell(attributes.apply(k)));
    }

    private void gauge(Meter meter, String name, String description, String unit, GaugeCell cell) {
        gauge(meter, name, description, unit, Map.of("", cell));
    }

    private void gauge(
            Meter meter, String name, String description, String unit, Map<?, GaugeCell> cells) {
        observables.add(
                meter.gaugeBuilder(name)
                        .setDescription(description)
                        .setUnit(unit)
                        .buildWithCallback((ObservableDoubleMeasurement m) -> observe(m, cells)));
    }

    private static void observe(ObservableDoubleMeasurement m, Map<?, GaugeCell> cells) {
        for (GaugeCell c : cells.values()) {
            m.record(c.value, c.attributes);
        }
    }

    private static final class GaugeCell {
        final Attributes attributes;
        volatile double value;

        GaugeCell(Attributes attributes) {
            this.attributes = attributes;
        }
    }

    private record SubMethod(String subchannel, String method) {}
}
