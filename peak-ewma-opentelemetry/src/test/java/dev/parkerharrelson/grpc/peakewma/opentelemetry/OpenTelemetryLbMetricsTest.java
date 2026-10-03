package dev.parkerharrelson.grpc.peakewma.opentelemetry;

import static org.assertj.core.api.Assertions.assertThat;

import dev.parkerharrelson.grpc.peakewma.PeakEwmaConfigKeys;
import dev.parkerharrelson.grpc.peakewma.PeakEwmaP2CProvider;
import io.grpc.LoadBalancerRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OpenTelemetryLbMetricsTest {

    private static final AttributeKey<String> SUBCHANNEL = AttributeKey.stringKey("subchannel");
    private static final AttributeKey<String> METHOD = AttributeKey.stringKey("method");
    private static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("outcome");

    private InMemoryMetricReader reader;
    private SdkMeterProvider meterProvider;
    private OpenTelemetryLbMetrics metrics;

    @BeforeEach
    void setUp() {
        reader = InMemoryMetricReader.create();
        meterProvider = SdkMeterProvider.builder().registerMetricReader(reader).build();
        metrics =
                new OpenTelemetryLbMetrics(
                        meterProvider.get(OpenTelemetryLbMetrics.INSTRUMENTATION_SCOPE));
    }

    @AfterEach
    void tearDown() {
        metrics.close();
        meterProvider.close();
    }

    @Test
    void recordPick_withOutcomes_countsPerOutcome() {
        metrics.recordPick("ok");
        metrics.recordPick("ok");
        metrics.recordPick("no_ready");

        Collection<MetricData> data = reader.collectAllMetrics();

        assertThat(longSum(data, "lb.pick", Attributes.of(OUTCOME, "ok"))).isEqualTo(2);
        assertThat(longSum(data, "lb.pick", Attributes.of(OUTCOME, "no_ready"))).isEqualTo(1);
    }

    @Test
    void setInflight_withUpdates_reportsLatestValue() {
        metrics.setInflight("sc-1", 3);
        metrics.setInflight("sc-1", 7);

        Collection<MetricData> data = reader.collectAllMetrics();

        assertThat(gauge(data, "lb.subchannel.inflight", Attributes.of(SUBCHANNEL, "sc-1")))
                .contains(7.0);
    }

    @Test
    void readyAndEjectedCounts_whenSet_areReported() {
        metrics.setReadySubchannelCount(4);
        metrics.setEjectedSubchannelCount(1);

        Collection<MetricData> data = reader.collectAllMetrics();

        assertThat(gauge(data, "lb.ready.subchannels", Attributes.empty())).contains(4.0);
        assertThat(gauge(data, "lb.ejected.subchannels", Attributes.empty())).contains(1.0);
    }

    @Test
    void removeSubchannel_afterPublishing_stopsReportingItsSeries() {
        metrics.setInflight("sc-1", 1);
        metrics.setInflight("sc-2", 2);
        metrics.setCost("sc-1", "svc/Method", 1.5);
        metrics.recordOutlierEjection("sc-1", "latency", 0.3, 2.0);

        metrics.removeSubchannel("sc-1");
        Collection<MetricData> data = reader.collectAllMetrics();

        assertThat(gauge(data, "lb.subchannel.inflight", Attributes.of(SUBCHANNEL, "sc-1")))
                .isEmpty();
        assertThat(gauge(data, "lb.subchannel.inflight", Attributes.of(SUBCHANNEL, "sc-2")))
                .contains(2.0);
        assertThat(
                        gauge(
                                data,
                                "lb.subchannel.method.cost",
                                Attributes.of(SUBCHANNEL, "sc-1", METHOD, "svc/Method")))
                .isEmpty();
        assertThat(gauge(data, "lb.outlier.last_error_rate", Attributes.of(SUBCHANNEL, "sc-1")))
                .isEmpty();
    }

    @Test
    void recordOutlierEjection_withSubchannel_countsAndPublishesLastValues() {
        metrics.recordOutlierEjection("sc-1", "errors", 0.4, 1.2);

        Collection<MetricData> data = reader.collectAllMetrics();

        assertThat(
                        longSum(
                                data,
                                "lb.outlier.ejections",
                                Attributes.of(
                                        SUBCHANNEL,
                                        "sc-1",
                                        AttributeKey.stringKey("reason"),
                                        "errors")))
                .isEqualTo(1);
        assertThat(gauge(data, "lb.outlier.last_error_rate", Attributes.of(SUBCHANNEL, "sc-1")))
                .contains(0.4);
        assertThat(gauge(data, "lb.outlier.last_latency_ratio", Attributes.of(SUBCHANNEL, "sc-1")))
                .contains(1.2);
    }

    @Test
    void methodGauges_whenSet_areReportedPerMethod() {
        metrics.setMethodLatencyEwma("svc/A", 1200.0, 900.0);
        metrics.setMethodRate("svc/A", 50.0);
        metrics.setMethodErrorRate("svc/A", 0.05);
        metrics.setAdaptiveTuning("inflightWeightEff", 0.2);

        Collection<MetricData> data = reader.collectAllMetrics();
        Attributes a = Attributes.of(METHOD, "svc/A");

        assertThat(gauge(data, "lb.method.latency_ewma_slow_micros", a)).contains(1200.0);
        assertThat(gauge(data, "lb.method.latency_ewma_fast_micros", a)).contains(900.0);
        assertThat(gauge(data, "lb.method.rate_per_sec", a)).contains(50.0);
        assertThat(gauge(data, "lb.method.error_rate", a)).contains(0.05);
        assertThat(
                        gauge(
                                data,
                                "lb.tuning.value",
                                Attributes.of(AttributeKey.stringKey("key"), "inflightWeightEff")))
                .contains(0.2);
    }

    @Test
    void recordObservedRtt_withPositiveSample_recordsSecondsHistogram() {
        metrics.recordObservedRtt("svc/A", 25_000_000L); // 25ms
        metrics.recordObservedRtt("svc/A", 0L); // ignored
        metrics.recordObservedRtt(null, 10L); // ignored

        Collection<MetricData> data = reader.collectAllMetrics();
        HistogramPointData point =
                find(data, "lb.stream.rtt").getHistogramData().getPoints().stream()
                        .filter(p -> p.getAttributes().equals(Attributes.of(METHOD, "svc/A")))
                        .findFirst()
                        .orElseThrow();

        assertThat(point.getCount()).isEqualTo(1);
        assertThat(point.getSum()).isEqualTo(0.025);
        assertThat(find(data, "lb.stream.rtt").getUnit()).isEqualTo("s");
    }

    @Test
    void nullKeys_areIgnored() {
        metrics.recordPick(null);
        metrics.setInflight(null, 1);
        metrics.setCost(null, "m", 1.0);
        metrics.setCost("sc", null, 1.0);
        metrics.recordOutlierEjection(null, "r", 0, 0);
        metrics.setAdaptiveTuning(null, 1.0);
        metrics.setMethodLatencyEwma(null, 1, 1);
        metrics.setMethodRate(null, 1);
        metrics.setMethodErrorRate(null, 1);
        metrics.removeSubchannel(null);

        Collection<MetricData> data = reader.collectAllMetrics();

        assertThat(data.stream().map(MetricData::getName))
                .doesNotContain("lb.pick", "lb.outlier.ejections");
    }

    @Test
    void close_unregistersGaugeCallbacks() {
        metrics.setInflight("sc-1", 1);
        metrics.close();

        Collection<MetricData> data = reader.collectAllMetrics();

        assertThat(data.stream().map(MetricData::getName)).doesNotContain("lb.subchannel.inflight");
    }

    @Test
    void register_withOtelMetrics_isSelectedForPolicy() {
        LoadBalancerRegistry registry = new LoadBalancerRegistry();
        registry.register(new PeakEwmaP2CProvider());

        PeakEwmaP2CProvider registered = PeakEwmaP2CProvider.register(registry, metrics);

        assertThat(registry.getProvider(PeakEwmaConfigKeys.POLICY_NAME)).isSameAs(registered);
    }

    private static MetricData find(Collection<MetricData> data, String name) {
        return data.stream().filter(m -> m.getName().equals(name)).findFirst().orElseThrow();
    }

    private static long longSum(Collection<MetricData> data, String name, Attributes attrs) {
        return find(data, name).getLongSumData().getPoints().stream()
                .filter(p -> p.getAttributes().equals(attrs))
                .mapToLong(LongPointData::getValue)
                .sum();
    }

    private static Optional<Double> gauge(
            Collection<MetricData> data, String name, Attributes attrs) {
        List<DoublePointData> points =
                data.stream()
                        .filter(m -> m.getName().equals(name))
                        .flatMap(m -> m.getDoubleGaugeData().getPoints().stream())
                        .filter(p -> p.getAttributes().equals(attrs))
                        .toList();
        return points.stream().findFirst().map(DoublePointData::getValue);
    }
}
