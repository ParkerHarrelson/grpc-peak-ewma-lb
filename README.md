# grpc-peak-ewma-lb

A latency-aware load balancing policy for [grpc-java](https://github.com/grpc/grpc-java).

grpc-java ships `pick_first` and `round_robin`. Neither looks at how backends are actually
performing, so one slow pod (GC pause, noisy neighbour, cold cache) still gets its full share of
traffic and drags tail latency up for everyone. `peak_ewma_p2c` routes away from slow and failing
backends using **Peak EWMA** latency scoring with **Power of Two Choices** selection, plus built-in
outlier ejection.

It is a standard `LoadBalancerProvider`. Add the jar and set the policy name, and any grpc-java
channel can use it. No framework required.

```java
ManagedChannel channel =
        ManagedChannelBuilder.forTarget("dns:///my-service.internal:9090")
                .defaultLoadBalancingPolicy("peak_ewma_p2c")
                .usePlaintext()
                .build();
```

## Modules

| Module                    | What it is                                                                 | Depends on                 |
|---------------------------|----------------------------------------------------------------------------|----------------------------|
| `peak-ewma-core`          | The load balancer. Auto-registers via SPI as `peak_ewma_p2c`.              | `grpc-api`, `slf4j-api`    |
| `peak-ewma-micrometer`    | `LbMetrics` adapter for any Micrometer `MeterRegistry`.                    | core, `micrometer-core`    |
| `peak-ewma-opentelemetry` | `LbMetrics` adapter for the OpenTelemetry metrics API.                     | core, `opentelemetry-api`  |
| `peak-ewma-harness`       | In-process benchmark rig with fault injection. Not published.              | core, micrometer, netty    |
| `peak-ewma-bench`         | CPU/memory overhead + JMH comparison vs grpc-java's built-in policies. Not published. See [its README](peak-ewma-bench/README.md). | core, harness, grpc-xds, JMH |

```xml
<dependency>
    <groupId>dev.parkerharrelson.grpc</groupId>
    <artifactId>peak-ewma-core</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## How it works

For every RPC the picker samples **two** ready subchannels at random and sends the call to the one
with the lower cost. Choosing the better of two random options is the
[Power of Two Choices](https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf): it gets
most of the benefit of always picking the global best, without every client stampeding the same
"best" backend.

**Cost** is computed per `(subchannel, gRPC method)`, so a slow `Search` call doesn't penalise a fast
`Get` on the same backend:

```
cost = latencyEwma × (1 + inflightWeight × inflight) × warmupFactor
```

- **Two EWMAs per method.**
  - *Fast (peak) EWMA:* `max(rtt, previous × decay)`. A single slow response raises the score
    right away, and the score decays back down over time.
  - *Slow EWMA:* standard smoothing, used for cold or stale peers and as the baseline for outlier
    detection.
- **Inflight penalty.** Backends with more outstanding streams cost more, which spreads load before
  latency degrades. The weight adapts to the cluster's median inflight count.
- **Warmup.** A newly connected backend starts at 2× cost, decaying to 1× so it isn't flooded
  before its JIT and caches are warm.
- **Adaptive decay.** Half-lives are tuned from each method's observed coefficient of variation, so
  noisy methods don't get stuck on an old peak.

**Outlier ejection.** A background tick tracks per-subchannel error rate over a sliding window and
the fast/slow latency ratio. If a backend crosses `outlierErrorRate` or `outlierLatencyMultiplier`,
it is ejected for `outlierEjectMillis`. A re-entry cooldown prevents flapping. If every backend is
ejected, the picker falls back to the least-bad one rather than failing calls.

RTTs come from a `ClientStreamTracer` attached to each pick, so no interceptors are needed.

## Configuration

The defaults work without any configuration. To tune, use the standard gRPC service config:

```java
Map<String, ?> serviceConfig =
        Map.of(
                "loadBalancingConfig",
                List.of(Map.of("peak_ewma_p2c", Map.of("inflightWeight", 0.25))));

ManagedChannelBuilder.forTarget("dns:///my-service.internal:9090")
        .defaultServiceConfig(serviceConfig)
        .disableServiceConfigLookUp()
        .usePlaintext()
        .build();
```

| Key                            | Default  | Meaning                                                        |
|--------------------------------|----------|----------------------------------------------------------------|
| `tauFastMillis`                | `1000`   | Base half-life of the fast (peak) EWMA                         |
| `tauSlowMillis`                | `30000`  | Base half-life of the slow EWMA                                |
| `inflightWeight`               | `0.15`   | Base cost multiplier per outstanding stream                    |
| `initialRttMicros`             | `50000`  | Seed latency for methods with no samples                       |
| `outlierEnabled`               | `true`   | Enable outlier ejection                                        |
| `outlierWindowMillis`          | `15000`  | Sliding window for error-rate tracking                         |
| `outlierErrorRate`             | `0.20`   | Error rate that triggers ejection                              |
| `outlierLatencyMultiplier`     | `2.5`    | Fast/slow latency ratio that triggers ejection                 |
| `outlierEjectMillis`           | `15000`  | How long an ejected backend stays out                          |
| `outlierReentryCooldownMillis` | `5000`   | Minimum time between re-entry and another ejection             |
| `outlierTickIntervalMillis`    | `1000`   | Outlier evaluation period (min 100)                            |
| `staleMillisForRatio`          | `30000`  | Ignore latency ratio if samples are older than this            |
| `methodMaxEntries`             | `512`    | Max tracked methods per subchannel                             |
| `methodPruneStaleAfterMillis`  | `120000` | Drop per-method stats that haven't been updated in this long   |

## Metrics

The core module depends on no metrics library. It reports through a small interface,
[`LbMetrics`](peak-ewma-core/src/main/java/dev/parkerharrelson/grpc/peakewma/metrics/LbMetrics.java),
and defaults to a no-op implementation. To publish metrics, register a provider that has a real sink
**once at startup**, before you build channels:

```java
// Micrometer: Prometheus, Datadog, OTLP, JMX, ... whatever registry you already have
PeakEwmaP2CProvider.register(new MicrometerLbMetrics(meterRegistry));

// OpenTelemetry: SDK, Java agent, any exporter
PeakEwmaP2CProvider.register(new OpenTelemetryLbMetrics(GlobalOpenTelemetry.get()));
```

`register` adds the provider to gRPC's `LoadBalancerRegistry` at a higher priority than the
SPI-loaded default, so every channel using `peak_ewma_p2c` picks it up. It returns the provider
so you can `deregister` it later. For any other backend, implement `LbMetrics` yourself.

Both adapters emit the same series. Names are shown in OpenTelemetry form; the Micrometer adapter
names the pick counter `lb.pick.total`, and both export to Prometheus as `lb_pick_total`.

| Metric                                                     | Type      | Tags                    |
|------------------------------------------------------------|-----------|-------------------------|
| `lb.pick`                                                  | counter   | `outcome`               |
| `lb.outlier.ejections`                                     | counter   | `subchannel`, `reason`  |
| `lb.stream.rtt`                                            | histogram | `method`                |
| `lb.ready.subchannels` / `lb.ejected.subchannels`          | gauge     |                         |
| `lb.subchannel.inflight`                                   | gauge     | `subchannel`            |
| `lb.subchannel.method.cost`                                | gauge     | `subchannel`, `method`  |
| `lb.method.latency_ewma_{slow,fast}_micros`                | gauge     | `method`                |
| `lb.method.rate_per_sec` / `lb.method.error_rate`          | gauge     | `method`                |
| `lb.outlier.last_error_rate` / `lb.outlier.last_latency_ratio` | gauge | `subchannel`            |
| `lb.tuning.value`                                          | gauge     | `key`                   |

When gRPC removes a subchannel, its per-subchannel series are dropped as well, so dashboards don't
fill up with dead pods.

## Harness

`peak-ewma-harness` starts N real Netty gRPC servers on localhost with injectable latency, error
rate, brownouts, and outages. It sends open-loop traffic through a single channel and reports each
backend's traffic share, p50/p95/p99 latency, and the live LB metrics.

```bash
./mvnw -q install -DskipTests
./mvnw -q -pl peak-ewma-harness exec:java \
    -Dexec.args="--scenario=brownout --backends=5 --qps=500 --duration=15s"

# compare against grpc-java's built-in policy
./mvnw -q -pl peak-ewma-harness exec:java \
    -Dexec.args="--scenario=brownout --policy=round_robin"
```

Scenarios: `baseline`, `brownout`, `outage`, `errors`. The timing-sensitive scenario assertions
are tagged `perf` and run with `./mvnw -pl peak-ewma-harness test -Pperf`.

## Building

Requires Java 21+.

```bash
./mvnw clean install        # build, test, check formatting
./mvnw spotless:apply       # format (google-java-format, AOSP)
```

## License

[MIT](LICENSE)
