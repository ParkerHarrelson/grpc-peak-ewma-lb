# grpc-peak-ewma-lb

A latency-aware load balancing policy for [grpc-java](https://github.com/grpc/grpc-java).

grpc-java's `pick_first` and `round_robin` don't look at how backends are actually
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
| `peak-ewma-loadtest`      | Multi-process / Kubernetes load test (#94): overhead matrix + routing quality under failures, one JVM per pod. Not published. See [its README](peak-ewma-loadtest/README.md). | core, grpc-xds, HdrHistogram |

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
cost = peakLatency(now) × (inflight + 1)
```

- **Peak latency.** `max(rtt, previous × decay)`: a single slow response raises the score right
  away, and the score decays back toward zero *at read time*. A backend that stopped getting
  traffic gets cheaper until it is probed again, so a spike wears off and recovery is automatic.
- **Inflight.** The standard Peak-EWMA load term (as in Finagle and tower): the expected wait behind
  the unary calls already outstanding on that backend. Long-lived streams don't count as load.
- **Failures** (`UNAVAILABLE`, `INTERNAL`, …) count as a penalty, so a backend that fails instantly
  doesn't look like the fastest one.
- **New backends** start at the fleet's typical latency for each method, so they compete at par and
  their own first responses decide.

### Self-tuning: no knobs to set

Nothing in the scoring is a fixed number of milliseconds. Every outlier tick derives, per method,
from what the fleet actually observes:

| | derived as |
|---|---|
| peak half-life | ~20 samples of a fair share of the method's traffic, at least 2 RTTs, at most 60 s; shortened for noisy methods |
| baseline half-life | ~600 samples, at least 20 RTTs, within 1 s – 5 min |
| error window | ~200 calls of the method, at least 2 ticks, at most 5 min |
| staleness | two baseline half-lives |

A 2 ms method at 2,000 rps and a 1 s method at 5 rps therefore behave the same *per sample*: in
simulation the same 3×-slow backend gets 0.2–2% of traffic across that whole range, versus
0.1–7.5% with the old fixed 1 s half-life (`AdversarialScaleTest`).

**Outlier ejection.** A background tick looks at each backend per method. A backend is ejected entirely when
it is statistically clear (95% lower bound) that its error rate exceeds `outlierErrorRate`;
only backend-health failures count (`UNAVAILABLE`, `INTERNAL`, `DEADLINE_EXCEEDED`, …), not
application errors such as `NOT_FOUND`. Ejections back off: each repeat lasts one more base
period, and healthy time forgives it. A backend that is slow for one method
compared with the fleet's median for it is ejected for that method only. At least one backend can
always be ejected, and at least one always stays in rotation. A returned backend is only re-ejected on fresh evidence, and repeat ejections back off.

RTTs come from a `ClientStreamTracer` attached to each pick, so no interceptors are needed.

## Configuration

The defaults are meant to be left alone: the balancer adapts to your fleet size, latencies and
request rates. What remains configurable is **policy** (how aggressive ejection should be) and
**resource limits**, through the standard gRPC service config:

```java
Map<String, ?> serviceConfig =
        Map.of(
                "loadBalancingConfig",
                List.of(Map.of("peak_ewma_p2c", Map.of("outlierErrorRate", 0.5))));

ManagedChannelBuilder.forTarget("dns:///my-service.internal:9090")
        .defaultServiceConfig(serviceConfig)
        .disableServiceConfigLookUp()
        .usePlaintext()
        .build();
```

| Key                            | Default  | Meaning                                                        |
|--------------------------------|----------|----------------------------------------------------------------|
| `outlierEnabled`               | `true`   | Enable outlier ejection                                        |
| `outlierErrorRate`             | `0.20`   | Backend-health error rate that ejects a backend (judged statistically) |
| `outlierLatencyMultiplier`     | `2.5`    | How many times slower than the fleet median ejects a method (adjusted for noise) |
| `outlierEjectMillis`           | `5000`   | Base ejection time; each repeat ejection adds one more (backoff, ≤ 5 min) |
| `outlierReentryCooldownMillis` | `0`      | Optional extra wait before a returned backend can be re-ejected (re-ejection already requires fresh evidence) |
| `outlierTickIntervalMillis`    | `1000`   | Outlier evaluation period (min 100)                            |
| `initialRttMicros`             | `50000`  | Latency assumed before anything has been measured anywhere     |
| `methodMaxEntries`             | `512`    | Max tracked methods per subchannel                             |
| `methodPruneStaleAfterMillis`  | `120000` | Drop per-method stats that haven't been updated in this long   |

**Deprecated, ignored:** `tauFastMillis`, `tauSlowMillis`, `inflightWeight`, `outlierWindowMillis`,
`staleMillisForRatio`. They still parse so existing service configs keep working, and log a warning
once; the balancer derives these from observed traffic.

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
