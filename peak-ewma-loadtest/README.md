# peak-ewma-loadtest

The multi-process load test from #94: what `peak_ewma_p2c` costs on the client (**Tier 1**, the
overhead matrix) and whether it routes better under realistic failures (**Tier 2**), compared with
`round_robin`, `least_request_experimental` and `least_request` + grpc `outlier_detection`
(`lr_od`), plus a `control` client (one backend, `pick_first`) that sets the floor for client CPU
and allocation.

Unlike `peak-ewma-bench` (one JVM, in-process transport), every backend pod and every client pod
here is **its own JVM** talking real gRPC over TCP, and all policies run **concurrently** against
the same backends. The same jar runs locally (`scripts/run.py`) or on Kubernetes
([`k8s/`](k8s/README.md)).

```bash
./mvnw -pl peak-ewma-loadtest -am package -DskipTests
python3 peak-ewma-loadtest/scripts/run.py --suite quick --out peak-ewma-loadtest/results/<run-id>  # ~35 min
python3 peak-ewma-loadtest/scripts/run.py --suite all   --out peak-ewma-loadtest/results/<run-id>  # ~80 min
python3 peak-ewma-loadtest/scripts/run.py --suite smoke --out /tmp/lt-smoke                        # ~5 min
pip install -r peak-ewma-loadtest/scripts/requirements.txt
python3 peak-ewma-loadtest/scripts/report.py peak-ewma-loadtest/results/<run-id>
```

**`quick` is the one to run for a change to the balancer.** It is the 7 scenarios and 4 Tier 1 cells
that separated the policies in the first full run (`results/cluster-2026-10-04`): steady uneven
fleet, steady identical fleet (false ejections), brownout, crash-loop, black hole, node contention
and client restart; Tier 1 at 10, 50 and 500 backends and the 500 × 100 × 50k corner. Everything
else tied across policies there. `all` is the full #94 matrix, for a release or a real cluster.
Both default to 2 repeats (`--repeats 3` tightens the confidence intervals; finished runs are
kept, so re-running with a higher count only adds the missing repeats).

**Read `SUMMARY.md` first.** It opens with the decision table: per scenario, each policy's
worst-case p99 and error rate, and a verdict that names a winner only for a material margin
(p99 ≥ 20% and ≥ 2 ms, error rate ≥ 0.1 pp and 1.5×, recovery ≥ 5 s faster). The rest of the
summary is the go/no-go table and the key charts. `report/report.pdf` is a short brief of the
same; `report/report.html` has every chart for drill-down (`--full-pdf` puts them all in the PDF).

`run.py` resumes: finished runs (a `run.json`, or a JSONL ending in a summary line) are skipped,
so an interrupted suite can be restarted with the same command. `--only` restricts to named
scenarios / cells; `--repeats`, `--t2-duration`, `--t1-duration` shorten things.

## Pieces

| Class / file | What it is |
|---|---|
| `Backend` | One or more gRPC ports (one per simulated pod), each with its own `Behaviour`; HTTP admin on `--admin-port`: `/set?ports=all\|p1,p2&key=value` changes behaviour at runtime, `/stats` (JSON counters + process CPU), `/metrics` (Prometheus: calls per method, service-time histogram), `/health` |
| `Behaviour` | `latencyFactor`, `sigma` (lognormal service time), `slots` (concurrency; the rest queue FIFO, so capacity is finite), `errorRate` (UNAVAILABLE), `mode` (`normal` / `unavailable` / `blackhole`), `gcPeriodMillis` + `gcPauseMillis` (stop-the-world), `extraDelayMillis` (cross-zone RTT) |
| `Probe` | Methods are created on demand and their name is their shape: `U<ms>_<name>` unary with median `<ms>`, `S<ms>_<name>` server streaming (5 messages), `W_<name>` watch (1 msg/s until cancelled). Any number of names, including churning ones, without a proto |
| `LoadGen` | One client pod, one policy. Open-loop at `--rps` (latency from each call's *intended* start: no coordinated omission) or closed-loop (`--rps 0 --outstanding N`) for saturation. Writes per-interval JSONL and a summary |
| `TimedPolicyProvider` | `timed_<policy>`: any policy, unchanged, with 1 in 1,000 picks timed, so pick times compare like for like across policies |
| `RecordingLbMetrics` | `LbMetrics` sink for LB internals: picks by outcome, ejections by reason, `MethodScale` per method, cost gauges, outlier-tick / sampled pick / tracer durations |
| `FileNameResolverProvider` | `file:///path`: endpoints are the lines of a file, polled every 500 ms. Pod churn = rewriting the file (what a headless Service's DNS answer does) |
| `loadtest.jfc` | JFR template (`--jfr out.jfr --jfr-settings loadtest`): JDK `profile` with CPU sampling every 5 ms, for the flame-graph cells |
| `scripts/run.py` | Scenario runner, both tiers (below) |
| `scripts/report.py` | Raw JSONL → CSV + self-contained HTML + PDF + markdown summary with PNGs |

### Instrumentation in the core (off the hot path unless enabled)

- `LbMetrics.setMethodScale(method, peakHalfLifeMs, baselineHalfLifeMs, seedMicros)`: the
  half-lives and seed the balancer derived for each method, every tick. Micrometer / OTel export
  `lb.method.peak_half_life_millis`, `lb.method.baseline_half_life_millis`, `lb.method.seed_micros`.
- `LbMetrics.recordOutlierTick(nanos)`: `lb.outlier.tick` timer.
- `LbMetrics.recordPickNanos` / `recordTracerNanos(callback, nanos)`: sampled `pickSubchannel` and
  `streamCreated` / `streamClosed` timers (`lb.pick.duration`, `lb.tracer.duration`), **only**
  with `-Dpeakewma.sampledTimers=true` (1 in `-Dpeakewma.sampledTimers.every=1000`).
  `SampledTimers.ENABLED` is a static final, so with the property unset the JIT drops the branch.

All new `LbMetrics` methods are default no-ops, so existing implementations keep compiling.

## Tier 1: overhead matrix

Homogeneous backends that answer immediately (one backend JVM, one port per backend), one loadgen
JVM per run (4 CPUs, 1 GB heap), policies run one at a time and interleaved across repeats.
One-at-a-time sweeps around the centre (10 backends, 10 methods, 10k RPS, 8 threads, unary, 100 B):
backends 3 / 50 / 200 / 500; methods 1 / 100 / 1,000 churning (10 new names per second);
RPS 1k / 50k / saturation; threads 1 / 32; 80/20 unary/streaming; 50 watch streams + unary;
16 KB payload; corners 500 × 100 × 50k and 3 × 1 × 1k. Each run: 4 s warmup, 6 s measured;
2 repeats per cell. Extras: heap after GC every 10 s for 4 min
under 1,000 churning methods (must plateau), and JFR profiles of the centre and corner cells.

Per run the loadgen measures, itself, over the window: process CPU per RPC, bytes allocated per
RPC (all threads), GC count / pause p99 / allocation rate, pick time (p50 / p99 / p99.9, 1 in
1,000), tracer time and outlier tick (peak only), pick outcomes, and throughput (cores used).

## Tier 2: routing quality

20 backend pods by default, each its own JVM with 8 slots and a 10 ms lognormal method (σ 0.3).
The heterogeneous fleet is 14 normal, 3 noisy neighbours (2× median, σ 0.8), 1 GC-pausing
(500 ms every 10 s), 1 flaky (5% UNAVAILABLE) and 1 throttled (half the slots, 1.5× slower).
Two client pods per policy, all 8 at once, at 70% of the fleet's computed capacity in total.
Each run: 20 s warmup, 90 s measured, faults from t = 20 s to 60 s, 2 repeats.

| Scenario | Fleet | What happens |
|---|---|---|
| `s01_steady_het` | het | nothing |
| `s02_steady_homo` | homo | nothing (false ejections, fairness) |
| `s03_brownout` | homo | pod 0 latency ×5, 20–60 s |
| `s04_crashloop` | homo | pod 0 answers UNAVAILABLE instantly, 20–60 s |
| `s05_blackhole` | homo | pod 0 accepts calls and never answers, 20–60 s |
| `s06_gc_pause` | homo | pod 0 stops the world 500 ms every 10 s, 20–60 s |
| `s07_rolling_restart` | homo | every pod replaced by a new (cold, new address) JVM, one every 2 s, 20–60 s |
| `s08_scale_up_down` | homo | +5 cold pods at 20 s, 5 original pods removed at 55 s |
| `s09_max_connection_age` | homo | every backend GOAWAYs connections after 10 s |
| `s10_node_cpu_contention` | homo | pods 0–3 (one "node") 2× slower with half the slots, 20–60 s |
| `s11_client_restart` | het | every client's channel replaced at 30 s (cold LB, warm fleet) |
| `s12_cross_zone` | homo | a third of the pods +2 ms RTT |
| `s13_steady_het_5_methods` | het | 5 methods: 1 / 10 / 50 / 200 / 1,000 ms at 40 / 30 / 20 / 8 / 2 % |

Plus a sensitivity grid (steady het, backends 5 / 20 / 50 × load 30 / 70 / 90%, one client per
policy) and herd runs (1 / 4 / 16 `peak_ewma_p2c` clients alone on the fleet, 16 `round_robin`
for reference).

## Output layout

```
<run-id>/
  SUMMARY.md, charts/*.png            committed: decision table, go/no-go, key charts
  manifest.json                       committed: commit, JDK, gRPC, machine, JVM flags, policy configs
  report/data/*.csv                   committed: every table behind the report
  report/report.html, report.pdf      local: full interactive report, PDF brief
  run.log
  tier1/<cell>/cell.json, <policy>-r<k>.jsonl, logs/             local (raw)
  tier1_extra/memory_churn-<policy>.jsonl, jfr_<cell>-<policy>.{jsonl,jfr}
  tier2/<scenario|sens_*|herd_*>/r<k>/run.json, <policy>-c<i>.jsonl, servers.jsonl, logs/
  kind-smoke/                         same JSONL format, from k8s/kind-smoke.sh
```

`results/.gitignore` keeps the raw data (hundreds of MB per run) out of git; `report.py`
regenerates everything from it.

Loadgen JSONL: a `meta` line (policy, args, JDK, gRPC version), one `interval` line per second
(`t` = seconds since measurement start, negative during warmup; per-method latency percentiles
and status counts; calls per backend; CPU, allocation and GC deltas; LB internals) and a final
`summary` over the measurement window (also split into the `before` / `fault` / `after` phases
for fault scenarios). Latency percentiles are of successful calls; failures are counted by
status.

## Caveats of the local mode

- It is one machine (18 cores) and loopback, not a cluster: no real network, DNS or kubelet.
  Pod CPU limits are approximated with `-XX:ActiveProcessorCount`; there's no cgroup throttling.
- Backends are a queueing model (slots + lognormal service time), not real work. Their capacity
  is known, which is what makes "70% load" meaningful.
- Policies share the fleet concurrently, as #94 asks, so one policy's routing changes the load
  the others see (e.g. round_robin keeps loading a slow pod for everyone).
- The loadgen's own floor (~25–30 µs CPU and ~32 KB allocated per RPC over Netty, identical for
  every policy) dilutes percentage deltas; read the absolute deltas vs `control` too.
