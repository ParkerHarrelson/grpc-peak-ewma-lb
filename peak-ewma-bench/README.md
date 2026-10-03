# peak-ewma-bench

Measures what `peak_ewma_p2c` costs, and how it routes, compared with grpc-java's built-in
policies: `pick_first`, `round_robin`, `least_request_experimental` and `weighted_round_robin`
(the last two come from `grpc-xds`; without ORCA load reports WRR uses equal weights).

```bash
peak-ewma-bench/run-benchmarks.sh            # full run, ~25-35 min
QUICK=1 peak-ewma-bench/run-benchmarks.sh    # smoke run, ~5 min
```

Output: `peak-ewma-bench/target/report/REPORT.md` (plus raw `overhead.md`, `picker.csv`, `rpc.csv`).

## What's measured

| Part | Class | What it tells you |
|---|---|---|
| LB overhead | `OverheadReport` | Per policy × fleet size, each in a **fresh JVM**: process CPU µs per RPC (all threads, including JIT/GC), bytes allocated per RPC (all threads), retained heap of channel + LB state after GC, GC ms/s, p50/p99, and deltas vs `round_robin`. Closed-loop unary RPCs over the in-process transport to zero-latency backends, so transport and server cost are small and identical across policies; the deltas are the LB. |
| Routing quality | `OverheadReport` (quality) | 10 backends @ 2 ms; b0 slows to 20 ms, b1 fails every RPC instantly. Success rate, p50/p99 of successful calls, traffic share to the slow, failing and healthy backends. |
| Pick path | `PickerBenchmark` (JMH) | `pickSubchannel` + the stream-tracer lifecycle the policy attaches, on the policy's **real** picker over real in-process subchannels (captured by `CapturingLoadBalancerProvider`). 1 and 8 threads, 3–500 backends, ns/op and B/op. |
| Full RPC | `RpcBenchmark` (JMH) | One blocking unary RPC through the channel, 1 and 8 threads. |

Run pieces individually:

```bash
JAR=peak-ewma-bench/target/benchmarks.jar
java -jar $JAR PickerBenchmark -prof gc -p backends=10,100 -p policy=round_robin,peak_ewma_p2c
java -jar $JAR RpcBenchmark -prof gc
java -cp $JAR dev.parkerharrelson.grpc.peakewma.bench.OverheadReport --sizes 10,100 --policies round_robin,peak_ewma_p2c
```

## Caveats

- Client and backends share a JVM. Backend work is identical across policies, so compare
  **deltas**, not absolute CPU numbers.
- Close other heavy processes; numbers move ±10% run to run on a laptop. Use the JMH numbers for
  small differences, and the overhead report for whole-process cost.
- Quick mode uses very short JMH iterations; single-thread `RpcBenchmark` numbers are noisy there.
