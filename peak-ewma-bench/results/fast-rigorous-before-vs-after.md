# Fast rigorous comparison: before vs after the fixes

Same bench harness (commit 084323a) built against the pre-fix core (11bc8f2) and the fixed core (084323a). openjdk version "25.0.3" 2026-04-21 LTS, 18 CPUs, Darwin arm64. Overhead: 3 fresh-JVM runs per row, interleaved, 2 s warmup + 4 s measured, mean ± 95% CI, Δ marked n.s. when a Welch t-test can't separate it from round_robin. JMH: 3 forks × 3 iterations, ± 99.9% error. Reproduce with `FAST_RIGOROUS=1 peak-ewma-bench/run-benchmarks.sh`.

## Before (pre-fix core)
### 100 backends

| policy | RPC/s | CPU µs/RPC | Δ CPU vs RR | alloc B/RPC | Δ alloc vs RR | retained heap (KB) | GC ms/s | p50 µs | p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| round_robin | 599,304 ± 26,070 | 3.60 ± 0.04 | baseline | 3,663 ± 303 | baseline | 673 ± 1 | 2.5 ± 0.0 | 2 ± 0 | 101 ± 5 |
| peak_ewma_p2c | 261,934 ± 8,093 | 15.21 ± 0.44 | +323% | 9,980 ± 7,019 | +172% (n.s.) | 922 ± 11 | 3.2 ± 2.3 | 12 ± 1 | 146 ± 4 |

### 500 backends

| policy | RPC/s | CPU µs/RPC | Δ CPU vs RR | alloc B/RPC | Δ alloc vs RR | retained heap (KB) | GC ms/s | p50 µs | p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| round_robin | 619,233 ± 21,769 | 3.40 ± 0.07 | baseline | 3,685 ± 57 | baseline | 1,998 ± 68 | 2.7 ± 0.4 | 2 ± 0 | 101 ± 5 |
| peak_ewma_p2c | 145,147 ± 3,483 | 48.12 ± 0.97 | +1315% | 26,220 ± 854 | +611% | 2,883 ± 6 | 4.2 ± 0.4 | 47 ± 1 | 274 ± 21 |


Score ± JMH 99.9% confidence error over 9 measurement iterations (forks × iterations); allocation in parentheses.

### PickerBenchmark.pick_1thread (ns/op, allocation per op)

| policy | 10 backends | 500 backends |
|---|---:|---:|
| peak_ewma_p2c | 613 ± 9 (608 B) | 32,958 ± 3,244 (22,520 B) |

### PickerBenchmark.pick_8threads (ns/op, allocation per op)

| policy | 10 backends | 500 backends |
|---|---:|---:|
| peak_ewma_p2c | 4,013 ± 210 (667 B) | 41,499 ± 1,884 (22,311 B) |


## After (contract + scoring + O(1)-pick fixes)
### 100 backends

| policy | RPC/s | CPU µs/RPC | Δ CPU vs RR | alloc B/RPC | Δ alloc vs RR | retained heap (KB) | GC ms/s | p50 µs | p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| round_robin | 603,255 ± 22,589 | 3.60 ± 0.18 | baseline | 3,692 ± 617 | baseline | 672 ± 2 | 2.7 ± 0.7 | 2 ± 0 | 99 ± 3 |
| peak_ewma_p2c | 511,750 ± 17,315 | 4.91 ± 0.19 | +36% | 3,754 ± 35 | +2% (n.s.) | 929 ± 6 | 2.4 ± 0.4 | 3 ± 0 | 108 ± 3 |

### 500 backends

| policy | RPC/s | CPU µs/RPC | Δ CPU vs RR | alloc B/RPC | Δ alloc vs RR | retained heap (KB) | GC ms/s | p50 µs | p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| round_robin | 619,286 ± 6,243 | 3.39 ± 0.17 | baseline | 3,704 ± 72 | baseline | 2,006 ± 57 | 2.7 ± 0.0 | 2 ± 0 | 99 ± 3 |
| peak_ewma_p2c | 506,095 ± 73,655 | 4.73 ± 0.21 | +40% | 4,089 ± 411 | +10% (n.s.) | 2,955 ± 47 | 2.6 ± 0.7 | 3 ± 0 | 110 ± 8 |


Score ± JMH 99.9% confidence error over 9 measurement iterations (forks × iterations); allocation in parentheses.

### PickerBenchmark.pick_1thread (ns/op, allocation per op)

| policy | 10 backends | 500 backends |
|---|---:|---:|
| least_request_experimental | 9.6 ± 0.4 (48 B) | 9.6 ± 0.2 (48 B) |
| peak_ewma_p2c | 134 ± 1 (136 B) | 160 ± 3 (137 B) |
| round_robin | 2.6 ± 0.1 (0 B) | 2.6 ± 0.0 (0 B) |

### PickerBenchmark.pick_8threads (ns/op, allocation per op)

| policy | 10 backends | 500 backends |
|---|---:|---:|
| least_request_experimental | 278 ± 92 (48 B) | 96.5 ± 2.4 (48 B) |
| peak_ewma_p2c | 2,451 ± 116 (139 B) | 1,535 ± 116 (252 B) |
| round_robin | 171 ± 4 (0 B) | 172 ± 3 (0 B) |

