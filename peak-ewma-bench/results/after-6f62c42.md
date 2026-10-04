# peak_ewma_p2c vs grpc-java built-in policies

Generated 2026-10-03T05:29Z from benchmarks.jar built at 6f62c42 (after #85–#87).



JVM 25.0.3, 18 CPUs, Mac OS X aarch64. Closed loop, 8 caller threads, 5s warmup + 10s measured per row, fresh JVM per row, in-process transport, zero-latency backends.

## 3 backends

| policy | RPC/s | CPU µs/RPC | Δ CPU vs RR | alloc B/RPC | Δ alloc vs RR | retained heap (KB) | GC ms/s | p50 µs | p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| pick_first | 311,456 | 7.60 | -16% | 3,811 | +2% | 318 | 1.4 | 3 | 252 |
| round_robin | 288,488 | 9.00 | +0% | 3,745 | +0% | 362 | 1.4 | 5 | 176 |
| least_request_experimental | 264,288 | 9.67 | +7% | 3,805 | +2% | 357 | 1.2 | 5 | 180 |
| weighted_round_robin | 390,090 | 7.57 | -16% | 3,753 | +0% | 396 | 1.7 | 4 | 134 |
| peak_ewma_p2c | 266,671 | 10.52 | +17% | 3,695 | -1% | 408 | 2.4 | 6 | 211 |

## 10 backends

| policy | RPC/s | CPU µs/RPC | Δ CPU vs RR | alloc B/RPC | Δ alloc vs RR | retained heap (KB) | GC ms/s | p50 µs | p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| pick_first | 230,847 | 12.09 | +114% | 3,808 | +0% | 319 | 2.1 | 6 | 397 |
| round_robin | 456,287 | 5.65 | +0% | 3,806 | +0% | 401 | 2.1 | 2 | 112 |
| least_request_experimental | 451,390 | 4.92 | -13% | 3,550 | -7% | 397 | 1.9 | 2 | 116 |
| weighted_round_robin | 455,157 | 5.95 | +5% | 3,782 | -1% | 435 | 2.0 | 3 | 110 |
| peak_ewma_p2c | 386,675 | 7.59 | +34% | 3,721 | -2% | 476 | 1.7 | 4 | 126 |

## 100 backends

| policy | RPC/s | CPU µs/RPC | Δ CPU vs RR | alloc B/RPC | Δ alloc vs RR | retained heap (KB) | GC ms/s | p50 µs | p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| pick_first | 345,008 | 7.53 | +106% | 3,959 | +8% | 319 | 1.7 | 4 | 187 |
| round_robin | 582,500 | 3.66 | +0% | 3,664 | +0% | 676 | 2.6 | 2 | 103 |
| least_request_experimental | 604,119 | 3.52 | -4% | 3,552 | -3% | 674 | 2.4 | 2 | 103 |
| weighted_round_robin | 596,976 | 3.83 | +5% | 3,816 | +4% | 736 | 2.7 | 2 | 99 |
| peak_ewma_p2c | 507,107 | 4.97 | +36% | 3,768 | +3% | 955 | 2.3 | 3 | 107 |

## 500 backends

| policy | RPC/s | CPU µs/RPC | Δ CPU vs RR | alloc B/RPC | Δ alloc vs RR | retained heap (KB) | GC ms/s | p50 µs | p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| pick_first | 384,205 | 7.49 | +132% | 3,501 | +15% | 322 | 1.6 | 4 | 191 |
| round_robin | 578,057 | 3.22 | +0% | 3,048 | +0% | 2,026 | 2.4 | 2 | 105 |
| least_request_experimental | 626,581 | 3.13 | -3% | 3,727 | +22% | 1,915 | 2.7 | 2 | 97 |
| weighted_round_robin | 603,427 | 3.73 | +16% | 3,967 | +30% | 2,213 | 3.0 | 2 | 101 |
| peak_ewma_p2c | 478,226 | 4.70 | +46% | 4,030 | +32% | 2,962 | 2.4 | 2 | 110 |

## Routing quality: 10 backends @ 2 ms, b0 slow (20 ms), b1 fails instantly

Closed loop, 16 threads. Lower share for b0/b1 and higher success are better.

| policy | RPC/s | success % | p50 µs (ok) | p99 µs (ok) | share b0 (slow) | share b1 (failing) | share min..max of healthy |
|---|---:|---:|---:|---:|---:|---:|---|
| pick_first | 787 | 100.0 | 20024 | 24409 | 100.0% | 0.0% | 0.0%..0.0% |
| round_robin | 4,349 | 90.0 | 2095 | 23001 | 10.0% | 10.0% | 10.0%..10.0% |
| least_request_experimental | 7,610 | 82.2 | 2054 | 21249 | 2.3% | 17.8% | 9.9%..10.1% |
| weighted_round_robin | 4,229 | 90.0 | 2223 | 23461 | 10.0% | 10.0% | 10.0%..10.0% |
| peak_ewma_p2c | 6,220 | 100.0 | 2179 | 20424 | 2.3% | 0.0% | 12.1%..12.3% |

# JMH: LB pick path (pickSubchannel + tracer lifecycle), real picker over in-process subchannels

## PickerBenchmark.pick_1thread (ns/op, allocation per op)

| policy | 3 backends | 10 backends | 100 backends | 500 backends |
|---|---:|---:|---:|---:|
| least_request_experimental | 10.1 (48 B) | 9.4 (48 B) | 9.4 (48 B) | 9.5 (48 B) |
| peak_ewma_p2c | 102 (136 B) | 134 (136 B) | 132 (136 B) | 149 (137 B) |
| pick_first | 1.4 (0 B) | 1.4 (0 B) | 1.4 (0 B) | 1.4 (0 B) |
| round_robin | 2.5 (0 B) | 2.5 (0 B) | 2.6 (0 B) | 2.6 (0 B) |
| weighted_round_robin | 78.7 (224 B) | 77.6 (224 B) | 86.3 (224 B) | 86.0 (224 B) |

## PickerBenchmark.pick_8threads (ns/op, allocation per op)

| policy | 3 backends | 10 backends | 100 backends | 500 backends |
|---|---:|---:|---:|---:|
| least_request_experimental | 350 (48 B) | 312 (48 B) | 131 (48 B) | 95.5 (48 B) |
| peak_ewma_p2c | 1,712 (137 B) | 2,486 (195 B) | 1,454 (200 B) | 1,558 (292 B) |
| pick_first | 1.8 (0 B) | 1.9 (0 B) | 1.9 (0 B) | 1.9 (0 B) |
| round_robin | 169 (0 B) | 172 (0 B) | 171 (0 B) | 169 (0 B) |
| weighted_round_robin | 260 (224 B) | 258 (224 B) | 267 (224 B) | 347 (224 B) |

# JMH: full unary RPC (in-process transport)

## RpcBenchmark.unary_1thread (us/op, allocation per op)

| policy | 3 backends | 10 backends | 100 backends |
|---|---:|---:|---:|
| least_request_experimental | 2.8 (3,887 B) | 2.9 (3,991 B) | 2.8 (3,904 B) |
| peak_ewma_p2c | 1.3 (4,096 B) | 1.3 (4,072 B) | 1.4 (3,837 B) |
| pick_first | 2.6 (3,894 B) | 2.7 (4,030 B) | 2.6 (3,854 B) |
| round_robin | 2.8 (4,032 B) | 2.8 (4,032 B) | 2.8 (3,894 B) |
| weighted_round_robin | 1.2 (4,320 B) | 1.2 (4,192 B) | 1.3 (4,312 B) |

## RpcBenchmark.unary_8threads (us/op, allocation per op)

| policy | 3 backends | 10 backends | 100 backends |
|---|---:|---:|---:|
| least_request_experimental | 20.7 (3,908 B) | 15.3 (3,996 B) | 13.2 (3,976 B) |
| peak_ewma_p2c | 20.5 (3,972 B) | 20.6 (4,041 B) | 15.3 (4,022 B) |
| pick_first | 20.8 (3,845 B) | 20.3 (3,918 B) | 19.4 (3,780 B) |
| round_robin | 20.6 (3,832 B) | 17.2 (3,902 B) | 12.8 (3,824 B) |
| weighted_round_robin | 19.1 (3,807 B) | 17.3 (3,789 B) | 12.8 (3,791 B) |

