# peak_ewma_p2c vs grpc-java built-in policies

Generated 2026-10-03T03:20Z from benchmarks.jar built at 11bc8f2 (pre-fix code).



JVM 25.0.3, 18 CPUs, Mac OS X aarch64. Closed loop, 8 caller threads, 5s warmup + 10s measured per row, fresh JVM per row, in-process transport, zero-latency backends.

## 3 backends

| policy | RPC/s | CPU µs/RPC | Δ CPU vs RR | alloc B/RPC | Δ alloc vs RR | retained heap (KB) | GC ms/s | p50 µs | p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| pick_first | 400,863 | 7.24 | -10% | 3,636 | -10% | 319 | 1.6 | 4 | 198 |
| round_robin | 338,449 | 8.00 | +0% | 4,033 | +0% | 362 | 1.5 | 3 | 142 |
| least_request_experimental | 377,946 | 7.66 | -4% | 3,677 | -9% | 358 | 1.6 | 3 | 134 |
| weighted_round_robin | 412,245 | 7.18 | -10% | 3,952 | -2% | 396 | 1.8 | 3 | 128 |
| peak_ewma_p2c | 379,888 | 7.89 | -1% | 4,125 | +2% | 410 | 1.8 | 4 | 142 |

## 10 backends

| policy | RPC/s | CPU µs/RPC | Δ CPU vs RR | alloc B/RPC | Δ alloc vs RR | retained heap (KB) | GC ms/s | p50 µs | p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| pick_first | 362,371 | 7.08 | +27% | 3,704 | +6% | 318 | 1.5 | 3 | 180 |
| round_robin | 458,109 | 5.59 | +0% | 3,487 | +0% | 401 | 1.8 | 2 | 114 |
| least_request_experimental | 510,260 | 4.69 | -16% | 3,621 | +4% | 398 | 2.1 | 2 | 107 |
| weighted_round_robin | 468,080 | 5.81 | +4% | 3,775 | +8% | 436 | 2.1 | 3 | 110 |
| peak_ewma_p2c | 360,926 | 8.62 | +54% | 4,499 | +29% | 483 | 1.9 | 5 | 126 |

## 100 backends

| policy | RPC/s | CPU µs/RPC | Δ CPU vs RR | alloc B/RPC | Δ alloc vs RR | retained heap (KB) | GC ms/s | p50 µs | p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| pick_first | 418,536 | 7.21 | +107% | 3,633 | +4% | 319 | 1.8 | 4 | 202 |
| round_robin | 553,753 | 3.49 | +0% | 3,499 | +0% | 675 | 2.3 | 2 | 112 |
| least_request_experimental | 613,013 | 3.49 | +0% | 3,609 | +3% | 665 | 2.6 | 2 | 101 |
| weighted_round_robin | 597,899 | 3.79 | +9% | 3,816 | +9% | 737 | 2.7 | 2 | 97 |
| peak_ewma_p2c | 237,935 | 15.55 | +345% | 13,025 | +272% | 949 | 3.7 | 12 | 156 |

## 500 backends

| policy | RPC/s | CPU µs/RPC | Δ CPU vs RR | alloc B/RPC | Δ alloc vs RR | retained heap (KB) | GC ms/s | p50 µs | p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| pick_first | 414,023 | 7.14 | +114% | 3,537 | -4% | 323 | 1.7 | 4 | 198 |
| round_robin | 561,893 | 3.34 | +0% | 3,673 | +0% | 1,982 | 2.5 | 2 | 114 |
| least_request_experimental | 598,565 | 3.46 | +4% | 3,761 | +2% | 1,954 | 2.7 | 2 | 103 |
| weighted_round_robin | 615,946 | 3.63 | +9% | 3,736 | +2% | 2,168 | 2.9 | 2 | 101 |
| peak_ewma_p2c | 135,960 | 49.83 | +1393% | 50,257 | +1268% | 2,965 | 7.5 | 49 | 295 |

## Routing quality: 10 backends @ 2 ms, b0 slow (20 ms), b1 fails instantly

Closed loop, 16 threads. Lower share for b0/b1 and higher success are better.

| policy | RPC/s | success % | p50 µs (ok) | p99 µs (ok) | share b0 (slow) | share b1 (failing) | share min..max of healthy |
|---|---:|---:|---:|---:|---:|---:|---|
| pick_first | 764 | 100.0 | 20833 | 25395 | 100.0% | 0.0% | 0.0%..0.0% |
| round_robin | 4,222 | 90.0 | 2223 | 23461 | 10.0% | 10.0% | 10.0%..10.0% |
| least_request_experimental | 7,252 | 82.6 | 2179 | 21249 | 2.3% | 17.4% | 9.9%..10.1% |
| weighted_round_robin | 4,216 | 90.0 | 2267 | 23461 | 10.0% | 10.0% | 10.0%..10.0% |
| peak_ewma_p2c | 7,219 | 97.9 | 2313 | 2764 | 0.0% | 2.1% | 12.0%..12.4% |

# JMH: LB pick path (pickSubchannel + tracer lifecycle), real picker over in-process subchannels

## PickerBenchmark.pick_1thread (ns/op, allocation per op)

| policy | 3 backends | 10 backends | 100 backends | 500 backends |
|---|---:|---:|---:|---:|
| least_request_experimental | 10.2 (48 B) | 9.4 (48 B) | 9.3 (48 B) | 9.5 (48 B) |
| peak_ewma_p2c | 238 (304 B) | 613 (608 B) | 5,802 (4,571 B) | 31,800 (22,502 B) |
| pick_first | 1.4 (0 B) | 1.4 (0 B) | 1.4 (0 B) | 1.4 (0 B) |
| round_robin | 2.5 (0 B) | 2.6 (0 B) | 2.5 (0 B) | 2.6 (0 B) |
| weighted_round_robin | 84.7 (224 B) | 85.9 (336 B) | 85.9 (336 B) | 90.1 (224 B) |

## PickerBenchmark.pick_8threads (ns/op, allocation per op)

| policy | 3 backends | 10 backends | 100 backends | 500 backends |
|---|---:|---:|---:|---:|
| least_request_experimental | 335 (48 B) | 238 (48 B) | 142 (48 B) | 117 (48 B) |
| peak_ewma_p2c | 3,271 (362 B) | 4,211 (1,148 B) | 11,562 (4,625 B) | 39,562 (22,268 B) |
| pick_first | 1.8 (0 B) | 1.9 (0 B) | 1.9 (0 B) | 1.9 (0 B) |
| round_robin | 169 (0 B) | 172 (0 B) | 169 (0 B) | 172 (0 B) |
| weighted_round_robin | 326 (224 B) | 263 (224 B) | 317 (224 B) | 358 (224 B) |

# JMH: full unary RPC (in-process transport)

## RpcBenchmark.unary_1thread (us/op, allocation per op)

| policy | 3 backends | 10 backends | 100 backends |
|---|---:|---:|---:|
| least_request_experimental | 2.8 (3,926 B) | 2.9 (4,065 B) | 2.9 (3,886 B) |
| peak_ewma_p2c | 1.4 (4,264 B) | 1.8 (4,528 B) | 7.4 (8,484 B) |
| pick_first | 2.6 (3,894 B) | 2.5 (3,829 B) | 2.6 (3,853 B) |
| round_robin | 2.7 (4,006 B) | 2.8 (3,857 B) | 2.8 (3,856 B) |
| weighted_round_robin | 1.2 (4,208 B) | 1.2 (4,184 B) | 1.3 (4,088 B) |

## RpcBenchmark.unary_8threads (us/op, allocation per op)

| policy | 3 backends | 10 backends | 100 backends |
|---|---:|---:|---:|
| least_request_experimental | 20.3 (3,948 B) | 14.9 (3,956 B) | 12.8 (3,935 B) |
| peak_ewma_p2c | 20.0 (4,132 B) | 21.5 (4,473 B) | 31.2 (13,200 B) |
| pick_first | 20.3 (3,828 B) | 18.9 (3,938 B) | 18.7 (3,914 B) |
| round_robin | 20.7 (3,968 B) | 18.4 (3,806 B) | 12.8 (3,824 B) |
| weighted_round_robin | 19.4 (3,943 B) | 16.6 (3,765 B) | 12.9 (3,823 B) |

