# Ordered TopK Concurrency Benchmark

> **VERIFIED:** With eight 16 MiB children and a 16 MiB global topK,
> application-aware Streaming increased combined median QPS from 91.78 at K1
> to 366.84 at K64. Unary saturated near 95 QPS by K4 and declined to 89.38
> QPS at K64. The Streaming/Unary QPS ratio therefore increased from 1.29x to
> 4.10x across the tested concurrency range.

## Test

| Item | Value |
|---|---|
| Run | `20260831T084746Z`; completed 2026-08-31 UTC |
| Source | `d03c62b6d72062919e0e0ffac89e37154e724384` - `docs: document benchmark configuration` |
| Host | `10.15.9.42`; Linux ARM64; 16 CPUs; 123 GiB RAM |
| Topology | One benchmark host; loopback TCP; one shared logical gRPC channel; eight child processes |
| Workflow | `ordered_topk` with interleaved child results and dynamic gRPC flow-control windows |
| Available input | Eight children times 16 MiB = 128 MiB per operation |
| Required output | 65,536 Units times 256 B = 16 MiB global topK |
| Streaming Chunk | 1,024 Units times 256 B = 256 KiB |
| Concurrency | K1, K2, K4, K8, K16, K32, and K64 |
| Repetitions | Four per K; repetitions 1 and 3 ran Unary first, while 2 and 4 ran Streaming first |
| Measurement | At least 20 seconds and 200 successful operations per mode and repetition |
| Aggregation | Median of two repetitions per mode-order stratum; combined median over all four repetitions |

All 28 executions passed with zero request errors. `go test -race -count=1
./...` passed before execution. The K64 gate observed 122 GiB available against
the required 16 GiB minimum.

Streaming delivered 71 Chunks, or 17.75 MiB, to application `RecvMsg()` per
operation: the 16 MiB global topK plus one 256 KiB retained head from each of
the other seven children. Unary delivered all 128 MiB before reduction.

## Combined Medians

| K | Unary QPS | Streaming QPS | S/U | Unary p50 | Streaming p50 | Unary p95 | Streaming p95 | Unary p99 | Streaming p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 70.96 | 91.78 | 1.29x | 13.36 ms | 11.22 ms | 18.31 ms | 12.90 ms | 21.96 ms | 13.79 ms |
| 2 | 86.58 | 134.59 | 1.55x | 22.41 ms | 15.22 ms | 28.76 ms | 18.13 ms | 32.97 ms | 19.69 ms |
| 4 | 93.15 | 204.34 | 2.19x | 42.88 ms | 19.59 ms | 52.63 ms | 22.97 ms | 60.68 ms | 24.87 ms |
| 8 | 95.19 | 265.05 | 2.78x | 84.21 ms | 29.96 ms | 103.51 ms | 34.66 ms | 117.04 ms | 37.15 ms |
| 16 | 95.73 | 306.51 | 3.20x | 164.01 ms | 51.66 ms | 192.65 ms | 59.56 ms | 239.97 ms | 63.39 ms |
| 32 | 94.23 | 338.06 | 3.59x | 330.44 ms | 93.89 ms | 389.60 ms | 105.15 ms | 780.96 ms | 113.23 ms |
| 64 | 89.38 | 366.84 | 4.10x | 674.14 ms | 172.68 ms | 839.00 ms | 191.91 ms | 1726.08 ms | 225.14 ms |

## Mode Order

The following QPS medians keep Unary-first and Streaming-first repetitions
separate. `SF vs UF` compares Streaming QPS between those two strata.

| K | UF Unary | UF Streaming | UF S/U | SF Unary | SF Streaming | SF S/U | Streaming SF vs UF |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 71.16 | 75.84 | 1.07x | 70.63 | 116.47 | 1.65x | +53.6% |
| 2 | 86.60 | 110.10 | 1.27x | 86.58 | 161.04 | 1.86x | +46.3% |
| 4 | 93.01 | 186.31 | 2.00x | 93.16 | 227.00 | 2.44x | +21.8% |
| 8 | 95.01 | 255.93 | 2.69x | 95.30 | 275.50 | 2.89x | +7.7% |
| 16 | 94.97 | 301.02 | 3.17x | 96.01 | 312.93 | 3.26x | +4.0% |
| 32 | 93.73 | 338.06 | 3.61x | 94.72 | 297.61 | 3.14x | -12.0% |
| 64 | 89.44 | 367.82 | 4.11x | 89.26 | 309.44 | 3.47x | -15.9% |

Each latency cell below is `p50 / p95 / p99` in milliseconds.

| K | Unary-first Unary | Unary-first Streaming | Streaming-first Unary | Streaming-first Streaming |
|---:|---:|---:|---:|---:|
| 1 | 13.31 / 18.27 / 21.99 | 13.17 / 14.97 / 15.95 | 13.42 / 18.39 / 21.95 | 8.59 / 10.08 / 10.88 |
| 2 | 22.37 / 28.76 / 33.08 | 18.07 / 20.84 / 22.40 | 22.41 / 28.63 / 32.51 | 12.40 / 15.33 / 16.89 |
| 4 | 43.04 / 52.55 / 60.74 | 21.29 / 24.62 / 26.08 | 42.71 / 52.86 / 60.68 | 17.51 / 20.88 / 22.93 |
| 8 | 84.41 / 103.95 / 114.38 | 31.01 / 35.69 / 38.34 | 84.14 / 102.76 / 120.76 | 28.82 / 33.65 / 35.78 |
| 16 | 164.96 / 197.50 / 239.97 | 52.34 / 59.56 / 65.18 | 163.77 / 192.35 / 279.55 | 50.68 / 58.70 / 62.18 |
| 32 | 331.52 / 395.06 / 772.21 | 93.89 / 105.15 / 113.23 | 329.03 / 378.95 / 789.50 | 109.88 / 120.51 / 126.94 |
| 64 | 674.14 / 890.53 / 1696.73 | 172.22 / 191.91 / 225.09 | 673.19 / 831.41 / 1765.72 | 213.88 / 231.35 / 244.82 |

## Streaming Transport

These are combined medians per successful operation. Run-ahead is parent
connection-read bytes minus bytes delivered to application `RecvMsg()`.
Efficiency is the 16 MiB final topK divided by parent connection-read bytes.

| K | App RecvMsg | Child send completed | Parent connection read | Run-ahead | Equivalent Chunks | Efficiency |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 17.75 MiB | 93.02 MiB | 92.19 MiB | 74.44 MiB | 297.5 | 20.4% |
| 2 | 17.75 MiB | 71.21 MiB | 69.62 MiB | 51.87 MiB | 207.5 | 24.4% |
| 4 | 17.75 MiB | 45.23 MiB | 43.71 MiB | 25.96 MiB | 104.0 | 37.6% |
| 8 | 17.75 MiB | 34.07 MiB | 32.59 MiB | 14.84 MiB | 59.5 | 49.3% |
| 16 | 17.75 MiB | 28.92 MiB | 27.46 MiB | 9.71 MiB | 39.0 | 58.3% |
| 32 | 17.75 MiB | 25.84 MiB | 24.39 MiB | 6.64 MiB | 26.5 | 65.6% |
| 64 | 17.75 MiB | 23.63 MiB | 22.20 MiB | 4.45 MiB | 18.0 | 72.1% |

Mode order materially changed transport run-ahead, especially at low K.

| K | UF parent read | SF parent read | UF run-ahead | SF run-ahead |
|---:|---:|---:|---:|---:|
| 1 | 127.85 MiB | 42.84 MiB | 440.0 Chunks | 100.0 Chunks |
| 2 | 88.89 MiB | 51.25 MiB | 284.5 Chunks | 134.0 Chunks |
| 4 | 51.15 MiB | 35.52 MiB | 133.5 Chunks | 71.0 Chunks |
| 8 | 35.05 MiB | 29.93 MiB | 69.5 Chunks | 48.5 Chunks |
| 16 | 28.26 MiB | 26.42 MiB | 42.0 Chunks | 34.5 Chunks |
| 32 | 24.39 MiB | 27.06 MiB | 26.5 Chunks | 37.5 Chunks |
| 64 | 22.20 MiB | 25.52 MiB | 18.0 Chunks | 31.0 Chunks |

## Findings

- **VERIFIED:** Unary reached 93.15 QPS at K4, varied only from 94.23 to
  95.73 QPS through K32, and declined to 89.38 QPS at K64. Its throughput knee
  was therefore around K4-K8 in this topology.
- **VERIFIED:** Streaming QPS increased at every tested concurrency, reaching
  366.84 QPS at K64. This matrix did not find its throughput plateau.
- **VERIFIED:** Streaming had lower combined-median p50, p95, and p99 latency
  at every K. At K64, p50 was 172.68 ms versus 674.14 ms and p99 was 225.14 ms
  versus 1726.08 ms.
- **VERIFIED:** Application-received Streaming data remained fixed at 17.75
  MiB, while median parent connection-read data fell from 92.19 MiB at K1 to
  22.20 MiB at K64. Per-operation transport run-ahead fell from 297.5 to 18.0
  equivalent Chunks.
- **VERIFIED:** Child completed-send bytes and parent connection-read bytes
  remained close at every K. Most bytes accepted by child `SendMsg()` had
  therefore crossed into the parent transport, rather than remaining solely
  at the child application boundary.
- **VERIFIED:** Mode order substantially affected Streaming but barely affected
  Unary. At K1, Streaming-first produced 53.6% more Streaming QPS and 340 fewer
  run-ahead Chunks than Unary-first.
- **LIKELY:** The low-K order effect is consistent with the preceding Unary
  transfer expanding dynamic transport capacity before Streaming starts. This
  run establishes correlation, not the internal flow-control cause.
- **VERIFIED:** The K32 Streaming-first stratum contains a 251.06 QPS run while
  its other repetition reached 344.16 QPS. K64 similarly contains 248.54 and
  370.33 QPS. With only two repetitions per stratum, those high-K order
  medians should not be interpreted as evidence that Streaming-first becomes
  intrinsically slower.

## Caveats

- The benchmark used same-host loopback TCP. It excludes physical network
  latency, packet loss, and cross-machine bandwidth limits.
- Closed-loop concurrency changes scheduling, CPU pressure, memory bandwidth,
  cancellation timing, and transport behavior together. It does not isolate
  one saturation resource.
- The two repetitions per mode-order stratum are enough to expose order
  sensitivity but not to characterize high-K variance robustly.
- Child send, connection, and application counters are cumulative byte-flow
  boundaries. Their differences do not measure instantaneous memory residency.
- The payload is synthetic and excludes ANN execution and Milvus production
  interceptors, retries, tracing, and service discovery.

## Artifacts

- [Normalized per-repetition measurements](runs/20260831T084746Z/summary.tsv)
- [Order-stratified and combined medians](runs/20260831T084746Z/aggregate.tsv)
- [Run manifest](runs/20260831T084746Z/manifest.tsv)
- [Runner log](runs/20260831T084746Z/runner.log)
- [Resource gate](runs/20260831T084746Z/resource_gate_k64.txt)
- [Host environment](runs/20260831T084746Z/environment.txt)
- [Source commit](runs/20260831T084746Z/source_commit.txt)
- [Source status](runs/20260831T084746Z/source_status.txt)
- [Approved test plan](TEST_PLAN.md)

The complete raw logs remain in the same immutable run directory locally and
on `10.15.9.42`.
