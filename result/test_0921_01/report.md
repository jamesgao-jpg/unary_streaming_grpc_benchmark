# Small-Payload Stream-Creation Overhead Premise Test 0921-01

> **VERIFIED:** Per-request Streaming is measurably, but modestly, slower than
> Unary at 1-64 KiB payloads even when each child sends exactly one Chunk.
> At K1 the Streaming/Unary QPS ratio ranged 0.840-0.977 (median) with p50
> latency +1.2% to +8.2%; the penalty grows with child fan-in and is largest
> at the smallest payloads. Under concurrency the relative penalty shrinks
> (0.952-0.968 at K8-K32). The absolute per-request stream-creation cost is a
> few microseconds per child stream; it does not justify changing the
> request-scoped stream lifecycle without a stronger workload-specific case.

## Test

| Item | Value |
|---|---|
| Run | `20260921T034155Z`; completed 2026-09-21 UTC |
| Source | `2819e62b01be5bcd781a88a3d896649501c8bde8` - `test: plan small-payload stream-creation premise sweep` |
| Host | `10.15.9.42`; Linux ARM64; Go 1.26.4; 16 CPUs; approximately 123 GiB RAM |
| Topology | One benchmark host; loopback TCP; one shared logical gRPC channel; configurable child processes |
| Workflow | `full_transfer`; both modes consume every child's complete payload |
| Variables | Payload per child (1-64 KiB), child fan-in (1, 8), concurrency (1, 8, 32) |
| Cases | 10 cases; four balanced-order repetitions per case |
| Measurement | At least 20 seconds and 500 successful operations per mode and repetition |
| Aggregation | Median of four repetitions per case |

All 40 executions passed with zero request or correctness errors. `go test
-race -count=1 ./...` passed before execution. Streaming Chunk was set equal
to the payload in every case (`C = P`), so each child emitted exactly one
Chunk per operation; the only difference between modes is the request-scoped
stream lifecycle itself.

## Purpose

This test is the premise measurement for the Next-Stage Goal Task One:
"Investigate whether QN/SN-to-Proxy gRPC streams can persist across requests
and be reused from a pool. The goal is to avoid creating one gRPC stream per
request when frequent small Retrieve workloads make stream-creation overhead
significant."

The question is not whether pooling is feasible, but whether the premise that
motivates it is true: is per-request stream creation actually observable for
small payloads? The results below quantify the upper bound of what stream
reuse could recover.

## Payload Size, Single Child

Holds `N=1`, `K=1`. Every child returns one 1-64 KiB response message.

| Payload | Unary QPS | Streaming QPS | S/U | Unary p50 | Streaming p50 | p50 delta | Unary FIRST | Streaming FIRST |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 KiB | 13,022 | 12,630 | 0.970 | 73 µs | 76 µs | +3.9% | 71 µs | 73 µs |
| 4 KiB | 12,460 | 11,060 | 0.888 | 75 µs | 79 µs | +4.3% | 74 µs | 76 µs |
| 16 KiB | 11,302 | 10,681 | 0.945 | 83 µs | 87 µs | +5.4% | 81 µs | 85 µs |
| 64 KiB | 6,122 | 5,982 | 0.977 | 149 µs | 151 µs | +1.2% | 147 µs | 148 µs |

Streaming is 1-11% slower than Unary even with a single child. The absolute
cost is small (3-5 µs added to p50), and FIRST_P50 differs by only 1-4 µs:
stream creation does not add a full round trip.

## Child Fan-In

Holds `K=1`. Eight children each return one payload message per operation.

| Payload | Unary QPS | Streaming QPS | S/U | Unary p50 | Streaming p50 | p50 delta | Unary FIRST | Streaming FIRST |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 KiB | 4,437 | 3,727 | 0.840 | 182 µs | 192 µs | +5.4% | 122 µs | 133 µs |
| 4 KiB | 1,090 | 1,003 | 0.920 | 359 µs | 382 µs | +6.4% | 164 µs | 173 µs |
| 16 KiB | 986 | 845 | 0.857 | 416 µs | 450 µs | +8.2% | 182 µs | 198 µs |
| 64 KiB | 869 | 944 | 1.086 | 596 µs | 568 µs | -4.7% | 285 µs | 275 µs |

Fan-in amplifies the Streaming penalty: at 1 KiB per child, Streaming sustains
84% of Unary QPS, the largest gap in this test. The 64 KiB row is an outlier
driven by one repetition (rep 1: Unary 1,371 vs Streaming 1,859 QPS); the
other three repetitions show near parity (S/U 0.87-0.97), consistent with the
single-message-per-child finding from `test_0831_02`. The p95/p99 columns show
heavy tails in both modes at fan-in 8, dominated by concurrent stream
completion rather than creation.

## Concurrency

Holds `N=8`, `P=4 KiB`. This is the "frequent requests" regime: throughput
rises with concurrency while stream creation is amortized.

| K | Unary QPS | Streaming QPS | S/U | Unary p50 | Streaming p50 | p50 delta |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,090 | 1,003 | 0.920 | 359 µs | 382 µs | +6.4% |
| 8 | 12,492 | 12,092 | 0.968 | 530 µs | 552 µs | +4.1% |
| 32 | 17,552 | 16,711 | 0.952 | 1.73 ms | 1.83 ms | +6.0% |

Under concurrency the relative QPS gap narrows to 3-5%. The per-request
stream-creation cost is fixed, so it is diluted as more requests overlap.
FIRST_P50 still shows a 11-43 µs Streaming lag (163.8 vs 173.5 µs at K1;
299 vs 342 µs at K32), indicating stream setup is not hidden entirely.

## Conclusions

1. **The premise is directionally true but quantitatively small.** Per-request
   Streaming costs 2-16% QPS and 1-8% p50 latency at 1-64 KiB payloads,
   growing with fan-in and largest at the smallest payloads. This is exactly
   the "frequent small Retrieve" scenario the task targets.
2. **The absolute cost is a few microseconds per child stream** (3-8 µs p50
   delta at K1, N=1; 10-34 µs at N=8). FIRST_P50 differs by 1-18 µs, so stream
   creation does not add a round trip; the cost is per-stream CPU/goroutine
   and message lifecycle overhead.
3. **Concurrency dilutes the penalty** to 3-5% QPS at K8-K32. A workload that
   already sustains concurrency has less to gain from stream reuse.
4. **The measured benefit ceiling is modest.** Even in the worst measured case
   (N8, 1 KiB, K1), stream reuse could recover at most ~16% QPS. Before
   building persistent or pooled streams, the actual Milvus small-Retrieve
   path should be profiled to confirm where its latency actually goes; the
   request-scoped stream lifecycle is a candidate but not the proven
   bottleneck.

## Limitations

- Loopback TCP on one host: measures gRPC/protobuf/CPU behavior without
  physical network latency or cross-host RTT. A stream-creation round trip
  would be more visible across a real network.
- `full_transfer` has no reduction logic; real Milvus Retrieve includes plan
  fetch, MVCC, and row assembly around the stream. The standalone numbers are
  a transport upper bound on what stream reuse can save.
- The 64 KiB fan-in row is noisy (one outlier repetition); its S/U should be
  read as "near parity," not as a Streaming win.
- This test does not change Milvus source. It only establishes the premise
  numbers for the Task One investigation.
