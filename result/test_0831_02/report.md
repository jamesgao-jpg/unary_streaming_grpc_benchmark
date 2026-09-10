# Full-Transfer Factor Benchmark

> **VERIFIED:** When Unary and Streaming transferred the same complete child
> payload, multi-message Streaming generally reduced QPS and increased p50
> latency. The Streaming/Unary QPS ratio fell from 0.86x to 0.71x as
> concurrency increased from K1 to K64, and from 0.98x to about 0.73x as fan-in
> increased from N4 to N16-N32. Streaming reached parity when each child sent
> its complete payload in one message.

## Test

| Item | Value |
|---|---|
| Run | `20260831T094849Z`; completed 2026-08-31 UTC |
| Source | `d03c62b6d72062919e0e0ffac89e37154e724384` - `docs: document benchmark configuration` |
| Host | `10.15.9.42`; Linux ARM64; Go 1.26.4; 16 CPUs; approximately 123 GiB RAM |
| Topology | One benchmark host; loopback TCP; one shared logical gRPC channel; configurable child processes |
| Workflow | `full_transfer`; both modes consume every child's complete payload |
| Variables | Streaming Chunk size, payload per child, child fan-in, and concurrency |
| Cases | 21 deduplicated cases; four balanced-order repetitions per case |
| Measurement | At least 30 seconds and 200 successful operations per mode and repetition |
| Aggregation | Median of two repetitions per mode-order stratum; combined median over all four repetitions |

All 84 executions passed with zero request or correctness errors. `go test
-race -count=1 ./...` passed before execution. The K64 resource gate observed
approximately 122 GiB available against the required 2 GiB minimum.

Unary and Streaming application-received bytes were identical in every case.
Application retention was zero, so the results isolate complete-transfer RPC
behavior rather than early termination.

## Chunk Size

This sweep holds eight children, 16 MiB per child, and K1. Unary uses eight
response messages per operation.

| Chunk | Streaming messages/op | Unary QPS | Streaming QPS | S/U | Unary p50 | Streaming p50 | p50 delta |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 16 KiB | 8,192 | 81.17 | 59.15 | 0.73x | 11.59 ms | 16.80 ms | +45.0% |
| 64 KiB | 2,048 | 80.94 | 38.90 | 0.48x | 11.61 ms | 25.74 ms | +121.7% |
| 256 KiB | 512 | 80.53 | 66.66 | 0.83x | 11.64 ms | 14.96 ms | +28.5% |
| 1 MiB | 128 | 80.66 | 61.72 | 0.77x | 11.60 ms | 16.66 ms | +43.6% |
| 4 MiB | 32 | 80.06 | 78.17 | 0.98x | 11.66 ms | 12.66 ms | +8.6% |
| 16 MiB | 8 | 80.52 | 80.56 | 1.00x | 11.63 ms | 11.61 ms | -0.2% |

The curve is not monotonic: 64 KiB performed worse than 16 KiB, and 1 MiB
performed worse than 256 KiB. This experiment therefore does not establish a
generally optimal Chunk size.

## Payload Size

This sweep holds eight children, 256 KiB Chunks, and K1.

| Payload/child | Streaming messages/op | Unary QPS | Streaming QPS | S/U | Unary p50 | Streaming p50 | p50 delta |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 MiB | 32 | 752.76 | 645.78 | 0.86x | 1.23 ms | 1.49 ms | +21.0% |
| 4 MiB | 128 | 288.18 | 230.03 | 0.80x | 3.21 ms | 4.31 ms | +33.9% |
| 16 MiB | 512 | 80.53 | 66.66 | 0.83x | 11.64 ms | 14.96 ms | +28.5% |
| 32 MiB | 1,024 | 41.84 | 34.45 | 0.82x | 22.35 ms | 28.95 ms | +29.5% |
| 60 MiB | 1,920 | 22.39 | 18.41 | 0.82x | 41.68 ms | 54.09 ms | +29.8% |

Beyond the 1 MiB case, Streaming sustained approximately 80-83% of Unary QPS
and added approximately 29-34% p50 latency. Increasing payload did not make the
relative penalty grow continuously.

## Child Fan-In

This sweep holds 16 MiB per child, 256 KiB Chunks, and K1. Aggregate payload
increases with child count.

| Children | Streaming messages/op | Unary QPS | Streaming QPS | S/U | Unary p50 | Streaming p50 | p50 delta |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 64 | 134.12 | 162.41 | 1.21x | 6.90 ms | 6.12 ms | -11.4% |
| 2 | 128 | 118.92 | 133.40 | 1.12x | 7.60 ms | 7.47 ms | -1.8% |
| 4 | 256 | 103.04 | 101.15 | 0.98x | 8.81 ms | 9.86 ms | +12.0% |
| 8 | 512 | 80.53 | 66.66 | 0.83x | 11.64 ms | 14.96 ms | +28.5% |
| 16 | 1,024 | 47.98 | 35.19 | 0.73x | 19.90 ms | 27.96 ms | +40.5% |
| 32 | 2,048 | 24.41 | 18.14 | 0.74x | 39.21 ms | 54.92 ms | +40.1% |

Streaming was faster at N1-N2, approximately equal at N4, and progressively
slower through N16. Because aggregate payload and fan-in change together, this
sweep measures their combined effect.

## Concurrency

This sweep holds eight children, 1 MiB per child, and 256 KiB Chunks. Unary
uses eight messages and Streaming uses 32 messages per operation.

| K | Unary QPS | Streaming QPS | S/U | Unary p50 | Streaming p50 | p50 delta |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 752.76 | 645.78 | 0.86x | 1.23 ms | 1.49 ms | +21.0% |
| 2 | 934.68 | 778.00 | 0.83x | 2.12 ms | 2.57 ms | +21.4% |
| 4 | 1,101.58 | 888.83 | 0.81x | 3.61 ms | 4.47 ms | +23.7% |
| 8 | 1,226.91 | 937.55 | 0.76x | 6.48 ms | 8.55 ms | +31.9% |
| 16 | 1,302.84 | 963.00 | 0.74x | 12.27 ms | 16.79 ms | +36.8% |
| 32 | 1,369.97 | 981.77 | 0.72x | 23.46 ms | 33.12 ms | +41.2% |
| 64 | 1,413.40 | 999.57 | 0.71x | 45.38 ms | 64.41 ms | +41.9% |

Both modes gained QPS with concurrency, but Streaming gained less. Its QPS
penalty expanded from 14% at K1 to 29% at K64, while its p50 penalty expanded
from 21% to 42%.

## Mode Order

The normalized aggregate retains Unary-first and Streaming-first medians for
all 21 cases. The largest Streaming order differences were:

| Case | Unary-first Streaming QPS | Streaming-first Streaming QPS | Difference |
|---|---:|---:|---:|
| 1 MiB Chunk | 50.15 | 73.30 | +46.2% |
| 4 MiB Chunk | 74.00 | 81.84 | +10.6% |
| N1 fan-in | 151.31 | 164.66 | +8.8% |
| 64 KiB Chunk | 40.79 | 37.01 | -9.3% |
| 60 MiB payload | 19.22 | 17.00 | -11.6% |

The other cases had less than 5% Streaming QPS difference between order
strata. The Chunk sweep should still be interpreted cautiously because its
most non-monotonic cases include substantial order sensitivity.

## Relation to Application-Aware Streaming

At the shared N8, 16 MiB per child, 256 KiB Chunk, and K1 transport shape:

| Workflow | Unary QPS | Streaming QPS | S/U | Unary app bytes | Streaming app bytes |
|---|---:|---:|---:|---:|---:|
| Full transfer (`0831_02`) | 80.53 | 66.66 | 0.83x | 128 MiB | 128 MiB |
| Ordered topK (`0831_01`) | 70.96 | 91.78 | 1.29x | 128 MiB | 17.75 MiB |

**VERIFIED:** Avoiding irrelevant child tails changed the observed end-to-end
outcome from a Streaming penalty to a Streaming advantage in this matched
transport shape. The exact difference is not a pure estimate of saved-transfer
value because `ordered_topk` also performs ordered merge work that
`full_transfer` omits.

## Findings

- **VERIFIED:** Streaming and Unary were at parity when each child returned one
  complete-payload message.
- **VERIFIED:** With 256 KiB Chunks and N8, equal-byte Streaming sustained
  80-86% of Unary QPS across 1-60 MiB per-child payloads.
- **VERIFIED:** Equal-byte Streaming overhead increased with fan-in after N4
  and with concurrency across K1-K64.
- **VERIFIED:** Streaming improved first-response latency in multi-Chunk cases,
  but complete consumption normally produced worse request-completion latency.
- **VERIFIED:** Equal application bytes, child completed-send bytes, and parent
  connection-read bytes were recorded in every mode, confirming that early
  termination did not influence this experiment.

## Caveats

- The benchmark used same-host loopback TCP. It excludes physical network
  latency, loss, and bandwidth limits.
- The synthetic payload excludes ANN execution, ordered reduction, Milvus
  interceptors, retries, tracing, and service discovery.
- The fan-in sweep increases aggregate bytes together with child count.
- Two repetitions per mode-order stratum expose large order effects but do not
  characterize their variance robustly.
- Byte counters are cumulative transport boundaries, not instantaneous memory
  residency.

## Artifacts

- [Normalized per-repetition measurements](runs/20260831T094849Z/summary.tsv)
- [Order-stratified and combined medians](runs/20260831T094849Z/aggregate.tsv)
- [Run manifest](runs/20260831T094849Z/manifest.tsv)
- [Runner log](runs/20260831T094849Z/runner.log)
- [K64 resource gate](runs/20260831T094849Z/k64_resource_gate.txt)
- [Host environment](runs/20260831T094849Z/environment.txt)
- [Source commit](runs/20260831T094849Z/source_commit.txt)
- [Source status](runs/20260831T094849Z/source_status.txt)
- [Approved test plan](TEST_PLAN.md)
