# Unary versus Streaming gRPC Transport Baseline

> **VERIFIED:** On same-host loopback with equal complete payload bytes, 256 KiB
> Streaming Chunks at fan-in 8 reduced QPS by 12-21% and increased p50 latency
> by 17-36% for 1-32 MiB per-child payloads; one response message per child was
> at parity with Unary.

## Questions

| ID | Benchmark question | Status |
|---|---|---|
| Q1 | When every child transfers the same complete payload, how do Unary and fixed-Chunk Streaming compare in latency and QPS? | Answered by this run |
| Q2 | When `ReduceStream` stops children after producing global topK, how much transfer must be avoided before Streaming becomes faster than Unary? | Not answered by this run |

## Test

| Item | Value |
|---|---|
| Run | `20260820T033454Z`; completed 2026-08-20 UTC |
| Source | `2e14cac6cf1bfe9acc5511ef8ff5c7fd5394c417` - `bench: share one channel across children` |
| Branch / tree | `master`; dirty; the execution patch is preserved in Artifacts |
| Host | `10.15.9.42`; Linux ARM64; Go `1.26.4` |
| Topology | One benchmark host; loopback TCP; one shared logical gRPC channel; 1-32 child processes |
| Workload | 23 valid cases, five repetitions per case, 20 warm-up operations, then at least 20 seconds and 200 completed operations per mode |
| Variables | Per-child payload `P`, Streaming Chunk size `C`, child count `N`, and fan-in concurrency `K` |
| Baseline | Unary transfers `P` bytes once per child; Streaming transfers the same `P` bytes in `ceil(P/C)` messages per child |
| Order control | Odd repetitions ran Unary first; even repetitions ran Streaming first |
| Aggregation | Arithmetic mean within each mode-order stratum, followed by an equal-weight mean of the two strata |

Every valid repetition exited successfully, reported zero request errors, and
completed the configured minimum work. The separate 64 MiB invalid case was
rejected by the configured gRPC message-limit validation as expected.

## Results

The primary comparison holds `N=8`, `C=256 KiB`, and `K=1`. Negative QPS
delta favors Unary; negative first-response delta favors Streaming.

| P per child | Unary QPS | Streaming QPS | QPS delta | Unary p50 | Streaming p50 | p50 delta | First-response delta |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 KiB | 1511.03 | 1510.99 | 0.0% | 0.586 ms | 0.585 ms | -0.1% | +0.3% |
| 1 MiB | 724.72 | 638.36 | -11.9% | 1.274 ms | 1.494 ms | +17.3% | -24.5% |
| 4 MiB | 287.65 | 226.77 | -21.2% | 3.211 ms | 4.373 ms | +36.2% | -78.9% |
| 16 MiB | 80.73 | 66.97 | -17.0% | 11.602 ms | 15.003 ms | +29.3% | -94.5% |
| 32 MiB | 42.05 | 34.56 | -17.8% | 22.394 ms | 28.933 ms | +29.2% | -97.2% |

## Findings

- **VERIFIED:** When each child returned one response message, Unary and
  Streaming were at parity: QPS differed by at most 0.1% in `PAYLOAD-001` and
  `CHUNK-006`.
- **VERIFIED:** With 256 KiB Chunks, fan-in 8, and complete consumption,
  Streaming reduced QPS by 12-21% and increased p50 by 17-36% for 1-32 MiB
  per-child payloads.
- **VERIFIED:** At fan-in 8 with a 1 MiB per-child payload, increasing
  concurrency from 1 to 64 expanded the Streaming QPS penalty from 12% to 28%
  and the p50 penalty from 18% to 40%.
- **VERIFIED:** With aggregate payload fixed at 32 MiB, Streaming QPS was 16%
  and 11% higher at fan-in 1 and 2, near parity at fan-in 4, and 15-21% lower
  at fan-in 8-32.
- **VERIFIED:** Streaming substantially improved time to first response for
  multi-Chunk payloads, but this run consumed every Chunk, so that improvement
  did not establish a completed-request latency benefit.

## Caveats

- The topology used loopback TCP on one host. It excludes physical network
  latency, packet loss, and cross-machine bandwidth constraints.
- Every child transferred its complete payload in both modes. The run measured
  transport overhead, not `ReduceStream`, ordered merge, or early termination.
- The payload was synthetic bytes and did not include ANN search execution or
  production Milvus interceptors, retries, tracing, or service discovery.
- The Chunk-size sweep was not monotonic. In `CHUNK-004`, Streaming QPS was 48%
  higher when Streaming ran first than when it ran second, showing material
  mode-order or system-state sensitivity at that point.
- The benchmark ran from a dirty tree. The exact patch affecting execution is
  retained with the source commit and status.

## Details

### Chunk Size

This sweep holds `N=8`, `P=16 MiB`, and `K=1`.

| C | Messages per child | Unary QPS | Streaming QPS | QPS delta | Unary p50 | Streaming p50 | p50 delta |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 16 KiB | 1024 | 80.66 | 59.26 | -26.5% | 11.58 ms | 16.73 ms | +44.5% |
| 64 KiB | 256 | 80.35 | 38.45 | -52.1% | 11.61 ms | 26.22 ms | +125.9% |
| 256 KiB | 64 | 80.32 | 66.09 | -17.7% | 11.67 ms | 15.03 ms | +28.8% |
| 1 MiB | 16 | 80.40 | 61.77 | -23.2% | 11.65 ms | 16.64 ms | +42.9% |
| 4 MiB | 4 | 79.87 | 77.54 | -2.9% | 11.67 ms | 12.64 ms | +8.3% |
| 16 MiB | 1 | 80.12 | 80.02 | -0.1% | 11.65 ms | 11.66 ms | +0.1% |

### Child Fan-In

This sweep holds aggregate payload at 32 MiB, `C=256 KiB`, and `K=1`.

| N | P per child | Unary QPS | Streaming QPS | QPS delta | Unary p50 | Streaming p50 | p50 delta |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 32 MiB | 67.39 | 78.00 | +15.7% | 13.73 ms | 12.80 ms | -6.8% |
| 2 | 16 MiB | 118.42 | 130.86 | +10.5% | 7.58 ms | 7.64 ms | +0.8% |
| 4 | 8 MiB | 198.08 | 191.79 | -3.2% | 4.55 ms | 5.19 ms | +14.2% |
| 8 | 4 MiB | 287.01 | 227.66 | -20.7% | 3.21 ms | 4.36 ms | +35.6% |
| 16 | 2 MiB | 275.20 | 225.44 | -18.1% | 3.54 ms | 4.42 ms | +24.6% |
| 32 | 1 MiB | 245.36 | 207.70 | -15.3% | 4.04 ms | 4.79 ms | +18.6% |

### Concurrency

This sweep holds `N=8`, `P=1 MiB`, and `C=256 KiB`.

| K | Unary QPS | Streaming QPS | QPS delta | Unary p50 | Streaming p50 | p50 delta |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 725.37 | 637.73 | -12.1% | 1.269 ms | 1.493 ms | +17.7% |
| 4 | 1083.40 | 882.75 | -18.5% | 3.671 ms | 4.495 ms | +22.4% |
| 16 | 1293.88 | 960.78 | -25.7% | 12.359 ms | 16.784 ms | +35.8% |
| 32 | 1361.44 | 992.87 | -27.1% | 23.650 ms | 32.582 ms | +37.8% |
| 64 | 1399.25 | 1007.12 | -28.0% | 45.702 ms | 63.793 ms | +39.6% |

## Artifacts

- [Normalized per-repetition measurements](results/20260820T033454Z/summary.tsv)
- [Run manifest](results/20260820T033454Z/manifest.tsv)
- [Source commit](results/20260820T033454Z/source_commit.txt)
- [Source status](results/20260820T033454Z/source_status.txt)
- [Execution patch](results/20260820T033454Z/source_diff.patch)
- [Host environment](results/20260820T033454Z/environment.txt)
- [Approved test plan](TEST_PLAN.md)

The complete raw logs remain in the immutable ARM run directory:
`/home/ubuntu/unary_streaming_grpc_benchmark/result/test_0820_01/runs/20260820T033454Z`.
