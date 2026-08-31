# Ordered TopK Concurrency Benchmark

Status: Approved

## Objective

Measure how higher concurrency changes Unary-versus-Streaming QPS, latency,
and transport behavior while the existing application-aware `ordered_topk`
workflow remains enabled.

Each child owns the same 16 MiB ordered result. Unary receives all eight child
results before reduction. Streaming receives ordered Chunks and stops child
streams after producing the 16 MiB global topK.

## Questions

1. Where do Unary and Streaming QPS stop scaling as concurrency rises?
2. How does the Streaming/Unary QPS ratio change before and after saturation?
3. How do p50, p95, and p99 latency change near the throughput knee?
4. Does concurrent Streaming increase child send completion or parent
   connection-read run-ahead per operation?

## Fixed Setup

| Parameter | Value |
| --- | ---: |
| Workflow | `ordered_topk` |
| Children | 8 |
| Payload per child | 16 MiB |
| Global topK | 65,536 Units / 16 MiB |
| Unit size | 256 B |
| Streaming Chunk | 256 KiB / 1,024 Units |
| Result distribution | `interleaved` |
| Parent receive window | dynamic (`static_window_bytes: 0`) |
| Logical gRPC channels | 1 |
| Warm-up requests per mode | 20 |
| Minimum measured requests per mode | 200 |
| Minimum measurement duration per mode | 20 seconds |
| Repetitions per case | 4 |

Repetitions 1 and 3 run Unary first. Repetitions 2 and 4 run Streaming first.
Every repetition starts a fresh benchmark process while reusing persistent
connections within that process.

## Matrix

Only concurrency changes:

| Case | Concurrency | Maximum active child RPCs | Logical Unary bytes in flight |
| --- | ---: | ---: | ---: |
| `CONCURRENCY-K1` | 1 | 8 | 128 MiB |
| `CONCURRENCY-K2` | 2 | 16 | 256 MiB |
| `CONCURRENCY-K4` | 4 | 32 | 512 MiB |
| `CONCURRENCY-K8` | 8 | 64 | 1 GiB |
| `CONCURRENCY-K16` | 16 | 128 | 2 GiB |
| `CONCURRENCY-K32` | 32 | 256 | 4 GiB |
| `CONCURRENCY-K64` | 64 | 512 | 8 GiB |

The matrix contains 7 cases and 28 benchmark process executions. Each process
measures both modes, for 56 measured mode intervals and at least 1,120 seconds
of measurement time before warm-up, startup, and verification.

## Hypotheses

- Both modes should gain QPS below saturation and then plateau while latency
  rises.
- Unary should approach its resource limit earlier because every operation
  receives 128 MiB from children.
- Streaming/Unary QPS should initially rise because Streaming normally consumes
  approximately `topK + (children - 1) * Chunk`, or 17.75 MiB per operation.
- At high concurrency, Streaming may plateau from active-stream scheduling,
  message processing, ordered merging, cancellation, and transport run-ahead.

These are hypotheses, not acceptance criteria.

## Recorded Metrics

Retain the benchmark's existing metrics for each mode and child:

- QPS, logical MiB/s, p50/p95/p99 latency, and time to first response.
- Success and error counts.
- Parent application messages, payload bytes, protobuf bytes, and gRPC wire
  bytes delivered to `RecvMsg()`.
- Child attempted and completed send messages and payload bytes.
- Child and parent `net.Conn` read/write bytes.
- Linux TCP sent, acknowledged, received, and not-sent bytes.
- Received, emitted, and unused Units.
- Unary and Streaming output count and correctness hash verification.

## Derived Comparisons

Normalize cumulative counters by successful operations before calculating:

```text
QPS ratio = Streaming QPS / Unary QPS
application overhead = parent application received bytes - final topK bytes
transport run-ahead = parent connection read bytes - parent application received bytes
gRPC send queue gap = child completed-send bytes - child connection-write bytes
transport efficiency = final topK bytes / parent connection read bytes
active child RPCs = concurrency * children
```

Report medians separately for Unary-first and Streaming-first repetitions, then
report the combined median. Do not treat cumulative byte counters as
simultaneously retained memory.

## Resource-Safety Gate

`CONCURRENCY-K64` runs only after all 24 executions through K32 pass.
Immediately before K64, the runner records `/proc/meminfo` and requires:

```text
MemAvailable >= 2 * children * payload per child * concurrency
             >= 16 GiB
```

The factor of two gives the 8 GiB logical Unary in-flight volume explicit
headroom for protobuf, gRPC, child-process, and benchmark allocations. A failed
gate stops the run and preserves a `FAILED` marker and `resource_gate_k64.txt`;
it does not silently omit K64.

## Procedure

1. Start one detached, timestamped artifact run on the Linux benchmark host.
2. Record source commit, status, diff, untracked paths, environment, test plan,
   and runner.
3. Run `go test -race -count=1 ./...` and build one immutable benchmark binary.
4. Generate one YAML configuration for every case and repetition.
5. Run K1 through K32 sequentially with four balanced-order repetitions each.
6. Apply the documented resource-safety gate, then run K64.
7. Validate successful output, ordered topK count, per-child metrics, and Linux
   TCP counter availability after every execution.
8. Preserve raw logs, generated YAML, manifest, and completion marker.

Run from the repository root:

```bash
./result/test_0831_01/run.sh
```

The launcher prints the PID and artifact directory. Follow progress with:

```bash
tail -f result/test_0831_01/runs/<UTC-run-id>/runner.log
```

## Acceptance Criteria

- All 28 executions complete with zero request errors.
- Unary and Streaming each emit exactly 65,536 Units and pass the benchmark's
  output-hash equivalence check.
- Unary receives exactly 128 MiB of application payload per operation.
- Every child emits application, gRPC, transport, and Linux TCP metrics per
  mode.
- Every mode measures for at least 20 seconds and completes at least 200
  operations.
- K64 runs only after the resource-safety gate passes.
- Mode-order effects are reported before repetitions are combined.
- Raw evidence and exact source state are retained before completion.

## Boundary

This experiment measures closed-loop concurrency on one host and one logical
gRPC channel. It does not separate CPU saturation from memory-bandwidth or
loopback-network saturation, and cumulative transport counters do not identify
instantaneous memory residency inside gRPC or the kernel.
