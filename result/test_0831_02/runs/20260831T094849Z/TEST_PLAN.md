# Full-Transfer Factor Benchmark

Status: Approved

## Objective

Measure the transport cost of Streaming gRPC relative to Unary gRPC when both
modes transfer every child's complete payload. This removes the
application-level transfer saving provided by the `ordered_topk` workflow.

Every case uses:

```text
workflow = full_transfer
Unary bytes per operation = children * payload per child
Streaming bytes per operation = children * payload per child
```

`total_payload_bytes_per_child` is the per-child topK payload proxy. The
`full_transfer` workflow does not perform reduction, so `global_topk` is not
used.

## Questions

1. How much QPS and latency overhead does Streaming add when it saves no data?
2. How does that difference change with Chunk size, per-child payload, fan-in,
   and concurrency?
3. Does Streaming approach Unary when one Streaming Chunk contains the entire
   per-child payload?
4. Does concurrency amplify the cost of multiple Streaming messages and
   `RecvMsg()` calls?

## Topology

```text
Benchmark parent process
    |
    | one logical gRPC channel
    |
    +-- child process 0
    +-- child process 1
    +-- ...
    +-- child process N-1
```

One fan-in operation starts one Unary RPC or one Streaming stream per child.
All processes run on one Linux host over loopback TCP. Connections are
established before measurement and reused within each benchmark process.

## Parameters

| Symbol | YAML field | Meaning |
| --- | --- | --- |
| `N` | `child_processes` | Child processes and RPCs or streams per operation |
| `P` | `total_payload_bytes_per_child` | Complete payload returned by each child |
| `C` | `stream_chunk_bytes` | Maximum payload in one Streaming response |
| `K` | `concurrency` | Concurrent fan-in operations |

Derived values:

```text
payload bytes per operation = N * P
Unary response messages per operation = N
Streaming response messages per operation = N * ceil(P / C)
maximum concurrent RPCs or streams = N * K
```

## Fixed Controls

| Parameter | Value |
| --- | ---: |
| Workflow | `full_transfer` |
| Parent receive window | Dynamic (`static_window_bytes: 0`) |
| Compression | Disabled |
| Internal TLS | Disabled |
| Warm-up operations per mode | 3 |
| Minimum measured operations per mode | 200 |
| Minimum measurement duration per mode | 30 seconds |
| Repetitions per case | 4 |
| Request timeout | 120 seconds |

Repetitions 1 and 3 run Unary first. Repetitions 2 and 4 run Streaming first.
Each repetition starts a fresh benchmark process.

## Matrix

### Chunk Sweep

Hold `N=8`, `P=16 MiB`, and `K=1`.

| Case | Chunk | Messages per child | Messages per operation |
| --- | ---: | ---: | ---: |
| `FT-CHUNK-C16K` | 16 KiB | 1,024 | 8,192 |
| `FT-CHUNK-C64K` | 64 KiB | 256 | 2,048 |
| `FT-CHUNK-C256K` | 256 KiB | 64 | 512 |
| `FT-CHUNK-C1M` | 1 MiB | 16 | 128 |
| `FT-CHUNK-C4M` | 4 MiB | 4 | 32 |
| `FT-CHUNK-C16M` | 16 MiB | 1 | 8 |

### Payload/TopK Sweep

Hold `N=8`, `C=256 KiB`, and `K=1`.

| Case | Payload per child | Payload per operation |
| --- | ---: | ---: |
| `FT-PAYLOAD-P1` | 1 MiB | 8 MiB |
| `FT-PAYLOAD-P4` | 4 MiB | 32 MiB |
| `FT-CHUNK-C256K` | 16 MiB | 128 MiB |
| `FT-PAYLOAD-P32` | 32 MiB | 256 MiB |
| `FT-PAYLOAD-P60` | 60 MiB | 480 MiB |

The 16 MiB baseline reuses `FT-CHUNK-C256K`.

### Fan-In Sweep

Hold `P=16 MiB` per child, `C=256 KiB`, and `K=1`. Aggregate payload is
intentionally not held constant.

| Case | Children | Payload per operation |
| --- | ---: | ---: |
| `FT-FANIN-N1` | 1 | 16 MiB |
| `FT-FANIN-N2` | 2 | 32 MiB |
| `FT-FANIN-N4` | 4 | 64 MiB |
| `FT-CHUNK-C256K` | 8 | 128 MiB |
| `FT-FANIN-N16` | 16 | 256 MiB |
| `FT-FANIN-N32` | 32 | 512 MiB |

The N8 baseline reuses `FT-CHUNK-C256K`.

### Concurrency Sweep

Hold `N=8`, `P=1 MiB`, and `C=256 KiB` so logical in-flight bytes remain
bounded.

| Case | Concurrency | Maximum concurrent RPCs | Logical bytes in flight |
| --- | ---: | ---: | ---: |
| `FT-PAYLOAD-P1` | 1 | 8 | 8 MiB |
| `FT-CONCURRENCY-K2` | 2 | 16 | 16 MiB |
| `FT-CONCURRENCY-K4` | 4 | 32 | 32 MiB |
| `FT-CONCURRENCY-K8` | 8 | 64 | 64 MiB |
| `FT-CONCURRENCY-K16` | 16 | 128 | 128 MiB |
| `FT-CONCURRENCY-K32` | 32 | 256 | 256 MiB |
| `FT-CONCURRENCY-K64` | 64 | 512 | 512 MiB |

The K1 baseline reuses `FT-PAYLOAD-P1`. Before K64, the runner requires all
four K32 repetitions to pass and at least 2 GiB of host `MemAvailable`. The
memory threshold is four times the K64 logical in-flight payload.

The deduplicated matrix contains 21 cases and 84 benchmark process runs.

## Metrics

Record the current benchmark outputs for every mode and child:

- QPS, p50/p95/p99 latency, and first-response latency.
- Parent application messages and payload bytes.
- Parent protobuf and gRPC wire bytes delivered to `RecvMsg()`.
- Child attempted and completed send messages and payload bytes.
- Child and parent `net.Conn` read/write bytes.
- Linux TCP sent, acknowledged, received, and not-sent bytes.
- Full-transfer application retention, which must remain zero.

Primary comparisons within each matrix cell:

```text
QPS ratio = Streaming QPS / Unary QPS
latency ratio = Streaming latency / Unary latency
message ratio = Streaming response messages / Unary response messages
bytes ratio = Streaming application bytes / Unary application bytes
```

Report medians separately for Unary-first and Streaming-first repetitions,
then report the combined median.

## Procedure

1. Start one detached, timestamped run on the Linux benchmark server.
2. Record the source commit, status, diff, host environment, plan, and runner.
3. Run `go test -race -count=1 ./...` and build one immutable benchmark binary.
4. Generate one YAML configuration per case and repetition.
5. Run four balanced-order repetitions for every unique case.
6. Validate complete payload receipt and current per-child gRPC/transport metrics.
7. Apply the K64 resource gate after K32 completes.
8. Preserve generated YAML, raw logs, manifest, and completion marker.

Run from the repository root:

```bash
./result/test_0831_02/run.sh
```

The command returns immediately with the worker PID and artifact directory.

## Acceptance Criteria

- All 84 executions complete without correctness or request errors.
- Every mode completes at least 200 measured operations and 30 seconds.
- Unary and Streaming each receive exactly `N * P` bytes per operation.
- Unary reports exactly `N` response messages per operation.
- Streaming reports exactly `N * ceil(P / C)` messages per operation.
- Every child reports application, gRPC, connection, and Linux TCP metrics.
- Application retention is zero in both modes.
- K64 runs only after its resource gate succeeds.
- Exact source state, generated configurations, raw logs, and manifest remain
  available in the timestamped run directory.

## Boundary

This test isolates complete-transfer RPC behavior. It does not execute ordered
merge, early termination, ANN search, or production Milvus interceptors. Byte
counters are cumulative transport evidence, not instantaneous memory readings.
