# Equal-TopK Unary Versus Streaming Benchmark

Status: Proposed

## Objective

Compare Unary and Streaming when each child owns the same topK payload that the
parent must ultimately emit:

```text
payload per child = global topK * Unit size
```

Unary receives every child's complete topK before merging. Streaming performs
the same ordered merge while receiving Chunks and stops after emitting the
global topK. Both modes therefore satisfy the same logical request.

This complements `test_0827_01`, which held global topK at 1 MiB while increasing
child payload to isolate transport run-ahead.

## Source Baseline

The proposed source baseline is commit `77f5fbd`. It includes the application,
gRPC, connection, and Linux TCP metrics introduced by `440055c`.

The runner records the actual source commit, branch status, diff, environment,
binary, generated configuration, and raw logs.

## Fixed Setup

| Parameter | Value |
| --- | ---: |
| Workflow | `ordered_topk` |
| Unit size | 256 B |
| Streaming Chunk size | 256 KiB |
| Distribution | `interleaved` |
| Concurrency | 1 |
| Warm-up requests per mode | 3 |
| Minimum measured requests per mode | 10 |
| Minimum measurement duration per mode | 1 second |
| Repetitions | 4 |

Mode order is balanced across fresh processes:

```text
repetitions 1 and 3: Unary first
repetitions 2 and 4: Streaming first
```

## Matrix

| Case | Children | Payload per child | Global topK | Final topK payload | Unary available bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| `EQUALTOPK-N2-P4` | 2 | 4 MiB | 16,384 | 4 MiB | 8 MiB |
| `EQUALTOPK-N2-P16` | 2 | 16 MiB | 65,536 | 16 MiB | 32 MiB |
| `EQUALTOPK-N2-P32` | 2 | 32 MiB | 131,072 | 32 MiB | 64 MiB |
| `EQUALTOPK-N2-P60` | 2 | 60 MiB | 245,760 | 60 MiB | 120 MiB |
| `EQUALTOPK-N8-P4` | 8 | 4 MiB | 16,384 | 4 MiB | 32 MiB |
| `EQUALTOPK-N8-P16` | 8 | 16 MiB | 65,536 | 16 MiB | 128 MiB |
| `EQUALTOPK-N8-P32` | 8 | 32 MiB | 131,072 | 32 MiB | 256 MiB |
| `EQUALTOPK-N8-P60` | 8 | 60 MiB | 245,760 | 60 MiB | 480 MiB |

The 60 MiB Unary response remains below the configured 64 MiB client receive
limit after protobuf framing.

## Recorded Metrics

- QPS, p50/p95/p99 latency, and time to first response.
- Parent application messages and bytes received per child.
- Child attempted and completed send messages and bytes.
- Received, emitted, and unused Units.
- Parent gRPC application, protobuf, and gRPC-wire bytes.
- Child and parent `net.Conn` read/write bytes.
- Linux TCP bytes received, sent, acknowledged, and not-sent bytes.
- Application payload left unconsumed when global topK completes.

## Primary Comparisons

For each case and mode, calculate medians separately for Unary-first and
Streaming-first repetitions before reporting the combined result.

```text
Unary transfer fraction = Unary received bytes / (children * payload per child)
Streaming application fraction = Streaming received bytes / (children * payload per child)
Streaming transport fraction = parent connection-read bytes / (children * payload per child)
Streaming completed-send fraction = completed bytes / (children * payload per child)
QPS delta = Streaming QPS / Unary QPS - 1
latency delta = Streaming latency / Unary latency - 1
```

Report absolute bytes per operation together with fractions. Do not infer peak
memory from cumulative transport bytes.

## Procedure

1. Run on the Linux benchmark server from the recorded source state.
2. Run `go test -race -count=1 ./...` and build one immutable binary.
3. Execute every matrix cell four times in a fresh benchmark process.
4. Alternate mode order as recorded above.
5. Preserve generated YAML, raw logs, source evidence, environment, and manifest.
6. Verify Unary and Streaming output hashes and emitted Unit counts match.
7. Normalize all cumulative byte counters by successful operations.
8. Compare mode-order groups before combining repetitions.
9. Compare against 0827-01 only where topology, payload, and mode order match.

## Acceptance Criteria

- All executions complete without correctness or request errors.
- Unary and Streaming each emit exactly the configured global topK.
- Unary receives exactly `children * payload per child` application bytes.
- Every child emits one application, gRPC, and transport metric record per mode.
- TCP counters are available at both connection endpoints.
- Streaming completed bytes do not exceed all child payload bytes available.
- Conclusions report mode-order sensitivity and do not treat cumulative bytes as
  simultaneous retained memory.
- Raw evidence and the exact source state are retained before completion.
