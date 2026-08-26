# Ordered TopK Streaming Test 0826-01

## Status

Proposed. Per-child instrumentation is implemented, but do not treat this
document as test evidence until a run directory contains `COMPLETED` and its
manifest has been reviewed.

## Objective

Measure when ordered-topK Streaming outperforms Unary after accounting for both:

- Streaming's additional RPC-message and `RecvMsg()` overhead.
- Streaming's ability to stop receiving child results once the parent has
  emitted the requested global topK.

Full-transfer controls measure Unary versus Streaming when both modes receive
the same payload bytes and no early stopping is possible.

## Topology

```text
Benchmark parent process
    |
    | one gRPC ClientConn with child-address routing
    |
    +-- child process 0
    +-- child process 1
    +-- ...
    +-- child process N-1
```

Each fan-in operation opens one Unary RPC or one bidirectional Streaming RPC
per child. Every child owns a complete, locally ordered result of `T` Units.

## Parameters

| Symbol | YAML field | Meaning |
| --- | --- | --- |
| `N` | `child_processes` | Cross-process children participating in one fan-in |
| `U` | `per_unit_bytes` | Bytes in one synthetic ordered Unit |
| `T` | `global_topk` | Units emitted by the parent |
| `P` | `total_payload_bytes_per_child` | Complete ordered result bytes held by each child |
| `C` | `stream_chunk_bytes` | Maximum payload bytes in one Streaming response |
| `R` | `concurrency` | Concurrent fan-in operations |
| `D` | `result_distribution` | Child rank distribution |

For every ordered-topK case:

```text
P = T * U
potential child-result bytes per operation = N * P
required final output bytes per operation = T * U
```

`interleaved` distributes consecutive global ranks across all children.
`dominant_child` places the first `T` global ranks in child 0.

## Phase-One Matrix

### TopK Scale

Hold `N=8`, `U=256 B`, `C=256 KiB`, and `R=1`. Run every row once with
`D=interleaved` and once with `D=dominant_child`.

| Case suffix | `T` | `P` per child |
| --- | ---: | ---: |
| `001` | 512 | 128 KiB |
| `002` | 1,024 | 256 KiB |
| `003` | 4,096 | 1 MiB |
| `004` | 16,384 | 4 MiB |
| `005` | 65,536 | 16 MiB |

Case IDs use `TOPK-INT-<suffix>` and `TOPK-DOM-<suffix>`.

### Chunk Size

Hold `N=8`, `T=16,384`, `U=256 B`, `P=4 MiB`, and `R=1`. Run every row
once with each result distribution.

| Case suffix | `C` | Maximum messages per child without early stopping |
| --- | ---: | ---: |
| `001` | 16 KiB | 256 |
| `002` | 64 KiB | 64 |
| `003` | 256 KiB | 16 |
| `004` | 1 MiB | 4 |
| `005` | 4 MiB | 1 |

Case IDs use `CHUNK-INT-<suffix>` and `CHUNK-DOM-<suffix>`.

### Full-Transfer Controls

Hold `N=8`, `C=256 KiB`, and `R=1`.

| Case | `P` per child | Logical bytes per operation |
| --- | ---: | ---: |
| `CONTROL-001` | 1 MiB | 8 MiB |
| `CONTROL-002` | 4 MiB | 32 MiB |
| `CONTROL-003` | 16 MiB | 128 MiB |

These cases use `workflow=full_transfer`; Unary and Streaming must both receive
all `N * P` application payload bytes.

## Execution Controls

- Run five repetitions of every matrix cell.
- Odd repetitions run Unary before Streaming.
- Even repetitions run Streaming before Unary.
- Warm up each mode with 20 fan-in operations.
- Measure each mode for at least 20 seconds and 200 completed operations.
- Reuse the same pre-established `ClientConn` within each repetition.
- Start child processes outside measured intervals.
- Run on an otherwise idle host.
- Preserve the source revision, working-tree diff, generated configurations,
  raw logs, environment, and manifest for every run.

The phase contains 23 cells and 115 benchmark executions. Its minimum measured
duration is approximately 77 minutes before warm-up and process overhead.

## Metrics

### Performance

| Metric | Interpretation |
| --- | --- |
| `QPS` | Completed fan-in operations per second |
| `P50`, `P95`, `P99` | Complete fan-in operation latency |
| `FIRST_P50` | Time to the first child response |

### Application Transfer

| Metric | Interpretation |
| --- | --- |
| `potential_bytes_per_operation` | Complete results available across all children |
| `received_bytes_per_operation` | Payload returned by parent `RecvMsg()` calls |
| `received_units_per_operation` | Units returned by parent `RecvMsg()` calls |
| `emitted_units_per_operation` | Units emitted by ordered topK |
| `unused_units_per_operation` | Received Units left unused when topK completes |
| `saved_percent` | Potential bytes not returned to the parent application |

### Required Per-Child Counters

For each child `i`, record:

```text
received_messages[i]
received_bytes[i]
send_attempted_messages[i]
send_attempted_bytes[i]
send_completed_messages[i]
send_completed_bytes[i]
```

`received_*` measures what the parent application consumed before topK
completion. The child increments `send_attempted_*` immediately before handing
a response to gRPC. Streaming increments `send_completed_*` after `SendMsg()`
returns successfully; Unary increments it on gRPC's server-side `OutPayload`
event. `send_completed_* - received_*` estimates gRPC send-ahead, but is not
exact TCP wire waste. Cancellation races also mean it is diagnostic rather than
a strict invariant.

Each mode resets child counters after warm-up and snapshots them after all
measured RPCs become inactive. Reset and snapshot control RPCs execute outside
the measured interval.

Exact transport wire bytes are outside this phase unless a separate minimally
instrumented gRPC-stats run is approved.

## Comparisons

Compare Unary and Streaming only within the same matrix cell and repetition.
Aggregate all five order-balanced repetitions before drawing conclusions.

Report:

```text
Streaming QPS / Unary QPS
Streaming latency / Unary latency
Streaming received bytes / potential bytes
Streaming response messages per operation
per-child received and send-completed message distributions
```

Define the observed break-even point as the smallest `saved_percent` for which:

```text
Streaming P50 <= Unary P50
Streaming QPS >= Unary QPS
```

This is an empirical result for the tested topology, not a universal threshold.

## Acceptance Criteria

- Every execution exits successfully with zero request errors.
- Unary and Streaming emit the same ordered result hash and Unit count.
- Ordered-topK Streaming never receives more than `N * P` application bytes.
- Full-transfer controls receive exactly `N * P` application bytes in both modes.
- Every mode completes at least 200 operations and runs for at least 20 seconds.
- All five repetitions and their generated inputs remain available for review.
- Conclusions distinguish parent-received bytes, child send completion, and
  actual transport bytes.

## Running

From the repository root:

```bash
./result/test_0826_01/run.sh
```

Expected launcher output:

```text
started run <UTC-run-id>
pid: <process-id>
artifacts: <repository>/result/test_0826_01/runs/<UTC-run-id>
```

Follow progress with:

```bash
tail -f result/test_0826_01/runs/<UTC-run-id>/runner.log
```
