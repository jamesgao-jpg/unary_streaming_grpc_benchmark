# Unary versus Streaming gRPC Test 0820-01

## Objective

Measure the QPS and latency difference between Unary and bidirectional
Streaming gRPC while both modes transfer the same logical payload bytes.
`ReduceStream`, ANN search, and reduction behavior are outside this test.

## Topology

```text
Benchmark process
    |
    | one logical gRPC channel
    |
    +-- child process 0
    +-- child process 1
    +-- ...
    +-- child process N-1
```

The manual resolver publishes every child address through one `ClientConn`.
One fan-in operation starts one Unary RPC or one Streaming stream per child.
Streaming Chunks are response messages within a stream, not separate streams.

## Parameters

| Symbol | YAML field | Meaning |
| --- | --- | --- |
| `N` | `child_processes` | Child processes and RPCs or streams per fan-in operation |
| `P` | `total_payload_bytes_per_child` | Logical payload bytes returned by each child |
| `C` | `stream_chunk_bytes` | Maximum payload bytes in one Streaming response message |
| `K` | `concurrency` | Concurrent fan-in operations |

Derived values:

```text
Logical bytes per operation     = N * P
RPCs or streams per operation   = N
Streaming messages per operation = N * ceil(P / C)
Maximum concurrent RPCs         = N * K
```

## Matrix

### Chunk Message Count

Hold `P=16 MiB`, `N=8`, and `K=1`.

| Case | `C` | Messages per child | Messages per operation |
| --- | ---: | ---: | ---: |
| `CHUNK-001` | 16 KiB | 1,024 | 8,192 |
| `CHUNK-002` | 64 KiB | 256 | 2,048 |
| `CHUNK-003` | 256 KiB | 64 | 512 |
| `CHUNK-004` | 1 MiB | 16 | 128 |
| `CHUNK-005` | 4 MiB | 4 | 32 |
| `CHUNK-006` | 16 MiB | 1 | 8 |

`CHUNK-006` compares one Unary response with one Streaming response per child.

### Shared-Channel Fan-In

Hold logical payload at `32 MiB` per operation, `C=256 KiB`, and `K=1`.

| Case | `N` | `P` per child | RPCs per operation | Streaming messages per operation |
| --- | ---: | ---: | ---: | ---: |
| `FANIN-001` | 1 | 32 MiB | 1 | 128 |
| `FANIN-002` | 2 | 16 MiB | 2 | 128 |
| `FANIN-003` | 4 | 8 MiB | 4 | 128 |
| `FANIN-004` | 8 | 4 MiB | 8 | 128 |
| `FANIN-005` | 16 | 2 MiB | 16 | 128 |
| `FANIN-006` | 32 | 1 MiB | 32 | 128 |

This group changes fan-in while holding logical bytes and total Streaming
message count constant.

### Payload Scale

Hold `N=8`, `C=256 KiB`, and `K=1`.

| Case | `P` per child | Logical bytes per operation | Streaming messages per operation |
| --- | ---: | ---: | ---: |
| `PAYLOAD-001` | 256 KiB | 2 MiB | 8 |
| `PAYLOAD-002` | 1 MiB | 8 MiB | 32 |
| `PAYLOAD-003` | 4 MiB | 32 MiB | 128 |
| `PAYLOAD-004` | 16 MiB | 128 MiB | 512 |
| `PAYLOAD-005` | 32 MiB | 256 MiB | 1,024 |

### Concurrent Streams

Hold `N=8`, `P=1 MiB`, and `C=256 KiB`.

| Case | `K` | Maximum concurrent RPCs | Logical bytes in flight |
| --- | ---: | ---: | ---: |
| `CONCURRENCY-001` | 1 | 8 | 8 MiB |
| `CONCURRENCY-002` | 4 | 32 | 32 MiB |
| `CONCURRENCY-003` | 16 | 128 | 128 MiB |
| `CONCURRENCY-004` | 32 | 256 | 256 MiB |
| `CONCURRENCY-005` | 64 | 512 | 512 MiB |

Run `CONCURRENCY-005` only after the preceding concurrency cases complete
without RPC errors or host resource exhaustion.

### Message-Limit Boundary

| Case | `N` | `P` | `C` | Expected result |
| --- | ---: | ---: | ---: | --- |
| `BOUNDARY-VALID` | 1 | 60 MiB | 256 KiB | Both modes complete |
| `BOUNDARY-INVALID` | 1 | 64 MiB | 256 KiB | Configuration is rejected |

The invalid case includes protobuf overhead above the configured 64 MiB client
receive limit. It runs once and is not included in performance comparisons.

## Execution Controls

- Run five repetitions of every valid case.
- Odd repetitions run Unary before Streaming.
- Even repetitions run Streaming before Unary.
- Warm up each mode with 20 fan-in operations.
- Measure each mode for at least 20 seconds and 200 completed operations.
- Reuse the same logical channel for both modes within one repetition.
- Start child processes and establish the channel outside measured intervals.
- Execute the matrix on an otherwise idle host.

The complete valid matrix contains 23 cases and 115 benchmark process runs.
Its minimum measured time is approximately 77 minutes before warm-up, process
startup, large-message overhead, and the expected-rejection check.

## Metrics

Record these existing benchmark outputs for every mode and repetition:

| Metric | Interpretation |
| --- | --- |
| `QPS` | Completed fan-in operations per second |
| `MiB/S` | Logical payload throughput across all children |
| `P50`, `P95`, `P99` | Complete fan-in operation latency |
| `FIRST_P50` | Earliest response message from any child |
| `SUCCESS`, `ERROR` | Completed and failed operations |

Compare Unary and Streaming only within the same matrix cell. `FIRST_P50`
cannot be compared directly across different child counts because it records
the earliest response among `N` children.

## Acceptance Criteria

- Every valid repetition exits successfully with zero reported errors.
- Unary and Streaming pass the pre-measurement payload verification.
- Every valid mode completes at least 200 operations and 20 seconds.
- `BOUNDARY-INVALID` exits nonzero with the configured message-limit error.
- Every generated configuration, raw log, and source revision is retained.
- Conclusions use all five repetitions and report Streaming-to-Unary ratios.

## Running

From the repository root:

```bash
./result/test_0820_01/run.sh
```

Expected launcher output:

```text
started run <UTC-run-id>
pid: <process-id>
artifacts: <repository>/result/test_0820_01/runs/<UTC-run-id>
```

Follow progress with:

```bash
tail -f result/test_0820_01/runs/<UTC-run-id>/runner.log
```

The run directory contains `COMPLETED` on success or `FAILED` on failure.

## Result Layout

```text
result/test_0820_01/runs/<UTC-run-id>/
    benchmark
    source_commit.txt
    source_status.txt
    source_diff.patch
    environment.txt
    manifest.tsv
    pid
    runner.log
    COMPLETED | FAILED
    configs/
    logs/
```

The current topology uses loopback TCP between processes on one host. Results
measure gRPC, protobuf, process scheduling, and memory-copy behavior without
physical network latency.
