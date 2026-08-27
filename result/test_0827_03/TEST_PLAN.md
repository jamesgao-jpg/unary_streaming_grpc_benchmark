# Equal-TopK Factor and Flow-Control Benchmark

Status: Running (Phase A)

## Objective

Measure how fan-in, final topK payload, Streaming Chunk size, and the parent
gRPC receive window affect the Unary-versus-Streaming behavior observed in
`test_0827_02`.

Every case preserves the application invariant:

```text
payload per child = final global topK payload
global topK = payload per child / 256-byte Unit
```

Unary receives one complete topK payload from every child before reduction.
Streaming receives ordered Chunks and stops after the parent emits the same
global topK. Unary and Streaming must produce the same output count and hash.

## Questions

1. How do fan-in, topK, and Chunk size change Streaming/Unary QPS?
2. How much data reaches the parent application beyond final topK demand?
3. How much transport data runs ahead of parent `RecvMsg()` consumption?
4. Does a static parent receive window control that run-ahead?
5. Do these effects interact, rather than behave as independent main effects?

## Fixed Setup

| Parameter | Value |
| --- | ---: |
| Workflow | `ordered_topk` |
| Unit size | 256 B |
| Result distribution | `interleaved` |
| Logical gRPC channels | 1 |
| Concurrency | 1 |
| Warm-up requests per mode | 3 |
| Minimum measured requests per mode | 10 |
| Minimum measurement duration per mode | 1 second |
| Repetitions per case | 4 |

Repetitions 1 and 3 run Unary first. Repetitions 2 and 4 run Streaming
first. Every repetition starts a fresh benchmark process while reusing
persistent connections inside that process.

## Factors

| Factor | Values |
| --- | --- |
| Children | 1, 2, 4, 8, 16, 32 |
| Final topK payload per child | 1, 4, 16, 32, 60 MiB |
| Streaming Chunk | 16, 64, 256 KiB; 1, 4 MiB |
| Parent receive window | dynamic; 64 KiB, 256 KiB, 1 MiB, 4 MiB, 16 MiB static |

The 60 MiB child response remains below the configured 64 MiB parent receive
message limit after protobuf framing.

`static_window_bytes: 0` preserves gRPC-Go's dynamic flow control. A positive
value applies the same static stream and connection receive window on the
parent. gRPC-Go documents that these static options disable dynamic flow
control and use 64 KiB as the minimum effective value:

- [Static stream window option](https://github.com/grpc/grpc-go/blob/v1.80.0/dialoptions.go#L210-L227)
- [Static connection window option](https://github.com/grpc/grpc-go/blob/v1.80.0/dialoptions.go#L229-L246)

## Phase A: Main Effects

Baseline:

```text
children = 8
topK payload = 16 MiB
Chunk = 256 KiB
parent receive window = dynamic
```

Change one factor at a time:

| Sweep | Cases |
| --- | ---: |
| Children: 1, 2, 4, 8, 16, 32 | 6 |
| TopK payload: 1, 4, 32, 60 MiB | 4 |
| Chunk: 16, 64 KiB; 1, 4 MiB | 4 |
| Static window: 64, 256 KiB; 1, 4, 16 MiB | 5 |
| Total unique cases | 19 |

The baseline is included once in the children sweep.

## Phase B: Dynamic-Window Interactions

Run the complete matrix with dynamic flow control:

```text
children: 2, 8, 32
topK payload: 4, 16, 60 MiB
Chunk: 64 KiB, 256 KiB, 1 MiB
parent receive window: dynamic
```

This phase contains 27 cases.

## Phase C: Static-Window Interactions

Use static windows of 64 KiB, 1 MiB, and 16 MiB in these slices:

| Slice | Fixed values | Varied values | Unique cases |
| --- | --- | --- | ---: |
| Fan-in x window | topK 16 MiB, Chunk 256 KiB | children 2, 8, 32 | 9 |
| TopK x window | children 8, Chunk 256 KiB | topK 4, 60 MiB | 6 |
| Chunk x window | children 8, topK 16 MiB | Chunk 64 KiB, 1 MiB | 6 |
| Total | | | 21 |

The common baseline combinations are not repeated between slices.

## Recorded Metrics

For each mode and child, retain the existing metrics:

- QPS, p50/p95/p99 latency, and time to first response.
- Parent application messages and payload bytes received.
- Parent protobuf and gRPC wire bytes delivered to `RecvMsg()`.
- Child attempted and completed send messages and payload bytes.
- Child and parent `net.Conn` read/write bytes.
- Linux TCP sent, acknowledged, received, and not-sent bytes.
- Received, emitted, and unused Units.
- Unary and Streaming output count and correctness hash.

## Derived Comparisons

Normalize cumulative counters by successful Streaming operations before
calculating:

```text
application overhead = parent application received bytes - final topK bytes
transport run-ahead = parent connection read bytes - parent application received bytes
gRPC send queue gap = child completed-send bytes - child connection-write bytes
transport efficiency = final topK bytes / parent connection read bytes
run-ahead Chunks = transport run-ahead / Streaming Chunk bytes
QPS ratio = Streaming QPS / Unary QPS
```

Report medians separately for Unary-first and Streaming-first repetitions,
then report the combined median. Do not interpret cumulative byte counters as
simultaneously retained memory.

## Procedure

1. Run each phase independently on the Linux benchmark server.
2. Record source commit, source status, diff, environment, test plan, and runner.
3. Run `go test -race -count=1 ./...` and build one immutable binary per phase.
4. Generate one YAML configuration for every case and repetition.
5. Run every case four times with balanced mode order.
6. Validate output count, hash, per-child metrics, and TCP counter availability.
7. Preserve raw logs, generated YAML, manifest, and completion marker.
8. Review Phase A before interpreting the larger interaction phases.

Run a phase with:

```bash
./result/test_0827_03/run.sh phase_a
./result/test_0827_03/run.sh phase_b
./result/test_0827_03/run.sh phase_c
```

Each command starts a detached run and prints its PID and artifact directory.

## Acceptance Criteria

- Every execution completes without correctness or request errors.
- Unary and Streaming emit exactly the configured global topK and matching hashes.
- Unary receives exactly `children * payload per child` application bytes.
- Every child emits application, gRPC, transport, and Linux TCP metrics per mode.
- The log records the configured dynamic or static parent receive window.
- Static-window results are compared with the matching dynamic-window cases.
- Mode-order effects are reported before repetitions are combined.
- Raw evidence and exact source state are retained before completion.

## Boundary

This test identifies cumulative application and transport run-ahead. It does
not locate instantaneous resident bytes inside gRPC queues, HTTP/2 buffers, or
kernel socket buffers. Exact memory residency requires a separate synchronized
snapshot experiment.
