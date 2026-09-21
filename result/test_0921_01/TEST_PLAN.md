# Small-Payload Stream-Creation Overhead Premise Test 0921-01

Status: Proposed.

## Objective

Test the premise behind Next-Stage Goal Task One: "frequent small Retrieve
workloads make stream-creation overhead significant." Before designing any
persistent or pooled stream lifecycle, measure whether creating one gRPC
stream per request is actually observable for small payloads, and whether a
small Retrieve that fits in one Chunk needs Streaming at all.

This test does not change any Milvus source. It uses the standalone
`full_transfer` workload, which has no reduction logic, so the measured
difference isolates the gRPC stream lifecycle (stream creation, `RecvMsg`
loop, stream teardown) from any `ReduceStream` application logic.

## Questions

1. For small per-child payloads (1-64 KiB), how much QPS and latency overhead
   does a per-request Streaming stream add relative to Unary?
2. Does the overhead change with payload size, child count, and concurrency?
3. Is the overhead measurable at the 1-Chunk-per-stream boundary, where
   Streaming transfers exactly the same logical bytes as Unary?
4. Does the measured overhead justify investigating persistent stream reuse,
   or is stream creation cheap enough that pooling would not pay?

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
All processes run on one host over loopback TCP. Connections are established
before measurement and reused within each benchmark process. This matches the
topology of the completed `test_0831_02` full-transfer factor benchmark, so
the results are directly comparable to the established baseline.

## Parameters

| Symbol | YAML field | Meaning |
| --- | --- | --- |
| `N` | `child_processes` | Child processes and RPCs or streams per operation |
| `P` | `total_payload_bytes_per_child` | Complete payload returned by each child |
| `C` | `stream_chunk_bytes` | Maximum payload in one Streaming response |
| `K` | `concurrency` | Concurrent fan-in operations |

`workflow` is always `full_transfer`, so `global_topk` and the ordered-topK
settings are not used. In every case the Streaming Chunk is set equal to the
payload (`C = P`), so each child emits exactly one Chunk; the only difference
between modes is the request-scoped stream lifecycle itself.

## Parameter Matrix

All cases run `full_transfer`, four repetitions each, with alternating
`mode_order` (Unary-first and Streaming-first) across repetitions.

| Case ID | Group | N | P (per child) | C (Chunk) | K |
| --- | --- | ---: | ---: | ---: | ---: |
| SP-P1K | payload | 1 | 1 KiB | 1 KiB | 1 |
| SP-P4K | payload | 1 | 4 KiB | 4 KiB | 1 |
| SP-P16K | payload | 1 | 16 KiB | 16 KiB | 1 |
| SP-P64K | payload | 1 | 64 KiB | 64 KiB | 1 |
| SP-N8-P1K | fanin | 8 | 1 KiB | 1 KiB | 1 |
| SP-N8-P4K | fanin | 8 | 4 KiB | 4 KiB | 1 |
| SP-N8-P16K | fanin | 8 | 16 KiB | 16 KiB | 1 |
| SP-N8-P64K | fanin | 8 | 64 KiB | 64 KiB | 1 |
| SP-N8-K8 | concurrency | 8 | 4 KiB | 4 KiB | 8 |
| SP-N8-K32 | concurrency | 8 | 4 KiB | 4 KiB | 32 |

Case count: 10 cases x 4 repetitions = 40 executions, each running both Unary
and Streaming within one benchmark process.

The concurrency cases use the smallest payload (`P=4 KiB`) because a 64 KiB
payload at K=32 would push logical in-flight bytes to 16 MiB per child and
confound the stream-creation question with HTTP/2 flow-control pressure.

## Execution Procedure

1. `go test -race -count=1 ./...` and `go build` in the benchmark repository.
2. For each case and repetition:
   - Generate a YAML config from the repository default with the case's
     parameters and the alternating `mode_order`.
   - Launch child processes, dial one shared channel, warm up, then measure
     Unary and Streaming for at least 20 seconds and at least 500 completed
     operations each.
   - Validate the log contains complete transfer metrics for both modes,
     correct message counts, zero errors, and TCP_INFO availability for every
     child.
3. Record each execution in the run manifest with exit code and outcome.

## Metrics

Per mode (Unary and Streaming), per repetition:

- QPS (complete fan-in operations per second)
- Throughput MiB/s
- p50, p95, p99 end-to-end latency
- FIRST_P50: time to first response message from any child
- Per-child received messages and bytes
- gRPC wire bytes (application + protobuf + framing)
- TCP_INFO sent/acked bytes where available

The primary comparison is the Streaming/Unary ratio of QPS, p50, and
FIRST_P50 within each case, balanced across both execution orders.

## Acceptance Criteria

A repetition passes when:

- Both modes report at least 500 successful operations and zero errors.
- Unary received bytes equal `N * P` per operation.
- Streaming received bytes equal `N * P` per operation (full transfer; no
  early stop in this workload).
- Streaming messages per operation equal `N` (one Chunk per child).
- The workflow identity line matches the case parameters.
- TCP_INFO is available for every child in both modes.

The test is complete when all 40 executions pass. The report then answers the
objective questions with the median of the four repetitions per case.

## Risks

- Loopback TCP on one host measures gRPC/protobuf/CPU behavior without
  physical network latency. The completed baseline benchmark has the same
  limitation; results are comparable to that baseline.
- FIRST_P50 for Unary is the complete response (single message), while
  Streaming's FIRST_P50 is the first of its Chunks. With one Chunk per stream
  these coincide, but the distinction is recorded in the report.
- If per-request stream creation proves cheap at these sizes, the premise
  fails and Task One should pivot to measuring connection-level or
  higher-level costs instead of stream pooling.
