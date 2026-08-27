# gRPC Streaming Run-Ahead Threshold Test

Status: Proposed

## Objective

Determine whether stopping parent `RecvMsg()` after ordered topK completion bounds
child and transport run-ahead as each child's complete result grows.

This test does not compare reduction algorithms. It uses the existing
`ordered_topk` workflow and focuses on the Streaming measurements. Unary still
runs because the current benchmark executes both modes, but it is secondary in
this test.

## Source Baseline

The 0826 formal run used source commit `a360a0c`. The current proposed baseline
is `4f4715d`.

| Commit | Change since the 0826 run | Measurement effect |
| --- | --- | --- |
| `1017267` | Preserved initial benchmark results | None |
| `bf93b6a` | Preserved the ordered-topK formal run | None |
| `440055c` | Added gRPC, connection, TCP, and application-retention counters | Supplies the transport evidence required by this test |
| `4f4715d` | Required source documentation | None |

The runner records the actual source commit, status, and diff for every run.

## Fixed Setup

| Parameter | Value |
| --- | ---: |
| Workflow | `ordered_topk` |
| Unit size | 256 B |
| Streaming Chunk size | 256 KiB |
| Global topK | 4,096 Units |
| Required output | 1 MiB |
| Distribution | `interleaved` |
| Concurrency | 1 |
| Warm-up requests per mode | 3 |
| Minimum measured requests per mode | 10 |
| Minimum measurement duration per mode | 1 second |
| Repetitions | 3 |
| Measured mode order | `streaming_first` |

Connections are persistent and warmed before counters are reset. This measures
steady-state run-ahead, including any flow-control window growth produced by
the warm-up.

## Matrix

| Case | Children | Payload per child | Chunks per child | Potential bytes per operation |
| --- | ---: | ---: | ---: | ---: |
| `RUNAHEAD-N2-P4` | 2 | 4 MiB | 16 | 8 MiB |
| `RUNAHEAD-N2-P16` | 2 | 16 MiB | 64 | 32 MiB |
| `RUNAHEAD-N2-P32` | 2 | 32 MiB | 128 | 64 MiB |
| `RUNAHEAD-N2-P60` | 2 | 60 MiB | 240 | 120 MiB |
| `RUNAHEAD-N8-P4` | 8 | 4 MiB | 16 | 32 MiB |
| `RUNAHEAD-N8-P16` | 8 | 16 MiB | 64 | 128 MiB |
| `RUNAHEAD-N8-P32` | 8 | 32 MiB | 128 | 256 MiB |
| `RUNAHEAD-N8-P60` | 8 | 60 MiB | 240 | 480 MiB |

The 60 MiB application payload remains below the configured 64 MiB client
receive limit after protobuf framing.

## Recorded Metrics

### Existing Application Metrics

- QPS, p50/p95/p99 latency, and time to first response.
- Parent `received_messages` and `received_bytes`, per child.
- Child `send_attempted_*` and `send_completed_*`, per child.
- Received, emitted, and unused Units.
- Application payload left unconsumed when topK completes.

### Metrics Added After the 0826 Run

- Parent gRPC `InPayload`: message count, application bytes, protobuf bytes,
  and gRPC wire bytes.
- Child and parent `net.Conn` read/write bytes.
- Linux `TCP_INFO` bytes received, sent, acknowledged, and not-sent bytes.
- TCP counter availability for both connection endpoints.

`net.Conn` counters include HTTP/2 framing and connection-control traffic.
`grpc_wire_bytes` includes gRPC framing but excludes HTTP/2 framing.
`tcp_not_sent_bytes` is an end-of-window gauge, not a cumulative counter.

## Derived Ratios

Calculate per operation and per child:

```text
parent application fraction = received_bytes / payload bytes available
child completion fraction = send_completed_bytes / payload bytes available
child connection-write fraction = child_connection_write_bytes / payload bytes available
child TCP-sent fraction = child_tcp_sent_bytes / payload bytes available
child TCP-acked fraction = child_tcp_acked_bytes / payload bytes available
parent connection-read fraction = parent_connection_read_bytes / payload bytes available
parent gRPC-delivered fraction = grpc application_payload_bytes / payload bytes available
```

Do not use `send_completed / send_attempted` as the run-ahead fraction. A final
failed send after cancellation makes that ratio approach 100% as Chunk count
increases even when the child sends only part of its complete result.

## Procedure

1. Run on the Linux benchmark server with the repository at the recorded HEAD.
2. Run `go test -race -count=1 ./...` and build one immutable benchmark binary.
3. Execute each matrix case three times in a fresh benchmark process.
4. Measure Streaming first in every process; treat Unary output as secondary.
5. Preserve generated YAML, raw logs, source state, environment, and manifest.
6. Normalize cumulative counters by successful operations before comparison.
7. Compare absolute bytes per operation and fractions across payload sizes.
8. Compare `N=2` with `N=8` to identify fan-in-dependent run-ahead.

## Interpretation

Evidence of bounded transport run-ahead requires parent application bytes to
remain approximately fixed while child completion, TCP sent/acked, and parent
connection-read bytes plateau or grow substantially slower than complete child
payload bytes.

If only parent application bytes plateau while child TCP bytes scale with the
complete payload, stopping `RecvMsg()` bounds application consumption but does
not bound network transfer.

The test is investigative; it must report the observed curve rather than assume
a fixed gRPC buffer threshold.

## Acceptance Criteria

- All cases complete without request or correctness errors.
- Every Streaming child emits exactly one `child_transfer`, `grpc_receive`, and
  `transport` record.
- Linux TCP counters are available on both child and parent endpoints.
- `send_completed_bytes <= send_attempted_bytes` for every child.
- Parent received bytes never exceed all child payload bytes available.
- Every conclusion uses bytes per successful operation, not raw cumulative bytes.
- No result is marked completed until raw logs and source evidence are retained.

## References

- [gRPC flow control](https://grpc.io/docs/guides/flow-control/)
- [gRPC-Go v1.80.0 transport defaults](https://github.com/grpc/grpc-go/blob/v1.80.0/internal/transport/defaults.go#L26-L46)
- [gRPC-Go write quota](https://github.com/grpc/grpc-go/blob/v1.80.0/internal/transport/flowcontrol.go#L28-L79)
- [gRPC-Go BDP estimator](https://github.com/grpc/grpc-go/blob/v1.80.0/internal/transport/bdp_estimator.go#L26-L42)
- [gRPC-Go application-read window updates](https://github.com/grpc/grpc-go/blob/v1.80.0/internal/transport/transport.go#L437-L473)
