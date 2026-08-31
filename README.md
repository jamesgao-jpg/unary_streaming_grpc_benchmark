# Unary versus Streaming gRPC Benchmark

This Go demo compares one Unary response with a sequence of bidirectional
Streaming responses. It supports both equal-byte transport measurement and a
synthetic ordered-topK workflow that stops child streams after enough results
have been merged. It does not import Milvus or execute ANN search.

## Run

```bash
go run . --config config.yaml
```

The main process reads the YAML, starts the configured child server processes,
creates one resolver-backed gRPC channel for all children, verifies both RPC
modes, and runs Unary followed by Streaming. A deterministic picker routes
each request stream to its intended child. Child startup and channel
establishment are outside the measured interval.

## Process Model

`benchmark.child_processes` and `benchmark.concurrency` control different
layers of parallelism:

| Setting | Implementation | Meaning |
| --- | --- | --- |
| `child_processes: N` | N operating-system child processes | Simulated QN/SN servers. Each process listens on `base_port + child_index`. |
| `concurrency: K` | K parent worker goroutines | At most K complete fan-in operations execute concurrently. |

Each fan-in operation also starts short-lived goroutines to call all N child
processes concurrently. The resulting upper bound is approximately `N * K`
active child RPCs or streams. `concurrency` is therefore a request-level load
setting, not the total number of goroutines in the program. gRPC itself also
uses internal goroutines in the parent and child processes.

## Command-Line Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--config PATH` | `config.yaml` | YAML configuration used by the parent and all child processes. |
| `--child` | `false` | Runs the executable as one child gRPC server instead of as the benchmark parent. The parent sets this internally. |
| `--child-index N` | `0` | Identifies the child and its deterministic payload. Used with `--child`; normally set internally. |
| `--listen ADDRESS` | empty | Child gRPC listen address. Required with `--child`; normally set internally. |

Normal benchmark runs only need `--config`. The parent starts child processes
using the other three flags.

Example output:

```text
workflow=ordered_topk children=2 logical_channels=1 total_payload_bytes_per_child=4194304 stream_chunk_bytes=262144 concurrency=1 mode_order=unary_first static_window_bytes=0 per_unit_bytes=256 global_topk=4096 result_distribution=interleaved
MODE        SUCCESS    ERROR        QPS        MiB/S          P50          P95          P99      FIRST_P50
unary           100        0      ...          ...          ...          ...          ...              ...
transfer mode=unary potential_bytes_per_operation=... received_bytes_per_operation=... saved_percent=...
streaming       100        0      ...          ...          ...          ...          ...              ...
transfer mode=streaming potential_bytes_per_operation=... received_bytes_per_operation=... saved_percent=...
```

`QPS` counts complete fan-in operations. `MiB/S` uses the logical payload bytes
received by the parent. `FIRST_P50` measures the first response message from
any child. The `transfer` line separates potential child bytes from bytes and
messages actually received.

## Workflows

`full_transfer` preserves the original transport-only benchmark. Every child
returns `total_payload_bytes_per_child` in both RPC modes; Streaming only splits
that payload into `stream_chunk_bytes` messages.

`ordered_topk` encodes fixed-size Units containing a rank, ID, and padding.
Unary receives every child's complete result before merging. Streaming receives
one Chunk from every child, repeatedly selects the lowest-ranked head Unit, and
refills only the Buffer that supplied the selected Unit. After `global_topk`
Units are emitted, the parent cancels the remaining child streams.

`result_distribution: interleaved` spreads the best Units across children.
`dominant_child` places the best Units in child zero. Comparing them shows how
the per-child contribution pattern changes the number of required Chunks.

## YAML Reference

### gRPC

| Field | Meaning |
| --- | --- |
| `grpc.compression_enabled` | Must remain `false`; the benchmark does not enable gRPC compression. |
| `grpc.internal_tls_enabled` | Must remain `false`; the benchmark uses insecure transport credentials. |

### gRPC Client

These settings apply to the parent-side channel.

| Field | Meaning |
| --- | --- |
| `grpc.client.max_send_bytes` | Maximum protobuf request-message size in bytes. |
| `grpc.client.max_receive_bytes` | Maximum protobuf response-message size in bytes. |
| `grpc.client.static_window_bytes` | `0` uses dynamic flow control. A value of at least 65,536 sets both static stream and connection receive windows. |
| `grpc.client.dial_timeout_ms` | Minimum connection-attempt timeout used by gRPC connection backoff. |
| `grpc.client.keepalive_time_ms` | Idle time before the client sends a keepalive ping. |
| `grpc.client.keepalive_timeout_ms` | Time to wait for a keepalive response. |
| `grpc.client.permit_without_stream` | Allows keepalive pings when no RPC is active. |
| `grpc.client.backoff_base_delay_ms` | Initial reconnection backoff delay. |
| `grpc.client.backoff_multiplier` | Multiplier applied to successive reconnection delays. |
| `grpc.client.backoff_jitter` | Random variation applied to reconnection delays. |
| `grpc.client.backoff_max_delay_ms` | Maximum reconnection backoff delay. |

### gRPC Server

These settings apply independently to every child process.

| Field | Meaning |
| --- | --- |
| `grpc.server.max_send_bytes` | Maximum protobuf response-message size in bytes. |
| `grpc.server.max_receive_bytes` | Maximum protobuf request-message size in bytes. |
| `grpc.server.keepalive_time_ms` | Idle time before the server sends a keepalive ping. |
| `grpc.server.keepalive_timeout_ms` | Time to wait for a keepalive response. |
| `grpc.server.minimum_ping_interval_ms` | Minimum accepted interval between client pings. |
| `grpc.server.permit_without_stream` | Allows client pings when no RPC is active. |
| `grpc.server.graceful_stop_timeout_ms` | Time allowed for child shutdown before the parent force-stops it. |

### Benchmark

| Field | Meaning |
| --- | --- |
| `benchmark.child_processes` | Number of child OS processes and child RPCs per fan-in operation. |
| `benchmark.host` | Host used for child listen and resolver addresses. |
| `benchmark.base_port` | First child port; child `i` listens on `base_port + i`. |
| `benchmark.workflow` | `full_transfer` or `ordered_topk`. Defaults to `full_transfer` when omitted. |
| `benchmark.total_payload_bytes_per_child` | Complete payload available from each child. Unary always transfers it; `full_transfer` Streaming also transfers it completely. |
| `benchmark.stream_chunk_bytes` | Maximum payload bytes in each Streaming response message. |
| `benchmark.per_unit_bytes` | Fixed Unit size for `ordered_topk`; must be at least 16 bytes. |
| `benchmark.global_topk` | Number of Units emitted by `ordered_topk`; ignored by `full_transfer`. |
| `benchmark.result_distribution` | `interleaved` or `dominant_child` for `ordered_topk`; defaults to `interleaved`. |
| `benchmark.concurrency` | Number of concurrent fan-in operations managed by parent worker goroutines. |
| `benchmark.warmup_requests` | Sequential warm-up operations run before each measured mode. |
| `benchmark.measured_requests` | Minimum measured operations per mode; the duration requirement may increase the total. |
| `benchmark.minimum_measurement_duration_ms` | Minimum measurement duration per mode. |
| `benchmark.mode_order` | `unary_first` or `streaming_first`. |
| `benchmark.request_timeout_ms` | Timeout applied to each fan-in operation and counter-control request. |
| `benchmark.startup_timeout_ms` | Maximum time allowed to establish the parent channel to the children. |

For `ordered_topk`, payload and Chunk sizes must be divisible by
`per_unit_bytes`. The configured global topK cannot exceed the total Units
available across all children.

## Configuration Boundary

The checked-in YAML mirrors the current Milvus QueryNode-to-Proxy transport
settings relevant to this experiment:

- protobuf over gRPC-Go `v1.80.0`;
- no compression;
- no internal TLS;
- Proxy-side 64 MiB receive limit;
- QueryNode-side 512 MiB send limit;
- client keepalive of 10 seconds with a 20-second timeout;
- server keepalive of 60 seconds with a 10-second timeout; and
- a minimum client ping interval of 5 seconds.

Milvus service discovery, production balancing, retries, tracing, logging, and
cluster interceptors are intentionally excluded. The benchmark uses a manual
resolver and deterministic picker to route each RPC to its intended child over
one shared logical channel.

`total_payload_bytes_per_child` fixes each child's complete potential result.
It is fully transferred by both modes under `full_transfer` and by Unary under
`ordered_topk`. `stream_chunk_bytes` controls only the payload bytes in each
Streaming response message. Protobuf and gRPC framing make actual wire bytes
larger, especially for Streaming.

The complete Unary protobuf response and every individual Streaming protobuf
response must fit within the smaller of `grpc.server.max_send_bytes` and
`grpc.client.max_receive_bytes`. Invalid configurations are rejected before
any child process starts.

`grpc.client.static_window_bytes: 0` keeps gRPC-Go's default dynamic flow
control. A value of at least 65,536 applies the same static receive window to
each parent-side stream and connection and disables dynamic window sizing.

The defaults were verified against the sibling Milvus checkout at these source
locations:

- `../milvus-qv/configs/milvus.yaml:451-455` for Proxy gRPC limits;
- `../milvus-qv/configs/milvus.yaml:683-687` for QueryNode gRPC limits;
- `../milvus-qv/configs/milvus.yaml:1013-1025` for client compression,
  keepalive, and connection settings;
- `../milvus-qv/configs/milvus.yaml:1117-1118` for internal TLS; and
- `../milvus-qv/internal/distributed/querynode/service.go:224-242` for server
  keepalive and message-limit wiring.
