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

Example output:

```text
workflow=ordered_topk children=2 logical_channels=1 total_payload_bytes_per_child=4194304 stream_chunk_bytes=262144 concurrency=1 mode_order=unary_first per_unit_bytes=256 global_topk=4096 result_distribution=interleaved
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

The defaults were verified against the sibling Milvus checkout at these source
locations:

- `../milvus-qv/configs/milvus.yaml:451-455` for Proxy gRPC limits;
- `../milvus-qv/configs/milvus.yaml:683-687` for QueryNode gRPC limits;
- `../milvus-qv/configs/milvus.yaml:1013-1025` for client compression,
  keepalive, and connection settings;
- `../milvus-qv/configs/milvus.yaml:1117-1118` for internal TLS; and
- `../milvus-qv/internal/distributed/querynode/service.go:224-242` for server
  keepalive and message-limit wiring.
