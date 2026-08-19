# Unary versus Streaming gRPC Benchmark

This Go demo compares one unary response with a sequence of bidirectional
streaming responses for the same logical payload bytes. It intentionally does
not include Milvus `ReduceStream`, ANN search, or reduction behavior.

## Run

```bash
go run . --config config.yaml
```

The main process reads the YAML, starts the configured child server processes,
creates one resolver-backed gRPC channel for all children, verifies both
workflows, and runs Unary followed by Streaming. A deterministic picker routes
each request stream to its intended child. Child startup and channel
establishment are outside the measured interval.

Example output:

```text
children=2 logical_channels=1 total_payload_bytes_per_child=4194304 stream_chunk_bytes=262144 concurrency=1
MODE        SUCCESS    ERROR        QPS        MiB/S          P50          P95          P99      FIRST_P50
unary           100        0      ...          ...          ...          ...          ...              ...
streaming       100        0      ...          ...          ...          ...          ...              ...
```

`QPS` counts complete fan-in operations. One operation transfers
`child_processes * total_payload_bytes_per_child` logical bytes. `MiB/S` uses
those logical bytes. `FIRST_P50` measures the first response message received
from any child; Unary has one response message and Streaming has multiple.

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

Milvus service discovery, balancing, retries, tracing, logging, and cluster
interceptors are intentionally excluded. The benchmark uses direct child
addresses so only the unary-versus-streaming transfer behavior changes.

`total_payload_bytes_per_child` fixes the logical data transferred by each
child in both workflows. `stream_chunk_bytes` controls only the payload bytes
in each Streaming response message. Protobuf and gRPC framing make actual wire
bytes larger, especially for Streaming.

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
