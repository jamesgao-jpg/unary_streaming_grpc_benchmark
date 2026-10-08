# Stage 2 ReduceStream Failure Qualification

## Status

Draft. Do not run until the benchmark-only fault controls and this runner are
committed, synchronized to `10.15.9.42`, and the remote Milvus binary is
rebuilt from that commit.

Tracking issue:
[unary_streaming_grpc_benchmark#13](https://github.com/jamesgao-jpg/unary_streaming_grpc_benchmark/issues/13).

## Objective

Qualify the externally visible behavior of the Milvus Streaming Reduce path
when its QueryNode gRPC stream fails before or after the first final Chunk.
The experiment must determine:

1. whether Proxy retries the complete request;
2. whether the SDK receives a complete success, an incomplete success, or an
   error;
3. whether cancellation reaches the QueryNode stream handler; and
4. whether the Milvus processes recover for the next request.

This is a correctness and failure-semantics experiment, not a latency test.

## Scope

### Included

- Plain ANN Search iterator with `topK=8192`.
- Bounded Plain Query with `limit=8192`.
- One QueryNode selected as the only WorkNode for one vchannel.
- One-shot handler error, clean EOF, and blocking checkpoints.
- Client deadline, QueryNode `SIGKILL`, QueryNode `SIGTERM`, and TCP reset.
- Failure checkpoints before and after the first child Chunk.

### Deferred

- StreamingNode failure. The preserved Cohere 1M collection contains sealed
  data only, and accepted topology evidence records
  `streamingNodePresent=false`. A StreamingNode case requires a separate
  growing-data topology.
- Multi-child pending-`Recv()` cleanup. Stage 1 already covers that boundary
  directly; N1 is intentional here so failure timing is unambiguous.
- Stage 3 race-instrumented Milvus processes. Run it only if this experiment
  exposes an unresolved cleanup or concurrency problem.

## Source Under Test

| Purpose | Repository and branch |
| --- | --- |
| Milvus | `jamesgao-jpg/milvus:codex/qv-reducestream-e2e-benchmark-20260915` |
| Experiment | `jamesgao-jpg/unary_streaming_grpc_benchmark:master` |

The runner records exact commits, Git statuses, diffs, and the Milvus build
log. A formal run requires local and remote Milvus source files to match the
same commit. Unrelated local changes under `docs/` are recorded but do not
change the binary and therefore do not block the run.

Implementation references:

- Search and Query stream handlers and Chunk splitting:
  [`internal/views/viewquery/server.go`](https://github.com/jamesgao-jpg/milvus/blob/codex/qv-reducestream-e2e-benchmark-20260915/internal/views/viewquery/server.go#L148-L308).
- Search complete-request retry before the first final Chunk:
  [`internal/views/queryclient/legacy_client.go`](https://github.com/jamesgao-jpg/milvus/blob/codex/qv-reducestream-e2e-benchmark-20260915/internal/views/queryclient/legacy_client.go#L205-L278).
- Query complete-request retry before the first final Chunk:
  [`internal/views/queryclient/legacy_client.go`](https://github.com/jamesgao-jpg/milvus/blob/codex/qv-reducestream-e2e-benchmark-20260915/internal/views/queryclient/legacy_client.go#L326-L385).

## Machines And Topology

| Role | Machine | Processes |
| --- | --- | --- |
| Milvus server | `10.15.9.42` | MixCoord, DataNode, StreamingNode, QueryNode, Proxy; etcd, Pulsar, and MinIO in containers |
| PyMilvus client and test control | `10.15.2.233` | One request process at a time |

```text
PyMilvus (.233)
    |
    | SDK request
    v
Proxy (.42)
    |
    | request-scoped bidirectional gRPC stream
    v
QueryNode (.42, one selected WorkNode)
```

Proxy and QueryNode are separate processes even though they run on the same
machine. Their communication uses the normal Milvus gRPC path.

## Fixed Workload

| Setting | Value |
| --- | --- |
| Collection | `cohere_1m_qn_fanin` |
| Rows | 1,000,000 Cohere vectors |
| Vector dimension | 768 |
| Vchannels | 1 |
| Sealed segments | 63 |
| QueryNodes | 1 |
| Search | iterator, `topK=8192`, `ef=8192`, `ignore_growing=true` |
| Query | `pk >= 0`, `limit=8192`, output `pk, vector` |
| Search Chunk threshold | 16 KiB reducible payload |
| Query Chunk threshold | 16 KiB reducible payload |
| Concurrency | 1 |

The small Chunk threshold ensures that a successful request needs multiple
child Chunks. Result count and a deterministic hash are captured for every
successful request.

## Fault Control

The QueryNode process accepts one benchmark-only environment value:
`MILVUS_REDUCE_STREAM_FAULT`.

| Value | Behavior |
| --- | --- |
| `error_before_first_chunk` | Return `Unavailable` before the first child Chunk. |
| `error_after_first_chunk` | Send one child Chunk, then return `Unavailable`. |
| `eof_after_first_chunk` | Send one child Chunk, then return clean EOF. |
| `block_before_first_chunk` | Block before the first child Chunk until cancellation. |
| `block_after_first_chunk` | Send one child Chunk, then block until cancellation. |

The fault is consumed once per QueryNode process. Every handler entry while the
setting is active logs `ReduceStream fault request accepted` with operation,
request ID, fault, and `injected=true|false`. The injected checkpoint logs
`ReduceStream fault checkpoint reached`.

Each case starts a fresh QueryNode so its one-shot state cannot leak into the
next case.

## Cases

Run every case once for `Search` and once for `Query`.

| Case | Fault | External action | Expected SDK result | Retry evidence |
| --- | --- | --- | --- | --- |
| `BASELINE` | none | none | complete success; 8,192 Units | not applicable |
| `ERROR-BEFORE` | error before first Chunk | none | complete success equal to baseline | one `injected=true` attempt followed by an `injected=false` attempt |
| `ERROR-AFTER` | error after first Chunk | none | error; no partial success | one accepted attempt |
| `EOF-AFTER` | EOF after first Chunk | none | record success/error, count, and hash | one accepted attempt |
| `DEADLINE-BEFORE` | block before first Chunk | client deadline | deadline or cancellation error | checkpoint reached and handler exits |
| `DEADLINE-AFTER` | block after first Chunk | client deadline | deadline or cancellation error; no partial success | checkpoint reached and handler exits |
| `SIGKILL-BEFORE` | block before first Chunk | `SIGKILL` QueryNode | error; no partial success | record Proxy attempts and process exit |
| `SIGKILL-AFTER` | block after first Chunk | `SIGKILL` QueryNode | error; no partial success | no successful complete-request retry after output begins |
| `SIGTERM-BEFORE` | block before first Chunk | `SIGTERM` QueryNode | error; no partial success | record Proxy attempts and process exit |
| `SIGTERM-AFTER` | block after first Chunk | `SIGTERM` QueryNode | error; no partial success | no successful complete-request retry after output begins |
| `TCPRESET-BEFORE` | block before first Chunk | reset QueryNode TCP connection | complete success equal to baseline | faulted stream followed by a new handler attempt |
| `TCPRESET-AFTER` | block after first Chunk | reset QueryNode TCP connection | error; no partial success | one accepted request; no complete-request retry |

`EOF-AFTER` is an observation case. Clean EOF is the gRPC signal for
authoritative stream completion, so the receiver cannot infer that the server
silently omitted a suffix. The report must record the returned data without
misclassifying this as a transport-detected failure.

## Procedure

1. Verify local and `.42` Milvus branches, commits, and clean statuses.
2. Verify `.42` has no active Milvus process or unrelated container.
3. Verify `.233` has PyMilvus and no active benchmark process.
4. Build Milvus once on `.42`; do not run `make clean`.
5. Start the accepted etcd, Pulsar, and MinIO volumes and the native Milvus
   roles.
6. Load the preserved Cohere 1M collection with one QueryNode and verify the N1
   placement and WorkNode selection.
7. Run the Search and Query baselines and retain their count and hash.
8. For each fault case, restart one QueryNode with the selected fault, wait for
   collection load, then execute exactly one request.
9. For blocking cases, wait for the exact checkpoint log before applying the
   deadline, signal, or TCP reset.
10. Save client JSON, client log, QueryNode log, Proxy log, accepted-attempt
    count, checkpoint count, socket snapshots, and process state.
11. Restart a fault-free QueryNode and run one final Search and Query recovery
    request.
12. Build `manifest.tsv` and `summary.json`; preserve all raw evidence under
    `runs/<UTC-run-id>/`.

## Required Assertions

- Baseline and final recovery requests return exactly 8,192 Units with the
  same operation-specific hash.
- `ERROR-BEFORE` and `TCPRESET-BEFORE` return the complete baseline result and
  show a later handler attempt after the injected attempt.
- Every after-first-Chunk error, deadline, termination, or reset returns an SDK
  error rather than partial SDK success.
- Deadline cases reach the selected checkpoint before the client deadline.
- Signal cases record that the QueryNode process exited.
- Every case is followed by a successful health and collection-load check.
- No run claims StreamingNode coverage.

## Evidence Layout

```text
runs/<UTC-run-id>/
  TEST_PLAN.md
  run_test.sh
  run.log
  local-milvus-status.txt
  remote-milvus-status.txt
  remote-build.log
  topology/
  Search/<case>/
    client.json
    client.log
    querynode.log
    proxy.log
    attempts.txt
    sockets-before.txt
    sockets-after.txt
    process-state.txt
  Query/<case>/
    ...
  manifest.tsv
  summary.json
  final-recovery/
```

## Exit Criteria

The experiment is complete when all non-observation assertions pass, raw
evidence is retained, `EOF-AFTER` behavior is described without assuming a
missing protocol signal, and `report.md` distinguishes verified behavior from
remaining StreamingNode and multi-child questions.
