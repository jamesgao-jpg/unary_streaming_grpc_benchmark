# Bounded Plain Query Vector-Payload Batch/Streaming Benchmark

## Status

Proposed. The experiment has not been executed.

Tracking issue:
[unary_streaming_grpc_benchmark#8](https://github.com/jamesgao-jpg/unary_streaming_grpc_benchmark/issues/8).

## Objective

Compare Milvus Batch and Streaming execution for the same bounded ordinary
Plain Query when every returned row includes its 768-dimensional vector.

The existing ANN Search experiment returns only IDs and scores. Its child result
is approximately 80 KiB for topK 8,192. This Query workload returns about 24 MiB
of raw vector values before protobuf overhead:

```text
8,192 rows * 768 float32 values * 4 bytes = 25,165,824 bytes
```

The experiment asks whether this larger result payload changes the QPS and
latency relationship between Batch and Streaming observed for ANN Search.

## Experimental Interpretation

This is an N1 equal-payload experiment:

```text
Batch:
    QueryNode returns 8,192 rows in one unary response

Streaming:
    QueryNode returns the same 8,192 rows in eight 1,024-Unit Chunks
    Proxy consumes the Chunks through Query ReduceStream
```

Both modes return one final Query response to PyMilvus. With one QueryNode and a
requested limit equal to the child's result limit, Streaming does not avoid
transferring child rows. The comparison therefore measures internal gRPC
chunking, Query ReduceStream work, and Proxy materialization under the same
logical payload. It does not measure early-stop transfer savings.

## Environment

| Item | Value |
| --- | --- |
| Milvus server | `ubuntu@10.15.9.42` |
| PyMilvus client | `ubuntu@10.15.2.233` |
| Milvus checkout | `/home/ubuntu/milvus-qv` |
| Client checkout | `/home/ubuntu/reducestream_perf` |
| Milvus branch | `codex/qv-reducestream-e2e-benchmark-20260915` |
| Required Milvus commit | `9d3dd3019283df4cfe77a180e4433666f7faaa98` |
| Milvus build target | `make build-go` |
| Server binary | `/home/ubuntu/milvus-qv/bin/milvus` |

The runner must abort unless local, GitHub, and `.42` Milvus checkouts resolve
to the required commit and contain no unexplained source changes.

## Reused Dataset

Reuse the immutable collection qualified by `test_0915_01` run
`20260915T090613Z-20989`.

| Setting | Value |
| --- | --- |
| Collection | `cohere_1m_qn_fanin` |
| Dataset | Cohere 1M |
| Rows | 1,000,000 |
| Primary key field | `pk`, `INT64` |
| Auxiliary ID field | `id`, `INT64` |
| Vector field | `vector`, `FLOAT_VECTOR` |
| Dimension | 768 |
| Vchannels | 1 |
| Sealed segments | 63 |
| Growing segments | 0 |
| Replicas | 1 |

The collection's existing HNSW index is retained but is not exercised by this
scalar-filter Query workload. The experiment must not reinsert data, rebuild the
index, or create a replacement collection.

## Topology

| Component | Count |
| --- | ---: |
| Proxy | 1 |
| MixCoord | 1 |
| DataNode, including index service | 1 |
| StreamingNode | 1 |
| QueryNode | 1 |

The collection has one vchannel. The runner must verify that QueryView selects
the single QueryNode as the only QueryNode WorkNode and does not select the
StreamingNode for the sealed-only request.

## Fixed Query Workload

| Parameter | Value |
| --- | --- |
| SDK operation | ordinary `query()` |
| Iterator | disabled |
| Expression | `pk >= 0` |
| Limit | 8,192 |
| Offset | 0 |
| Output fields | `pk`, `vector` |
| Consistency | Strong |
| Streaming Chunk size | 1,024 Units |
| Client concurrency | 1 |
| Warm-up operations per interval | 5 |
| Minimum timed operations per interval | 30 |
| Minimum timed duration per interval | 60 seconds |
| Repetitions | 4 |

Each operation issues one bounded ordinary Query and consumes the complete SDK
response. The PyMilvus connection remains open throughout one interval and is
recreated after every Proxy restart.

The runner must perform a preflight request before timing and record the exact
logical field bytes represented by the returned IDs and float32 vectors. This
is not a protobuf or wire-byte measurement. The designed Chunk size remains
Unit-based; this experiment does not calibrate or tune Chunk size by serialized
bytes.

## A/B Modes

| Mode | Proxy configuration |
| --- | --- |
| Batch | `proxy.queryView.enableQueryStreaming=false` |
| Streaming | `proxy.queryView.enableQueryStreaming=true`; `proxy.queryView.queryStreamChunkSize=1024` |

Use the same Milvus binary in both modes. Restart only Proxy when switching the
mode, wait for health, reconnect PyMilvus, and keep the QueryNode and loaded
collection unchanged.

Use balanced mode order:

| Repetition | Order |
| --- | --- |
| 1 | Batch, Streaming |
| 2 | Streaming, Batch |
| 3 | Batch, Streaming |
| 4 | Streaming, Batch |

The complete matrix contains eight timed intervals.

## Correctness Gate

Before running timed intervals:

1. Verify the reusable collection has 1,000,000 rows, 63 sealed segments, and
   no growing segments.
2. Require three identical consecutive segment-placement observations.
3. Verify exactly one healthy QueryNode owns all sealed segments.
4. Run the identical Query once in Batch and once in Streaming mode.
5. Require exactly 8,192 rows from each mode.
6. Require IDs to be ordered identically between modes.
7. Hash each row as the primary key followed by the exact float32 vector bytes.
8. Require the ordered whole-result hash to match between modes.
9. Preserve the Proxy environment for both modes. The enabled configuration and
   supported request shape select Query Streaming without a Batch fallback;
   Query-specific runtime routing counters do not currently exist.

Do not run performance intervals if any correctness assertion fails.

## Procedure

1. Record local, GitHub, `.42`, and `.233` source states and machine inventory.
2. Verify no unrelated resource-intensive process or container is running.
3. Start the existing dependency services and fixed Milvus roles on `.42`.
4. Start one QueryNode, load the reusable collection, and qualify placement.
5. Execute the correctness gate and preserve both complete result hashes.
6. Execute four balanced-order repetitions.
7. Before each interval, restart Proxy with the selected mode and wait for
   health from `.233`.
8. Run five excluded warm-up operations.
9. Measure until both 60 seconds and 30 successful operations are reached.
10. Sample Proxy CPU and RSS once per second during the timed interval.
11. Capture Proxy Prometheus snapshots before and after every interval.
12. Capture Proxy heap and cumulative allocation profiles before and after
    every interval.
13. Run separate 30-second Batch and Streaming CPU-profile intervals after the
    timed matrix; exclude them from QPS and latency aggregation.
14. Preserve configurations, client output, process samples, profiles, logs,
    placement, and exit status.
15. Stop experiment processes without deleting the reusable collection data.

## Metrics

### Primary

- successful operations and errors;
- QPS;
- p50, p95, p99, and maximum end-to-end Query latency;
- result row count;
- ordered ID/vector result hash; and
- Streaming QPS / Batch QPS and Streaming p95 / Batch p95.

### Process Diagnostics

- one-second Proxy CPU and RSS;
- Proxy Go heap and cumulative allocation deltas;
- Proxy GC count and pause-time deltas;
- Proxy CPU profile; and
- Proxy heap and allocation profile attribution.

### Available Request Diagnostics

- effective QueryNode WorkNode identity;
- Query duration from existing Milvus metrics and logs;
- Proxy request duration; and
- exact logical ID and vector field bytes in the final SDK result.

Query-specific internal application, gRPC payload, connection, TCP, and
reduction counters do not currently exist. This experiment must not substitute
Search counters or infer internal Query bytes from process RSS. If the primary
result cannot be explained using the available evidence, detailed Query
instrumentation becomes a separate follow-up rather than an unrecorded change
to this run.

## Aggregation and Reporting

For each mode, report the median across four repetitions for QPS, p50, p95,
p99, maximum latency, peak Proxy RSS, and sampled Proxy CPU. Also report every
individual repetition so order effects remain visible.

Report these ratios:

```text
Streaming / Batch QPS
Streaming / Batch p95 latency
Streaming / Batch allocated bytes per operation
```

Do not combine Batch and Streaming process metrics. Treat pprof allocation
profiles as sampled attribution; use runtime counters for allocation totals.

## Exit Criteria

- All eight timed intervals complete without request errors.
- Every operation returns exactly 8,192 rows.
- Batch and Streaming ordered ID/vector hashes match.
- All four balanced repetitions are present for both modes.
- Median QPS, p95 latency, and their Streaming/Batch ratios are reported.
- Proxy CPU, RSS, allocation, GC, and profile evidence is retained.
- Proxy environments prove the intended mode configuration, and the request
  shape satisfies the Query Streaming eligibility contract.
- `report.md` clearly separates verified observations from hypotheses.

## Evidence Layout

```text
runs/<run-id>/
  preflight/
    source-state/
    machine-state/
    collection/
    placement/
    correctness/{batch,streaming}/
  timed/
    rep1/{batch,streaming}/
    rep2/{streaming,batch}/
    rep3/{batch,streaming}/
    rep4/{streaming,batch}/
  profiles/{batch,streaming}/
  manifest.tsv
  summary.json
```

`run_test.sh` must write immutable evidence under a new run ID. `report.md` is
created only after the preserved evidence has been analyzed.

## Out of Scope

- QueryNode fan-in greater than one;
- client concurrency greater than one;
- Query iterator behavior;
- unlimited Query;
- count, aggregate, group-by, order-by, and element-level Query;
- static gRPC flow-control windows;
- application-level credit coordination;
- Query-specific internal transport instrumentation; and
- comparison against ANN execution time.
