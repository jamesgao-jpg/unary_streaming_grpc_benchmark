# Cohere 1M Batch-versus-Streaming Fan-In Benchmark

## Status

Draft. This experiment has not been executed.

## Objective

Determine whether the QueryNode fan-in behavior observed in the standalone
gRPC benchmark also appears in the Milvus end-to-end ANN Search iterator path.

For each qualified QueryNode count, compare the existing Batch path with the
Streaming Reduce path while keeping the Milvus binary, collection, index,
segment placement, query vectors, request shape, and client concurrency fixed.

## Question

At fixed topK, Streaming Chunk size, and concurrency, how does effective
QueryNode fan-in change:

- Batch and Streaming QPS;
- Batch and Streaming p50, p95, and p99 latency; and
- the Streaming-to-Batch performance ratio?

## Relationship to the Standalone Benchmark

This experiment migrates only the fan-in main effect from the standalone
`ordered_topk` benchmark:

```text
Batch:
    each QueryNode returns its complete local topK through one unary RPC
    Proxy reduces all child results

Streaming:
    each QueryNode sends its local topK in fixed-Unit Chunks
    Proxy reduces Chunks and stops after producing the requested topK
```

The test uses Milvus's production dynamic gRPC flow control. It does not test
static windows, full transfer, artificial rank distributions, or transport-only
payloads.

## Machines

| Role | Machine | Working directory |
| --- | --- | --- |
| Milvus server | `ubuntu@10.15.9.42` | `/home/ubuntu/milvus-qv` |
| Dataset and load client | `ubuntu@10.15.2.233` | `/home/ubuntu/reducestream_perf` |
| Experiment controller and evidence | Local machine | `unary_streaming_grpc_benchmark/milvus_result/test_0915_02` |

All Milvus components run as native processes on `.42`. Client traffic crosses
from `.233` to Proxy on `.42`; QueryNode-to-Proxy traffic uses `.42` loopback.

## Source and Build

The runner must record the actual values before execution:

| Item | Draft-time value |
| --- | --- |
| Milvus branch | `codex/qv-reducestream-e2e-benchmark-20260915` |
| Milvus commit | `eede3bdb4c5e396edc35c985c89677307a2d60ca` |
| Benchmark repository branch | `master` |
| Build target | `make milvus` |
| Server binary | `/home/ubuntu/milvus-qv/bin/milvus` |

Abort if the local and `.42` Milvus commits differ or either checkout contains
an unexplained source change. Do not rebuild between Batch and Streaming modes.

## Reused Collection

Reuse the collection qualified by `test_0915_01` run
`20260915T090613Z-20989`:

| Setting | Value |
| --- | --- |
| Collection | `cohere_1m_qn_fanin` |
| Dataset | Cohere 1M |
| Rows | 1,000,000 |
| Vector type | `FLOAT_VECTOR` |
| Dimension | 768 |
| Metric | COSINE |
| Vchannels | 1 |
| Replicas | 1 |
| Sealed segments | 63 |
| Growing segments | 0 |
| Index | HNSW, `M=16`, `efConstruction=200` |
| Search parameter | `ef=8192` |

The experiment must verify that all 63 segment IDs and row counts match the
accepted `test_0915_01` snapshot. It must not reinsert data, rebuild the index,
or create a replacement collection. Missing or changed persistent data is an
experiment failure requiring a separately approved preparation run.

## Milvus Topology

| Component | Count |
| --- | ---: |
| Proxy | 1 |
| MixCoord | 1 |
| DataNode, including index build | 1 |
| StreamingNode | 1 |
| QueryNode | `1, 2, 4, 8, 16, 32` |

The collection has one vchannel, so the observed QueryNode WorkNode count is
the effective cross-process child count for its per-vchannel reduction.
StreamingNode must not be selected as a WorkNode in this sealed-only test.

## Fixed Workload

| Parameter | Value |
| --- | ---: |
| Request type | ANN Search iterator |
| NQ | 1 |
| topK | 8,192 |
| Iterator `batch_size` | 8,192 |
| Iterator `limit` | 8,192 |
| Streaming Chunk size | 1,024 Units |
| Client concurrency | 1 |
| Query vectors | First 100 Cohere test vectors, fixed cyclic order |
| Output fields | None beyond ID and score |
| `ignore_growing` | `true` |
| Consistency | Strong |
| Warm-up operations per mode | 20 |
| Minimum measured operations per mode | 100 |
| Minimum measured duration per mode | 60 seconds |
| Repetitions per topology | 4 |

TopK 8,192 is below the smallest qualified N32 QueryNode row ownership
(16,111 rows). This preserves the intended condition that every child can
produce a complete local topK while exercising eight 1,024-Unit Streaming
Chunks per complete child result. HNSW `ef` equals topK because `ef` must not
be smaller than topK. TopK 16,384 is excluded from this fan-in test because at
least one qualified N32 QueryNode owns fewer rows than that.

One measured operation creates one Search iterator, consumes all 8,192
results, verifies the result count, and closes the iterator. The PyMilvus
connection remains open for the complete measured mode interval.

## A/B Modes

| Mode | Configuration |
| --- | --- |
| Batch | `proxy.queryView.enableSearchStreaming=false` |
| Streaming | `proxy.queryView.enableSearchStreaming=true` and `proxy.queryView.searchStreamChunkSize=1024` |

The mode switch is a Proxy process configuration, not an SDK option. Restart
only Proxy when changing the mode, wait for health, and reconnect the load
client. Keep QueryNodes and the loaded collection unchanged.

For each topology, use balanced mode order:

| Repetition | Order |
| --- | --- |
| 1 | Batch, then Streaming |
| 2 | Streaming, then Batch |
| 3 | Batch, then Streaming |
| 4 | Streaming, then Batch |

## Matrix

| Case | QueryNodes | Expected QueryNode WorkNodes | Mode intervals |
| --- | ---: | ---: | ---: |
| `FANIN-N1` | 1 | 1 | 8 |
| `FANIN-N2` | 2 | 2 | 8 |
| `FANIN-N4` | 4 | 4 | 8 |
| `FANIN-N8` | 8 | 8 | 8 |
| `FANIN-N16` | 16 | 16 | 8 |
| `FANIN-N32` | 32 | 32 | 8 |

The complete matrix contains 48 measured mode intervals. Run topologies in
ascending order and finish all repetitions for one topology before changing
the QueryNode count.

## Correctness Gate

Before timing a topology:

1. Confirm exactly the requested QueryNodes are healthy and eligible.
2. Load the existing collection with one replica.
3. Require three identical consecutive segment-placement observations.
4. Verify the 63 segment IDs and row counts against `test_0915_01`.
5. Run one Batch and one Streaming request for ten fixed query vectors.
6. Require 8,192 ordered IDs from every request.
7. Require identical ordered IDs and scores within `1e-6` between modes.
8. Verify the observed QueryNode WorkNode IDs match segment placement.

Disable Proxy debug logging after WorkNode qualification and before warm-up.
Do not run performance intervals if this gate fails.

## Procedure

1. Record local, `.42`, and `.233` source states and machine inventories.
2. Verify that no unrelated resource-intensive process or container is active.
3. Start the existing dependency services and fixed Milvus base roles on `.42`.
4. Verify the reusable collection and index without modifying them.
5. For each QueryNode count, release the collection, replace the QueryNode set,
   reload the collection, and qualify placement and WorkNodes.
6. Apply the correctness gate for both modes.
7. Run four balanced-order repetitions with a fresh client process for each
   mode interval and one persistent connection within that interval.
8. Warm up before each measurement and exclude warm-up latency from results.
9. Measure until both 60 seconds and 100 successful operations are reached.
10. Preserve raw client output, process samples, configurations, placement,
    WorkNode evidence, component logs, and exit status after every interval.
11. Stop experiment processes after the matrix while preserving the reusable
    collection volumes and immutable evidence.

## Recorded Metrics

### Primary

- successful operations and errors;
- QPS;
- p50, p95, p99, and maximum end-to-end operation latency;
- ordered result count and correctness hash; and
- observed QueryNode WorkNode identities.

### Secondary Diagnostics

- Proxy CPU time and peak RSS during each measured interval;
- aggregate QueryNode CPU time and peak RSS;
- client CPU time and peak RSS; and
- host CPU, memory, disk, and network-interface counters before and after each
  interval.

Process RSS and host counters are diagnostics only. They do not establish where
bytes reside inside gRPC, HTTP/2, or kernel buffers.

## Derived Comparisons

Calculate from per-repetition results:

```text
QPS ratio = Streaming QPS / Batch QPS
pXX latency ratio = Streaming pXX / Batch pXX
Batch fan-in scaling = Batch metric at N / Batch metric at N1
Streaming fan-in scaling = Streaming metric at N / Streaming metric at N1
```

Report Batch-first and Streaming-first strata separately before calculating the
combined median. In the Milvus report, call the existing non-streaming mode
`Batch`; use `Unary` only when relating it to the standalone benchmark.

## Hypotheses

These are predictions, not pass criteria:

- At low fan-in, Streaming may have little advantage or may be slower because
  it sends more response messages and performs incremental reduction.
- As fan-in increases, Batch transfer and retained child results grow with the
  number of QueryNodes, while Streaming can stop after producing global topK.
- The Streaming-to-Batch QPS ratio may therefore increase with fan-in.
- QueryNode ANN execution cost should remain broadly similar because the
  current stream server first materializes the local Search result and then
  splits it into Chunks.

## Acceptance Criteria

The experiment passes execution qualification only when:

```text
all six fan-in cases complete
all 48 measured mode intervals complete
zero request errors occur
every operation returns exactly 8,192 results
Batch and Streaming correctness results match
observed QueryNode WorkNodes equal the requested fan-in
StreamingNode WorkNodes equal zero
the same 63 sealed segments are reused throughout
each interval runs for at least 60 seconds and 100 successful operations
all required source, configuration, and raw measurement evidence is retained
```

No required performance direction is an acceptance criterion. A slower
Streaming result is valid evidence.

## Evidence

Store each execution under:

```text
milvus_result/test_0915_02/runs/<UTC-run-id>/
```

The immutable run directory must contain:

- this test plan and the exact runner;
- source commits, branches, Git statuses, and preserved diffs;
- generated Batch and Streaming Proxy configurations;
- collection schema, index description, and the accepted segment snapshot;
- per-topology placement and WorkNode observations;
- client query-vector identity and request parameters;
- raw per-operation latency and correctness records;
- per-interval process and host samples;
- a manifest covering all 48 intervals; and
- `COMPLETED` or `FAILED` with the first failure reason.

After analysis, add `report.md` containing the environment, topology table,
per-repetition results, order-stratified medians, combined comparison table,
QPS and latency ratios, verified conclusions, limitations, and artifact path.

## Boundaries

This experiment does not:

- sweep topK, Chunk size, concurrency, or gRPC flow-control windows;
- reproduce standalone synthetic payload byte sizes;
- guarantee `interleaved` or `dominant_child` rank placement;
- attribute instantaneous memory to application, gRPC, HTTP/2, or kernel
  buffers;
- measure a full-transfer Streaming control; or
- claim that QueryNode search execution or memory is reduced.

Those require separate experiments after this fan-in result is reviewed.

## Source Basis

- [The iterator-only mode switch selects Streaming or the existing Batch path](../../../milvus-qv/internal/views/queryclient/legacy_client.go).
- [The per-vchannel Streaming path opens one child stream per planned WorkNode](../../../milvus-qv/internal/views/queryclient/shard_client.go).
- [The QN/SN stream server materializes Search output before splitting it into Chunks](../../../milvus-qv/internal/views/viewquery/server.go).
- [The Proxy task consumes the final ReduceStream before common post-processing](../../../milvus-qv/internal/proxy/task_search.go).
- [The Proxy configuration defines the mode switch and Unit-count Chunk limit](../../../milvus-qv/pkg/util/paramtable/component_param.go).

## Runner

`run_test.sh` implements this plan and has passed local shell and embedded
Python syntax checks. It has not started Milvus or executed the experiment and
must be reviewed before the run begins.
