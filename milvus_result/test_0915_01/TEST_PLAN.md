# Cohere 10M QN Fan-In Topology Qualification

## Status

Draft. Not executed. `run_test.sh` is drafted; `report.md` does not exist yet.

## Objective

Prepare one reusable Cohere 10M collection and verify that each requested
QueryNode count produces the same effective QueryNode fan-in for its single
vchannel.

This is a topology qualification. It establishes that later Unary-versus-
Streaming measurements use the intended child-stream count.

## Outputs

The completed experiment must produce:

- one accepted `dataCoord.segment.maxSize` value and its calculation;
- one fixed sealed-segment layout reused by every QueryNode count;
- segment-to-QueryNode placement evidence for QueryNode counts
  `1, 2, 4, 8, 16, 32`;
- observed QueryNode and StreamingNode `GetQueryPlan.WorkNodes` for every
  QueryNode count;
- a pass/fail decision for each topology; and
- a reusable procedure for preparing later Milvus performance experiments.

## Non-Goals

This experiment does not measure Unary-versus-Streaming QPS, latency, memory,
gRPC window behavior, Chunk-size effects, concurrency, or rank distribution.

## Machines

| Role | Machine | Working directory |
| --- | --- | --- |
| Milvus server | `ubuntu@10.15.9.42` | `/home/ubuntu/milvus-qv` |
| Dataset and test client | `ubuntu@10.15.2.233` | `/home/ubuntu/reducestream_perf` |

The run record must capture the actual Milvus and VectorDBBench commits, branch
names, Git status, host resources, and executed commands. Draft-time reference
commits are Milvus `3ddff3aad8d0cb43d0f61a4fbd401e0b2867ba72` and
VectorDBBench `75628b4581d4778d348c68c7c170f17044354af8`.

## Milvus Topology

| Component | Count |
| --- | ---: |
| Proxy | 1 |
| MixCoord | 1 |
| DataNode, including index build | 1 |
| IndexNode | 0 |
| StreamingNode | 1 |
| QueryNode | `1, 2, 4, 8, 16, 32` |

This `qv` source tree has merged IndexNode responsibilities into DataNode, so
the experiment must not try to start a removed standalone IndexNode role.

All QueryNodes for a case must be healthy, non-stopping, and eligible in the
collection replica's resource group. No unrelated QueryNode may be eligible.
Use one replica. QueryNode count and StreamingNode count must be controlled
independently.

## Collection

| Setting | Value |
| --- | --- |
| Dataset | Cohere 10M |
| Rows | 10,000,000 |
| Vector type | `FLOAT_VECTOR` |
| Dimension | 768 |
| Metric | COSINE |
| Fields | `pk INT64`, `id INT64`, `vector FLOAT_VECTOR(768)` |
| Vchannels | 1 |
| Replicas | 1 |
| Target sealed segments | 256 |
| Automatic compaction | Disabled |
| Seal-proportion jitter | 0 |
| Intermediate flushes | None |
| Final flushes | One, after all rows are inserted |
| Index | HNSW |
| HNSW `M` | 16 |
| HNSW `efConstruction` | 200 |
| Search `ef` | 128 |
| Qualification Search limit | 10 |

The same collection and sealed segments must be reused across all QueryNode
counts. Release and reload the collection between topologies; do not reinsert
the dataset.

## Segment-Size Calculation

Milvus estimates the minimal collection schema as:

```text
estimatedBytesPerRow = 8 + 8 + 768 * 4 = 3,088 bytes
targetRowsPerSegment = ceil(10,000,000 / 256) = 39,063 rows
sealProportion = 0.12

segment.maxSizeMiB = ceil(
    targetRowsPerSegment * estimatedBytesPerRow
    / sealProportion
    / 1,048,576
) = 959 MiB
```

With `segment.maxSize = 959`, the nominal maximum is 325,642 rows and the
nominal sealing point is 39,078 rows, producing 256 segments for 10M rows.
Actual sealed segments, rather than this estimate, determine whether setup
passes.

The persistent calculation script must accept dataset row count and the QN
fan-in matrix, use the collection schema above, print every intermediate value,
and generate the Milvus configuration override. It must not modify a running
cluster because `segment.maxSize` is not refreshable.

## Preflight Gates

Before data preparation:

1. Synchronize the intended Milvus source to `.42` and build it successfully.
2. Confirm `.42` and `.233` contain no unrelated resource-intensive workload.
3. Record free disk, memory, CPU count, and process/container inventory.
4. Require at least 100 GiB free on `.42` and 40 GiB free on `.233`.
5. Download and checksum the Cohere 10M dataset on `.233`.
6. Confirm one vchannel and one replica in the collection definition.
7. Start Milvus with `segment.maxSize=959`, seal jitter zero, and automatic
   compaction disabled.
8. Confirm the exact configured values from the running components.

Abort before insertion if a preflight gate fails.

## Procedure

### Phase 1: Prepare the Fixed Segment Layout

1. Start the cluster with one QueryNode and one StreamingNode.
2. Create the one-vchannel collection with the recorded schema and index.
3. Insert all 10M rows in bounded requests without explicitly flushing between
   requests.
4. Flush once after insertion completes.
5. Wait for every row to become sealed and for index construction to finish.
6. Poll the sealed segment list three times and require the same IDs and row
   counts in all observations.
7. Record every segment ID and row count.
8. Require between 240 and 272 sealed segments. If outside this range, stop and
   report the observed count; do not silently recalculate or reinsert.

### Phase 2: Qualify Each QueryNode Fan-In

Run the following cases in order:

| Case | QueryNodes | Expected WorkNodes |
| --- | ---: | ---: |
| `N1` | 1 | 1 |
| `N2` | 2 | 2 |
| `N4` | 4 | 4 |
| `N8` | 8 | 8 |
| `N16` | 16 | 16 |
| `N32` | 32 | 32 |

For each case:

1. Release the collection and wait until the previous placement is removed.
2. Stop all QueryNodes, then start exactly the requested count.
3. Wait until exactly those QueryNodes are healthy and eligible.
4. Load the existing collection with one replica.
5. Wait for load completion.
6. Poll segment placement three times and require identical observations.
7. Record each segment ID, QueryNode ID, and row count.
8. Run one ANN Search iterator request with `ignore_growing=true` and record
   its QueryNode and StreamingNode `GetQueryPlan.WorkNodes`.
9. Verify that the iterator succeeds and returns the requested result count.
10. Evaluate the topology acceptance criteria before continuing.

If a topology fails, preserve its evidence and stop. Do not proceed to a larger
QueryNode count.

## Per-Topology Acceptance Criteria

A topology passes only when all conditions hold:

```text
healthy eligible QueryNodes == requested QueryNodes
distinct QueryNodes owning sealed rows == requested QueryNodes
QueryNode GetQueryPlan.WorkNodes == requested QueryNodes
StreamingNode GetQueryPlan.WorkNodes == 0
every QueryNode owns at least one sealed segment
max(rows per QueryNode) - min(rows per QueryNode)
    <= largest sealed segment row count
placement is unchanged across three consecutive observations
sealed-only ANN Search iterator succeeds
```

Segment counts per QueryNode may differ. Row distribution is authoritative.

## Source Basis

- [Milvus estimates bytes per record from the collection schema](../../../milvus-qv/pkg/util/typeutil/schema.go).
- [DataCoord converts segment maximum bytes to maximum rows](../../../milvus-qv/internal/datacoord/segment_allocation_policy.go).
- [QueryView derives its fan-in budget from eligible nodes, segments, and rows](../../../milvus-qv/internal/views/coord/balancer/scoring.go).
- [QueryView assigns larger segments first and records rows by node](../../../milvus-qv/internal/views/coord/balancer/allocate.go).
- [The query plan emits one WorkNode for each selected QueryNode](../../../milvus-qv/internal/streamingnode/server/wal/adaptor/wal_adaptor.go).

## Evidence

Store immutable run evidence under a timestamped `runs/<UTC-run-id>/`
directory inside this experiment directory. It must include:

- source commits, branches, Git statuses, and preserved diffs;
- generated Milvus configuration and the segment-size calculation;
- component process IDs, ports, logs, and health checks;
- dataset identity, file sizes, and checksums;
- collection schema, index configuration, vchannel count, and replica count;
- insertion, flush, index, load, release, and Search iterator commands;
- all segment-placement observations;
- observed QueryPlan WorkNode identities;
- per-case pass/fail output and command exit codes; and
- disk and memory snapshots before and after preparation.

After analysis, create `report.md` with the calculated and actual segment
layout, one topology table, the largest qualified fan-in, failures, and the
decision on whether performance testing may begin.

## WorkNode Evidence

The benchmark Milvus branch adds one debug-only structured log immediately
after `workNodesFromPlan()` on the Search iterator streaming path. It records:

```text
vchannel
replicaID
queryNodeIDs
streamingNodePresent
workNodeCount
```

`run_test.sh` enables debug logging only for Proxy, runs one sealed-only Search
iterator, and requires the logged QueryNode IDs to match the QueryNodes found
in the segment-placement snapshot. The log is benchmark instrumentation only;
it is not part of the Milvus API.

## Experiment Exit Criteria

The experiment passes only if all six QueryNode topologies pass and the same
fixed segment layout is used throughout. The next Unary-versus-Streaming
experiment must not begin otherwise.

## Runner

Run `run_test.sh` on the local machine. It uses SSH to:

1. verify and build the selected Milvus commit on `.42`;
2. start one MixCoord, DataNode, StreamingNode, and Proxy on `.42`;
3. start and stop QueryNodes independently for each requested fan-in;
4. use the pinned VectorDBBench environment on `.233` to download Cohere 10M,
   insert it, build HNSW, load it, inspect placement, and run Search iterator;
5. evaluate every acceptance criterion before advancing; and
6. retain local and remote evidence under the run ID.

The runner stops all experiment processes and dependency containers when it
finishes, but preserves the timestamped MinIO, etcd, logs, and local-storage
directories on `.42`.
