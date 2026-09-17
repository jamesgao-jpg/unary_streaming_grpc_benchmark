# Streaming Chunk-Size Sweep for Bounded Plain Query Batch vs Streaming

## Status

Proposed. The experiment has not been executed.

Tracking issue:
[unary_streaming_grpc_benchmark#10](https://github.com/jamesgao-jpg/unary_streaming_grpc_benchmark/issues/10).

## Objective

Sweep the Streaming Chunk size (`proxy.queryView.queryStreamChunkSize`) at fixed
QueryNode fan-in for the bounded Plain Query vector-payload workload, and compare
Streaming against Batch at every Chunk size.

`test_0917_01` (issue #9, closed) established that at Chunk 1,024 Units the
Streaming/Batch QPS ratio grows with fan-in from 0.973x (N1) to 1.709x (N32),
with Batch peak Proxy RSS scaling ~11.5x and Streaming ~1.6x. That run fixed the
Chunk size; this experiment isolates the Chunk-size dimension.

## Experimental Interpretation

Chunk size is a Streaming-only knob: `queryStreamChunkSize` configures the
maximum number of Units per streamed Chunk and is not read by the Batch path.
Batch therefore serves as a fixed baseline that is invariant to the sweep.

The tradeoff under test:

```text
smaller Chunk -> more messages, less retained input per child, more overhead
larger Chunk  -> fewer messages, more retained input per child, less overhead
```

At fixed fan-in, Streaming retained input is expected to scale with
`Chunk size * child count`, so smaller Chunks should lower peak Proxy RSS while
adding per-message gRPC/reduction overhead that may cost QPS and latency. The
experiment maps this tradeoff and locates the practical operating range around
the current 1,024-Unit default.

## Fixed Topology

| Component | Count |
| --- | ---: |
| Proxy | 1 |
| MixCoord | 1 |
| DataNode, including index service | 1 |
| StreamingNode | 1 |
| QueryNode | 16 for the primary case, 32 for the confirmation case |

Cases run `FANIN-N16` first, then `FANIN-N32`. N16 is the primary case (the
crossover region with a clean Streaming advantage in `test_0917_01`); N32 is a
confirmation point and is required to qualify placement and WorkNode identity
before measurement, exactly as in `test_0917_01`.

Between cases, the runner releases the collection, stops all QueryNodes, starts
the next case's QueryNode count, and reloads the collection. The 63 sealed
segments redistribute over the new QueryNode set; placement and WorkNode
identity must be re-qualified for every case before any measurement.

## Chunk-Size Sweep

| Chunk size (Units) | Chunks per 8,192-row result |
| ---: | ---: |
| 256 | 32 |
| 512 | 16 |
| 1,024 | 8 |
| 2,048 | 4 |
| 4,096 | 2 |
| 8,192 | 1 |

Sweep order is ascending within each case. Every Chunk size is measured in
Streaming mode with two balanced repetitions.

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
| Client concurrency | 1 |
| Warm-up operations per interval | 5 |
| Minimum timed operations per interval | 30 |
| Minimum timed duration per interval | 60 seconds |
| Repetitions per Streaming Chunk size | 2 |
| Batch repetitions per case | 2 (one before, one after the sweep) |

Each operation issues one bounded ordinary Query and consumes the complete SDK
response. The PyMilvus connection remains open throughout one interval and is
recreated after every Proxy restart.

## A/B Modes

| Mode | Proxy configuration |
| --- | --- |
| Batch | `proxy.queryView.enableQueryStreaming=false` |
| Streaming | `proxy.queryView.enableQueryStreaming=true`; `proxy.queryView.queryStreamChunkSize=<256..8192>` |

Use the same Milvus binary in both modes. Restart only Proxy when switching the
mode or Chunk size, wait for health, reconnect PyMilvus, and keep the QueryNodes
and loaded collection unchanged.

Batch is measured twice per case: one interval before the Chunk sweep and one
after. Bracketing exposes any drift across the sweep window and provides the
Batch baseline that every Streaming Chunk size is compared against. Because the
Batch path does not read `queryStreamChunkSize`, Batch is invariant to the
sweep, and repeated Batch intervals for every Chunk size would be redundant.

## Correctness Gate per Case

Before running timed intervals for a case:

1. Verify the reusable collection has 1,000,000 rows, 63 sealed segments, and
   no growing segments.
2. Run the identical Query once in Batch mode.
3. Run the identical Query once in Streaming mode at every Chunk size in the
   sweep.
4. Require exactly 8,192 rows from every verification.
5. Require IDs to be ordered identically between Batch and every Streaming
   Chunk size.
6. Hash each row as the primary key followed by the exact float32 vector bytes.
7. Require the ordered whole-result hash to match between Batch and every
   Streaming Chunk size, and across all fan-in cases (Chunk-size invariance and
   fan-in invariance).
8. Preserve the Proxy environment for every verification.

Do not run performance intervals for a case if any correctness assertion fails.

## Procedure

1. Record local, GitHub, `.42`, and `.233` source states and machine inventory.
2. Verify no unrelated resource-intensive process or container is running.
3. Build Milvus once on `.42` and verify the binary exists.
4. Start the existing dependency services and fixed Milvus roles on `.42`.
5. For each case in FANIN-N16, FANIN-N32:
   a. Start the case's QueryNode count (first case) or release the collection,
      stop QueryNodes, start the case's QueryNode count, and reload.
   b. Qualify placement and WorkNode identity.
   c. Run one Batch timed interval.
   d. Execute the correctness gate across all Chunk sizes.
   e. For each Chunk size in 256, 512, 1,024, 2,048, 4,096, 8,192:
      - restart Proxy in Streaming mode with that Chunk size;
      - run two timed intervals (rep 1, rep 2).
   f. Run a second Batch timed interval.
   g. Run separate 30-second CPU-profile intervals: one Batch, and Streaming at
      Chunk sizes 256, 1,024, and 8,192.
   h. Before each interval, restart Proxy with the selected mode and Chunk size
      and wait for health from `.233`.
   i. Run five excluded warm-up operations.
   j. Measure until both 60 seconds and 30 successful operations are reached.
   k. Sample Proxy CPU and RSS once per second during the timed interval.
   l. Capture Proxy Prometheus snapshots before and after every interval.
   m. Capture Proxy heap and cumulative allocation profiles before and after
      every interval.
   n. Preserve configurations, client output, process samples, profiles, logs,
      placement, and exit status.
6. Stop experiment processes without deleting the reusable collection data.

## Metrics

### Primary

- successful operations and errors per interval;
- QPS per interval;
- p50, p95, p99, and maximum end-to-end Query latency;
- result row count (8,192) and logical field bytes per operation;
- ordered ID/vector result hash per interval;
- Streaming QPS / Batch QPS and Streaming p95 / Batch p95 per Chunk size; and
- median peak Proxy RSS per mode per Chunk size.

### Process Diagnostics (per interval)

- one-second Proxy CPU and RSS samples;
- Proxy Go heap and cumulative allocation deltas from the Prometheus snapshot;
- Proxy GC count and pause-time deltas;
- Proxy CPU profile (Batch and Streaming at Chunk sizes 256, 1,024, 8,192); and
- Proxy heap and allocation profile attribution.

### Available Request Diagnostics (per case)

- effective QueryNode WorkNode identity log;
- Query duration from existing Milvus metrics and logs;
- Proxy request duration; and
- exact logical ID and vector field bytes in the final SDK result.

Query-specific internal application, gRPC payload, connection, TCP, and
reduction counters do not currently exist. This experiment relies on
process-level evidence only, as in `test_0917_01`. The report must not
substitute Search counters or infer internal Query bytes from process RSS
alone.

## Aggregation and Reporting

For each case and Chunk size, report the median across two Streaming
repetitions for QPS, p50, p95, p99, maximum latency, peak Proxy RSS, and
sampled Proxy CPU. Report Batch as the median of its two bracketing intervals.
Also report every individual repetition so order effects remain visible.

Report these per Chunk size:

```text
Streaming / Batch QPS
Streaming / Batch p95 latency
Streaming / Batch peak Proxy RSS
```

Report the Chunk-size scaling table:

```text
chunk size | Streaming QPS | Streaming p95 | Streaming peak RSS | S/B QPS | S/B p95
```

`report.md` must separate verified observations from hypotheses. The primary
questions are whether peak Proxy RSS scales with Chunk size at fixed fan-in and
where the QPS/latency overhead of smaller Chunks begins to dominate.

## Exit Criteria

- All timed intervals complete without request errors.
- Every operation returns exactly 8,192 rows.
- Batch and Streaming ordered ID/vector hashes match at every Chunk size.
- Result hashes are identical across all Chunk sizes and both fan-in cases.
- Placement and WorkNode evidence qualifies every measured case.
- Median QPS, p95 latency, peak Proxy RSS, and their Streaming/Batch ratios are
  reported per Chunk size.
- Proxy CPU, RSS, allocation, GC, and profile evidence is retained per interval.
- `report.md` clearly separates verified observations from hypotheses.

## Evidence Layout

```text
runs/<run-id>/
  collection.json
  FANIN-N16/
    load.json
    placement-{1,2,3}.json
    placement-summary.json
    placement-validation.log
    worknodes.log
    worknodes-probe.json
    correctness/batch-verify.json
    correctness/chunk-0256-verify.json ... chunk-8192-verify.json
    correctness/comparison.json
    batch/rep1/...
    batch/rep2/...
    chunk-0256/rep1/streaming/...
    chunk-0256/rep2/streaming/...
    chunk-0512/... chunk-8192/... (same layout)
    profiles/batch/...
    profiles/chunk-0256/streaming/...
    profiles/chunk-1024/streaming/...
    profiles/chunk-8192/streaming/...
    server-logs/
    status.txt
  FANIN-N32/ (same layout)
  manifest.tsv
  summary.json
  report-input.json
```

`run_test.sh` must write immutable evidence under a new run ID. `report.md` is
created only after the preserved evidence has been analyzed.

## Out of Scope

- QueryNode fan-in other than 16 and 32;
- client concurrency greater than one (issue #11);
- Query iterator behavior;
- unlimited Query;
- count, aggregate, group-by, order-by, and element-level Query;
- static gRPC flow-control windows (standalone benchmark);
- application-level credit coordination;
- Query-specific internal transport instrumentation; and
- comparison against ANN execution time.
