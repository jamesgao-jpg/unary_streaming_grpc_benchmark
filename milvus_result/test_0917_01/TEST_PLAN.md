# Bounded Plain Query Vector-Payload Fan-In Batch/Streaming Benchmark

## Status

Proposed. The experiment has not been executed.

Tracking issue:
[unary_streaming_grpc_benchmark#8](https://github.com/jamesgao-jpg/unary_streaming_grpc_benchmark/issues/8).

## Objective

Sweep effective QueryNode fan-in per vchannel in descending order through N32,
N16, N8, N4, N2, and N1 for the bounded Plain Query vector-payload workload
that `test_0916_02` established as its N1 baseline, and compare Batch and
Streaming execution at every fan-in level.

Cases run from most QueryNodes to fewest: the highest fan-in case executes
first so that any unexpected fan-in behavior surfaces immediately and the run
stops to investigate that direction instead of discovering it last.

The experiment asks whether the promised Streaming Reduce memory property
appears as fan-in grows:

```text
Batch retained input    ~= full result bytes * child count
Streaming retained input ~= Chunk bytes * child count
```

`test_0916_02` could not answer this: with one QueryNode child there is no
redundant child result to stop early, so N1 is only a transport-overhead
baseline. Multiple QueryNode children contributing to the same vchannel request
are required to observe the scaling behavior.

## Experimental Interpretation

Every case reuses the same immutable collection and the identical Query request:

```text
ordinary query(), expr "pk >= 0", limit 8192
output fields: pk (INT64) and vector (768-dim FLOAT_VECTOR)
Streaming Chunk size: 1,024 Units
concurrency: 1
```

The final response always contains 8,192 rows and 25,231,360 logical field
bytes. Fan-in changes only the number of QueryNode children that each produce
their local 8,192-row result:

| Case | QueryNodes | Nominal QN-to-Proxy child payload (logical) |
| --- | ---: | ---: |
| N32 | 32 | 807,403,520 bytes |
| N16 | 16 | 403,701,760 bytes |
| N8 | 8 | 201,850,880 bytes |
| N4 | 4 | 100,925,440 bytes |
| N2 | 2 | 50,462,720 bytes |
| N1 | 1 | 25,231,360 bytes |

Batch transfers and materializes the complete child results before Proxy
reduction. Streaming transfers the same logical rows in 1,024-Unit Chunks and
reduces incrementally, so Proxy retention should approach Chunk bytes times
child count rather than complete-result bytes times child count if the
incremental reduction and early-stop behavior work as designed.

This experiment measures the scaling relationship; it does not assume the
promised behavior is present. The N1 case, which runs last, is retained as the
in-run transport baseline.

## Environment

| Item | Value |
| --- | --- |
| Milvus server | `ubuntu@10.15.9.42` |
| PyMilvus client | `ubuntu@10.15.2.233` |
| Milvus checkout | `/home/ubuntu/milvus-qv` |
| Client checkout | `/home/ubuntu/reducestream_perf` |
| Milvus branch | `codex/qv-reducestream-e2e-benchmark-20260915` |
| Required Milvus commit | `9d3dd3019283df4cfe77a180e4433666f7faaa98` |
| Milvus build target | `make milvus` |
| Server binary | `/home/ubuntu/milvus-qv/bin/milvus` |

The runner must abort unless local and `.42` Milvus checkouts resolve to the
required commit and contain no unexplained source changes.

## Reused Dataset

Reuse the immutable collection qualified by `test_0915_01` run
`20260915T090613Z-20989` and exercised by `test_0916_02` run
`20260916T125931Z-97545`.

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

The experiment must not reinsert data, rebuild the index, or create a
replacement collection.

## Topology Matrix

| Component | Count |
| --- | ---: |
| Proxy | 1 |
| MixCoord | 1 |
| DataNode, including index service | 1 |
| StreamingNode | 1 |
| QueryNode | 32, 16, 8, 4, 2, or 1 depending on the case |

Cases run in descending order: `FANIN-N32`, `FANIN-N16`, `FANIN-N8`,
`FANIN-N4`, `FANIN-N2`, `FANIN-N1`.

The runner starts the first case's QueryNode count directly. Between cases, the
runner releases the collection, stops all QueryNodes, starts the next case's
QueryNode count, and reloads the collection. The 63 sealed segments
redistribute over the new QueryNode set; placement and WorkNode identity must
be re-qualified for every case before any measurement.

### Qualification Gate per Case

1. Record segment-to-QueryNode placement three times 10 seconds apart and
   require three identical consecutive observations.
2. Require exactly the requested number of QueryNodes to own sealed segments,
   and the same 63 segment IDs with the same row counts as the `test_0915_01`
   baseline.
3. Issue one Streaming-mode request and record the effective WorkNodes from the
   Proxy log (`query view work nodes selected`).
4. Require the observed WorkNode IDs to equal the placement owner IDs, the
   WorkNode count to equal the requested QueryNode count, and
   `streamingNodePresent=false` for the sealed-only request.

Do not measure a case whose placement does not qualify.

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
| Repetitions per mode per case | 2 |

Each operation issues one bounded ordinary Query and consumes the complete SDK
response. The PyMilvus connection remains open throughout one interval and is
recreated after every Proxy restart.

## A/B Modes

| Mode | Proxy configuration |
| --- | --- |
| Batch | `proxy.queryView.enableQueryStreaming=false` |
| Streaming | `proxy.queryView.enableQueryStreaming=true`; `proxy.queryView.queryStreamChunkSize=1024` |

Use the same Milvus binary in both modes. Restart only Proxy when switching the
mode, wait for health, reconnect PyMilvus, and keep the QueryNodes and loaded
collection unchanged.

Use balanced mode order per case:

| Repetition | Order |
| --- | --- |
| 1 | Batch, Streaming |
| 2 | Streaming, Batch |

The complete matrix contains 6 cases * 4 timed intervals = 24 timed intervals,
plus 12 CPU-profile intervals (one per mode per case) excluded from QPS and
latency aggregation.

## Correctness Gate per Case

Before running timed intervals for a case:

1. Verify the reusable collection has 1,000,000 rows, 63 sealed segments, and
   no growing segments.
2. Run the identical Query once in Batch and once in Streaming mode.
3. Require exactly 8,192 rows from each mode.
4. Require IDs to be ordered identically between modes.
5. Hash each row as the primary key followed by the exact float32 vector bytes.
6. Require the ordered whole-result hash to match between modes.
7. Require the Batch and Streaming result hashes to be identical across all
   fan-in cases, because the collection, expression, limit, and offset are
   fixed and only the child distribution changes.
8. Preserve the Proxy environment for both modes.

Do not run performance intervals for a case if any correctness assertion fails.

## Procedure

1. Record local, GitHub, `.42`, and `.233` source states and machine inventory.
2. Verify no unrelated resource-intensive process or container is running.
3. Build Milvus once on `.42` and verify the binary exists.
4. Start the existing dependency services and fixed Milvus roles on `.42`.
5. Start the first case's QueryNode count (32) and inspect the reusable
   collection.
6. For each case in N32, N16, N8, N4, N2, N1:
   a. Start the case's QueryNode count (first case) or release the collection,
      stop QueryNodes, start the case's QueryNode count, and reload.
   b. Qualify placement and WorkNode identity.
   c. Execute the correctness gate and preserve both complete result hashes.
   d. Execute two balanced-order repetitions.
   e. Before each interval, restart Proxy with the selected mode and wait for
      health from `.233`.
   f. Run five excluded warm-up operations.
   g. Measure until both 60 seconds and 30 successful operations are reached.
   h. Sample Proxy CPU and RSS once per second during the timed interval.
   i. Capture Proxy Prometheus snapshots before and after every interval.
   j. Capture Proxy heap and cumulative allocation profiles before and after
      every interval.
   k. Run separate 30-second Batch and Streaming CPU-profile intervals.
   l. Preserve configurations, client output, process samples, profiles, logs,
      placement, and exit status.
7. Stop experiment processes without deleting the reusable collection data.

If the first high-fan-in case produces unexpected results (correctness,
placement, or performance), stop the run and investigate that direction before
continuing the sweep.

## Metrics

### Primary

- successful operations and errors per interval;
- QPS per interval;
- p50, p95, p99, and maximum end-to-end Query latency;
- result row count (8,192) and logical field bytes per operation;
- ordered ID/vector result hash per interval;
- Streaming QPS / Batch QPS and Streaming p95 / Batch p95 per case; and
- median peak Proxy RSS per mode per case, and its scaling across fan-in.

### Process Diagnostics (per interval)

- one-second Proxy CPU and RSS samples;
- Proxy Go heap and cumulative allocation deltas from the Prometheus snapshot;
- Proxy GC count and pause-time deltas;
- Proxy CPU profile per mode per case; and
- Proxy heap and allocation profile attribution.

### Available Request Diagnostics (per case)

- effective QueryNode WorkNode identity log;
- Query duration from existing Milvus metrics and logs;
- Proxy request duration; and
- exact logical ID and vector field bytes in the final SDK result.

Query-specific internal application, gRPC payload, connection, TCP, and
reduction counters do not currently exist. This experiment relies on
process-level evidence only: Proxy RSS, Prometheus runtime counters, and pprof
profiles. The report must not substitute Search counters, infer internal Query
bytes from process RSS alone, or claim a retained-memory conclusion from a
single noisy RSS number. Retained-memory interpretation must combine peak RSS,
heap/allocation deltas, and the per-case fan-in scaling trend.

## Aggregation and Reporting

For each case and mode, report the median across four repetitions for QPS, p50,
p95, p99, maximum latency, peak Proxy RSS, and sampled Proxy CPU. Also report
every individual repetition so order effects remain visible.

Report these ratios per case:

```text
Streaming / Batch QPS
Streaming / Batch p95 latency
Streaming / Batch peak Proxy RSS
```

Report the fan-in scaling table:

```text
case | Batch median peak RSS | Streaming median peak RSS | S/B QPS | S/B p95
```

`report.md` must separate verified observations from hypotheses. The primary
questions are whether the Streaming-to-Batch QPS gap grows with fan-in and
whether Streaming peak Proxy RSS scales with Chunk bytes times child count
rather than full-result bytes times child count.

## Exit Criteria

- All 24 timed intervals complete without request errors.
- Every operation returns exactly 8,192 rows.
- Batch and Streaming ordered ID/vector hashes match within every case.
- Result hashes are identical across all six fan-in cases.
- Both balanced repetitions are present for both modes in every case.
- Placement and WorkNode evidence qualifies every measured case.
- Median QPS, p95 latency, peak Proxy RSS, and their Streaming/Batch ratios are
  reported per case.
- Proxy CPU, RSS, allocation, GC, and profile evidence is retained per interval.
- `report.md` clearly separates verified observations from hypotheses.

## Evidence Layout

```text
runs/<run-id>/
  collection.json
  FANIN-N32/
    load.json
    placement-{1,2,3}.json
    placement-summary.json
    placement-validation.log
    worknodes.log
    worknodes-probe.json
    correctness/{batch,streaming}/verify.json
    correctness/comparison.json
    rep1/{batch,streaming}/...
    rep2/{streaming,batch}/...
    profiles/{batch,streaming}/...
    server-logs/
    status.txt
  FANIN-N16/ ... FANIN-N1/ (same layout)
  manifest.tsv
  summary.json
  report-input.json
```

`run_test.sh` must write immutable evidence under a new run ID. `report.md` is
created only after the preserved evidence has been analyzed.

## Out of Scope

- QueryNode fan-in greater than 32;
- client concurrency greater than one;
- Query iterator behavior;
- unlimited Query;
- count, aggregate, group-by, order-by, and element-level Query;
- Streaming Chunk-size sweep;
- static gRPC flow-control windows;
- application-level credit coordination;
- Query-specific internal transport instrumentation; and
- comparison against ANN execution time.
