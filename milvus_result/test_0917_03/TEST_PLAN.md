# Client Concurrency Sweep for Bounded Plain Query Batch vs Streaming

## Status

Proposed. The experiment has not been executed.

Tracking issue:
[unary_streaming_grpc_benchmark#11](https://github.com/jamesgao-jpg/unary_streaming_grpc_benchmark/issues/11).

## Objective

Sweep client concurrency at fixed QueryNode fan-in and the best Chunk size from
`test_0917_02`, and compare Batch against Streaming under overlapping requests.

`test_0917_01` (issue #9) established that Streaming/Batch QPS grows with
fan-in, and `test_0917_02` (issue #10) established that Chunk size 256 offers
the best QPS and lowest peak Proxy RSS at N16. Both experiments ran at client
concurrency 1, so the behavior under overlapping requests and saturation was
not measured.

## Experimental Interpretation

Concurrency is the last fixed dimension in the primary matrix. It tests the
bounded-memory claim under load:

```text
Batch retained input    ~= full result bytes * child count * concurrency
Streaming retained input ~= Chunk bytes * child count * concurrency
```

If Streaming's advantage is primarily bounded Proxy intermediate memory, it
should retain less memory per in-flight request and degrade later as
concurrency rises. The experiment identifies the concurrency at which each mode
saturates, becomes unstable, or fails.

## Fixed Topology and Chunk Size

| Component | Count |
| --- | ---: |
| Proxy | 1 |
| MixCoord | 1 |
| DataNode, including index service | 1 |
| StreamingNode | 1 |
| QueryNode | 16 |

- One vchannel, 63 sealed segments, one replica.
- Streaming Chunk size: 256 Units (best QPS and lowest peak RSS at N16 in
  `test_0917_02`).
- Placement and WorkNode identity must be qualified before measurement.

## Concurrency Sweep

| Concurrency | Expected in-flight requests |
| ---: | ---: |
| 1 | 1 |
| 2 | 2 |
| 4 | 4 |
| 8 | 8 |
| 16 | 16 |
| 32 | 32 |

Sweep order is ascending within the case. Every concurrency level is measured
in both Batch and Streaming mode with two balanced-order repetitions (rep 1
Batch then Streaming; rep 2 Streaming then Batch). The 32-level is expected to
push Proxy toward saturation at N16 given the concurrency-1 throughput
(approximately 1.8 QPS Batch / 2.4 QPS Streaming). Saturation is identified by
a QPS ceiling and latency growth rather than request errors: the client request
timeout is 600 seconds, so timed intervals are expected to complete with zero
errors at every level, and `build_summary` rejects any interval with errors, as
in the prior experiments.

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
| Streaming Chunk size | 256 Units |
| Warm-up operations per client thread per interval | 5 |
| Minimum timed operations per interval (aggregate) | 30 |
| Minimum timed duration per interval | 60 seconds |
| Repetitions per mode per concurrency | 2 |

The PyMilvus connection is process-wide and shared by the concurrent client
threads. Each operation issues one bounded ordinary Query and consumes the
complete SDK response. The client records end-to-end latency per operation in
nanoseconds.

## A/B Modes

| Mode | Proxy configuration |
| --- | --- |
| Batch | `proxy.queryView.enableQueryStreaming=false` |
| Streaming | `proxy.queryView.enableQueryStreaming=true`; `proxy.queryView.queryStreamChunkSize=256` |

Use the same Milvus binary in both modes. Restart only Proxy when switching the
mode, wait for health, reconnect PyMilvus, and keep the QueryNodes and loaded
collection unchanged.

## Correctness Gate

Before running timed intervals:

1. Verify the reusable collection has 1,000,000 rows, 63 sealed segments, and
   no growing segments.
2. Run the identical Query once in Batch and once in Streaming mode at
   concurrency 1.
3. Require exactly 8,192 rows from each verification.
4. Require IDs to be ordered identically between modes.
5. Hash each row as the primary key followed by the exact float32 vector bytes.
6. Require the ordered whole-result hash to match between modes.
7. Preserve the Proxy environment for both modes.

Do not run performance intervals if any correctness assertion fails.

## Procedure

1. Record local, GitHub, `.42`, and `.233` source states and machine inventory.
2. Verify no unrelated resource-intensive process or container is running.
3. Build Milvus once on `.42` and verify the binary exists.
4. Start the existing dependency services and fixed Milvus roles on `.42`.
5. Start 16 QueryNodes, load the reusable collection, and qualify placement and
   WorkNode identity.
6. Execute the correctness gate.
7. For each concurrency level in 1, 2, 4, 8, 16, 32:
   a. Restart Proxy in Batch mode, run rep 1 Batch then rep 1 Streaming.
   b. Restart Proxy in Streaming mode for rep 2, run rep 2 Streaming then rep 2
      Batch (balanced order).
   c. Before each interval, wait for health from `.233`.
   d. Run five excluded warm-up operations per client thread.
   e. Measure until both 60 seconds and 30 aggregate successful operations are
      reached.
   f. Sample Proxy CPU and RSS once per second during the timed interval.
   g. Capture Proxy Prometheus snapshots before and after every interval.
   h. Capture Proxy heap and cumulative allocation profiles before and after
      every interval.
   i. Preserve configurations, client output, process samples, profiles, logs,
      placement, and exit status.
8. Run separate 30-second CPU-profile intervals: Batch and Streaming at
   concurrency 1 and concurrency 32.
9. Stop experiment processes without deleting the reusable collection data.

## Metrics

### Primary

- successful operations and errors per interval;
- aggregate QPS across concurrent clients;
- p50, p95, p99, and maximum end-to-end Query latency;
- result row count (8,192) and logical field bytes per operation;
- ordered ID/vector result hash per interval;
- Streaming QPS / Batch QPS and Streaming p95 / Batch p95 per concurrency; and
- median peak Proxy RSS per mode per concurrency.

### Process Diagnostics (per interval)

- one-second Proxy CPU and RSS samples;
- Proxy Go heap and cumulative allocation deltas from the Prometheus snapshot;
- Proxy GC count and pause-time deltas;
- Proxy CPU profile (Batch and Streaming at concurrency 1 and 32); and
- Proxy heap and allocation profile attribution.

### Available Request Diagnostics

- effective QueryNode WorkNode identity log;
- Query duration from existing Milvus metrics and logs;
- Proxy request duration; and
- exact logical ID and vector field bytes in the final SDK result.

Query-specific internal application, gRPC payload, connection, TCP, and
reduction counters do not currently exist. This experiment relies on
process-level evidence only, as in `test_0917_01` and `test_0917_02`.

## Aggregation and Reporting

For each concurrency level and mode, report the median across two repetitions
for QPS, p50, p95, p99, maximum latency, peak Proxy RSS, and sampled Proxy CPU.
Also report every individual repetition so order effects remain visible.

Report these per concurrency level:

```text
Streaming / Batch QPS
Streaming / Batch p95 latency
Streaming / Batch peak Proxy RSS
```

Report the saturation table:

```text
concurrency | Batch QPS | Streaming QPS | S/B QPS | Batch p95 | Streaming p95 | S/B p95 | Batch peak RSS | Streaming peak RSS
```

`report.md` must separate verified observations from hypotheses. The primary
questions are whether Streaming retains less memory per in-flight request and
at which concurrency each mode saturates or becomes unstable.

## Exit Criteria

- All timed intervals complete without request errors at every concurrency
  level.
- Every operation returns exactly 8,192 rows.
- Batch and Streaming ordered ID/vector hashes match.
- Result hashes are identical across all concurrency levels.
- Placement and WorkNode evidence qualifies the measured case.
- Median QPS, p95 latency, peak Proxy RSS, and their Streaming/Batch ratios are
  reported per concurrency level.
- The concurrency at which each mode saturates or becomes unstable is
  identified and reported.
- Proxy CPU, RSS, allocation, GC, and profile evidence is retained per interval.
- `report.md` clearly separates verified observations from hypotheses.

## Evidence Layout

```text
runs/<run-id>/
  collection.json
  placement-{1,2,3}.json
  placement-summary.json
  placement-validation.log
  worknodes.log
  worknodes-probe.json
  correctness/{batch,streaming}/verify.json
  correctness/comparison.json
  conc1/rep1/{batch,streaming}/...
  conc1/rep2/{streaming,batch}/...
  conc2/... conc4/... conc8/... conc16/... conc32/... (same layout)
  profiles/{batch,streaming}-conc{1,32}/...
  server-logs/
  manifest.tsv
  summary.json
  report-input.json
```

`run_test.sh` must write immutable evidence under a new run ID. `report.md` is
created only after the preserved evidence has been analyzed.

## Out of Scope

- QueryNode fan-in other than 16;
- Streaming Chunk sizes other than 256;
- client concurrency greater than 32;
- Query iterator behavior;
- unlimited Query;
- count, aggregate, group-by, order-by, and element-level Query;
- static gRPC flow-control windows (standalone benchmark);
- application-level credit coordination;
- Query-specific internal transport instrumentation; and
- comparison against ANN execution time.
