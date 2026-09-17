# Bounded Plain Query Vector-Payload Fan-In Benchmark Report

## Status

The sweep completed all 24 timed intervals across six fan-in cases and satisfies
every exit criterion in the test plan. Correctness passed at every case with the
same result hash, so the result is fan-in invariant. Evidence:
`milvus_result/test_0917_01/runs/20260917T030022Z-25203/`.

Tracking issue:
[unary_streaming_grpc_benchmark#9](https://github.com/jamesgao-jpg/unary_streaming_grpc_benchmark/issues/9).

## Environment

| Item | Value |
| --- | --- |
| Run ID | `20260917T030022Z-25203` |
| Server | `10.15.9.42` (aarch64) |
| Client | `10.15.2.233` |
| Milvus commit, local and server | `9d3dd3019283df4cfe77a180e4433666f7faaa98` |
| Test-plan commit used by run | `fc37f3774597a83efa8fbce56b53c42dffcdb2fb` |
| Dataset | Cohere 1M, `cohere_1m_qn_fanin`, 1,000,000 rows, 63 sealed segments, 1 vchannel, 1 replica |
| Query | ordinary `query()`, `expr "pk >= 0"`, `limit=8192`, output `pk` + 768-dim float `vector`, Strong consistency |
| Streaming Chunk | 1,024 Units |
| Concurrency | 1 |
| Repetitions | 2, ordered `B,S / S,B` |
| Interval | At least 60 seconds and 30 successful operations |
| Timed intervals | 24 (6 cases x 4) plus 12 CPU-profile intervals, excluded from QPS/latency |

## Qualified Topology

Every case reused the same 63 sealed segments with unchanged segment IDs and row
counts. The Proxy WorkNode log showed exactly the requested QueryNodes and
`streamingNodePresent=false` for every case, and the observed WorkNode IDs
matched the placement owners.

| Case | QueryNodes | Rows per QueryNode, min-max | WorkNodes | Status |
| --- | ---: | ---: | ---: | --- |
| N1 | 1 | 1,000,000 | 1 | PASS |
| N2 | 2 | 495,926-504,074 | 2 | PASS |
| N4 | 4 | 240,000-255,926 | 4 | PASS |
| N8 | 8 | 112,037-127,963 | 8 | PASS |
| N16 | 16 | 48,148-64,074 | 16 | PASS |
| N32 | 32 | 24,074-1,000,000 | 32 | PASS |

Cases ran in descending fan-in order (N32 first) so unexpected high-fan-in
behavior would surface immediately; none did, and the sweep ran to completion.

## Correctness

Every case passed the correctness gate: exactly 8,192 rows, 25,231,360 logical
field bytes, identical ordered IDs and identical ID/vector SHA-256 between Batch
and Streaming, and the same hash across all six cases:

```text
sha256 = 0afdba6012d63db22b2591fa28964fb73f883b5e589f40607729d50e7ef7746b
```

`build_summary` verified every timed interval returned this hash and rejected
any deviation.

## Per-Repetition Results

| Case | Rep | Order | Batch QPS | Streaming QPS | S/B QPS | Batch p95 ms | Streaming p95 ms |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| N1 | 1 | B,S | 2.694 | 2.636 | 0.979x | 394.96 | 406.20 |
| N1 | 2 | S,B | 2.680 | 2.592 | 0.967x | 395.12 | 412.02 |
| N2 | 1 | B,S | 2.679 | 2.656 | 0.991x | 398.45 | 397.19 |
| N2 | 2 | S,B | 2.676 | 2.655 | 0.992x | 402.73 | 401.76 |
| N4 | 1 | B,S | 2.578 | 2.688 | 1.043x | 423.68 | 400.84 |
| N4 | 2 | S,B | 2.602 | 2.687 | 1.033x | 416.37 | 396.98 |
| N8 | 1 | B,S | 2.332 | 2.615 | 1.121x | 473.61 | 407.19 |
| N8 | 2 | S,B | 2.338 | 2.610 | 1.116x | 470.89 | 411.28 |
| N16 | 1 | B,S | 1.823 | 2.402 | 1.318x | 618.92 | 435.63 |
| N16 | 2 | S,B | 1.839 | 2.420 | 1.316x | 617.69 | 435.33 |
| N32 | 1 | B,S | 1.098 | 1.870 | 1.704x | 1052.37 | 554.43 |
| N32 | 2 | S,B | 1.085 | 1.861 | 1.715x | 1045.11 | 560.93 |

## Combined Comparison (median of two repetitions)

| Case | Batch QPS | Streaming QPS | S/B QPS | Batch p95 ms | Streaming p95 ms | S/B p95 | Batch peak RSS MiB | Streaming peak RSS MiB | Batch CPU % | Streaming CPU % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| N1 | 2.687 | 2.614 | 0.973x | 395.04 | 409.11 | 1.036x | 1456.09 | 1344.37 | 9.70 | 11.65 |
| N2 | 2.677 | 2.655 | 0.992x | 400.59 | 399.48 | 0.997x | 2215.66 | 1519.16 | 13.88 | 12.58 |
| N4 | 2.590 | 2.688 | 1.038x | 420.02 | 398.91 | 0.950x | 3213.83 | 1567.71 | 21.35 | 12.40 |
| N8 | 2.335 | 2.612 | 1.119x | 472.25 | 409.24 | 0.867x | 6334.99 | 1743.03 | 38.75 | 16.58 |
| N16 | 1.831 | 2.411 | 1.317x | 618.31 | 435.48 | 0.704x | 8376.66 | 1864.81 | 59.53 | 19.43 |
| N32 | 1.091 | 1.866 | 1.709x | 1048.74 | 557.68 | 0.532x | 16767.94 | 2196.74 | 78.73 | 22.58 |

## Verified Conclusions

- All 24 timed intervals completed with zero request errors, exactly 8,192 rows
  per operation, and the identical result hash at every fan-in level.
- The Streaming-to-Batch QPS gap is fan-in dependent and reverses sign: Streaming
  is 0.973x Batch QPS at N1, becomes faster from N4 onward, and reaches 1.709x
  Batch QPS at N32. Streaming p95 is 1.036x Batch at N1 and 0.532x at N32.
- Batch peak Proxy RSS grows approximately linearly with fan-in: 1,456 MiB at N1
  to 16,768 MiB at N32 (~11.5x for 32x fan-in), consistent with materializing
  full child results times child count in Proxy.
- Streaming peak Proxy RSS remains nearly flat: 1,344 MiB at N1 to 2,197 MiB at
  N32 (~1.6x), consistent with retaining Chunk-sized input times child count.
- Batch Proxy CPU climbs steeply with fan-in (9.7% at N1 to 78.7% at N32);
  Streaming CPU grows modestly (11.7% to 22.6%).
- The N32 CPU profiles attribute Batch Proxy CPU to copying and protobuf-decoding
  the full child result payloads (`runtime.memmove` 20.9%, `consumeFloatSlice`
  14.6%, `ConsumeFixed32` 10.4%, `memclrNoHeapPointers` 6.1%, `SelectMinPK`
  16.6% cumulative). Streaming total sampled CPU was 33.5% of one core versus
  115.0% for Batch over the same 30-second window.
- The N1 numbers reproduce the `test_0916_02` baseline (2.687 vs 2.662 median
  QPS, 395 vs 397 ms p95), cross-validating the harness.
- The two execution-order strata agree closely at every case; no consistent
  Batch-first or Streaming-first bias explains the result.

## Interpretation

This is the first experiment in which Streaming Reduce demonstrates its
fan-in-dependent benefit end-to-end for a real Milvus Query workload. At N1,
Streaming is a slight overhead because it replaces one unary response with
multiple Chunks and has no redundant child result to stop. As fan-in grows,
Batch's Proxy cost scales with complete-result bytes times child count (16.8 GiB
peak RSS at N32), while Streaming's cost scales with Chunk bytes times child
count and stays near 2 GiB. The QPS, latency, RSS, and CPU evidence move
together and monotonically with fan-in.

The observed scaling matches the design's promise:

```text
Batch retained input    ~= full result bytes * child count
Streaming retained input ~= Chunk bytes * child count
```

## Limitations

- This is process-level evidence only (as planned): Proxy peak RSS is
  whole-process, and Query-specific internal reduction/transport counters do not
  exist. The retained-memory conclusion is supported by the RSS trend, CPU
  divergence, and pprof attribution, not by a single RSS number.
- Each interval measured at least 60 seconds, but at N32 Batch the binding
  constraint produced only 66 operations per interval; the median QPS difference
  at N32 is therefore based on ~66-operation windows.
- One dataset, one topK-equivalent limit (8,192), one Chunk size (1,024 Units),
  one concurrency (1), and one server/client pair. Chunk-size, concurrency,
  dataset, and index effects are not isolated.
- QueryNode ANN/Query work still materializes each local result before Chunk
  transmission, so this measures the current M1A transport and Proxy reduction
  path rather than stateful segment-level streaming.
- Peak RSS does not distinguish retained reduction buffers from transient
  materialization; the flat Streaming RSS is nonetheless consistent with bounded
  retention across a 16 GiB input span.

## Evidence

Raw evidence: `milvus_result/test_0917_01/runs/20260917T030022Z-25203/`

Generated summaries: `manifest.tsv`, `summary.json`, and `report-input.json` in
that run directory. Per-case placement, WorkNode, correctness, profile, process
sample, Prometheus, and log evidence is preserved under each `FANIN-N*`
subdirectory.
