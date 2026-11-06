# Streaming Reduce Performance Evidence

> VERIFIED: In the historical 10M-row Milvus experiment, wide scalar projections improved both Query and Search throughput, while small outputs and several grouped/filter workloads showed little benefit or regressions; these measurements do not establish performance of the current master-based PR.

Prepared on 2026-11-06 for [Milvus PR #55219](https://github.com/milvus-io/milvus/pull/55219), which ports the earlier implementation in [chyezh/milvus#65](https://github.com/chyezh/milvus/pull/65). This is an analysis of completed experiments, not a new benchmark run. The [complete appendix](appendix.md) includes every successful matched configuration, not only favorable examples.

## Test

| Item | Value |
|---|---|
| Question | When does worker-to-Proxy streaming improve or regress end-to-end performance compared with Batch? |
| Primary experiment | `test_1029_06`, run `20261029T122650Z-matrix-resume`; 326 matched cells, 652 timed intervals, 15 semantic families; one additional cell blocked in Batch |
| Milvus measured | `9b6b87c8716ce24bf7e1d79b58c7bd8e3a8a58ff`, reported version `unknown-20261028-9b6b87c8`; historical qv deployment |
| VDBBench | `139f8a3d38fee6d3e8bd62ffbd8ea652b6172108` — `feat: add CLI control for Milvus force-merge size and toggle (#871)` |
| Harness / tree | `codex/reducestream-functional-cases`, modified upstream checkout; [compressed working-tree patch](evidence/vdbbench-working-tree.patch.gz) reconstructs all 12 VDBBench files matching the recorded source hashes; external driver hashes are in [source manifest](evidence/source.json) |
| SDK | Isolated `3.1.0rc69+reducestream2`, with forwarding of the historical request-level streaming controls |
| Dataset | 10,000,000 logical rows; 768D float vectors; wide scalar fields. Auxiliary vector/struct fixtures are populated on only 4,096 rows |
| Deployment | Kubernetes; user-reported `perf, 16 CU` service tier; 1 Proxy, 4 QueryNodes, 1 StreamingNode, 1 MixCoord; 3 vchannels, 1 replica; separate client machine |
| Effective fan-in evidence | Baseline traces record 12 child RPC tasks for ordinary requests and 24 for two-branch Hybrid requests; iterator counts are per page. This is not proof of per-vchannel WorkNode placement or routing stability during timing |
| Index / Search | HNSW, M=16, efConstruction=200, ef=8192, COSINE; NQ=1; one frozen query vector; Strong consistency |
| Chunk threshold | 262,144 bytes (256 KiB), fixed across this matrix; a reducible-payload threshold, not a strict wire-message or heap bound |
| Workloads | Limit/TopK 512, 1,024, 8,192 where valid; iterator partial-page variant 1,025; concurrency 1, 8, 32. One-factor sweeps, not a full Cartesian product |
| Baseline | Same server, collection and logical request; Batch followed by Streaming for each cell |
| Timing | One 45-second interval per mode per cell. Full result comparisons before/after timing, not in the measured request loop |
| Metrics | Native VDBBench QPS and client latency percentiles; direct Proxy `process_resident_memory_bytes` polling approximately once per second |

The ordinary Query payload sweep uses `i64_timestamp >= 1700000000000`, which matches this fixture; Search uses the same frozen query across projections. Search returns scores as well as the listed fields. QPS at NQ=1 means completed logical operations/s; iterator operations include all pages, not just one `Next()` call. Latency is client-observed end-to-end completion latency, not a reduction-stage timer.

**Revision boundary:** The current PR uses refreshable server-side controls and has later changes absent from this deployed revision. It has not been performance-tested by this matrix. Historical SDK overrides must not be used to reproduce the A/B switch on current master; use `proxy.queryView.enableReduceStream` and `proxy.queryView.reduceStreamChunkBytes` as documented in the [current PR controls](https://github.com/milvus-io/milvus/pull/55219#controls). The deployed qv revision also predates later grouped partial-state adaptations. These results support the motivation, not a claim of current-head performance parity.

## Results

### Controlled Output-Payload Sweep

Fixed limit/TopK=1,024, concurrency=1 and NQ=1. Only output projection changes within each operation. Each QPS ratio is one matched pair, not an average over unrelated semantics. Higher Streaming/Batch QPS is better.

| Projection | Returned fields | Approx. logical bytes/row | Query Batch QPS | Query Streaming QPS | Query S/B | Search Batch QPS | Search Streaming QPS | Search S/B |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| P0 | PK only | 8 | 163.69 | 163.86 | 1.001x | 38.73 | 38.20 | 0.986x |
| P1 | PK + six numeric/Boolean fields | 28 | 121.98 | 115.06 | 0.943x | 28.43 | 28.14 | 0.990x |
| P2 | PK + title + vc_desc | 324 | 67.78 | 75.03 | 1.107x | 30.82 | 32.82 | 1.065x |
| P3 | P2 + content | 1,959 | 18.07 | 34.96 | 1.934x | 12.46 | 22.56 | 1.811x |
| P4 | All 15 scalar fields, including PK | 2,529 | 14.66 | 27.50 | 1.877x | 7.19 | 10.36 | 1.441x |

P1 adds `i8_status`, `i16_category`, `i32_region`, `f32_score`, `f64_price`, `bool_active`. P4 additionally includes `title`, `content`, `vc_uuid`, `vc_tag`, `vc_country`, `vc_email_domain`, `vc_desc`, `bluesky_json`. Widths use fixed numeric widths and UTF-8 averages from the first 5M source rows, counting NULL as zero: they are not measured protobuf bytes, transferred bytes, or heap occupancy. Search score/framing bytes are excluded. The [field profile](evidence/field-profile.json) records the sizing basis.

| Projection | Query Batch p95 ms | Query Streaming p95 ms | Search Batch p95 ms | Search Streaming p95 ms |
|---|---:|---:|---:|---:|
| P0 | 6.44 | 6.56 | 36.30 | 35.57 |
| P1 | 8.56 | 9.19 | 35.65 | 35.93 |
| P2 | 15.81 | 14.39 | 33.92 | 31.31 |
| P3 | 66.50 | 30.96 | 89.43 | 50.26 |
| P4 | 81.20 | 39.11 | 148.46 | 102.86 |

VERIFIED: These controlled measurements show gains for Search as well as Query. They do not show that every extra output column helps: P1 remains small and does not improve throughput. P3/P4 ratios are not monotonic with row width.

### Large Limits and Concurrency

P4 throughout this table. The K=1,024/C=1 row is the same measurement as the payload sweep, not an extra repetition. Row identifiers link to the full paired evidence in the appendix.

| Operation | Cell | Limit/K | C | Batch QPS | Streaming QPS | S/B | Batch p95 ms | Streaming p95 ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Query | 002 | 512 | 1 | 27.68 | 42.72 | 1.543x | 40.16 | 24.97 |
| Query | 323 | 1,024 | 1 | 14.66 | 27.50 | 1.877x | 81.20 | 39.11 |
| Query | 003 | 8,192 | 1 | 1.9916 | 4.7038 | 2.362x | 531.55 | 226.14 |
| Query | 004 | 1,024 | 8 | 33.85 | 81.74 | 2.415x | 310.48 | 136.71 |
| Query | 005 | 1,024 | 32 | 63.77 | 90.45 | 1.418x | 710.18 | 499.45 |
| Search | 065 | 512 | 1 | 11.48 | 14.06 | 1.225x | 104.17 | 91.10 |
| Search | 327 | 1,024 | 1 | 7.19 | 10.36 | 1.441x | 148.46 | 102.86 |
| Search | 066 | 8,192 | 1 | 0.5196 | 0.6246 | 1.202x | 1,974.67 | 1,641.40 |
| Search | 067 | 1,024 | 8 | 26.91 | 30.42 | 1.130x | 396.00 | 358.57 |
| Search | 068 | 1,024 | 32 | 29.76 | 36.26 | 1.218x | 2,110.20 | 1,285.76 |

VERIFIED: Wide Query benefits are substantial here, but neither operation has a monotonic ratio versus concurrency. These are NQ=1 observations, not evidence for the reviewer's large-NQ Search scenario. The slowest cells complete relatively few operations in 45 seconds, making tail percentiles particularly sensitive to sample count.

### Drawbacks and Neutral Cases

The lowest observed QPS ratio among all 326 cells is 0.911x. The examples below intentionally include regressions, near parity, and a tail-latency drawback that a QPS-only report would hide. They are single-run observations, not significance-tested regression estimates.

| Workload | Cell | Limit/K | C | Batch QPS | Streaming QPS | S/B | Batch p95 ms | Streaming p95 ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Query composite Group By | 060 | 512 | 1 | 116.75 | 106.32 | 0.911x | 12.61 | 14.90 |
| Filtered Query, PK only | 015 | 1,024 | 32 | 1,355.80 | 1,252.12 | 0.924x | 45.16 | 44.70 |
| Query grouped MAX | 054 | 1,024 | 1 | 16.37 | 15.22 | 0.929x | 73.71 | 80.12 |
| Iterative-filter Search | 166 | 512 | 1 | 36.73 | 34.40 | 0.937x | 27.86 | 34.30 |
| Query P1, small scalars | 320 | 1,024 | 1 | 121.98 | 115.06 | 0.943x | 8.56 | 9.19 |
| Query, PK + vector | 024 | 1,024 | 8 | 47.00 | 45.36 | 0.965x | 231.80 | 329.36 |
| Search, PK only | 064 | 1,024 | 1 | 38.73 | 38.20 | 0.986x | 36.30 | 35.57 |
| Search, vector/requery | 086 | 8,192 | 1 | 2.1816 | 2.1916 | 1.005x | 507.26 | 497.17 |

The vector/requery case is not equivalent to transporting vectors in the initial Search candidate stream. The [implementation retains unary requery](https://github.com/milvus-io/milvus/pull/55219#execution-and-ownership). A large final SDK response alone does not establish a large reducible child payload. Worker Search and segment reduction also remain Batch-based; this work does not remove their full computation/materialization.

### Proxy Memory

The metric is the sampled peak of the whole Proxy process RSS, **not peak Go heap, retained-input bytes, incremental per-request memory, or a hard bound**. Lower Streaming/Batch RSS is better. Start-of-interval RSS differs between modes; original baselines and samples are retained in [RSS evidence](evidence/rss).

| Query projection | Cell | Limit | C | Batch peak MiB | Streaming peak MiB | RSS S/B | Change |
|---|---|---:|---:|---:|---:|---:|---:|
| P0, PK only | 001 | 1,024 | 1 | 1,076.6 | 680.6 | 0.632x | -36.8% |
| P4, all scalars | 002 | 512 | 1 | 984.7 | 833.4 | 0.846x | -15.4% |
| P4, all scalars | 003 | 8,192 | 1 | 3,120.3 | 2,122.2 | 0.680x | -32.0% |
| P4, all scalars | 004 | 1,024 | 8 | 1,969.3 | 1,914.7 | 0.972x | -2.8% |
| P4, all scalars | 005 | 1,024 | 32 | 3,122.5 | 2,323.3 | 0.744x | -25.6% |
| PK + vector | 023 | 8,192 | 1 | 2,904.6 | 3,401.2 | 1.171x | +17.1% |

The last row is an important counterexample: vector-output Query reached 2.109x QPS, but Streaming's sampled RSS was higher. This does not identify the cause, but rules out a universal process-memory reduction claim.

Of 652 intervals, 53 have available RSS, three have partial samples and 596 are unavailable. There are only 26 complete A/B RSS pairs, with one additional unpaired available interval. The metrics endpoint became unavailable during the run; unavailable values are not zero and partial samples are not treated as complete peaks. Ordinary Search's wide-field timing cells have no complete RSS pairs. One-second sampling can miss short peaks, and its window includes concurrent-runner startup/shutdown. RSS is therefore supporting evidence, not a substitute for the requested peak-heap measurement.

## Findings

- VERIFIED: The controlled wide-text projection improves Query QPS by 1.934x and Search QPS by 1.811x, with lower p95 latency in both. The gain is not limited to Query.
- VERIFIED: Wide Query reaches 2.362x QPS at limit 8,192 and 2.415x at concurrency 8; higher concurrency does not guarantee a larger advantage.
- VERIFIED: Small outputs are mostly near parity or slower, and grouped/filter cases include observed QPS reductions of roughly 6-9%. Query vector output at C=8 has a 42.1% higher p95 despite only a 3.5% QPS reduction.
- VERIFIED: Available wide-scalar Query RSS pairs are lower for Streaming, but vector-output Query also provides a higher-RSS counterexample. Peak heap and worker CPU were not measured alongside this matrix.
- LIKELY: Avoiding surplus transfer and Proxy materialization helps wide-result workloads, while chunk processing adds costs. The end-to-end data does not isolate saved bytes, worker encoding cost, or reduction CPU as the cause of each result.

## Caveats

1. **Historical revision, not current PR head.** This is evidence from the predecessor qv implementation. Current master, newer grouped aggregation contracts, retry behavior, configuration changes and fixes need their own live verification. It is not a performance comparison with current official master.
2. **One interval, fixed order.** Every cell ran Batch first and Streaming second for 45 seconds. Cache/order effects, temporal drift and run-to-run variance are not quantified. No confidence interval or statistical significance is claimed.
3. **Limited workload diversity.** NQ=1 and one frozen query vector do not represent a production query distribution. The matrix varies selected limits/concurrency/projections, not all combinations. It does not test large NQ, many-shard scaling or a Chunk-size sweep in this deployment.
4. **Coverage is not universal.** Element-vector Search and corresponding Hybrid variants were excluded after Batch-side failures. Composite Search Group By has no timed comparison. Query/aggregation limits can produce fewer rows/groups than requested; iterator limits refer to complete iteration, and Search aggregation uses bucket-size semantics rather than ordinary ANN TopK. Auxiliary fixtures have only 4,096 populated rows. ANN recall was not measured; zero recall fields in result JSON are placeholders, not measured recall.
5. **Resource attribution is incomplete.** `perf, 16 CU` is the supplied service tier, not a verified per-pod CPU/RAM allocation. Total RPC counts do not establish stable per-vchannel placement. No worker-CPU, allocation, peak-heap or request-attributed memory claim follows from these intervals.
6. **Reproduction has an archive gap.** The VDBBench base and all 12 recorded VDBBench modifications are reconstructable from the published patch and hashes. Several external-driver versions and the private dataset/query inputs remain outside this bundle. The historical client's source archive could not be retrieved because SSH to that machine timed out during preparation of this report. Published results are auditable measurements, not a complete one-command reproduction package.

## Details

### Breadth Across Semantics

These medians and ranges summarize **matched per-cell QPS ratios**, not pooled throughput. Different output sizes, limits, empty/nonempty cases and concurrency are mixed within a family, so this table is a coverage/dispersion summary, not a payload-effect estimate.

| Family | Matched cells | Median S/B QPS | Minimum | Maximum |
|---|---:|---:|---:|---:|
| Plain Query | 29 | 1.107x | 0.924x | 2.415x |
| Query iterator | 8 | 1.013x | 0.986x | 1.272x |
| Element Query / iterator | 10 | 1.008x | 0.988x | 1.222x |
| Query Group By | 30 | 0.987x | 0.911x | 1.065x |
| Plain ANN Search | 29 | 0.990x | 0.949x | 1.811x |
| Search iterator | 8 | 1.000x | 0.950x | 1.001x |
| Embedding-list Search | 5 | 0.962x | 0.952x | 0.969x |
| Range Search | 15 | 0.986x | 0.938x | 1.067x |
| Iterative-filter Search | 10 | 1.002x | 0.937x | 1.018x |
| Search ORDER BY | 20 | 0.985x | 0.945x | 1.006x |
| Search Group By | 10 | 1.000x | 0.993x | 1.009x |
| Search aggregation | 32 | 1.000x | 0.981x | 1.017x |
| Hybrid ANN | 30 | 0.988x | 0.954x | 1.072x |
| Hybrid range / iterative filter | 60 | 0.993x | 0.950x | 1.110x |
| Hybrid embedding-list | 30 | 0.984x | 0.946x | 1.014x |

### Synthetic gRPC Experiments: Separate Evidence

The earlier bare-bones Go benchmark isolates transport/application effects and is not a Milvus end-to-end result. Both experiments below use eight child processes on one host over loopback TCP, 256 KiB Chunks, and four balanced-order repetitions. Values are combined medians.

| Workflow | Payload/child | Required output | Concurrency | Unary QPS | Streaming QPS | S/U |
|---|---:|---:|---:|---:|---:|---:|
| Ordered topK, early stop | 16 MiB | 16 MiB global | 1 | 70.96 | 91.78 | 1.29x |
| Ordered topK, early stop | 16 MiB | 16 MiB global | 64 | 89.38 | 366.84 | 4.10x |
| Full transfer | 16 MiB | All 128 MiB | 1 | 80.53 | 66.66 | 0.83x |
| Full transfer | 1 MiB | All 8 MiB | 64 | 1,413.40 | 999.57 | 0.71x |

The [early-stop report](../../result/test_0831_01/report.md) and [full-transfer report](../../result/test_0831_02/report.md) contain complete matrices, latency, transport counters and order-stratified medians. They show why streaming is not inherently faster: sending all bytes in multiple messages can cost more, whereas avoiding child tails can outweigh that cost. The two workflows differ in ordered-reduction work as well as transfer volume; this is not a pure framing-cost subtraction. Mode order materially affects several synthetic cases, and loopback excludes real network behavior. Cumulative transport byte gaps do not locate live memory in kernel/gRPC buffers.

### What Remains for the Review

The [reviewer's performance request](https://github.com/milvus-io/milvus/pull/55219#pullrequestreview-5798108938) is only partially answered: we have historical end-to-end throughput/latency and sampled RSS, including Search gains and drawbacks. We do not have current-head peak heap, matched worker CPU per operation, large-NQ Search, or a current-head many-shard sweep. In particular, the [worker split-cost concern](https://github.com/milvus-io/milvus/pull/55219#discussion_r4490866019) is not dismissed by the wide-payload gains; a one-Chunk fast path and its worker-side benefit need measurement, not inference from these tables. This report does not mark that review thread resolved or claim the proposed optimization is implemented.

## Artifacts

- [All 326 paired configurations](appendix.md), linking both redacted result JSONs per cell; the original VDBBench schema and all numerical measurements are retained. p50/p95/p99 and average latency are stored in seconds in JSON; tables convert to milliseconds.
- Example wide Query: [Batch result](evidence/results/result_20261029_003-Q01-A-k8192-c1-r1-capture_milvus.json), [Streaming result](evidence/results/result_20261029_003-Q01-A-k8192-c1-r1-verify_milvus.json).
- Example wide Search: [Batch result](evidence/results/result_20261029_326-S01-A-k1024-c1-r1-capture_milvus.json), [Streaming result](evidence/results/result_20261029_326-S01-A-k1024-c1-r1-verify_milvus.json).
- [Publication provenance](evidence/): `manifest.json` records original/public SHA-256 and exact redaction paths; `source.json` records harness/SDK hashes; `vdbbench-working-tree.patch.gz` preserves the VDBBench modifications verbatim, including original whitespace; `field-profile.json` records logical field sizes. [RSS status and samples](evidence/rss) retain complete, partial and unavailable intervals.
- [Exporter and verification](export_evidence.py): `python3 export_evidence.py` verifies this published bundle. Exporting from the private run directory is optional; originals are never modified. Connection credentials/addresses, local reference paths, embedded environment notes and expected row contents are removed from public copies. Reference and route-evidence hashes remain, but raw route traces and full comparison outputs are not redistributed here.

Verification expectation: `PASS: 652 result files; 326 matched pairs; unchanged published hashes; 26 complete RSS pairs; appendix arithmetic.` The hashes establish file identity, not independent correctness of the benchmark implementation. Original measurements remain unchanged locally.
