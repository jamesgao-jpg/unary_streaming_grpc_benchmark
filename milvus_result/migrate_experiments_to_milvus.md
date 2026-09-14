# Migrate gRPC Experiments to Milvus

This is an inventory of executed benchmark setups, not a claim that Milvus has
reproduced them. Each linked `TEST_PLAN.md` defines the benchmark case IDs and
procedure; generated YAML and manifests record what actually ran. The Milvus
translation table states what an end-to-end test must preserve or explicitly change.
The executed setups below are historical records, not Milvus test candidates.

## Common Benchmark Boundary

| Item | Executed setup |
| --- | --- |
| Topology | Parent and child processes on `10.15.9.42`, using loopback TCP and one logical gRPC `ClientConn` with child-address routing. One operation opens one RPC/stream per child. |
| Modes | Unary sends one complete response per child. Streaming sends Chunks over a bidirectional stream. `ordered_topk` stops after global topK; `full_transfer` consumes every child result. |
| Units | `ordered_topk` uses synthetic 256-byte Units. `stream_chunk_bytes` limits synthetic payload bytes, not serialized protobuf bytes. |
| gRPC | Generated YAML is authoritative for each run. Representative settings: compression and internal TLS disabled, client receive limit 64 MiB, and dynamic parent receive window unless a case sets `static_window_bytes`. |
| Evidence | Run manifests, source commit/status/diff, generated YAML, environment, and raw logs are in each timestamped run directory. Completed executions are not necessarily committed artifacts. |

## Executed Setups

| Experiment | Workflow and fixed setup | Varied cases (sweeps, not a Cartesian product unless stated) | Measurement and mode order | Recorded run |
| --- | --- | --- | --- | --- |
| [0820_01](../result/test_0820_01/TEST_PLAN.md) | `full_transfer`; no reduction. One shared channel. | Chunk 16 KiB-16 MiB at N8/P16 MiB/K1; N1-32 at fixed **32 MiB aggregate** and C256 KiB; P256 KiB-32 MiB at N8/C256 KiB; K1/4/16/32/64 at N8/P1 MiB/C256 KiB. Separate P60 MiB valid and P64 MiB rejected message-limit checks. | 5 repetitions, odd Unary-first/even Streaming-first; 20 warm-ups, at least 200 operations and 20 s per mode. | [20260820T033454Z manifest](../result/test_0820_01/results/20260820T033454Z/manifest.tsv): 115 passed, 1 expected rejection; source `2e14cac` with preserved dirty-tree patch. |
| [0826_01](../result/test_0826_01/TEST_PLAN.md) | `ordered_topk`; N8, U256 B, K1; both `interleaved` and `dominant_child` rank distributions; P = topK x U. | TopK 512/1,024/4,096/16,384/65,536 Units at C256 KiB; C16/64/256 KiB, 1/4 MiB at topK16,384; `full_transfer` controls P1/4/16 MiB. | 5 alternating-order repetitions; 20 warm-ups, at least 200 operations and 20 s per mode. | [20260826T081854Z manifest](../result/test_0826_01/runs/20260826T081854Z/manifest.tsv): 115 passed; source `a360a0c`. |
| [0827_01](../result/test_0827_01/TEST_PLAN.md) | `ordered_topk`; U256 B, global topK4,096 Units/1 MiB, C256 KiB, `interleaved`, K1; tests transport run-ahead rather than equal child/global topK. | N2 or N8 crossed with P4/16/32/60 MiB per child. | 3 Streaming-first repetitions; 3 warm-ups, at least 10 operations and 1 s per mode. | [20260827T062400Z manifest](../result/test_0827_01/runs/20260827T062400Z/manifest.tsv): 24 passed; source `29a7703`. |
| [0827_02](../result/test_0827_02/TEST_PLAN.md) | `ordered_topk`; U256 B, C256 KiB, `interleaved`, K1; **each child payload equals final topK payload**. | N2 or N8 crossed with P4/16/32/60 MiB; topK = P / 256 B. | 4 repetitions, 1/3 Unary-first and 2/4 Streaming-first; 3 warm-ups, at least 10 operations and 1 s per mode. | [20260827T074104Z manifest](../result/test_0827_02/runs/20260827T074104Z/manifest.tsv): 32 passed; source `bf87f64`. |
| [0827_03](../result/test_0827_03/TEST_PLAN.md) | `ordered_topk`; U256 B, `interleaved`, K1, child payload = final topK payload; baseline N8/P16 MiB/C256 KiB/dynamic window. | Phase A: 19 single-factor cases across N1/2/4/8/16/32, P1/4/16/32/60 MiB, C16/64/256 KiB/1/4 MiB, dynamic or static 64/256 KiB/1/4/16 MiB. Phase B: 27 dynamic-window N2/8/32 x P4/16/60 MiB x C64/256 KiB/1 MiB. Phase C: 21 selected N, P, C interactions with static 64 KiB/1/16 MiB. | 4 balanced-order repetitions per case; 3 warm-ups, at least 10 operations and 1 s per mode. | Phase [A](../result/test_0827_03/runs/phase_a_20260827T125230Z/manifest.tsv)/[B](../result/test_0827_03/runs/phase_b_20260827T130023Z/manifest.tsv)/[C](../result/test_0827_03/runs/phase_c_20260827T130630Z/manifest.tsv): 76/108/84 passed; source `62d6854`. Phase artifacts are **local and untracked** in this checkout. |
| [0831_01](../result/test_0831_01/TEST_PLAN.md) | `ordered_topk`; N8, P16 MiB/child, topK65,536 Units/16 MiB, U256 B, C256 KiB, `interleaved`, dynamic window. | Concurrency K1/2/4/8/16/32/64; up to 512 active child RPCs. | 4 balanced-order repetitions; 20 warm-ups, at least 200 operations and 20 s per mode; resource gate before K64. | [20260831T084746Z manifest](../result/test_0831_01/runs/20260831T084746Z/manifest.tsv): 28 passed; source `d03c62b`. |
| [0831_02](../result/test_0831_02/TEST_PLAN.md) | `full_transfer`; no ordered merge or early stop; dynamic window. | C16/64/256 KiB/1/4/16 MiB at N8/P16 MiB/K1; P1/4/16/32/60 MiB at N8/C256 KiB/K1; N1/2/4/8/16/32 at **fixed P16 MiB per child**; K1/2/4/8/16/32/64 at **N8/P1 MiB/C256 KiB**. | 4 balanced-order repetitions; 3 warm-ups, at least 200 operations and 30 s per mode; resource gate before K64. | [20260831T094849Z manifest](../result/test_0831_02/runs/20260831T094849Z/manifest.tsv): 84 passed; source `d03c62b`. |

## Milvus End-to-End Translation

| Benchmark control | Required Milvus observation or constraint |
| --- | --- |
| Unary versus Streaming | Use the **same iterator request**, collection, query vector, result fields, index, and Milvus binary. Change only the server-side Batch/Streaming switch. A non-iterator Search is not the same A/B request. |
| N children | Record configured QN/SN and vchannel counts **and** observed `GetQueryPlan.WorkNodes`/child streams per vchannel. A node count alone does not prove fan-in. For the closest one-level comparison, isolate one vchannel; still record Proxy's final reduction level. |
| P bytes per child and U256 B | Use the same requested topK for every child and the final result. Treat benchmark P = topK x 256 B as a nominal comparison label, not a Milvus payload target; do not calibrate actual protobuf bytes for this matrix. |
| Global topK | Use identical topK in both modes. Normal Milvus Search defaults to a maximum of 16,384 results per request; verify `quotaAndLimits.limits.topK` on the test deployment. Do not migrate larger benchmark topK cases into this M1A matrix. See [limit configuration](../../milvus-qv/pkg/util/paramtable/quota_param.go) and [validation](../../milvus-qv/internal/proxy/util.go). |
| C bytes per Chunk | `proxy.queryView.searchStreamChunkSize` already sets maximum Units per Search Chunk (default 1,024); no new Chunk-size knob is needed. Map benchmark C to Milvus Units/Chunk as C / 256 B, without adjusting for serialized protobuf size. See [configuration](../../milvus-qv/pkg/util/paramtable/component_param.go) and [server split](../../milvus-qv/internal/views/viewquery/server.go). |
| Rank distribution | Verify which child owns each rank. `interleaved` and `dominant_child` require controlled data placement and an observed per-child result trace; a collection's shard count does not guarantee either pattern. |
| gRPC window | Preserve production settings for the main A/B. The standalone benchmark sets both client stream and connection static windows; this checkout has no corresponding Milvus dial-option/config wiring. A Milvus static-window sweep would need a separate, explicitly marked benchmark-only transport setting, not a ReduceStream change. |
| `full_transfer` | `0820_01` and `0831_02` are transport controls without reduction. A faithful Milvus comparison requires a benchmark-only full-drain control; ordinary early-stopping iterator execution cannot be labeled equal-byte transfer. |
| `0827_01` oversized child result | Its fixed 1 MiB global topK and up to 60 MiB/child deliberately break the equal-child/global-topK relation. Treat it as a run-ahead diagnostic, not a standard ANN iterator workload unless an explicit Milvus overfetch scenario is specified. |
| Timing and order | Preserve each plan's warm-up, minimum duration, repetition count, and Unary-first/Streaming-first order when comparing to that plan, or record a deliberate Milvus timing deviation. Report order strata separately. `0831_01` and `0831_02` concurrency sweeps use different P values and are not directly paired. |
| Metrics | Compare ordered IDs/scores and result counts first; then QPS, p50/p95/p99, first result, per-child sent/application-received/connection-read bytes, and Proxy retained intermediate bytes. Cumulative transport bytes are not peak resident memory. |

## Milvus Candidate Cases

Run only ordered-topK cases with requested topK at most 16,384. Keep production gRPC windows; exclude `full_transfer`, static-window, and oversized-child controls. Historical executions above remain unchanged.

| Case | Fixed setup | Vary | Benchmark basis |
| --- | --- | --- | --- |
| Baseline | 8 effective children, topK 16,384, Chunk 1,024 Units, concurrency 1, interleaved ranks | Unary versus Streaming | `0827_02` P4 MiB, rebased baseline for later sweeps |
| topK | Baseline except topK | 512, 1,024, 4,096, 16,384 | `0826_01` within normal limit |
| Fan-in | Baseline except effective children | 1, 2, 4, 8, 16, 32 | `0827_03`, rebased from topK 65,536 |
| Chunk | Baseline except Units/Chunk | 64, 256, 1,024, 4,096, 16,384 | `0826_01` Chunk sweep |
| Interaction | Interleaved ranks, concurrency 1 | children 2/8/32 x topK 4,096/16,384 x Chunk 256/1,024/4,096 Units | `0827_03` Phase B plus lower-topK cases |
| Concurrency | Baseline except concurrent iterator requests | 1, 2, 4, 8, 16, 32, 64; gate 64 on resources | `0831_01`, rebased from topK 65,536 |
| Rank distribution | Baseline except controlled child rank placement | interleaved, dominant child | `0826_01` |

Nominal mapping: benchmark C16/64/256 KiB and C1/4 MiB correspond to Milvus Chunk limits of 64/256/1,024 and 4,096/16,384 Units. Verify observed child streams and rank placement before treating a case as reproduced.

No Milvus end-to-end run is recorded by this document. Before running one,
freeze a Milvus-specific `TEST_PLAN.md` with the actual dataset, topology,
observed fan-in, supported topK values, Chunk configuration, source commits,
and the explicitly chosen benchmark-to-Milvus substitutions.
