# Bounded Plain Query Vector-Payload Batch/Streaming Benchmark Report (N1)

## Status

The N1 bounded Plain Query experiment completed and passed correctness for
8,192 rows and 25,231,360 logical field bytes. This is a transport-overhead
baseline: with one QueryNode child there is no redundant child result to stop
early, so it neither proves nor disproves the fan-in retained-memory claim.

Tracking issue:
[unary_streaming_grpc_benchmark#8](https://github.com/jamesgao-jpg/unary_streaming_grpc_benchmark/issues/8).

## Environment

| Item | Value |
| --- | --- |
| Run ID | `20260916T125931Z-97545` |
| Server | `10.15.9.42` (aarch64) |
| Client | `10.15.2.233` |
| Milvus commit, local and server | `9d3dd3019283df4cfe77a180e4433666f7faaa98` |
| Experiment plan commit | `b8da141c221d2268e8a20bef7515f2597105e9a5` |
| Dataset | Cohere 1M, `cohere_1m_qn_fanin`, 1,000,000 rows, 63 sealed segments, 1 vchannel, 1 replica |
| Query | ordinary `query()`, `expr "pk >= 0"`, `limit=8192`, output `pk` + 768-dim float `vector`, Strong consistency |
| Streaming Chunk | 1,024 Units |
| Concurrency | 1 |
| Repetitions | 4, balanced order `B,S / S,B / B,S / S,B` |
| Interval | At least 60 seconds and 30 successful operations |
| Timed intervals | 8, plus 2 CPU-profile intervals excluded from QPS/latency |

## Qualified Topology

One vchannel, one QueryNode (WorkNode) owning all 63 sealed segments, and
`streamingNodePresent=false` for the sealed-only request. The WorkNode
identity was recorded before the run was accepted.

## Correctness

Batch and Streaming returned identical ordered IDs and identical
ID/vector SHA-256 for all 8,192 rows:

```text
sha256 = 0afdba6012d63db22b2591fa28964fb73f883b5e589f40607729d50e7ef7746b
```

## Results (median of four balanced repetitions)

| Metric | Batch | Streaming |
| --- | ---: | ---: |
| Median QPS | 2.662 | 2.617 |
| Median p50 | 370.63 ms | 376.12 ms |
| Median p95 | 396.89 ms | 408.93 ms |
| Median p99 | 423.87 ms | 433.85 ms |
| Median peak Proxy RSS | 1,453.47 MiB | 1,312.48 MiB |
| Median Proxy CPU | 10.05% | 12.50% |

Ratios:

```text
Streaming / Batch QPS = 0.9834
Streaming / Batch p95  = 1.0303
```

## Verified Conclusions

- Correctness passed: identical 8,192 ordered rows and identical hash in both
  modes, across all four repetitions.
- Streaming was a modest overhead at N1: approximately 1.7% lower QPS and 3.0%
  higher p95, consistent with replacing one unary response with eight 1,024-Unit
  Chunks and the extra reduction work, with no transfer to save.
- Median peak Proxy RSS was approximately 141 MiB lower for Streaming
  (1,312.48 vs 1,453.47 MiB). Whole-process RSS is noisy and this single fan-in
  level does not establish retained-memory scaling.
- This is an N1 transport-overhead baseline. It neither proves nor disproves the
  fan-in memory claim because one child leaves no redundant child result to
  stop early.

## Follow-up

The fan-in sweep in `test_0917_01` (issue
[#9](https://github.com/jamesgao-jpg/unary_streaming_grpc_benchmark/issues/9))
reused this request and measurement procedure across N1, N2, N4, N8, N16, and
N32 QueryNodes. Its N1 numbers reproduce this baseline (2.687 vs 2.662 median
QPS), cross-validating the harness, and it demonstrated the fan-in-dependent
Streaming advantage at high fan-in.

## Evidence

Raw evidence: `milvus_result/test_0916_02/runs/20260916T125931Z-97545/`
(summary: `summary.json`, per-interval: `manifest.tsv`). Next-agent handoff:
`milvus_result/test_0916_02/HANDOFF.md`.
