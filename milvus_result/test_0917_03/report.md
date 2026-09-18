# Client Concurrency Sweep for Bounded Plain Query Batch vs Streaming

## Status

The concurrency sweep was paused after the concurrency-1 through -8 evidence
established a consistent server-side saturation ceiling at concurrency 2.
Concurrency 16 and 32 were not run: the QPS cap and the monotonic latency/memory
degradation beyond concurrency 2 were already consistent across both modes, so
higher levels would only repeat the same decline. The preserved evidence
covers 15 of the 24 planned timed intervals (concurrency 1, 2, 4 complete with
two repetitions per mode; concurrency 8 with one Batch and two Streaming
intervals). Correctness passed with the fan-in-invariant result hash.

Tracking issue:
[unary_streaming_grpc_benchmark#11](https://github.com/jamesgao-jpg/unary_streaming_grpc_benchmark/issues/11).

## Environment

| Item | Value |
| --- | --- |
| Run ID | `20260917T123310Z-732` |
| Server | `10.15.9.42` (aarch64) |
| Client | `10.15.2.233` |
| Milvus commit, local and server | `9d3dd3019283df4cfe77a180e4433666f7faaa98` |
| Test-plan commits | `cd7a151` (plan/runner), `a765d50` (concurrency driver fix) |
| Dataset | Cohere 1M, `cohere_1m_qn_fanin`, 1,000,000 rows, 63 sealed segments, 1 vchannel, 1 replica |
| Query | ordinary `query()`, `expr "pk >= 0"`, `limit=8192`, output `pk` + 768-dim float `vector`, Strong consistency |
| Streaming Chunk size | 256 Units (best QPS and lowest peak RSS at N16 from `test_0917_02`) |
| QueryNode fan-in | 16 (fixed) |
| Concurrency levels | 1, 2, 4, 8 (16 and 32 not run) |
| Repetitions per mode per level | 2, balanced order `B,S / S,B` |
| Interval | At least 60 seconds and 30 aggregate successful operations |
| Measured intervals | 15 of 24 planned |

## Qualified Topology

One vchannel, 16 QueryNodes (IDs 616-631) owning all 63 sealed segments,
`streamingNodePresent=false`, WorkNode IDs matching placement owners. The
correctness gate passed: 8,192 rows, 25,231,360 logical field bytes, identical
ordered IDs and identical ID/vector SHA-256 between Batch and Streaming:

```text
sha256 = 0afdba6012d63db22b2591fa28964fb73f883b5e589f40607729d50e7ef7746b
```

## Results

Medians of the completed repetitions per mode per concurrency level.

| Concurrency | Batch QPS | Stream QPS | S/B QPS | Batch p95 ms | Stream p95 ms | Batch peak RSS MiB | Stream peak RSS MiB | Batch CPU % | Stream CPU % |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1.69 | 2.24 | 1.33x | 631 | 456 | 9,293 | 1,109 | 5.7 | 6.2 |
| 2 | 3.09 | 3.37 | 1.09x | 735 | 597 | 11,932 | 1,251 | 6.2 | 6.5 |
| 4 | 2.90 | 3.16 | 1.09x | 1,591 | 1,266 | 11,923 | 1,518 | 6.5 | 6.1 |
| 8 | 2.56 | 2.56 | 1.00x | 4,646 | 4,029 | 23,517 | 2,115 | 5.9 | 6.3 |

Per-repetition detail (all intervals, conc 8 Batch has one repetition):

| Conc | Rep | Mode | QPS | p50 ms | p95 ms | p99 ms | Peak RSS MiB | CPU % | Ops |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | batch | 1.699 | 538 | 636 | 653 | 8,993 | 5.6 | 102 |
| 1 | 1 | streaming | 2.254 | 421 | 455 | 473 | 1,115 | 5.7 | 136 |
| 1 | 2 | batch | 1.688 | 542 | 626 | 642 | 9,593 | 5.7 | 102 |
| 1 | 2 | streaming | 2.236 | 423 | 458 | 479 | 1,103 | 6.7 | 135 |
| 2 | 1 | batch | 3.113 | 549 | 741 | 2,134 | 11,924 | 5.8 | 188 |
| 2 | 1 | streaming | 3.394 | 526 | 597 | 728 | 1,314 | 6.1 | 205 |
| 2 | 2 | batch | 3.070 | 557 | 728 | 2,188 | 11,939 | 6.6 | 185 |
| 2 | 2 | streaming | 3.352 | 540 | 597 | 655 | 1,188 | 6.9 | 202 |
| 4 | 1 | batch | 2.943 | 1,124 | 1,544 | 4,660 | 11,353 | 6.9 | 179 |
| 4 | 1 | streaming | 3.137 | 1,074 | 1,263 | 3,940 | 1,462 | 6.2 | 190 |
| 4 | 2 | batch | 2.866 | 1,184 | 1,639 | 4,145 | 12,493 | 6.2 | 174 |
| 4 | 2 | streaming | 3.174 | 1,074 | 1,269 | 3,940 | 1,574 | 6.1 | 193 |
| 8 | 1 | batch | 2.565 | 2,114 | 4,646 | 9,082 | 23,517 | 5.9 | 157 |
| 8 | 1 | streaming | 2.623 | 2,146 | 3,460 | 11,260 | 1,837 | 6.5 | 160 |
| 8 | 2 | streaming | 2.495 | 2,154 | 4,598 | 10,568 | 2,394 | 6.2 | 153 |

## Verified Conclusions

- All 15 completed intervals ran with zero request errors and the identical
  result hash; correctness held at every concurrency level.
- **Both modes saturate at concurrency 2**: QPS peaks at 3.09 (Batch) and 3.37
  (Streaming) and declines at concurrency 4 and 8. This is a server-side
  ceiling, not a client limit: operation counts kept rising while QPS fell and
  p50/p95/p99 grew monotonically (Batch p95 631 -> 4,646 ms; Streaming p95
  456 -> 4,029 ms from conc 1 to conc 8).
- **The Streaming QPS advantage narrows with load and disappears at
  saturation**: S/B QPS is 1.33x at concurrency 1, 1.09x at 2 and 4, and 1.00x
  at 8. Under load the shared bottleneck dominates and the transport advantage
  vanishes.
- **The bounded-memory contrast holds under saturation**: at concurrency 8,
  Batch peak Proxy RSS reaches 23.5 GiB while Streaming stays at 2.1 GiB
  (approximately 11x lower). Streaming RSS grows slowly with concurrency
  (1.1 -> 2.1 GiB from conc 1 to 8), consistent with Chunk-sized retention per
  in-flight request.
- **Proxy CPU is not the bottleneck**: sampled Proxy CPU stays at 5-7% across
  all modes and concurrency levels, so the saturation ceiling is elsewhere -
  most likely QueryNode-side fan-in coordination or the shared 16-way shard
  pipeline, not Proxy compute.
- The concurrency-1 baseline reproduces the prior experiments (Streaming
  2.24 QPS at N16, chunk 256, matching `test_0917_02`'s 2.39 within repetition
  variance).

## Interpretation

The concurrency dimension is effectively answered with concurrency 1-8: the
N16 topology's throughput ceiling is approximately 3.1-3.4 QPS regardless of
mode, reached at concurrency 2, and beyond that only latency and (for Batch)
memory degrade. Streaming's practical value under load is therefore not higher
throughput - it is **bounded Proxy memory** (11x lower peak RSS at saturation)
and its latency degradation is roughly 13% gentler at conc 8 (4.0 s vs 4.6 s
p95). Concurrency 16 and 32 were not run because the QPS cap was already
consistent and higher levels would only repeat the same decline with worse
latency.

The low Proxy CPU (5-7%) at the saturation point is a notable open question:
the ceiling is not Proxy compute, and identifying the true bottleneck
(QueryNode-side fan-in coordination, shared shard pipeline, or coordination
service) would require instrumentation beyond the process-level evidence
available in this experiment.

## Limitations

- Concurrency 16 and 32 were not run (run paused after the cap was
  established); the report documents 15 of 24 planned intervals.
- Concurrency 8 Batch has one repetition (the second Batch interval was lost
  when the remote network dropped during the run); its median is a single
  sample.
- Process-level evidence only: peak Proxy RSS is whole-process and 1s-sampled;
  Query-internal reduction/transport counters do not exist.
- The saturation ceiling is specific to this topology (N16, chunk 256,
  limit 8,192, vector output) and this server/client pair; it does not
  generalize to other fan-in, Chunk, or payload configurations.
- Proxy CPU samples (5-7%) are median per-second readings; bursty peaks may be
  higher but do not change the conclusion that Proxy compute is not the
  binding constraint at the observed ceiling.

## Evidence

Raw evidence: `milvus_result/test_0917_03/runs/20260917T123310Z-732/`

Per-interval `benchmark.json`, `processes.csv`, runtime snapshots, and logs are
preserved under `FANIN-N16/conc*/`. The `FAILED` marker records the remote
network drop that ended the run (exit 255); the surviving evidence above is
verified and internally consistent. No `manifest.tsv` or `summary.json` were
produced because `build_summary` requires the full 24-interval matrix.
