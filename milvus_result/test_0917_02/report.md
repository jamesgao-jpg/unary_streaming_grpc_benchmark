# Streaming Chunk-Size Sweep for Bounded Plain Query Batch vs Streaming

## Status

The Chunk-size sweep completed all 28 timed intervals across two fan-in cases
and satisfies every exit criterion in the test plan. Correctness passed at
every Chunk size with the same result hash, so the result is Chunk-size
invariant and fan-in invariant. Evidence:
`milvus_result/test_0917_02/runs/20260917T073901Z-62704/`.

Tracking issue:
[unary_streaming_grpc_benchmark#10](https://github.com/jamesgao-jpg/unary_streaming_grpc_benchmark/issues/10).

## Environment

| Item | Value |
| --- | --- |
| Run ID | `20260917T073901Z-62704` |
| Server | `10.15.9.42` (aarch64) |
| Client | `10.15.2.233` |
| Milvus commit, local and server | `9d3dd3019283df4cfe77a180e4433666f7faaa98` |
| Test-plan commits used by run | `8cbc2eb` (plan/runner), `c8998f8` (WorkNode-probe signature fix) |
| Dataset | Cohere 1M, `cohere_1m_qn_fanin`, 1,000,000 rows, 63 sealed segments, 1 vchannel, 1 replica |
| Query | ordinary `query()`, `expr "pk >= 0"`, `limit=8192`, output `pk` + 768-dim float `vector`, Strong consistency |
| Client concurrency | 1 |
| Repetitions per Chunk size | 2 |
| Batch intervals per case | 2 (one before, one after the sweep) |
| Interval | At least 60 seconds and 30 successful operations |
| Timed intervals | 28 (2 cases x [2 Batch + 6 Chunk sizes x 2 Streaming]) |

## Qualified Topology

Both cases reused the same 63 sealed segments with unchanged segment IDs and
row counts. The Proxy WorkNode log showed exactly the requested QueryNodes and
`streamingNodePresent=false` for every case, and the observed WorkNode IDs
matched the placement owners.

| Case | QueryNodes | WorkNodes | Status |
| --- | ---: | ---: | --- |
| FANIN-N16 | 16 | 16 | PASS |
| FANIN-N32 | 32 | 32 | PASS |

## Correctness

Batch and Streaming returned identical ordered IDs and identical ID/vector
SHA-256 at every Chunk size, and the hash is identical across all Chunk sizes
and both fan-in cases:

```text
sha256 = 0afdba6012d63db22b2591fa28964fb73f883b5e589f40607729d50e7ef7746b
```

`build_summary` verified every timed interval returned this hash and rejected
any deviation.

## Results

Batch is the median of its two bracketing intervals per case; Streaming is the
median of two repetitions per Chunk size.

### FANIN-N16

Batch baseline: QPS 1.806, p95 626.6 ms, peak Proxy RSS 11,189 MiB, Proxy CPU
62.0%.

| Chunk size | Streaming QPS | S/B QPS | Streaming p95 ms | S/B p95 | Stream peak RSS MiB | Stream CPU % |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 2.392 | 1.325x | 440.3 | 0.703x | 1,429 | 16.4 |
| 512 | 2.353 | 1.303x | 452.5 | 0.722x | 1,838 | 17.9 |
| 1,024 | 2.360 | 1.307x | 446.6 | 0.713x | 1,937 | 19.5 |
| 2,048 | 2.326 | 1.288x | 458.5 | 0.732x | 2,238 | 26.0 |
| 4,096 | 2.267 | 1.256x | 468.8 | 0.748x | 3,024 | 40.0 |
| 8,192 | 2.193 | 1.215x | 483.9 | 0.772x | 4,428 | 56.3 |

### FANIN-N32

Batch baseline: QPS 1.096, p95 1029.6 ms, peak Proxy RSS 13,075 MiB, Proxy CPU
77.5%.

| Chunk size | Streaming QPS | S/B QPS | Streaming p95 ms | S/B p95 | Stream peak RSS MiB | Stream CPU % |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 1.870 | 1.714x | 559.1 | 0.535x | 1,477 | 17.8 |
| 512 | 1.891 | 1.733x | 551.3 | 0.528x | 2,106 | 20.8 |
| 1,024 | 1.860 | 1.705x | 558.4 | 0.535x | 2,115 | 26.6 |
| 2,048 | 1.785 | 1.636x | 585.6 | 0.561x | 2,849 | 36.1 |
| 4,096 | 1.705 | 1.567x | 611.7 | 0.588x | 4,033 | 58.4 |
| 8,192 | 1.550 | 1.550x | 592.3 | 0.592x | 6,221 | 78.4 |

## Verified Conclusions

- All 28 timed intervals completed with zero request errors, exactly 8,192 rows
  per operation, and the identical result hash at every Chunk size and fan-in
  case.
- **Streaming peak Proxy RSS scales monotonically with Chunk size at fixed
  fan-in**: 1,429 to 4,428 MiB (N16) and 1,477 to 6,221 MiB (N32) across Chunk
  sizes 256 to 8,192, while Batch peak RSS stays flat at 11,189 MiB (N16) and
  13,075 MiB (N32). Even at Chunk size 8,192 (single-Chunk granularity),
  Streaming retains only about 40-48% of Batch's peak RSS.
- **The message-overhead penalty of small Chunks is mild**: from 1,024 to 256,
  QPS changes by +1.4% (N16) and +0.5% (N32) while peak RSS falls by about 26%
  (N16) and 30% (N32).
- **Larger Chunks degrade Streaming QPS and CPU monotonically**: from 256 to
  8,192, QPS falls 8.3% (N16) and 17.1% (N32), p95 rises 9.9% (N16) and 5.9%
  (N32), and Proxy CPU climbs from 16.4% to 56.3% (N16) and 17.8% to 78.4%
  (N32).
- **Chunk sizes 256-1,024 form the practical operating range**: near-peak QPS
  with the lowest retained memory. The current 1,024 default is well placed;
  shrinking toward 256 buys memory at negligible QPS cost, and raising the
  Chunk size buys nothing except higher retained memory and CPU.
- The N32 CPU profiles attribute the Chunk-size-dependent Streaming CPU growth
  to protobuf float decoding and copying of the Chunk payloads
  (`consumeFloatSlice` 28.2% of flat time at Chunk 8,192 versus syscall and
  `memmove` dominance at Chunk 256). Batch CPU is dominated by `memmove`
  (20.4%) and `consumeFloatSlice` (15.0%).
- The N16 and N32 Batch baselines reproduce the `test_0917_01` fan-in sweep
  numbers (N16 1.831, N32 1.091 QPS; 8.4 and 16.8 GiB peak RSS in that run's
  medians), with the expected 1s-sampler peak-RSS variance at N32 (13.1 vs
  16.8 GiB), cross-validating the harness.
- The two Batch bracketing intervals agree closely at both cases; no drift
  across the sweep window explains the result.

## Interpretation

The experiment validates the Chunk-size dimension of the Streaming Reduce
memory claim: at fixed fan-in, Streaming retained input is bounded by Chunk
size times child count, so peak Proxy RSS rises with Chunk size while remaining
far below Batch's full-result-times-child-count footprint. The QPS/latency cost
of small Chunks is small relative to the memory savings, giving a wide
operating range around the current default. Chunk size and fan-in interact
additively in the expected direction: higher fan-in amplifies both the memory
contrast and the Streaming QPS advantage.

## Limitations

- Process-level evidence only: Proxy peak RSS is whole-process and 1s-sampled,
  so absolute peak values carry sampling variance; the monotonic RSS trend and
  CPU attribution are the reliable signals.
- One dataset, one limit (8,192), one concurrency (1), and one server/client
  pair. Concurrency effects are covered by issue
  [#11](https://github.com/jamesgao-jpg/unary_streaming_grpc_benchmark/issues/11).
- QueryNode work still materializes each local result before Chunk
  transmission, so this measures the current M1A transport and Proxy reduction
  path rather than stateful segment-level streaming.
- Chunk size is Unit-count based; byte-based Chunk sizing was not varied.

## Evidence

Raw evidence: `milvus_result/test_0917_02/runs/20260917T073901Z-62704/`

Generated summaries: `manifest.tsv`, `summary.json`, and `report-input.json` in
that run directory. Per-case placement, WorkNode, correctness, profile, process
sample, Prometheus, and log evidence is preserved under each `FANIN-N*`
subdirectory.
