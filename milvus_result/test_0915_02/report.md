# Milvus Streaming Fan-In Benchmark Report

## Status

The workload completed all 48 intervals and satisfies the experiment's execution criteria. The original runner exited after measurement because its final validator matched non-directory `FANIN-N*` files and parsed `FANIN-N*` incorrectly. Commit `eda52208b49faca14302e65205a92d03d4d1435e` fixes both validator defects; rerunning that validator against the preserved data passed. The original `FAILED` marker remains in the run directory for auditability.

## Environment

| Item | Value |
| --- | --- |
| Run ID | `20260916T032825Z-97546` |
| Server | `10.15.9.42` |
| Client | `10.15.2.233` |
| Milvus commit, local and server | `eede3bdb4c5e396edc35c985c89677307a2d60ca` |
| Test-plan commit used by run | `28d5c8bfc26f437fd047022aa79d6eaf9f6e442e` |
| PyMilvus | `2.6.17` |
| Dataset | Cohere 1M, 1,000,000 rows, 768 dimensions |
| Index | HNSW, COSINE, `M=16`, `efConstruction=200` |
| Search | `topK=8192`, `ef=8192`, concurrency 1 |
| Streaming Chunk | 1,024 results |
| Repetitions | 4, ordered `B,S / S,B / B,S / S,B` |
| Interval | At least 60 seconds and 100 successful operations |

## Qualified Topology

All cases reused the same 63 sealed segments. The Proxy WorkNode log showed exactly the requested QueryNodes and `streamingNodePresent=false` for every case.

| Case | QueryNodes | Rows per QueryNode, min-max | WorkNodes | Status |
| --- | ---: | ---: | ---: | --- |
| N1 | 1 | 1,000,000-1,000,000 | 1 | PASS |
| N2 | 2 | 495,926-504,074 | 2 | PASS |
| N4 | 4 | 240,000-255,926 | 4 | PASS |
| N8 | 8 | 112,037-127,963 | 8 | PASS |
| N16 | 16 | 48,148-64,074 | 16 | PASS |
| N32 | 32 | 16,111-32,037 | 32 | PASS |

## Per-Repetition Results

| Case | Rep | Order | Batch QPS | Streaming QPS | S/B QPS | Batch p95 ms | Streaming p95 ms |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| N1 | 1 | B,S | 14.021 | 13.424 | 0.957x | 107.62 | 109.72 |
| N1 | 2 | S,B | 14.026 | 13.280 | 0.947x | 106.51 | 111.81 |
| N1 | 3 | B,S | 13.847 | 13.394 | 0.967x | 106.67 | 109.96 |
| N1 | 4 | S,B | 13.896 | 13.572 | 0.977x | 109.66 | 109.13 |
| N2 | 1 | B,S | 14.338 | 13.861 | 0.967x | 104.37 | 106.64 |
| N2 | 2 | S,B | 14.328 | 13.758 | 0.960x | 105.11 | 107.39 |
| N2 | 3 | B,S | 14.296 | 13.747 | 0.962x | 108.29 | 109.44 |
| N2 | 4 | S,B | 14.473 | 13.776 | 0.952x | 104.04 | 106.75 |
| N4 | 1 | B,S | 14.409 | 13.666 | 0.948x | 104.13 | 107.79 |
| N4 | 2 | S,B | 14.124 | 13.718 | 0.971x | 105.66 | 107.30 |
| N4 | 3 | B,S | 14.170 | 13.736 | 0.969x | 108.10 | 107.85 |
| N4 | 4 | S,B | 14.250 | 13.713 | 0.962x | 104.06 | 106.88 |
| N8 | 1 | B,S | 14.099 | 13.639 | 0.967x | 106.33 | 108.20 |
| N8 | 2 | S,B | 14.193 | 13.772 | 0.970x | 104.25 | 107.21 |
| N8 | 3 | B,S | 14.123 | 13.607 | 0.963x | 105.10 | 108.10 |
| N8 | 4 | S,B | 14.219 | 13.665 | 0.961x | 104.96 | 108.33 |
| N16 | 1 | B,S | 13.774 | 13.123 | 0.953x | 107.10 | 110.13 |
| N16 | 2 | S,B | 13.797 | 13.370 | 0.969x | 106.73 | 108.25 |
| N16 | 3 | B,S | 13.650 | 13.244 | 0.970x | 108.75 | 109.29 |
| N16 | 4 | S,B | 13.678 | 13.136 | 0.960x | 107.40 | 114.37 |
| N32 | 1 | B,S | 12.730 | 12.396 | 0.974x | 111.65 | 114.50 |
| N32 | 2 | S,B | 12.677 | 12.359 | 0.975x | 115.40 | 115.80 |
| N32 | 3 | B,S | 12.732 | 12.346 | 0.970x | 114.55 | 117.91 |
| N32 | 4 | S,B | 12.633 | 12.290 | 0.973x | 115.06 | 118.28 |

## Order-Stratified Medians

| Case | Order | Batch QPS | Streaming QPS | S/B QPS | Batch p95 ms | Streaming p95 ms | S/B p95 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| N1 | B,S | 13.934 | 13.409 | 0.962x | 107.14 | 109.84 | 1.025x |
| N1 | S,B | 13.961 | 13.426 | 0.962x | 108.09 | 110.47 | 1.022x |
| N2 | B,S | 14.317 | 13.804 | 0.964x | 106.33 | 108.04 | 1.016x |
| N2 | S,B | 14.401 | 13.767 | 0.956x | 104.57 | 107.07 | 1.024x |
| N4 | B,S | 14.289 | 13.701 | 0.959x | 106.12 | 107.82 | 1.016x |
| N4 | S,B | 14.187 | 13.715 | 0.967x | 104.86 | 107.09 | 1.021x |
| N8 | B,S | 14.111 | 13.623 | 0.965x | 105.72 | 108.15 | 1.023x |
| N8 | S,B | 14.206 | 13.719 | 0.966x | 104.60 | 107.77 | 1.030x |
| N16 | B,S | 13.712 | 13.184 | 0.961x | 107.92 | 109.71 | 1.017x |
| N16 | S,B | 13.737 | 13.253 | 0.965x | 107.06 | 111.31 | 1.040x |
| N32 | B,S | 12.731 | 12.371 | 0.972x | 113.10 | 116.21 | 1.027x |
| N32 | S,B | 12.655 | 12.324 | 0.974x | 115.23 | 117.04 | 1.016x |

## Combined Comparison

| Case | Batch median QPS | Streaming median QPS | S/B QPS | Batch median p95 ms | Streaming median p95 ms | S/B p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| N1 | 13.959 | 13.409 | 0.961x | 107.14 | 109.84 | 1.025x |
| N2 | 14.333 | 13.767 | 0.961x | 104.74 | 107.07 | 1.022x |
| N4 | 14.210 | 13.715 | 0.965x | 104.90 | 107.55 | 1.025x |
| N8 | 14.158 | 13.652 | 0.964x | 105.03 | 108.15 | 1.030x |
| N16 | 13.726 | 13.190 | 0.961x | 107.25 | 109.71 | 1.023x |
| N32 | 12.704 | 12.353 | 0.972x | 114.81 | 116.85 | 1.018x |

## Verified Conclusions

- All 48 intervals completed with zero request errors, exactly 8,192 results per operation, and identical Batch and Streaming result hashes.
- Streaming was slower at every fan-in, with 96.1%-97.2% of Batch QPS and 1.8%-3.0% higher median p95 latency.
- The relative Streaming gap did not grow with fan-in. N32 had the smallest measured gap, but the ratios were not monotonic from N1 through N32.
- Both modes declined at N32. Relative to N1, Batch QPS was 91.0% and Streaming QPS was 92.1%.
- The two execution-order strata were close; no consistent Batch-first or Streaming-first bias explains the result.

## Limitations

- This run covers one `topK`, Chunk size, concurrency, dataset, index, and server/client pair. It does not isolate Chunk-size, topK, or concurrency effects.
- QueryNode ANN work still materializes each local result before Chunk transmission, so this measures the current M1A transport and Proxy reduction path rather than stateful segment-level streaming.
- The run does not establish a process-memory conclusion; process samples are retained for separate analysis.
- The runner's final validator failed after all measurements. The corrected validator passed offline, but the preserved run directory intentionally retains the original `FAILED` marker.

## Evidence

Raw evidence: `milvus_result/test_0915_02/runs/20260916T032825Z-97546/`

Generated summaries: `manifest.tsv` and `summary.json` in that run directory.
