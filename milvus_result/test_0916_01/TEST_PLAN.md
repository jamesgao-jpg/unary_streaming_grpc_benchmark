# QueryView Search Instrumentation Reconciliation and Overhead

## Status

Approved for execution. This experiment completes the validation and overhead
exit criteria of unary/Streaming gRPC benchmark issue 2.

## Objective

Validate that the QueryView Search instrumentation measures the same request
consistently at the application, gRPC, connection, TCP, reduction, and final
result boundaries. Then quantify the performance and process-resource overhead
introduced when that instrumentation is enabled.

This is an N1 diagnostic experiment. It does not repeat the fan-in matrix from
`test_0915_02` and does not make a new Batch-versus-Streaming performance claim.

## Environment

| Item | Value |
| --- | --- |
| Milvus server | `ubuntu@10.15.9.42` |
| PyMilvus client | `ubuntu@10.15.2.233` |
| Milvus branch | `codex/qv-reducestream-e2e-benchmark-20260915` |
| Dataset | Cohere 1M, 1,000,000 rows, 768 dimensions |
| Collection | `cohere_1m_qn_fanin` |
| Index | HNSW, COSINE, `M=16`, `efConstruction=200` |
| Vchannels | 1 |
| QueryNodes | 1 |
| StreamingNodes selected as WorkNodes | 0 |
| Sealed segments | 63 |
| Client concurrency | 1 |

The experiment reuses the immutable collection and infrastructure volumes from
`test_0915_01` run `20260915T090613Z-20989`. It must not reinsert data or rebuild
the index.

## Fixed Search Workload

| Parameter | Value |
| --- | ---: |
| Request | ANN Search iterator |
| NQ | 1 |
| topK, iterator batch size, iterator limit | 8,192 |
| HNSW `ef` | 8,192 |
| Streaming Chunk size | 1,024 Units |
| Query vectors | First 100 fixed Cohere test vectors |
| Warm-up per timed interval | 20 operations |
| Minimum timed operations | 100 |
| Minimum timed duration | 60 seconds |

## Controls

| Mode | Streaming | Benchmark metrics |
| --- | --- | --- |
| `batch-off` | disabled | disabled |
| `batch-on` | disabled | enabled |
| `streaming-off` | enabled | disabled |
| `streaming-on` | enabled | enabled |

The metric switch is
`proxy.queryView.enableSearchBenchmarkMetrics`. Both Proxy and QueryNode are
restarted for every cell so their gRPC handlers and connection wrappers use the
same setting. The collection is released before the restart and loaded only
after the replacement processes are healthy. Startup and loading are excluded
from the timed interval.

## Phase 1: Corrected N1 Reconciliation

Run one fixed query in `batch-on`, then the identical query in `streaming-on`.
Preserve the client results and the single matching Proxy and QueryNode metric
records for each mode.

Required assertions:

1. Batch and Streaming return 8,192 identical ordered IDs and scores within
   `1e-6`.
2. Proxy and QueryNode request IDs match within each mode.
3. `generatedUnits` is 8,192 in both modes.
4. Batch sends and receives one result message; Streaming sends and receives
   eight result messages.
5. QueryNode `sendCompletedBytes` equals `grpcOutPayloadBytes`.
6. Proxy child application-received bytes equal child gRPC input payload bytes.
7. Proxy receives 8,192 Units in both modes; Streaming consumes 8,192 Units
   through `ReduceStream`.
8. Proxy final result count and hash match between modes.
9. Proxy root gRPC counters remain zero; child counters own the QN RPC.
10. N1 connection/TCP attribution is available and names the selected QN peer.

Connection bytes are not required to equal gRPC payload bytes. They include
HTTP/2 framing and control traffic, and the QueryNode snapshot may occur while
bytes accepted by `SendMsg` remain below the application boundary.

## Phase 2: Instrumentation Overhead

Run four repetitions of all four controls. Each repetition uses a different
order to balance mode, instrumentation, and thermal/order effects.

| Repetition | Order |
| --- | --- |
| 1 | `batch-off`, `batch-on`, `streaming-off`, `streaming-on` |
| 2 | `streaming-on`, `streaming-off`, `batch-on`, `batch-off` |
| 3 | `batch-on`, `batch-off`, `streaming-on`, `streaming-off` |
| 4 | `streaming-off`, `streaming-on`, `batch-off`, `batch-on` |

Every interval records:

- QPS and p50/p95/p99/max end-to-end latency;
- result count and ordered-ID hash;
- one-second Proxy process CPU and RSS samples;
- Proxy Prometheus metrics before and after the workload;
- Proxy heap and cumulative allocation pprof profiles before and after;
- Proxy and QueryNode logs; and
- exact process configuration.

Compare metrics-on against metrics-off separately within Batch and Streaming.
Report median QPS and p95 latency across four repetitions. Also report peak
Proxy RSS, sampled CPU, heap allocation, GC, and total allocation deltas as
secondary diagnostics. Do not add Batch and Streaming values together.

## Phase 3: CPU Profiles

Run separate 30-second CPU-profile workloads for `batch-on` and `streaming-on`.
These intervals are excluded from the overhead QPS comparison. Preserve the
raw CPU, heap, and allocation profiles and a text `go tool pprof -top` summary.

## Exit Criteria

- All reconciliation assertions pass for Batch N1 and Streaming N1.
- All 16 timed overhead intervals finish without errors and return 8,192
  results with identical hashes.
- Metrics-disabled logs contain no QueryView benchmark metric records.
- Metrics-enabled logs contain correlated Proxy and QueryNode records.
- The report separates application/gRPC payload counters from connection/TCP
  counters.
- Metrics-on versus metrics-off QPS and p95 ratios are reported for both modes.
- Process samples, Prometheus snapshots, and pprof artifacts are retained.

## Evidence Layout

```text
runs/<run-id>/
  diagnostic/{batch,streaming}/
  overhead/rep<1-4>/<mode>-metrics-<off|on>/
  profiles/{batch,streaming}-metrics-on/
  reconciliation.json
  manifest.tsv
  summary.json
  report-input.json
```

`report.md` is written only after the preserved evidence has been analyzed.
