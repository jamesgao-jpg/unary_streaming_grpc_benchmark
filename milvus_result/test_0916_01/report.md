# QueryView Search Instrumentation Reconciliation and Overhead Report

## Status

The experiment completed successfully. All deterministic reconciliation checks,
all 16 timed overhead intervals, and both CPU-profile intervals passed.

GitHub issue 2 remains open. The implemented metrics reconcile request behavior
and have little observed QPS or latency impact, but the current single switch
enables low-overhead counters and diagnostic-only work together. In particular,
Batch allocation volume increased materially when metrics were enabled.

## Run

| Item | Value |
| --- | --- |
| Run ID | `20260916T091158Z-55130` |
| Milvus commit | `f315c09f4ad34499492a384fb89b839c04be680c` |
| Test-plan commit | `b431ecc` |
| Dataset | Cohere 1M, 1,000,000 rows, 768 dimensions |
| Topology | 1 vchannel, 1 QueryNode, 0 StreamingNode WorkNodes |
| Index | HNSW, COSINE, `M=16`, `efConstruction=200` |
| Search | ANN iterator, NQ 1, topK 8,192, `ef=8,192` |
| Streaming Chunk | 1,024 Units |
| Client concurrency | 1 |
| Evidence | `runs/20260916T091158Z-55130/` |

The run reused the immutable collection prepared by `test_0915_01`. It did not
reinsert data or rebuild the index.

## Reconciliation

Batch and Streaming returned the same 8,192 ordered results. Their final result
hash was:

```text
cc0e3f661fea558e7ccb581a1afd63d18c127a4fe4274d71d7eeae683a422d8f
```

| Boundary | Batch | Streaming |
| --- | ---: | ---: |
| QueryNode generated Units | 8,192 | 8,192 |
| QueryNode generated result bytes | 81,808 | 81,808 |
| Send completed messages | 1 | 8 |
| Send completed bytes | 81,812 | 82,226 |
| gRPC output payload bytes | 81,812 | 82,226 |
| gRPC output wire bytes | 81,817 | 82,266 |
| Proxy child gRPC input messages | 1 | 8 |
| Proxy child gRPC input payload bytes | 81,812 | 82,226 |
| Proxy application-received messages | 1 | 8 |
| Proxy application-received bytes | 81,812 | 82,226 |
| Proxy application-received Units | 8,192 | 8,192 |
| Units consumed by `ReduceStream` | 0 | 8,192 |
| Proxy connection-read bytes | 81,991 | 82,489 |
| Final result count | 8,192 | 8,192 |

The application and gRPC payload counters reconcile exactly in both modes:

```text
send completed bytes
  = QueryNode gRPC OutPayload bytes
  = Proxy gRPC InPayload bytes
  = Proxy application-received bytes
```

Connection-read bytes are larger because they are measured below the gRPC
message boundary and include transport framing and control traffic. They are
reported separately and are not treated as protobuf payload bytes.

### Request phases

| Phase | Batch | Streaming |
| --- | ---: | ---: |
| QueryNode ANN | 32.359 ms | 33.245 ms |
| QueryNode split | 0 | 0.778 ms |
| First child response | 34.386 ms | 35.819 ms |
| Per-vchannel reduction | 0 | 37.273 ms |
| Final reduction | 0.243 ms | 38.421 ms |
| First final Chunk | N/A | 36.885 ms |
| Final response construction | N/A | 0.453 ms |
| Proxy request | 36.073 ms | 39.932 ms |

### Connection-counter limitation

The Batch QueryNode record reports zero connection-write and TCP-send bytes,
while the Proxy measured 81,991 connection-read bytes. The unary handler takes
its QueryNode transport snapshot before gRPC writes the returned unary response.
Streaming `Send()` calls happen inside the handler, so its QueryNode snapshot
does observe 65,621 connection-write bytes.

This does not affect application/gRPC reconciliation or the measured Proxy
connection-read value. It does mean the current QueryNode connection-write
counter cannot provide exact unary response attribution and must not be used as
Batch sent-byte evidence.

## Instrumentation Overhead

Each value below is the median across four 60-second repetitions. Every interval
completed without errors and returned correct 8,192-result hashes.

| Mode | Metric | Off | On | On / Off |
| --- | --- | ---: | ---: | ---: |
| Batch | QPS | 13.954 | 13.788 | 0.988 |
| Batch | p95 latency | 108.417 ms | 110.837 ms | 1.022 |
| Batch | peak Proxy RSS | 611.457 MiB | 613.436 MiB | 1.003 |
| Batch | sampled Proxy CPU | 6.35% | 6.60% | 1.039 |
| Streaming | QPS | 13.320 | 13.316 | 1.000 |
| Streaming | p95 latency | 114.425 ms | 112.531 ms | 0.983 |
| Streaming | peak Proxy RSS | 617.037 MiB | 616.455 MiB | 0.999 |
| Streaming | sampled Proxy CPU | 8.25% | 8.55% | 1.036 |

The paired repetition medians provide the same overall result:

| Mode | QPS on/off | p95 on/off | RSS delta | CPU delta |
| --- | ---: | ---: | ---: | ---: |
| Batch | 0.991 | 1.011 | +3.094 MiB | +0.275 percentage points |
| Streaming | 1.000 | 0.983 | -0.146 MiB | +0.200 percentage points |

The Streaming p95 decrease is treated as run variance, not an instrumentation
improvement. The data supports a small QPS/latency impact under this N1 workload.

## Allocation and GC

Runtime counters show a different overhead profile from QPS and latency.

| Mode | Metrics | Allocated | Allocation per operation | GC cycles | GC pause sum |
| --- | --- | ---: | ---: | ---: | ---: |
| Batch | Off | 807.4 MiB | 985.9 KiB | 12 | 0.987 ms |
| Batch | On | 1,330.1 MiB | 1,643.8 KiB | 20 | 1.816 ms |
| Streaming | Off | 2,667.1 MiB | 3,422.9 KiB | 40 | 5.113 ms |
| Streaming | On | 2,726.3 MiB | 3,493.6 KiB | 41 | 5.099 ms |

The median paired allocation-per-operation ratio was 1.668 for Batch and 1.020
for Streaming. Therefore, the present instrumentation is not low-overhead with
respect to Batch allocations even though its measured QPS impact was about 1%.

Allocation profiles attribute most of the Batch increase to diagnostic result
inspection and serialization. In the representative profiles:

- Batch protobuf ID decoding increased from approximately 349 MiB to 800 MiB.
- Batch metrics-on added approximately 44 MiB under protobuf marshaling.
- Streaming metrics-on added approximately 50 MiB under final-result recording
  and protobuf marshaling.

The runtime counters are the primary allocation totals. The pprof values are
sampled attribution and are not expected to equal those totals exactly.

## CPU Profiles

The 30-second metrics-on profiles were separate from the overhead intervals.

- Batch captured 1.38 seconds of CPU samples, or 4.60% of wall time. Its leading
  flat costs included syscalls, futex work, memory clearing, and protobuf decode.
- Streaming captured 2.42 seconds of CPU samples, or 8.07% of wall time. The
  relevant cumulative paths included `buildSearchChunk`,
  `OrderedReduceStream.produceNextUnits`, `OrderedReduceStream.getReadyBuffers`,
  `AppendFieldData`, and `GetPK`.

For this N1 workload, the Streaming profile shows that result Chunk construction
and reduction are significant Proxy CPU paths. The profile does not isolate gRPC
framing as the sole source of the Streaming cost.

## Conclusions

1. The request, child, gRPC, application, reduction, and final-result metrics
   correlate correctly for deterministic N1 Batch and Streaming requests.
2. Batch and Streaming correctness is equivalent for the diagnostic request.
3. Enabling the current instrumentation changed median QPS by approximately
   -1.2% for Batch and 0.0% for Streaming. p95 and RSS differences were small
   relative to repetition variance.
4. Diagnostic instrumentation materially increases Batch allocation volume and
   GC work. It must not be treated as a low-overhead performance configuration.
5. QueryNode connection-write attribution for unary responses remains incomplete
   because the response write occurs after the handler snapshot.

## Issue 2 Status

Completed:

- phase, payload, gRPC, connection/TCP, reduction, and correctness counters;
- request and child correlation;
- deterministic Batch N1 and Streaming N1 reconciliation;
- Proxy CPU, allocation, heap/RSS, and GC evidence;
- metrics-disabled versus metrics-enabled comparison; and
- focused Batch/Streaming evidence with preserved profiles and logs.

Still required before closing issue 2:

- separate low-overhead performance counters from diagnostic-only connection,
  TCP, result-hash, and deep result-inspection work; and
- rerun the overhead comparison with the low-overhead configuration to confirm
  that both latency/QPS and allocation/GC overhead are acceptably small.

The raw 85 MiB run directory remains local and is intentionally not committed.
`summary.json`, `reconciliation.json`, `manifest.tsv`, profiles, logs, process
samples, and Prometheus snapshots remain under the evidence path above.
