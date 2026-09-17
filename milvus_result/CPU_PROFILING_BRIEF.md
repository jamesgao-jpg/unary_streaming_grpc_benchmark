# CPU-Profiling Analysis Brief (reusable)

Task: analyze Proxy CPU-profile behavior across the Milvus Batch/Streaming
experiments. This brief is the instruction set for any agent performing or
repeating the analysis. It works **only** on the preserved `cpu.pb.gz`
artifacts: do not re-run experiments, modify Milvus source, or re-profile on
the remote machines.

## Inventory

CPU profiles under
`milvus_result/test_0916_01|0916_02|0917_01|0917_02|0917_03/runs/*/` (paths
pattern: `.../<CASE>/profiles/<MODE>/cpu.pb.gz`).

Verified state (see `cpu_profiling_analysis.md`):

| Experiment | Content | CPU profiles |
| --- | --- | ---: |
| `test_0916_01` | metrics validation (ANN Search N1, metrics-on) | 2 |
| `test_0916_02` | N1 plain-query baseline | 2 |
| `test_0917_01` | fan-in sweep N1–N32, Chunk 1024 | 12 |
| `test_0917_02` | Chunk sweep N16/N32 (Batch-1024, Streaming-256/1024/8192) | 8 |
| `test_0917_03` | concurrency sweep (conc 1/32 × Batch/Streaming, Chunk 256, N16) | 4, pending |

Every profile is a 30s CPU profile of the Proxy process (`milvus` binary) on
`10.15.9.42` (aarch64). Each profile dir also contains the runner's
`cpu-top.txt`.

## Extraction

```bash
go tool pprof -top -nodecount=30 <profile>     # flat/cum attribution
go tool pprof -traces <profile>                # call stacks
go tool pprof -peek=<regex> <profile>          # caller/callee of key frames
go tool pprof -top -cum -nodecount=15 <profile>
```

Record for each profile: run ID, case (fan-in N), mode (Batch/Streaming), Chunk
size (from the path), profile start time, Duration, and
`Total samples = Xs (Y%)` (the "Y%" is average cores used in the window).

Classify top flat/cumulative functions:
(a) protobuf decode/encode — `consumeFloatSlice`, `protowire.*`,
    `fastpb.*`, `releaseCodec.Unmarshal`;
(b) copying/GC — `runtime.memmove`, `memclrNoHeapPointers`, `mallocgc`;
(c) syscall/network — `linux.Syscall6`, `io.ReadAtLeast`, gRPC `readFrame`/
    `readDataFrame`/`RawRead`;
(d) Query reduction — `typeutil.SelectMinPK`, `GetPK`, `GetSizeOfIDs`,
    `queryutil.(*ReduceByPKOperator).mergeByPK`, `OrderedReduceStream.*`,
    `retrieveUnit.pk`, `AppendFieldData`, `buildMergedVectorField`;
(e) other.

## Questions to answer (with evidence tables per experiment)

1. Fan-in sweep (`0917_01`): how does Batch Proxy CPU composition change
   N1→N32? Where does Streaming's smaller CPU go, and what dominates at N32?
2. Chunk sweep (`0917_02`): how does Streaming CPU composition change Chunk
   256→8192 (more copy/decode as Chunks grow)?
3. Metrics validation (`0916_01`): what does the instrumentation overhead look
   like in the profile? (Only metrics-on CPU profiles exist — the delta must
   come from `report.md` runtime counters.)
4. Concurrency (`0917_03`): how does per-op CPU scale under load, and does the
   Batch/Streaming CPU gap hold at concurrency 32?

## Flamegraphs

`go tool pprof -svg <profile> > <name>.svg` requires graphviz (`dot`); install
with `brew install graphviz`. Without graphviz, use
`go tool pprof -http=:8081 <profile>` for the interactive flamegraph view.

## Deliverable

- `milvus_result/cpu_profiling_analysis.md`: methodology, provenance,
  per-experiment attribution tables, flamegraph references (committed SVGs
  under `milvus_result/cpu_profiling_evidence/`), and a conclusions section
  separating verified attribution from hypotheses.
- Commit the doc + evidence dir to the benchmark repo. Do **not** commit the
  raw profile/run directories (large, intentionally local).

## Notes from the first pass (2026-09-17)

- graphviz 16.1.0 installed locally; go1.26.4.
- N32 Batch vs Streaming: 34.49s vs 10.06s total samples (S/B 0.29x).
- N32 Streaming-8192 ≈ Batch CPU (37.09s vs 35.90s): single-Chunk granularity
  removes the Streaming CPU advantage.
- Streaming never calls `SelectMinPK` (0 trace hits) — it merges ordered units
  via `retrieveUnit.pk`; Batch reduce is `mergeByPK` → `SelectMinPK` +
  `buildMergedRetrieveResults`.
