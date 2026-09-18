# Milvus Proxy CPU-Profile Analysis: Batch vs Streaming (M1A)

## Status

Analysis of the 24 preserved Proxy CPU profiles from experiments
`test_0916_01`, `test_0916_02`, `test_0917_01`, and `test_0917_02`. The
concurrency sweep `test_0917_03` was still running at analysis time and had no
CPU profiles yet; its section below is a fold-in procedure, not a result.

Scope follows the brief in
[`CPU_PROFILING_BRIEF.md`](CPU_PROFILING_BRIEF.md): work on the preserved
`cpu.pb.gz` artifacts only. Nothing was re-run, no Milvus source was modified,
and no remote profiling was performed.

## Methodology

### Artifacts

Every profile is a 30-second Go CPU profile of the **Proxy** process (the
`milvus` binary) captured on the server `10.15.9.42` (aarch64) by the
experiment runner (test plans: "Run separate 30-second Batch and Streaming
CPU-profile intervals"). One profile exists per mode per case, excluded from
QPS/latency aggregation. Each profile directory also carries the runner's
`cpu-top.txt`; spot checks confirm it matches our `-top` runs (e.g. N32 Batch
`cpu-top.txt` and our `pprof -top` both report `Total samples = 34.49s
(114.97%)`).

### Tooling and commands

- `go tool pprof` (go1.26.4, local) on each `cpu.pb.gz`; no binary needed,
  function names are embedded in the profile.
- `go tool pprof -top -nodecount=30 <profile>` — flat/cum tables (the basis
  for every table below).
- `go tool pprof -top -cum -nodecount=15 <profile>` — cumulative-ranked view.
- `go tool pprof -traces <profile>` — full call stacks.
- `go tool pprof -peek=<regex> <profile>` — caller/callee attribution for key
  functions (`mergeByPK`, `releaseCodec.Unmarshal`, `Syscall6`, ...).
- `go tool pprof -svg <profile> > <name>.svg` — call-graph SVG via graphviz
  `dot` 16.1.0 (installed locally with `brew install graphviz`). Committed
  under `cpu_profiling_evidence/svg/`. The interactive flamegraph view is
  available without graphviz via `go tool pprof -http=:8081 <profile>`.
- `-top` text tables for all 24 profiles are committed under
  `cpu_profiling_evidence/top30/` for auditability.

### Provenance

| Profile | Experiment | Case | Mode | Chunk | Milvus build ID | Profile start (CST) | Total samples / wall % |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `0916_01-batch-metrics-on` | metrics validation (ANN Search N1) | N1 | Batch | 1024 | `d143016c…` | 2026-09-16 18:07:01 | 1.38s / 4.60% |
| `0916_01-streaming-metrics-on` | same | N1 | Streaming | 1024 | `d143016c…` | 2026-09-16 18:09:41 | 2.42s / 8.07% |
| `0916_02-batch` | N1 plain-query baseline | N1 | Batch | 1024 | `c822a938…` | 2026-09-16 21:22:06 | 2.50s / 8.33% |
| `0916_02-streaming` | same | N1 | Streaming | 1024 | `c822a938…` | 2026-09-16 21:23:31 | 3.57s / 11.90% |
| `0917_01-N{1,2,4,8,16,32}-batch` | fan-in sweep | N1…N32 | Batch | 1024 | `ab33ced0…` | 2026-09-17 11:36–13:25 | see below |
| `0917_01-N{1,2,4,8,16,32}-streaming` | fan-in sweep | N1…N32 | Streaming | 1024 | `ab33ced0…` | 2026-09-17 11:41–13:27 | see below |
| `0917_02-N{16,32}-batch-1024` | chunk sweep | N16 / N32 | Batch | 1024 | `7a16f8e9…` | 2026-09-17 16:44 / 18:40 | 27.41s / 35.90s |
| `0917_02-N{16,32}-streaming-{256,1024,8192}` | chunk sweep | N16 / N32 | Streaming | 256/1024/8192 | `7a16f8e9…` | 2026-09-17 16:47–18:58 | see below |

All profiles are 30s windows. "Total samples" ≈ CPU-seconds consumed by the
Proxy in that window; the `% wall` column is therefore average cores used
(>100% means more than one core, i.e. saturation).

Total samples per 0917_01 profile (Batch → Streaming): N1 2.49s → 3.64s; N2
4.66s → 4.05s; N4 8.78s → 4.59s; N8 15.99s → 5.52s; N16 26.32s → 7.48s; N32
34.49s → 10.06s. Per 0917_02: N16 batch 27.41s, streaming 5.70s (256), 7.42s
(1024), 23.95s (8192); N32 batch 35.90s, streaming 6.48s (256), 10.35s (1024),
37.09s (8192).

Workload (0916_02/0917_01/0917_02/0917_03): ordinary `query()`, `pk >= 0`,
limit 8192, output `pk` (INT64) + 768-dim float `vector` (25,231,360 logical
field bytes per result), concurrency 1, result hash
`0afdba60…6b` identical across every case and mode. `test_0916_01` is a
different workload: ANN Search iterator, NQ 1, topK 8192, IDs + scores only
(~81 KB per result).

---

## 1. Fan-in sweep (`test_0917_01`) — how does Proxy CPU change N1→N32?

### 1.1 Total sampled CPU (core-seconds in the 30s window)

| Case | Batch (s) | Streaming (s) | S/B |
| --- | ---: | ---: | ---: |
| N1 | 2.49 | 3.64 | 1.46x |
| N2 | 4.66 | 4.05 | 0.87x |
| N4 | 8.78 | 4.59 | 0.52x |
| N8 | 15.99 | 5.52 | 0.35x |
| N16 | 26.32 | 7.48 | 0.28x |
| N32 | 34.49 | 10.06 | 0.29x |

Batch Proxy CPU grows ~13.9x from N1 to N32 (32x fan-in) — it processes
complete child results (32 × 24 MiB of vector payload per request). Streaming
grows only ~2.8x and at N32 uses ~29% of Batch's CPU. This mirrors the report's
sampled-Proxy-CPU medians (Batch 9.7% → 78.7%; Streaming 11.7% → 22.6%) and the
QPS crossover (S/B QPS 0.973x at N1 → 1.709x at N32).

### 1.2 Batch CPU composition N1 → N32 (flat % of total)

| Function (class) | N1 | N2 | N4 | N8 | N16 | N32 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `runtime.memmove` (copy) | 12.1 | 17.4 | 17.1 | 22.3 | 19.3 | 20.9 |
| `consumeFloatSlice` (protobuf decode) | 21.3 | 16.5 | 17.8 | 16.0 | 16.3 | 14.6 |
| `linux.Syscall6` (network) | 22.9 | 17.0 | 17.7 | 14.8 | 13.9 | 13.0 |
| `protowire.ConsumeFixed32` (decode) | 7.2 | 9.4 | 8.1 | 10.1 | 10.1 | 10.4 |
| `runtime.memclrNoHeapPointers` (zero) | 5.6 | 6.7 | 7.4 | 8.9 | 8.3 | 6.1 |
| copy+decode+zero (sum) | 46.2 | 50.0 | 50.4 | 57.3 | 54.0 | 52.0 |

Batch composition is **stable**: copy (`memmove`), protobuf float decode
(`consumeFloatSlice` + `ConsumeFixed32`), zeroing (`memclr`), and network
syscalls together are ~50–57% of flat time at every fan-in level. The total
grows because each of these scales with the bytes transferred (payload ×
children); the mix barely changes.

### 1.3 Batch reduction machinery (cumulative %)

| Function | N2 | N4 | N8 | N16 | N32 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `ReduceByPKOperator.mergeByPK` | 12.5 | 14.6 | 20.5 | 24.4 | 32.7 |
| `typeutil.SelectMinPK` | 1.5 | 2.3 | 5.4 | 8.8 | 16.6 |
| `buildMergedVectorField` | 8.4 | 9.5 | 11.4 | 10.4 | 11.3 |
| `typeutil.GetPK` | 1.5 | 1.4 | 3.6 | 5.0 | 8.6 |
| `typeutil.GetSizeOfIDs` | – | 0.7 | 1.8 | 3.6 | 6.5 |

`mergeByPK` grows from 12.5% to 32.7% cumulative with fan-in. `-peek` shows
`mergeByPK` (called 100% from `ReduceByPKOperator.Run`) spends 50.6% of its
time in `SelectMinPK` (min-PK selection across children), 35.6% in
`buildMergedRetrieveResults`, 9.2% in `mapaccess2` — i.e. the batch reduce
cost is proportional to children × rows. Traces confirm `consumeFloatSlice`
sits under `unmarshalPointerEager` while decoding the child vector payloads.

### 1.4 Streaming CPU composition N1 → N32 (flat %)

| Function (class) | N1 | N2 | N4 | N8 | N16 | N32 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `linux.Syscall6` (network) | 16.5 | 18.5 | 17.9 | 18.1 | 24.3 | 27.9 |
| `runtime.memmove` (copy) | 17.9 | 17.8 | 16.3 | 18.3 | 12.2 | 10.0 |
| `consumeFloatSlice` (decode) | 10.4 | 11.6 | 12.2 | 10.0 | 10.0 | 12.1 |
| `runtime.memclrNoHeapPointers` (zero) | 9.6 | 9.6 | 7.8 | 11.1 | 9.9 | 9.2 |
| `protowire.ConsumeFixed32` (decode) | 5.5 | 4.2 | 5.2 | 5.4 | 4.7 | 7.3 |

### 1.5 Where does Streaming's smaller CPU go at N32?

Top cumulative chain at N32 Streaming (30s window, 10.06s total):

```
getReadyBuffers.func1          40.7%  ← per-child receive loop
  grpcReduceStream.Recv        32.6%
    viewpb QueryOnViewStreamClient.Recv / otelgrpc / clientStream.RecvMsg
      grpc.recv                32.4%
        releaseCodec.Unmarshal 31.3%  ← protobuf decode of Chunks
          readFrame/readDataFrame / io.ReadAtLeast / Syscall6 (27.9% flat)
```

`-peek` on `Syscall6` shows 97.9% of its flat time comes from
`syscall.RawSyscall6` (active net reads/writes; `EpollWait` is <2%), so the
syscall share is real I/O work, not idle epoll sampling.

**Answer:** Streaming's smaller CPU goes into (a) network receive — polling and
reading many child streams (Syscall6 becomes the top flat item, 27.9% at N32);
(b) per-Chunk protobuf decode (`consumeFloatSlice` 12.1% flat / 21.4% cum +
`ConsumeFixed32` 7.3%); (c) copy/zeroing (19%); and (d) the incremental reduce
loop (`OrderedReduceStream.Recv` 9.2% cum, `getReadyBuffers` 1.3%,
`retrieveUnit.pk` 4.6%, `AppendFieldData` 1.6%). **At N32 the single largest
item is the network receive/syscall path**, unlike Batch where copy + decode
dominate. Notably, streaming never calls `SelectMinPK` (0 hits in traces): it
merges ordered unit streams by `retrieveUnit.pk` and builds the merged vector
field per Chunk inside `Recv` (`buildRetrieveChunk` → `buildMergedRetrieveResults`
→ `buildMergedFieldData` → `buildMergedVectorField`), so it avoids Batch's
whole-result PK selection.

---

## 2. Chunk sweep (`test_0917_02`) — Streaming CPU from Chunk 256 → 8192

### 2.1 Total sampled CPU

| Profile | N16 (s / %wall) | N32 (s / %wall) |
| --- | ---: | ---: |
| Batch-1024 | 27.41 / 91.4% | 35.90 / 119.7% |
| Streaming-256 | 5.70 / 19.0% | 6.48 / 21.6% |
| Streaming-1024 | 7.42 / 24.7% | 10.35 / 34.5% |
| Streaming-8192 | 23.95 / 79.8% | 37.09 / 123.6% |

Streaming CPU grows ~4–6x from Chunk 256 to 8192 (256→8192: 5.70→23.95s at N16,
6.48→37.09s at N32), consistent with the report's sampled CPU (N32: 17.8% →
78.4%; Batch 77.5%).

### 2.2 Composition shift (flat %; N32 shown, N16 in same direction)

| Function | Streaming-256 | Streaming-1024 | Streaming-8192 | Batch-1024 |
| --- | ---: | ---: | ---: | ---: |
| `linux.Syscall6` | **28.2** | 29.1 | 19.8 | 11.7 |
| `consumeFloatSlice` | 6.8 | 10.9 | **20.0** (cum 37.0) | 15.0 (cum 26.4) |
| `ConsumeFixed32` | 4.6 | 5.4 | **14.0** | 9.3 |
| `runtime.memmove` | 9.7 | 14.4 | 17.1 | 20.4 |
| `runtime.memclrNoHeapPointers` | 4.9 | 9.0 | 11.0 | 6.9 |

**Answer:** smaller Chunks make the Proxy **syscall-bound** (many small
messages: 32 chunks × children per result → the receive loop dominates);
larger Chunks make it **decode/copy-bound** (fewer, bigger messages: at 8192 a
Chunk carries the child's entire result, so protobuf decode and copying of the
full payload dominate, mirroring Batch's composition). `-peek` at N32-8192:
`releaseCodec.Unmarshal` (55.9% cum) is called 100% from `grpc.recv` and splits
into `proto.Unmarshal` (68.6% = 14.2s, i.e. `consumeFloatSlice` decode) and
`mem.BufferSlice.MaterializeToBuffer` (31.4% = 6.5s, i.e. wire-buffer copy).

At Chunk 8192 (single-Chunk granularity) Streaming CPU (37.09s) matches/exceeds
Batch (35.90s): there is no per-message overhead left to save, and the reduce
machinery is pure extra work on top of the same payload. This is the profile
explanation for the report's finding that Chunk sizes 256–1024 are the
practical operating range.

---

## 3. Metrics validation (`test_0916_01`) — instrumentation overhead in the profile

Only **metrics-on** CPU profiles were preserved (no metrics-off CPU profile
exists in the run directory), so the profile cannot quantify a metrics-on/off
delta directly; it shows the absolute share of instrumentation-adjacent work
under metrics-on, and the delta evidence comes from the report's runtime
counters.

This is an N1 ANN Search workload (IDs + scores, ~81 KB results), so absolute
CPU is tiny: Batch 1.38s / 4.60% wall, Streaming 2.42s / 8.07%.

| Item | Batch metrics-on | Streaming metrics-on |
| --- | --- | --- |
| Top flat | `Syscall6` 13.0%, `futex` 7.3%, `memclr` 5.1%, `fastpb.consumeVarint` 3.6% | `futex` 5.8%, `AppendFieldData` 5.4%, `nextFreeFast` 5.0%, `Syscall6` 4.1%, `GetPK` 3.7% |
| Instrumentation-visible | `fastpb.UnmarshalSearchResultData` 7.97% cum (fast ID/result decode); `mlog/zap` logging ~6.5–8.7% cum; OTel spans ~2.2% cum | OTel gRPC `RecvMsg` 7.0% cum; `mlog.Info` 2.1% cum; OTel spans ~1.2% cum |
| Reduce/stream path (cum) | – | `OrderedReduceStream.Recv` 33.9% → `getReadyBuffers.func1` 25.2% → `consumeResultStream` 19.4% → `createOutputChunk`/`buildSearchChunk` 16.5%; `mallocgc` 16.9% |

**Answer:** the profile-visible instrumentation footprint is small. The most
directly attributable item is Batch's `fastpb.UnmarshalSearchResultData`
(~8% cum) — the metrics path's fast decode of result IDs for inspection —
plus background `mlog`/zap logging and OTel span creation (a few % cum; these
run regardless of the metrics switch). The material overhead is **not CPU**:
per the report, enabling metrics changed QPS by −1.2% (Batch) / 0.0%
(Streaming), sampled CPU +0.2–0.3 pp, but Batch per-operation allocation rose
66.8% (985.9 → 1,643.8 KiB) with GC cycles 12 → 20 — allocation/GC work that a
CPU profile under this light N1 load (4–8% wall) can barely resolve. The
`fastpb` decode share is the profile-side corroboration of that deep
result-inspection cost.

---

## 4. N1 plain-query baseline (`test_0916_02`)

Batch 2.50s / 8.33%; Streaming 3.57s / 11.90% — Streaming is a ~1.4x CPU
overhead at N1 (no redundant child result to save), matching the report's
QPS 0.983x and p95 1.030x. Composition: Batch `Syscall6` 24.4%,
`consumeFloatSlice` 20.0% (cum 29.6%), `memmove` 10.4%; Streaming `Syscall6`
16.0%, `consumeFloatSlice` 13.7% (cum 21.3%), `memmove` 12.6%, `memclr` 12.0%,
with `OrderedReduceStream.Recv` 10.4% cum and `clientStream.RecvMsg` 33.6% cum
(the chunk receive loop). The 0917_01 N1 profiles reproduce these numbers
(Batch 2.49s vs 2.50s; Streaming 3.64s vs 3.57s).

---

## 5. Concurrency sweep (`test_0917_03`) — PENDING

At analysis time the run (`runs/20260917T123310Z-732/`) had not yet captured
any CPU profiles (still in `conc1` repetitions). Expected layout per
`run_test.sh`: `FANIN-N16/profiles/{batch,streaming}-conc{1,32}/cpu.pb.gz`
(4 profiles: concurrency 1/32 × Batch/Streaming, Chunk 256, N16). When the run
finishes, fold in by repeating Section 1–2 commands on those 4 profiles and
answering: (a) how per-op CPU scales under concurrency 1→32 (does the
reduction work amortize, or does contention/GC grow per-op cost?); (b) whether
the Batch/Streaming CPU gap holds at concurrency 32 (expect Streaming to remain
below Batch per the memory-bounded design, but the gap to shrink under
saturation); (c) whether Batch crosses into sustained multi-core saturation
(>100% wall) before Streaming does.

---

## 6. Flamegraph / call-graph evidence

Committed under [`cpu_profiling_evidence/`](cpu_profiling_evidence/):

- `flamegraph/` — **one flamegraph PNG per profile, organized by experiment**
  (`flamegraph/test_0917_01/N32-batch.png`, ...), rendered from the pprof web
  UI Flame Graph tab with headless Chrome; see
  [`flamegraph/INDEX.md`](cpu_profiling_evidence/flamegraph/INDEX.md).
- `svg/` — 11 `go tool pprof -svg` call graphs (graphviz 16.1.0):
  `0917_01-N{1,32}-{batch,streaming}.svg`,
  `0917_02-N16-streaming-{256,8192}.svg`,
  `0917_02-N32-streaming-8192.svg`, `0917_02-N32-batch-1024.svg`,
  `0916_01-{batch,streaming}-metrics-on.svg`, `0916_02-streaming.svg`.
- `top30/` — `-top -nodecount=30` text tables for all 24 profiles.

The `-svg` files are pprof call graphs, not flame graphs. For the interactive
flamegraph view (no graphviz needed): `go tool pprof -http=:8081 <profile>`.

---

## 7. Conclusions

### Verified (from profiles)

1. **Batch Proxy CPU is payload-bound and fan-in-proportional.** Copy
   (`memmove`), protobuf float decode (`consumeFloatSlice` +
   `ConsumeFixed32`), zeroing (`memclr`), and network syscalls are the top flat
   items at every fan-in level (~50–57% combined); total CPU grows 2.49s →
   34.49s (N1→N32). The batch reduce (`mergeByPK`) grows 12.5% → 32.7% cum and
   is dominated by `SelectMinPK` (50.6% of it) plus `buildMergedRetrieveResults`
   (35.6%) — cost scales with children × rows.
2. **Streaming Proxy CPU is receive-loop-bound and fan-in-mild.** Total grows
   only 3.64s → 10.06s (N1→N32); at N32 the top flat item is `Syscall6`
   (27.9%, ~98% active net reads per `-peek`), with the top cumulative chain
   `getReadyBuffers.func1 → grpcReduceStream.Recv → grpc.recv →
   releaseCodec.Unmarshal` (31.3% cum). Streaming avoids `SelectMinPK`
   entirely (0 trace hits) and merges ordered units via `retrieveUnit.pk`,
   building merged fields per Chunk inside `Recv`.
3. **Chunk size is a composition switch for Streaming.** 256 → syscall-bound
   (many messages); 8192 → decode/copy-bound (single big message), at which
   point Streaming CPU (37.09s at N32) meets/exceeds Batch (35.90s) —
   consistent with the report's finding that 256–1024 is the practical range.
4. **At N1 Streaming is a small CPU overhead** (3.64s vs 2.49s), matching the
   QPS/latency overhead measured at N1.
5. **Instrumentation CPU footprint is small; its cost is allocation.** The only
   clearly instrumentation-attributable profile item is Batch's
   `fastpb.UnmarshalSearchResultData` (~8% cum); the material measured overhead
   is Batch allocation +66.8% / GC 12→20 cycles, not CPU.

### Hypotheses (not independently verified here)

- The `memmove` share mixes gRPC wire-buffer materialization
  (`MaterializeToBuffer` = 6.5s of the 20.75s under `releaseCodec.Unmarshal`
  at N32-8192) with reduction merge copying; without code-level markers the
  split is inferred from call paths.
- `GetSizeOfIDs`/`GetPK` cum growth with fan-in suggests per-op O(children ×
  result) PK processing in batch reduce; the exact per-op scaling needs the
  concurrency run.
- Some Syscall6 samples may include short futex/epoll wakeups; `-peek` shows
  <2% are `EpollWait`, so the share is overwhelmingly active I/O.

### Cross-check

The QPS/latency/RSS medians in `test_0917_01/report.md` and
`test_0917_02/report.md` move with the profile totals (Batch CPU climbs
steeply with fan-in and with Chunk size; Streaming stays low except at
single-Chunk granularity), so CPU attribution, RSS, and throughput agree.

## Reproduction

```bash
# per profile
go tool pprof -top -nodecount=30 <cpu.pb.gz>
go tool pprof -traces <cpu.pb.gz>
go tool pprof -top -cum -nodecount=15 <cpu.pb.gz>
go tool pprof -peek='mergeByPK|releaseCodec.Unmarshal|Syscall6' <cpu.pb.gz>
go tool pprof -svg <cpu.pb.gz> > out.svg   # requires graphviz dot
```
