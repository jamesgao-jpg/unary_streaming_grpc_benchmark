# CPU Flamegraphs (pprof web UI)

One flamegraph PNG per preserved Proxy CPU profile, organized by experiment.
Generated 2026-09-17 from the preserved `cpu.pb.gz` artifacts only (no
re-profiling).

## How these were produced

For each profile:

```bash
go tool pprof -http=127.0.0.1:<port> -no_browser <cpu.pb.gz> &   # web UI
# headless Chrome screenshot of the Flame Graph tab (default view):
#   http://127.0.0.1:<port>/ui/flamegraph
Google\ Chrome --headless=new --disable-gpu --hide-scrollbars \
  --window-size=2400,1600 --virtual-time-budget=12000 \
  --screenshot=<name>.png http://127.0.0.1:<port>/ui/flamegraph
```

The PNG is the pprof Flame Graph (d3-flame-graph): stacked by call stack,
width proportional to sampled CPU time, `total` = 100% of the profile window.
For the interactive version (zoom/search), run
`go tool pprof -http=:8081 <cpu.pb.gz>` and open the Flame Graph tab.

## Layout

| Experiment | Content | Files |
| --- | --- | --- |
| `test_0916_01/` | metrics validation (ANN Search N1, metrics-on) | `batch-metrics-on.png`, `streaming-metrics-on.png` |
| `test_0916_02/` | N1 plain-query baseline | `batch.png`, `streaming.png` |
| `test_0917_01/` | fan-in sweep, Chunk 1024 | `N{1,2,4,8,16,32}-{batch,streaming}.png` (12) |
| `test_0917_02/` | Chunk sweep N16/N32 | `N{16,32}-batch-1024.png`, `N{16,32}-streaming-{256,1024,8192}.png` (8) |

24 PNGs, ~9.5 MiB total, 2400x1600 each.

## Quick orientation (matches `cpu_profiling_analysis.md`)

- `test_0917_01/N32-batch.png` — wide copy/decode towers (`runtime.memmove`,
  `consumeFloatSlice`); reduce block `mergeByPK → SelectMinPK`.
- `test_0917_01/N32-streaming.png` — flat receive-heavy graph; `Syscall6` is
  the widest leaf; no `SelectMinPK` tower.
- `test_0917_02/N16-streaming-256.png` — thin repeated frames (many small
  Chunks), syscall-dominated.
- `test_0917_02/N16-streaming-8192.png` — one wide decode tower per child
  (`consumeFloatSlice`), visually Batch-like.

`test_0917_03` (concurrency sweep) had no CPU profiles at analysis time; its 4
profiles (`FANIN-N16/profiles/{batch,streaming}-conc{1,32}/cpu.pb.gz`) can be
added here when the run finishes.
