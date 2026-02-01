# Polling Watcher Spec (fileproc)

Goal: high‑perf polling watcher with **zero per‑pass per‑file allocs** after warm‑up, scalable to 1M files, cross‑platform. Rename support optional; default: delete+create.

## Behavior Summary

- Polling = “changes since last scan”. No inotify / fs events.
- One scan at a time. If scan+delivery exceeds interval, next scan starts immediately (no overlap, no backlog).
- Only regular files; directories/symlinks skipped (same as `Process`).
- Errors: surfaced via handler or returned; cancellation via context.

## API Shapes

### Callback API (fastest)

```
Watch(ctx, opts...)
// options:
WithOnEvent(func(Event))
WithInterval(d time.Duration)
WithRecursive()
WithSuffix(s string)
WithFileWorkers(n int)
WithScanWorkers(n int)
WithChunkSize(n int)
WithExpectedFiles(n int)
```

- Default: call `OnEvent` inline from workers ⇒ **concurrent**, unordered.

### Channel API (sequential by design)

```
Events(ctx, opts...) <-chan Event
// options:
WithEventBuffer(n int)         // channel buffer, default small
```

Default behavior: **block** on backpressure (no drops). Caller controls buffer.

## Event Model

```
Type: Create | Modify | Delete
Path: string (absolute)
Stat: {Size, ModTime, Mode, Inode}
// Rename: not supported by default; modeled as Delete+Create
```

Rename detection optional later (requires stable file ID).

## Internal Refactor Plan (Process + Watcher)

Goal: reuse coordinator + scan workers + chunking. Swap consumer.

- Extract shared “scan engine” that yields `fileChunk{arena, lease, start,end}`.
- `Process()` consumer: current behavior (build `File`, call `ProcessFunc`).
- `Watch()` consumer: `statFile` + diff table + emit events (no abs path build).
- Keep existing backpressure, arena pool, cancellation logic.

## Core Dataflow

```
[coordinator] -> dirJob -> [scan worker]
                     | readDirBatch
                     v
                [pathArena + dirLease]
                     | chunk (start,end)
                     v
                [watch worker]
```

Watch worker per chunk:

```
for name in entries:
  st = statFile(dirLease.dh, name)
  key = hash(base+sep+name)   // no abs path build
  rec = table.lookup(key, nameBytes)
  if !rec:
     path = copy base+sep+name into persistent arena
     insert rec; emit CREATE
  else if st != rec.st:
     update rec; emit MODIFY
  rec.gen = curGen
```

After scan completes:

```
for rec in table:
  if rec.gen != curGen:
     emit DELETE
     remove entry (tombstone)
```

## State / Memory Design

### Persistent Arena

- Chunked `[]byte` storage for paths; append‑only; never reuses old bytes.
- Each entry stores `{off, len}` into arena.
- Copy path bytes **once**, only on new file.

### Hash Table (open addressing)

- Key = hash of full path bytes.
- Store: hash + entry index; verify by length+bytes compare to handle collisions.
- Expected load factor 0.7–0.8. `WithExpectedFiles` pre-sizes.

### Entry Record (packed)

```
mtime int64
size  int64
mode  uint32
inode uint64
gen   uint32
pathOff uint32/uint64
pathLen uint16/uint32
hash uint64
```

### Memory Roughness (1M files, avg path 40B)

- arena: ~41 MB
- entries: ~32–48 MB
- hash: ~16–24 MB
- transient scan buffers: ~10–50 MB
- total: ~100–160 MB

Map[string] baseline: ~200–350 MB.

## Perf Knobs (default guidance)

- file workers: 8–16 (I/O bound)
- scan workers: flat=1; recursive=2–4
- chunk size: 256–512 for 1M files
- interval: must be >= scan time

From benchmarks:
- flat_1m_stat ≈ 224–239ms (4.2–4.5M files/sec)
- nested1_1m_stat ≈ 134–145ms (6.9–7.4M files/sec)

So realistic interval: ~250–400ms flat; ~200ms nested (with headroom).

## Rename Support (later)

- Needs stable file ID:
  - Unix/Linux: (dev,inode). Current `Stat` lacks dev ⇒ add internal `statEx` or extend `Stat`.
  - Windows: use file ID + volume (GetFileInformationByHandle). Not in current API.
- Default: rename = Delete+Create.

## Notes

- `pathArena` used by scan is ephemeral; watcher needs its own persistent arena.
- `AbsPathBorrowed()` cannot be stored; must copy if persisted.
- No per‑pass per‑file allocs after warmup; only new files allocate path bytes.

## Testing + Feedback Loop

### Unit tests

- Deterministic time: use `testing/synctest` (Go 1.25) to avoid real sleeps.
  - Wrap test in `synctest.Test` (note: `Run` is deprecated).
  - `t.Parallel()` is allowed **outside** the bubble (before `synctest.Test`), but panics inside.
  - Start watcher inside bubble; after fs changes call `synctest.Wait()` to let ticker fire.
  - Keep channels/timers inside bubble only; avoid cross-bubble channel ops.
  - `T.Run`, `T.Parallel`, `T.Deadline` not allowed in bubble; `Wait` not concurrent.
  - I/O blocking is not “durably blocked”, so prefer small dirs + explicit scan-done signals to avoid deadlock.
- Scenarios:
  - baseline scan semantics (emit creates vs silent baseline)
  - create/modify/delete
  - recursive + suffix
  - symlink ignored
  - cancellation mid-scan
  - error handling (stat failures)
  - channel backpressure blocks
- Run under `-race`.

### Benchmarks (optimize only after correctness)

Watcherbench (new) focuses on watcher-specific costs:
- Scan overhead vs `Process(stat)` baseline.
- Event latency (p50/p95) from write → event.
- Memory: steady-state RSS/heap for 100k/1m files.

Fileprocbench remains core scan/IO benchmark.

Optimization policy: **only tune after tests pass and baseline established**.

## Metrics

Expose `Stats()` (public, struct; no map alloc). Suggested fields:

- `Scans`, `FilesSeen`, `Creates`, `Modifies`, `Deletes`, `Errors`
- `LastScanNs`, `MaxScanNs`, `LastScanFiles`
- `BehindCount` (scan duration > interval)
- `EventCount` (delivered)
