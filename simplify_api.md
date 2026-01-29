# fileproc API Simplification Plan

## Overview

Consolidate three file processing APIs (`Process`, `ProcessReader`, `ProcessLazy`) into a single
`ProcessLazy` API. Benchmarks show identical performance, and `ProcessLazy` provides a superset
of functionality with arena-backed memory management.

## Current State

### Three APIs

| API | Signature | Purpose |
|-----|-----------|---------|
| `Process` | `func(path, data []byte) (*T, error)` | Read file prefix, pass content directly |
| `ProcessReader` | `func(path []byte, r io.Reader) (*T, error)` | Streaming access via io.Reader |
| `ProcessLazy` | `func(f *File, scratch *Scratch) (*T, error)` | Full control: lazy stat, Bytes(), Read(), Fd() |

### Why consolidate?

1. **Identical performance** — Profiling shows both paths are syscall-bound (~85% in kernel)
2. **ProcessLazy is superset** — Can do everything the others do:
   - `f.Bytes()` → like `Process` but arena-backed
   - `f.Read()` → like `ProcessReader`
   - `f.Stat()` → metadata without opening
   - `f.Fd()` → raw descriptor access
3. **Code duplication** — Three code paths in workers, duplicated tests
4. **Maintenance burden** — Every feature needs implementing 3x

### Performance evidence

From CPU profiling (flat_100k, repeat=10):

| Metric | Process (frontmatter) | ProcessLazy |
|--------|----------------------|-------------|
| Throughput | 1.86M files/s | 1.84M files/s |
| Syscall time | 6.48s (85%) | 6.74s (87%) |
| Memory in-use | 30.2MB | 25.2MB |

Syscall counts (strace):
- Both: 100k openat, 100k read, 100k close
- No stat calls in either path

## Files to Modify

### Source files

| File | Lines | Changes |
|------|-------|---------|
| `fileproc.go` | 1666 | Remove `Process`, `ProcessFunc`, `procKindBytes`. Remove `ProcessReader`, `ProcessReaderFunc`, `procKindReader`. Remove `ReadSize` from Options. Keep shared infrastructure. |
| `fileproc_lazy.go` | 370 | Rename to `fileproc.go` or keep as main API file. May rename `ProcessLazy` → `Process`. |
| `fileproc_reader.go` | 129 | **DELETE** — replay reader only used by ProcessReader |
| `fileproc_workers.go` | 715 | Remove `procKindBytes` and `procKindReader` branches. Simplify to single code path. |

### Test files

| File | Lines | Action |
|------|-------|--------|
| `fileproc_test.go` | 1904 | **DELETE** after migrating unique tests |
| `fileproc_lazy_test.go` | 2135 | Keep + add migrated tests |
| `fileproc_buffer_alias_test.go` | 262 | **DELETE** or migrate buffer aliasing tests |
| `fileproc_buffer_alias_processreader_pipelined_test.go` | 91 | **DELETE** |
| `fileproc_pipelined_processreader_test.go` | 158 | **DELETE** or migrate pipelined tests |
| `fileproc_onerror_io_drop_test.go` | 182 | Migrate OnError tests to lazy |
| `fileproc_onerror_concurrency_test.go` | 348 | Migrate to lazy |
| `fileproc_cancellation_test.go` | 353 | Migrate cancellation tests to lazy |
| `fileproc_cancellation_tree_pipelined_test.go` | 187 | Migrate to lazy |
| `fileproc_tree_concurrency_test.go` | 222 | Migrate to lazy |
| `testing_test.go` | 87 | Keep (test helpers) |

### Benchmark/tooling files

| File | Changes |
|------|---------|
| `cmd/fileprocbench/main.go` | Remove `frontmatter` and `noop` modes, keep only `lazy`. Or rename `lazy` to default. |
| `cmd/benchreport/compare.go` | Update to handle single process mode |
| `.pi/skills/fileproc-benchmark/scripts/bench_profile.sh` | Remove `--process` flag or simplify |
| `.pi/skills/fileproc-benchmark/scripts/bench_sweep.sh` | Remove `--process` flag or simplify |
| `.pi/skills/fileproc-benchmark/scripts/bench_regress.sh` | Remove frontmatter/noop/lazy variants |
| `.pi/skills/fileproc-benchmark/SKILL.md` | Update documentation |

## Test Migration Table

### fileproc_test.go (45 tests)

| Line | Test Name | Status | Notes |
|------|-----------|--------|-------|
| 21 | Test_Process_Returns_TopLevel_Files_When_NonRecursive | DUPLICATE | Covered by lazy:1680 |
| 85 | Test_Process_Applies_Suffix_Filter_When_Recursive | DUPLICATE | Covered by lazy:1764 |
| 130 | Test_Process_Truncates_Prefix_When_ReadSize_Is_Small | DELETE | ReadSize goes away |
| 177 | Test_ProcessReader_Reads_Empty_And_Full_Files_When_Processing | DUPLICATE | Covered by lazy:310,239 |
| 245 | Test_Process_Collects_ProcessError_When_Callback_Returns_Error | DUPLICATE | Covered by lazy:1902 |
| 280 | Test_Process_Drops_Errors_When_OnError_Returns_False | MIGRATE | OnError behavior |
| 328 | Test_Process_Reports_Dot_Path_When_Root_Open_Fails | MIGRATE | Error path |
| 391 | Test_Process_Returns_Error_When_Suffix_Has_Nul | MIGRATE | Input validation |
| 417 | Test_Process_Stops_Early_When_Context_Canceled | MIGRATE | Cancellation |
| 460 | Test_Process_Returns_Same_Paths_When_CopyResultPath_Set | MIGRATE | CopyResultPath option |
| 484 | Test_Process_Processes_All_Files_When_Pipelined_Workers | DUPLICATE | Covered by lazy:1248 |
| 530 | Test_Process_Skips_Result_When_Callback_Returns_Nil | MIGRATE | Nil result handling |
| 560 | Test_ProcessReader_Collects_ProcessError_When_Callback_Returns_Error | DUPLICATE | Covered by lazy:1902 |
| 593 | Test_ProcessReader_Drops_Errors_When_OnError_Returns_False | DUPLICATE | Same as 280 |
| 641 | Test_Process_Returns_No_Results_When_Context_Already_Canceled | MIGRATE | Cancellation |
| 673 | Test_ProcessReader_Returns_No_Results_When_Context_Already_Canceled | DUPLICATE | Same as 641 |
| 705 | Test_Process_Returns_IOError_When_Path_Contains_Nul | MIGRATE | Input validation |
| 748 | Test_ProcessReader_Returns_IOError_When_Path_Contains_Nul | DUPLICATE | Same as 705 |
| 791 | Test_Process_Reports_IOError_When_File_Not_Readable | DUPLICATE | Covered by lazy:1348 |
| 853 | Test_Process_Reports_IOError_When_Subdir_Not_Readable_Recursive | MIGRATE | Subdir error handling |
| 928 | Test_Process_Skips_NonRegular_When_Fifo_Present | MIGRATE | FIFO handling |
| 968 | Test_Process_Returns_No_Results_When_Directory_Empty | MIGRATE | Empty directory |
| 996 | Test_ProcessReader_Returns_No_Results_When_Directory_Empty | DUPLICATE | Same as 968 |
| 1024 | Test_ProcessReader_Applies_Suffix_Filter_When_Recursive | DUPLICATE | Same as 85 |
| 1064 | Test_Process_Uses_Default_ReadSize_When_ReadSize_Not_Set | DELETE | ReadSize goes away |
| 1108 | Test_Process_Propagates_Panic_When_Callback_Panics | MIGRATE | Panic handling |
| 1126 | Test_ProcessReader_Propagates_Panic_When_Callback_Panics | DUPLICATE | Same as 1108 |
| 1144 | Test_ProcessReader_Skips_Result_When_Callback_Returns_Nil | DUPLICATE | Same as 530 |
| 1174 | Test_ProcessReader_Stops_Early_When_Context_Canceled | DUPLICATE | Same as 417 |
| 1214 | Test_Process_Continues_When_OnError_Drops_Errors | MIGRATE | OnError behavior |
| 1263 | Test_ProcessReader_Returns_Same_Paths_When_CopyResultPath_Set | DUPLICATE | Same as 460 |
| 1287 | Test_ProcessReader_Replays_Probe_When_File_Exceeds_Probe_Size | DELETE | ProcessReader-specific |
| 1328 | Test_ProcessReader_Continues_When_OnError_Drops_Errors | DUPLICATE | Same as 1214 |
| 1377 | Test_Process_Returns_IOError_When_Path_Is_File | MIGRATE | Path-is-file error |
| 1420 | Test_Process_Returns_IOError_When_Path_Is_Symlink | MIGRATE | Path-is-symlink error |
| 1469 | Test_Process_Returns_Result_Value_When_Callback_Returns_Value | MIGRATE | Basic result test |
| 1493 | Test_ProcessReader_Returns_Result_Value_When_Callback_Returns_Value | DUPLICATE | Same as 1469 |
| 1517 | Test_Process_OnError_Counts_Are_Cumulative_When_Multiple_Errors | MIGRATE | OnError counts |
| 1571 | Test_ProcessReader_Skips_Symlink_When_File_Is_Symlink | DUPLICATE | Covered by lazy:1680 |
| 1605 | Test_ProcessReader_Skips_NonRegular_When_Fifo_Present | DUPLICATE | Same as 928 |
| 1645 | Test_ProcessReader_Reports_IOError_When_File_Not_Readable | DUPLICATE | Same as 791 |
| 1707 | Test_ProcessReader_Reports_IOError_When_Subdir_Not_Readable_Recursive | DUPLICATE | Same as 853 |
| 1782 | Test_ProcessReader_Returns_Error_When_Suffix_Has_Nul | DUPLICATE | Same as 391 |
| 1814 | Test_ProcessReader_Returns_IOError_When_Path_Is_File | DUPLICATE | Same as 1377 |
| 1857 | Test_ProcessReader_Returns_IOError_When_Path_Is_Symlink | DUPLICATE | Same as 1420 |

### Summary

| Category | Count | Action |
|----------|-------|--------|
| DUPLICATE | 23 | Delete (already covered) |
| MIGRATE | 18 | Port to ProcessLazy |
| DELETE | 3 | API-specific, no longer needed |
| **Total** | 44 | |

### Other test files

| File | Tests | Action |
|------|-------|--------|
| `fileproc_buffer_alias_test.go` | 3 | Migrate buffer aliasing to lazy (arena already tested) |
| `fileproc_buffer_alias_processreader_pipelined_test.go` | 1 | DELETE (ProcessReader-specific) |
| `fileproc_pipelined_processreader_test.go` | 3 | Migrate pipelined tests to lazy |
| `fileproc_onerror_io_drop_test.go` | 2 | Migrate OnError IO drop tests |
| `fileproc_onerror_concurrency_test.go` | 2 | Migrate concurrent error handling |
| `fileproc_cancellation_test.go` | 4 | Migrate cancellation tests |
| `fileproc_cancellation_tree_pipelined_test.go` | 2 | Migrate tree cancellation |
| `fileproc_tree_concurrency_test.go` | 4 | Migrate tree concurrency |

## API Changes

### Before

```go
// Three entry points
func Process[T any](ctx, path, fn ProcessFunc[T], opts) ([]Result[T], []error)
func ProcessReader[T any](ctx, path, fn ProcessReaderFunc[T], opts) ([]Result[T], []error)
func ProcessLazy[T any](ctx, path, fn ProcessLazyFunc[T], opts) ([]Result[T], []error)

// Three callback signatures
type ProcessFunc[T any] func(path []byte, data []byte) (*T, error)
type ProcessReaderFunc[T any] func(path []byte, r io.Reader) (*T, error)
type ProcessLazyFunc[T any] func(f *File, scratch *Scratch) (*T, error)

// Options with ReadSize (only used by Process)
type Options struct {
    ReadSize  int  // DELETE
    // ...
}
```

### After

```go
// Single entry point (consider renaming ProcessLazy → Process)
func Process[T any](ctx, path, fn ProcessFunc[T], opts) ([]Result[T], []error)

// Single callback signature
type ProcessFunc[T any] func(f *File, scratch *Scratch) (*T, error)

// Options without ReadSize
type Options struct {
    // ReadSize removed
    // ...
}
```

## Estimated Impact

### Lines of code

| Category | Before | After | Change |
|----------|--------|-------|--------|
| Source (*.go) | 2880 | ~1500 | -48% |
| Tests (*_test.go) | 5929 | ~2500 | -58% |
| **Total** | 8809 | ~4000 | **-55%** |

### Complexity

- Remove 2 of 3 code paths in `processFilesInto()`
- Remove `procKind` enum (or reduce to single value)
- Remove `replayReader` infrastructure
- Remove `ReadSize` option handling
- Single test suite instead of three overlapping ones

## Implementation Steps

1. **Migrate tests** (18 tests from fileproc_test.go + other files)
2. **Rename ProcessLazy → Process** (or keep name, deprecate others)
3. **Remove Process/ProcessReader** from fileproc.go
4. **Remove procKindBytes/procKindReader** from workers
5. **Delete fileproc_reader.go**
6. **Delete old test files**
7. **Update benchmarks** to use single API
8. **Update documentation**

## Backwards Compatibility

Not a concern — library not yet released publicly.
