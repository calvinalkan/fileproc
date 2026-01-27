# Design B — Linux fast path + `os.Root` backend for non-Linux (Go 1.25)

This is a **sketch** of a hybrid approach:

- **Linux:** keep the current syscall-based fast path (getdents64 + openat) implemented in `io_linux.go`
- **Everything else:** replace `io_unix.go` + `io_other.go` with a stdlib `os.Root` backend

The goal is to keep the high-performance, well-understood Linux path, while removing most of the “many OS variants” complexity.

> References below name functions/files; line numbers are intentionally omitted to avoid drift.

---

## 0) What stays the same

The pipeline and orchestration remain intact:

- `processEntry` (`fileproc.go`)
- Non-recursive orchestration: `processor.processDir` (`fileproc.go`)
- Recursive traversal & coordinator: `processor.processTree` (`fileproc.go`)
- Large directory pipeline: `processor.processDirPipelined` (`fileproc_workers.go`)
- Core file loop: `processFilesInto` (`fileproc_workers.go`)

The *only* thing we swap is the implementation behind:

- `openDirEnumerator`
- `readDirBatch`
- `openDir` / `openDirFromReaddir`
- `dirHandle.openFile`
- `fileHandle.readInto` / `closeHandle`

---

## 1) Linux side: keep as-is

Linux already has the fast path we want to preserve (all in `io_linux.go`):

- Raw `openat` helper
- `openDirEnumerator` using `SYS_OPENAT` + `O_DIRECTORY`
- `readDirBatch` using getdents64 parsing
- `openDir` using `SYS_OPENAT`
- `dirHandle` + `fileHandle` with `read(2)`-based `readInto`

**In the hybrid plan:** Linux builds continue to compile `io_linux.go` unchanged.

---

## 2) Non-Linux: replace per-OS open/openat code with `os.Root`

### 2.1 What we want to delete / retire

On non-Linux platforms, most of the complexity is currently spread across:

- `io_unix.go` (openat-based opens + `(*os.File).ReadDir` enumeration)
- `io_other.go` (filepath-based opens + `(*os.File).ReadDir` enumeration)

In a hybrid design we can replace these with **one** implementation:

- new file (sketch): `io_osroot_nonlinux.go` with build tag `//go:build !linux`

### 2.2 The stdlib-backed handle types (non-linux only)

Keep the existing interface surface so the pipeline doesn’t change.

#### `readdirHandle`

```go
//go:build !linux

type readdirHandle struct {
    root *os.Root
    f    *os.File // opened directory file used for ReadDir
}
```

`openDirEnumerator(path []byte)`:
- convert `[]byte` NUL path to string via existing `pathStr`
- `os.OpenRoot(p)`
- open `"."` for enumeration (`root.Open(".")`)

This plugs into:
- non-recursive `processDir`
- tree worker loop

#### `readDirBatch`

Implement using `rh.f.ReadDir(readDirBatchSize)`.

Semantics note:
- if you “don’t care much” about symlinks, the easiest version is: treat symlink entries as files and let `root.Open(...)` decide.
- recursion still only uses `DirEntry.Type().IsDir()` unless you add “try-open-as-dir” fallback.

#### `dirHandle`

```go
//go:build !linux

type dirHandle struct {
    root *os.Root
}
```

- `openDir(path []byte)` → `os.OpenRoot(pathStr(path))`
- `openDirFromReaddir(rh readdirHandle, _ string)` → reuse `rh.root`

This is the key connection to the hot loop:
- `processFilesInto` calls `dh.openFile(name)`.

#### `fileHandle`

```go
//go:build !linux

type fileHandle struct { f *os.File }
```

- `Read` delegates to `f.Read`
- `readInto` delegates to `f.Read` and keeps the existing “(n>0, io.EOF) is ok” behavior

### 2.3 Name/path representation (keep existing `nameBatch`)

The pipeline currently uses `nameBatch` to store NUL-terminated `[]byte` names (`type nameBatch` in `fileproc.go`).

In the hybrid plan we can keep this unchanged (so the pipeline doesn’t change):

- non-linux `readDirBatch` uses `batch.appendString(e.Name())` (already exists in `fileproc_batch_helpers_nonlinux.go`)
- non-linux `dirHandle.openFile(name []byte)` converts `name[:nameLen(name)]` to a string and calls `root.Open(nameStr)`

This means:
- Linux stays allocation-free per entry
- non-linux will do a per-file string conversion for `root.Open` (acceptable if the goal is simplifying the codebase)

---

## 3) Why this hybrid is attractive

- Keeps Linux path performance characteristics (getdents64 + openat) where the wins are huge.
- Replaces *multiple OS-specific openat/open/readdir variants* with one stdlib-backed implementation.
- `os.Root` is maintained by the Go team and already encapsulates a lot of cross-platform edge cases.

---

## 4) Potential follow-ups / optional refinements

- If you want symlink-to-dir recursion on non-linux:
  - when `DirEntry.Type()` isn’t a directory, attempt `rh.root.OpenRoot(e.Name())` and if it succeeds, treat it as a directory job.
- If per-file string conversions on non-linux matter:
  - you could change `nameBatch` to store `[]string` on `!linux` only (requires splitting `nameBatch` by build tag).

---

## 5) Concrete “touch points” (where integration happens)

If you implement the `!linux` backend described above, the existing pipeline will automatically use it via these call sites:

- Directory enumeration:
  - `processor.processDir` (`openDirEnumerator` + `readDirBatch`)
  - tree worker loop (`openDirEnumerator` + `readDirBatch`)

- File opening / reading:
  - `processFilesInto` (`dh.openFile` + `fh.readInto`)

No other changes are required to the coordinator/pipeline logic.
