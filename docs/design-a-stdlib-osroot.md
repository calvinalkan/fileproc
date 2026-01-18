# Design A — stdlib-only backend using `os.OpenRoot` (Go 1.25)

This is a **sketch** of how to re-implement the *I/O backend* of `fileproc` using only the Go 1.25 standard library (`os.Root`, `(*os.File).ReadDir`, `*os.File.Read`).

The intent is to keep the current **pipeline / coordination / allocation strategy** intact, but replace the platform-specific syscall-heavy backend defined by `io_contract.go` with a stdlib-only implementation.

> References below name functions/files; line numbers are intentionally omitted to avoid drift.

---

## 0) What stays the same (pipeline + orchestration)

These parts are already solid and should remain mostly unchanged:

- Entry orchestration and option defaults: `processEntry` (`fileproc.go`)
- Non-recursive directory mode orchestration: `processor.processDir` (`fileproc.go`)
- Recursive tree mode orchestration/coordinator: `processor.processTree` (`fileproc.go`)
- Core per-file processing loop (borrowed buffers, callback invocation): `processFilesInto` (`fileproc_workers.go`)
- Large-directory producer→worker pipeline: `processor.processDirPipelined` (`fileproc_workers.go`)

All of these are written in terms of **abstract backend operations** like:

- `openDirEnumerator(...)`
- `readDirBatch(...)`
- `openDir(...)` / `openDirFromReaddir(...)`
- `dirHandle.openFile(...)`
- `fileHandle.readInto(...)`

Those are the seams where the stdlib backend would attach.

---

## 1) What gets replaced (OS abstraction points)

Today these are implemented with platform-specific backend files:

- Linux fast path (`io_linux.go`):
  - `openDirEnumerator` (SYS_OPENAT + O_DIRECTORY)
  - `readDirBatch` (getdents64 parsing)
  - `openDir` / raw `openat`
  - `dirHandle` / `fileHandle` (fd-based)

- Mainstream non-Linux Unix (`io_unix.go`):
  - `openDirEnumerator` + `readDirBatch` via `(*os.File).ReadDir`
  - `openDir` / `openat` via syscalls

- “Other” platforms (`io_other.go`):
  - `openDirEnumerator` + `readDirBatch` via `(*os.File).ReadDir`
  - `openDir` / `openFile` via `filepath.Join`

In a stdlib-only design, all of the above can be replaced by a single implementation based on `os.Root`.

---

## 2) Core idea: treat each processed directory as an `os.Root`

### Motivation

- `os.Root` is a stdlib abstraction that internally uses “open relative to a directory handle” (openat-like) behavior on platforms that support it.
- It centralizes platform quirks in the stdlib rather than in `fileproc`.

### Key design choice

Use **one `*os.Root` per directory job** (not one global root for the entire tree) so we preserve the *“open directory once, open many children cheaply”* shape that the current code has.

That means:

- For every directory that `processDir`/`processTree` is about to process, create a directory-scoped root:

```go
r, err := os.OpenRoot(dirPathString) // follows symlinks in the dir name
```

- Enumerate entries using a file opened from the root:

```go
f, _ := r.Open(".")
entries, err := f.ReadDir(readDirBatchSize)
```

- Open each child file by name from the root (no full path join required):

```go
child, err := r.Open(entry.Name())
```

This matches the current shape where `dirHandle` represents “an already-open directory context” used by `processFilesInto` (`fileproc_workers.go`).

---

## 3) Sketch of the stdlib I/O types

You can implement the existing abstractions (`readdirHandle`, `dirHandle`, `fileHandle`) purely with stdlib.

### 3.1 `readdirHandle`

Proposed fields:

```go
// new file: io_osroot.go (no build tags; Go 1.25 only)

type readdirHandle struct {
    root *os.Root
    f    *os.File // directory file opened from root, used for ReadDir
}
```

`openDirEnumerator(path []byte)` replaces the current per-OS versions:

```go
func openDirEnumerator(path []byte) (readdirHandle, error) {
    p := pathStr(path)          // existing helper in fileproc.go
    r, err := os.OpenRoot(p)
    if err != nil { return readdirHandle{...}, err }

    f, err := r.Open(".")
    if err != nil { r.Close(); return ..., err }

    return readdirHandle{root: r, f: f}, nil
}
```

`closeHandle()` closes both.

### 3.2 `readDirBatch` (directory enumeration)

Replace both:
- Linux getdents implementation (`io_linux.go`)
- non-linux ReadDir implementation (`io_unix.go` / `io_other.go`)

with one implementation:

```go
func readDirBatch(rh readdirHandle, _ []byte, suffix string, batch *nameBatch, reportSubdir func([]byte)) error {
    entries, err := rh.f.ReadDir(readDirBatchSize)
    for _, e := range entries {
        // Decide recursion based on Type().IsDir(), or optionally attempt OpenRoot on the name.
        if e.Type().IsDir() {
            if reportSubdir != nil { reportSubdir([]byte(e.Name())) }
            continue
        }

        // If you don't care about symlink semantics, you can treat symlinks as files and just attempt Open.
        // (If it's a symlink to a dir, Open will succeed but Read/probe might return EISDIR.)

        if hasSuffix(e.Name(), suffix) { // helper in fileproc_batch_helpers.go
            batch.appendString(e.Name()) // fileproc_batch_helpers_nonlinux.go
        }
    }
    if err == io.EOF { return io.EOF }
    return err
}
```

Notes:
- For performance, prefer `(*os.File).ReadDir(n)` over `os.ReadDir(name)` to avoid sorting.
- If you *want* to recurse into symlink-to-dir, you can implement “try-open-as-dir”:
  - attempt `rh.root.OpenRoot(e.Name())` and if it succeeds, treat it as a directory job.

### 3.3 `dirHandle` (opening children)

Proposed:

```go
type dirHandle struct {
    root *os.Root
}
```

`openDirFromReaddir` becomes trivial (it already is in the current backends):

```go
func openDirFromReaddir(rh readdirHandle, _ string) (dirHandle, error) {
    return dirHandle{root: rh.root}, nil
}
```

`openDir(path []byte)` for sequential mode (`processFilesSequential`) would:

```go
func openDir(path []byte) (dirHandle, error) {
    r, err := os.OpenRoot(pathStr(path))
    if err != nil { return dirHandle{}, err }
    return dirHandle{root: r}, nil
}
```

### 3.4 `fileHandle`

Proposed:

```go
type fileHandle struct {
    f *os.File
}
```

- `dirHandle.openFile(name []byte)` converts name bytes to string and calls `d.root.Open(nameStr)`.
- `fileHandle.readInto` delegates to `f.Read` and uses the same “treat `io.EOF` as success with short read” policy as the current backends.

---

## 4) Where it plugs into the existing pipeline

No structural changes are required to the pipeline code; the key call sites already abstract I/O:

- Directory open + enumeration in non-recursive mode: `processor.processDir` uses `openDirEnumerator` and `readDirBatch`.
- Directory open + enumeration in tree workers: tree worker loop around `openDirEnumerator(job.abs)` and `readDirBatch(...)`.
- Per-file open/read in hot loop: `processFilesInto` uses `dh.openFile(name)` and `fh.readInto(...)`.

So this is primarily a “replace backend types + functions” exercise.

---

## 5) Expected tradeoffs

**Pros**
- Massive reduction in cross-platform syscall complexity.
- `os.Root` gives you “open relative to directory handle” behavior without implementing `openat` yourself.

**Cons**
- Loses Linux `getdents64` + borrowed `[]byte` name advantages (allocations and `DirEntry` objects return).
- Names are `string` from `ReadDir`, so the fast NUL-terminated `[]byte` path tricks are gone unless you do conversions.

---

## 6) Files likely to become unnecessary

If fully stdlib-only, these become dead code / candidates for removal:

- `io_linux.go`
- `io_unix.go`
- `io_other.go`

(`io_contract.go` remains as the backend contract.)

(You may keep `fileproc_batch_helpers.go`/`*_nonlinux.go` depending on whether `nameBatch` stays byte-based or moves to `[]string`.)
