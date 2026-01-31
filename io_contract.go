package fileproc

import "io"

// ============================================================================
// Internal I/O backend contract
// ============================================================================
//
// The file processor pipeline (processDir/processTree and the worker/pipeline
// code in fileproc_workers.go) is written against a small set of unexported,
// platform-dependent functions and types.
//
// Those symbols form an internal *backend contract* that each supported OS
// group must provide via build-tagged files.
//
// This file intentionally contains **no runtime dispatch** (no interfaces used
// by the hot path). Instead, it uses compile-time assignments to:
//   - document the required surface area
//   - ensure each build provides the expected functions/methods
//
// Implementations live in build-tagged backend files:
//   - Linux fast path:                 io_linux.go
//   - Mainstream non-Linux Unix:       io_unix.go
//   - "Other" platforms (windows/etc): io_other.go
//
// Naming:
//
//   - readDirBatchImpl is the backend hook that streams directory entries into a
//     pathArena and optionally reports discovered subdirectories.
//
// Semantics notes (expected by the pipeline):
//
//   - Paths passed to openDir are NUL-terminated (NulTermPath with a trailing 0),
//     as produced by NewNulTermPath().
//
//   - Directory enumeration fills a *pathArena*. Every entry stored in
//     pathArena.entries includes both the full path (NUL-terminated) and
//     the basename (NUL-terminated) so it can be passed directly to Unix
//     syscalls (openat/fstatat).
//
//   - Recursive mode is signaled by a non-nil reportSubdir callback passed to
//     readDirBatch. Non-recursive mode passes reportSubdir=nil.
//
//     Backends must:
//       - call reportSubdir(entryName) for each discovered subdirectory if and
//         only if reportSubdir != nil
//       - pass entryName as NulTermName (with trailing NUL terminator)
//       - treat entryName as ephemeral (it may point into a reusable buffer);
//         reportSubdir must copy it if it needs to retain it
//
//   - fileHandle.readInto must report directories via (isDir=true, err=nil)
//     rather than returning an error. This lets the pipeline skip directories
//     (including races where a path changes type between readdir and open/read)
//     without turning them into user-visible IOErrors.
//
//
// The package-level behavior (symlink handling, which file types are processed,
// etc.) is documented in fileproc.go; backends must implement that behavior.

// Function signatures required by the pipeline.
var (
	_ func(nulTermPath) (dirHandle, error)                                              = openDir
	_ func(dirHandle, nulTermPath, []byte, string, *pathArena, func(nulTermName)) error = readDirBatchImpl
)

// Method sets required by the pipeline.
// These interfaces are only used for compile-time checking.
type (
	ioDirHandle interface {
		closeHandle() error
		openFile(name nulTermName) (fileHandle, error)
		statFile(name nulTermName) (Stat, statKind, error)
	}

	ioFileHandle interface {
		io.Reader
		closeHandle() error
		readInto(buf []byte) (n int, isDir bool, err error)
		fdValue() uintptr
	}
)

var (
	_ ioDirHandle  = dirHandle{}
	_ ioFileHandle = fileHandle{}
)
