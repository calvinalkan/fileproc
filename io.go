package fileproc

import "io"

// ============================================================================
// Internal I/O backend contract
// ============================================================================
//
// processor.go is written against a small set of unexported, platform-specific
// functions and types (this is the internal backend contract).
//
// Implementations live in build-tagged backend files:
//   - Linux fast path:                 io_linux.go
//   - Mainstream non-Linux Unix:       io_unix.go
//   - "Other" platforms (windows/etc): io_other.go
//
// readDirBatch is the pipeline entry point for directory enumeration.
//
// Semantics expected by the pipeline:
//
//   - Paths passed to openDir are NUL-terminated (nulTermPath with trailing 0),
//     as produced by newNulTermPath().
//
//   - Directory enumeration fills a *pathArena*. Every entry stored in
//     pathArena.entries is a NUL-terminated basename (for openat/fstatat).
//
//   - Recursive mode is signaled by a non-nil reportSubdir callback passed to
//     readDirBatch. Non-recursive mode passes reportSubdir=nil.
//
//     Backends must:
//       - call reportSubdir(entryName) for each discovered subdirectory if and
//         only if reportSubdir != nil
//       - pass entryName as nulTermName (with trailing NUL terminator)
//       - treat entryName as ephemeral (it may point into a reusable buffer);
//         reportSubdir must copy it if it needs to retain it
//
//   - fileHandle.readInto must report directories via (isDir=true, err=nil)
//     rather than returning an error. This lets the pipeline skip directories
//     (including races where a path changes type between readdir and open/read)
//     without turning them into user-visible IOErrors.
//
// The package-level behavior (symlink handling, which file types are processed,
// etc.) is documented in fileproc.go; backends must implement that behavior.

// Function signatures required by the pipeline.
var (
	_ func(nulTermPath) (dirHandle, error)                                             = openDir
	_ func(dirHandle, nulTermPath, []byte, string, *pathArena, reportSubdirFunc) error = readDirBatch
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

// statKind classifies stat results so callers can skip non-regular files
// without extra syscalls or mode checks.
type statKind uint8

const (
	// statKindReg indicates a regular file.
	statKindReg statKind = iota
	// statKindDir indicates a directory.
	statKindDir
	// statKindSymlink indicates a symlink.
	statKindSymlink
	// statKindOther indicates a non-regular, non-dir, non-symlink entry.
	statKindOther
)
