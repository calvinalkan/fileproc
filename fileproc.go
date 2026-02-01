// Package fileproc provides fast parallel file processing.
//
// It uses platform-specific fast paths where available (for example Linux
// getdents/openat) and falls back to portable APIs on other platforms.
//
// # Symlinks
//
// Symbolic links are not followed. Symlinks to files and directories are
// ignored entirely: they are not recursed into and are not passed to user
// callbacks.
//
// # File types
//
// Only regular files are processed. Directories, symlinks, and other
// non-regular file types (FIFOs, sockets, devices, etc.) are skipped.
//
// # Panics
//
// Panics in user-defined callbacks ([ProcessFunc]) are not recovered by this
// package.
//
// A callback panic will unwind the goroutine executing it; in concurrent modes
// this will crash the process. If you need to guard against panics, recover
// inside your callback.
//
// # Usage
//
// [Process] provides a *File for lazy access to stat/content and a *Worker for
// reusable temporary buffers. Use [File.Bytes] for full-content reads or
// [File.Read] for streaming access.
//
// # Architecture
//
// The package provides a single entry point: [Process]. It supports
// single-directory and recursive tree processing based on [WithRecursive].
//
// Processing uses a tiered strategy based on file count:
//
//	Files ≤ SmallFileThreshold:  Sequential file processing (no pipeline workers)
//	Files > SmallFileThreshold:  Streaming pipeline (overlapped readdir + processing)
//
// # Memory Architecture
//
// Buffers are allocated at orchestration points and passed down to workers.
// The implementation is designed to avoid per-file allocations in steady state;
// allocations may still occur when internal slices/arenas grow.
//
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│ ALLOCATION POINTS                                                       │
//	├─────────────────────────────────────────────────────────────────────────┤
//	│                                                                         │
//	│ Non-recursive (processDir):                                             │
//	│   - dirBuf: 32KB directory-entry buffer on Linux (also used as a        │
//	│     sizing heuristic on other platforms)                                │
//	│   - pathArena: for collecting paths                                     │
//	│   - Pipeline workers allocate their own buffers for:                    │
//	│       • Worker.Buf (callback scratch space)                             │
//	│       • File.Bytes arena (when used)                                   │
//	│                                                                         │
//	│ Recursive (processTree):                                               │
//	│   - per-worker results/errors slices (merged at the end)                │
//	│   - Per tree worker (allocated once, reused for ALL directories):       │
//	│       • dirBuf  (32KB): Linux dirent buffer / sizing heuristic          │
//	│       • Worker.Buf (callback scratch space)                             │
//	│       • pathArena: collecting paths                                     │
//	│       • freeArenas: channel-as-freelist for pipeline arenas           │
//	│                                                                         │
//	└─────────────────────────────────────────────────────────────────────────┘
//
// Rough memory budget for recursive mode with 8 workers (excluding results
// and other overhead), assuming defaults:
//
//	8 × (32KB dirBuf + 6×(≈64KB arena storage + ≈75KB name headers)) ≈ 5–6MB
package fileproc

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// ProcessFunc is called for each file.
//
// ProcessFunc may be called concurrently and must be safe for concurrent use.
//
// f provides access to [File] metadata and content. f.AbsPathBorrowed() is
// ephemeral and only valid during the callback.
//
// w provides reusable temporary buffer space. w.Buf() returns a slice that is
// only valid during the callback.
//
// Callbacks are not invoked for directories, symlinks, or other non-regular
// files.
//
// Return values:
//   - (*T, nil): emit the result
//   - (nil, nil): skip this file silently
//   - (_, error): skip and report the error as a [ProcessError]
//
// Whether [ProcessError]s (and [IOError]s) are included in the returned error
// slice depends on [WithOnError]. If OnError is nil, all errors are collected.
//
// Panics are not recovered by this package. Callbacks must not panic; if you
// need to guard against panics, recover inside the callback.
type ProcessFunc[T any] func(f *File, w *Worker) (*T, error)

// Process processes files in a directory.
//
// By default, only the specified directory is processed. Use [WithRecursive]
// to process subdirectories recursively.
//
// f.AbsPathBorrowed() returns the absolute path. Results are unordered
// when processed with multiple workers.
//
// To stop processing on error, use [WithOnError] with a cancelable context:
//
//	ctx, cancel := context.WithCancelCause(ctx)
//	results, errs := Process(ctx, path, fn, WithOnError(func(err error, _, _ int) bool {
//	        cancel(err)
//	        return true
//	}))
//
// Returns collected results and any errors ([IOError] or [ProcessError]).
//
// # Cancellation
//
// Cancellation stops processing as soon as possible. Process returns whatever
// results and errors were already produced/collected before cancellation was
// observed. It does not guarantee that all already-discovered files are
// processed before returning.
//
// Cancellation itself is not added to the error slice; check ctx.Err() (or
// context.Cause(ctx) when using [context.WithCancelCause]).
//
// # Concurrent Modifications
//
// Files or directories created during processing (e.g., by a callback) may or
// may not be seen depending on timing. Do not rely on newly created entries
// being processed in the same call.
func Process[T any](ctx context.Context, path string, fn ProcessFunc[T], opts ...Option) ([]*T, []error) {
	if strings.IndexByte(path, 0) >= 0 {
		return nil, []error{&IOError{Path: ".", Op: "open", Err: errContainsNUL}}
	}

	path, err := filepath.Abs(path)
	if err != nil {
		// fails only os.getwd()
		return nil, []error{fmt.Errorf("file absolute path: %w", errContainsNUL)}
	}

	if ctx.Err() != nil {
		return nil, nil
	}

	cfg := applyOptions(opts)
	if strings.IndexByte(cfg.Suffix, 0) >= 0 {
		return nil, []error{fmt.Errorf("invalid suffix: %w", errContainsNUL)}
	}

	proc := processor[T]{
		fn:                 fn,
		workers:            cfg.Workers,
		suffix:             cfg.Suffix,
		smallFileThreshold: cfg.SmallFileThreshold,
		errNotify:          newErrNotifier(cfg.OnError),
	}
	rootPath := newNulTermPath(path)

	if cfg.Recursive {
		return proc.processRecursive(ctx, rootPath)
	}

	return proc.processSingleDir(ctx, rootPath)
}

// IOError is returned when a file system operation fails.
type IOError struct {
	// Path is the absolute path for the failed operation.
	Path string
	// Op is the operation that failed: "open", "read", "close", or "readdir".
	Op string
	// Err is the underlying error.
	Err error
}

func (e *IOError) Error() string {
	return fmt.Sprintf("%s %s: %v", e.Op, e.Path, e.Err)
}

func (e *IOError) Unwrap() error {
	return e.Err
}

// ProcessError is returned when a user callback ([ProcessFunc]) returns an error.
type ProcessError struct {
	// Path is the absolute file path (owned, not borrowed).
	Path string
	// Err is the error returned by the callback.
	Err error
}

func (e *ProcessError) Error() string {
	return fmt.Sprintf("process %s: %v", e.Path, e.Err)
}

func (e *ProcessError) Unwrap() error {
	return e.Err
}

var errContainsNUL = errors.New("contains NUL byte")
