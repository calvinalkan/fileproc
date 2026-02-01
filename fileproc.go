// Package fileproc provides ultra-fast parallel file processing and watching.
//
// # Processing
//
// [Process] scans directories and invokes callbacks for each file, with
// concurrent execution, efficient memory reuse, and platform-specific IO
// fast paths (Linux getdents/openat) where available.
//
// # Watching
//
// [Watcher] provides high-performance polling-based file watching that works
// identically across all platforms without inotify/kqueue/FSEvents. It detects
// file creates, modifications, and deletions by comparing periodic directory
// scans.
//
// # Features
//
//   - Zero per-file allocations after warmup
//   - Scales to 1M+ files
//   - Cross-platform with consistent behavior
//   - Configurable concurrency and chunking
//   - Suffix filtering and recursive traversal
package fileproc

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
)

// ErrSkip signals that the callback wants to skip this file without error.
// Modeled after filepath.SkipDir.
var ErrSkip = errors.New("skip")

// Process scans a directory and invokes fn for each matching regular file.
//
// By default, only the specified directory is scanned. Use [WithRecursive] to
// include subdirectories. Use [WithSuffix] to filter by name suffix (empty
// suffix matches all files).
//
// Callbacks may run concurrently; results are unordered.
//
// Each callback receives:
//   - *File: lazy access to path, metadata, and content (AbsPath, Stat,
//     Bytes, Read, Fd).
//   - *FileWorker: reusable low-level memory helpers (Buf, RetainBytes).
//
// File and FileWorker data are only valid during the callback; do not retain
// *File or any borrowed buffers/slices beyond the callback.
//
// # Errors:
//   - Directory open/readdir errors are reported as [IOError].
//   - Errors returned by fn are wrapped as [ProcessError].
//   - File/dir close errors are ignored.
//   - Other errors may be returned for invalid inputs (e.g., NUL in path/suffix).
//   - If an entry changes type between scan and use (e.g., becomes a directory
//     or symlink), [File] methods return a skip error that Process silently
//     ignores when returned by the callback.
//
// To stop processing on error, use [WithOnError] with a cancelable context:
//
//	ctx, cancel := context.WithCancelCause(ctx)
//	results, errs := Process(ctx, path, fn, WithOnError(func(err error, _, _ int) bool {
//	        cancel(err)
//	        return true
//	}))
//
// # Cancellation
//
// Cancellation stops processing as soon as possible. Process returns whatever
// results and errors were already produced/collected before cancellation was
// observed. It does not guarantee that all already-discovered files are
// processed before returning.
//
// Cancellation itself is NOT ADDED to the error slice; check ctx.Err() (or
// context.Cause(ctx) when using [context.WithCancelCause]).
//
// # Results and errors
//
// Process returns ([]*T, []error). Results are the non-nil values returned by fn.
// Errors include [IOError] and [ProcessError], collected per [WithOnError], plus
// any immediate validation errors returned before processing starts. If OnError
// is nil, all errors are collected.
//
// # Concurrent Modifications
//
// Files or directories created during processing (e.g., by a callback) may or
// may not be seen depending on timing. Do not rely on newly created entries
// being processed in the same call.
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
// [Process] provides a *File for lazy access to stat/content and a *FileWorker
// for reusable temporary buffers. Use [File.Bytes] for full-content reads or
// [File.Read] for streaming access.
//
// Example:
//
//	type Result struct {
//	    Size   int64
//	    Prefix []byte
//	}
//
//	opt := fileproc.WithSizeHint(4 * 1024)
//
//	results, errs := fileproc.Process(ctx, dir, func(f *fileproc.File, w *fileproc.FileWorker) (*Result, error) {
//	    st, err := f.Stat()
//	    if err != nil {
//	        return nil, err
//	    }
//
//	    data, err := f.Bytes(opt)
//	    if err != nil {
//	        return nil, err
//	    }
//
//	    prefix := data
//	    if len(prefix) > 16 {
//	        prefix = prefix[:16]
//	    }
//
//	    return &Result{
//	        Size:   st.Size,
//	        Prefix: w.RetainBytes(prefix),
//	    }, nil
//	}, fileproc.WithRecursive(), fileproc.WithSuffix(".log"))
func Process[T any](ctx context.Context, path string, fn ProcessFunc[T], opts ...Option) ([]*T, []error) {
	if ctx.Err() != nil {
		return nil, nil
	}

	if strings.IndexByte(path, 0) >= 0 {
		return nil, []error{&IOError{Path: ".", Op: "open", Err: errors.New("contains NUL byte")}}
	}

	path, err := filepath.Abs(path)
	if err != nil {
		// fails only os.getwd()
		return nil, []error{fmt.Errorf("file absolute path: %w", err)}
	}

	cfg := applyOptions(opts)
	if strings.IndexByte(cfg.Suffix, 0) >= 0 {
		return nil, []error{fmt.Errorf("invalid suffix: %w", errors.New("contains NUL byte"))}
	}

	proc := &processor[T]{
		fn:          fn,
		rootLen:     len(path),
		fileWorkers: cfg.Workers,
		scanWorkers: cfg.ScanWorkers,
		chunkSize:   cfg.ChunkSize,
		suffix:      cfg.Suffix,
		recursive:   cfg.Recursive,
		errNotify:   newErrNotifier(cfg.OnError),
	}

	rootPath := newNulTermPath(path)

	return proc.process(ctx, rootPath)
}

// ProcessFunc is called for each file.
//
// ProcessFunc may be called concurrently and must be safe for concurrent use.
//
// f provides access to [File] metadata and content (AbsPath, Stat, Bytes,
// Read, Fd). w provides reusable temporary buffers (Buf, RetainBytes).
//
// Both f and w are only valid during the callback.
//
// Callbacks are not invoked for directories, symlinks, or other non-regular
// files.
//
// Return values:
//   - (*T, nil): emit the result
//   - (_, ErrSkip): skip this file silently
//   - (nil, nil): treated as error (use ErrSkip to skip intentionally)
//   - (_, error): report as a [ProcessError], except for internal skip errors
//     returned by [File] methods (for example when a file becomes a directory
//     or symlink), which are silently ignored
//
// Whether [ProcessError]s (and [IOError]s) are included in the returned error
// slice depends on [WithOnError]. If OnError is nil, all errors are collected.
//
// Panics are not recovered by this package. Callbacks must not panic; if you
// need to guard against panics, recover inside the callback.
type ProcessFunc[T any] func(f *File, w *FileWorker) (*T, error)

// FileWorker provides low-level memory helpers to avoid per-callback
// allocations and reduce GC pressure.
//
// Use Buf for scratch space that can be reused across callbacks, and
// RetainBytes to copy data into a per-worker arena when you need it to live
// beyond the current callback.
type FileWorker struct {
	// buf is the reusable scratch buffer for the current callback.
	buf []byte
	// retain is the append-only arena for retained data.
	retain []byte
}

// Buf returns a reusable buffer with at least the requested capacity.
//
// Returns a slice with len=0 and cap>=size, ready for append:
//
//	buf := w.Buf(4096)
//	buf = append(buf, data...)  // no alloc if fits in capacity
//
// For use as a fixed-size read target, expand to full capacity:
//
//	buf := w.Buf(4096)
//	buf = buf[:cap(buf)]
//	n, _ := io.ReadFull(r, buf)
//
// The capacity grows to accommodate the largest request seen across all
// files processed by this worker, then stabilizes.
//
// The returned slice is only valid during the current callback.
func (w *FileWorker) Buf(size int) []byte {
	if cap(w.buf) < size {
		w.buf = make([]byte, 0, size)
	}

	return w.buf[:0]
}

// RetainBytes copies b into a per-worker arena and returns a stable subslice.
//
// The returned slice remains valid until [Process] returns; if the caller keeps
// the slice, it remains valid until GC (standard Go slice lifetime).
func (w *FileWorker) RetainBytes(b []byte) []byte {
	if len(b) == 0 {
		return []byte{}
	}

	start := len(w.retain)
	w.retain = append(w.retain, b...)

	return w.retain[start:]
}

// IOError is returned when a file system operation fails.
type IOError struct {
	// Path is the absolute path for the failed operation.
	Path string
	// Op is the operation that failed, i.e. "open", "read", or "readdir".
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

// ProcessError is returned when [ProcessFunc] returns an error.
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

// ============================================================================
// Error notification
// ============================================================================

// errNotifier tracks error counts and calls OnError callback.
// Zero value is valid (no-op). Safe for concurrent use.
type errNotifier struct {
	// mu serializes error count updates and handler execution.
	mu sync.Mutex
	// ioErrs counts IO errors observed so far.
	ioErrs int
	// callbackErrs counts callback errors observed so far.
	callbackErrs int
	// onError is the user-provided handler (serialized).
	onError func(err error, ioErrs, callbackErrs int) bool
}

func newErrNotifier(onError func(err error, ioErrs, procErrs int) bool) *errNotifier {
	if onError == nil {
		return nil
	}

	return &errNotifier{onError: onError}
}

// ioErr increments IO error count and calls callback.
// Returns true if error should be collected, false to discard.
func (n *errNotifier) ioErr(err error) bool {
	if n == nil || n.onError == nil {
		return true
	}

	n.mu.Lock()
	n.ioErrs++
	ioCount, procCount := n.ioErrs, n.callbackErrs
	collect := n.onError(err, ioCount, procCount)
	n.mu.Unlock()

	return collect
}

// callbackErr increments process error count and calls callback.
// Returns true if error should be collected, false to discard.
func (n *errNotifier) callbackErr(err error) bool {
	if n == nil || n.onError == nil {
		return true
	}

	n.mu.Lock()
	n.callbackErrs++
	ioCount, callbackCount := n.ioErrs, n.callbackErrs
	collect := n.onError(err, ioCount, callbackCount)
	n.mu.Unlock()

	return collect
}
