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
//	│   - batch: nameBatch for collecting filenames                           │
//	│   - Pipeline workers allocate their own pathBuf and buffers for:        │
//	│       • Worker.Buf (callback scratch space)                             │
//	│       • File.Bytes arena (when used)                                   │
//	│                                                                         │
//	│ Recursive (processTree):                                               │
//	│   - per-worker results/errors slices (merged at the end)                │
//	│   - Per tree worker (allocated once, reused for ALL directories):       │
//	│       • dirBuf  (32KB): Linux dirent buffer / sizing heuristic          │
//	│       • Worker.Buf (callback scratch space)                             │
//	│       • pathBuf (512B): building relative paths                         │
//	│       • batch: collecting filenames                                     │
//	│       • freeBatches: channel-as-freelist for pipeline batches           │
//	│                                                                         │
//	└─────────────────────────────────────────────────────────────────────────┘
//
// Rough memory budget for recursive mode with 8 workers (excluding results
// and other overhead), assuming defaults:
//
//	8 × (32KB dirBuf + 512B pathBuf + 6×(≈64KB batch storage + ≈75KB name headers)) ≈ 6–7MB
package fileproc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// Internal stat classification used by stat-only processing.
type statKind uint8

const (
	statKindReg statKind = iota
	statKindDir
	statKindSymlink
	statKindOther
)

// Stat holds metadata for a file discovered by [Process].
//
// ModTime is expressed as Unix nanoseconds to avoid time.Time allocations
// in hot paths. Use time.Unix(0, st.ModTime) to convert when needed.
type Stat struct {
	Size    int64
	ModTime int64
	Mode    uint32
	Inode   uint64
}

// File provides access to a file being processed by Process.
//
// All methods are lazy: the underlying file is opened on first content access
// (Bytes, Read, or Fd). The handle is owned by fileproc and closed after the
// callback returns. File must not be retained beyond the callback.
//
// Bytes() and Read() are mutually exclusive per file. Calling one after
// the other returns an error.
type File struct {
	dh   dirHandle
	name []byte // NUL-terminated filename

	relPath  []byte // ephemeral relative path (no NUL)
	st       Stat
	statDone bool

	fh     *fileHandle
	fhOpen *bool

	dataBuf   *[]byte // temporary read buffer (reused across files)
	dataArena *[]byte // append-only arena for Bytes() results

	mode    fileMode
	openErr error
	statErr error
}

type fileMode uint8

const (
	fileModeNone fileMode = iota
	fileModeBytes
	fileModeReader
)

var (
	errBytesAfterRead     = errors.New("Bytes: cannot call after Read")
	errBytesAlreadyCalled = errors.New("Bytes: already called")
	errReadAfterBytes     = errors.New("Read: cannot call after Bytes")

	// errSkipFile is an internal sentinel indicating the file should be
	// silently skipped (e.g., became a directory due to race condition).
	// Not reported as an error to the user.
	errSkipFile = errors.New("skip file")
)

// RelPathBorrowed returns the file path relative to the root directory passed to
// Process.
//
// The returned slice is ephemeral and only valid during the callback.
// Copy if you need to retain it.
func (f *File) RelPathBorrowed() []byte {
	return f.relPath
}

// Stat returns file metadata.
//
// Lazy: the stat syscall is made on first call and cached. Subsequent calls
// return the cached value with no additional I/O.
//
// Returns zero Stat and an error if the stat syscall fails (e.g., file was
// deleted or became a non-regular file).
func (f *File) Stat() (Stat, error) {
	if !f.statDone {
		f.lazyStat()
	}

	return f.st, f.statErr
}

// BytesOption configures the behavior of [File.Bytes].
type BytesOption struct {
	sizeHint int
}

// WithSizeHint provides an expected file size to optimize buffer allocation.
//
// Use when file sizes are known or predictable (e.g., uniform log entries,
// fixed-format records) to avoid buffer resizing without a stat syscall.
//
// The hint is a suggestion, not a limit. Files larger than the hint are
// read completely; smaller files don't waste the extra space (only the
// actual content is stored in the arena).
//
// The hint is ignored if [File.Stat] was called previously, since the
// actual size is already known.
//
// Example:
//
//	// Pre-create option outside the processing loop (zero allocation)
//	opt := fileproc.WithSizeHint(4096)
//
//	fileproc.Process(ctx, dir, func(f *fileproc.File, _ *fileproc.Worker) (*T, error) {
//	    data, err := f.Bytes(opt)
//	    // ...
//	}, opts)
func WithSizeHint(size int) BytesOption {
	return BytesOption{sizeHint: size}
}

// Bytes reads and returns the full file content.
//
// The returned slice points into an internal arena and remains valid until
// Process returns. Subslices share the same lifetime.
//
// Empty files return a non-nil empty slice ([]byte{}, nil).
//
// Single-use: Bytes can only be called once per File.
// Returns error if called after [File.Read], or on I/O failure.
//
// Memory: content is retained in the arena until Process returns.
// For large files or memory-constrained use cases, consider [File.Read]
// with streaming processing instead.
//
// Buffer sizing: Bytes does not call stat internally. The buffer size is
// determined by (in priority order):
//  1. The actual size from [File.Stat], if it was called previously
//  2. The hint from [WithSizeHint], if provided
//  3. A default 512-byte buffer, grown as needed
//
// For workloads with known/uniform file sizes, use [WithSizeHint] to avoid
// buffer resizing without the overhead of a stat syscall.
func (f *File) Bytes(opts ...BytesOption) ([]byte, error) {
	if f.mode == fileModeReader {
		return nil, errBytesAfterRead
	}

	if f.mode == fileModeBytes {
		return nil, errBytesAlreadyCalled
	}

	f.mode = fileModeBytes

	// Open file if needed
	openErr := f.open()
	if openErr != nil {
		return nil, openErr
	}

	// Buffer size priority: stat > sizeHint > default
	maxInt := int(^uint(0) >> 1)
	maxSize := maxInt - 1

	var size int

	if f.statDone && f.statErr == nil {
		if f.st.Size > 0 {
			if f.st.Size > int64(maxSize) {
				size = maxSize
			} else {
				size = int(f.st.Size)
			}
		}
	} else if len(opts) > 0 && opts[0].sizeHint > 0 {
		size = min(opts[0].sizeHint, maxSize)
	}

	// +1 to detect growth / read past expected size
	readSize := max(size+1, defaultReadBufSize)

	// Ensure dataBuf capacity
	if cap(*f.dataBuf) < readSize {
		*f.dataBuf = make([]byte, 0, readSize)
	}

	*f.dataBuf = (*f.dataBuf)[:readSize]
	buf := *f.dataBuf

	// Single read syscall using backend
	n, isDir, err := f.fh.readInto(buf)
	if isDir {
		return nil, errSkipFile
	}

	if err != nil {
		return nil, err
	}

	// Buffer was filled - file may be larger, continue reading
	if n == readSize {
		var readErr error

		for {
			if n == len(buf) {
				growBy := max(len(buf), 4096)
				*f.dataBuf = append(*f.dataBuf, make([]byte, growBy)...)
				buf = *f.dataBuf
			}

			m, isDir, err := f.fh.readInto(buf[n:])
			if isDir {
				return nil, errSkipFile
			}

			if err != nil {
				readErr = err
			}

			n += m

			if m == 0 || readErr != nil {
				break
			}
		}

		if readErr != nil {
			return nil, readErr
		}
	}

	// Empty file
	if n == 0 {
		return []byte{}, nil
	}

	// Copy to arena, return subslice
	start := len(*f.dataArena)
	*f.dataArena = append(*f.dataArena, buf[:n]...)

	return (*f.dataArena)[start:], nil
}

// Read implements io.Reader for streaming access.
//
// Use when you need only a prefix or want to process in chunks without
// retaining the full content. Data read via Read() is NOT arena-allocated;
// caller provides and manages the buffer.
//
// Returns error if called after Bytes().
//
// Lazy: file is opened on first Read() call.
func (f *File) Read(p []byte) (int, error) {
	if f.mode == fileModeBytes {
		return 0, errReadAfterBytes
	}

	f.mode = fileModeReader

	err := f.open()
	if err != nil {
		return 0, err
	}

	n, err := f.fh.Read(p)
	if err != nil {
		if errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.ELOOP) {
			return 0, errSkipFile
		}
	}

	return n, err
}

// Fd returns the underlying file descriptor.
//
// Lazy: file is opened if not already open.
//
// Use for low-level operations (sendfile, mmap, etc.). The fd is owned by
// fileproc and will be closed after the callback returns.
//
// Returns ^uintptr(0) (i.e., -1) if the file cannot be opened.
func (f *File) Fd() uintptr {
	err := f.open()
	if err != nil {
		return ^uintptr(0)
	}

	return f.fh.fdValue()
}

func (f *File) lazyStat() {
	st, kind, err := f.dh.statFile(f.name)
	f.statDone = true

	if err != nil {
		if errors.Is(err, syscall.ELOOP) || kind == statKindSymlink {
			f.statErr = errSkipFile
		} else {
			f.statErr = err
		}

		return
	}

	if kind != statKindReg {
		f.statErr = errSkipFile

		return
	}

	f.st = st
}

func (f *File) open() error {
	if f.fhOpen != nil && *f.fhOpen {
		return nil // already open
	}

	if f.openErr != nil {
		return f.openErr // previous attempt failed
	}

	fh, err := f.dh.openFile(f.name)
	if err != nil {
		// Symlink detected (race: was regular file during scan)
		if errors.Is(err, syscall.ELOOP) || errors.Is(err, syscall.EISDIR) {
			f.openErr = errSkipFile

			return errSkipFile
		}

		f.openErr = err

		return err
	}

	*f.fh = fh
	*f.fhOpen = true

	return nil
}

// Worker provides reusable temporary buffer space for callback processing.
//
// The buffer is reused across files within a worker. It is only valid during
// the current callback and will be overwritten for the next file.
//
// Use for temporary parsing work, not for data that must survive the callback.
type Worker struct {
	buf []byte
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
func (w *Worker) Buf(size int) []byte {
	if cap(w.buf) < size {
		w.buf = make([]byte, 0, size)
	}

	return w.buf[:0]
}

// ProcessFunc is called for each file.
//
// ProcessFunc may be called concurrently and must be safe for concurrent use.
//
// f provides access to file metadata and content. f.RelPath() is ephemeral and
// only valid during the callback.
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

// IOError is returned when a file system operation fails.
type IOError struct {
	// Path is the path relative to the root directory passed to [Process].
	// The root directory itself is reported as ".".
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
	// Path is the relative file path (owned, not borrowed).
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

// Internal constants for buffer sizes and limits.
const (
	// dirReadBufSize is the size of the directory-entry read buffer.
	// On Linux it backs getdents64/ReadDirent parsing. On other platforms the
	// code uses os.File.ReadDir and this size is primarily used as a sizing
	// heuristic for batching.
	dirReadBufSize = 32 * 1024

	// maxWorkers caps worker counts (tree traversal workers and non-recursive
	// pipeline workers) to avoid excessive goroutine/memory overhead.
	maxWorkers = 256

	// defaultReadBufSize is the initial buffer size for file reads and
	// path buffer growth heuristics.
	defaultReadBufSize = 512

	// pathBufExtra is extra capacity for path buffers to reduce reallocs
	// when joining directory prefix with filename.
	pathBufExtra = defaultReadBufSize

	// defaultSmallFileThreshold is the default for WithSmallFileThreshold.
	// Below this file count, sequential file processing beats pipelined workers.
	defaultSmallFileThreshold = 1500

	// maxPipelineQueue caps the number of batches buffered between the readdir
	// producer and file-processing workers in non-recursive pipelined mode.
	//
	// This prevents excessive memory usage when Workers is set very high.
	// When the cap is hit, the producer blocks until workers catch up.
	maxPipelineQueue = maxWorkers

	// pipelineBatchCount is the number of nameBatch objects to pre-allocate
	// for the pipelining free-list when running in tree worker context.
	// Formula: 1 (producer) + 4 (channel buffer) + 1 (worker) = 6
	// (Tree workers use Workers=1 for pipelining within a directory).
	pipelineBatchCount = 6
)

var errContainsNUL = errors.New("contains NUL byte")

// workerBufs holds pre-allocated buffers that a worker reuses across all
// files and directories it processes. Passed by pointer to avoid copying
// and to allow growable fields (batch) to expand.
//
// Lifetime: created once per worker goroutine, lives until worker exits.
type workerBufs struct {
	// dirBuf holds raw directory-entry bytes on Linux (getdents64/ReadDirent
	// parsing). On other platforms directory reading uses os.File.ReadDir and
	// dirBuf is not used by readdir itself (its capacity is still used as a
	// sizing heuristic for batching).
	// Sized at 32KB - large enough to read many entries per syscall,
	// small enough to stay in L1 cache.
	// Reused: reset for each directory.
	dirBuf []byte

	// pathBuf is scratch space for building "prefix/filename" paths.
	// Reused: reset (len=0) for each file, capacity preserved.
	pathBuf []byte

	// batch collects filenames read from a directory (arena-style storage).
	// Reused: reset for each directory, internal capacity preserved.
	// See nameBatch comments for the arena allocation pattern.
	batch nameBatch

	// dataBuf is temporary buffer for File.Bytes() read syscall.
	// Sized to st.Size+1, grows to max file size seen. Reused across files.
	dataBuf []byte

	// dataArena is append-only storage for File.Bytes() results.
	// Each Bytes() result is a subslice. Grows across all files, never
	// shrinks. GC'd when results are released.
	dataArena []byte

	// file is a reusable File struct for callbacks.
	// Reset for each file to avoid per-file heap allocation.
	file File

	// worker is a reusable Worker struct for callbacks.
	worker Worker
}

type processor[T any] struct {
	fn ProcessFunc[T]
}

// Process processes files in a directory.
//
// By default, only the specified directory is processed. Use [WithRecursive]
// to process subdirectories recursively.
//
// f.RelPath() returns the path relative to the root. Results are unordered
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
	path = filepath.Clean(path)

	if strings.IndexByte(path, 0) >= 0 {
		return nil, []error{&IOError{Path: ".", Op: "open", Err: errContainsNUL}}
	}

	if ctx.Err() != nil {
		return nil, nil
	}

	cfg := applyOptions(opts)
	if strings.IndexByte(cfg.Suffix, 0) >= 0 {
		return nil, []error{fmt.Errorf("invalid suffix: %w", errContainsNUL)}
	}

	notifier := newErrNotifier(cfg.OnError)
	proc := processor[T]{fn: fn}

	if cfg.Recursive {
		return proc.processRecursive(ctx, path, cfg, notifier)
	}

	return proc.processDir(ctx, path, cfg, notifier)
}

// ============================================================================
// NON-RECURSIVE: SINGLE DIRECTORY PROCESSING
// ============================================================================

// processDir processes files in a single directory (non-recursive).
//
// Allocations (one-shot, not pooled):
//   - dirBuf (32KB): for reading directory entries
//   - batch: for collecting filenames
//
// For large directories, switches to processDirPipelined which allocates
// additional buffers per worker.
func (p processor[T]) processDir(
	ctx context.Context,
	dir string,
	opts options,
	notifier *errNotifier,
) ([]*T, []error) {
	dirPath := pathWithNul(dir)

	dirEnumerator, err := openDirEnumerator(dirPath)
	if err != nil {
		ioErr := &IOError{Path: ".", Op: "open", Err: err}
		if notifier.ioErr(ioErr) {
			return nil, []error{ioErr}
		}

		return nil, nil
	}

	defer func() { _ = dirEnumerator.closeHandle() }()

	dirBuf := make([]byte, 0, dirReadBufSize)
	batch := &nameBatch{}
	batch.reset(cap(dirBuf) * 2)

	for {
		if ctx.Err() != nil {
			return nil, nil
		}

		err := readDirBatch(dirEnumerator, dirBuf[:cap(dirBuf)], opts.Suffix, batch, nil)
		if err != nil && !errors.Is(err, io.EOF) {
			// Best-effort: keep names already read, but surface the readdir error.
			readErr := err

			if len(batch.names) == 0 {
				ioErr := &IOError{Path: ".", Op: "readdir", Err: readErr}
				if notifier.ioErr(ioErr) {
					return nil, []error{ioErr}
				}

				return nil, nil
			}

			results, errs := p.processFilesSequential(ctx, dir, nil, batch.names, notifier)

			ioErr := &IOError{Path: ".", Op: "readdir", Err: readErr}
			if notifier.ioErr(ioErr) {
				errs = append(errs, ioErr)
			}

			return results, errs
		}

		if len(batch.names) > opts.SmallFileThreshold {
			return p.processDirPipelined(ctx, &dirPipelinedArgs{
				dir:           dir,
				relPrefix:     nil,
				dirEnumerator: dirEnumerator,
				initialNames:  batch.names,
				opts:          opts,
				reportSubdir:  nil,
				notifier:      notifier,
				dirBuf:        dirBuf[:cap(dirBuf)],
			})
		}

		if err == nil {
			continue
		}

		if errors.Is(err, io.EOF) {
			break
		}
	}

	if len(batch.names) == 0 {
		return nil, nil
	}

	return p.processFilesSequential(ctx, dir, nil, batch.names, notifier)
}

// ============================================================================
// RECURSIVE: TREE PROCESSING
// ============================================================================

// processRecursive processes files in a directory tree.
//
// Entry point for recursive mode. The root directory is processed like any
// other directory job by the tree workers.
func (p processor[T]) processRecursive(
	ctx context.Context,
	root string,
	opts options,
	notifier *errNotifier,
) ([]*T, []error) {
	return p.processTree(ctx, root, opts, notifier)
}

// processTree processes a directory tree with parallel workers.
//
// Each tree worker:
//   - Owns all its buffers (allocated once at goroutine start)
//   - Processes directories from the jobs channel
//   - Reuses buffers across ALL directories it processes
//   - Discovers subdirectories and adds them to the found channel
//
// Memory architecture:
//
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│ processTree                                                             │
//	│   │                                                                     │
//	│   │ per-worker results/errors slices (merged at the end)                │
//	│   │                                                                     │
//	│   └─► [N tree workers] each owns:                                       │
//	│         │                                                               │
//	│         │ dirBuf  [32KB]  ← reused for ALL directories                  │
//	│         │ Worker.Buf  ← reused for ALL files                            │
//	│         │ pathBuf [512B]  ← reused for ALL paths                        │
//	│         │ batch           ← reused for ALL directories                  │
//	│         │ freeBatches     ← channel-as-freelist for large dirs          │
//	│         │                                                               │
//	│         └─► for job := range jobs:                                      │
//	│               batch.reset()      ← amortized reuse (may grow)           │
//	│               processFilesInto() ← uses worker's buffers                │
//	│                                                                         │
//	└─────────────────────────────────────────────────────────────────────────┘
func (p processor[T]) processTree(
	ctx context.Context,
	root string,
	opts options,
	notifier *errNotifier,
) ([]*T, []error) {
	// Convert root path to NUL-terminated []byte for syscalls.
	rootPath := pathWithNul(root)

	// Per-worker result slices - no mutex contention for results.
	// Each worker appends to its own slice, merged at the end.
	//
	// We build these via append (instead of make(len=opts.Workers)) to keep
	// golangci-lint's makezero happy while still allowing workerID indexing.
	workerResults := make([][]*T, 0, opts.Workers)
	workerErrs := make([][]error, 0, opts.Workers)

	for range opts.Workers {
		workerResults = append(workerResults, nil)
		workerErrs = append(workerErrs, nil)
	}

	type dirJob struct {
		abs []byte // absolute path for syscalls (NUL-terminated)
		rel []byte // relative path for results (nil for root)
	}

	// ========================================================================
	// COORDINATOR GOROUTINE: Dynamic Work Distribution
	// ========================================================================
	//
	// THE PROBLEM: We don't know the tree structure upfront
	//
	// Unlike processing a flat list of files, tree traversal discovers work
	// dynamically. When a worker processes directory "foo/", it might find
	// subdirectories "foo/bar/" and "foo/baz/" that also need processing.
	// We can't pre-partition the work because we don't know it exists yet.
	//
	// THE SOLUTION: A coordinator that acts as a work queue manager
	//
	// The coordinator maintains a queue of directories to process. Workers
	// pull directories from this queue, process them, and push any discovered
	// subdirectories back. The coordinator routes work between these flows.
	//
	// CHANNEL ROLES:
	//
	//   jobs  ←── coordinator sends directories for workers to process
	//   found ──► workers send newly-discovered subdirectories back
	//   doneDir ──► workers signal "I finished processing a directory"
	//
	// WHY THREE CHANNELS?
	//
	// The tricky part is knowing when we're done. We can't just close `jobs`
	// when the queue is empty - a worker might be about to discover more
	// subdirectories. We need to track directories that are "in flight"
	// (dispatched to workers but not yet fully processed).
	//
	// `pending` counts both queued AND in-flight directories. Only when
	// pending hits zero do we know the entire tree has been processed.
	//
	// TERMINATION GUARANTEE:
	//
	// Every directory dispatched via `jobs` will eventually produce exactly
	// one signal on `doneDir`. This one-to-one correspondence ensures
	// `pending` accurately reflects outstanding work, preventing both
	// premature termination and deadlock.
	//
	jobs := make(chan dirJob)                      // workers pull from here
	found := make(chan dirJob, opts.Workers*64)    // workers push discovered subdirs here
	doneDir := make(chan struct{}, opts.Workers*4) // workers signal completion here

	var coordWG sync.WaitGroup

	coordWG.Go(func() {
		queue := make([]dirJob, 0, 1024) // typical tree breadth; grows if needed

		// Seed queue with the root directory itself.
		queue = append(queue, dirJob{abs: rootPath, rel: nil})

		pending := len(queue)
		jobsClosed := false

		// ====================================================================
		// MAIN COORDINATION LOOP
		// ====================================================================
		//
		// This select multiplexes three events:
		//
		// 1. DISCOVERY: Worker found a subdirectory → add to queue
		//    This grows our work dynamically as we explore the tree.
		//
		// 2. COMPLETION: Worker finished a directory → track progress
		//    When pending hits zero, the entire tree has been processed.
		//
		// 3. DISPATCH: Send next directory to an available worker
		//    This is conditional - only enabled when queue is non-empty.
		//    The "nil channel" trick (jobCh is nil when queue empty) makes
		//    the send case block forever, effectively disabling it.
		//
		// WHY NOT A SIMPLER DESIGN?
		//
		// A naive approach might use a mutex-protected queue that workers
		// access directly. But this creates contention - workers would
		// fight over the lock. The coordinator pattern serializes queue
		// access in a single goroutine, using channels for communication.
		// This is faster and easier to reason about.
		//
		for pending > 0 {
			stopping := ctx.Err() != nil

			if stopping {
				if len(queue) > 0 {
					pending -= len(queue)
					queue = queue[:0]
				}

				if !jobsClosed {
					close(jobs)

					jobsClosed = true
				}
			}

			var (
				nextJob dirJob
				jobCh   chan dirJob
			)

			if !stopping && len(queue) > 0 {
				nextJob = queue[0]
				jobCh = jobs
			}

			select {
			case job := <-found:
				if stopping {
					continue
				}

				pending++

				queue = append(queue, job)

			case <-doneDir:
				pending--

				if pending == 0 && !stopping {
					// found is buffered. Drain any already-discovered work before deciding
					// that we're done, otherwise pending accounting can terminate early.
					for {
						select {
						case job := <-found:
							pending++

							queue = append(queue, job)
						default:
							goto drained
						}
					}
				}

			drained:
				if pending == 0 && !jobsClosed {
					close(jobs)

					jobsClosed = true
				}

			case jobCh <- nextJob:
				queue = queue[1:]
			}
		}

		if !jobsClosed {
			close(jobs)
		}
	})

	// ========================================================================
	// TREE WORKER: Directory-Level Parallelism
	// ========================================================================
	//
	// PARALLELISM STRATEGY: Why parallelize across directories, not within?
	//
	// Consider two approaches for a tree with 100 directories, 50 files each:
	//
	//   A) 8 workers, each processes multiple directories sequentially
	//      → 8 directories processed in parallel at any moment
	//      → Each worker reads its directory, then reads its files
	//
	//   B) 1 worker per directory, but 8 file-workers within each directory
	//      → 1 directory at a time, 8 files read in parallel
	//      → Must finish one directory before starting the next
	//
	// Approach A wins because:
	//   - Directory I/O is the bottleneck (readdir syscalls, inode lookups)
	//   - Files within a directory are often physically close on disk
	//   - Fewer synchronization points (no coordination within directories)
	//
	// BUFFER OWNERSHIP: Why each worker owns its buffers?
	//
	// Alternative: Use sync.Pool to share buffers across workers.
	// Problem: Pool introduces lock contention and unpredictable lifetimes.
	//
	// Our approach: Each worker allocates buffers once at startup, reuses
	// them for EVERY directory it processes. A worker processing 1000
	// directories allocates the same memory as one processing 10.
	//
	// RESULT COLLECTION: Why per-worker slices instead of shared mutex?
	//
	// With a shared results slice, every result append needs a mutex lock.
	// For 100K files, that's 100K lock/unlock cycles with potential contention.
	//
	// Per-worker slices eliminate contention entirely. Each worker appends
	// freely to its own slice. At the end, we merge once - O(workers) locks
	// instead of O(files).
	//
	worker := func(workerID int) {
		bufs := &workerBufs{
			dirBuf:  make([]byte, 0, dirReadBufSize),
			pathBuf: make([]byte, 0, pathBufExtra),
			// dataBuf, dataArena, worker.buf start zero-valued, grow as needed.
		}

		// Pre-allocate batches for pipelining large directories.
		// Uses channel-as-freelist pattern (see fileproc_workers.go).
		//
		// Why pipelineBatchCount (6)?
		//   In tree mode, pipelining within a directory uses Workers=1,
		//   so we need: 1 (producer) + 4 (channel buffer) + 1 (worker) = 6 batches.
		//
		freeBatches := make(chan *nameBatch, pipelineBatchCount)
		for range pipelineBatchCount {
			freeBatches <- &nameBatch{}
		}

		// Workers=1: parallelism is across directories, not within each one.
		dirOpts := options{
			Workers:            1,
			SmallFileThreshold: opts.SmallFileThreshold,
			Suffix:             opts.Suffix,
			OnError:            opts.OnError,
		}

		// ====================================================================
		// MAIN WORK LOOP: Process directories until coordinator closes jobs
		// ====================================================================
		//
		// Each iteration processes one directory:
		//   1. Read directory entries (discovering files AND subdirectories)
		//   2. Send discovered subdirs to coordinator (dynamic work discovery)
		//   3. Process files (sequential for small dirs, pipelined for large)
		//   4. Signal completion to coordinator
		//
		// The closure wrapping each job provides defer semantics for cleanup.
		// Without it, the defer would only run when the worker exits entirely.
		//
		for job := range jobs {
			func() {
				if ctx.Err() != nil {
					return
				}

				dirRel := "."
				if len(job.rel) > 0 {
					dirRel = string(job.rel)
				}

				dirEnumerator, err := openDirEnumerator(job.abs)
				if err != nil {
					ioErr := &IOError{Path: dirRel, Op: "open", Err: err}
					if notifier.ioErr(ioErr) {
						workerErrs[workerID] = append(workerErrs[workerID], ioErr)
					}

					return
				}

				defer func() { _ = dirEnumerator.closeHandle() }()

				// ============================================================
				// SUBDIRECTORY DISCOVERY
				// ============================================================
				//
				// As we read directory entries, the reportSubdir callback fires for
				// each subdirectory. We immediately send it to the coordinator,
				// which adds it to the work queue for another worker to process.
				//
				// This is how tree traversal discovers work dynamically - we
				// don't know the full tree structure until we've visited every
				// directory.
				//
				// NOTE: We must allocate new slices for rel (relative path)
				// because dirJob is sent to a channel and processed later.
				// The job's byte slices must outlive this callback.
				//
				reportSubdir := func(name []byte) {
					relCap := len(name)
					if len(job.rel) > 0 {
						relCap = len(job.rel) + 1 + len(name)
					}

					rel := make([]byte, 0, relCap)
					rel = appendPathPrefix(rel, job.rel)
					rel = append(rel, name...)

					select {
					case found <- dirJob{abs: joinPathWithNul(job.abs, name), rel: rel}:
					case <-ctx.Done():
					}
				}

				bufs.batch.reset(cap(bufs.dirBuf) * 2)

				// ============================================================
				// ADAPTIVE PROCESSING STRATEGY
				// ============================================================
				//
				// We start reading the directory sequentially. If we discover
				// more than SmallFileThreshold files, we switch mid-stream to
				// pipelined processing. The files already read become the
				// "initial" batch for the pipeline.
				//
				// This avoids pipeline setup overhead for small directories
				// while still benefiting from overlap in large ones.
				//
				large, err := readDirUntilLargeOrEOF(
					ctx,
					dirEnumerator,
					bufs.dirBuf[:cap(bufs.dirBuf)],
					dirOpts.Suffix,
					&bufs.batch,
					reportSubdir,
					dirOpts.SmallFileThreshold,
				)

				if ctx.Err() != nil {
					return
				}

				var readErr error
				if err != nil && !errors.Is(err, io.EOF) {
					// Best-effort: process already-read names, but surface the readdir error.
					readErr = err
				}

				// Large directory? Switch to pipelined processing.
				if large {
					dirResults, dirErrs := p.processDirPipelinedWithBatches(ctx, &treeDirPipelinedArgs{
						dir:           job.abs,
						relPrefix:     job.rel,
						dirEnumerator: dirEnumerator,
						initialNames:  bufs.batch.names,
						opts:          dirOpts,
						reportSubdir:  reportSubdir,
						notifier:      notifier,
						bufs:          bufs,
						freeBatches:   freeBatches,
					})
					if len(dirErrs) > 0 {
						workerErrs[workerID] = append(workerErrs[workerID], dirErrs...)
					}

					if len(dirResults) > 0 {
						workerResults[workerID] = append(workerResults[workerID], dirResults...)
					}

					return
				}

				// Small/medium directory: process files directly.
				if ctx.Err() == nil && len(bufs.batch.names) > 0 {
					dh, err := openDir(job.abs)
					if err != nil {
						ioErr := &IOError{Path: dirRel, Op: "open", Err: err}
						if notifier.ioErr(ioErr) {
							workerErrs[workerID] = append(workerErrs[workerID], ioErr)
						}

						if readErr != nil {
							readErrIO := &IOError{Path: dirRel, Op: "readdir", Err: readErr}
							if notifier.ioErr(readErrIO) {
								workerErrs[workerID] = append(workerErrs[workerID], readErrIO)
							}
						}

						return
					}

					var (
						dirResults []*T
						dirErrs    []error
					)

					cfg := fileProcCfg[T]{
						fn:       p.fn,
						notifier: notifier,
					}
					out := fileProcOut[T]{results: &dirResults, errs: &dirErrs}

					processFilesInto(ctx, dh, job.rel, bufs.batch.names, cfg, bufs, out)
					_ = dh.closeHandle()

					if len(dirErrs) > 0 {
						workerErrs[workerID] = append(workerErrs[workerID], dirErrs...)
					}

					if len(dirResults) > 0 {
						workerResults[workerID] = append(workerResults[workerID], dirResults...)
					}
				}

				if readErr != nil {
					readErrIO := &IOError{Path: dirRel, Op: "readdir", Err: readErr}
					if notifier.ioErr(readErrIO) {
						workerErrs[workerID] = append(workerErrs[workerID], readErrIO)
					}
				}
			}()

			// Signal coordinator that this directory is done.
			doneDir <- struct{}{}
		}
	}

	// Start tree workers.
	var wg sync.WaitGroup
	wg.Add(opts.Workers)

	for i := range opts.Workers {
		go func(id int) {
			defer wg.Done()

			worker(id)
		}(i)
	}

	wg.Wait()
	coordWG.Wait()

	// Merge per-worker results into final slice.
	totalResults := 0
	for i := range opts.Workers {
		totalResults += len(workerResults[i])
	}

	results := make([]*T, 0, totalResults)
	for i := range opts.Workers {
		results = append(results, workerResults[i]...)
	}

	// Merge per-worker errors.
	totalErrs := 0
	for i := range opts.Workers {
		totalErrs += len(workerErrs[i])
	}

	allErrs := make([]error, 0, totalErrs)
	for i := range opts.Workers {
		allErrs = append(allErrs, workerErrs[i]...)
	}

	return results, allErrs
}

func readDirUntilLargeOrEOF(ctx context.Context, dirEnumerator readdirHandle, dirBuf []byte, suffix string, batch *nameBatch, reportSubdir func([]byte), threshold int) (bool, error) {
	for {
		if ctx.Err() != nil {
			return false, io.EOF
		}

		err := readDirBatch(dirEnumerator, dirBuf, suffix, batch, reportSubdir)
		if len(batch.names) > threshold {
			if err != nil && !errors.Is(err, io.EOF) {
				// Best-effort: signal error but keep already-read names.
				return false, err
			}

			return true, nil
		}

		if err == nil {
			continue
		}

		if errors.Is(err, io.EOF) {
			return false, io.EOF
		}

		return false, err
	}
}

// processDirPipelinedWithBatches handles large directories within tree traversal.
//
// WHY A SEPARATE FUNCTION FROM processDirPipelined?
//
// In tree traversal, each tree worker already owns buffers. When that worker
// encounters a large directory (>1500 files), we want to pipeline within
// that directory without allocating new buffers.
//
// This function borrows the tree worker's buffers and freeBatches channel.
// Since we use Workers=1 for within-directory processing (parallelism is
// across directories, not within), there's no conflict - the tree worker
// is blocked waiting for this function to return.
//
// BUFFER REUSE GUARANTEE:
//
// The tree worker's buffers are safe to reuse here because:
//  1. Tree worker is blocked on this call (synchronous from its perspective)
//  2. Workers=1 means single file processor, no concurrent buffer access
//  3. freeBatches channel provides bounded, reusable batch storage
type treeDirPipelinedArgs struct {
	dir           []byte
	relPrefix     []byte
	dirEnumerator readdirHandle
	initialNames  [][]byte
	opts          options
	reportSubdir  func(name []byte)
	notifier      *errNotifier
	bufs          *workerBufs
	freeBatches   chan *nameBatch
}

func (p processor[T]) processDirPipelinedWithBatches(ctx context.Context, args *treeDirPipelinedArgs) ([]*T, []error) {
	dirRel := "."
	if len(args.relPrefix) > 0 {
		dirRel = string(args.relPrefix)
	}

	dh, err := openDirFromReaddir(args.dirEnumerator, pathStr(args.dir))
	if err != nil {
		ioErr := &IOError{Path: dirRel, Op: "open", Err: err}
		if args.notifier.ioErr(ioErr) {
			return nil, []error{ioErr}
		}

		return nil, nil
	}

	// Tree workers already own buffers and a batch pool; reuse them here to
	// avoid per-directory allocations. workerCount must stay 1 to keep this safe.
	return p.runDirPipeline(ctx, &pipelineArgs{
		dirEnumerator: args.dirEnumerator,
		dirHandle:     dh,
		relPrefix:     args.relPrefix,
		dirRel:        dirRel,
		initialNames:  args.initialNames,
		suffix:        args.opts.Suffix,
		reportSubdir:  args.reportSubdir,
		notifier:      args.notifier,
		dirBuf:        args.bufs.dirBuf,
		workerCount:   1,
		queueCapacity: 4,
		freeBatches:   args.freeBatches,
		sharedBufs:    args.bufs,
	})
}

// ============================================================================
// nameBatch: Arena-Style Allocation for Directory Entries
// ============================================================================
//
// nameBatch collects filenames using an "arena" allocation pattern that
// minimizes heap allocations when reading directories with many files.
//
// THE PROBLEM:
//
// A naive implementation would allocate each filename separately:
//
//	names := []string{}
//	for _, entry := range dirEntries {
//	    names = append(names, entry.Name())  // ALLOCATES for each file!
//	}
//
// For a directory with 1000 files, this causes 1000+ allocations. When
// processing thousands of directories, allocation overhead dominates.
//
// THE SOLUTION: Arena-Style Storage
//
// Instead of allocating each filename separately, we pack all filenames
// into a single contiguous byte buffer ("storage"), then create slices
// that point into this buffer ("names"). This reduces allocations from
// O(files) to O(1).
//
// MEMORY LAYOUT:
//
// After appending "file1.md", "file2.md", and "doc.txt":
//
//	storage (one contiguous []byte allocation):
//	┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
//	│ f │ i │ l │ e │ 1 │ . │ m │ d │\0 │ f │ i │ l │ e │ 2 │ . │ m │ d │\0 │ d │ o │ c │ . │ t │ x │ t │\0 │
//	└───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
//	  0   1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19  20  21  22  23  24  25
//	  ▲                               ▲   ▲                               ▲   ▲                           ▲
//	  │                               │   │                               │   │                           │
//	  └───────── names[0] ────────────┘   └───────── names[1] ────────────┘   └──────── names[2] ─────────┘
//	         storage[0:9]                        storage[9:18]                      storage[18:26]
//
//	names ([][]byte - slice of slice headers, NOT new allocations):
//	┌─────────────────┬─────────────────┬─────────────────┐
//	│ ptr=&storage[0] │ ptr=&storage[9] │ ptr=&storage[18]│
//	│ len=9, cap=9    │ len=9, cap=9    │ len=8, cap=8    │
//	└─────────────────┴─────────────────┴─────────────────┘
//	      names[0]          names[1]          names[2]
//
// KEY INSIGHT: Each entry in `names` is just a slice header (24 bytes:
// pointer + length + capacity). The slice header points into `storage`
// rather than owning its own memory. No per-filename allocation occurs!
//
// WHY NUL TERMINATORS?
//
// The NUL byte (\0) after each filename is required for Unix syscalls.
// Functions like openat(2) expect C-style NUL-terminated strings. By
// storing the NUL as part of each name slice, we can pass names directly
// to syscalls without any conversion or allocation.
//
// ALLOCATION COMPARISON:
//
//	Directory with 1000 files:
//	┌─────────────────────┬──────────────────────────────────┐
//	│ Approach            │ Heap Allocations                 │
//	├─────────────────────┼──────────────────────────────────┤
//	│ []string            │ ~1000 (one per filename)         │
//	│ nameBatch (arena)   │ ~2 (storage + names slice)       │
//	└─────────────────────┴──────────────────────────────────┘
//
// USAGE PATTERNS:
//
//	// For syscalls (NUL included, ready to use):
//	fd, err := unix.Openat(dirfd, &name[0], flags, 0)
//
//	// For display/logging (exclude NUL):
//	fmt.Println(string(name[:nameLen(name)]))  // "file1.md"
//
//	// For path building (get length without NUL):
//	pathLen := prefixLen + nameLen(name)  // 8, not 9
//
// INVARIANT: Every slice in names includes its NUL terminator, so
// name[len(name)-1] == 0 always holds. This makes the contract explicit
// and allows direct use with syscalls.
type nameBatch struct {
	// storage is the arena: a single contiguous byte buffer that holds
	// all filenames packed together with NUL terminators between them.
	// Pre-sized to avoid growth during directory reading.
	storage []byte

	// names contains slice headers pointing into storage. Each slice
	// represents one filename INCLUDING its NUL terminator. These are
	// NOT separate allocations - they're just "views" into storage.
	names [][]byte
}

// byteSeq is a small constraint used for shared helper functions that work on
// both string directory entry names (non-Linux/Windows ReadDir) and []byte
// directory entry names (Linux getdents64 parsing).
//
// Keeping these helpers in one place avoids drift across platform-specific
// files.
type byteSeq interface {
	~string | ~[]byte
}

// hasSuffix reports whether name ends with suffix. Empty suffix matches all.
//
// Implemented once for both string and []byte names.
func hasSuffix[S byteSeq](name S, suffix string) bool {
	if suffix == "" {
		return true
	}

	if len(name) < len(suffix) {
		return false
	}

	start := len(name) - len(suffix)
	for i := range len(suffix) {
		if name[start+i] != suffix[i] {
			return false
		}
	}

	return true
}

// reset prepares the batch for reuse, preserving allocated capacity.
//
// The storageCap parameter hints at expected total bytes for all filenames.
// Typically set to len(dirBuf) * 2, estimating that filenames occupy about
// half of the raw dirent data returned by getdents64.
//
// After reset, the batch is empty but retains its backing arrays, enabling
// zero-allocation reuse across multiple directories.
func (b *nameBatch) reset(storageCap int) {
	// Grow storage capacity if needed, otherwise just reset length to 0.
	// The backing array is retained for reuse.
	if storageCap > 0 && cap(b.storage) < storageCap {
		b.storage = make([]byte, 0, storageCap)
	} else {
		b.storage = b.storage[:0]
	}

	// Pre-size the names slice to avoid growth allocations during append.
	//
	// Heuristic: assume average filename is ~20 bytes (including NUL).
	// For storageCap=64KB, this pre-allocates space for ~3200 names.
	// This eliminates the repeated allocations that occur when a slice
	// grows: 0→1→2→4→8→16→...→2048 (about 11 allocations for 1000 files).
	//
	// If the estimate is wrong, append still works - it just allocates.
	// But for typical directories, this eliminates all names slice growth.
	namesCap := storageCap / 20
	if namesCap > 0 && cap(b.names) < namesCap {
		b.names = make([][]byte, 0, namesCap)
	} else {
		b.names = b.names[:0]
	}
}

// appendBytes / appendString add a filename to the batch, appending a NUL
// terminator. Both variants avoid conversions in hot paths.

// appendBytes adds a filename to the batch, appending a NUL terminator.
//
// name must NOT include a NUL terminator (it is added here).
func (b *nameBatch) appendBytes(name []byte) {
	start := len(b.storage)
	b.storage = append(b.storage, name...) // copy filename bytes into arena
	b.storage = append(b.storage, 0)       // append NUL terminator for syscalls
	b.names = append(b.names, b.storage[start:len(b.storage)])
}

// copyName copies a name that already includes its NUL terminator.
// Used when transferring names between batches (e.g., pipelining).
func (b *nameBatch) copyName(name []byte) {
	start := len(b.storage)
	b.storage = append(b.storage, name...) // name already includes NUL
	b.names = append(b.names, b.storage[start:len(b.storage)])
}

// nameLen returns the length of the filename EXCLUDING the NUL terminator.
// Use this when calculating buffer sizes or path lengths.
func nameLen(name []byte) int {
	if len(name) == 0 {
		return 0
	}

	return len(name) - 1
}

// ============================================================================
// Path helpers
// ============================================================================

// appendPathBytesPrefix builds a path from prefix and name (which includes NUL terminator).
// Returns a slice WITHOUT NUL terminator (for display/string conversion).
func appendPathBytesPrefix(buf []byte, prefix []byte, name []byte) []byte {
	buf = buf[:0]
	buf = appendPathPrefix(buf, prefix)

	buf = append(buf, name[:nameLen(name)]...)

	return buf
}

// appendPathPrefix appends prefix and a separator (if needed) to buf.
// Caller controls buf capacity and initial length.
func appendPathPrefix(buf []byte, prefix []byte) []byte {
	if len(prefix) == 0 {
		return buf
	}

	buf = append(buf, prefix...)

	last := prefix[len(prefix)-1]
	if last != os.PathSeparator && last != '/' {
		buf = append(buf, os.PathSeparator)
	}

	return buf
}

// ============================================================================
// Error notification
// ============================================================================

// errNotifier tracks error counts and calls OnError callback.
// Zero value is valid (no-op). Safe for concurrent use.
type errNotifier struct {
	mu       sync.Mutex
	ioErrs   int
	procErrs int
	onError  func(err error, ioErrs, procErrs int) bool
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
	ioCount, procCount := n.ioErrs, n.procErrs
	n.mu.Unlock()

	return n.onError(err, ioCount, procCount)
}

// callbackErr increments process error count and calls callback.
// Returns true if error should be collected, false to discard.
func (n *errNotifier) callbackErr(err error) bool {
	if n == nil || n.onError == nil {
		return true
	}

	n.mu.Lock()
	n.procErrs++
	ioCount, procCount := n.ioErrs, n.procErrs
	n.mu.Unlock()

	return n.onError(err, ioCount, procCount)
}

// pathWithNul converts a string path to []byte with NUL terminator.
// Used for syscalls that require NUL-terminated paths.
func pathWithNul(s string) []byte {
	b := make([]byte, 0, len(s)+1)
	b = append(b, s...)
	b = append(b, 0)

	return b
}

// pathStr converts a NUL-terminated path back to string (strips NUL).
// Used for error messages.
func pathStr(p []byte) string {
	if len(p) > 0 && p[len(p)-1] == 0 {
		return string(p[:len(p)-1])
	}

	return string(p)
}

// joinPathWithNul joins a base path (NUL-terminated) with a name and returns
// a new NUL-terminated path. base must be NUL-terminated.
func joinPathWithNul(base, name []byte) []byte {
	// base includes NUL at end, name does not include NUL.
	baseLen := len(base)
	if baseLen > 0 && base[baseLen-1] == 0 {
		baseLen--
	}

	sep := byte(os.PathSeparator)

	// Note: We must not blindly append a separator. For example, when base is the
	// filesystem root ("/" on Unix, "C:\\" on Windows), it already ends with a
	// separator.
	result := make([]byte, 0, baseLen+1+len(name)+1)

	result = append(result, base[:baseLen]...)
	if baseLen > 0 {
		last := base[baseLen-1]
		if last != sep && last != '/' {
			result = append(result, sep)
		}
	}

	result = append(result, name...)
	result = append(result, 0)

	return result
}
