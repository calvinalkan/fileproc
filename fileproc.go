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
// Panics in user-defined callbacks ([ProcessFunc] and [ProcessReaderFunc]) are
// not recovered by this package.
//
// A callback panic will unwind the goroutine executing it; in concurrent modes
// this will crash the process. If you need to guard against panics, recover
// inside your callback.
//
// # Choosing an API
//
// Use [Process] when you only need a small prefix of each file (headers,
// metadata, magic bytes) and you can decide validity from the first N bytes.
// [Process] reads up to [Options.ReadSize] bytes per file with a single read and
// passes the prefix as a borrowed []byte.
//
// Use [ProcessReader] when you need to stream or fully consume file contents
// (e.g. hashing, decompression, parsing large files). The callback receives an
// [io.Reader] and controls how much to read.
//
// # Architecture
//
// The package provides two entry points: [Process] (prefix-bytes API) and
// [ProcessReader] (streaming reader API). Both support single-directory and
// recursive tree processing based on [Options.Recursive].
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
//	│   - Pipeline workers allocate their own pathBuf/pathArena and either:   │
//	│       • readBuf  (Process, prefix-bytes API), or                         │
//	│       • probeBuf (ProcessReader, streaming API)                         │
//	│                                                                         │
//	│ Recursive (processTree, shared by both APIs):                          │
//	│   - per-worker results/errors slices (merged at the end)                │
//	│   - Per tree worker (allocated once, reused for ALL directories):       │
//	│       • dirBuf  (32KB): Linux dirent buffer / sizing heuristic          │
//	│       • readBuf (Process): reading a prefix of file contents            │
//	│       • probeBuf (ProcessReader): initial probe read + replay           │
//	│       • pathBuf (512B): building relative paths                         │
//	│       • batch: collecting filenames                                     │
//	│       • freeBatches: channel-as-freelist for pipeline batches           │
//	│                                                                         │
//	└─────────────────────────────────────────────────────────────────────────┘
//
// Rough memory budget for recursive mode with 8 workers (excluding results,
// path arenas, and other overhead), assuming the defaults:
//
//   - ReadSize = 2KB
//
//   - readerProbeSize = 4KB
//
//     8 × (32KB dirBuf + (2KB readBuf or 4KB probeBuf) + 512B pathBuf + 6×(≈64KB batch storage + ≈75KB name headers)) ≈ 7–8MB
package fileproc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// ProcessFunc is called for each file with a prefix of its contents.
//
// ProcessFunc may be called concurrently and must be safe for concurrent use.
//
// path is relative to the directory passed to [Process]. The path and data
// slices alias internal reusable buffers; they are only valid until
// ProcessFunc returns. Copy them if needed.
//
// Callbacks are not invoked for directories, symlinks, or other non-regular
// files.
//
// Empty files are passed to ProcessFunc with a zero-length data slice.
//
// Return values:
//   - (*T, nil): emit the result
//   - (nil, nil): skip this file silently
//   - (_, error): skip and report the error as a [ProcessError]
//
// Whether [ProcessError]s (and [IOError]s) are included in the returned error
// slice depends on [Options.OnError]. If OnError is nil, all errors are
// collected.
//
// Panics are not recovered by this package. Callbacks must not panic; if you
// need to guard against panics, recover inside the callback.
type ProcessFunc[T any] func(path, data []byte) (*T, error)

// Result holds a successfully processed item.
// Uses value type (not pointer) to avoid per-result heap allocation.
type Result[T any] struct {
	// Path is the relative file path as a byte slice.
	//
	// The slice points into internal storage managed by the processor (unless
	// [Options.CopyResultPath] is true).
	//
	// It remains valid as long as the Result itself remains reachable (for
	// example as long as you keep the returned results slice).
	//
	// To convert to string: string(r.Path)
	// Many operations work directly with []byte, avoiding allocation.
	Path []byte
	// Value is the pointer returned by the user callback (not copied).
	Value *T
}

// IOError is returned when a file system operation fails.
type IOError struct {
	// Path is the path relative to the root directory passed to [Process] or
	// [ProcessReader]. The root directory itself is reported as ".".
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

// ProcessError is returned when a user callback ([ProcessFunc] or
// [ProcessReaderFunc]) returns an error.
type ProcessError struct {
	// Path is the relative file path.
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

// Options configures the file processor.
type Options struct {
	// Workers controls concurrency.
	//   - Non-recursive: pipeline worker count for large directories.
	//     Default: ~2/3 GOMAXPROCS (min 4).
	//   - Recursive: number of concurrent directory workers.
	//     Default: NumCPU()/2 clamped to [4, 16].
	//
	// Worker counts are capped at maxWorkers to prevent extreme memory usage when
	// misconfigured.
	//
	// In non-recursive pipelined mode, internal buffering between the directory
	// reader and the workers is also capped (see maxPipelineQueue).
	Workers int

	// ReadSize is the maximum number of bytes read per file by [Process]
	// (default: 2048).
	//
	// A single read is performed; files larger than ReadSize are truncated.
	// Reads may return fewer bytes than ReadSize.
	//
	// ReadSize is ignored by [ProcessReader], where the callback reads from an
	// [io.Reader] and controls how much data to consume.
	ReadSize int

	// Suffix is the file suffix filter, e.g. ".md" (empty = all files).
	Suffix string

	// Recursive enables recursive processing of subdirectories.
	// When false (default), only the specified directory is processed.
	// Symlinks are ignored (not recursed into, not processed).
	Recursive bool

	// SmallFileThreshold is the file-count cutoff below which simple
	// sequential file processing is used instead of pipelined workers
	// (default: 1500).
	// Below this threshold, worker setup overhead exceeds parallelism gains.
	SmallFileThreshold int

	// OnError is called when an error occurs ([IOError] or [ProcessError]).
	// ioErrs and procErrs are cumulative counts including the current error.
	// May be called concurrently from multiple goroutines (including internal
	// orchestration code) - must be safe for concurrent use.
	//
	// Return value controls error collection:
	//   - true:  collect the error in the returned []error slice
	//   - false: discard the error (not added to returned slice)
	//
	// To stop processing, cancel the context. Use [context.WithCancelCause]
	// to provide a custom stop reason retrievable via [context.Cause].
	//
	// If nil, all errors are collected (equivalent to always returning true).
	//
	// Example use cases:
	//
	//	// Collect all errors, stop after 100 total:
	//	OnError: func(err error, ioErrs, procErrs int) bool {
	//	    if ioErrs+procErrs >= 100 {
	//	        cancel(fmt.Errorf("too many errors: %d", ioErrs+procErrs))
	//	    }
	//	    return true
	//	}
	//
	//	// Log errors, don't buffer in memory:
	//	OnError: func(err error, _, _ int) bool {
	//	    slog.Error("file error", "err", err)
	//	    return false
	//	}
	//
	//	// Collect first 10 errors only:
	//	OnError: func(err error, ioErrs, procErrs int) bool {
	//	    return ioErrs+procErrs <= 10
	//	}
	//
	//	// Stop on first ProcessError, ignore IOErrors:
	//	OnError: func(err error, _, procErrs int) bool {
	//	    if procErrs > 0 {
	//	        cancel(err)
	//	    }
	//	    return true
	//	}
	OnError func(err error, ioErrs, procErrs int) (collect bool)

	// CopyResultPath controls how Result.Path is stored.
	//
	// When false (default), Result.Path slices point into an internal arena.
	// This is allocation-free per result, but retaining many results also
	// retains the arena backing storage.
	//
	// When true, each Result.Path is copied into its own allocation.
	// This is safer/easier for consumers who retain results long-term, at the
	// cost of one allocation per emitted result.
	CopyResultPath bool
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

	// pathBufExtra is extra capacity for path buffers to reduce reallocs
	// when joining directory prefix with filename.
	pathBufExtra = 512

	// defaultReadSize is the default for Options.ReadSize.
	defaultReadSize = 2048

	// defaultSmallFileThreshold is the default for Options.SmallFileThreshold.
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
// and to allow growable fields (pathArena, batch) to expand.
//
// Lifetime: created once per worker goroutine, lives until worker exits.
// All Result.Path slices point into pathArena, so the arena stays alive
// (via GC) as long as any Result from this worker exists.
type workerBufs struct {
	// dirBuf holds raw directory-entry bytes on Linux (getdents64/ReadDirent
	// parsing). On other platforms directory reading uses os.File.ReadDir and
	// dirBuf is not used by readdir itself (its capacity is still used as a
	// sizing heuristic for batching).
	// Sized at 32KB - large enough to read many entries per syscall,
	// small enough to stay in L1 cache.
	// Reused: reset for each directory.
	dirBuf []byte

	// readBuf holds file content read from disk (used by [Process]).
	// Sized from Options.ReadSize (default 2KB) - only reads file prefix.
	// Reused: overwritten for each file, never grows.
	readBuf []byte

	// probeBuf holds an initial chunk read from a file before invoking a reader
	// callback (used by ProcessReader).
	// The chunk is then replayed to the user via a small wrapper reader.
	// Reused: overwritten for each file.
	probeBuf []byte

	// pathBuf is scratch space for building "prefix/filename" paths.
	// Reused: reset (len=0) for each file, capacity preserved.
	// The built path is then copied into pathArena if needed for Result.
	pathBuf []byte

	// pathArena is append-only storage for Result.Path byte slices.
	// Each Result.Path is a slice pointing into this arena.
	// Grows: starts nil, expands as results are added across all directories.
	// Never shrinks or resets - paths must remain valid for caller.
	pathArena []byte

	// batch collects filenames read from a directory (arena-style storage).
	// Reused: reset for each directory, internal capacity preserved.
	// See nameBatch comments for the arena allocation pattern.
	batch nameBatch

	// replay is a reusable wrapper reader used by ProcessReader to replay the
	// initial probe read (stored in probeBuf) before continuing with the
	// underlying file handle.
	replay replayReader
}

type procKind uint8

const (
	procKindBytes procKind = iota
	procKindReader
	procKindStat
)

type processor[T any] struct {
	kind procKind

	fnBytes  ProcessFunc[T]
	fnReader ProcessReaderFunc[T]
	fnStat   ProcessStatFunc[T]
}

func (p processor[T]) run(ctx context.Context, path string, opts Options, notifier *errNotifier) ([]Result[T], []error) {
	if opts.Recursive {
		return p.processRecursive(ctx, path, opts, notifier)
	}

	return p.processDir(ctx, path, opts, notifier)
}

// processEntry contains the shared entry-point orchestration for both Process
// and ProcessReader.
func processEntry[T any](
	ctx context.Context,
	path string,
	opts Options,
	run func(ctx context.Context, path string, opts Options, notifier *errNotifier) ([]Result[T], []error),
) ([]Result[T], []error) {
	path = filepath.Clean(path)

	if strings.IndexByte(path, 0) >= 0 {
		return nil, []error{&IOError{Path: ".", Op: "open", Err: errContainsNUL}}
	}

	if ctx.Err() != nil {
		return nil, nil
	}

	opts = withDefaults(opts)
	if strings.IndexByte(opts.Suffix, 0) >= 0 {
		return nil, []error{fmt.Errorf("invalid suffix: %w", errContainsNUL)}
	}

	notifier := newErrNotifier(opts.OnError)

	return run(ctx, path, opts, notifier)
}

// Process processes files in a directory.
//
// By default, only the specified directory is processed. Set opts.Recursive
// to true to process subdirectories recursively.
//
// Paths passed to ProcessFunc are relative to path. Results are unordered
// when processed with multiple workers.
//
// For streaming/full-content processing, see [ProcessReader].
//
// To stop processing on error, use [Options.OnError] with a cancelable context:
//
//	ctx, cancel := context.WithCancelCause(ctx)
//	opts := Options{
//	    OnError: func(err error, _, _ int) bool {
//	        cancel(err)
//	        return true
//	    },
//	}
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
func Process[T any](ctx context.Context, path string, fn ProcessFunc[T], opts Options) ([]Result[T], []error) {
	proc := processor[T]{kind: procKindBytes, fnBytes: fn}

	return processEntry[T](ctx, path, opts, proc.run)
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
	opts Options,
	notifier *errNotifier,
) ([]Result[T], []error) {
	dirPath := pathWithNul(dir)

	rh, err := openDirEnumerator(dirPath)
	if err != nil {
		ioErr := &IOError{Path: ".", Op: "open", Err: err}
		if notifier.ioErr(ioErr) {
			return nil, []error{ioErr}
		}

		return nil, nil
	}

	defer func() { _ = rh.closeHandle() }()

	dirBuf := make([]byte, 0, dirReadBufSize)
	batch := &nameBatch{}
	batch.reset(cap(dirBuf) * 2)

	for {
		if ctx.Err() != nil {
			return nil, nil
		}

		err := readDirBatch(rh, dirBuf[:cap(dirBuf)], opts.Suffix, batch, nil)

		if len(batch.names) > opts.SmallFileThreshold {
			if opts.Workers <= 0 {
				opts.Workers = defaultWorkers()
			}

			if opts.Workers > maxWorkers {
				opts.Workers = maxWorkers
			}

			return p.processDirPipelined(ctx, &dirPipelinedArgs{
				dir:          dir,
				relPrefix:    nil,
				rh:           rh,
				initial:      batch.names,
				opts:         opts,
				reportSubdir: nil,
				notifier:     notifier,
				dirBuf:       dirBuf[:cap(dirBuf)],
			})
		}

		if err == nil {
			continue
		}

		if errors.Is(err, io.EOF) {
			break
		}

		ioErr := &IOError{Path: ".", Op: "readdir", Err: err}
		if notifier.ioErr(ioErr) {
			return nil, []error{ioErr}
		}

		return nil, nil
	}

	if len(batch.names) == 0 {
		return nil, nil
	}

	return p.processFilesSequential(ctx, dir, nil, batch.names, opts, notifier)
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
	opts Options,
	notifier *errNotifier,
) ([]Result[T], []error) {
	if opts.Workers <= 0 {
		opts.Workers = defaultTreeWorkers()
	}

	if opts.Workers > maxWorkers {
		opts.Workers = maxWorkers
	}

	return p.processTree(ctx, root, opts, notifier)
}

// processTree processes a directory tree with parallel workers.
//
// Shared by both [Process] and [ProcessReader].
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
//	│         │ readBuf [ReadSize] / probeBuf [4KB]  ← reused for ALL files   │
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
	opts Options,
	notifier *errNotifier,
) ([]Result[T], []error) {
	// Convert root path to NUL-terminated []byte for syscalls.
	rootPath := pathWithNul(root)

	// Per-worker result slices - no mutex contention for results.
	// Each worker appends to its own slice, merged at the end.
	//
	// We build these via append (instead of make(len=opts.Workers)) to keep
	// golangci-lint's makezero happy while still allowing workerID indexing.
	workerResults := make([][]Result[T], 0, opts.Workers)
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
		var bufs *workerBufs

		switch p.kind {
		case procKindBytes:
			bufs = &workerBufs{
				dirBuf:  make([]byte, 0, dirReadBufSize),
				readBuf: make([]byte, opts.ReadSize),
				pathBuf: make([]byte, 0, pathBufExtra),
				// pathArena and batch start zero-valued, grow as needed.
			}
		case procKindReader:
			bufs = &workerBufs{
				dirBuf:   make([]byte, 0, dirReadBufSize),
				probeBuf: make([]byte, readerProbeSize),
				pathBuf:  make([]byte, 0, pathBufExtra),
				// pathArena and batch start zero-valued, grow as needed.
			}
		case procKindStat:
			bufs = &workerBufs{
				dirBuf:  make([]byte, 0, dirReadBufSize),
				pathBuf: make([]byte, 0, pathBufExtra),
				// pathArena and batch start zero-valued, grow as needed.
			}
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
		dirOpts := Options{
			Workers:            1,
			ReadSize:           opts.ReadSize,
			SmallFileThreshold: opts.SmallFileThreshold,
			Suffix:             opts.Suffix,
			CopyResultPath:     opts.CopyResultPath,
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

				rh, err := openDirEnumerator(job.abs)
				if err != nil {
					ioErr := &IOError{Path: dirRel, Op: "open", Err: err}
					if notifier.ioErr(ioErr) {
						workerErrs[workerID] = append(workerErrs[workerID], ioErr)
					}

					return
				}

				defer func() { _ = rh.closeHandle() }()

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
					if len(job.rel) > 0 {
						rel = append(rel, job.rel...)
						if last := job.rel[len(job.rel)-1]; last != os.PathSeparator && last != '/' {
							rel = append(rel, os.PathSeparator)
						}
					}

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
					rh,
					bufs.dirBuf[:cap(bufs.dirBuf)],
					dirOpts.Suffix,
					&bufs.batch,
					reportSubdir,
					dirOpts.SmallFileThreshold,
				)

				if ctx.Err() != nil {
					return
				}

				// Large directory? Switch to pipelined processing.
				if large {
					dirResults, dirErrs := p.processDirPipelinedWithBatches(ctx, &treeDirPipelinedArgs{
						dir:          job.abs,
						relPrefix:    job.rel,
						rh:           rh,
						initial:      bufs.batch.names,
						opts:         dirOpts,
						reportSubdir: reportSubdir,
						notifier:     notifier,
						bufs:         bufs,
						freeBatches:  freeBatches,
					})
					if len(dirErrs) > 0 {
						workerErrs[workerID] = append(workerErrs[workerID], dirErrs...)
					}

					if len(dirResults) > 0 {
						workerResults[workerID] = append(workerResults[workerID], dirResults...)
					}

					return
				}

				if err != nil && !errors.Is(err, io.EOF) {
					ioErr := &IOError{Path: dirRel, Op: "readdir", Err: err}
					if notifier.ioErr(ioErr) {
						workerErrs[workerID] = append(workerErrs[workerID], ioErr)
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

						return
					}

					var (
						dirResults []Result[T]
						dirErrs    []error
					)

					cfg := fileProcCfg[T]{
						kind:           p.kind,
						fnBytes:        p.fnBytes,
						fnReader:       p.fnReader,
						fnStat:         p.fnStat,
						notifier:       notifier,
						copyResultPath: opts.CopyResultPath,
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

	results := make([]Result[T], 0, totalResults)
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

func readDirUntilLargeOrEOF(ctx context.Context, rh readdirHandle, dirBuf []byte, suffix string, batch *nameBatch, reportSubdir func([]byte), threshold int) (bool, error) {
	for {
		if ctx.Err() != nil {
			return false, io.EOF
		}

		err := readDirBatch(rh, dirBuf, suffix, batch, reportSubdir)
		if len(batch.names) > threshold {
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
	dir          []byte
	relPrefix    []byte
	rh           readdirHandle
	initial      [][]byte
	opts         Options
	reportSubdir func(name []byte)
	notifier     *errNotifier
	bufs         *workerBufs
	freeBatches  chan *nameBatch
}

func (p processor[T]) processDirPipelinedWithBatches(ctx context.Context, args *treeDirPipelinedArgs) ([]Result[T], []error) {
	dirRel := "."
	if len(args.relPrefix) > 0 {
		dirRel = string(args.relPrefix)
	}

	dh, err := openDirFromReaddir(args.rh, pathStr(args.dir))
	if err != nil {
		ioErr := &IOError{Path: dirRel, Op: "open", Err: err}
		if args.notifier.ioErr(ioErr) {
			return nil, []error{ioErr}
		}

		return nil, nil
	}

	cfg := fileProcCfg[T]{
		kind:           p.kind,
		fnBytes:        p.fnBytes,
		fnReader:       p.fnReader,
		notifier:       args.notifier,
		copyResultPath: args.opts.CopyResultPath,
	}

	nameCh := make(chan *nameBatch, 4)

	var (
		mu      sync.Mutex
		results []Result[T]
		allErrs []error
	)

	addErr := func(err error) {
		if err == nil {
			return
		}

		mu.Lock()

		allErrs = append(allErrs, err)

		mu.Unlock()
	}

	workerDone := make(chan struct{})

	getBatch := func(storageCap int) *nameBatch {
		select {
		case batch := <-args.freeBatches:
			batch.reset(storageCap)

			return batch
		case <-ctx.Done():
			return nil
		case <-workerDone:
			return nil
		}
	}

	putBatch := func(batch *nameBatch) {
		batch.reset(0)

		select {
		case args.freeBatches <- batch:
		default:
		}
	}

	sendBatch := func(batch *nameBatch) bool {
		if batch == nil {
			return false
		}

		if len(batch.names) == 0 {
			putBatch(batch)

			return true
		}

		select {
		case nameCh <- batch:
			return true
		case <-ctx.Done():
			putBatch(batch)

			return false
		case <-workerDone:
			putBatch(batch)

			return false
		}
	}

	var producerWG sync.WaitGroup
	producerWG.Go(func() {
		defer close(nameCh)

		storageCap := cap(args.bufs.dirBuf) * 2

		if len(args.initial) > 0 {
			batch := getBatch(storageCap)
			if batch == nil {
				return
			}

			for _, n := range args.initial {
				batch.copyName(n)
			}

			if !sendBatch(batch) {
				return
			}
		}

		for {
			if ctx.Err() != nil {
				return
			}

			batch := getBatch(storageCap)
			if batch == nil {
				return
			}

			err := readDirBatch(args.rh, args.bufs.dirBuf[:cap(args.bufs.dirBuf)], args.opts.Suffix, batch, args.reportSubdir)

			if !sendBatch(batch) {
				return
			}

			if err == nil {
				continue
			}

			if errors.Is(err, io.EOF) {
				return
			}

			ioErr := &IOError{Path: dirRel, Op: "readdir", Err: err}
			if args.notifier.ioErr(ioErr) {
				addErr(ioErr)
			}

			return
		}
	})

	go func() {
		localErrs := make([]error, 0, 64)

		defer func() {
			mu.Lock()

			allErrs = append(allErrs, localErrs...)

			mu.Unlock()
			close(workerDone)
		}()

		out := fileProcOut[T]{results: &results, errs: &localErrs}

		for batch := range nameCh {
			if ctx.Err() != nil {
				putBatch(batch)

				continue
			}

			func() {
				defer putBatch(batch)

				processFilesInto(ctx, dh, args.relPrefix, batch.names, cfg, args.bufs, out)
			}()
		}
	}()

	producerWG.Wait()
	<-workerDone

	return results, allErrs
}

// ============================================================================
// HELPERS
// ============================================================================

func withDefaults(opts Options) Options {
	if opts.ReadSize <= 0 {
		opts.ReadSize = defaultReadSize
	}

	if opts.SmallFileThreshold <= 0 {
		opts.SmallFileThreshold = defaultSmallFileThreshold
	}

	return opts
}

func defaultWorkers() int {
	w := max((runtime.GOMAXPROCS(0)*2)/3, 4)

	return w
}

// defaultTreeWorkers returns the default worker count for tree traversal.
//
// Benchmarks on a 24-core machine showed 12 workers optimal for tree mode,
// with regression at 16+ workers due to futex contention from goroutine
// coordination (job queue + found channel for subdirectories).
//
// Formula: NumCPU/2, clamped to [4, 16].
func defaultTreeWorkers() int {
	return min(max(runtime.NumCPU()/2, 4), 16)
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

// appendBytes (Linux) / appendString (non-Linux) add a filename to the batch,
// appending a NUL terminator. Implemented in fileproc_batch_helpers.go.

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
	if len(prefix) > 0 {
		buf = append(buf, prefix...)

		last := prefix[len(prefix)-1]
		if last != os.PathSeparator && last != '/' {
			buf = append(buf, os.PathSeparator)
		}
	}

	buf = append(buf, name[:nameLen(name)]...)

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
