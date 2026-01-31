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
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ProcessFunc is called for each file.
//
// ProcessFunc may be called concurrently and must be safe for concurrent use.
//
// f provides access to [File] metadata and content. f.PathBorrowed() is
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
// f.PathBorrowed() returns the absolute path. Results are unordered
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

	notifier := newErrNotifier(cfg.OnError)
	proc := processor[T]{fn: fn}
	rootPath := newNulTermPath(path)

	if cfg.Recursive {
		return proc.processTree(ctx, rootPath, cfg, notifier)
	}

	return proc.processDir(ctx, rootPath, cfg, notifier)
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
	// buffer growth heuristics.
	defaultReadBufSize = 512

	// defaultSmallFileThreshold is the default for WithSmallFileThreshold.
	// Below this file count, sequential file processing beats pipelined workers.
	defaultSmallFileThreshold = 1500

	// maxPipelineQueue caps the number of arenas buffered between the readdir
	// producer and file-processing workers in non-recursive pipelined mode.
	//
	// This prevents excessive memory usage when Workers is set very high.
	// When the cap is hit, the producer blocks until workers catch up.
	maxPipelineQueue = maxWorkers

	// pipelineBatchCount is the number of pathArena objects to pre-allocate
	// for the pipelining free-list when running in tree worker context.
	// Formula: 1 (producer) + 4 (channel buffer) + 1 (worker) = 6
	// (Tree workers use Workers=1 for pipelining within a directory).
	pipelineBatchCount = 6
)

var errContainsNUL = errors.New("contains NUL byte")

// workerBufs holds pre-allocated buffers that a worker reuses across all
// files and directories it processes. Passed by pointer to avoid copying
// and to allow growable fields (pathArena) to expand.
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

	// paths collects paths read from a directory (arena-style storage).
	// Reused: reset for each directory, internal capacity preserved.
	// See pathArena comments for the arena allocation pattern.
	paths pathArena

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

// ============================================================================
// NON-RECURSIVE: SINGLE DIRECTORY PROCESSING
// ============================================================================

// processDir processes files in a single directory (non-recursive).
//
// Allocations (one-shot, not pooled):
//   - dirBuf (32KB): for reading directory entries
//   - pathArena: for collecting paths
//
// For large directories, switches to processDirPipelined which allocates
// additional buffers per worker.
func (p processor[T]) processDir(
	ctx context.Context,
	dirPath nulTermPath,
	opts options,
	notifier *errNotifier,
) ([]*T, []error) {
	dirEnumerator, readDirErr := openDirEnumerator(dirPath)
	if readDirErr != nil {
		ioErr := &IOError{Path: dirPath.String(), Op: "open", Err: readDirErr}
		if notifier.ioErr(ioErr) {
			return nil, []error{ioErr}
		}

		return nil, nil
	}

	defer func() { _ = dirEnumerator.closeHandle() }()

	dirBuf := make([]byte, 0, dirReadBufSize)
	paths := &pathArena{}
	paths.reset(cap(dirBuf) * 2)

	return p.processDirAdaptive(ctx, &adaptiveDirArgs{
		dirPath:       dirPath,
		dirEnumerator: dirEnumerator,
		dirBuf:        dirBuf[:cap(dirBuf)],
		paths:         paths,
		opts:          opts,
		notifier:      notifier,
		reportSubdir:  nil,
		reuse:         nil,
	})
}

type pipelineReuse struct {
	freeArenas    chan *pathArena
	queueCapacity int
	sharedBufs    *workerBufs
}

type adaptiveDirArgs struct {
	dirPath       nulTermPath
	dirEnumerator readdirHandle
	dirBuf        []byte
	paths         *pathArena
	opts          options
	notifier      *errNotifier
	reportSubdir  func(nulTermName)
	reuse         *pipelineReuse
}

// processDirAdaptive decides between sequential and pipelined processing
// based on SmallFileThreshold, and wires error reporting consistently.
//
// ADAPTIVE PROCESSING STRATEGY:
//
// We start reading the directory sequentially. If we discover more than
// SmallFileThreshold files, we switch mid-stream to pipelined processing.
// The files already read become the "initial" batch for the pipeline.
//
// This avoids pipeline setup overhead for small directories while still
// benefiting from overlap in large ones.
func (p processor[T]) processDirAdaptive(ctx context.Context, args *adaptiveDirArgs) ([]*T, []error) {
	large, readErr := readDirUntilThresholdOrEOF(
		ctx,
		args.dirEnumerator,
		args.dirPath,
		args.dirBuf,
		args.opts.Suffix,
		args.paths,
		args.reportSubdir,
		args.opts.SmallFileThreshold,
	)

	if ctx.Err() != nil {
		return nil, nil
	}

	if large {
		pipelineArgs := dirPipelineArgs{
			dirPath:        args.dirPath,
			dirEnumerator:  args.dirEnumerator,
			initialEntries: args.paths.entries,
			suffix:         args.opts.Suffix,
			reportSubdir:   args.reportSubdir,
			notifier:       args.notifier,
			dirBuf:         args.dirBuf,
			workerCount:    args.opts.Workers,
		}

		if args.reuse != nil {
			pipelineArgs.queueCapacity = args.reuse.queueCapacity
			pipelineArgs.freeArenas = args.reuse.freeArenas
			pipelineArgs.sharedBufs = args.reuse.sharedBufs
		}

		return p.processDirPipelined(ctx, &pipelineArgs)
	}

	var sharedBufs *workerBufs
	if args.reuse != nil {
		sharedBufs = args.reuse.sharedBufs
	}

	return p.processDirNames(ctx, args.dirPath, args.paths.entries, readErr, args.notifier, sharedBufs)
}

// processTree processes a directory tree with parallel workers.
//
// Each tree worker:
//   - Owns all its buffers (allocated once at goroutine start)
//   - Processes directories from the jobs channel
//   - Reuses buffers across ALL directories it processes
//   - Discovers subdirectories and reports them via the events channel
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
//	│         │ pathArena       ← reused for ALL directories                  │
//	│         │ freeArenas     ← channel-as-freelist for large dirs          │
//	│         │                                                               │
//	│         └─► for job := range jobs:                                      │
//	│               pathArena.reset()  ← amortized reuse (may grow)           │
//	│               processFilesInto() ← uses worker's buffers                │
//	│                                                                         │
//	└─────────────────────────────────────────────────────────────────────────┘
func (p processor[T]) processTree(
	ctx context.Context,
	root nulTermPath,
	opts options,
	notifier *errNotifier,
) ([]*T, []error) {
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
		absPath nulTermPath
	}

	type treeEvent struct {
		job  dirJob
		done bool
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
	//   jobs   ←── coordinator sends directories for workers to process
	//   events ──► workers send newly-discovered subdirs or completion signals
	//
	// WHY EVENTS?
	//
	// We need both "found subdir" and "done directory" signals to keep an
	// accurate pending count. An event channel carries both kinds of signals,
	// reducing channel count while keeping the same coordinator semantics.
	//
	// `pending` counts queued + in-flight directories. Only when pending hits
	// zero do we know the entire tree has been processed.
	//
	jobs := make(chan dirJob)                       // workers pull from here
	events := make(chan treeEvent, opts.Workers*64) // workers push discovered subdirs/completions here

	var coordWG sync.WaitGroup

	coordWG.Go(func() {
		queue := make([]dirJob, 0, 1024) // typical tree breadth; grows if needed

		// Seed queue with the root directory itself, this starts the tree walk.
		queue = append(queue, dirJob{absPath: root})

		pendingDirJobs := len(queue)
		jobsClosed := false

		// Drain buffered events to avoid premature termination when pending hits zero.
		drainEvents := func(pending int, queue []dirJob) (int, []dirJob) {
			for {
				select {
				case ev := <-events:
					if ev.done {
						continue
					}

					pending++

					queue = append(queue, ev.job)
				default:
					return pending, queue
				}
			}
		}

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
		// fight over the lock. Benchmarks showed that channels were faster.
		//
		for pendingDirJobs > 0 {
			stopping := ctx.Err() != nil

			if stopping {
				if len(queue) > 0 {
					pendingDirJobs -= len(queue)
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
			case ev := <-events:
				if ev.done {
					pendingDirJobs--
				} else if !stopping {
					pendingDirJobs++

					queue = append(queue, ev.job)
				}

				if pendingDirJobs == 0 && !stopping {
					// events is buffered. Drain any already-discovered work before deciding
					// that we're done, otherwise pending accounting can terminate early.
					pendingDirJobs, queue = drainEvents(pendingDirJobs, queue)
				}

				if pendingDirJobs == 0 && !jobsClosed {
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
			dirBuf: make([]byte, 0, dirReadBufSize),
			// dataBuf, dataArena, worker.buf start zero-valued, grow as needed.
		}

		// Workers=1: parallelism is across directories, not within each one.
		dirOpts := options{
			Workers:            1,
			SmallFileThreshold: opts.SmallFileThreshold,
			Suffix:             opts.Suffix,
			OnError:            opts.OnError,
		}
		// Pre-allocate arenas for pipelining large directories.
		// Uses channel-as-freelist pattern (see fileproc_workers.go).
		//
		// Why pipelineBatchCount (6)?
		//   In tree mode, pipelining within a directory uses Workers=1,
		//   so we need: 1 (producer) + 4 (channel buffer) + 1 (worker) = 6 arenas.
		//
		freeArenas := make(chan *pathArena, pipelineBatchCount)
		for range pipelineBatchCount {
			freeArenas <- &pathArena{}
		}
		// Keep queue capacity in sync with pipelineBatchCount sizing.
		pipelineQueueCap := pipelineBatchCount - dirOpts.Workers - 1
		reuse := pipelineReuse{
			freeArenas:    freeArenas,
			queueCapacity: pipelineQueueCap,
			sharedBufs:    bufs,
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

				dirEnumerator, err := openDirEnumerator(job.absPath)
				if err != nil {
					ioErr := &IOError{Path: job.absPath.String(), Op: "open", Err: err}
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
				reportSubdir := func(name nulTermName) {
					// Build child path: parent + sep + name (includes NUL from name).
					parentLen := job.absPath.LenWithoutNul()
					childAbs := make([]byte, 0, parentLen+1+len(name))
					childAbs = append(childAbs, job.absPath[:parentLen]...)
					if parentLen > 0 && job.absPath[parentLen-1] != os.PathSeparator {
						childAbs = append(childAbs, os.PathSeparator)
					}
					childAbs = append(childAbs, name...) // includes NUL

					select {
					case events <- treeEvent{job: dirJob{absPath: childAbs}}:
					case <-ctx.Done():
					}
				}

				bufs.paths.reset(cap(bufs.dirBuf) * 2)

				// Adaptive processing: switch to pipelined mode when the directory
				// exceeds SmallFileThreshold, otherwise process sequentially.
				dirResults, dirErrs := p.processDirAdaptive(ctx, &adaptiveDirArgs{
					dirPath:       job.absPath,
					dirEnumerator: dirEnumerator,
					dirBuf:        bufs.dirBuf[:cap(bufs.dirBuf)],
					paths:         &bufs.paths,
					opts:          dirOpts,
					notifier:      notifier,
					reportSubdir:  reportSubdir,
					reuse:         &reuse,
				})
				if len(dirErrs) > 0 {
					workerErrs[workerID] = append(workerErrs[workerID], dirErrs...)
				}

				if len(dirResults) > 0 {
					workerResults[workerID] = append(workerResults[workerID], dirResults...)
				}
			}()

			// Signal coordinator that this directory is done.
			// Always report completion to avoid pending-count leaks on cancel.
			events <- treeEvent{done: true}
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

func readDirUntilThresholdOrEOF(ctx context.Context, dirEnumerator readdirHandle, dirPath nulTermPath, dirBuf []byte, suffix string, arena *pathArena, reportSubdir func(nulTermName), threshold int) (bool, error) {
	for {
		if ctx.Err() != nil {
			return false, io.EOF
		}

		err := readDirBatch(dirEnumerator, dirPath, dirBuf, suffix, arena, reportSubdir)
		if len(arena.entries) > threshold {
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
