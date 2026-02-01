package fileproc

// processor.go contains the directory processing pipeline and worker orchestration
// used by [Process].
//
// This file has no build tags - it works on all platforms by calling into
// platform-specific I/O functions (openDir, openFile, etc.).
//
// # Memory Architecture
//
// This package uses a "allocate at orchestration points, pass down to workers"
// pattern to minimize allocations in the hot path (per-file processing).
//
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│ ALLOCATION HIERARCHY                                                    │
//	├─────────────────────────────────────────────────────────────────────────┤
//	│                                                                         │
//	│  Process()                      ← Entry point                           │
//	│    │                                                                    │
//	│    ├─► processDir()                  ← Allocates: dirBuf, pathArena     │
//	│    │     │                                                              │
//	│    │     ├─► processDirNames() ← Allocates: worker bufs         │
//	│    │     │     └─► processFilesInto()   ← Uses passed buffers           │
//	│    │     │                                                              │
//	│    │     └─► runDirPipeline()       ← Allocates: freeArenas channel   │
//	│    │           ├─► producer            ← Uses passed dirBuf             │
//	│    │           └─► workers             ← Each allocates per-worker bufs │
//	│    │                                                                    │
//	│    └─► processTree()                 ← Allocates: per-worker bufs slices│
//	│          │                                                              │
//	│          └─► [N tree workers]        ← Each allocates ALL buffers once  │
//	│                └─► processFilesInto()  ← Uses worker's buffers          │
//	│                                                                         │
//	└─────────────────────────────────────────────────────────────────────────┘

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
)

// processor owns per-call configuration and orchestration state.
type processor[T any] struct {
	// fn is the user callback invoked per file.
	fn ProcessFunc[T]
	// workers is the file-processing worker count (pipeline + tree).
	workers int
	// suffix filters files by name suffix.
	suffix string
	// smallFileThreshold switches between sequential and pipelined processing when a directory contains >= this many files.
	smallFileThreshold int
	// errNotify routes IO and callback errors to WithOnError, if any.
	errNotify *errNotifier
}

type reportSubdirFunc func(nulTermName)

// dirScan bundles the inputs needed to enumerate a single directory.
type dirScan struct {
	// path is a NUL-terminated absolute dir path for syscalls and error messages.
	path nulTermPath
	// handle is the open directory handle used for readDirBatch/openat.
	handle dirHandle
	// dirEntryBuf is the directory-entry scratch buffer (Linux getdents).
	dirEntryBuf []byte
	// reportSubdir is optional; when set, it receives discovered subdirectories.
	reportSubdir reportSubdirFunc
}

// workerBufs holds pre-allocated buffers that a worker reuses across all
// files and directories it processes. Passed by pointer to avoid copying
// and to allow growable fields (pathArena) to expand.
//
// Lifetime: created once per worker goroutine, lives until worker exits.
type workerBufs struct {
	// dirEntryBuf holds raw directory-entry bytes on Linux (getdents64/ReadDirent
	// parsing). On other platforms directory reading uses os.File.ReadDir and
	// dirEntryBuf is not used by readdir itself (its capacity is still used as a
	// sizing heuristic for batching).
	// Sized at 32KB - large enough to read many entries per syscall,
	// small enough to stay in L1 cache.
	// Reused: reset for each directory.
	dirEntryBuf []byte

	// pathsArena collects pathsArena read from a directory (arena-style storage).
	// Reused: reset for each directory, internal capacity preserved.
	// See pathsArena comments for the arena allocation pattern.
	pathsArena pathArena

	// fileBytesBuf is temporary buffer for File.Bytes() read syscall.
	// Sized to st.Size+1, grows to max file size seen. Reused across files.
	fileBytesBuf []byte

	// fileBytesArena is append-only storage for File.Bytes() results.
	// Each Bytes() result is a subslice. Grows across all files, never
	// shrinks. GC'd when results are released.
	fileBytesArena []byte

	// file is a reusable File struct for callbacks.
	// Reset for each file to avoid per-file heap allocation.
	file File

	// worker is a reusable Worker struct for callbacks.
	worker Worker
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

	// pipelineArenaCount is the number of pathArena objects to pre-allocate
	// for the pipelining free-list when running in tree worker context.
	// Formula: 1 (producer) + 4 (channel buffer) + 1 (worker) = 6
	// (Tree workers use Workers=1 for pipelining within a directory).
	pipelineArenaCount = 6
)

// ============================================================================
// TOP-LEVEL PROCESSING
// ============================================================================

// processSingleDir processes files in a single directory (non-recursive).
//
// Allocations (one-shot, not pooled):
//   - dirBuf (32KB): for reading directory entries
//   - pathArena: for collecting paths
//
// For large directories, switches to runDirPipeline which allocates
// additional buffers per worker.
func (p processor[T]) processSingleDir(
	ctx context.Context,
	dirPath nulTermPath,
) ([]*T, []error) {
	dirHandle, openErr := openDir(dirPath)
	if openErr != nil {
		ioErr := &IOError{Path: dirPath.String(), Op: "open", Err: openErr}
		if p.errNotify.ioErr(ioErr) {
			return nil, []error{ioErr}
		}

		return nil, nil
	}

	defer func() { _ = dirHandle.close() }()

	dirBuf := make([]byte, 0, dirReadBufSize)
	pathsArena := &pathArena{}
	pathsArena.reset(cap(dirBuf) * 2)

	dirScan := dirScan{
		path:         dirPath,
		handle:       dirHandle,
		dirEntryBuf:  dirBuf[:cap(dirBuf)],
		reportSubdir: nil, // all subdirs are ignored when this is nil.
	}

	return p.processDirAdaptive(ctx, dirScan, pathsArena, p.workers, nil, 0, nil)
}

// processRecursive processes a directory tree with parallel workers.
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
//	│ processRecursive                                                        │
//	│   │                                                                     │
//	│   │ per-worker results/errors slices (merged at the end)                │
//	│   │                                                                     │
//	│   └─► [N tree workers] each owns:                                       │
//	│         │                                                               │
//	│         │ dirBuf  [32KB]  ← reused for ALL directories                  │
//	│         │ Worker.Buf  ← reused for ALL files                            │
//	│         │ pathArena       ← reused for ALL directories                  │
//	│         │ freeArenas     ← channel-as-freelist for large dirs           │
//	│         │                                                               │
//	│         └─► for job := range jobs:                                      │
//	│               pathArena.reset()  ← amortized reuse (may grow)           │
//	│               processFilesInto() ← uses worker's buffers                │
//	│                                                                         │
//	└─────────────────────────────────────────────────────────────────────────┘
func (p processor[T]) processRecursive(
	ctx context.Context,
	root nulTermPath,
) ([]*T, []error) {
	// Per-worker result slices - no mutex contention for results.
	// Each worker appends to its own slice, merged at the end.
	//
	// We build these via append (instead of make(len=p.workers)) to keep
	// golangci-lint's makezero happy while still allowing workerID indexing.
	workerResults := make([][]*T, 0, p.workers)
	workerErrs := make([][]error, 0, p.workers)

	for range p.workers {
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
	//   workerPullDirJobsChan   ←── coordinator sends directories for workers to process
	//   workerPushEvents ──► workers send newly-discovered subdirs or completion signals
	//
	// WHY EVENTS?
	//
	// We need both "found subdir" and "done directory" signals to keep an
	// accurate pending count. An event channel carries both kinds of signals,
	// reducing channel count while keeping the same coordinator semantics.
	//
	// `pendingDirJobs` counts queued + in-flight directories. Incremented on
	// discovery, decremented on completion (not dispatch). Only when it hits
	// zero do we know the entire tree has been processed.
	//
	workerPullDirJobsChan := make(chan dirJob)             // workers pull from here
	workerPushEvents := make(chan treeEvent, p.workers*64) // workers push discovered subdirs/completions here
	var coordWG sync.WaitGroup
	coordWG.Go(func() {
		dirJobQueue := make([]dirJob, 0, 1024) // typical tree breadth; grows if needed

		// Seed queue with the root directory itself, this starts the tree walk.
		dirJobQueue = append(dirJobQueue, dirJob{absPath: root})

		pendingDirJobs := len(dirJobQueue)

		dirJobQueueClosed := false

		// Drain buffered events to avoid premature termination when pending hits zero.
		// Only discovery events matter here—done events are ignored because we've
		// already accounted for all completions (pending is zero).
		drainEvents := func(pending int, queue []dirJob) (int, []dirJob) {
			for {
				select {
				case ev := <-workerPushEvents:
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
			cancelled := ctx.Err() != nil

			if cancelled {
				// Discard queued work on cancellation - no point dispatching jobs
				// that workers will skip due to ctx.Err().
				if len(dirJobQueue) > 0 {
					pendingDirJobs -= len(dirJobQueue)
					dirJobQueue = dirJobQueue[:0]
				}

				if !dirJobQueueClosed {
					close(workerPullDirJobsChan)

					dirJobQueueClosed = true
				}
			}

			// Nil channel trick: when jobCh is nil, the send case blocks forever,
			// effectively disabling dispatch when cancelled or queue is empty.
			var (
				nextJob dirJob
				jobCh   chan dirJob
			)

			if !cancelled && len(dirJobQueue) > 0 {
				nextJob = dirJobQueue[0]
				jobCh = workerPullDirJobsChan
			}

			select {
			case ev := <-workerPushEvents:
				if ev.done {
					pendingDirJobs--
				} else if !cancelled {
					pendingDirJobs++

					dirJobQueue = append(dirJobQueue, ev.job)
				}

				if pendingDirJobs == 0 && !cancelled {
					// events is buffered. Drain any already-discovered work before deciding
					// that we're done, otherwise pending accounting can terminate early.
					pendingDirJobs, dirJobQueue = drainEvents(pendingDirJobs, dirJobQueue)
				}

				if pendingDirJobs == 0 && !dirJobQueueClosed {
					close(workerPullDirJobsChan)

					dirJobQueueClosed = true
				}

			case jobCh <- nextJob:
				dirJobQueue = dirJobQueue[1:]
			}
		}

		if !dirJobQueueClosed {
			close(workerPullDirJobsChan)
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
	// BUFFER OWNERSHIP: Why each dirWalkerWorker owns its buffers?
	//
	// Alternative: Use sync.Pool to share buffers across workers.
	// Problem: Pool introduces lock contention and unpredictable lifetimes.
	//
	// Our approach: Each dirWalkerWorker allocates buffers once at startup, reuses
	// them for EVERY directory it processes. A dirWalkerWorker processing 1000
	// directories allocates the same memory as one processing 10.
	//
	// RESULT COLLECTION: Why per-dirWalkerWorker slices instead of shared mutex?
	//
	// With a shared results slice, every result append needs a mutex lock.
	// For 100K files, that's 100K lock/unlock cycles with potential contention.
	//
	// Per-dirWalkerWorker slices eliminate contention entirely. Each dirWalkerWorker appends
	// freely to its own slice. At the end, we merge once - O(workers) locks
	// instead of O(files).
	dirWalkerWorker := func(workerID int) {
		bufs := &workerBufs{
			dirEntryBuf: make([]byte, 0, dirReadBufSize),
			// dataBuf, dataArena, worker.buf start zero-valued, grow as needed.
		}

		// When processing directories recursively, within a single directory, we use Workers=1.
		// Parallelism is across directories (multiple tree workers), not within each one.
		dirWorkerCount := 1

		// Pre-allocate arenas for pipelining large directories.
		// Channel-as-freelist: producer/workers grab arenas, return when done.
		//
		// Why pipelineArenaCount (6)?
		//   With Workers=1, we need: 1 (producer holding) + 4 (queued) + 1 (worker holding) = 6.
		freeArenas := make(chan *pathArena, pipelineArenaCount)
		for range pipelineArenaCount {
			freeArenas <- &pathArena{}
		}
		pipelineQueueCap := pipelineArenaCount - dirWorkerCount - 1

		// These are passed to processDirAdaptive so it reuses our pre-allocated
		// resources instead of allocating fresh ones for each directory.
		reuseFreeArenas := freeArenas
		reuseQueueCap := pipelineQueueCap
		reuseSharedBufs := bufs

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
		for job := range workerPullDirJobsChan {
			func() {
				if ctx.Err() != nil {
					return
				}

				dh, err := openDir(job.absPath)
				if err != nil {
					ioErr := &IOError{Path: job.absPath.String(), Op: "open", Err: err}
					if p.errNotify.ioErr(ioErr) {
						workerErrs[workerID] = append(workerErrs[workerID], ioErr)
					}

					return
				}

				defer func() { _ = dh.close() }()

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
					// sending data into a channel needs to own its memory, so we need to copy here.
					case workerPushEvents <- treeEvent{job: dirJob{absPath: childAbs}}:
					case <-ctx.Done():
					}
				}

				bufs.pathsArena.reset(cap(bufs.dirEntryBuf) * 2)

				scan := dirScan{
					path:         job.absPath,
					handle:       dh,
					dirEntryBuf:  bufs.dirEntryBuf[:cap(bufs.dirEntryBuf)],
					reportSubdir: reportSubdir,
				}

				// Adaptive processing: switch to pipelined mode when the directory
				// exceeds SmallFileThreshold, otherwise process sequentially.
				dirResults, dirErrs := p.processDirAdaptive(
					ctx,
					scan,
					&bufs.pathsArena,
					dirWorkerCount,
					reuseFreeArenas,
					reuseQueueCap,
					reuseSharedBufs,
				)
				if len(dirErrs) > 0 {
					workerErrs[workerID] = append(workerErrs[workerID], dirErrs...)
				}

				if len(dirResults) > 0 {
					workerResults[workerID] = append(workerResults[workerID], dirResults...)
				}
			}()

			// Signal coordinator that this directory is done.
			// Always report completion to avoid pending-count leaks on cancel.
			workerPushEvents <- treeEvent{done: true}
		}
	}

	// Start tree workers.
	var wg sync.WaitGroup
	wg.Add(p.workers)

	for i := range p.workers {
		go func(id int) {
			defer wg.Done()

			dirWalkerWorker(id)
		}(i)
	}

	wg.Wait()
	coordWG.Wait()

	// Merge per-worker results into final slice.
	totalResults := 0
	for i := range p.workers {
		totalResults += len(workerResults[i])
	}

	results := make([]*T, 0, totalResults)
	for i := range p.workers {
		results = append(results, workerResults[i]...)
	}

	// Merge per-worker errors.
	totalErrs := 0
	for i := range p.workers {
		totalErrs += len(workerErrs[i])
	}

	allErrs := make([]error, 0, totalErrs)
	for i := range p.workers {
		allErrs = append(allErrs, workerErrs[i]...)
	}

	return results, allErrs
}

// ============================================================================
// ADAPTIVE DIRECTORY PROCESSING
// ============================================================================

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
func (p processor[T]) processDirAdaptive(
	ctx context.Context,
	dirScan dirScan,
	pathArena *pathArena,
	workerCount int,
	reuseFreeArenas chan *pathArena,
	reuseQueueCap int,
	reuseSharedBufs *workerBufs,
) ([]*T, []error) {
	if ctx.Err() != nil {
		return nil, nil
	}

	crossedTreshold, readErr := p.readDirUntilThresholdOrEOFIntoArena(ctx, dirScan, pathArena, p.smallFileThreshold)

	if readErr != nil && errors.Is(readErr, io.EOF) {
		readErr = nil
	}

	// If no entries are found, return early.
	if len(pathArena.entries) == 0 {
		if readErr == nil {
			return nil, nil
		}

		ioErr := &IOError{Path: dirScan.path.String(), Op: "readdir", Err: readErr}
		if p.errNotify.ioErr(ioErr) {
			return nil, []error{ioErr}
		}

		return nil, nil
	}

	var results []*T
	var errors []error

	if readErr != nil {
		// If an error occurred while reading from the directory handle, report it aswell,
		// We keep processing files found before the error regrardless of the error.
		ioErr := &IOError{Path: dirScan.path.String(), Op: "readdir", Err: readErr}
		if p.errNotify.ioErr(ioErr) {
			errors = append(errors, ioErr)
		}
	}

	// Recheck if we got a cancellation (ioErr allows the user to cancel early).
	if ctx.Err() != nil {
		return nil, nil
	}

	if crossedTreshold {
		results, errors = p.processFilesParallel(
			ctx,
			dirScan,
			pathArena.entries,
			workerCount,
			reuseQueueCap,
			reuseFreeArenas,
			reuseSharedBufs,
		)
	} else {
		var buffers *workerBufs
		if reuseSharedBufs != nil {
			buffers = reuseSharedBufs
		} else {
			buffers = &workerBufs{
				// dataBuf, dataArena, worker.buf start zero-valued, grow as needed.
			}
		}

		results = make([]*T, 0, len(pathArena.entries))

		p.processFilesSequentiallyInto(ctx, dirScan.handle, pathArena.entries, buffers, &results, &errors)
	}

	return results, errors
}

// readDirUntilThresholdOrEOFIntoArena reads directory entries from the opened dir handle
// into the pathArean until EOF, or until the passed threshold is reached.
func (p processor[T]) readDirUntilThresholdOrEOFIntoArena(
	ctx context.Context,
	dirScan dirScan,
	arena *pathArena,
	threshold int,
) (bool, error) {
	for {
		if ctx.Err() != nil {
			return false, io.EOF
		}

		err := p.readDirBatch(dirScan, arena)
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

func (p processor[T]) processFilesParallel(
	ctx context.Context,
	scan dirScan,
	initialEntries []pathEntry,
	workerCount int,
	queueCap int,
	freeArenas chan *pathArena,
	sharedBufs *workerBufs,
) ([]*T, []error) {
	// If caller didn't supply a pool, allocate a new one sized for this pipeline.
	if freeArenas == nil {
		var numArenas int
		if queueCap == 0 {
			queueCap, numArenas = pipelineSizing(workerCount)
		} else {
			numArenas = workerCount + queueCap + 1
		}

		freeArenas = make(chan *pathArena, numArenas)
		for range numArenas {
			freeArenas <- &pathArena{}
		}
	} else if queueCap == 0 {
		// Derive queue capacity from the provided pool size.
		// This keeps queue sizing and pool sizing consistent.
		queueCap = max(cap(freeArenas)-workerCount-1, 0)
	}

	// Shared pipeline runner for large directories.
	// Producer enumerates entries into arenas; workers drain arenas.
	// freeArenas provides bounded arena reuse to avoid allocations.
	arenaCh := make(chan *pathArena, queueCap)
	// Producer errors are collected by the producer goroutine and merged later.
	producerErrs := make([]error, 0, 1)

	// workersDone lets producer/arena-pool exit cleanly when workers stop.
	workersDone := make(chan struct{})
	pool := arenaPool{
		freeArenas: freeArenas,
		done:       workersDone,
	}

	sendArena := func(arena *pathArena) bool {
		if arena == nil {
			return false
		}

		if len(arena.entries) == 0 {
			pool.put(arena)

			return true
		}

		select {
		case arenaCh <- arena:
			return true
		case <-ctx.Done():
			pool.put(arena)

			return false
		case <-workersDone:
			pool.put(arena)

			return false
		}
	}

	var producerWG sync.WaitGroup

	producerWG.Go(func() {
		defer close(arenaCh)

		storageCap := cap(scan.dirEntryBuf) * 2

		if len(initialEntries) > 0 {
			arena := pool.get(ctx, storageCap)
			if arena == nil {
				return
			}

			for _, entry := range initialEntries {
				arena.addPath(scan.path, entry.name)
			}

			if !sendArena(arena) {
				return
			}
		}

		for {
			if ctx.Err() != nil {
				return
			}

			arena := pool.get(ctx, storageCap)
			if arena == nil {
				return
			}

			err := p.readDirBatch(scan, arena)

			if !sendArena(arena) {
				return
			}

			if err == nil {
				continue
			}

			if errors.Is(err, io.EOF) {
				return
			}

			ioErr := &IOError{Path: scan.path.String(), Op: "readdir", Err: err}
			if p.errNotify.ioErr(ioErr) {
				// Producer errors are single-threaded; collect and merge after wait.
				producerErrs = append(producerErrs, ioErr)
			}

			return
		}
	})

	// Per-worker slices avoid lock contention on hot paths.
	workerResults := make([][]*T, 0, workerCount)

	workerErrs := make([][]error, 0, workerCount)
	for range workerCount {
		workerResults = append(workerResults, nil)
		workerErrs = append(workerErrs, nil)
	}

	worker := func(workerID int) {
		// sharedBufs is only safe when workerCount==1 (tree pipeline).
		bufs := sharedBufs
		if bufs == nil {
			bufs = &workerBufs{
				// dataBuf, dataArena, worker.buf start zero-valued, grow as needed.
			}
		}

		localResults := make([]*T, 0, 64)
		localErrs := make([]error, 0, 32)

		// Drain arenas even after cancellation to return them to the pool.
		for arena := range arenaCh {
			if ctx.Err() != nil {
				pool.put(arena)

				continue
			}

			func() {
				defer pool.put(arena)

				p.processFilesSequentiallyInto(ctx, scan.handle, arena.entries, bufs, &localResults, &localErrs)
			}()
		}

		workerResults[workerID] = localResults
		workerErrs[workerID] = localErrs
	}

	var wg sync.WaitGroup
	wg.Add(workerCount)

	for workerID := range workerCount {
		go func(id int) {
			defer wg.Done()

			worker(id)
		}(workerID)
	}

	go func() {
		wg.Wait()
		close(workersDone)
	}()

	producerWG.Wait()
	wg.Wait()

	// Merge per-worker results/errors without lock contention.
	totalResults := 0
	for i := range workerCount {
		totalResults += len(workerResults[i])
	}

	results := make([]*T, 0, totalResults)
	for i := range workerCount {
		results = append(results, workerResults[i]...)
	}

	totalErrs := 0
	for i := range workerCount {
		totalErrs += len(workerErrs[i])
	}

	totalErrs += len(producerErrs)

	allErrs := make([]error, 0, totalErrs)
	for i := range workerCount {
		allErrs = append(allErrs, workerErrs[i]...)
	}

	if len(producerErrs) > 0 {
		allErrs = append(allErrs, producerErrs...)
	}

	return results, allErrs
}

// processFilesSequentiallyInto is the innermost processing loop.
func (p processor[T]) processFilesSequentiallyInto(
	ctx context.Context,
	dh dirHandle,
	entries []pathEntry,
	bufs *workerBufs,
	results *[]*T,
	errs *[]error,
) {
	var (
		openFH fileHandle
		fhOpen bool
	)

	// Ensure open file handles are closed even if a user callback panics.
	// We intentionally do not do per-file defers; this is one defer per arena.
	defer func() {
		if recovered := recover(); recovered != nil {
			if fhOpen {
				_ = openFH.close()
			}

			panic(recovered)
		}
	}()

	// closeIfOpen closes the current file handle and reports close errors.
	// Converts path to string only when reporting an error.
	closeIfOpen := func(path nulTermPath) bool {
		if !fhOpen {
			return false
		}

		closeErr := openFH.close()
		fhOpen = false

		if closeErr != nil {
			ioErr := &IOError{Path: path.String(), Op: "close", Err: closeErr}
			if p.errNotify.ioErr(ioErr) {
				*errs = append(*errs, ioErr)
			}

			if ctx.Err() != nil {
				return true
			}
		}

		return false
	}

	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}

		bufs.worker.buf = bufs.worker.buf[:0]

		// Reuse File struct from workerBufs to avoid per-file heap allocation.
		bufs.file = File{
			dh:        dh,
			name:      entry.name,
			path:      entry.path,
			fh:        &openFH,
			fhOpen:    &fhOpen,
			dataBuf:   &bufs.fileBytesBuf,
			dataArena: &bufs.fileBytesArena,
		}

		val, fnErr := p.fn(&bufs.file, &bufs.worker)
		if fnErr != nil {
			// Silent skip for race conditions (file became dir/symlink)
			if errors.Is(fnErr, errSkipFile) {
				if fhOpen {
					_ = openFH.close()
					fhOpen = false
				}

				continue
			}

			procErr := &ProcessError{Path: entry.path.String(), Err: fnErr}
			if p.errNotify.callbackErr(procErr) {
				*errs = append(*errs, procErr)
			}

			// Callback may have opened file; ensure handle closed on error.
			if closeIfOpen(entry.path) {
				return
			}

			continue
		}

		if closeIfOpen(entry.path) {
			return
		}

		if val != nil {
			*results = append(*results, val)
		}
	}
}

// ============================================================================
// HELPERS
// ============================================================================

func (p processor[T]) readDirBatch(scan dirScan, arena *pathArena) error {
	// Expand buffer to full capacity—it's often passed as zero-length with
	// pre-allocated capacity, but the syscall needs actual space to write into.
	buf := scan.dirEntryBuf
	if cap(buf) != len(buf) {
		buf = buf[:cap(buf)]
	}

	return readDirBatch(scan.handle, scan.path, buf, p.suffix, arena, scan.reportSubdir)
}

func pipelineSizing(workerCount int) (int, int) {
	// Bound buffering so high worker counts can't over-allocate arenas.
	queueCap := min(workerCount*4, maxPipelineQueue)

	// Needed arenas: 1 (producer) + queueCap (channel buffer) + workers (in-flight).
	numArenas := workerCount + queueCap + 1

	return queueCap, numArenas
}

// arenaPool wraps a free-list channel with cancellation awareness.
type arenaPool struct {
	freeArenas chan *pathArena
	done       <-chan struct{}
}

func (p arenaPool) get(ctx context.Context, storageCap int) *pathArena {
	select {
	case arena := <-p.freeArenas:
		arena.reset(storageCap)

		return arena
	case <-ctx.Done():
		return nil
	case <-p.done:
		return nil
	}
}

func (p arenaPool) put(arena *pathArena) {
	arena.reset(0)

	select {
	case p.freeArenas <- arena:
	default:
	}
}

// ============================================================================
// Error notification
// ============================================================================

// errNotifier tracks error counts and calls OnError callback.
// Zero value is valid (no-op). Safe for concurrent use.
type errNotifier struct {
	mu           sync.Mutex
	ioErrs       int
	callbackErrs int
	onError      func(err error, ioErrs, callbackErrs int) bool
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
	n.callbackErrs++
	ioCount, callbackCount := n.ioErrs, n.callbackErrs
	n.mu.Unlock()

	return n.onError(err, ioCount, callbackCount)
}
