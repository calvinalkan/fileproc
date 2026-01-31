package fileproc

// fileproc_workers.go contains the worker pool and pipeline orchestration
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
//	│    │     └─► processDirPipelined()   ← Allocates: freeArenas channel   │
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
	"sync"
)

type fileProcCfg[T any] struct {
	fn       ProcessFunc[T]
	notifier *errNotifier
}

type fileProcOut[T any] struct {
	results *[]*T
	errs    *[]error
}

type dirPipelineArgs struct {
	// dirPath is NUL-terminated path for syscalls.
	dirPath nulTermPath

	dirHandle      dirHandle
	initialEntries []pathEntry
	suffix         string
	reportSubdir   func(nulTermName)
	notifier       *errNotifier
	dirBuf         []byte

	// workerCount/queueCapacity define pipeline sizing.
	// If queueCapacity is zero and freeArenas is nil, we compute sizing from workerCount.
	workerCount   int
	queueCapacity int

	// Optional: reuse pre-allocated arenas/buffers (tree pipeline).
	// sharedBufs requires workerCount==1 and an exclusive caller (tree worker).
	freeArenas chan *pathArena
	sharedBufs *workerBufs
}

// processDirNames handles open+process+readdir-error reporting in one place.
// If bufs is nil, it allocates per-call worker buffers (non-recursive path).
// Tree workers pass their owned bufs to avoid per-directory allocations.
func (p processor[T]) processDirNames(
	ctx context.Context,
	dirPath nulTermPath,
	dh dirHandle,
	entries []pathEntry,
	readErr error,
	notifier *errNotifier,
	bufs *workerBufs,
) ([]*T, []error) {
	if readErr != nil && errors.Is(readErr, io.EOF) {
		readErr = nil
	}

	if len(entries) == 0 {
		if readErr == nil {
			return nil, nil
		}

		ioErr := &IOError{Path: dirPath.String(), Op: "readdir", Err: readErr}
		if notifier.ioErr(ioErr) {
			return nil, []error{ioErr}
		}

		return nil, nil
	}

	if bufs == nil {
		bufs = &workerBufs{
			// dataBuf, dataArena, worker.buf start zero-valued, grow as needed.
		}
	}

	cfg := fileProcCfg[T]{
		fn:       p.fn,
		notifier: notifier,
	}

	results, errs := processFilesWithHandle(ctx, dh, entries, cfg, bufs)
	if readErr == nil {
		return results, errs
	}

	ioErr := &IOError{Path: dirPath.String(), Op: "readdir", Err: readErr}
	if notifier.ioErr(ioErr) {
		errs = append(errs, ioErr)
	}

	return results, errs
}

func processFilesWithHandle[T any](
	ctx context.Context,
	dh dirHandle,
	entries []pathEntry,
	cfg fileProcCfg[T],
	bufs *workerBufs,
) ([]*T, []error) {
	results := make([]*T, 0, len(entries))

	var allErrs []error

	out := fileProcOut[T]{results: &results, errs: &allErrs}
	processFilesInto(ctx, dh, entries, cfg, bufs, out)

	return results, allErrs
}

// ============================================================================
// CORE FILE PROCESSING
// ============================================================================

// processFilesInto is the innermost processing loop.
func processFilesInto[T any](
	ctx context.Context,
	dh dirHandle,
	entries []pathEntry,
	cfg fileProcCfg[T],
	bufs *workerBufs,
	out fileProcOut[T],
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
				_ = openFH.closeHandle()
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

		closeErr := openFH.closeHandle()
		fhOpen = false

		if closeErr != nil {
			ioErr := &IOError{Path: path.String(), Op: "close", Err: closeErr}
			if cfg.notifier.ioErr(ioErr) {
				*out.errs = append(*out.errs, ioErr)
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
			dataBuf:   &bufs.dataBuf,
			dataArena: &bufs.dataArena,
		}

		val, fnErr := cfg.fn(&bufs.file, &bufs.worker)
		if fnErr != nil {
			// Silent skip for race conditions (file became dir/symlink)
			if errors.Is(fnErr, errSkipFile) {
				if fhOpen {
					_ = openFH.closeHandle()
					fhOpen = false
				}

				continue
			}

			procErr := &ProcessError{Path: entry.path.String(), Err: fnErr}
			if cfg.notifier.callbackErr(procErr) {
				*out.errs = append(*out.errs, procErr)
			}

			// Callback may have opened file; ensure handle closed on error.
			if closeIfOpen(entry.path) {
				return
			}

			if ctx.Err() != nil {
				return
			}

			continue
		}

		if closeIfOpen(entry.path) {
			return
		}

		if val != nil {
			*out.results = append(*out.results, val)
		}
	}
}

// ============================================================================
// PIPELINED PROCESSING (large directories)
// ============================================================================

// dirPipelineArgs controls the shared pipeline runner.
//
// Invariants:
//   - sharedBufs is only set when workerCount==1 (tree pipeline).
//   - initialEntries include trailing NUL terminators.
//   - queueCapacity/freeArenas sizing must be coordinated to avoid leaks.

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

func (p processor[T]) runDirPipeline(ctx context.Context, args *dirPipelineArgs) ([]*T, []error) {
	// Shared pipeline runner for large directories.
	// Producer enumerates entries into arenas; workers drain arenas.
	// freeArenas provides bounded arena reuse to avoid allocations.
	cfg := fileProcCfg[T]{
		fn:       p.fn,
		notifier: args.notifier,
	}

	arenaCh := make(chan *pathArena, args.queueCapacity)
	// Producer errors are collected by the producer goroutine and merged later.
	producerErrs := make([]error, 0, 1)

	// workersDone lets producer/arena-pool exit cleanly when workers stop.
	workersDone := make(chan struct{})
	pool := arenaPool{
		freeArenas: args.freeArenas,
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

		storageCap := cap(args.dirBuf) * 2

		if len(args.initialEntries) > 0 {
			arena := pool.get(ctx, storageCap)
			if arena == nil {
				return
			}

			for _, entry := range args.initialEntries {
				arena.addPath(args.dirPath, entry.name)
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

			err := readDirBatch(args.dirHandle, args.dirPath, args.dirBuf[:cap(args.dirBuf)], args.suffix, arena, args.reportSubdir)

			if !sendArena(arena) {
				return
			}

			if err == nil {
				continue
			}

			if errors.Is(err, io.EOF) {
				return
			}

			ioErr := &IOError{Path: args.dirPath.String(), Op: "readdir", Err: err}
			if args.notifier.ioErr(ioErr) {
				// Producer errors are single-threaded; collect and merge after wait.
				producerErrs = append(producerErrs, ioErr)
			}

			return
		}
	})

	// Per-worker slices avoid lock contention on hot paths.
	workerResults := make([][]*T, 0, args.workerCount)

	workerErrs := make([][]error, 0, args.workerCount)
	for range args.workerCount {
		workerResults = append(workerResults, nil)
		workerErrs = append(workerErrs, nil)
	}

	worker := func(workerID int) {
		// sharedBufs is only safe when workerCount==1 (tree pipeline).
		bufs := args.sharedBufs
		if bufs == nil {
			bufs = &workerBufs{
				// dataBuf, dataArena, worker.buf start zero-valued, grow as needed.
			}
		}

		localResults := make([]*T, 0, 64)
		localErrs := make([]error, 0, 32)
		out := fileProcOut[T]{results: &localResults, errs: &localErrs}

		// Drain arenas even after cancellation to return them to the pool.
		for arena := range arenaCh {
			if ctx.Err() != nil {
				pool.put(arena)

				continue
			}

			func() {
				defer pool.put(arena)

				processFilesInto(ctx, args.dirHandle, arena.entries, cfg, bufs, out)
			}()
		}

		workerResults[workerID] = localResults
		workerErrs[workerID] = localErrs
	}

	var wg sync.WaitGroup
	wg.Add(args.workerCount)

	for workerID := range args.workerCount {
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
	for i := range args.workerCount {
		totalResults += len(workerResults[i])
	}

	results := make([]*T, 0, totalResults)
	for i := range args.workerCount {
		results = append(results, workerResults[i]...)
	}

	totalErrs := 0
	for i := range args.workerCount {
		totalErrs += len(workerErrs[i])
	}

	totalErrs += len(producerErrs)

	allErrs := make([]error, 0, totalErrs)
	for i := range args.workerCount {
		allErrs = append(allErrs, workerErrs[i]...)
	}

	if len(producerErrs) > 0 {
		allErrs = append(allErrs, producerErrs...)
	}

	return results, allErrs
}

func (p processor[T]) processDirPipelined(ctx context.Context, args *dirPipelineArgs) ([]*T, []error) {
	workerCount := args.workerCount
	queueCap := args.queueCapacity
	freeArenas := args.freeArenas

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

	pipelineArgs := *args
	pipelineArgs.workerCount = workerCount
	pipelineArgs.queueCapacity = queueCap
	pipelineArgs.freeArenas = freeArenas

	return p.runDirPipeline(ctx, &pipelineArgs)
}
