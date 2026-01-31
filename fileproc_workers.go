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
//	│    ├─► processDir()                  ← Allocates: dirBuf, batch         │
//	│    │     │                                                              │
//	│    │     ├─► processDirNames() ← Allocates: pathBuf             │
//	│    │     │     └─► processFilesInto()   ← Uses passed buffers           │
//	│    │     │                                                              │
//	│    │     └─► processDirPipelined()   ← Allocates: freeBatches channel   │
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
	// One of dirPath or dirPathBytes must be set for openDirFromReaddir.
	// dirPathBytes is NUL-terminated (tree traversal).
	dirPath      string
	dirPathBytes []byte

	relPrefix     []byte
	dirEnumerator readdirHandle
	initialNames  [][]byte
	suffix        string
	reportSubdir  func(name []byte)
	notifier      *errNotifier
	dirBuf        []byte
	// Filled by processDirPipelined before running the pipeline.
	dirHandle dirHandle
	dirRel    string

	// workerCount/queueCapacity define pipeline sizing.
	// If queueCapacity is zero and freeBatches is nil, we compute sizing from workerCount.
	workerCount   int
	queueCapacity int

	// Optional: reuse pre-allocated batches/buffers (tree pipeline).
	// sharedBufs requires workerCount==1 and an exclusive caller (tree worker).
	freeBatches chan *nameBatch
	sharedBufs  *workerBufs
}

// processDirNames handles open+process+readdir-error reporting in one place.
// If bufs is nil, it allocates per-call worker buffers (non-recursive path).
// Tree workers pass their owned bufs to avoid per-directory allocations.
func (p processor[T]) processDirNames(
	ctx context.Context,
	dirPath []byte,
	relPrefix []byte,
	names [][]byte,
	readErr error,
	notifier *errNotifier,
	bufs *workerBufs,
) ([]*T, []error) {
	if readErr != nil && errors.Is(readErr, io.EOF) {
		readErr = nil
	}

	if len(names) == 0 {
		if readErr == nil {
			return nil, nil
		}

		ioErr := &IOError{Path: relPathString(relPrefix), Op: "readdir", Err: readErr}
		if notifier.ioErr(ioErr) {
			return nil, []error{ioErr}
		}

		return nil, nil
	}

	dh, openErr, ok := openDirForFiles(dirPath, relPrefix, notifier)
	if !ok {
		var errs []error
		if openErr != nil {
			errs = append(errs, openErr)
		}

		if readErr != nil {
			ioErr := &IOError{Path: relPathString(relPrefix), Op: "readdir", Err: readErr}
			if notifier.ioErr(ioErr) {
				errs = append(errs, ioErr)
			}
		}

		return nil, errs
	}

	defer func() { _ = dh.closeHandle() }()

	if bufs == nil {
		pathBufCap := len(relPrefix) + pathBufExtra
		bufs = &workerBufs{
			pathBuf: make([]byte, 0, pathBufCap),
			// dataBuf, dataArena, worker.buf start zero-valued, grow as needed.
		}
	}

	cfg := fileProcCfg[T]{
		fn:       p.fn,
		notifier: notifier,
	}

	results, errs := processFilesWithHandle(ctx, dh, relPrefix, names, cfg, bufs)
	if readErr == nil {
		return results, errs
	}

	ioErr := &IOError{Path: relPathString(relPrefix), Op: "readdir", Err: readErr}
	if notifier.ioErr(ioErr) {
		errs = append(errs, ioErr)
	}

	return results, errs
}

func openDirForFiles(dirPath []byte, relPrefix []byte, notifier *errNotifier) (dirHandle, *IOError, bool) {
	dh, err := openDir(dirPath)
	if err == nil {
		return dh, nil, true
	}

	ioErr := &IOError{Path: relPathString(relPrefix), Op: "open", Err: err}
	if notifier.ioErr(ioErr) {
		return dirHandle{}, ioErr, false
	}

	return dirHandle{}, nil, false
}

func processFilesWithHandle[T any](
	ctx context.Context,
	dh dirHandle,
	relPrefix []byte,
	names [][]byte,
	cfg fileProcCfg[T],
	bufs *workerBufs,
) ([]*T, []error) {
	results := make([]*T, 0, len(names))

	var allErrs []error

	out := fileProcOut[T]{results: &results, errs: &allErrs}
	processFilesInto(ctx, dh, relPrefix, names, cfg, bufs, out)

	return results, allErrs
}

// ============================================================================
// CORE FILE PROCESSING
// ============================================================================

// processFilesInto is the innermost processing loop.
func processFilesInto[T any](
	ctx context.Context,
	dh dirHandle,
	relPrefix []byte,
	names [][]byte,
	cfg fileProcCfg[T],
	bufs *workerBufs,
	out fileProcOut[T],
) {
	var (
		openFH fileHandle
		fhOpen bool
	)

	// Ensure open file handles are closed even if a user callback panics.
	// We intentionally do not do per-file defers; this is one defer per batch.
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
	closeIfOpen := func(relPath []byte) bool {
		if !fhOpen {
			return false
		}

		closeErr := openFH.closeHandle()
		fhOpen = false

		if closeErr != nil {
			ioErr := &IOError{Path: string(relPath), Op: "close", Err: closeErr}
			if cfg.notifier.ioErr(ioErr) {
				*out.errs = append(*out.errs, ioErr)
			}

			if ctx.Err() != nil {
				return true
			}
		}

		return false
	}

	for _, name := range names {
		if ctx.Err() != nil {
			return
		}

		if nameLen(name) == 0 {
			continue
		}

		var relPath []byte

		if len(relPrefix) > 0 {
			bufs.pathBuf = buildRelPath(bufs.pathBuf, relPrefix, name)
			relPath = bufs.pathBuf
		} else {
			relPath = name[:nameLen(name)]
		}

		bufs.worker.buf = bufs.worker.buf[:0]

		// Reuse File struct from workerBufs to avoid per-file heap allocation.
		bufs.file = File{
			dh:        dh,
			name:      name,
			relPath:   relPath,
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

			procErr := &ProcessError{Path: string(relPath), Err: fnErr}
			if cfg.notifier.callbackErr(procErr) {
				*out.errs = append(*out.errs, procErr)
			}

			// Callback may have opened file; ensure handle closed on error.
			if closeIfOpen(relPath) {
				return
			}

			if ctx.Err() != nil {
				return
			}

			continue
		}

		if closeIfOpen(relPath) {
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
//   - initialNames entries include a trailing NUL terminator.
//   - queueCapacity/freeBatches sizing must be coordinated to avoid leaks.

func pipelineSizing(workerCount int) (int, int) {
	// Bound buffering so high worker counts can't over-allocate batches.
	queueCap := min(workerCount*4, maxPipelineQueue)

	// Needed batches: 1 (producer) + queueCap (channel buffer) + workers (in-flight).
	numBatches := workerCount + queueCap + 1

	return queueCap, numBatches
}

// batchPool wraps a free-list channel with cancellation awareness.
type batchPool struct {
	freeBatches chan *nameBatch
	done        <-chan struct{}
}

func (p batchPool) get(ctx context.Context, storageCap int) *nameBatch {
	select {
	case batch := <-p.freeBatches:
		batch.reset(storageCap)

		return batch
	case <-ctx.Done():
		return nil
	case <-p.done:
		return nil
	}
}

func (p batchPool) put(batch *nameBatch) {
	batch.reset(0)

	select {
	case p.freeBatches <- batch:
	default:
	}
}

func (p processor[T]) runDirPipeline(ctx context.Context, args *dirPipelineArgs) ([]*T, []error) {
	// Shared pipeline runner for large directories.
	// Producer enumerates entries into batches; workers drain batches.
	// freeBatches provides bounded batch reuse to avoid allocations.
	cfg := fileProcCfg[T]{
		fn:       p.fn,
		notifier: args.notifier,
	}

	dirRel := args.dirRel
	if dirRel == "" {
		dirRel = relPathString(args.relPrefix)
	}

	batchCh := make(chan *nameBatch, args.queueCapacity)
	// Producer errors are collected by the producer goroutine and merged later.
	producerErrs := make([]error, 0, 1)

	// workersDone lets producer/batch-pool exit cleanly when workers stop.
	workersDone := make(chan struct{})
	pool := batchPool{
		freeBatches: args.freeBatches,
		done:        workersDone,
	}

	sendBatch := func(batch *nameBatch) bool {
		if batch == nil {
			return false
		}

		if len(batch.names) == 0 {
			pool.put(batch)

			return true
		}

		select {
		case batchCh <- batch:
			return true
		case <-ctx.Done():
			pool.put(batch)

			return false
		case <-workersDone:
			pool.put(batch)

			return false
		}
	}

	var producerWG sync.WaitGroup

	producerWG.Go(func() {
		defer close(batchCh)

		storageCap := cap(args.dirBuf) * 2

		if len(args.initialNames) > 0 {
			batch := pool.get(ctx, storageCap)
			if batch == nil {
				return
			}

			for _, n := range args.initialNames {
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

			batch := pool.get(ctx, storageCap)
			if batch == nil {
				return
			}

			err := readDirBatch(args.dirEnumerator, args.dirBuf[:cap(args.dirBuf)], args.suffix, batch, args.reportSubdir)

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
			pathBufCap := len(args.relPrefix) + pathBufExtra
			bufs = &workerBufs{
				pathBuf: make([]byte, 0, pathBufCap),
				// dataBuf, dataArena, worker.buf start zero-valued, grow as needed.
			}
		}

		localResults := make([]*T, 0, 64)
		localErrs := make([]error, 0, 32)
		out := fileProcOut[T]{results: &localResults, errs: &localErrs}

		// Drain batches even after cancellation to return them to the pool.
		for batch := range batchCh {
			if ctx.Err() != nil {
				pool.put(batch)

				continue
			}

			func() {
				defer pool.put(batch)

				processFilesInto(ctx, args.dirHandle, args.relPrefix, batch.names, cfg, bufs, out)
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

func (args *dirPipelineArgs) openDirPath() string {
	if args.dirPath != "" {
		return args.dirPath
	}

	if len(args.dirPathBytes) > 0 {
		return pathStr(args.dirPathBytes)
	}

	return ""
}

func (p processor[T]) processDirPipelined(ctx context.Context, args *dirPipelineArgs) ([]*T, []error) {
	dirRel := relPathString(args.relPrefix)

	dh, err := openDirFromReaddir(args.dirEnumerator, args.openDirPath())
	if err != nil {
		ioErr := &IOError{Path: dirRel, Op: "open", Err: err}
		if args.notifier.ioErr(ioErr) {
			return nil, []error{ioErr}
		}

		return nil, nil
	}

	workerCount := args.workerCount
	if workerCount <= 0 {
		// Defensive: callers should always set this (opts.Workers or 1 for tree).
		workerCount = 1
	}

	queueCap := args.queueCapacity
	freeBatches := args.freeBatches

	// If caller didn't supply a batch pool, allocate a new one sized for this pipeline.
	if freeBatches == nil {
		var numBatches int
		if queueCap == 0 {
			queueCap, numBatches = pipelineSizing(workerCount)
		} else {
			numBatches = workerCount + queueCap + 1
		}

		freeBatches = make(chan *nameBatch, numBatches)
		for range numBatches {
			freeBatches <- &nameBatch{}
		}
	} else if queueCap == 0 {
		// Derive queue capacity from the provided pool size.
		// This keeps queue sizing and pool sizing consistent.
		queueCap = max(cap(freeBatches)-workerCount-1, 0)
	}

	pipelineArgs := *args
	pipelineArgs.dirHandle = dh
	pipelineArgs.dirRel = dirRel
	pipelineArgs.workerCount = workerCount
	pipelineArgs.queueCapacity = queueCap
	pipelineArgs.freeBatches = freeBatches

	return p.runDirPipeline(ctx, &pipelineArgs)
}
