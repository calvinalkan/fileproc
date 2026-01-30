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
//	│    │     ├─► processFilesSequential() ← Allocates: pathBuf              │
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

type dirPipelinedArgs struct {
	dir          string
	relPrefix    []byte
	rh           readdirHandle
	initial      [][]byte
	opts         options
	reportSubdir func(name []byte)
	notifier     *errNotifier
	dirBuf       []byte
}

// ============================================================================
// SEQUENTIAL PROCESSING (small directories)
// ============================================================================

func (p processor[T]) processFilesSequential(
	ctx context.Context,
	dir string,
	relPrefix []byte,
	names [][]byte,
	notifier *errNotifier,
) ([]*T, []error) {
	dirPath := pathWithNul(dir)

	dh, err := openDir(dirPath)
	if err != nil {
		dirRel := "."
		if len(relPrefix) > 0 {
			dirRel = string(relPrefix)
		}

		ioErr := &IOError{Path: dirRel, Op: "open", Err: err}
		if notifier.ioErr(ioErr) {
			return nil, []error{ioErr}
		}

		return nil, nil
	}

	defer func() { _ = dh.closeHandle() }()

	pathBufCap := len(relPrefix) + pathBufExtra
	bufs := &workerBufs{
		pathBuf: make([]byte, 0, pathBufCap),
		// dataBuf, dataArena, worker.buf start zero-valued, grow as needed.
	}

	results := make([]*T, 0, len(names))

	var allErrs []error

	cfg := fileProcCfg[T]{
		fn:       p.fn,
		notifier: notifier,
	}

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
	getPathStr := func(relPath []byte) string { return string(relPath) }

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

	for _, name := range names {
		if ctx.Err() != nil {
			return
		}

		if nameLen(name) == 0 {
			continue
		}

		var relPath []byte

		if len(relPrefix) > 0 {
			bufs.pathBuf = appendPathBytesPrefix(bufs.pathBuf, relPrefix, name)
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

			procErr := &ProcessError{Path: getPathStr(relPath), Err: fnErr}
			if cfg.notifier.callbackErr(procErr) {
				*out.errs = append(*out.errs, procErr)
			}

			// Callback may have opened file; ensure handle closed on error.
			if fhOpen {
				closeErr := openFH.closeHandle()
				fhOpen = false

				if closeErr != nil {
					ioErr := &IOError{Path: getPathStr(relPath), Op: "close", Err: closeErr}
					if cfg.notifier.ioErr(ioErr) {
						*out.errs = append(*out.errs, ioErr)
					}

					if ctx.Err() != nil {
						return
					}
				}
			}

			if ctx.Err() != nil {
				return
			}

			continue
		}

		if fhOpen {
			closeErr := openFH.closeHandle()
			fhOpen = false

			if closeErr != nil {
				ioErr := &IOError{Path: getPathStr(relPath), Op: "close", Err: closeErr}
				if cfg.notifier.ioErr(ioErr) {
					*out.errs = append(*out.errs, ioErr)
				}

				if ctx.Err() != nil {
					return
				}
			}
		}

		if val != nil {
			*out.results = append(*out.results, val)
		}
	}
}

// ============================================================================
// PIPELINED PROCESSING (large directories)
// ============================================================================

func (p processor[T]) processDirPipelined(ctx context.Context, args *dirPipelinedArgs) ([]*T, []error) {
	dirRel := "."
	if len(args.relPrefix) > 0 {
		dirRel = string(args.relPrefix)
	}

	dh, err := openDirFromReaddir(args.rh, args.dir)
	if err != nil {
		ioErr := &IOError{Path: dirRel, Op: "open", Err: err}
		if args.notifier.ioErr(ioErr) {
			return nil, []error{ioErr}
		}

		return nil, nil
	}

	relPrefix := args.relPrefix
	rh := args.rh
	initial := args.initial
	opts := args.opts
	reportSubdir := args.reportSubdir
	notifier := args.notifier
	dirBuf := args.dirBuf

	cfg := fileProcCfg[T]{
		fn:       p.fn,
		notifier: notifier,
	}

	// Buffering between the readdir producer and workers is bounded to avoid
	// excessive memory usage when Workers is set very high.
	queueCap := min(opts.Workers*4, maxPipelineQueue)

	// Needed batches: 1 (producer) + queueCap (channel buffer) + Workers (in-flight).
	numBatches := opts.Workers + queueCap + 1

	freeBatches := make(chan *nameBatch, numBatches)
	for range numBatches {
		freeBatches <- &nameBatch{}
	}

	nameCh := make(chan *nameBatch, queueCap)

	var (
		mu      sync.Mutex
		results []*T
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

	workersDone := make(chan struct{})

	getBatch := func(storageCap int) *nameBatch {
		select {
		case batch := <-freeBatches:
			batch.reset(storageCap)

			return batch
		case <-ctx.Done():
			return nil
		case <-workersDone:
			return nil
		}
	}

	putBatch := func(batch *nameBatch) {
		batch.reset(0)

		select {
		case freeBatches <- batch:
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
		case <-workersDone:
			putBatch(batch)

			return false
		}
	}

	var producerWG sync.WaitGroup

	producerWG.Go(func() {
		defer close(nameCh)

		storageCap := cap(dirBuf) * 2

		if len(initial) > 0 {
			batch := getBatch(storageCap)
			if batch == nil {
				return
			}

			for _, n := range initial {
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

			err := readDirBatch(rh, dirBuf[:cap(dirBuf)], opts.Suffix, batch, reportSubdir)

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
			if notifier.ioErr(ioErr) {
				addErr(ioErr)
			}

			return
		}
	})

	worker := func() {
		pathBufCap := len(relPrefix) + pathBufExtra

		bufs := &workerBufs{
			pathBuf: make([]byte, 0, pathBufCap),
			// dataBuf, dataArena, worker.buf start zero-valued, grow as needed.
		}

		localResults := make([]*T, 0, 64)

		var localErrs []error

		defer func() {
			mu.Lock()

			results = append(results, localResults...)
			allErrs = append(allErrs, localErrs...)

			mu.Unlock()
		}()

		out := fileProcOut[T]{results: &localResults, errs: &localErrs}

		for {
			select {
			case <-ctx.Done():
				return
			case batch, ok := <-nameCh:
				if !ok {
					return
				}

				func() {
					defer putBatch(batch)

					processFilesInto(ctx, dh, relPrefix, batch.names, cfg, bufs, out)
				}()
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(opts.Workers)

	for range opts.Workers {
		go func() {
			defer wg.Done()

			worker()
		}()
	}

	go func() {
		wg.Wait()
		close(workersDone)
	}()

	producerWG.Wait()
	wg.Wait()

	return results, allErrs
}
