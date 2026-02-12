package fileproc

// processor.go contains the directory processing pipeline and worker orchestration
// used by [Process].
//
// Model: decoupled scan (discovery) and file processing. Directory reads use
// large buffers for syscall efficiency; work units are fixed-size chunks of
// entries, independent of readdir batch size.
//
// # How It Works
//
// This pipeline has three roles: a coordinator goroutine, scan workers, and
// file workers.
//
// 1) Coordinator goroutine: owns the directory queue and is the only sender on
//    scanQueue. It receives scanEvents (subdir discovered / dir done), tracks
//    pending work, and decides when scanning is complete.
// 2) Scan workers: open a directory, readDirBatch into a pathArena, split the
//    entries into fixed-size chunks, enqueue those chunks on fileJobs, and
//    report subdirectories via scanEvents (if recursive).
// 3) File workers: drain fileJobs, process each chunk sequentially, and call
//    the user callback with reusable buffers.
//
// dirLease keeps a directory handle open while any chunks for that directory
// are in flight. arena.pending counts outstanding chunks plus the scan worker's
// dispatch reference; the arena returns to the pool only after all refs drop.
//
// Backpressure:
//   - fileJobs is bounded to cap in-flight arenas and memory.
//   - scanQueue is unbuffered so the coordinator controls scheduling.
//   - arenaPool is bounded to cap retained arena memory.
//
// Cancellation:
//   - coordinator stops dispatching and closes scanQueue
//   - scan workers exit, fileJobs closes, file workers drain and release
//
// # Dataflow (single directory or recursive)
//
//   scanEvents (subdir/done) ────────────────┐
//                                           v
//   [Coordinator] ── dirJobs ──> scanQueue ──> [Scan workers]
//        ^                                      │
//        │                                      │ readDirBatch
//        │                                      v
//        │                             pathArena + dirLease
//        │                                      │ chunkSize
//        │                                      v
//        └─────────── scanEvents <── subdirs ── fileJobs ──> [File workers] ──> ProcessFunc

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// ============================================================================
// SCAN PIPELINE
// ============================================================================

// scanPipeline orchestrates directory discovery and chunk scheduling.
// It owns the coordinator goroutine, scan workers, and arena pool.
// Chunk processing is delegated to a scanConsumer.
type scanPipeline struct {
	// scanWorkers controls concurrent directory readers.
	scanWorkers int
	// chunkSize controls entries per work unit.
	chunkSize int
	// suffix filters files before chunking.
	suffix string
	// recursive enables subdirectory discovery.
	recursive bool
	// errNotify handles IO errors during scanning.
	errNotify *errNotifier
	// queueDepth controls backpressure (set by consumer).
	queueDepth int
}

// consumeChunkFunc processes a chunk produced by scanPipeline.
// Must call chunk.release() exactly once when done, even on error/cancel.
type consumeChunkFunc func(chunk fileChunk)

// run executes the pipeline: coordinator -> scan workers -> consumer.
// Blocks until scanning completes or ctx is canceled.
func (p *scanPipeline) run(ctx context.Context, root nulTermPath, consumeChunk consumeChunkFunc) []error {
	if ctx.Err() != nil {
		return nil
	}

	queueDepth := p.queueDepth
	if queueDepth <= 0 {
		queueDepth = 16
	}

	// Unbuffered keeps scheduling centralized in the coordinator.
	// It prevents scan workers from running ahead without coordinator visibility.
	scanQueue := make(chan dirJob)

	// Buffer avoids discovery stalls when coordinator is busy.
	// Size is small but enough to absorb bursts from multiple scanners.
	scanEvents := make(chan scanEvent, p.scanWorkers*64)

	// Covers buffered + in-flight chunks + scanner hold; +1 keeps producer moving.
	arenaCount := queueDepth + p.scanWorkers + 1
	// Reuse arenas to avoid per-directory allocations.
	pool := newArenaPool(arenaCount)

	// Per-scan-worker slices keep error collection lock-free.
	scanErrs := make([][]error, 0, p.scanWorkers)
	scanErrs = scanErrs[:p.scanWorkers]

	// releaseChunk centralizes cleanup so every path returns leases and arenas.
	// This avoids refcount leaks across success, cancellation, and error paths.
	releaseChunk := func(chunk fileChunk) {
		if chunk.lease != nil {
			chunk.lease.dec()
		}

		if chunk.arena == nil {
			return
		}

		// Only return the arena after the last chunk finishes.
		if chunk.arena.pending.Add(-1) == 0 {
			pool.put(chunk.arena)
		}
	}

	// sendScanEvent is a signal for discovery + completion.
	// It drops on cancellation to avoid deadlocks during shutdown.
	sendScanEvent := func(ev scanEvent) {
		select {
		case scanEvents <- ev:
		case <-ctx.Done():
		}
	}

	// ============================================================================
	// COORDINATOR GOROUTINE: DIRECTORY SCHEDULING
	// ============================================================================
	//
	// Owns the directory queue and is the only sender on scanQueue.
	// Receives scanEvents from scanners and feeds new work until pending hits zero.
	// Centralizing scheduling avoids shared-queue locks and keeps accounting exact.
	var coordWG sync.WaitGroup
	coordWG.Go(func() {
		scanQueueClosed := false

		// Slice queue avoids mutex contention of a shared queue.
		queue := make([]dirJob, 0, 1024)
		// Start with the root directory.
		queue = append(queue, dirJob{path: root})
		pending := len(queue)

		drainEvents := func(pending int, queue []dirJob) (int, []dirJob) {
			for {
				select {
				case ev := <-scanEvents:
					if ev.done {
						continue
					}

					if p.recursive {
						pending++

						queue = append(queue, dirJob{path: ev.path})
					}
				default:
					// Drain buffered discoveries before declaring completion.
					return pending, queue
				}
			}
		}

		for pending > 0 {
			if ctx.Err() != nil {
				// Stop dispatching; scanners will exit on closed queue.
				if !scanQueueClosed {
					close(scanQueue)
				}

				return
			}

			var (
				next  dirJob
				jobCh chan dirJob
			)

			if len(queue) > 0 {
				next = queue[0]
				jobCh = scanQueue
			}

			select {
			case ev := <-scanEvents:
				if ev.done {
					pending--
				} else if p.recursive {
					pending++

					queue = append(queue, dirJob{path: ev.path})
				}

				if pending == 0 {
					pending, queue = drainEvents(pending, queue)
				}

				if pending == 0 && !scanQueueClosed {
					// Close once to stop all scanners.
					close(scanQueue)

					scanQueueClosed = true
				}

			case <-ctx.Done():
				if !scanQueueClosed {
					close(scanQueue)
				}

				return

			case jobCh <- next:
				// Drop refs to popped dir to avoid retaining large trees in memory.
				queue[0] = dirJob{}
				queue = queue[1:]
			}
		}

		if !scanQueueClosed {
			close(scanQueue)
		}
	})

	// ============================================================================
	// SCAN WORKER POOL: DIRECTORY DISCOVERY
	// ============================================================================
	//
	// Each worker scans one directory at a time, producing chunks for the consumer.
	// Subdirectories are reported back to the coordinator via scanEvents.
	// Workers own their buffers to avoid contention and keep readdir hot.
	var scanWG sync.WaitGroup
	scanWG.Add(p.scanWorkers)

	// Scan workers run one directory at a time to keep readdir + openat scoped.
	// They push chunks to the consumer and subdir events to the coordinator.
	for scanID := range p.scanWorkers {
		go func(id int) {
			defer scanWG.Done()

			// Reuse the readdir buffer; size also seeds arena capacity.
			dirBuf := make([]byte, 0, dirReadBufSize)
			// Heuristic: dirent buffers include metadata; names are usually <= ~1/2.
			storageCap := cap(dirBuf) * 2
			localErrs := make([]error, 0, 8)

			for dirJob := range scanQueue {
				if ctx.Err() != nil {
					// Ensure coordinator can drain pending work on cancel.
					sendScanEvent(scanEvent{done: true})

					break
				}

				dh, err := openDir(dirJob.path)
				if err != nil {
					ioErr := &IOError{Path: dirJob.path.String(), Op: "open", Err: err}
					if p.errNotify == nil || p.errNotify.ioErr(ioErr) {
						localErrs = append(localErrs, ioErr)
					}
					// Always signal done to keep pending counts accurate.
					sendScanEvent(scanEvent{done: true})

					continue
				}

				// Hold the dir open so openat remains valid for all chunks.
				lease := &dirLease{dh: dh, base: dirJob.path, baseHash: hashBase(dirJob.path)}
				lease.refs.Store(1)

				// no-op unless recursive.
				reportSubdir := reportSubdirFunc(nil)
				if p.recursive {
					reportSubdir = func(name nulTermName) {
						child := dirJob.path.joinName(name)
						sendScanEvent(scanEvent{path: child})
					}
				}

				for ctx.Err() == nil {
					// Bounded pool ensures discovery can't run away in memory.
					arena := pool.get(ctx, storageCap)
					if arena == nil {
						// context canceled.
						break
					}

					buf := dirBuf[:cap(dirBuf)]

					err := readDirBatch(dh, dirJob.path, buf, p.suffix, arena, reportSubdir)

					empty := len(arena.entries) == 0
					if empty {
						pool.put(arena)
					} else {
						// Even on read error, process what we already discovered.
						chunkCount := int32((len(arena.entries) + p.chunkSize - 1) / p.chunkSize)
						// +1 holds the arena until the scan worker finishes dispatching.
						arena.pending.Store(chunkCount + 1)
						// Keep the dir handle alive until all chunks finish.
						lease.refs.Add(chunkCount)

						sentAll := true

						for start := 0; start < len(arena.entries); start += p.chunkSize {
							end := min(start+p.chunkSize, len(arena.entries))

							chunk := fileChunk{arena: arena, start: start, end: end, lease: lease}

							// Push to consumer; may block if consumer is slow (backpressure).
							select {
							case <-ctx.Done():
								sentAll = false
								// We couldn't deliver; release this and remaining chunk refs now.
								releaseChunk(chunk)

								for rest := start + p.chunkSize; rest < len(arena.entries); rest += p.chunkSize {
									releaseChunk(fileChunk{arena: arena, start: rest, end: min(rest+p.chunkSize, len(arena.entries)), lease: lease})
								}
							default:
								chunk.release = func() { releaseChunk(chunk) }
								consumeChunk(chunk)
							}

							if !sentAll {
								break
							}
						}

						// Release the scan worker's arena reference after dispatch.
						if arena.pending.Add(-1) == 0 {
							pool.put(arena)
						}

						if !sentAll {
							// Context canceled.
							break
						}
					}

					if err != nil {
						if errors.Is(err, io.EOF) {
							// Directory fully consumed.
							break
						}

						ioErr := &IOError{Path: dirJob.path.String(), Op: "readdir", Err: err}
						if p.errNotify == nil || p.errNotify.ioErr(ioErr) {
							localErrs = append(localErrs, ioErr)
						}

						break
					}

					if empty {
						// Empty batch usually means everything was filtered; keep reading.
						continue
					}
				}

				// Drop the scanner's lease; chunks keep it alive if needed.
				lease.dec()
				sendScanEvent(scanEvent{done: true})
			}

			scanErrs[id] = localErrs
		}(scanID)
	}

	// ============================================================================
	// SHUTDOWN
	// ============================================================================
	//
	// Shutdown flow (dependency chain):
	//   coordWG ──> close(scanQueue) ──> scanWG
	//
	// The coordinator is the only sender on scanQueue, so we wait for it first.
	// Scanners exit after scanQueue closes; caller handles fileJobs cleanup.
	coordWG.Wait()
	scanWG.Wait()

	// Merge scan errors.
	totalErrs := 0
	for i := range p.scanWorkers {
		totalErrs += len(scanErrs[i])
	}

	allErrs := make([]error, 0, totalErrs)
	for i := range p.scanWorkers {
		allErrs = append(allErrs, scanErrs[i]...)
	}

	return allErrs
}

// Internal constants keep tuning knobs centralized.
const (
	// dirReadBufSize is the size of the directory-entry read buffer.
	// On Linux it backs getdents64/ReadDirent parsing. On other platforms the
	// backend ignores this buffer and uses os.File.ReadDir instead; the size
	// is only used as a sizing heuristic for arena storage.
	//
	// Benchmarked 32/64/128/256KB on flat and nested dirs (1k–1m files):
	// 32KB consistently fastest (~7-13% over 128KB), likely due to L1 cache.
	dirReadBufSize = 32 * 1024

	// maxWorkers caps scan/file worker counts to avoid excessive goroutine/memory overhead.
	maxWorkers = 256

	// defaultReadBufSize is the initial buffer size for file reads and
	// buffer growth heuristics.
	defaultReadBufSize = 512

	// defaultChunkSize is the default number of entries per work unit.
	//
	// Benchmarked 16–512 on flat dirs (5k–1m files): optimal chunk size
	// scales with file count (5k→64, 100k→128, 1m→512). 128 is a good
	// middle ground: ~1% off optimal for 100k, ~3% for 1m.
	defaultChunkSize = 128

	// defaultQueueFactor controls file job queue depth as a multiple of file workers.
	defaultQueueFactor = 4
)

// reportSubdirFunc emits a discovered subdirectory name.
// It is a no-op when recursion is disabled to avoid per-entry branching.
type reportSubdirFunc func(nulTermName)

// dirJob represents a directory queued for scanning.
type dirJob struct {
	// path stays NUL-terminated to avoid per-job string allocations.
	path nulTermPath
}

// scanEvent reports subdirectory discovery or completion to the coordinator.
type scanEvent struct {
	// path carries newly discovered subdirs without creating a second channel.
	path nulTermPath
	// done lets the coordinator track pending work accurately.
	done bool
}

// fileChunk is a slice of entries backed by a shared arena.
// It carries a lease so openat/statat remain valid while processing.
type fileChunk struct {
	// arena owns the backing storage so entries survive past readdir.
	arena *pathArena
	// start/end avoid copying slices per chunk.
	start int
	end   int
	// lease keeps openat-safe handles alive while chunks are processed.
	lease *dirLease
	// release returns arena/lease refs; set by consumer when enqueueing.
	release func()
}

// dirLease keeps a directory handle alive while file chunks are processed.
type dirLease struct {
	// dh stays open so openat/statat can use names without full paths.
	dh dirHandle
	// base is used for error messages and AbsPath.
	base nulTermPath
	// baseHash is the precomputed hash for base (+sep) for watcher scans.
	baseHash uint64
	// refs prevents premature close while chunks are in flight.
	refs atomic.Int32
}

// dec releases a lease reference and closes the handle when the last chunk completes.
func (l *dirLease) dec() {
	// Close only when last in-flight chunk releases the lease.
	if l.refs.Add(-1) == 0 {
		_ = l.dh.closeHandle()
	}
}

// ============================================================================
// PROCESSOR
// ============================================================================

// processor keeps per-call config so orchestration stays allocation-free.
type processor[T any] struct {
	// fn is the user callback; kept on the processor to avoid per-call captures.
	fn ProcessFunc[T]
	// rootLen is the root path length without trailing NUL.
	rootLen int
	// fileWorkers bounds callback concurrency to avoid oversubscribing I/O/CPU.
	fileWorkers int
	// scanWorkers allows discovery to run ahead without flooding the system.
	scanWorkers int
	// chunkSize controls parallelism granularity independent of readdir size.
	chunkSize int
	// suffix filters before enqueueing to avoid wasted work.
	suffix string
	// recursive skips subdir enqueue when false to keep discovery cheap.
	recursive bool
	// errNotify serializes error reporting for deterministic counts.
	errNotify *errNotifier
}

// fileWorkerBufs is per-worker to eliminate per-file allocations in hot paths.
type fileWorkerBufs struct {
	// fileBytesBuf reuses a single read buffer to avoid per-file allocs.
	fileBytesBuf []byte

	// pathScratch avoids per-callback path allocations when needed.
	pathScratch []byte

	// file is reused to avoid per-file heap allocations.
	file File

	// worker is reused to preserve scratch/retain arenas across files.
	// This is the public api for the callback, which is why we name it FileWorker.
	worker FileWorker
}

// process runs the full scan + process pipeline and merges worker-local results.
// Worker-local buffers avoid contention on hot paths; merging happens once.
func (p *processor[T]) process(ctx context.Context, root nulTermPath) ([]*T, []error) {
	if ctx.Err() != nil {
		return nil, nil
	}

	// Bound backlog so memory stays predictable while still feeding workers.
	queueDepth := max(p.fileWorkers*defaultQueueFactor, 16)
	fileJobs := make(chan fileChunk, queueDepth)
	results := make([][]*T, 0, p.fileWorkers)
	results = results[:p.fileWorkers]
	errs := make([][]error, 0, p.fileWorkers)
	errs = errs[:p.fileWorkers]

	// ============================================================================
	// FILE WORKER POOL: FILE PROCESSING
	// ============================================================================
	//
	// Workers consume chunks from fileJobs and run callbacks sequentially per chunk.
	// This keeps buffer reuse efficient while still scaling across workers.
	// Backpressure from fileJobs throttles scan workers upstream.
	var fileWG sync.WaitGroup
	fileWG.Add(p.fileWorkers)

	for workerID := range p.fileWorkers {
		go func(id int) {
			defer fileWG.Done()

			bufs := &fileWorkerBufs{}
			bufs.worker.id = id
			localResults := make([]*T, 0, 64)
			localErrs := make([]error, 0, 32)

			for chunk := range fileJobs {
				if ctx.Err() != nil {
					// Drain to release arenas/leases after cancellation.
					chunk.release()

					continue
				}

				p.processChunk(ctx, chunk.lease, chunk.arena.entries[chunk.start:chunk.end], bufs, &localResults, &localErrs)
				chunk.release()
			}

			// Drop all outstanding lease bookkeeping when this Process run ends.
			bufs.worker.leases.shutdown()

			results[id] = localResults
			errs[id] = localErrs
		}(workerID)
	}

	pipeline := &scanPipeline{
		scanWorkers: p.scanWorkers,
		chunkSize:   p.chunkSize,
		suffix:      p.suffix,
		recursive:   p.recursive,
		errNotify:   p.errNotify,
		queueDepth:  queueDepth + p.fileWorkers,
	}

	scanErrs := pipeline.run(ctx, root, func(chunk fileChunk) {
		select {
		case fileJobs <- chunk:
		case <-ctx.Done():
			chunk.release()
		}
	})

	close(fileJobs)
	fileWG.Wait()

	// Single merge avoids per-append locking on hot paths.
	totalResults := 0
	for i := range p.fileWorkers {
		totalResults += len(results[i])
	}

	merged := make([]*T, 0, totalResults)
	for i := range p.fileWorkers {
		merged = append(merged, results[i]...)
	}

	// Single merge avoids per-error locking on hot paths.
	totalErrs := len(scanErrs)
	for i := range p.fileWorkers {
		totalErrs += len(errs[i])
	}

	allErrs := make([]error, 0, totalErrs)

	allErrs = append(allErrs, scanErrs...)
	for i := range p.fileWorkers {
		allErrs = append(allErrs, errs[i]...)
	}

	return merged, allErrs
}

// processChunk executes callbacks for a chunk of entries and applies
// skip/error semantics consistently (ErrSkip vs internal races).
func (p *processor[T]) processChunk(
	ctx context.Context,
	dirLease *dirLease,
	entries []nulTermName,
	bufs *fileWorkerBufs,
	results *[]*T,
	errs *[]error,
) {
	var (
		// Reuse a single file handle per worker to avoid per-file allocations.
		openFH fileHandle
		fhOpen bool
	)

	defer func() {
		if fhOpen {
			_ = openFH.closeHandle()
		}
	}()

	closeIfOpen := func() {
		if !fhOpen {
			return
		}

		_ = openFH.closeHandle()
		fhOpen = false
	}

	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}

		// Reuse scratch to avoid per-file allocations.
		bufs.worker.buf = bufs.worker.buf[:0]

		bufs.file = File{
			dh:          dirLease.dh,
			name:        entry,
			base:        dirLease.base,
			rootLen:     p.rootLen,
			fh:          &openFH,
			fhOpen:      &fhOpen,
			dataBuf:     &bufs.fileBytesBuf,
			pathScratch: &bufs.pathScratch,
			pathBuilt:   false,
		}

		val, fnErr := p.fn(&bufs.file, &bufs.worker)
		if fnErr != nil {
			if errors.Is(fnErr, errSkipFile) {
				// Skip races (file became dir/symlink) without surfacing error.
				closeIfOpen()

				continue
			}

			if errors.Is(fnErr, ErrSkip) {
				// User explicitly requested skip.
				closeIfOpen()

				continue
			}

			procErr := &ProcessError{Path: dirLease.base.joinNameString(entry), Err: fnErr}
			if p.errNotify.callbackErr(procErr) {
				*errs = append(*errs, procErr)
			}

			closeIfOpen()

			continue
		}

		closeIfOpen()

		if val == nil {
			// nil result without ErrSkip is a programming error.
			procErr := &ProcessError{
				Path: dirLease.base.joinNameString(entry),
				Err:  errors.New("callback returned nil result without ErrSkip"),
			}
			if p.errNotify.callbackErr(procErr) {
				*errs = append(*errs, procErr)
			}

			continue
		}

		*results = append(*results, val)
	}
}
