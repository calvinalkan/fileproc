package fileproc

import (
	"context"
	"errors"
	"io"
	"sync"
	"syscall"
)

type workerCounts struct {
	files    uint64
	creates  uint64
	modifies uint64
	events   uint64
	errors   uint64
}

type scanSummary struct {
	files    uint64
	creates  uint64
	modifies uint64
	deletes  uint64
	events   uint64
	errors   uint64
}

func (w *Watcher) scanOnce(ctx context.Context, gen uint32) (scanSummary, []error) {
	if ctx.Err() != nil {
		return scanSummary{}, nil
	}

	cfg := w.cfg

	queueDepth := max(cfg.Workers*defaultQueueFactor, 16)
	scanQueue := make(chan dirJob)
	scanEvents := make(chan scanEvent, cfg.ScanWorkers*64)
	fileJobs := make(chan fileChunk, queueDepth)

	arenaCount := queueDepth + cfg.Workers + cfg.ScanWorkers + 1
	pool := newArenaPool(arenaCount)

	workerErrs := make([][]error, 0, cfg.Workers)
	workerStats := make([]workerCounts, 0, cfg.Workers)
	for range cfg.Workers {
		workerErrs = append(workerErrs, nil)
		workerStats = append(workerStats, workerCounts{})
	}

	scanErrs := make([][]error, 0, cfg.ScanWorkers)
	scanErrCounts := make([]uint64, 0, cfg.ScanWorkers)
	for range cfg.ScanWorkers {
		scanErrs = append(scanErrs, nil)
		scanErrCounts = append(scanErrCounts, 0)
	}

	releaseChunk := func(chunk fileChunk) {
		if chunk.lease != nil {
			chunk.lease.dec()
		}

		if chunk.arena == nil {
			return
		}

		if chunk.arena.pending.Add(-1) == 0 {
			pool.put(chunk.arena)
		}
	}

	sendChunk := func(chunk fileChunk) bool {
		select {
		case fileJobs <- chunk:
			return true
		case <-ctx.Done():
			releaseChunk(chunk)
			return false
		}
	}

	sendScanEvent := func(ev scanEvent) {
		select {
		case scanEvents <- ev:
		case <-ctx.Done():
		}
	}

	var coordWG sync.WaitGroup
	coordWG.Go(func() {
		scanQueueClosed := false
		queue := make([]dirJob, 0, 1024)
		queue = append(queue, dirJob{path: w.root})
		pending := len(queue)

		drainEvents := func(pending int, queue []dirJob) (int, []dirJob) {
			for {
				select {
				case ev := <-scanEvents:
					if ev.done {
						continue
					}
					if cfg.Recursive {
						pending++
						queue = append(queue, dirJob{path: ev.path})
					}
				default:
					return pending, queue
				}
			}
		}

		for pending > 0 {
			if ctx.Err() != nil {
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
				} else if cfg.Recursive {
					pending++
					queue = append(queue, dirJob{path: ev.path})
				}

				if pending == 0 {
					pending, queue = drainEvents(pending, queue)
				}

				if pending == 0 && !scanQueueClosed {
					close(scanQueue)
					scanQueueClosed = true
				}

			case <-ctx.Done():
				if !scanQueueClosed {
					close(scanQueue)
				}
				return

			case jobCh <- next:
				queue[0] = dirJob{}
				queue = queue[1:]
			}
		}

		if !scanQueueClosed {
			close(scanQueue)
		}
	})

	var scanWG sync.WaitGroup
	scanWG.Add(cfg.ScanWorkers)
	for scanID := range cfg.ScanWorkers {
		go func(id int) {
			defer scanWG.Done()

			dirBuf := make([]byte, 0, dirReadBufSize)
			storageCap := cap(dirBuf) * 2
			localErrs := make([]error, 0, 8)
			var localErrCount uint64

			for dirJob := range scanQueue {
				if ctx.Err() != nil {
					sendScanEvent(scanEvent{done: true})
					break
				}

				dh, err := openDir(dirJob.path)
				if err != nil {
					ioErr := &IOError{Path: dirJob.path.String(), Op: "open", Err: err}
					localErrCount++
					if w.errNotify == nil || w.errNotify.ioErr(ioErr) {
						localErrs = append(localErrs, ioErr)
					}
					sendScanEvent(scanEvent{done: true})
					continue
				}

				dirLease := &dirLease{dh: dh, base: dirJob.path}
				dirLease.refs.Store(1)

				reportSubdir := reportSubdirFunc(nil)
				if cfg.Recursive {
					reportSubdir = func(name nulTermName) {
						child := dirJob.path.joinName(name)
						sendScanEvent(scanEvent{path: child})
					}
				}

				for ctx.Err() == nil {
					arena := pool.get(ctx, storageCap)
					if arena == nil {
						break
					}

					scan := dirScan{
						path:         dirJob.path,
						handle:       dh,
						dirEntryBuf:  dirBuf[:cap(dirBuf)],
						reportSubdir: reportSubdir,
					}
					buf := scan.dirEntryBuf
					if cap(buf) != len(buf) {
						buf = buf[:cap(buf)]
					}

					err := readDirBatch(scan.handle, scan.path, buf, cfg.Suffix, arena, scan.reportSubdir)

					empty := len(arena.entries) == 0
					if empty {
						pool.put(arena)
					} else {
						chunkCount := int32((len(arena.entries) + cfg.ChunkSize - 1) / cfg.ChunkSize)
						arena.pending.Store(chunkCount + 1)
						dirLease.refs.Add(chunkCount)

						sentAll := true
						for start := 0; start < len(arena.entries); start += cfg.ChunkSize {
							end := min(start+cfg.ChunkSize, len(arena.entries))
							chunk := fileChunk{arena: arena, start: start, end: end, lease: dirLease}
							if !sendChunk(chunk) {
								sentAll = false
								for rest := start + cfg.ChunkSize; rest < len(arena.entries); rest += cfg.ChunkSize {
									releaseChunk(fileChunk{arena: arena, start: rest, end: min(rest+cfg.ChunkSize, len(arena.entries)), lease: dirLease})
								}
								break
							}
						}

						if arena.pending.Add(-1) == 0 {
							pool.put(arena)
						}

						if !sentAll {
							break
						}
					}

					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}

						ioErr := &IOError{Path: dirJob.path.String(), Op: "readdir", Err: err}
						localErrCount++
						if w.errNotify == nil || w.errNotify.ioErr(ioErr) {
							localErrs = append(localErrs, ioErr)
						}

						break
					}

					if empty {
						continue
					}
				}

				dirLease.dec()
				sendScanEvent(scanEvent{done: true})
			}

			scanErrs[id] = localErrs
			scanErrCounts[id] = localErrCount
		}(scanID)
	}

	var fileWG sync.WaitGroup
	fileWG.Add(cfg.Workers)

	for workerID := range cfg.Workers {
		go func(id int) {
			defer fileWG.Done()

			localErrs := make([]error, 0, 32)
			localCounts := workerCounts{}

			for chunk := range fileJobs {
				if ctx.Err() != nil {
					releaseChunk(chunk)
					continue
				}

				w.watchChunk(ctx, gen, chunk.lease, chunk.arena.entries[chunk.start:chunk.end], &localCounts, &localErrs)
				releaseChunk(chunk)
			}

			workerErrs[id] = localErrs
			workerStats[id] = localCounts
		}(workerID)
	}

	coordWG.Wait()
	scanWG.Wait()
	close(fileJobs)
	fileWG.Wait()

	// Merge errors.
	totalErrs := 0
	for i := range cfg.Workers {
		totalErrs += len(workerErrs[i])
	}
	for i := range cfg.ScanWorkers {
		totalErrs += len(scanErrs[i])
	}

	allErrs := make([]error, 0, totalErrs)
	for i := range cfg.Workers {
		allErrs = append(allErrs, workerErrs[i]...)
	}
	for i := range cfg.ScanWorkers {
		allErrs = append(allErrs, scanErrs[i]...)
	}

	// Merge counts.
	var summary scanSummary
	for i := range cfg.Workers {
		summary.files += workerStats[i].files
		summary.creates += workerStats[i].creates
		summary.modifies += workerStats[i].modifies
		summary.events += workerStats[i].events
		summary.errors += workerStats[i].errors
	}
	for i := range cfg.ScanWorkers {
		summary.errors += scanErrCounts[i]
	}

	// Sweep deletes after workers finish.
	deletes, deleteEvents := w.sweepDeletes(ctx, gen)
	summary.deletes += deletes
	summary.events += deleteEvents

	return summary, allErrs
}

func (w *Watcher) watchChunk(
	ctx context.Context,
	gen uint32,
	dirLease *dirLease,
	entries []nulTermName,
	counts *workerCounts,
	errs *[]error,
) {
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}

		st, kind, err := dirLease.dh.statFile(entry)
		if err != nil {
			ioErr := &IOError{Path: dirLease.base.joinNameString(entry), Op: "stat", Err: err}
			counts.errors++
			if w.errNotify == nil || w.errNotify.ioErr(ioErr) {
				*errs = append(*errs, ioErr)
			}

			if !errors.Is(err, syscall.ENOENT) {
				hash := hashPath(dirLease.base, entry)
				shard := w.index.shard(hash)
				shard.mu.Lock()
				_, idx, found := shard.table.findSlot(hash, dirLease.base, entry, &shard.store)
				if found {
					shard.table.entries[idx].gen = gen
				}
				shard.mu.Unlock()
			}

			continue
		}

		if kind != statKindReg {
			continue
		}

		counts.files++

		hash := hashPath(dirLease.base, entry)
		shard := w.index.shard(hash)

		var (
			emit      bool
			created   bool
			modified  bool
			eventType EventType
			eventPath string
			eventStat Stat
		)

		shard.mu.Lock()
		shard.table.ensure(1)
		slot, idx, found := shard.table.findSlot(hash, dirLease.base, entry, &shard.store)
		if !found {
			off, length := shard.store.appendPath(dirLease.base, entry)
			shard.table.insert(hash, off, length, st, gen, slot)
			created = true
			eventType = Create
			eventPath = shard.store.pathString(off, length)
			eventStat = st
			// Suppress Create events on baseline scan unless EmitBaseline is set.
			emit = w.cfg.OnEvent != nil && (gen > 1 || w.cfg.EmitBaseline)
		} else {
			rec := &shard.table.entries[idx]
			if !statEqual(rec.st, st) {
				rec.st = st
				modified = true
				eventType = Modify
				eventPath = shard.store.pathString(rec.pathOff, rec.pathLen)
				eventStat = st
				emit = w.cfg.OnEvent != nil
			}
			rec.gen = gen
		}
		shard.mu.Unlock()

		if created {
			counts.creates++
		}
		if modified {
			counts.modifies++
		}

		if emit {
			w.cfg.OnEvent(Event{Type: eventType, Path: eventPath, Stat: eventStat})
			counts.events++
		}
	}
}

func (w *Watcher) sweepDeletes(ctx context.Context, gen uint32) (uint64, uint64) {
	var deletes uint64
	var events uint64

	if ctx.Err() != nil {
		return 0, 0
	}

	for i := range w.index.shards {
		shard := &w.index.shards[i]
		shard.mu.Lock()

		for idx := range shard.table.entries {
			if ctx.Err() != nil {
				break
			}

			entry := &shard.table.entries[idx]
			if !entry.alive {
				continue
			}

			if entry.gen == gen {
				continue
			}

			shard.table.remove(entry)
			deletes++

			if w.cfg.OnEvent != nil {
				path := shard.store.pathString(entry.pathOff, entry.pathLen)
				w.cfg.OnEvent(Event{Type: Delete, Path: path, Stat: entry.st})
				events++
			}
		}

		if shard.table.shouldRehash() {
			shard.table.rehash(len(shard.table.slots))
		}

		shard.mu.Unlock()
	}

	return deletes, events
}

func statEqual(a, b Stat) bool {
	return a.Size == b.Size && a.ModTime == b.ModTime && a.Mode == b.Mode && a.Inode == b.Inode
}
