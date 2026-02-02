package fileproc

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	defaultWatchInterval = 250 * time.Millisecond
	defaultEventBuffer   = 16
)

// EventType identifies a watcher event kind.
type EventType uint8

const (
	// Create indicates a file appeared since the last scan.
	Create EventType = iota + 1
	// Modify indicates a file changed since the last scan.
	Modify
	// Delete indicates a file disappeared since the last scan.
	Delete
)

// Event describes a file change observed by the watcher.
//
// Events are emitted when a regular file is created, modified, or deleted
// between scans. The Path is always absolute. For Delete events, Stat
// contains the last known metadata before deletion.
type Event struct {
	// Type is the kind of change (Create/Modify/Delete).
	Type EventType
	// Path is the absolute path for the file.
	// It may reference internal watcher storage; retaining it can keep
	// watcher memory alive even after the watcher stops. Use [WithOwnedPaths]
	// to copy Event.Path per event.
	Path string
	// Stat is the file metadata captured at observation time.
	// For Delete events, this is the last known metadata.
	Stat Stat
}

// Stats reports watcher activity counters.
//
// All counters are cumulative since watcher creation. Use [Watcher.Stats]
// to retrieve a snapshot at any time, even while the watcher is running.
type Stats struct {
	// Scans is the total number of completed scans.
	Scans uint64
	// FilesSeen is the total number of regular files observed across scans.
	FilesSeen uint64
	// Creates is the total number of create events emitted.
	Creates uint64
	// Modifies is the total number of modify events emitted.
	Modifies uint64
	// Deletes is the total number of delete events emitted.
	Deletes uint64
	// Errors is the total number of IO/stat errors observed.
	Errors uint64
	// LastScanNs is the duration of the most recent scan in nanoseconds.
	LastScanNs int64
	// MaxScanNs is the longest scan duration observed in nanoseconds.
	MaxScanNs int64
	// LastScanFiles is the file count observed in the most recent scan.
	LastScanFiles uint64
	// BehindCount counts scans that took longer than the interval.
	BehindCount uint64
	// EventCount is the total number of events delivered to handlers.
	EventCount uint64
}

// Watcher provides high-performance polling-based file watching.
//
// Unlike inotify/kqueue/FSEvents, Watcher uses periodic directory scanning
// to detect changes. This approach works identically across all platforms
// and handles edge cases (NFS, Docker volumes, etc.) that event-based
// watchers struggle with.
//
// # Creating a Watcher
//
// Use [NewWatcher] to create a watcher, then call [Watcher.Watch] or
// [Watcher.Events] to start watching:
//
//	w, err := fileproc.NewWatcher(dir,
//	    fileproc.WithRecursive(),
//	    fileproc.WithSuffix(".log"),
//	    fileproc.WithInterval(100 * time.Millisecond),
//	)
//	if err != nil {
//	    return err
//	}
//
//	// Callback API
//	w.Watch(ctx, func(ev fileproc.Event) {
//	    log.Printf("%s: %s", ev.Type, ev.Path)
//	})
//
//	// Or channel API
//	for ev := range w.Events(ctx) {
//	    log.Printf("%s: %s", ev.Type, ev.Path)
//	}
//
// # Event Types
//
//   - [Create]: file appeared since last scan
//   - [Modify]: file size, mode, mtime, or inode changed
//   - [Delete]: file disappeared since last scan
//
// Renames are reported as Delete + Create (no rename tracking).
//
// # Baseline Behavior
//
// By default, the first scan silently indexes existing files without emitting
// events. Only subsequent scans emit Create/Modify/Delete events. Use
// [WithEmitBaseline] to emit Create events for all existing files on startup.
//
// # Symlinks
//
// Symbolic links are ignored entirely. They are not followed, not indexed,
// and do not generate events.
//
// # File Types
//
// Only regular files are watched. Directories, symlinks, FIFOs, sockets,
// and devices are skipped.
//
// # Errors
//
// IO errors during scanning (permission denied, etc.) are reported via
// [WithOnError] if configured. The watcher continues scanning other files.
//
// # Concurrency
//
// Event callbacks may be invoked concurrently from multiple goroutines.
// Callbacks must be safe for concurrent use. Events for different files
// may arrive out of order.
//
// # Backpressure
//
// If callbacks block or the event channel fills up, the watcher blocks
// until events can be delivered. Changes made while blocked are not lost;
// they are detected on the next scan after unblocking.
//
// # Memory
//
// The watcher maintains an in-memory index of all watched files. Memory
// usage scales linearly with file count. Use [WithExpectedFiles] to
// pre-allocate tables and reduce allocations during initial scan.
// Event.Path strings are views into that internal storage; holding on to
// them can retain watcher memory longer than expected. [WithOwnedPaths]
// copies Event.Path and enables compaction after heavy delete churn.
//
// # Performance
//
// Set [WithInterval] based on your latency requirements and file count. If a scan takes longer
// than the interval, the next scan starts immediately (no backlog).
type Watcher struct {
	// root is the NUL-terminated absolute root path for scanning.
	root nulTermPath
	// cfg holds normalized watcher options.
	cfg options
	// stats tracks cumulative counters (atomic snapshot).
	stats atomicStats
	// index is the persistent path index for create/modify/delete detection.
	index watchIndex
	// errNotify reports IO errors while scanning.
	errNotify *errNotifier
	// gen is the scan generation counter used for delete sweeps.
	gen uint32
}

// NewWatcher creates a new watcher for the given directory.
//
// The watcher does not start until [Watcher.Watch] or [Watcher.Events] is
// called. Configure behavior using options:
//
//   - [WithRecursive]: watch subdirectories
//   - [WithSuffix]: filter by file extension
//   - [WithInterval]: set polling frequency (default 250ms)
//   - [WithOnEvent]: set event callback (or pass to Watch)
//   - [WithOnError]: handle IO errors during scanning
//   - [WithEmitBaseline]: emit Create events on first scan
//   - [WithExpectedFiles]: pre-allocate for large directories
//
// Returns an error if path is invalid or contains a NUL byte.
func NewWatcher(path string, opts ...Option) (*Watcher, error) {
	if strings.IndexByte(path, 0) >= 0 {
		return nil, errors.New("path contains NUL byte")
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("file absolute path: %w", err)
	}

	cfg := applyWatchOptions(opts)
	if strings.IndexByte(cfg.Suffix, 0) >= 0 {
		return nil, errors.New("suffix contains NUL byte")
	}

	w := &Watcher{
		root:      newNulTermPath(abs),
		cfg:       cfg,
		index:     newWatchIndex(cfg.WatchShards, cfg.ExpectedFiles),
		errNotify: newErrNotifier(cfg.OnError),
	}

	return w, nil
}

// Watch starts the watcher and blocks until ctx is canceled.
//
// For each file change detected, fn is called with an [Event]. If fn is nil,
// the callback set via [WithOnEvent] is used instead.
//
// Callbacks may be invoked concurrently from multiple goroutines when using
// multiple file workers. Callbacks must be safe for concurrent use.
//
// IO errors during scanning are reported via [WithOnError] if configured.
// The watcher continues scanning other files after errors.
//
// Example:
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	w.Watch(ctx, func(ev fileproc.Event) {
//	    switch ev.Type {
//	    case fileproc.Create:
//	        log.Printf("created: %s (%d bytes)", ev.Path, ev.Stat.Size)
//	    case fileproc.Modify:
//	        log.Printf("modified: %s", ev.Path)
//	    case fileproc.Delete:
//	        log.Printf("deleted: %s", ev.Path)
//	    }
//	})
func (w *Watcher) Watch(ctx context.Context, fn func(Event)) {
	if fn != nil {
		w.cfg.OnEvent = fn
	}

	w.poll(ctx)
}

// Events starts the watcher and returns a channel of events.
//
// The channel receives an [Event] for each file change detected. The channel
// is closed when ctx is canceled and the watcher stops.
//
// Use [WithEventBuffer] to control channel buffer size (default 16). If the
// channel fills up, the watcher blocks until events are consumed.
//
// Example:
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	for ev := range w.Events(ctx) {
//	    switch ev.Type {
//	    case fileproc.Create:
//	        log.Printf("created: %s", ev.Path)
//	    case fileproc.Modify:
//	        log.Printf("modified: %s", ev.Path)
//	    case fileproc.Delete:
//	        log.Printf("deleted: %s", ev.Path)
//	    }
//	}
func (w *Watcher) Events(ctx context.Context) <-chan Event {
	buf := w.cfg.EventBuffer
	if buf <= 0 {
		buf = defaultEventBuffer
	}

	ch := make(chan Event, buf)

	var mu sync.Mutex

	w.cfg.OnEvent = func(ev Event) {
		// Serialize sends so only one goroutine can block on a full channel.
		// This keeps backpressure bounded to a single sender.
		mu.Lock()
		defer mu.Unlock()

		select {
		case ch <- ev:
		case <-ctx.Done():
		}
	}

	go func() {
		w.poll(ctx)
		close(ch)
	}()

	return ch
}

// Stats returns a snapshot of watcher activity counters.
//
// Safe to call concurrently, including while the watcher is running.
// All counters are cumulative since watcher creation.
func (w *Watcher) Stats() Stats {
	return w.stats.snapshot()
}

// poll starts the watcher loop and blocks until ctx is canceled.
// Uses a reusable timer to avoid per-iteration allocations.
func (w *Watcher) poll(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	interval := w.cfg.Interval
	if interval <= 0 {
		interval = defaultWatchInterval
	}

	minIdle := max(w.cfg.MinIdle, 0)

	// Fire immediately for the first scan, then reuse the timer for periodic waits.
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}

	for ctx.Err() == nil {
		w.gen++
		gen := w.gen

		start := time.Now()
		summary := w.scanOnce(ctx, gen)
		duration := time.Since(start)

		w.updateStats(summary, duration, interval)

		if ctx.Err() != nil {
			break
		}

		// If we're behind schedule, skip waiting unless MinIdle enforces a pause.
		wait := max(minIdle, max(interval-duration, 0))

		if wait == 0 {
			continue
		}

		timer.Reset(wait)

		select {
		case <-ctx.Done():
			timer.Stop()

			return
		case <-timer.C:
		}
	}
}

// updateStats merges a scan summary into cumulative counters.
func (w *Watcher) updateStats(summary scanSummary, duration time.Duration, interval time.Duration) {
	ns := duration.Nanoseconds()

	w.stats.scans.Add(1)
	w.stats.filesSeen.Add(summary.files)
	w.stats.creates.Add(summary.creates)
	w.stats.modifies.Add(summary.modifies)
	w.stats.deletes.Add(summary.deletes)
	w.stats.errors.Add(summary.errors)
	w.stats.eventCount.Add(summary.events)
	w.stats.lastScanNs.Store(ns)
	w.stats.lastScanFiles.Store(summary.files)

	for {
		cur := w.stats.maxScanNs.Load()
		if ns <= cur {
			break
		}
		// CAS keeps max updates lock-free under concurrent calls.
		if w.stats.maxScanNs.CompareAndSwap(cur, ns) {
			break
		}
	}

	if interval > 0 && duration > interval {
		w.stats.behindCount.Add(1)
	}
}

// applyWatchOptions normalizes watcher-specific defaults and limits.
func applyWatchOptions(opts []Option) options {
	cfg := applyOptions(opts)
	// Normalize defaults once to keep the hot path branch-light.
	if cfg.Interval <= 0 {
		cfg.Interval = defaultWatchInterval
	}

	if cfg.MinIdle < 0 {
		cfg.MinIdle = 0
	}

	if cfg.EventBuffer <= 0 {
		cfg.EventBuffer = defaultEventBuffer
	}

	if cfg.ExpectedFiles < 0 {
		cfg.ExpectedFiles = 0
	}

	if cfg.WatchShards <= 0 {
		cfg.WatchShards = max(min(cfg.Workers*2, 64), 1)
	}

	return cfg
}

type atomicStats struct {
	// scans is the total number of completed scans.
	scans atomic.Uint64
	// filesSeen is the total number of regular files observed.
	filesSeen atomic.Uint64
	// creates is the total number of create events emitted.
	creates atomic.Uint64
	// modifies is the total number of modify events emitted.
	modifies atomic.Uint64
	// deletes is the total number of delete events emitted.
	deletes atomic.Uint64
	// errors is the total number of scan errors observed.
	errors atomic.Uint64
	// lastScanNs is the duration of the most recent scan.
	lastScanNs atomic.Int64
	// maxScanNs is the longest scan duration observed.
	maxScanNs atomic.Int64
	// lastScanFiles is the file count of the most recent scan.
	lastScanFiles atomic.Uint64
	// behindCount counts scans that exceeded the interval.
	behindCount atomic.Uint64
	// eventCount is the total number of events emitted.
	eventCount atomic.Uint64
}

// snapshot returns a point-in-time view of all counters.
func (s *atomicStats) snapshot() Stats {
	return Stats{
		Scans:         s.scans.Load(),
		FilesSeen:     s.filesSeen.Load(),
		Creates:       s.creates.Load(),
		Modifies:      s.modifies.Load(),
		Deletes:       s.deletes.Load(),
		Errors:        s.errors.Load(),
		LastScanNs:    s.lastScanNs.Load(),
		MaxScanNs:     s.maxScanNs.Load(),
		LastScanFiles: s.lastScanFiles.Load(),
		BehindCount:   s.behindCount.Load(),
		EventCount:    s.eventCount.Load(),
	}
}

// ============================================================================
// WATCH SCAN
// ============================================================================

// scanSummary aggregates per-worker results for a single scan.
type scanSummary struct {
	// files is the number of regular files seen.
	files uint64
	// creates is the number of create events emitted.
	creates uint64
	// modifies is the number of modify events emitted.
	modifies uint64
	// deletes is the number of delete events emitted.
	deletes uint64
	// events is the total events emitted (create/modify/delete).
	events uint64
	// errors is the number of IO/stat errors observed.
	errors uint64
}

// scanOnce performs a single scan and returns a summary of activity.
func (w *Watcher) scanOnce(ctx context.Context, gen uint32) scanSummary {
	if ctx.Err() != nil {
		return scanSummary{}
	}

	cfg := w.cfg
	queueDepth := max(cfg.Workers*defaultQueueFactor, 16)
	fileJobs := make(chan fileChunk, queueDepth)
	workerSummaries := make([]scanSummary, 0, cfg.Workers)
	workerSummaries = workerSummaries[:cfg.Workers]
	errs := make([][]error, 0, cfg.Workers)
	errs = errs[:cfg.Workers]

	// Start file workers before pipeline runs.
	var fileWG sync.WaitGroup
	fileWG.Add(cfg.Workers)

	for workerID := range cfg.Workers {
		go func(id int) {
			defer fileWG.Done()

			localErrs := make([]error, 0, 32)
			localSummary := scanSummary{}

			for chunk := range fileJobs {
				if ctx.Err() != nil {
					chunk.release()

					continue
				}

				w.processChunk(ctx, gen, chunk.lease, chunk.arena.entries[chunk.start:chunk.end], &localSummary, &localErrs)
				chunk.release()
			}

			errs[id] = localErrs
			workerSummaries[id] = localSummary
		}(workerID)
	}

	pipeline := &scanPipeline{
		scanWorkers: cfg.ScanWorkers,
		chunkSize:   cfg.ChunkSize,
		suffix:      cfg.Suffix,
		recursive:   cfg.Recursive,
		errNotify:   w.errNotify,
		queueDepth:  queueDepth + cfg.Workers,
	}

	// errors are purely reported via errNotify; return is ignored intentionally
	_ = pipeline.run(ctx, w.root, func(chunk fileChunk) {
		select {
		case fileJobs <- chunk:
		case <-ctx.Done():
			chunk.release()
		}
	})

	close(fileJobs)
	fileWG.Wait()

	// Merge worker stats.
	var summary scanSummary
	for i := range cfg.Workers {
		summary.files += workerSummaries[i].files
		summary.creates += workerSummaries[i].creates
		summary.modifies += workerSummaries[i].modifies
		summary.events += workerSummaries[i].events
		summary.errors += workerSummaries[i].errors
	}

	// Sweep deletes after workers finish.
	deletes, deleteEvents := w.sweepDeletes(ctx, gen)
	summary.deletes += deletes
	summary.events += deleteEvents

	return summary
}

// processChunk stats entries, updates the index, and emits events.
func (w *Watcher) processChunk(
	ctx context.Context,
	gen uint32,
	lease *dirLease,
	entries []nulTermName,
	localSummary *scanSummary,
	localErrs *[]error,
) {
	if len(entries) == 0 {
		return
	}

	// pendingEntry caches per-entry data so we can group by shard and lock once.
	type pendingEntry struct {
		entry     nulTermName
		st        Stat
		hash      uint64
		shard     int
		updateGen bool
	}

	// pendingEvent buffers events so we can emit after unlocking the shard.
	type pendingEvent struct {
		typ  EventType
		path string
		st   Stat
	}

	pending := make([]pendingEntry, 0, len(entries))
	shardCount := len(w.index.shards)
	shardCounts := make([]int, shardCount)
	mask := w.index.mask

	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}

		st, kind, err := lease.dh.statFile(entry)
		if err != nil {
			ioErr := &IOError{Path: lease.base.joinNameString(entry), Op: "stat", Err: err}
			localSummary.errors++

			if w.errNotify == nil || w.errNotify.ioErr(ioErr) {
				*localErrs = append(*localErrs, ioErr)
			}

			if !errors.Is(err, syscall.ENOENT) {
				// For transient stat failures, keep the entry alive to avoid
				// false deletes on this scan generation.
				hash := hashPathFromBase(lease.baseHash, entry)
				shardIdx := int(hash) & int(mask)
				pending = append(pending, pendingEntry{
					entry:     entry,
					hash:      hash,
					shard:     shardIdx,
					updateGen: true,
				})
				shardCounts[shardIdx]++
			}

			continue
		}

		if kind != statKindReg {
			continue
		}

		localSummary.files++

		hash := hashPathFromBase(lease.baseHash, entry)
		shardIdx := int(hash) & int(mask)

		pending = append(pending, pendingEntry{
			entry: entry,
			st:    st,
			hash:  hash,
			shard: shardIdx,
		})
		shardCounts[shardIdx]++
	}

	if len(pending) == 0 {
		return
	}

	// Group entries by shard using a counting-sort layout so we lock each shard only once,
	// per chunk, instead of once per entry.
	order := make([]int, len(pending))
	offsets := make([]int, shardCount)

	sum := 0
	for i, count := range shardCounts {
		offsets[i] = sum
		sum += count
	}

	next := make([]int, shardCount)
	copy(next, offsets)

	for i, pe := range pending {
		pos := next[pe.shard]
		order[pos] = i
		next[pe.shard]++
	}

	events := make([]pendingEvent, 0, len(pending))

	for shardIdx, count := range shardCounts {
		if count == 0 {
			continue
		}

		shard := &w.index.shards[shardIdx]
		start := offsets[shardIdx]

		shard.mu.Lock()
		shard.table.ensure(count)

		for i := range count {
			pe := pending[order[start+i]]

			if pe.updateGen {
				_, idx, found := shard.table.findSlot(pe.hash, lease.base, pe.entry, &shard.store)
				if found {
					shard.table.entries[idx].gen = gen
				}

				continue
			}

			var (
				emit      bool
				created   bool
				modified  bool
				eventType EventType
				eventPath string
				eventStat Stat
			)

			slot, idx, found := shard.table.findSlot(pe.hash, lease.base, pe.entry, &shard.store)
			if !found {
				off, length := shard.store.appendPath(lease.base, pe.entry)
				shard.table.insert(pe.hash, off, length, pe.st, gen, slot)

				created = true
				eventType = Create
				eventPath = w.eventPath(shard, off, length)
				eventStat = pe.st
				shard.liveBytes += uint64(length)
				// Suppress Create events on baseline scan unless EmitBaseline is set.
				emit = w.cfg.OnEvent != nil && (gen > 1 || w.cfg.EmitBaseline)
			} else {
				rec := &shard.table.entries[idx]
				if !statEqual(rec.st, pe.st) {
					rec.st = pe.st
					modified = true
					eventType = Modify
					eventPath = w.eventPath(shard, rec.pathOff, rec.pathLen)
					eventStat = pe.st
					emit = w.cfg.OnEvent != nil
				}

				rec.gen = gen
			}

			if created {
				localSummary.creates++
			}

			if modified {
				localSummary.modifies++
			}

			if emit {
				events = append(events, pendingEvent{typ: eventType, path: eventPath, st: eventStat})
			}
		}

		shard.mu.Unlock()

		// Emit after unlock to keep the critical section small.
		if w.cfg.OnEvent != nil && len(events) > 0 {
			for _, ev := range events {
				w.cfg.OnEvent(Event{Type: ev.typ, Path: ev.path, Stat: ev.st})

				localSummary.events++
			}

			events = events[:0]
		}
	}
}

// sweepDeletes removes entries not seen in this scan generation and emits deletes.
func (w *Watcher) sweepDeletes(ctx context.Context, gen uint32) (uint64, uint64) {
	var (
		deletes uint64
		events  uint64
	)

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

			// Entry not observed in this generation => delete.
			if entry.pathLen != 0 {
				length := uint64(entry.pathLen)
				if shard.liveBytes >= length {
					shard.liveBytes -= length
				} else {
					shard.liveBytes = 0
				}

				shard.deadBytes += length
			}

			shard.table.remove(entry)

			deletes++

			if w.cfg.OnEvent != nil {
				path := w.eventPath(shard, entry.pathOff, entry.pathLen)
				w.cfg.OnEvent(Event{Type: Delete, Path: path, Stat: entry.st})

				events++
			}
		}

		if w.cfg.OwnedPaths && shard.shouldCompact() {
			shard.compactLocked()
		} else if shard.table.shouldRehash() {
			// Rehash after deletions to restore probe locality.
			shard.table.rehash(len(shard.table.slots))
		}

		shard.mu.Unlock()
	}

	return deletes, events
}

// statEqual reports whether file metadata changes should trigger Modify.
func statEqual(a, b Stat) bool {
	return a.Size == b.Size && a.ModTime == b.ModTime && a.Mode == b.Mode && a.Inode == b.Inode
}

func (w *Watcher) eventPath(shard *watchShard, off, length uint32) string {
	if !w.cfg.OwnedPaths {
		return shard.store.pathString(off, length)
	}

	// Owned paths allow compaction and avoid pinning pathStore buffers.
	b := shard.store.bytes(off, length)
	if len(b) == 0 {
		return ""
	}

	return string(b)
}
