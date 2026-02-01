package fileproc

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
	LastScanNs uint64
	// MaxScanNs is the longest scan duration observed in nanoseconds.
	MaxScanNs uint64
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
//
// # Performance
//
// Set [WithInterval] based on your latency requirements and file count. If a scan takes longer
// than the interval, the next scan starts immediately (no backlog).
type Watcher struct {
	root      nulTermPath
	cfg       options
	stats     atomicStats
	index     watchIndex
	errNotify *errNotifier
	gen       uint32
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

// poll starts the watcher and blocks until ctx is canceled.
func (w *Watcher) poll(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	interval := w.cfg.Interval
	if interval <= 0 {
		interval = defaultWatchInterval
	}

	for {
		if ctx.Err() != nil {
			break
		}

		w.gen++
		gen := w.gen

		start := time.Now()
		summary, _ := w.scanOnce(ctx, gen)
		duration := time.Since(start)

		w.updateStats(summary, duration, interval)

		if ctx.Err() != nil {
			break
		}

		wait := interval - duration
		if wait <= 0 {
			continue
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// Stats returns a snapshot of watcher activity counters.
//
// Safe to call concurrently, including while the watcher is running.
// All counters are cumulative since watcher creation.
func (w *Watcher) Stats() Stats {
	return w.stats.snapshot()
}

func (w *Watcher) updateStats(summary scanSummary, duration time.Duration, interval time.Duration) {
	ns := uint64(duration.Nanoseconds())

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
		if w.stats.maxScanNs.CompareAndSwap(cur, ns) {
			break
		}
	}

	if interval > 0 && duration > interval {
		w.stats.behindCount.Add(1)
	}
}

func applyWatchOptions(opts []Option) options {
	cfg := applyOptions(opts)
	if cfg.Interval <= 0 {
		cfg.Interval = defaultWatchInterval
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
	scans         atomic.Uint64
	filesSeen     atomic.Uint64
	creates       atomic.Uint64
	modifies      atomic.Uint64
	deletes       atomic.Uint64
	errors        atomic.Uint64
	lastScanNs    atomic.Uint64
	maxScanNs     atomic.Uint64
	lastScanFiles atomic.Uint64
	behindCount   atomic.Uint64
	eventCount    atomic.Uint64
}

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
