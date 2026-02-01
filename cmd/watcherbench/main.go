// Watcherbench benchmarks the fileproc watcher.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/calvinalkan/fileproc"
)

const (
	modeCallback = "callback"
	modeChannel  = "channel"
	benchScan    = "scan"
	benchChanges = "changes"
	changeCreate = "create"
	changeModify = "modify"
	changeDelete = "delete"
	changeMixed  = "mixed"
)

type benchFlags struct {
	dir             string
	tree            bool
	suffix          string
	interval        time.Duration
	minIdle         time.Duration
	warmup          int
	scans           int
	bench           string
	mode            string
	eventBuffer     int
	emitBaseline    bool
	changeMode      string
	changeCount     int
	changeRounds    int
	changeSize      int
	changeTimeout   time.Duration
	changePrefix    string
	changeCleanup   bool
	changeMixCreate int
	changeMixModify int
	changeMixDelete int
	workers         int
	scanWorkers     int
	chunkSize       int
	expectedFiles   int
	gcPercent       int
	quiet           bool
	caseName        string
	notes           string
	out             string
	cpuProfile      string
	memProfile      string
	memProfileGC    bool
	memStatsOut     string
	memStatsEvery   time.Duration
	progressEvery   time.Duration
}

type benchResult struct {
	Timestamp time.Time `json:"ts"`

	Case  string `json:"case,omitempty"`
	Notes string `json:"notes,omitempty"`

	Dir              string        `json:"dir"`
	Tree             bool          `json:"tree"`
	Suffix           string        `json:"suffix"`
	Interval         time.Duration `json:"interval"`
	MinIdle          time.Duration `json:"min_idle"`
	Bench            string        `json:"bench"`
	Mode             string        `json:"mode"`
	EventBuffer      int           `json:"event_buffer"`
	EmitBaseline     bool          `json:"emit_baseline"`
	ChangeMode       string        `json:"change_mode,omitempty"`
	ChangeCount      int           `json:"change_count,omitempty"`
	ChangeRounds     int           `json:"change_rounds,omitempty"`
	ChangeSize       int           `json:"change_size,omitempty"`
	ChangeTimeout    time.Duration `json:"change_timeout,omitempty"`
	ChangeMixCreate  int           `json:"change_mix_create,omitempty"`
	ChangeMixModify  int           `json:"change_mix_modify,omitempty"`
	ChangeMixDelete  int           `json:"change_mix_delete,omitempty"`
	ChangeExpected   int           `json:"change_expected,omitempty"`
	ChangeSeen       int           `json:"change_seen,omitempty"`
	ChangeMissed     int           `json:"change_missed,omitempty"`
	ChangeMismatched int           `json:"change_mismatched,omitempty"`
	ChangeDuplicates int           `json:"change_duplicates,omitempty"`
	ChangeLatMeanNs  uint64        `json:"change_lat_mean_ns,omitempty"`
	ChangeLatP50Ns   uint64        `json:"change_lat_p50_ns,omitempty"`
	ChangeLatP95Ns   uint64        `json:"change_lat_p95_ns,omitempty"`
	ChangeLatP99Ns   uint64        `json:"change_lat_p99_ns,omitempty"`
	ChangeLatMaxNs   uint64        `json:"change_lat_max_ns,omitempty"`
	ChangeLatSamples int           `json:"change_lat_samples,omitempty"`

	Workers       int `json:"workers"`
	ScanWorkers   int `json:"scan_workers"`
	ChunkSize     int `json:"chunk_size"`
	ExpectedFiles int `json:"expected_files"`
	WarmupScans   int `json:"warmup_scans"`
	Scans         int `json:"scans"`
	GCPercent     int `json:"gc"`

	Duration    time.Duration `json:"duration"`
	ScanMeanNs  uint64        `json:"scan_mean_ns,omitempty"`
	ScanP50Ns   uint64        `json:"scan_p50_ns,omitempty"`
	ScanP95Ns   uint64        `json:"scan_p95_ns,omitempty"`
	ScanP99Ns   uint64        `json:"scan_p99_ns,omitempty"`
	ScanMaxNs   uint64        `json:"scan_max_ns,omitempty"`
	ScanSamples int           `json:"scan_samples"`

	FilesSeen uint64 `json:"files_seen"`
	Creates   uint64 `json:"creates"`
	Modifies  uint64 `json:"modifies"`
	Deletes   uint64 `json:"deletes"`
	Events    uint64 `json:"events"`
	Errors    uint64 `json:"errors"`
	Behind    uint64 `json:"behind"`

	ScansPerSec float64 `json:"scans_per_sec"`
	FilesPerSec float64 `json:"files_per_sec"`

	MemSamples        int    `json:"mem_samples,omitempty"`
	MemPeakHeapAlloc  uint64 `json:"mem_peak_heap_alloc,omitempty"`
	MemPeakHeapInuse  uint64 `json:"mem_peak_heap_inuse,omitempty"`
	MemPeakHeapSys    uint64 `json:"mem_peak_heap_sys,omitempty"`
	MemFinalHeapAlloc uint64 `json:"mem_final_heap_alloc,omitempty"`
	MemFinalHeapInuse uint64 `json:"mem_final_heap_inuse,omitempty"`
	MemFinalHeapSys   uint64 `json:"mem_final_heap_sys,omitempty"`

	GoVersion   string `json:"go"`
	GOOS        string `json:"goos"`
	GOARCH      string `json:"goarch"`
	GOMAXPROCS  int    `json:"gomaxprocs"`
	NumCPU      int    `json:"numcpu"`
	VCSRevision string `json:"vcs_revision,omitempty"`
	VCSTime     string `json:"vcs_time,omitempty"`
	VCSModified bool   `json:"vcs_modified,omitempty"`
}

func parseFlags() *benchFlags {
	flags := &benchFlags{}

	flag.Usage = func() {
		out := flag.CommandLine.Output()
		fmt.Fprint(out, `watcherbench - benchmark fileproc watcher

USAGE
  watcherbench -dir <path> [options]

BENCH MODES
  -bench=scan
    Measure idle scan throughput only (no file changes).
    Metrics: scan mean/p50/p95/p99, files/sec, behind count.
    Uses -scans and -warmup.

  -bench=changes
    Apply change bursts and measure detection latency + misses.
    Metrics: expected/seen/missed, latency p50/p95/p99, duplicates, mismatches.
    Uses -change-* flags. -scans is ignored for timing.

EVENT MODES
  -mode=callback (default)
    Direct OnEvent callback; lowest overhead.
  -mode=channel
    Uses Events() channel. Combine with -event-buffer to test backpressure.

CHANGE MODES (for -bench=changes)
  -change-mode=create
    Create N new files per round.
  -change-mode=modify
    Modify N existing files per round (appends 1 byte).
    Files are pre-created once before rounds.
  -change-mode=delete
    Delete N existing files per round.
    Files are re-created before each round to keep count stable.
  -change-mode=mixed
    Create/modify/delete mix per round. Ratios from -change-mix-*.

MIX FLAGS (for -change-mode=mixed)
  -change-mix-create / -change-mix-modify / -change-mix-delete
    Relative weights (not required to sum to 100). Sum must be > 0.
    Example: 50/30/20 => 50% create, 30% modify, 20% delete.

MEMORY / PROFILES
  -cpuprofile <file>       write CPU profile
  -memprofile <file>       write heap profile (snapshot after measurement)
  -memprofile-gc           run GC before heap profile
  -memstats-interval <d>   periodic runtime.MemStats sampling
  -memstats-out <file>     JSONL output for memstats samples

PROGRESS
  -progress <d>            progress log interval on stderr (0=disabled)

EXAMPLES
  # Idle scan throughput, recursive, all files
  watcherbench -dir . -bench=scan -tree -suffix "" -scans 10 -warmup 2

  # Changes: create 5k files per round, 3 rounds
  watcherbench -dir /tmp/wb -bench=changes -change-mode=create -change-count 5000 -change-rounds 3

  # Changes: modify 1k existing files, 5 rounds
  watcherbench -dir /tmp/wb -bench=changes -change-mode=modify -change-count 1000 -change-rounds 5

  # Changes: delete 2k files per round, cleanup disabled
  watcherbench -dir /tmp/wb -bench=changes -change-mode=delete -change-count 2000 -change-cleanup=false

  # Changes: mixed with custom ratios (create/modify/delete)
  watcherbench -dir /tmp/wb -bench=changes -change-mode=mixed -change-count 3000 \
    -change-mix-create 60 -change-mix-modify 20 -change-mix-delete 20

  # Channel mode with backpressure (buffer=0)
  watcherbench -dir /tmp/wb -bench=scan -mode=channel -event-buffer 0

  # Profiles + memstats samples
  watcherbench -dir . -bench=scan -cpuprofile cpu.pprof -memprofile heap.pprof \
    -memstats-interval 200ms -memstats-out memstats.jsonl

NOTES
  -change-timeout defaults to max(2*interval, 500ms) if unset.
  -change-prefix controls the file name prefix used for change files.
  -change-cleanup removes created files after the run (default true).
  -min-idle enforces a minimum sleep after each scan (0 disables).
  -suffix="" matches all files.
  -bench=changes will create -dir if it does not exist.
  stdout default = table; use -out for programmatic JSONL.

OPTIONS (all flags)
`)
		flag.CommandLine.SetOutput(out)
		flag.CommandLine.PrintDefaults()
		fmt.Fprintln(out)
	}

	flag.StringVar(&flags.dir, "dir", "", "directory to watch")
	flag.BoolVar(&flags.tree, "tree", false, "watch recursively")
	flag.StringVar(&flags.suffix, "suffix", ".md", "file suffix filter (empty = all files)")
	flag.DurationVar(&flags.interval, "interval", 250*time.Millisecond, "poll interval")
	flag.DurationVar(&flags.minIdle, "min-idle", 0, "minimum idle time after each scan (0=disabled)")
	flag.IntVar(&flags.warmup, "warmup", 2, "warmup scans to ignore")
	flag.IntVar(&flags.scans, "scans", 5, "number of measured scans")
	flag.StringVar(&flags.bench, "bench", "scan", "benchmark method: scan | changes")
	flag.StringVar(&flags.mode, "mode", modeCallback, "event mode: callback | channel")
	flag.IntVar(&flags.eventBuffer, "event-buffer", 0, "event channel buffer size (channel mode)")
	flag.BoolVar(&flags.emitBaseline, "emit-baseline", false, "emit Create events on initial scan")
	flag.StringVar(&flags.changeMode, "change-mode", changeCreate, "change mode (changes bench): create | modify | delete | mixed")
	flag.IntVar(&flags.changeCount, "change-count", 1000, "change count per round (changes bench)")
	flag.IntVar(&flags.changeRounds, "change-rounds", 3, "number of change rounds (changes bench)")
	flag.IntVar(&flags.changeSize, "change-size", 16, "bytes per created file (changes bench)")
	flag.DurationVar(&flags.changeTimeout, "change-timeout", 0, "max wait per change round (0=auto)")
	flag.StringVar(&flags.changePrefix, "change-prefix", "wb_", "filename prefix for changes bench")
	flag.BoolVar(&flags.changeCleanup, "change-cleanup", true, "remove created files after bench")
	flag.IntVar(&flags.changeMixCreate, "change-mix-create", 50, "mixed mode create share (percent-ish)")
	flag.IntVar(&flags.changeMixModify, "change-mix-modify", 30, "mixed mode modify share (percent-ish)")
	flag.IntVar(&flags.changeMixDelete, "change-mix-delete", 20, "mixed mode delete share (percent-ish)")
	flag.IntVar(&flags.workers, "workers", 0, "worker count (0=auto)")
	flag.IntVar(&flags.scanWorkers, "scan-workers", 0, "scan worker count (0=auto)")
	flag.IntVar(&flags.chunkSize, "chunk-size", 0, "entries per chunk (0=auto)")
	flag.IntVar(&flags.expectedFiles, "expected-files", 0, "expected file count for pre-sizing")
	flag.IntVar(&flags.gcPercent, "gc", -1, "if >=0, call debug.SetGCPercent(gc)")
	flag.BoolVar(&flags.quiet, "q", false, "quiet: print only files/sec")
	flag.StringVar(&flags.caseName, "case", "", "optional short case name to store in JSON output")
	flag.StringVar(&flags.notes, "notes", "", "optional freeform notes to store in JSON output")
	flag.StringVar(&flags.out, "out", "", "optional JSONL output file to append one result per run")
	flag.StringVar(&flags.cpuProfile, "cpuprofile", "", "write CPU profile to file")
	flag.StringVar(&flags.memProfile, "memprofile", "", "write heap profile after measurement")
	flag.BoolVar(&flags.memProfileGC, "memprofile-gc", false, "run GC before writing memprofile")
	flag.StringVar(&flags.memStatsOut, "memstats-out", "", "optional JSONL output for periodic memstats")
	flag.DurationVar(&flags.memStatsEvery, "memstats-interval", 0, "memstats sample interval (0=disabled)")
	flag.DurationVar(&flags.progressEvery, "progress", time.Second, "progress log interval to stderr (0=disabled)")

	return flags
}

func main() {
	flags := parseFlags()

	flag.Parse()
	os.Exit(run(flags))
}

func run(flags *benchFlags) int {
	if flags.dir == "" {
		fmt.Fprintln(os.Stderr, "-dir is required")

		return 2
	}

	if flags.warmup < 0 {
		fmt.Fprintln(os.Stderr, "-warmup must be >= 0")

		return 2
	}

	bench, err := parseBench(flags.bench)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)

		return 2
	}

	err = ensureBenchDir(flags.dir, bench)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)

		return 2
	}

	if bench == benchScan && flags.scans <= 0 {
		fmt.Fprintln(os.Stderr, "-scans must be >= 1")

		return 2
	}

	mode, err := parseMode(flags.mode)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)

		return 2
	}

	if flags.eventBuffer < 0 {
		fmt.Fprintln(os.Stderr, "-event-buffer must be >= 0")

		return 2
	}

	if flags.memStatsEvery < 0 {
		fmt.Fprintln(os.Stderr, "-memstats-interval must be >= 0")

		return 2
	}

	if flags.minIdle < 0 {
		fmt.Fprintln(os.Stderr, "-min-idle must be >= 0")

		return 2
	}

	if flags.progressEvery < 0 {
		fmt.Fprintln(os.Stderr, "-progress must be >= 0")

		return 2
	}

	if flags.memStatsEvery > 0 && flags.memStatsOut == "" {
		fmt.Fprintln(os.Stderr, "-memstats-out is required when -memstats-interval > 0")

		return 2
	}

	if flags.memStatsEvery == 0 && flags.memStatsOut != "" {
		fmt.Fprintln(os.Stderr, "-memstats-interval is required when -memstats-out is set")

		return 2
	}

	changeMode := ""
	mixCreate := 0
	mixModify := 0
	mixDelete := 0

	if bench == benchChanges {
		parsedMode, modeErr := parseChangeMode(flags.changeMode)
		if modeErr != nil {
			fmt.Fprintln(os.Stderr, modeErr)

			return 2
		}

		changeMode = parsedMode

		if flags.changeCount <= 0 {
			fmt.Fprintln(os.Stderr, "-change-count must be >= 1")

			return 2
		}

		if flags.changeRounds <= 0 {
			fmt.Fprintln(os.Stderr, "-change-rounds must be >= 1")

			return 2
		}

		if flags.changeSize < 0 {
			fmt.Fprintln(os.Stderr, "-change-size must be >= 0")

			return 2
		}

		if flags.changeTimeout <= 0 {
			flags.changeTimeout = maxDuration(2*flags.interval, 500*time.Millisecond)
		}

		if changeMode == changeMixed {
			if flags.changeMixCreate < 0 || flags.changeMixModify < 0 || flags.changeMixDelete < 0 {
				fmt.Fprintln(os.Stderr, "-change-mix-* must be >= 0")

				return 2
			}

			mixCreate = flags.changeMixCreate
			mixModify = flags.changeMixModify

			mixDelete = flags.changeMixDelete
			if mixCreate+mixModify+mixDelete == 0 {
				fmt.Fprintln(os.Stderr, "-change-mix-* sum must be > 0")

				return 2
			}
		}
	}

	if flags.gcPercent >= 0 {
		debug.SetGCPercent(flags.gcPercent)
	}

	var cpuFile *os.File
	if flags.cpuProfile != "" {
		cpuFile, err = os.Create(flags.cpuProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error creating cpuprofile: %v\n", err)

			return 1
		}

		err = pprof.StartCPUProfile(cpuFile)
		if err != nil {
			_ = cpuFile.Close()

			fmt.Fprintf(os.Stderr, "error starting cpuprofile: %v\n", err)

			return 1
		}

		defer func() {
			pprof.StopCPUProfile()

			_ = cpuFile.Close()
		}()
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	go func() {
		select {
		case s := <-sigCh:
			fmt.Fprintf(os.Stderr, "signal: %s, stopping\n", s)
			cancel(fmt.Errorf("signal: %s", s))
		case <-ctx.Done():
		}
	}()

	opts := []fileproc.Option{
		fileproc.WithSuffix(flags.suffix),
		fileproc.WithOnError(func(err error, _, _ int) bool {
			if errors.Is(err, os.ErrNotExist) {
				return false
			}

			cancel(err)

			return true
		}),
	}

	if flags.tree {
		opts = append(opts, fileproc.WithRecursive())
	}

	if flags.interval > 0 {
		opts = append(opts, fileproc.WithInterval(flags.interval))
	}

	if flags.minIdle > 0 {
		opts = append(opts, fileproc.WithMinIdle(flags.minIdle))
	}

	if flags.eventBuffer > 0 {
		opts = append(opts, fileproc.WithEventBuffer(flags.eventBuffer))
	}

	if flags.emitBaseline {
		opts = append(opts, fileproc.WithEmitBaseline())
	}

	if flags.workers > 0 {
		opts = append(opts, fileproc.WithFileWorkers(flags.workers))
	}

	if flags.scanWorkers > 0 {
		opts = append(opts, fileproc.WithScanWorkers(flags.scanWorkers))
	}

	if flags.chunkSize > 0 {
		opts = append(opts, fileproc.WithChunkSize(flags.chunkSize))
	}

	if flags.expectedFiles > 0 {
		opts = append(opts, fileproc.WithExpectedFiles(flags.expectedFiles))
	}

	w, err := fileproc.NewWatcher(flags.dir, opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating watcher: %v\n", err)

		return 1
	}

	var recorder *eventRecorder
	if bench == benchChanges {
		recorder = newEventRecorder()
	}

	var memSampler *memStatsSampler

	if flags.memStatsEvery > 0 {
		sampler, err := startMemStatsSampler(ctx, flags.memStatsEvery, flags.memStatsOut)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error starting memstats sampler: %v\n", err)

			return 1
		}

		memSampler = sampler
	}

	progress := newProgressState()

	var wg sync.WaitGroup

	switch mode {
	case modeCallback:
		wg.Go(func() {
			if recorder != nil {
				w.Watch(ctx, recorder.OnEvent)

				return
			}

			w.Watch(ctx, nil)
		})
	case modeChannel:
		ch := w.Events(ctx)

		wg.Go(func() {
			if recorder != nil {
				for ev := range ch {
					recorder.OnEvent(ev)
				}

				return
			}

			for range ch {
			}
		})
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", mode)

		return 2
	}

	if flags.progressEvery > 0 {
		startProgressLogger(ctx, &wg, w, flags, bench, recorder, progress)
	}

	pollInterval := flags.interval / 4
	if pollInterval <= 0 {
		pollInterval = 2 * time.Millisecond
	}

	if pollInterval > 25*time.Millisecond {
		pollInterval = 25 * time.Millisecond
	}

	var (
		samples      []uint64
		baseStats    fileproc.Stats
		baseSet      bool
		measureStart time.Time
		measureEnd   time.Time

		changeExpected   int
		changeSeen       int
		changeMismatched int
		changeDuplicates int
		changeLatencies  []time.Duration
		cleanupPaths     []string
	)

	if bench == benchScan {
		var lastScans uint64

		progress.Set("warmup", 0)

		if flags.warmup == 0 {
			baseStats = w.Stats()
			baseSet = true

			progress.Set("measure", 0)

			measureStart = time.Now()
		}

		targetScans := uint64(flags.warmup + flags.scans)

		for ctx.Err() == nil {
			stats := w.Stats()
			if stats.Scans > lastScans {
				delta := stats.Scans - lastScans
				for i := range delta {
					cur := lastScans + i + 1
					if !baseSet && int(cur) >= flags.warmup {
						baseStats = stats
						baseSet = true

						progress.Set("measure", 0)

						measureStart = time.Now()
					}

					if int(cur) > flags.warmup && int(cur) <= flags.warmup+flags.scans {
						samples = append(samples, durationNsToUint64(stats.LastScanNs))
					}
				}

				lastScans = stats.Scans
				if lastScans >= targetScans {
					progress.Set("done", 0)

					break
				}
			}

			if !waitInterval(ctx, pollInterval) {
				break
			}
		}

		measureEnd = time.Now()
	} else {
		if recorder == nil {
			cancel(errors.New("changes bench requires event recorder"))
		} else {
			rootAbs, err := filepath.Abs(flags.dir)
			if err != nil {
				cancel(err)
			} else {
				payload := make([]byte, 0, flags.changeSize)
				payload = payload[:flags.changeSize]
				modPayload := []byte{0x1}
				changePrefix := fmt.Sprintf("%s%s_", flags.changePrefix, time.Now().Format("150405.000000000"))

				ensurePaths := func(paths []string) error {
					return ensureFiles(paths, payload)
				}

				cleanupPaths = make([]string, 0, flags.changeCount*flags.changeRounds)

				var (
					modifyPaths []string
					deletePaths []string
				)

				switch changeMode {
				case changeModify:
					modifyPaths = buildPaths(rootAbs, changePrefix, flags.suffix, "m", flags.changeCount, 0, false)

					err = ensurePaths(modifyPaths)
					if err != nil {
						cancel(err)
					}

					cleanupPaths = append(cleanupPaths, modifyPaths...)
				case changeDelete:
					deletePaths = buildPaths(rootAbs, changePrefix, flags.suffix, "d", flags.changeCount, 0, false)

					err = ensurePaths(deletePaths)
					if err != nil {
						cancel(err)
					}

					cleanupPaths = append(cleanupPaths, deletePaths...)
				case changeMixed:
					_, mixModifyCount, mixDeleteCount := splitMixedCounts(flags.changeCount, mixCreate, mixModify, mixDelete)
					modifyPaths = buildPaths(rootAbs, changePrefix, flags.suffix, "m", mixModifyCount, 0, false)
					deletePaths = buildPaths(rootAbs, changePrefix, flags.suffix, "d", mixDeleteCount, 0, false)

					err = ensurePaths(modifyPaths)
					if err != nil {
						cancel(err)
					}

					err = ensurePaths(deletePaths)
					if err != nil {
						cancel(err)
					}

					cleanupPaths = append(cleanupPaths, modifyPaths...)
					cleanupPaths = append(cleanupPaths, deletePaths...)
				}

				if ctx.Err() == nil {
					progress.Set("warmup", 0)
					waitForScans(ctx, w, uint64(flags.warmup), pollInterval)
					baseStats = w.Stats()
					baseSet = true

					progress.Set("round", 1)

					measureStart = time.Now()

					sampler := startScanSampler(ctx, w, pollInterval)

					for round := 0; round < flags.changeRounds && ctx.Err() == nil; round++ {
						progress.Set("round", round+1)

						var ops []changeOp

						switch changeMode {
						case changeCreate:
							paths := buildPaths(rootAbs, changePrefix, flags.suffix, "c", flags.changeCount, round, true)
							ops = buildOps(paths, fileproc.Create)
							cleanupPaths = append(cleanupPaths, paths...)
						case changeModify:
							ops = buildOps(modifyPaths, fileproc.Modify)
						case changeDelete:
							err := ensurePaths(deletePaths)
							if err != nil {
								cancel(err)

								break
							}

							ops = buildOps(deletePaths, fileproc.Delete)
						case changeMixed:
							mixCreateCount, mixModifyCount, mixDeleteCount := splitMixedCounts(flags.changeCount, mixCreate, mixModify, mixDelete)
							createPaths := buildPaths(rootAbs, changePrefix, flags.suffix, "c", mixCreateCount, round, true)

							err := ensurePaths(deletePaths)
							if err != nil {
								cancel(err)

								break
							}

							ops = append(ops, buildOps(createPaths, fileproc.Create)...)
							ops = append(ops, buildOps(modifyPaths[:mixModifyCount], fileproc.Modify)...)
							ops = append(ops, buildOps(deletePaths[:mixDeleteCount], fileproc.Delete)...)
							cleanupPaths = append(cleanupPaths, createPaths...)
						}

						if ctx.Err() != nil {
							break
						}

						waitForScans(ctx, w, w.Stats().Scans+1, pollInterval)

						if ctx.Err() != nil {
							break
						}

						recorder.BeginRound(ops)

						err := applyOps(recorder, ops, payload, modPayload)
						if err != nil {
							cancel(err)

							break
						}

						waitForExpected(recorder, len(ops), flags.changeTimeout, pollInterval)
						roundStats := recorder.EndRound()

						changeExpected += roundStats.expected
						changeSeen += roundStats.seen
						changeMismatched += roundStats.mismatched
						changeDuplicates += roundStats.duplicates
						changeLatencies = append(changeLatencies, roundStats.latencies...)
					}

					samples = sampler.Stop()
					measureEnd = time.Now()

					progress.Set("done", flags.changeRounds)
				}
			}
		}
	}

	if measureEnd.IsZero() {
		measureEnd = time.Now()
	}

	if flags.memProfile != "" {
		if flags.memProfileGC {
			runtime.GC()
		}

		memFile, err := os.Create(flags.memProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error creating memprofile: %v\n", err)

			return 1
		}

		err = pprof.WriteHeapProfile(memFile)
		if err != nil {
			_ = memFile.Close()

			fmt.Fprintf(os.Stderr, "error writing memprofile: %v\n", err)

			return 1
		}

		err = memFile.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error closing memprofile: %v\n", err)

			return 1
		}
	}

	cancel(nil)
	wg.Wait()

	var memSummary memSummary

	if memSampler != nil {
		summary, err := memSampler.Stop()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error writing memstats: %v\n", err)

			return 1
		}

		memSummary = summary
	}

	if bench == benchChanges && flags.changeCleanup {
		_ = removeFiles(cleanupPaths)
	}

	cause := context.Cause(ctx)
	if cause != nil && !errors.Is(cause, context.Canceled) {
		fmt.Fprintf(os.Stderr, "stopped: %v\n", cause)

		return 1
	}

	if !baseSet {
		baseStats = w.Stats()
		measureStart = measureEnd
	}

	endStats := w.Stats()
	measureDuration := measureEnd.Sub(measureStart)

	filesSeen := endStats.FilesSeen - baseStats.FilesSeen
	creates := endStats.Creates - baseStats.Creates
	modifies := endStats.Modifies - baseStats.Modifies
	deletes := endStats.Deletes - baseStats.Deletes
	events := endStats.EventCount - baseStats.EventCount
	errorsCount := endStats.Errors - baseStats.Errors
	behind := endStats.BehindCount - baseStats.BehindCount

	var (
		scanMean uint64
		scanP50  uint64
		scanP95  uint64
		scanP99  uint64
		scanMax  uint64
		scanSum  uint64
	)

	if len(samples) > 0 {
		slices.Sort(samples)

		for _, v := range samples {
			scanSum += v
		}

		scanMean = scanSum / uint64(len(samples))
		scanP50 = percentile(samples, 50)
		scanP95 = percentile(samples, 95)
		scanP99 = percentile(samples, 99)
		scanMax = samples[len(samples)-1]
	}

	var (
		scansPerSec float64
		filesPerSec float64
	)

	if measureDuration > 0 {
		scansPerSec = float64(len(samples)) / measureDuration.Seconds()
		filesPerSec = float64(filesSeen) / measureDuration.Seconds()
	}

	intervalUsed := flags.interval
	if intervalUsed <= 0 {
		intervalUsed = 250 * time.Millisecond
	}

	scanMeanPct := "-"
	if intervalUsed > 0 && scanMean > 0 {
		scanMeanPct = fmt.Sprintf("%.1f%%", float64(scanMean)/float64(intervalUsed.Nanoseconds())*100)
	}

	scanUtilPct := "-"
	idlePct := "-"

	if measureDuration > 0 && scanSum > 0 {
		util := float64(scanSum) / float64(measureDuration.Nanoseconds())
		scanUtilPct = fmt.Sprintf("%.1f%%", util*100)

		idle := 1 - util
		if idle < 0 {
			idle = 0
		}

		idlePct = fmt.Sprintf("%.1f%%", idle*100)
	} else if measureDuration > 0 {
		scanUtilPct = "0.0%"
		idlePct = "100.0%"
	}

	changeMissed := 0
	if changeExpected > changeSeen {
		changeMissed = changeExpected - changeSeen
	}

	var (
		changeLatMean uint64
		changeLatP50  uint64
		changeLatP95  uint64
		changeLatP99  uint64
		changeLatMax  uint64
	)

	if len(changeLatencies) > 0 {
		latNs := make([]uint64, 0, len(changeLatencies))
		latNs = latNs[:len(changeLatencies)]

		var sum uint64

		for i, v := range changeLatencies {
			ns := uint64(v.Nanoseconds())
			latNs[i] = ns
			sum += ns
		}

		slices.Sort(latNs)
		changeLatMean = sum / uint64(len(latNs))
		changeLatP50 = percentile(latNs, 50)
		changeLatP95 = percentile(latNs, 95)
		changeLatP99 = percentile(latNs, 99)
		changeLatMax = latNs[len(latNs)-1]
	}

	goVersion := ""
	vcsRevision := ""
	vcsTime := ""
	vcsModified := false

	if bi, ok := debug.ReadBuildInfo(); ok && bi != nil {
		goVersion = bi.GoVersion
		for _, setting := range bi.Settings {
			switch setting.Key {
			case "vcs.revision":
				vcsRevision = setting.Value
			case "vcs.time":
				vcsTime = setting.Value
			case "vcs.modified":
				vcsModified = setting.Value == "true"
			}
		}
	}

	changeCountOut := 0
	changeRoundsOut := 0
	changeSizeOut := 0
	changeTimeoutOut := time.Duration(0)

	if bench == benchChanges {
		changeCountOut = flags.changeCount
		changeRoundsOut = flags.changeRounds
		changeSizeOut = flags.changeSize
		changeTimeoutOut = flags.changeTimeout
	}

	res := benchResult{
		Timestamp:         time.Now(),
		Case:              flags.caseName,
		Notes:             flags.notes,
		Dir:               flags.dir,
		Tree:              flags.tree,
		Suffix:            flags.suffix,
		Interval:          flags.interval,
		MinIdle:           flags.minIdle,
		Bench:             bench,
		Mode:              mode,
		EventBuffer:       flags.eventBuffer,
		EmitBaseline:      flags.emitBaseline,
		ChangeMode:        changeMode,
		ChangeCount:       changeCountOut,
		ChangeRounds:      changeRoundsOut,
		ChangeSize:        changeSizeOut,
		ChangeTimeout:     changeTimeoutOut,
		ChangeMixCreate:   mixCreate,
		ChangeMixModify:   mixModify,
		ChangeMixDelete:   mixDelete,
		ChangeExpected:    changeExpected,
		ChangeSeen:        changeSeen,
		ChangeMissed:      changeMissed,
		ChangeMismatched:  changeMismatched,
		ChangeDuplicates:  changeDuplicates,
		ChangeLatMeanNs:   changeLatMean,
		ChangeLatP50Ns:    changeLatP50,
		ChangeLatP95Ns:    changeLatP95,
		ChangeLatP99Ns:    changeLatP99,
		ChangeLatMaxNs:    changeLatMax,
		ChangeLatSamples:  len(changeLatencies),
		Workers:           flags.workers,
		ScanWorkers:       flags.scanWorkers,
		ChunkSize:         flags.chunkSize,
		ExpectedFiles:     flags.expectedFiles,
		WarmupScans:       flags.warmup,
		Scans:             flags.scans,
		GCPercent:         flags.gcPercent,
		Duration:          measureDuration,
		ScanMeanNs:        scanMean,
		ScanP50Ns:         scanP50,
		ScanP95Ns:         scanP95,
		ScanP99Ns:         scanP99,
		ScanMaxNs:         scanMax,
		ScanSamples:       len(samples),
		FilesSeen:         filesSeen,
		Creates:           creates,
		Modifies:          modifies,
		Deletes:           deletes,
		Events:            events,
		Errors:            errorsCount,
		Behind:            behind,
		ScansPerSec:       scansPerSec,
		FilesPerSec:       filesPerSec,
		MemSamples:        memSummary.Samples,
		MemPeakHeapAlloc:  memSummary.PeakHeapAlloc,
		MemPeakHeapInuse:  memSummary.PeakHeapInuse,
		MemPeakHeapSys:    memSummary.PeakHeapSys,
		MemFinalHeapAlloc: memSummary.FinalHeapAlloc,
		MemFinalHeapInuse: memSummary.FinalHeapInuse,
		MemFinalHeapSys:   memSummary.FinalHeapSys,
		GoVersion:         goVersion,
		GOOS:              runtime.GOOS,
		GOARCH:            runtime.GOARCH,
		GOMAXPROCS:        runtime.GOMAXPROCS(0),
		NumCPU:            runtime.NumCPU(),
		VCSRevision:       vcsRevision,
		VCSTime:           vcsTime,
		VCSModified:       vcsModified,
	}

	if flags.out != "" {
		err := appendJSONL(flags.out, &res)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error writing -out: %v\n", err)

			return 1
		}
	}

	if flags.quiet {
		fmt.Printf("%.0f\n", filesPerSec)

		return 0
	}

	if bench == benchChanges {
		printTable([]string{
			"bench",
			"event_mode",
			"change_mode",
			"rounds",
			"expected",
			"seen",
			"missed",
			"lat_p50",
			"lat_p95",
			"lat_p99",
			"interval",
			"duration",
			"scan_mean",
			"scan_mean_pct",
			"scan_util",
			"idle",
			"scan_p95",
			"files/sec",
			"events",
			"behind",
		}, []string{
			bench,
			mode,
			changeMode,
			strconv.Itoa(flags.changeRounds),
			strconv.Itoa(changeExpected),
			strconv.Itoa(changeSeen),
			strconv.Itoa(changeMissed),
			fmtMaybeDurationNs(int64(changeLatP50)),
			fmtMaybeDurationNs(int64(changeLatP95)),
			fmtMaybeDurationNs(int64(changeLatP99)),
			intervalUsed.String(),
			fmtMaybeDuration(measureDuration),
			fmtMaybeDurationNs(int64(scanMean)),
			scanMeanPct,
			scanUtilPct,
			idlePct,
			fmtMaybeDurationNs(int64(scanP95)),
			fmt.Sprintf("%.0f", filesPerSec),
			strconv.FormatUint(events, 10),
			strconv.FormatUint(behind, 10),
		})

		return 0
	}

	printTable([]string{
		"bench",
		"event_mode",
		"scans",
		"interval",
		"scan_mean",
		"scan_p95",
		"scan_p99",
		"scan_mean_pct",
		"scan_util",
		"idle",
		"duration",
		"files/sec",
		"events",
		"behind",
	}, []string{
		bench,
		mode,
		strconv.Itoa(len(samples)),
		intervalUsed.String(),
		fmtMaybeDurationNs(int64(scanMean)),
		fmtMaybeDurationNs(int64(scanP95)),
		fmtMaybeDurationNs(int64(scanP99)),
		scanMeanPct,
		scanUtilPct,
		idlePct,
		fmtMaybeDuration(measureDuration),
		fmt.Sprintf("%.0f", filesPerSec),
		strconv.FormatUint(events, 10),
		strconv.FormatUint(behind, 10),
	})

	return 0
}

func parseMode(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case modeCallback:
		return modeCallback, nil
	case modeChannel:
		return modeChannel, nil
	default:
		return "", fmt.Errorf("invalid -mode %q (expected: callback | channel)", mode)
	}
}

func parseBench(bench string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(bench)) {
	case benchScan:
		return benchScan, nil
	case benchChanges:
		return benchChanges, nil
	default:
		return "", fmt.Errorf("invalid -bench %q (expected: scan | changes)", bench)
	}
}

func ensureBenchDir(path string, bench string) error {
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("-dir must be a directory: %s", path)
		}

		return nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat -dir: %w", err)
	}

	if bench != benchChanges {
		return fmt.Errorf("missing -dir (create it or use -bench=changes to auto-create): %s", path)
	}

	mkErr := os.MkdirAll(path, 0o755)
	if mkErr != nil {
		return fmt.Errorf("create -dir: %w", mkErr)
	}

	return nil
}

func parseChangeMode(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case changeCreate:
		return changeCreate, nil
	case changeModify:
		return changeModify, nil
	case changeDelete:
		return changeDelete, nil
	case changeMixed:
		return changeMixed, nil
	default:
		return "", fmt.Errorf("invalid -change-mode %q (expected: create | modify | delete | mixed)", mode)
	}
}

type changeOp struct {
	path string
	kind fileproc.EventType
}

type roundStats struct {
	expected   int
	seen       int
	mismatched int
	duplicates int
	latencies  []time.Duration
}

type eventRecorder struct {
	mu         sync.Mutex
	active     bool
	index      map[string]int
	expected   []fileproc.EventType
	start      []time.Time
	seen       []bool
	latencies  []time.Duration
	seenCount  int
	mismatched int
	duplicates int
}

func newEventRecorder() *eventRecorder {
	return &eventRecorder{}
}

func (r *eventRecorder) BeginRound(ops []changeOp) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.active = true
	r.index = make(map[string]int, len(ops))
	r.expected = make([]fileproc.EventType, 0, len(ops))
	r.expected = r.expected[:len(ops)]
	r.start = make([]time.Time, 0, len(ops))
	r.start = r.start[:len(ops)]
	r.seen = make([]bool, 0, len(ops))
	r.seen = r.seen[:len(ops)]
	r.latencies = make([]time.Duration, 0, len(ops))
	r.seenCount = 0
	r.mismatched = 0

	r.duplicates = 0
	for i, op := range ops {
		r.index[op.path] = i
		r.expected[i] = op.kind
	}
}

func (r *eventRecorder) SetStart(idx int, t time.Time) {
	r.mu.Lock()

	if idx >= 0 && idx < len(r.start) {
		r.start[idx] = t
	}

	r.mu.Unlock()
}

func (r *eventRecorder) OnEvent(ev fileproc.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.active {
		return
	}

	idx, ok := r.index[ev.Path]
	if !ok {
		return
	}

	if ev.Type != r.expected[idx] {
		r.mismatched++

		return
	}

	if r.seen[idx] {
		r.duplicates++

		return
	}

	r.seen[idx] = true

	r.seenCount++
	if !r.start[idx].IsZero() {
		r.latencies = append(r.latencies, time.Since(r.start[idx]))
	}
}

func (r *eventRecorder) Progress() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.seenCount, len(r.expected)
}

func (r *eventRecorder) EndRound() roundStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.active = false
	lat := make([]time.Duration, 0, len(r.latencies))
	lat = lat[:len(r.latencies)]
	copy(lat, r.latencies)

	return roundStats{
		expected:   len(r.expected),
		seen:       r.seenCount,
		mismatched: r.mismatched,
		duplicates: r.duplicates,
		latencies:  lat,
	}
}

func buildPaths(root, prefix, suffix, label string, count int, round int, includeRound bool) []string {
	paths := make([]string, 0, count)

	paths = paths[:count]
	for i := range count {
		name := prefix + label
		if includeRound {
			name += fmt.Sprintf("_r%02d", round)
		}

		name += fmt.Sprintf("_%06d", i)
		if suffix != "" {
			name += suffix
		}

		paths[i] = filepath.Join(root, name)
	}

	return paths
}

func buildOps(paths []string, kind fileproc.EventType) []changeOp {
	ops := make([]changeOp, 0, len(paths))

	ops = ops[:len(paths)]
	for i, path := range paths {
		ops[i] = changeOp{path: path, kind: kind}
	}

	return ops
}

func splitMixedCounts(total, mixCreate, mixModify, mixDelete int) (int, int, int) {
	sum := mixCreate + mixModify + mixDelete
	if sum <= 0 {
		return 0, 0, 0
	}

	create := (total * mixCreate) / sum
	modify := (total * mixModify) / sum
	del := total - create - modify

	return create, modify, del
}

func ensureFiles(paths []string, payload []byte) error {
	for _, path := range paths {
		err := os.WriteFile(path, payload, 0o644)
		if err != nil {
			return fmt.Errorf("write file %s: %w", path, err)
		}
	}

	return nil
}

func applyOps(rec *eventRecorder, ops []changeOp, payload []byte, modPayload []byte) error {
	for i, op := range ops {
		rec.SetStart(i, time.Now())

		switch op.kind {
		case fileproc.Create:
			err := os.WriteFile(op.path, payload, 0o644)
			if err != nil {
				return fmt.Errorf("create file %s: %w", op.path, err)
			}
		case fileproc.Modify:
			f, err := os.OpenFile(op.path, os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				return fmt.Errorf("open file %s: %w", op.path, err)
			}

			_, err = f.Write(modPayload)
			closeErr := f.Close()

			if err != nil {
				return fmt.Errorf("write file %s: %w", op.path, err)
			}

			if closeErr != nil {
				return fmt.Errorf("close file %s: %w", op.path, closeErr)
			}
		case fileproc.Delete:
			err := os.Remove(op.path)
			if err != nil {
				return fmt.Errorf("remove file %s: %w", op.path, err)
			}
		default:
			return fmt.Errorf("unknown change op: %v", op.kind)
		}
	}

	return nil
}

func removeFiles(paths []string) error {
	var errs error

	for _, path := range paths {
		err := os.Remove(path)
		if err != nil && !os.IsNotExist(err) {
			errs = errors.Join(errs, err)
		}
	}

	return errs
}

func waitInterval(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func sleepInterval(d time.Duration) {
	timer := time.NewTimer(d)
	<-timer.C
}

func waitForScans(ctx context.Context, w *fileproc.Watcher, target uint64, pollInterval time.Duration) {
	for ctx.Err() == nil {
		if w.Stats().Scans >= target {
			return
		}

		if !waitInterval(ctx, pollInterval) {
			return
		}
	}
}

func waitForExpected(rec *eventRecorder, expected int, timeout time.Duration, pollInterval time.Duration) bool {
	deadline := time.Now().Add(timeout)

	for {
		seen, _ := rec.Progress()
		if seen >= expected {
			return true
		}

		if time.Now().After(deadline) {
			return false
		}

		sleepInterval(pollInterval)
	}
}

type scanSampler struct {
	stop    chan struct{}
	done    chan struct{}
	samples []uint64
}

func startScanSampler(ctx context.Context, w *fileproc.Watcher, pollInterval time.Duration) *scanSampler {
	s := &scanSampler{
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}

	go func() {
		defer close(s.done)

		lastScans := w.Stats().Scans

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stop:
				return
			default:
			}

			stats := w.Stats()
			if stats.Scans > lastScans {
				delta := stats.Scans - lastScans
				for range delta {
					s.samples = append(s.samples, durationNsToUint64(stats.LastScanNs))
				}

				lastScans = stats.Scans
			}

			timer := time.NewTimer(pollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()

				return
			case <-s.stop:
				timer.Stop()

				return
			case <-timer.C:
			}
		}
	}()

	return s
}

func (s *scanSampler) Stop() []uint64 {
	close(s.stop)
	<-s.done

	return s.samples
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}

	return b
}

func percentile(sorted []uint64, p float64) uint64 {
	if len(sorted) == 0 {
		return 0
	}

	if p <= 0 {
		return sorted[0]
	}

	if p >= 100 {
		return sorted[len(sorted)-1]
	}

	rank := min(max(int(math.Ceil((p/100)*float64(len(sorted)))), 1), len(sorted))

	return sorted[rank-1]
}

func durationNsToUint64(ns int64) uint64 {
	if ns <= 0 {
		return 0
	}

	return uint64(ns)
}

func fmtMaybeDurationNs(ns int64) string {
	if ns <= 0 {
		return "-"
	}

	return time.Duration(ns).String()
}

func fmtMaybeDuration(d time.Duration) string {
	if d == 0 {
		return "-"
	}

	return d.String()
}

type progressState struct {
	mu    sync.Mutex
	phase string
	round int
}

func newProgressState() *progressState {
	return &progressState{}
}

func (p *progressState) Set(phase string, round int) {
	p.mu.Lock()
	p.phase = phase
	p.round = round
	p.mu.Unlock()
}

func (p *progressState) Snapshot() (string, int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.phase, p.round
}

func startProgressLogger(
	ctx context.Context,
	wg *sync.WaitGroup,
	w *fileproc.Watcher,
	flags *benchFlags,
	bench string,
	recorder *eventRecorder,
	state *progressState,
) {
	wg.Go(func() {
		ticker := time.NewTicker(flags.progressEvery)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			stats := w.Stats()

			switch bench {
			case benchScan:
				target := flags.warmup + flags.scans

				scans := min(int(stats.Scans), target)

				phase := "warmup"
				if stats.Scans >= uint64(flags.warmup) {
					phase = "measure"
				}

				if stats.Scans >= uint64(target) {
					phase = "done"
				}

				fmt.Fprintf(os.Stderr,
					"progress bench=scan phase=%s scans=%d/%d last_scan=%s files=%d events=%d behind=%d\n",
					phase,
					scans,
					target,
					fmtMaybeDurationNs(stats.LastScanNs),
					stats.FilesSeen,
					stats.EventCount,
					stats.BehindCount,
				)
			case benchChanges:
				phase, round := state.Snapshot()

				seen, expected := 0, 0
				if recorder != nil {
					seen, expected = recorder.Progress()
				}

				fmt.Fprintf(os.Stderr,
					"progress bench=changes phase=%s round=%d/%d seen=%d/%d last_scan=%s events=%d behind=%d\n",
					phase,
					round,
					flags.changeRounds,
					seen,
					expected,
					fmtMaybeDurationNs(stats.LastScanNs),
					stats.EventCount,
					stats.BehindCount,
				)
			}
		}
	})
}

func printTable(headers []string, row []string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, strings.Join(headers, "\t"))
	fmt.Fprintln(w, strings.Join(row, "\t"))
	_ = w.Flush()
}

func appendJSONL(path string, res *benchResult) error {
	outFile, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open output: %w", err)
	}

	defer func() { _ = outFile.Close() }()

	writer := bufio.NewWriter(outFile)
	enc := json.NewEncoder(writer)
	enc.SetEscapeHTML(false)

	err = enc.Encode(res)
	if err != nil {
		return fmt.Errorf("encode json: %w", err)
	}

	err = writer.Flush()
	if err != nil {
		return fmt.Errorf("flush output: %w", err)
	}

	return nil
}

type memSample struct {
	Timestamp    time.Time `json:"ts"`
	HeapAlloc    uint64    `json:"heap_alloc"`
	HeapInuse    uint64    `json:"heap_inuse"`
	HeapSys      uint64    `json:"heap_sys"`
	HeapIdle     uint64    `json:"heap_idle"`
	HeapReleased uint64    `json:"heap_released"`
	HeapObjects  uint64    `json:"heap_objects"`
	Sys          uint64    `json:"sys"`
	NumGC        uint32    `json:"num_gc"`
	PauseTotalNs uint64    `json:"pause_total_ns"`
	LastPauseNs  uint64    `json:"last_pause_ns"`
}

type memSummary struct {
	Samples        int
	PeakHeapAlloc  uint64
	PeakHeapInuse  uint64
	PeakHeapSys    uint64
	FinalHeapAlloc uint64
	FinalHeapInuse uint64
	FinalHeapSys   uint64
}

type memStatsSampler struct {
	done chan memSamplerResult
}

type memSamplerResult struct {
	summary memSummary
	err     error
}

func startMemStatsSampler(ctx context.Context, interval time.Duration, outPath string) (*memStatsSampler, error) {
	outFile, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open memstats output: %w", err)
	}

	writer := bufio.NewWriter(outFile)
	enc := json.NewEncoder(writer)
	enc.SetEscapeHTML(false)

	done := make(chan memSamplerResult, 1)

	go func() {
		var (
			summary memSummary
			errs    error
		)

		defer func() {
			errs = errors.Join(errs, writer.Flush(), outFile.Close())
			done <- memSamplerResult{summary: summary, err: errs}
		}()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		sample := func() bool {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)

			lastPause := uint64(0)
			if ms.NumGC > 0 {
				lastPause = ms.PauseNs[(ms.NumGC-1)%uint32(len(ms.PauseNs))]
			}

			rec := memSample{
				Timestamp:    time.Now(),
				HeapAlloc:    ms.HeapAlloc,
				HeapInuse:    ms.HeapInuse,
				HeapSys:      ms.HeapSys,
				HeapIdle:     ms.HeapIdle,
				HeapReleased: ms.HeapReleased,
				HeapObjects:  ms.HeapObjects,
				Sys:          ms.Sys,
				NumGC:        ms.NumGC,
				PauseTotalNs: ms.PauseTotalNs,
				LastPauseNs:  lastPause,
			}

			err := enc.Encode(rec)
			if err != nil {
				errs = errors.Join(errs, fmt.Errorf("encode memstats: %w", err))

				return false
			}

			summary.Samples++
			summary.FinalHeapAlloc = rec.HeapAlloc
			summary.FinalHeapInuse = rec.HeapInuse
			summary.FinalHeapSys = rec.HeapSys

			if rec.HeapAlloc > summary.PeakHeapAlloc {
				summary.PeakHeapAlloc = rec.HeapAlloc
			}

			if rec.HeapInuse > summary.PeakHeapInuse {
				summary.PeakHeapInuse = rec.HeapInuse
			}

			if rec.HeapSys > summary.PeakHeapSys {
				summary.PeakHeapSys = rec.HeapSys
			}

			return true
		}

		if !sample() {
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !sample() {
					return
				}
			}
		}
	}()

	return &memStatsSampler{done: done}, nil
}

func (s *memStatsSampler) Stop() (memSummary, error) {
	if s == nil {
		return memSummary{}, nil
	}

	res := <-s.done

	return res.summary, res.err
}
