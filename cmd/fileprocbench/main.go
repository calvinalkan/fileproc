// Fileprocbench benchmarks the fileproc library.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/calvinalkan/fileproc"
)

// datasetMeta is the structure of entries in meta.json written by ticketgen.
type datasetMeta struct {
	Files      uint64 `json:"files"`
	TotalBytes uint64 `json:"total_bytes"`
	AvgBytes   uint64 `json:"avg_bytes"`
}

// allDatasetMeta maps dataset directory name to its metadata.
type allDatasetMeta map[string]datasetMeta

type benchResult struct {
	Timestamp time.Time `json:"ts"`

	Case  string `json:"case,omitempty"`
	Notes string `json:"notes,omitempty"`

	Dir     string `json:"dir"`
	Tree    bool   `json:"tree"`
	Process string `json:"process"`
	Suffix  string `json:"suffix"`

	Workers     int   `json:"workers"`
	ScanWorkers int   `json:"scan_workers"`
	ChunkSize   int   `json:"chunk_size"`
	Repeat      int   `json:"repeat"`
	GCPercent   int   `json:"gc"`
	Expect      int64 `json:"expect,omitempty"`

	Visited      uint64        `json:"visited"`
	Matched      uint64        `json:"matched"`
	Errors       uint64        `json:"errors"`
	Duration     time.Duration `json:"duration"`
	FilesPerSec  float64       `json:"files_per_sec"`
	BytesTotal   uint64        `json:"bytes_total,omitempty"`
	BytesPerSec  float64       `json:"bytes_per_sec,omitempty"`
	AvgFileBytes uint64        `json:"avg_file_bytes,omitempty"`
	MatchRate    float64       `json:"match_rate"`

	GoVersion   string `json:"go"`
	GOOS        string `json:"goos"`
	GOARCH      string `json:"goarch"`
	GOMAXPROCS  int    `json:"gomaxprocs"`
	NumCPU      int    `json:"numcpu"`
	VCSRevision string `json:"vcs_revision,omitempty"`
	VCSTime     string `json:"vcs_time,omitempty"`
	VCSModified bool   `json:"vcs_modified,omitempty"`
}

const (
	processBytes    = "bytes"
	processRead     = "read"
	processStat     = "stat"
	defaultReadSize = 2048
)

type benchFlags struct {
	dir         string
	tree        bool
	process     string
	suffix      string
	workers     int
	scanWorkers int
	chunkSize   int
	repeat      int
	gcPercent   int
	expect      int64
	quiet       bool
	caseName    string
	notes       string
	out         string
	cpuProfile  string
	memProfile  string
}

func parseFlags() *benchFlags {
	flags := &benchFlags{}

	flag.StringVar(&flags.dir, "dir", "", "directory to scan")
	flag.BoolVar(&flags.tree, "tree", false, "scan recursively")
	flag.StringVar(&flags.process, "process", "bytes", "process mode: bytes | read | stat")
	flag.StringVar(&flags.suffix, "suffix", ".md", "file suffix filter (empty = all files)")
	flag.IntVar(&flags.workers, "workers", 0, "worker count (0=auto)")
	flag.IntVar(&flags.scanWorkers, "scan-workers", 0, "scan worker count (0=auto)")
	flag.IntVar(&flags.chunkSize, "chunk-size", 0, "entries per chunk (0=auto)")
	flag.IntVar(&flags.repeat, "repeat", 1, "repeat the scan N times per invocation")
	flag.IntVar(&flags.gcPercent, "gc", -1, "if >=0, call debug.SetGCPercent(gc)")
	flag.Int64Var(&flags.expect, "expect", -1, "if >=0, require visited count to match (per scan)")
	flag.BoolVar(&flags.quiet, "q", false, "quiet: print only files/sec")
	flag.StringVar(&flags.caseName, "case", "", "optional short case name to store in JSON output")
	flag.StringVar(&flags.notes, "notes", "", "optional freeform notes to store in JSON output")
	flag.StringVar(&flags.out, "out", "", "optional JSONL output file to append one result per run")
	flag.StringVar(&flags.cpuProfile, "cpuprofile", "", "write CPU profile to file")
	flag.StringVar(&flags.memProfile, "memprofile", "", "write memory profile to file")

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

	if flags.repeat <= 0 {
		fmt.Fprintln(os.Stderr, "-repeat must be >= 1")

		return 2
	}

	if flags.expect == 0 {
		fmt.Fprintln(os.Stderr, "-expect must be -1 or > 0")

		return 2
	}

	// Load dataset metadata (for expect default and bytes stats)
	meta, metaErr := loadDatasetMeta(flags.dir)
	if metaErr != nil {
		fmt.Fprintf(os.Stderr, "warning: %v\n", metaErr)
	}

	// Use meta.Files as expect default if not explicitly set
	if flags.expect < 0 && meta != nil && meta.Files > 0 {
		flags.expect = int64(meta.Files)
	}

	selectedProcess, err := parseProcess(flags.process)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)

		return 2
	}

	if flags.gcPercent >= 0 {
		debug.SetGCPercent(flags.gcPercent)
	}

	if flags.cpuProfile != "" {
		cpuFile, err := os.Create(flags.cpuProfile)
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

	opts := []fileproc.Option{
		fileproc.WithSuffix(flags.suffix),
		fileproc.WithOnError(func(err error, _, _ int) bool {
			// Benchmarks should never error; stop as quickly as possible.
			cancel(err)

			return true
		}),
	}

	if flags.tree {
		opts = append(opts, fileproc.WithRecursive())
	}

	if flags.workers != 0 {
		opts = append(opts, fileproc.WithFileWorkers(flags.workers))
	}

	if flags.scanWorkers != 0 {
		opts = append(opts, fileproc.WithScanWorkers(flags.scanWorkers))
	}

	if flags.chunkSize != 0 {
		opts = append(opts, fileproc.WithChunkSize(flags.chunkSize))
	}

	processFn := makeProcessFn(selectedProcess)

	var visited uint64

	start := time.Now()

	for range flags.repeat {
		var (
			results []*struct{}
			errs    []error
		)

		results, errs = fileproc.Process(ctx, flags.dir, processFn, opts...)

		// In benchmarks we never expect errors.
		if len(errs) > 0 {
			maxErrsToPrint := min(len(errs), 10)
			for i := range maxErrsToPrint {
				fmt.Fprintf(os.Stderr, "error: %v\n", errs[i])
			}

			if len(errs) > maxErrsToPrint {
				fmt.Fprintf(os.Stderr, "... and %d more errors\n", len(errs)-maxErrsToPrint)
			}

			fmt.Fprintf(os.Stderr, "errors=%d\n", len(errs))

			return 1
		}

		visited += uint64(len(results))

		if flags.expect > 0 && int64(len(results)) != flags.expect {
			fmt.Fprintf(os.Stderr, "expected visited=%d, got %d\n", flags.expect, len(results))

			return 1
		}
	}

	if ctx.Err() != nil {
		fmt.Fprintf(os.Stderr, "stopped: %v\n", context.Cause(ctx))

		return 1
	}

	duration := time.Since(start)

	if flags.memProfile != "" {
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

	filesPerSec := float64(visited) / duration.Seconds()

	// Calculate bytes stats from metadata loaded earlier
	var (
		bytesTotal   uint64
		bytesPerSec  float64
		avgFileBytes uint64
	)

	if meta != nil && meta.TotalBytes > 0 {
		// Scale by repeat count since we processed the dataset multiple times
		bytesTotal = meta.TotalBytes * uint64(flags.repeat)
		bytesPerSec = float64(bytesTotal) / duration.Seconds()
		avgFileBytes = meta.AvgBytes
	}

	matchRate := 0.0
	if visited > 0 {
		matchRate = 1.0
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

	res := benchResult{
		Timestamp:    time.Now(),
		Case:         flags.caseName,
		Notes:        flags.notes,
		Dir:          flags.dir,
		Tree:         flags.tree,
		Process:      selectedProcess,
		Suffix:       flags.suffix,
		Workers:      flags.workers,
		ScanWorkers:  flags.scanWorkers,
		ChunkSize:    flags.chunkSize,
		Repeat:       flags.repeat,
		GCPercent:    flags.gcPercent,
		Expect:       flags.expect,
		Visited:      visited,
		Matched:      visited,
		Errors:       0,
		Duration:     duration,
		FilesPerSec:  filesPerSec,
		BytesTotal:   bytesTotal,
		BytesPerSec:  bytesPerSec,
		AvgFileBytes: avgFileBytes,
		MatchRate:    matchRate,
		GoVersion:    goVersion,
		GOOS:         runtime.GOOS,
		GOARCH:       runtime.GOARCH,
		GOMAXPROCS:   runtime.GOMAXPROCS(0),
		NumCPU:       runtime.NumCPU(),

		VCSRevision: vcsRevision,
		VCSTime:     vcsTime,
		VCSModified: vcsModified,
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

	if bytesPerSec > 0 {
		fmt.Printf("visited=%d errors=%d repeat=%d duration=%v files/sec=%.0f bytes/sec=%.0f MB/s=%.1f\n",
			visited, 0, flags.repeat, duration, filesPerSec, bytesPerSec, bytesPerSec/1_000_000)
	} else {
		fmt.Printf("visited=%d errors=%d repeat=%d duration=%v files/sec=%.0f\n",
			visited, 0, flags.repeat, duration, filesPerSec)
	}

	return 0
}

// loadDatasetMeta loads metadata for a dataset from parent's meta.json.
func loadDatasetMeta(dir string) (*datasetMeta, error) {
	parentDir := filepath.Dir(dir)
	datasetName := filepath.Base(dir)
	metaPath := filepath.Join(parentDir, "meta.json")

	data, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", metaPath, err)
	}

	var allMeta allDatasetMeta

	unmarshalErr := json.Unmarshal(data, &allMeta)
	if unmarshalErr != nil {
		return nil, fmt.Errorf("parse %s: %w", metaPath, unmarshalErr)
	}

	meta, ok := allMeta[datasetName]
	if !ok {
		return nil, fmt.Errorf("dataset %q not found in %s", datasetName, metaPath)
	}

	return &meta, nil
}

func parseProcess(processFlag string) (string, error) {
	processName := strings.ToLower(strings.TrimSpace(processFlag))
	switch processName {
	case processBytes, processRead, processStat:
		return processName, nil
	default:
		return "", fmt.Errorf("invalid -process %q (expected: bytes | read | stat)", processFlag)
	}
}

func makeProcessFn(process string) fileproc.ProcessFunc[struct{}] {
	var one struct{}

	switch process {
	case processStat:
		return func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
			_, err := f.Stat()
			if err != nil {
				return nil, fmt.Errorf("stat: %w", err)
			}

			return &one, nil
		}

	case processRead:
		return func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
			buf := w.Buf(defaultReadSize)
			buf = buf[:cap(buf)]

			_, err := f.Read(buf)
			if err != nil {
				return nil, fmt.Errorf("read: %w", err)
			}

			return &one, nil
		}

	case processBytes:
		return func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
			_, err := f.ReadAll()
			if err != nil {
				return nil, fmt.Errorf("bytes: %w", err)
			}

			return &one, nil
		}

	default:
		return func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
			return nil, fmt.Errorf("unknown process mode: %s", process)
		}
	}
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
