// Fileprocbench benchmarks the fileproc library.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/calvinalkan/fileproc"
)

type benchResult struct {
	Timestamp time.Time `json:"ts"`

	Case  string `json:"case,omitempty"`
	Notes string `json:"notes,omitempty"`

	Dir     string `json:"dir"`
	Tree    bool   `json:"tree"`
	Process string `json:"process"`
	Suffix  string `json:"suffix"`

	ReadSize  int   `json:"read"`
	Workers   int   `json:"workers"`
	Repeat    int   `json:"repeat"`
	GCPercent int   `json:"gc"`
	Expect    int64 `json:"expect,omitempty"`

	Visited   uint64        `json:"visited"`
	Matched   uint64        `json:"matched"`
	Errors    uint64        `json:"errors"`
	Duration  time.Duration `json:"duration"`
	PerSecond float64       `json:"files_per_sec"`
	MatchRate float64       `json:"match_rate"`

	GoVersion   string `json:"go"`
	GOOS        string `json:"goos"`
	GOARCH      string `json:"goarch"`
	GOMAXPROCS  int    `json:"gomaxprocs"`
	NumCPU      int    `json:"numcpu"`
	VCSRevision string `json:"vcs_revision,omitempty"`
	VCSTime     string `json:"vcs_time,omitempty"`
	VCSModified bool   `json:"vcs_modified,omitempty"`
}

var errMissingFrontmatterID = errors.New("missing frontmatter/id")

const (
	processFrontmatter = "frontmatter"
	processNoop        = "noop"
	processLazy        = "lazy"
)

type benchFlags struct {
	dir        string
	tree       bool
	process    string
	suffix     string
	readSize   int
	workers    int
	repeat     int
	gcPercent  int
	expect     int64
	quiet      bool
	caseName   string
	notes      string
	out        string
	cpuProfile string
	memProfile string
}

func parseFlags() *benchFlags {
	flags := &benchFlags{}

	flag.StringVar(&flags.dir, "dir", "", "directory to scan")
	flag.BoolVar(&flags.tree, "tree", false, "scan recursively")
	flag.StringVar(&flags.process, "process", "frontmatter", "process mode: frontmatter | noop | lazy")
	flag.StringVar(&flags.suffix, "suffix", ".md", "file suffix filter (empty = all files)")
	flag.IntVar(&flags.readSize, "read", 2048, "bytes to read per file (prefix only)")
	flag.IntVar(&flags.workers, "workers", 0, "worker count (0=auto)")
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

	opts := fileproc.Options{
		Workers:   flags.workers,
		ReadSize:  flags.readSize,
		Suffix:    flags.suffix,
		Recursive: flags.tree,
		OnError: func(err error, _, _ int) bool {
			// Benchmarks should never error; stop as quickly as possible.
			cancel(err)

			return true
		},
	}

	processFn := makeProcessFn(selectedProcess)
	lazyFn := makeLazyFn(selectedProcess, flags.readSize)

	var visited uint64

	start := time.Now()

	for range flags.repeat {
		var (
			results []fileproc.Result[struct{}]
			errs    []error
		)

		switch selectedProcess {
		case processLazy:
			results, errs = fileproc.ProcessLazy(ctx, flags.dir, lazyFn, opts)
		default:
			results, errs = fileproc.Process(ctx, flags.dir, processFn, opts)
		}

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

	perSec := float64(visited) / duration.Seconds()

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
		Timestamp:  time.Now(),
		Case:       flags.caseName,
		Notes:      flags.notes,
		Dir:        flags.dir,
		Tree:       flags.tree,
		Process:    selectedProcess,
		Suffix:     flags.suffix,
		ReadSize:   flags.readSize,
		Workers:    flags.workers,
		Repeat:     flags.repeat,
		GCPercent:  flags.gcPercent,
		Expect:     flags.expect,
		Visited:    visited,
		Matched:    visited,
		Errors:     0,
		Duration:   duration,
		PerSecond:  perSec,
		MatchRate:  matchRate,
		GoVersion:  goVersion,
		GOOS:       runtime.GOOS,
		GOARCH:     runtime.GOARCH,
		GOMAXPROCS: runtime.GOMAXPROCS(0),
		NumCPU:     runtime.NumCPU(),

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
		fmt.Printf("%.0f\n", perSec)

		return 0
	}

	fmt.Printf("visited=%d errors=%d repeat=%d duration=%v throughput=%.0f files/sec\n", visited, 0, flags.repeat, duration, perSec)

	return 0
}

func parseProcess(processFlag string) (string, error) {
	processName := strings.ToLower(strings.TrimSpace(processFlag))
	switch processName {
	case processFrontmatter, processNoop, processLazy:
		return processName, nil
	default:
		return "", fmt.Errorf("invalid -process %q (expected: frontmatter | noop | lazy)", processFlag)
	}
}

func makeProcessFn(process string) fileproc.ProcessFunc[struct{}] {
	var one struct{}

	switch process {
	case processNoop:
		return func(_ []byte, _ []byte) (*struct{}, error) {
			return &one, nil
		}
	case processFrontmatter:
		return func(_ []byte, data []byte) (*struct{}, error) {
			if !hasFrontMatterAndID(data) {
				return nil, errMissingFrontmatterID
			}

			return &one, nil
		}
	default:
		// parseProcess guards against unexpected values; keep a clear error in case
		// of internal misuse.
		return func(_ []byte, _ []byte) (*struct{}, error) {
			return nil, fmt.Errorf("unknown process mode: %s", process)
		}
	}
}

func makeLazyFn(process string, readSize int) fileproc.ProcessLazyFunc[struct{}] {
	var one struct{}

	switch process {
	case processLazy:
		// Pre-allocate buffer outside callback - reused across files per worker
		// Note: ProcessLazy provides Scratch for this, but we use a simple buffer
		// here to match frontmatter's prefix-read behavior exactly.
		return func(f *fileproc.File, scratch *fileproc.Scratch) (*struct{}, error) {
			// Read same prefix as frontmatter mode (no stat syscall)
			buf := scratch.Get(readSize)
			buf = buf[:cap(buf)]

			n, err := f.Read(buf)
			if err != nil && err.Error() != "EOF" {
				return nil, fmt.Errorf("read: %w", err)
			}

			data := buf[:n]
			if !hasFrontMatterAndID(data) {
				return nil, errMissingFrontmatterID
			}

			return &one, nil
		}
	default:
		return func(_ *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
			return nil, fmt.Errorf("unknown process mode: %s", process)
		}
	}
}

// hasFrontMatterAndID implements a small, allocation-free-ish frontmatter parser.
//
// Expected format:
//
//	---\n
//	key: value\n
//	...\n
//	---\n
func hasFrontMatterAndID(data []byte) bool {
	if len(data) < 4 || data[0] != '-' || data[1] != '-' || data[2] != '-' {
		return false
	}

	i := 3
	if data[i] == '\r' {
		i++
	}

	if i >= len(data) || data[i] != '\n' {
		return false
	}

	i++

	var sawID bool

	for i < len(data) {
		lineStart := i
		for i < len(data) && data[i] != '\n' {
			i++
		}

		lineEnd := i
		if lineEnd > lineStart && data[lineEnd-1] == '\r' {
			lineEnd--
		}

		line := data[lineStart:lineEnd]
		if len(line) == 3 && line[0] == '-' && line[1] == '-' && line[2] == '-' {
			return sawID
		}

		indentIdx := 0
		for indentIdx < len(line) && (line[indentIdx] == ' ' || line[indentIdx] == '\t') {
			indentIdx++
		}

		if len(line)-indentIdx >= 3 && line[indentIdx] == 'i' && line[indentIdx+1] == 'd' && line[indentIdx+2] == ':' {
			valueStart := indentIdx + 3
			for valueStart < len(line) && (line[valueStart] == ' ' || line[valueStart] == '\t') {
				valueStart++
			}

			if valueStart < len(line) {
				sawID = true
			}
		}

		if i < len(data) {
			i++
		}
	}

	return false
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
