package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const summarizeUsage = `benchreport summarize - generate summary from benchmark run

Usage:
  benchreport summarize [options] <run-dir>

Arguments:
  run-dir    Path to benchmark run directory (e.g. .benchmarks/regress-2026-01-17T17-44-10+0100)

Options:
  --history FILE    Path to history.jsonl (default: .benchmarks/history.jsonl)
  --max-history N   Keep last N entries in history (default: 100)
  --no-history      Don't append to history.jsonl
  -h, --help        Show this help

Examples:
  benchreport summarize .benchmarks/regress-2026-01-17T17-44-10+0100
  benchreport summarize --no-history .benchmarks/regress-2026-01-17T17-44-10+0100
`

func runSummarize(args []string) error {
	fs := flag.NewFlagSet("summarize", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, summarizeUsage) }

	historyFile := fs.String("history", ".benchmarks/history.jsonl", "path to history file")
	maxHistory := fs.Int("max-history", 100, "max entries to keep in history")
	noHistory := fs.Bool("no-history", false, "don't append to history")

	parseErr := fs.Parse(args)
	if parseErr != nil {
		return fmt.Errorf("parse flags: %w", parseErr)
	}

	if fs.NArg() < 1 {
		fs.Usage()

		return errors.New("missing run-dir argument")
	}

	runDir := fs.Arg(0)

	return summarize(runDir, *historyFile, *maxHistory, *noHistory)
}

func summarize(runDir, historyFile string, maxHistory int, noHistory bool) error {
	// Validate run directory
	_, statErr := os.Stat(runDir)
	if os.IsNotExist(statErr) {
		return fmt.Errorf("run directory does not exist: %s", runDir)
	}

	if statErr != nil {
		return fmt.Errorf("stat run directory: %w", statErr)
	}

	// Check required files
	sizes := []string{"1k", "5k", "100k", "1m"}
	for _, size := range sizes {
		jsonPath := filepath.Join(runDir, fmt.Sprintf("hyperfine-%s.json", size))

		_, jsonStatErr := os.Stat(jsonPath)
		if os.IsNotExist(jsonStatErr) {
			return fmt.Errorf("missing: %s", jsonPath)
		}

		if jsonStatErr != nil {
			return fmt.Errorf("stat %s: %w", jsonPath, jsonStatErr)
		}
	}

	metaPath := filepath.Join(runDir, "meta.txt")

	_, statErr = os.Stat(metaPath)
	if os.IsNotExist(statErr) {
		return fmt.Errorf("missing: %s", metaPath)
	}

	if statErr != nil {
		return fmt.Errorf("stat %s: %w", metaPath, statErr)
	}

	fmt.Printf("Summarizing: %s\n", runDir)

	// Parse metadata
	meta, err := parseMeta(metaPath)
	if err != nil {
		return fmt.Errorf("parsing meta.txt: %w", err)
	}

	// Load dataset metadata for file counts
	datasetMeta, datasetMetaErr := loadDatasetMeta(".data/meta.json")
	if datasetMetaErr != nil {
		fmt.Fprintf(os.Stderr, "warning: %v (using default file counts)\n", datasetMetaErr)
	}

	// Parse hyperfine results
	type sizeInfo struct {
		files int
		bytes int
	}

	sizeInfoMap := map[string]sizeInfo{
		"1k":   {getFileCount(datasetMeta, "tickets_flat_1k", 1000), getTotalBytes(datasetMeta, "tickets_flat_1k")},
		"5k":   {getFileCount(datasetMeta, "tickets_flat_5k", 5000), getTotalBytes(datasetMeta, "tickets_flat_5k")},
		"100k": {getFileCount(datasetMeta, "tickets_flat_100k", 100000), getTotalBytes(datasetMeta, "tickets_flat_100k")},
		"1m":   {getFileCount(datasetMeta, "tickets_flat_1m", 1000000), getTotalBytes(datasetMeta, "tickets_flat_1m")},
	}

	results := make(map[string]BenchResult)

	for _, size := range sizes {
		jsonPath := filepath.Join(runDir, fmt.Sprintf("hyperfine-%s.json", size))
		info := sizeInfoMap[size]

		sizeResults, parseErr := parseHyperfine(jsonPath, info.files, info.bytes)
		if parseErr != nil {
			return fmt.Errorf("parsing %s: %w", jsonPath, parseErr)
		}

		maps.Copy(results, sizeResults)
	}

	// Get additional metadata
	hostname, _ := os.Hostname()
	gitUserName := gitConfig("user.name")
	gitUserEmail := gitConfig("user.email")
	cpuModel := extractCPUModel(meta)

	// Build summary
	summary := Summary{
		Timestamp: meta["ts"],
		RunDir:    filepath.Base(runDir),
		Git: GitInfo{
			Rev:       meta["git_rev"],
			Dirty:     meta["git_dirty"] == "true",
			UserName:  gitUserName,
			UserEmail: gitUserEmail,
		},
		Env: EnvInfo{
			Hostname:       hostname,
			CPUModel:       cpuModel,
			GoVersion:      strings.TrimPrefix(meta["go"], "go version "),
			PowerSource:    meta["power_source"],
			CPUGovernor:    meta["cpu_governor"],
			EnergyPerfPref: meta["energy_perf_pref"],
		},
		Config: ConfigInfo{
			Workers:     meta["workers"],
			ScanWorkers: meta["scan_workers"],
			ChunkSize:   meta["chunk_size"],
			GC:          meta["gc"],
			Read:        meta["read"],
			CPUSet:      meta["cpu_set"],
		},
		Results: results,
	}

	// Write summary.json
	summaryPath := filepath.Join(runDir, "summary.json")

	summaryJSON, err := marshalJSON(summary, true)
	if err != nil {
		return fmt.Errorf("marshaling summary: %w", err)
	}

	writeErr := os.WriteFile(summaryPath, summaryJSON, 0o644)
	if writeErr != nil {
		return fmt.Errorf("writing summary.json: %w", writeErr)
	}

	fmt.Printf("Wrote: %s\n", summaryPath)

	// Append to history
	if !noHistory {
		err := appendHistory(historyFile, &summary, maxHistory)
		if err != nil {
			return fmt.Errorf("appending to history: %w", err)
		}

		fmt.Printf("Appended to: %s\n", historyFile)
	}

	fmt.Println("Done.")

	return nil
}

func parseMeta(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open meta file: %w", err)
	}

	defer func() { _ = file.Close() }()

	meta := make(map[string]string)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if idx := strings.Index(line, ": "); idx > 0 {
			key := line[:idx]
			value := strings.TrimSpace(line[idx+2:])
			meta[key] = value
		}
	}

	scanErr := scanner.Err()
	if scanErr != nil {
		return nil, fmt.Errorf("scan meta file: %w", scanErr)
	}

	return meta, nil
}

func parseHyperfine(path string, fileCount, totalBytes int) (map[string]BenchResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read hyperfine output %s: %w", path, err)
	}

	var hf HyperfineOutput

	unmarshalErr := json.Unmarshal(data, &hf)
	if unmarshalErr != nil {
		return nil, fmt.Errorf("unmarshal hyperfine output %s: %w", path, unmarshalErr)
	}

	results := make(map[string]BenchResult)

	for _, r := range hf.Results {
		bytesPerSec := 0.0
		mbPerSec := 0.0

		if totalBytes > 0 {
			bytesPerSec = float64(totalBytes) / r.Mean
			mbPerSec = bytesPerSec / 1_000_000
		}

		results[r.Command] = BenchResult{
			MeanMs:      r.Mean * 1000,
			StddevMs:    r.Stddev * 1000,
			MedianMs:    r.Median * 1000,
			MinMs:       r.Min * 1000,
			MaxMs:       r.Max * 1000,
			FilesPerSec: float64(fileCount) / r.Mean,
			BytesPerSec: bytesPerSec,
			MBPerSec:    mbPerSec,
			Runs:        len(r.Times),
		}
	}

	return results, nil
}

func appendHistory(path string, summary *Summary, maxLines int) error {
	// Ensure directory exists
	mkdirErr := os.MkdirAll(filepath.Dir(path), 0o755)
	if mkdirErr != nil {
		return fmt.Errorf("create history directory: %w", mkdirErr)
	}

	// Read existing lines
	var lines []string

	data, readErr := os.ReadFile(path)
	if readErr == nil {
		for line := range strings.SplitSeq(string(data), "\n") {
			if line = strings.TrimSpace(line); line != "" {
				lines = append(lines, line)
			}
		}
	}

	// Add new line
	newLine, err := marshalJSON(summary, false)
	if err != nil {
		return fmt.Errorf("marshal history entry: %w", err)
	}

	lines = append(lines, string(newLine))

	// Trim to max
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}

	// Write back
	content := strings.Join(lines, "\n") + "\n"

	writeErr := os.WriteFile(path, []byte(content), 0o644)
	if writeErr != nil {
		return fmt.Errorf("write history file: %w", writeErr)
	}

	return nil
}

func gitConfig(key string) string {
	cmd := exec.CommandContext(context.Background(), "git", "config", key)

	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}

	return strings.TrimSpace(string(out))
}

func extractCPUModel(meta map[string]string) string {
	// CPU model is in the lscpu section, look for "Model name:" key
	if v, ok := meta["Model name"]; ok {
		return v
	}

	return "unknown"
}

// datasetMetaEntry is the structure of entries in meta.json written by ticketgen.
type datasetMetaEntry struct {
	Files      int `json:"files"`
	TotalBytes int `json:"total_bytes"`
	AvgBytes   int `json:"avg_bytes"`
}

// loadDatasetMeta loads the dataset metadata file.
func loadDatasetMeta(path string) (map[string]datasetMetaEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	var meta map[string]datasetMetaEntry

	unmarshalErr := json.Unmarshal(data, &meta)
	if unmarshalErr != nil {
		return nil, fmt.Errorf("parse %s: %w", path, unmarshalErr)
	}

	return meta, nil
}

// getFileCount returns the file count for a dataset, or default if not found.
func getFileCount(meta map[string]datasetMetaEntry, dataset string, def int) int {
	if meta == nil {
		return def
	}

	entry, ok := meta[dataset]
	if !ok {
		return def
	}

	if entry.Files <= 0 {
		return def
	}

	return entry.Files
}

// getTotalBytes returns the total bytes for a dataset, or 0 if not found.
func getTotalBytes(meta map[string]datasetMetaEntry, dataset string) int {
	if meta == nil {
		return 0
	}

	entry, ok := meta[dataset]
	if !ok {
		return 0
	}

	return entry.TotalBytes
}

// marshalJSON encodes v to JSON without escaping <, >, & characters.
func marshalJSON(v any, indent bool) ([]byte, error) {
	var buf strings.Builder

	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	if indent {
		enc.SetIndent("", "  ")
	}

	err := enc.Encode(v)
	if err != nil {
		return nil, fmt.Errorf("encode json: %w", err)
	}
	// Encode adds a trailing newline, trim it for consistency
	return []byte(strings.TrimSuffix(buf.String(), "\n")), nil
}
