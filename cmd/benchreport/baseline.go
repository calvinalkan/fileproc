package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
)

const baselineUsage = `benchreport baseline - manage baseline entries

Usage:
  benchreport baseline add [options] <run-dir|summary.json>
  benchreport baseline prune [options]
  benchreport baseline list [options]

Options:
  --file FILE   Baseline file (default: .benchmarks/baseline.jsonl)
  --keep N      Keep last N entries (for add/prune)
  -h, --help    Show this help

Examples:
  benchreport baseline add .benchmarks/regress-2026-01-17T19-33-32+0100
  benchreport baseline prune --keep 5
  benchreport baseline list
`

func runBaseline(args []string) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, baselineUsage)

		return errors.New("missing baseline subcommand")
	}

	sub := args[0]
	subArgs := args[1:]

	switch sub {
	case "add":
		return baselineAdd(subArgs)
	case "prune":
		return baselinePrune(subArgs)
	case "list":
		return baselineList(subArgs)
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, baselineUsage)

		return nil
	default:
		fmt.Fprint(os.Stderr, baselineUsage)

		return fmt.Errorf("unknown baseline subcommand: %s", sub)
	}
}

const baselineAddUsage = `benchreport baseline add - append a baseline entry

Usage:
  benchreport baseline add [options] <run-dir|summary.json>

Options:
  --file FILE   Baseline file (default: .benchmarks/baseline.jsonl)
  --keep N      Keep last N entries (optional)
`

func baselineAdd(args []string) error {
	fs := flag.NewFlagSet("baseline add", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, baselineAddUsage) }

	baselineFile := fs.String("file", ".benchmarks/baseline.jsonl", "baseline file")
	keep := fs.Int("keep", 0, "keep last N entries")

	parseErr := fs.Parse(args)
	if parseErr != nil {
		return fmt.Errorf("parse flags: %w", parseErr)
	}

	if fs.NArg() < 1 {
		fs.Usage()

		return errors.New("missing run-dir or summary.json")
	}

	summary, err := loadSummaryFromPath(fs.Arg(0))
	if err != nil {
		return err
	}

	baselines, err := loadBaselineSet(*baselineFile)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("loading baseline file: %w", err)
		}

		baselines = nil
	}

	baselines = append(baselines, summary)
	baselines = trimToKeep(baselines, *keep)

	writeErr := writeBaselineSet(*baselineFile, baselines)
	if writeErr != nil {
		return fmt.Errorf("writing baseline file: %w", writeErr)
	}

	fmt.Printf("Wrote %d baseline entries to %s\n", len(baselines), *baselineFile)

	return nil
}

const baselinePruneUsage = `benchreport baseline prune - keep last N baseline entries

Usage:
  benchreport baseline prune [options]

Options:
  --file FILE   Baseline file (default: .benchmarks/baseline.jsonl)
  --keep N      Keep last N entries (required)
`

func baselinePrune(args []string) error {
	fs := flag.NewFlagSet("baseline prune", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, baselinePruneUsage) }

	baselineFile := fs.String("file", ".benchmarks/baseline.jsonl", "baseline file")
	keep := fs.Int("keep", 0, "keep last N entries")

	parseErr := fs.Parse(args)
	if parseErr != nil {
		return fmt.Errorf("parse flags: %w", parseErr)
	}

	if *keep <= 0 {
		fs.Usage()

		return errors.New("--keep must be > 0")
	}

	baselines, err := loadBaselineSet(*baselineFile)
	if err != nil {
		return fmt.Errorf("loading baseline file: %w", err)
	}

	baselines = trimToKeep(baselines, *keep)

	writeErr := writeBaselineSet(*baselineFile, baselines)
	if writeErr != nil {
		return fmt.Errorf("writing baseline file: %w", writeErr)
	}

	fmt.Printf("Trimmed baseline to %d entries in %s\n", len(baselines), *baselineFile)

	return nil
}

const baselineListUsage = `benchreport baseline list - show baseline entries

Usage:
  benchreport baseline list [options]

Options:
  --file FILE   Baseline file (default: .benchmarks/baseline.jsonl)
`

func baselineList(args []string) error {
	fs := flag.NewFlagSet("baseline list", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, baselineListUsage) }

	baselineFile := fs.String("file", ".benchmarks/baseline.jsonl", "baseline file")

	parseErr := fs.Parse(args)
	if parseErr != nil {
		return fmt.Errorf("parse flags: %w", parseErr)
	}

	baselines, err := loadBaselineSet(*baselineFile)
	if err != nil {
		return fmt.Errorf("loading baseline file: %w", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprint(w, "#\tTimestamp\tGit\tRun Dir\n")
	fmt.Fprint(w, "-\t---------\t---\t-------\n")

	for idx := range baselines {
		baseline := &baselines[idx]

		rev := baseline.Git.Rev
		if len(rev) > 8 {
			rev = rev[:8]
		}

		fmt.Fprintf(w, "%d\t%s\t%s\t%s\n", idx+1, baseline.Timestamp, rev, baseline.RunDir)
	}

	flushErr := w.Flush()
	if flushErr != nil {
		return fmt.Errorf("flush output: %w", flushErr)
	}

	return nil
}

func loadSummaryFromPath(path string) (Summary, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Summary{}, fmt.Errorf("stat %s: %w", path, err)
	}

	if info.IsDir() {
		path = filepath.Join(path, "summary.json")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return Summary{}, fmt.Errorf("read summary %s: %w", path, err)
	}

	var s Summary

	unmarshalErr := json.Unmarshal(data, &s)
	if unmarshalErr != nil {
		return Summary{}, fmt.Errorf("unmarshal summary %s: %w", path, unmarshalErr)
	}

	return s, nil
}

func loadBaselineSet(path string) ([]Summary, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return []Summary{}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("read baseline file: %w", err)
	}

	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return []Summary{}, nil
	}

	if strings.HasPrefix(trimmed, "[") {
		var list []Summary

		err := json.Unmarshal([]byte(trimmed), &list)
		if err != nil {
			return nil, fmt.Errorf("parsing baseline array: %w", err)
		}

		return list, nil
	}

	var single Summary

	singleErr := json.Unmarshal([]byte(trimmed), &single)
	if singleErr == nil {
		return []Summary{single}, nil
	}

	return parseJSONLSummaries(trimmed)
}

func parseJSONLSummaries(data string) ([]Summary, error) {
	var summaries []Summary

	for line := range strings.SplitSeq(data, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var s Summary

		err := json.Unmarshal([]byte(line), &s)
		if err != nil {
			return nil, fmt.Errorf("parsing baseline line: %w", err)
		}

		summaries = append(summaries, s)
	}

	return summaries, nil
}

func writeBaselineSet(path string, baselines []Summary) error {
	err := os.MkdirAll(filepath.Dir(path), 0o755)
	if err != nil {
		return fmt.Errorf("create baseline directory: %w", err)
	}

	lines := make([]string, 0, len(baselines))
	for idx := range baselines {
		baseline := &baselines[idx]

		encoded, err := marshalJSON(baseline, false)
		if err != nil {
			return fmt.Errorf("marshal baseline: %w", err)
		}

		lines = append(lines, string(encoded))
	}

	content := strings.Join(lines, "\n")
	if content != "" {
		content += "\n"
	}

	writeErr := os.WriteFile(path, []byte(content), 0o644)
	if writeErr != nil {
		return fmt.Errorf("write baseline file: %w", writeErr)
	}

	return nil
}

func trimToKeep(baselines []Summary, keep int) []Summary {
	if keep <= 0 || len(baselines) <= keep {
		return baselines
	}

	return baselines[len(baselines)-keep:]
}
