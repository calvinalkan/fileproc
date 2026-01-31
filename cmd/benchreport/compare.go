package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
)

const compareUsage = `benchreport compare - compare benchmark results for regression detection

Usage:
  benchreport compare [options]

Options:
  --against MODE    Comparison mode: prev, avg, baseline (default: prev)
  --n N             Runs to average for avg/baseline (avg = last N vs prev N; baseline = last N history vs baseline avg)
  --fail-above PCT  Exit with error if regression exceeds PCT percent
  --filter SIZES    Comma-separated sizes to filter on (e.g. 100k,1m)
  --process NAME    Benchmark process to compare: bytes | read | stat | all (default: all)
  --history FILE    Path to history.jsonl (default: .benchmarks/history.jsonl)
  --baseline FILE   Baseline file (default: .benchmarks/baseline.jsonl)
  --json            Output as JSON instead of table
  -h, --help        Show this help

Comparison Modes:
  prev      Compare latest vs previous run in history (ignores -n)
  avg       Compare avg of last N runs vs avg of previous N runs
  baseline  Compare avg of last N runs vs baseline average

Examples:
  # Compare latest vs previous run
  benchreport compare

  # Compare avg of last 5 runs vs avg of previous 5 runs
  benchreport compare --against avg --n 5

  # Compare avg of last 5 runs vs baseline average
  benchreport compare --against baseline --n 5

  # Compare latest run vs baseline average
  benchreport compare --against baseline --n 1

  # Focus only on large datasets
  benchreport compare --filter 100k,1m
`

type CompareResult struct {
	Name          string  `json:"name"`
	LatestMeanMs  float64 `json:"latest_mean_ms"`
	TargetMeanMs  float64 `json:"target_mean_ms"`
	MeanChangePct float64 `json:"mean_change_pct"`
	LatestFPS     float64 `json:"latest_fps"`
	TargetFPS     float64 `json:"target_fps"`
	FPSChangePct  float64 `json:"fps_change_pct"`
}

type Comparison struct {
	LatestDesc      string          `json:"latest_desc,omitempty"`
	LatestTS        string          `json:"latest_ts"`
	LatestRev       string          `json:"latest_rev"`
	TargetDesc      string          `json:"target_desc"`
	TargetTS        string          `json:"target_ts"`
	Benchmarks      []CompareResult `json:"benchmarks"`
	WorstRegression float64         `json:"worst_regression"`
}

func runCompare(args []string) error {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, compareUsage) }

	against := fs.String("against", "prev", "comparison mode: prev, avg, baseline")
	n := fs.Int("n", 5, "number of runs to average (avg/baseline)")
	failAbove := fs.Float64("fail-above", 0, "fail if regression exceeds this percent")
	filter := fs.String("filter", "", "comma-separated sizes to filter on")
	process := fs.String("process", "all", "benchmark process to compare: bytes | read | stat | all")
	historyFile := fs.String("history", ".benchmarks/history.jsonl", "path to history file")
	baselineFile := fs.String("baseline", ".benchmarks/baseline.jsonl", "path to baseline file")
	outputJSON := fs.Bool("json", false, "output as JSON")

	parseErr := fs.Parse(args)
	if parseErr != nil {
		return fmt.Errorf("parse flags: %w", parseErr)
	}

	return compare(*against, *n, *failAbove, *filter, *process, *historyFile, *baselineFile, *outputJSON)
}

func compare(against string, n int, failAbove float64, filter, process, historyFile, baselineFile string, outputJSON bool) error {
	// Load history
	history, err := loadHistory(historyFile)
	if err != nil {
		return fmt.Errorf("loading history: %w", err)
	}

	if len(history) == 0 {
		return fmt.Errorf("no runs in history file: %s (run bench_regress.sh first)", historyFile)
	}

	if n < 1 {
		return fmt.Errorf("n must be >= 1 (got %d)", n)
	}

	latest := history[len(history)-1]
	latestDesc := "latest run"

	// Get target based on mode
	var (
		target     Summary
		targetDesc string
	)

	switch against {
	case "prev":
		if len(history) < 2 {
			return fmt.Errorf("need at least 2 runs in history to compare with prev (have %d)", len(history))
		}

		target = history[len(history)-2]
		targetDesc = "previous run"

	case "avg":
		if len(history) < n*2 {
			return fmt.Errorf("need at least %d runs in history for avg comparison (have %d)", n*2, len(history))
		}

		latest = averageSummaries(history[len(history)-n:])
		latestDesc = fmt.Sprintf("avg of last %d runs", n)
		latest.Timestamp = ""

		available := history[:len(history)-n]
		toAvg := available[len(available)-n:]
		target = averageSummaries(toAvg)
		targetDesc = fmt.Sprintf("avg of previous %d runs", n)
		target.Timestamp = ""

	case "baseline":
		baselines, loadErr := loadBaselineSet(baselineFile)
		if loadErr != nil {
			return fmt.Errorf("loading baseline: %w (create with: benchreport baseline add .benchmarks/regress-<timestamp> --file %s)", loadErr, baselineFile)
		}

		if len(baselines) == 0 {
			return fmt.Errorf("baseline file is empty: %s", baselineFile)
		}

		latestCount := min(n, len(history))
		if latestCount > 1 {
			latest = averageSummaries(history[len(history)-latestCount:])
			latestDesc = fmt.Sprintf("avg of last %d runs", latestCount)
			latest.Timestamp = ""
		}

		target = averageSummaries(baselines)
		targetDesc = fmt.Sprintf("baseline avg (%d runs)", len(baselines))
		target.Timestamp = ""

	default:
		return fmt.Errorf("unknown mode: %s (expected: prev, avg, baseline)", against)
	}

	// Build size filter
	var filterSizes []string
	if filter != "" {
		filterSizes = strings.Split(filter, ",")
	}

	// Compare
	comparison, err := buildComparison(&latest, &target, targetDesc, filterSizes, process)
	if err != nil {
		return err
	}

	comparison.LatestDesc = latestDesc

	if outputJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")

		err := enc.Encode(comparison)
		if err != nil {
			return fmt.Errorf("encode comparison: %w", err)
		}

		return nil
	}

	// Print table
	printErr := printComparison(&comparison)
	if printErr != nil {
		return printErr
	}

	// Check regression threshold
	if failAbove > 0 {
		if comparison.WorstRegression > failAbove {
			fmt.Printf("\n❌ REGRESSION DETECTED: worst change is +%.1f%% (threshold: %.1f%%)\n",
				comparison.WorstRegression, failAbove)

			return fmt.Errorf("regression detected: worst change is +%.1f%% (threshold: %.1f%%)",
				comparison.WorstRegression, failAbove)
		}

		fmt.Printf("\n✓ No significant regression (worst: +%.1f%%, threshold: %.1f%%)\n",
			comparison.WorstRegression, failAbove)
	}

	return nil
}

func loadHistory(path string) ([]Summary, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("history file not found: %s", path)
	}

	if err != nil {
		return nil, fmt.Errorf("read history file: %w", err)
	}

	var history []Summary

	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var s Summary

		err := json.Unmarshal([]byte(line), &s)
		if err != nil {
			return nil, fmt.Errorf("parsing history line: %w", err)
		}

		history = append(history, s)
	}

	return history, nil
}

func averageSummaries(summaries []Summary) Summary {
	if len(summaries) == 0 {
		return Summary{}
	}

	// Collect all benchmark names
	benchNames := make(map[string]bool)

	for idx := range summaries {
		summary := &summaries[idx]
		for name := range summary.Results {
			benchNames[name] = true
		}
	}

	// Average results
	avgResults := make(map[string]BenchResult)

	for name := range benchNames {
		var (
			sumMean, sumFPS float64
			count           int
		)

		for idx := range summaries {
			summary := &summaries[idx]
			if r, ok := summary.Results[name]; ok {
				sumMean += r.MeanMs
				sumFPS += r.FilesPerSec
				count++
			}
		}

		if count > 0 {
			avgResults[name] = BenchResult{
				MeanMs:      sumMean / float64(count),
				FilesPerSec: sumFPS / float64(count),
			}
		}
	}

	return Summary{
		Timestamp: fmt.Sprintf("avg(%d runs)", len(summaries)),
		Git:       GitInfo{Rev: "various"},
		Results:   avgResults,
	}
}

func buildComparison(latest, target *Summary, targetDesc string, filterSizes []string, process string) (Comparison, error) {
	// Get benchmarks for the requested process, optionally filtered
	var (
		benchmarks      []CompareResult
		worstRegression float64
	)

	process = strings.ToLower(strings.TrimSpace(process))
	if process == "" {
		process = "all"
	}

	if process != "bytes" && process != "read" && process != "stat" && process != "all" {
		return Comparison{}, fmt.Errorf("invalid process: %s (expected: bytes | read | stat | all)", process)
	}

	orderedBenches := benchmarksForProcess(process)

	for _, name := range orderedBenches {
		// Apply size filter
		if len(filterSizes) > 0 {
			matched := false

			for _, size := range filterSizes {
				if strings.Contains(name, "_"+size+"_") {
					matched = true

					break
				}
			}

			if !matched {
				continue
			}
		}

		latestR, hasLatest := latest.Results[name]

		targetR, hasTarget := target.Results[name]
		if !hasLatest || !hasTarget {
			continue
		}

		meanChange := pctChange(latestR.MeanMs, targetR.MeanMs)
		fpsChange := pctChange(latestR.FilesPerSec, targetR.FilesPerSec)

		if meanChange > worstRegression {
			worstRegression = meanChange
		}

		benchmarks = append(benchmarks, CompareResult{
			Name:          name,
			LatestMeanMs:  latestR.MeanMs,
			TargetMeanMs:  targetR.MeanMs,
			MeanChangePct: meanChange,
			LatestFPS:     latestR.FilesPerSec,
			TargetFPS:     targetR.FilesPerSec,
			FPSChangePct:  fpsChange,
		})
	}

	latestRev := latest.Git.Rev
	if len(latestRev) > 8 {
		latestRev = latestRev[:8]
	}

	return Comparison{
		LatestTS:        latest.Timestamp,
		LatestRev:       latestRev,
		TargetDesc:      targetDesc,
		TargetTS:        target.Timestamp,
		Benchmarks:      benchmarks,
		WorstRegression: worstRegression,
	}, nil
}

func benchmarksForProcess(process string) []string {
	if process == "all" {
		// Return all benchmarks grouped by size
		return []string{
			"flat_1k_bytes", "flat_1k_read", "flat_1k_stat",
			"nested1_1k_bytes", "nested1_1k_read", "nested1_1k_stat",
			"flat_5k_bytes", "flat_5k_read", "flat_5k_stat",
			"nested1_5k_bytes", "nested1_5k_read", "nested1_5k_stat",
			"flat_100k_bytes", "flat_100k_read", "flat_100k_stat",
			"nested1_100k_bytes", "nested1_100k_read", "nested1_100k_stat",
			"flat_1m_bytes", "flat_1m_read", "flat_1m_stat",
			"nested1_1m_bytes", "nested1_1m_read", "nested1_1m_stat",
		}
	}

	suffix := "_" + process
	ordered := []string{
		"flat_1k" + suffix, "nested1_1k" + suffix,
		"flat_5k" + suffix, "nested1_5k" + suffix,
		"flat_100k" + suffix, "nested1_100k" + suffix,
		"flat_1m" + suffix, "nested1_1m" + suffix,
	}

	return ordered
}

func printComparison(c *Comparison) error {
	fmt.Println()
	fmt.Println("============================================================")
	fmt.Println("Benchmark Comparison")
	fmt.Println("============================================================")
	fmt.Println()

	if c.LatestTS == "" {
		if c.LatestDesc != "" {
			fmt.Printf("Latest:  %s\n", c.LatestDesc)
		} else {
			fmt.Print("Latest:  avg\n")
		}
	} else {
		fmt.Printf("Latest:  %s (%s)\n", c.LatestTS, c.LatestRev)
	}

	if c.TargetTS == "" {
		fmt.Printf("Target:  %s\n", c.TargetDesc)
	} else {
		fmt.Printf("Target:  %s (%s)\n", c.TargetTS, c.TargetDesc)
	}

	fmt.Println()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprint(w, "Benchmark\tMean(ms)\tBase(ms)\tΔ%\n")
	fmt.Fprint(w, "--------\t--------\t--------\t--\n")

	for _, benchmark := range c.Benchmarks {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
			benchmark.Name,
			fmtMs(benchmark.LatestMeanMs),
			fmtMs(benchmark.TargetMeanMs),
			fmtPctColored(benchmark.MeanChangePct),
		)
	}

	err := w.Flush()
	if err != nil {
		return fmt.Errorf("flush output: %w", err)
	}

	fmt.Println()
	fmt.Println("Legend: Δ% = mean change from target (positive = slower/regression)")
	fmt.Println("        red >= +1% regression, green <= -1% improvement")

	return nil
}

func pctChange(newValue, oldValue float64) float64 {
	if oldValue == 0 {
		return 0
	}

	return (newValue - oldValue) / oldValue * 100
}

func fmtMs(v float64) string {
	if v == 0 {
		return "N/A"
	}

	return fmt.Sprintf("%.2f", v)
}

func fmtPct(v float64) string {
	if v > 0 {
		return fmt.Sprintf("+%.1f%%", v)
	}

	return fmt.Sprintf("%.1f%%", v)
}

func fmtPctColored(v float64) string {
	s := fmtPct(v)
	if _, ok := os.LookupEnv("NO_COLOR"); ok {
		return s
	}

	switch {
	case v >= 1.0:
		return "\x1b[31m" + s + "\x1b[0m"
	case v <= -1.0:
		return "\x1b[32m" + s + "\x1b[0m"
	default:
		return s
	}
}
