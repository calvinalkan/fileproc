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
  --n N             For avg mode, number of runs to average (default: 5)
  --fail-above PCT  Exit with error if regression exceeds PCT percent
  --focus SIZES     Comma-separated sizes to focus on (e.g. 100k,1m)
  --history FILE    Path to history.jsonl (default: .benchmarks/history.jsonl)
  --baseline FILE   Baseline file (default: .benchmarks/baseline.jsonl)
  --json            Output as JSON instead of table
  -h, --help        Show this help

Comparison Modes:
  prev      Compare latest vs previous run in history
  avg       Compare latest vs rolling average of last N runs
  baseline  Compare latest vs baseline file (avg if multiple entries)

Examples:
  # Compare latest vs previous run
  benchreport compare

  # Compare latest vs average of last 5 runs
  benchreport compare --against avg --n 5

  # Compare vs baseline, fail if any regression > 5%
  benchreport compare --against baseline --fail-above 5

  # Focus only on large datasets
  benchreport compare --focus 100k,1m
`

type CompareResult struct {
	Name           string  `json:"name"`
	LatestMeanMs   float64 `json:"latest_mean_ms"`
	TargetMeanMs   float64 `json:"target_mean_ms"`
	MeanChangePct  float64 `json:"mean_change_pct"`
	LatestFPS      float64 `json:"latest_fps"`
	TargetFPS      float64 `json:"target_fps"`
	FPSChangePct   float64 `json:"fps_change_pct"`
	LatestRatio    float64 `json:"latest_ratio"`
	TargetRatio    float64 `json:"target_ratio"`
	RatioChangePct float64 `json:"ratio_change_pct"`
}

type Comparison struct {
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
	n := fs.Int("n", 5, "number of runs for avg mode")
	failAbove := fs.Float64("fail-above", 0, "fail if regression exceeds this percent")
	focus := fs.String("focus", "", "comma-separated sizes to focus on")
	historyFile := fs.String("history", ".benchmarks/history.jsonl", "path to history file")
	baselineFile := fs.String("baseline", ".benchmarks/baseline.jsonl", "path to baseline file")
	outputJSON := fs.Bool("json", false, "output as JSON")

	parseErr := fs.Parse(args)
	if parseErr != nil {
		return fmt.Errorf("parse flags: %w", parseErr)
	}

	return compare(*against, *n, *failAbove, *focus, *historyFile, *baselineFile, *outputJSON)
}

func compare(against string, n int, failAbove float64, focus, historyFile, baselineFile string, outputJSON bool) error {
	// Load history
	history, err := loadHistory(historyFile)
	if err != nil {
		return fmt.Errorf("loading history: %w", err)
	}

	if len(history) == 0 {
		return fmt.Errorf("no runs in history file: %s (run bench_regress.sh first)", historyFile)
	}

	latest := history[len(history)-1]

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
		if len(history) < 2 {
			return fmt.Errorf("need at least 2 runs in history for avg comparison (have %d)", len(history))
		}
		// Average last N runs excluding latest
		available := history[:len(history)-1]
		count := min(n, len(available))
		toAvg := available[len(available)-count:]
		target = averageSummaries(toAvg)
		targetDesc = fmt.Sprintf("avg of last %d runs", count)

	case "baseline":
		baselines, err := loadBaselineSet(baselineFile)
		if err != nil {
			return fmt.Errorf("loading baseline: %w (create with: benchreport baseline add .benchmarks/regress-<timestamp> --file %s)", err, baselineFile)
		}

		if len(baselines) == 0 {
			return fmt.Errorf("baseline file is empty: %s", baselineFile)
		}

		if len(baselines) == 1 {
			target = baselines[0]
			targetDesc = "baseline"
		} else {
			target = averageSummaries(baselines)
			targetDesc = fmt.Sprintf("baseline avg (%d runs)", len(baselines))
		}

	default:
		return fmt.Errorf("unknown mode: %s (expected: prev, avg, baseline)", against)
	}

	// Build focus filter
	var focusSizes []string
	if focus != "" {
		focusSizes = strings.Split(focus, ",")
	}

	// Compare
	comparison := buildComparison(&latest, &target, targetDesc, focusSizes)

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

	// Average ratios
	ratioNames := []string{"flat_1k", "nested1_1k", "flat_5k", "nested1_5k", "flat_100k", "nested1_100k", "flat_1m", "nested1_1m"}
	avgRatios := make(map[string]float64)

	for _, name := range ratioNames {
		var (
			sum   float64
			count int
		)

		for idx := range summaries {
			summary := &summaries[idx]
			if r, ok := summary.Ratios[name]; ok && r > 0 {
				sum += r
				count++
			}
		}

		if count > 0 {
			avgRatios[name] = sum / float64(count)
		}
	}

	return Summary{
		Timestamp: fmt.Sprintf("avg(%d runs)", len(summaries)),
		Git:       GitInfo{Rev: "various"},
		Results:   avgResults,
		Ratios:    avgRatios,
	}
}

func buildComparison(latest, target *Summary, targetDesc string, focusSizes []string) Comparison {
	// Get frontmatter benchmarks, optionally filtered
	var (
		benchmarks      []CompareResult
		worstRegression float64
	)

	// Order matters for nice output
	orderedBenches := []string{
		"flat_1k_frontmatter", "nested1_1k_frontmatter",
		"flat_5k_frontmatter", "nested1_5k_frontmatter",
		"flat_100k_frontmatter", "nested1_100k_frontmatter",
		"flat_1m_frontmatter", "nested1_1m_frontmatter",
	}

	for _, name := range orderedBenches {
		// Apply focus filter
		if len(focusSizes) > 0 {
			matched := false

			for _, size := range focusSizes {
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

		// Extract prefix for ratio lookup (e.g., "flat_1k")
		prefix := strings.TrimSuffix(name, "_frontmatter")

		latestRatio := latest.Ratios[prefix]
		targetRatio := target.Ratios[prefix]

		meanChange := pctChange(latestR.MeanMs, targetR.MeanMs)
		fpsChange := pctChange(latestR.FilesPerSec, targetR.FilesPerSec)
		ratioChange := pctChange(latestRatio, targetRatio)

		if meanChange > worstRegression {
			worstRegression = meanChange
		}

		benchmarks = append(benchmarks, CompareResult{
			Name:           name,
			LatestMeanMs:   latestR.MeanMs,
			TargetMeanMs:   targetR.MeanMs,
			MeanChangePct:  meanChange,
			LatestFPS:      latestR.FilesPerSec,
			TargetFPS:      targetR.FilesPerSec,
			FPSChangePct:   fpsChange,
			LatestRatio:    latestRatio,
			TargetRatio:    targetRatio,
			RatioChangePct: ratioChange,
		})
	}

	latestRev := latest.Git.Rev
	if len(latestRev) > 8 {
		latestRev = latestRev[:8]
	}

	targetTS := target.Timestamp
	if targetTS == "" {
		targetTS = targetDesc
	}

	return Comparison{
		LatestTS:        latest.Timestamp,
		LatestRev:       latestRev,
		TargetDesc:      targetDesc,
		TargetTS:        targetTS,
		Benchmarks:      benchmarks,
		WorstRegression: worstRegression,
	}
}

func printComparison(c *Comparison) error {
	fmt.Println()
	fmt.Println("============================================================")
	fmt.Println("Benchmark Comparison")
	fmt.Println("============================================================")
	fmt.Println()
	fmt.Printf("Latest:  %s (%s)\n", c.LatestTS, c.LatestRev)
	fmt.Printf("Target:  %s (%s)\n", c.TargetTS, c.TargetDesc)
	fmt.Println()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprint(w, "Benchmark\tMean(ms)\tBase(ms)\tΔ%%\tfiles/s\tΔ%%\tRatio\tΔ%%\n")
	fmt.Fprint(w, "--------\t--------\t--------\t--\t-------\t--\t-----\t--\n")

	for _, benchmark := range c.Benchmarks {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			benchmark.Name,
			fmtMs(benchmark.LatestMeanMs),
			fmtMs(benchmark.TargetMeanMs),
			fmtPct(benchmark.MeanChangePct),
			fmtFPS(benchmark.LatestFPS),
			fmtPct(benchmark.FPSChangePct),
			fmtRatio(benchmark.LatestRatio),
			fmtPct(benchmark.RatioChangePct),
		)
	}

	err := w.Flush()
	if err != nil {
		return fmt.Errorf("flush output: %w", err)
	}

	fmt.Println()
	fmt.Println("Legend: Δ% = change from target (positive = slower/regression)")
	fmt.Println("        Ratio = frontmatter_mean / noop_mean (lower = better)")

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

func fmtFPS(v float64) string {
	if v == 0 {
		return "N/A"
	}

	if v >= 1_000_000 {
		return fmt.Sprintf("%.1fM", v/1_000_000)
	}

	if v >= 1_000 {
		return fmt.Sprintf("%.1fK", v/1_000)
	}

	return fmt.Sprintf("%.0f", v)
}

func fmtRatio(v float64) string {
	if v == 0 {
		return "N/A"
	}

	return fmt.Sprintf("%.3f", v)
}

func fmtPct(v float64) string {
	if v > 0 {
		return fmt.Sprintf("+%.1f%%", v)
	}

	return fmt.Sprintf("%.1f%%", v)
}
