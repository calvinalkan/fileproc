// benchreport summarizes and compares benchmark results from bench_regress.sh.
//
// Usage:
//
//	benchreport summarize <run-dir>
//	benchreport compare [options]
//
// See 'benchreport <command> --help' for command-specific options.
package main

import (
	"fmt"
	"os"
)

const usage = `benchreport - summarize and compare benchmark results

Usage:
  benchreport <command> [options]

Commands:
  summarize   Generate summary.json from a benchmark run and append to history
  compare     Compare latest benchmark against previous/avg/baseline
  baseline    Manage baseline entries (add/list/prune)

Examples:
  # After bench_regress.sh completes, summarize the run
  benchreport summarize .benchmarks/regress-2026-01-17T17-44-10+0100

  # Compare latest run vs previous
  benchreport compare

  # Compare latest vs rolling average of last 5 runs
  benchreport compare --against avg --n 5

  # Compare vs baseline with regression threshold
  benchreport compare --against baseline --fail-above 5

  # Add a baseline entry from a run
  benchreport baseline add .benchmarks/regress-2026-01-17T17-44-10+0100

Run 'benchreport <command> --help' for command-specific help.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "summarize":
		err := runSummarize(args)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "compare":
		err := runCompare(args)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "baseline":
		err := runBaseline(args)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", cmd)
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}
}
