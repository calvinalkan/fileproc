// Package main is a fast ticket dataset generator.
//
// Examples:
//
//	go run ./cmd/ticketgen --out .data/tickets_100k --files 100000
//	go run ./cmd/ticketgen --out .data/tickets_deep --files 2000000 --layout deep
//	go run ./cmd/ticketgen --out .data/tickets_shallow --files 2000000 --layout shallow
//	go run ./cmd/ticketgen --out .data/tickets_fanout --files 2000000 --layout fanout --depth 3 --fanout 60
//
// Notes:
// - Generates markdown-like files with YAML frontmatter.
// - Frontmatter is intentionally simple: it starts with "---\n" and ends at the next line "---".
// - Uses threads = available CPU cores by default.
package main

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

type Layout int

const (
	LayoutFlat Layout = iota
	LayoutShallow
	LayoutDeep
	LayoutFanout
)

type Args struct {
	Out           string
	Files         uint64
	Threads       int
	Ext           string
	Layout        Layout
	StartYear     int
	Years         int
	ShallowDirs   uint64
	DaysPerMonth  int
	FrontLines    int
	FrontLinesMax int
	BodyBytes     int
	BodyBytesMax  int
	Depth         int
	Fanout        int
	ProgressEvery uint64
}

func main() {
	args, err := parseArgs()
	if err != nil {
		if err.Error() != "" {
			fmt.Fprintln(os.Stderr, err)
		}

		printUsage()
		os.Exit(2)
	}

	runErr := run(args)
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", runErr)
		os.Exit(1)
	}
}

func run(args *Args) error {
	if args.Files == 0 {
		return errors.New("--files must be > 0")
	}

	if args.Threads == 0 {
		return errors.New("--threads must be > 0")
	}

	mkdirErr := os.MkdirAll(args.Out, 0o750)
	if mkdirErr != nil {
		return fmt.Errorf("mkdir --out %s: %w", args.Out, mkdirErr)
	}

	start := time.Now()

	var processedFiles atomic.Uint64

	var waitGroup sync.WaitGroup

	errCh := make(chan error, args.Threads)

	for threadID := range args.Threads {
		startIdx, endIdx, rangeErr := splitRange(args.Files, args.Threads, threadID)
		if rangeErr != nil {
			return rangeErr
		}

		startIdxCopy := startIdx
		endIdxCopy := endIdx

		waitGroup.Go(func() {
			workerErr := worker(args, startIdxCopy, endIdxCopy, &processedFiles)
			if workerErr != nil {
				errCh <- workerErr
			}
		})
	}

	waitGroup.Wait()
	close(errCh)

	for workerErr := range errCh {
		return workerErr
	}

	elapsed := time.Since(start)
	processedTotal := processedFiles.Load()
	perSec := float64(processedTotal) / elapsed.Seconds()
	fmt.Fprintf(os.Stderr, "done: files=%d elapsed=%v throughput=%.0f files/sec\n", processedTotal, elapsed, perSec)

	return nil
}

func worker(args *Args, startIdx, endIdx uint64, processedFiles *atomic.Uint64) error {
	createdDirs := make(map[string]struct{})

	seed := seedForThread(startIdx)
	rng := NewXorShift64(seed)

	buf := bytes.NewBuffer(make([]byte, 0, args.BodyBytesMax+4096))
	lastProgress := time.Now()

	for i := startIdx; i < endIdx; i++ {
		dirPath, dirErr := dirForTicket(args, i)
		if dirErr != nil {
			return dirErr
		}

		if _, ok := createdDirs[dirPath]; !ok {
			mkdirErr := os.MkdirAll(dirPath, 0o750)
			if mkdirErr != nil {
				return fmt.Errorf("mkdir %s: %w", dirPath, mkdirErr)
			}

			createdDirs[dirPath] = struct{}{}
		}

		filename := ticketFilename(args, i)
		filePath := filepath.Join(dirPath, filename)

		buf.Reset()
		writeTicket(buf, args, rng, i)

		writeErr := os.WriteFile(filePath, buf.Bytes(), 0o600)
		if writeErr != nil {
			return fmt.Errorf("write %s: %w", filePath, writeErr)
		}

		processedCount := processedFiles.Add(1)
		if args.ProgressEvery != 0 && processedCount%args.ProgressEvery == 0 {
			deltaTime := time.Since(lastProgress)
			if deltaTime > 200*time.Millisecond {
				fmt.Fprintf(os.Stderr, "  %d files...\n", processedCount)

				lastProgress = time.Now()
			}
		}
	}

	return nil
}

func splitRange(total uint64, threads int, threadID int) (uint64, uint64, error) {
	if threads <= 0 {
		return 0, 0, errors.New("threads must be > 0")
	}

	if threadID < 0 || threadID >= threads {
		return 0, 0, fmt.Errorf("thread id out of range: %d", threadID)
	}

	threadsU64 := uint64(threads)
	threadIDU64 := uint64(threadID)
	start := (total * threadIDU64) / threadsU64
	end := (total * (threadIDU64 + 1)) / threadsU64

	return start, end, nil
}

func seedForThread(startIdx uint64) uint64 {
	// Deterministic seed derived from the start index.
	// Keeps generation reproducible and avoids time-based conversions.
	seed := startIdx ^ (startIdx * 0x9E3779B97F4A7C15) ^ 0xD1B54A32D192ED03
	if seed == 0 {
		seed = 0x123456789abcdef0
	}

	return seed
}

func ticketFilename(args *Args, i uint64) string {
	return fmt.Sprintf("ticket-%09d%s", i, args.Ext)
}

func dirForTicket(args *Args, ticketIndex uint64) (string, error) {
	switch args.Layout {
	case LayoutFlat:
		return args.Out, nil

	case LayoutShallow:
		dirs := args.ShallowDirs
		if dirs == 0 {
			years := max(args.Years, 1)

			if years < 0 {
				return "", errors.New("--years must be >= 0")
			}

			dirs = uint64(years) * 12
		}

		filesPerDir := (args.Files + dirs - 1) / dirs

		dirIdx := uint64(0)
		if filesPerDir != 0 {
			dirIdx = ticketIndex / filesPerDir
		}

		yearDeltaU64 := dirIdx / 12

		if yearDeltaU64 > uint64(math.MaxInt) {
			return "", fmt.Errorf("year delta out of range: %d", yearDeltaU64)
		}

		yearDelta := int(yearDeltaU64)
		if args.StartYear > math.MaxInt-yearDelta {
			return "", fmt.Errorf("year out of range: start=%d delta=%d", args.StartYear, yearDelta)
		}

		year := args.StartYear + yearDelta
		monthU64 := dirIdx % 12

		if monthU64 > uint64(math.MaxInt-1) {
			return "", fmt.Errorf("month out of range: %d", monthU64)
		}

		month := 1 + int(monthU64)

		return filepath.Join(args.Out, fmt.Sprintf("%04d-%02d", year, month)), nil

	case LayoutDeep:
		years := max(args.Years, 1)

		if years < 0 {
			return "", errors.New("--years must be >= 0")
		}

		daysPerMonth := max(args.DaysPerMonth, 1)

		if daysPerMonth < 0 {
			return "", errors.New("--days-per-month must be >= 0")
		}

		dpm := uint64(daysPerMonth)
		dirs := uint64(years) * 12 * dpm
		filesPerDir := (args.Files + dirs - 1) / dirs

		dirIdx := uint64(0)
		if filesPerDir != 0 {
			dirIdx = ticketIndex / filesPerDir
		}

		monthDays := 12 * dpm
		yearDeltaU64 := dirIdx / monthDays

		if yearDeltaU64 > uint64(math.MaxInt) {
			return "", fmt.Errorf("year delta out of range: %d", yearDeltaU64)
		}

		yearDelta := int(yearDeltaU64)
		if args.StartYear > math.MaxInt-yearDelta {
			return "", fmt.Errorf("year out of range: start=%d delta=%d", args.StartYear, yearDelta)
		}

		year := args.StartYear + yearDelta
		rem := dirIdx % monthDays
		monthU64 := rem / dpm
		dayU64 := rem % dpm

		if monthU64 > uint64(math.MaxInt-1) {
			return "", fmt.Errorf("month out of range: %d", monthU64)
		}

		if dayU64 > uint64(math.MaxInt-1) {
			return "", fmt.Errorf("day out of range: %d", dayU64)
		}

		month := 1 + int(monthU64)
		day := 1 + int(dayU64)

		return filepath.Join(args.Out, fmt.Sprintf("%04d", year), fmt.Sprintf("%02d", month), fmt.Sprintf("%02d", day)), nil

	case LayoutFanout:
		depth := max(args.Depth, 1)
		fanout := max(args.Fanout, 2)

		if fanout < 0 {
			return "", errors.New("--fanout must be >= 0")
		}

		base := uint64(fanout)
		index := ticketIndex

		pathSoFar := args.Out

		for level := range depth {
			digit := index % base
			index /= base
			pathSoFar = filepath.Join(pathSoFar, fmt.Sprintf("l%02d_%03d", level, digit))
		}

		return pathSoFar, nil

	default:
		return "", fmt.Errorf("unknown layout: %d", args.Layout)
	}
}

func writeTicket(buf *bytes.Buffer, args *Args, rng *XorShift64, ticketNum uint64) {
	// Frontmatter
	buf.WriteString("---\n")

	// Required fields
	pushKV(buf, "id", fmt.Sprintf("TCK-%09d", ticketNum))
	pushKV(buf, "uuid", randomToken(rng, 26))

	var status string

	switch rng.Next() % 3 {
	case 0:
		status = "open"
	case 1:
		status = "closed"
	default:
		status = "pending"
	}

	pushKV(buf, "status", status)

	priority := rng.Next() % 5
	pushKV(buf, "priority", strconv.FormatUint(priority, 10))

	// Additional synthetic key/value lines
	extraMin := args.FrontLines
	extraMax := max(args.FrontLinesMax, extraMin)

	extra := extraMin
	if extraMax != extraMin {
		spanInt := extraMax - extraMin + 1
		if spanInt <= 0 {
			spanInt = 1
		}

		spanU64 := uint64(spanInt)
		offsetU64 := rng.Next() % spanU64

		if offsetU64 > uint64(math.MaxInt) {
			offsetU64 = 0
		}

		extra = extraMin + int(offsetU64)
	}

	for lineIdx := range extra {
		key := fmt.Sprintf("k%02d", lineIdx)
		value := fmt.Sprintf("v%d", rng.Next()&0xffff)
		pushKV(buf, key, value)
	}

	buf.WriteString("---\n\n")

	// Body
	fmt.Fprintf(buf, "# Ticket %d\n\n", ticketNum)

	bodyMin := args.BodyBytes
	bodyMax := max(args.BodyBytesMax, bodyMin)

	bodyLen := bodyMin
	if bodyMax != bodyMin {
		spanInt := bodyMax - bodyMin + 1
		if spanInt <= 0 {
			spanInt = 1
		}

		spanU64 := uint64(spanInt)
		offsetU64 := rng.Next() % spanU64

		if offsetU64 > uint64(math.MaxInt) {
			offsetU64 = 0
		}

		bodyLen = bodyMin + int(offsetU64)
	}

	// Fill body with repetitive pattern + newlines
	remaining := bodyLen
	for remaining > 0 {
		chunk := min(remaining, 80)
		for range chunk {
			buf.WriteByte('x')
		}

		buf.WriteByte('\n')

		remaining -= chunk
	}
}

func pushKV(buf *bytes.Buffer, key, value string) {
	buf.WriteString(key)
	buf.WriteString(": ")
	buf.WriteString(value)
	buf.WriteByte('\n')
}

const tokenAlphabet32 = "abcdefghijklmnopqrstuvwxyz234567" // 32 chars

func randomToken(rng *XorShift64, length int) string {
	if length <= 0 {
		return ""
	}

	out := make([]byte, 0, length)

	bitsLeft := 0
	bits := uint64(0)

	for range length {
		if bitsLeft < 5 {
			bits = rng.Next()
			bitsLeft = 64
		}

		out = append(out, tokenAlphabet32[bits&31])
		bits >>= 5
		bitsLeft -= 5
	}

	return string(out)
}

// XorShift64 is a fast PRNG.
type XorShift64 struct {
	state uint64
}

func NewXorShift64(seed uint64) *XorShift64 {
	state := seed
	if state == 0 {
		state = 0x123456789abcdef0
	}

	return &XorShift64{state: state}
}

func (rng *XorShift64) Next() uint64 {
	state := rng.state
	state ^= state >> 12
	state ^= state << 25
	state ^= state >> 27
	rng.state = state

	return state * 0x2545F4914F6CDD1D
}

func parseArgs() (*Args, error) {
	args := os.Args[1:]

	var (
		out      string
		files    uint64
		filesSet bool
	)

	threads := runtime.NumCPU()
	ext := ".md"
	layout := LayoutFlat

	startYear := 2020
	years := 5

	var shallowDirs uint64

	daysPerMonth := 28

	frontLines := 8
	frontLinesMax := 8

	bodyBytes := 256
	bodyBytesMax := 256

	depth := 3
	fanout := 60

	progressEvery := uint64(100_000)

	i := 0
	for i < len(args) {
		arg := args[i]
		i++

		switch arg {
		case "--out":
			if i >= len(args) {
				return nil, errors.New("missing value for --out")
			}

			out = args[i]
			i++

		case "--files":
			if i >= len(args) {
				return nil, errors.New("missing value for --files")
			}

			v, err := parseHumanU64(args[i])
			if err != nil {
				return nil, err
			}

			files = v
			filesSet = true
			i++

		case "--threads":
			if i >= len(args) {
				return nil, errors.New("missing value for --threads")
			}

			v, err := parseHumanU64(args[i])
			if err != nil {
				return nil, err
			}

			threads = int(v)
			i++

		case "--ext":
			if i >= len(args) {
				return nil, errors.New("missing value for --ext")
			}

			ext = args[i]
			i++

		case "--layout":
			if i >= len(args) {
				return nil, errors.New("missing value for --layout")
			}

			switch args[i] {
			case "flat":
				layout = LayoutFlat
			case "shallow":
				layout = LayoutShallow
			case "deep":
				layout = LayoutDeep
			case "fanout":
				layout = LayoutFanout
			default:
				return nil, fmt.Errorf("invalid --layout %q", args[i])
			}

			i++

		case "--start-year":
			if i >= len(args) {
				return nil, errors.New("missing value for --start-year")
			}

			v, err := strconv.Atoi(args[i])
			if err != nil {
				return nil, fmt.Errorf("invalid --start-year: %w", err)
			}

			startYear = v
			i++

		case "--years":
			if i >= len(args) {
				return nil, errors.New("missing value for --years")
			}

			v, err := strconv.Atoi(args[i])
			if err != nil {
				return nil, fmt.Errorf("invalid --years: %w", err)
			}

			years = v
			i++

		case "--shallow-dirs":
			if i >= len(args) {
				return nil, errors.New("missing value for --shallow-dirs")
			}

			v, err := parseHumanU64(args[i])
			if err != nil {
				return nil, err
			}

			shallowDirs = v
			i++

		case "--days-per-month":
			if i >= len(args) {
				return nil, errors.New("missing value for --days-per-month")
			}

			v, err := strconv.Atoi(args[i])
			if err != nil {
				return nil, fmt.Errorf("invalid --days-per-month: %w", err)
			}

			daysPerMonth = v
			i++

		case "--front-lines":
			if i >= len(args) {
				return nil, errors.New("missing value for --front-lines")
			}

			v, err := parseHumanU64(args[i])
			if err != nil {
				return nil, err
			}

			frontLines = int(v)
			frontLinesMax = frontLines
			i++

		case "--front-lines-max":
			if i >= len(args) {
				return nil, errors.New("missing value for --front-lines-max")
			}

			v, err := parseHumanU64(args[i])
			if err != nil {
				return nil, err
			}

			frontLinesMax = int(v)
			i++

		case "--body-bytes":
			if i >= len(args) {
				return nil, errors.New("missing value for --body-bytes")
			}

			v, err := parseHumanU64(args[i])
			if err != nil {
				return nil, err
			}

			bodyBytes = int(v)
			bodyBytesMax = bodyBytes
			i++

		case "--body-bytes-max":
			if i >= len(args) {
				return nil, errors.New("missing value for --body-bytes-max")
			}

			v, err := parseHumanU64(args[i])
			if err != nil {
				return nil, err
			}

			bodyBytesMax = int(v)
			i++

		case "--depth":
			if i >= len(args) {
				return nil, errors.New("missing value for --depth")
			}

			v, err := parseHumanU64(args[i])
			if err != nil {
				return nil, err
			}

			depth = int(v)
			i++

		case "--fanout":
			if i >= len(args) {
				return nil, errors.New("missing value for --fanout")
			}

			v, err := parseHumanU64(args[i])
			if err != nil {
				return nil, err
			}

			fanout = int(v)
			i++

		case "--progress":
			if i >= len(args) {
				return nil, errors.New("missing value for --progress")
			}

			v, err := parseHumanU64(args[i])
			if err != nil {
				return nil, err
			}

			progressEvery = v
			i++

		case "-h", "--help":
			return nil, errors.New("")

		default:
			return nil, fmt.Errorf("unknown argument: %s", arg)
		}
	}

	if out == "" {
		return nil, errors.New("missing --out")
	}

	if !filesSet {
		return nil, errors.New("missing --files")
	}

	if !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}

	return &Args{
		Out:           out,
		Files:         files,
		Threads:       threads,
		Ext:           ext,
		Layout:        layout,
		StartYear:     startYear,
		Years:         years,
		ShallowDirs:   shallowDirs,
		DaysPerMonth:  daysPerMonth,
		FrontLines:    frontLines,
		FrontLinesMax: frontLinesMax,
		BodyBytes:     bodyBytes,
		BodyBytesMax:  bodyBytesMax,
		Depth:         depth,
		Fanout:        fanout,
		ProgressEvery: progressEvery,
	}, nil
}

// parseHumanU64 parses human-friendly numbers like: 10k, 100k, 1m, 2.5m, 1g, 16ki, 64mib.
//
// Suffixes:
// - Decimal: k, m, g (and kb/mb/gb)
// - Binary:  ki, mi, gi (and kib/mib/gib).
func parseHumanU64(input string) (uint64, error) {
	normalized := strings.ToLower(strings.TrimSpace(input))

	normalized = strings.ReplaceAll(normalized, "_", "")
	if normalized == "" {
		return 0, errors.New("empty number")
	}

	split := 0

	for i, ch := range normalized {
		if unicode.IsDigit(ch) || ch == '.' {
			split = i + 1

			continue
		}

		break
	}

	if split == 0 {
		return 0, fmt.Errorf("invalid number: %s", input)
	}

	numStr := normalized[:split]
	suffix := normalized[split:]

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number: %s", input)
	}

	var mult float64

	switch suffix {
	case "":
		mult = 1.0
	case "k", "kb":
		mult = 1_000.0
	case "m", "mb":
		mult = 1_000_000.0
	case "g", "gb":
		mult = 1_000_000_000.0
	case "ki", "kib":
		mult = 1024.0
	case "mi", "mib":
		mult = 1024.0 * 1024.0
	case "gi", "gib":
		mult = 1024.0 * 1024.0 * 1024.0
	default:
		return 0, fmt.Errorf("invalid suffix in number: %s", input)
	}

	if num < 0 {
		return 0, fmt.Errorf("invalid number: %s", input)
	}

	val := num * mult
	if val < 0 || val > float64(^uint64(0)) {
		return 0, fmt.Errorf("number out of range: %s", input)
	}

	rounded := uint64(val + 0.5)

	return rounded, nil
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `Usage: ticketgen --out <dir> --files <n> [options]

Numbers can be human-friendly: 10k, 100k, 1m, 2.5m, 1g (and ki/mi/gi).

Options:
  --layout flat|shallow|deep|fanout   (default: flat)
  --threads N                        (default: CPU cores)
  --ext .md                          (default: .md)

Frontmatter size:
  --front-lines N                    extra kv lines (default: 8)
  --front-lines-max N                randomize up to N

Body size:
  --body-bytes N                     (default: 256)
  --body-bytes-max N                 randomize up to N

Deep/shallow date params:
  --start-year YYYY                  (default: 2020)
  --years N                          (default: 5)
  --shallow-dirs N                   (optional: override years*12 for layout=shallow)
  --days-per-month N                 (default: 28)

Fanout params:
  --depth N                          (default: 3)
  --fanout N                         (default: 60)

Progress:
  --progress N                       print every N files (default: 100000)`)
}
