package fileproc_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"fileproc"
)

func Test_Process_Processes_All_Files_When_Pipelined_With_Multiple_ReadDirBatches(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	const workers = 4

	pad := strings.Repeat("x", testNamePad)

	wantPaths := make([]string, 0, testNumFilesMed)
	for i := range testNumFilesMed {
		name := fmt.Sprintf("f-%04d-%s.txt", i, pad)
		writeFile(t, root, name, []byte("x"))
		wantPaths = append(wantPaths, name)
	}

	opts := fileproc.Options{
		Workers:            workers,
		SmallFileThreshold: 1, // force pipelined mode
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]int, testNumFilesMed)
		ok   struct{}
	)

	results, errs := fileproc.Process(t.Context(), root, func(path, _ []byte) (*struct{}, error) {
		mu.Lock()

		seen[string(path)]++

		mu.Unlock()

		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), wantPaths)

	mu.Lock()
	defer mu.Unlock()

	if len(seen) != testNumFilesMed {
		t.Fatalf("unexpected callback set size: got=%d want=%d", len(seen), testNumFilesMed)
	}

	for p, c := range seen {
		if c != 1 {
			t.Fatalf("file processed %d times: %s", c, p)
		}
	}
}

func Test_ProcessReader_Processes_All_Files_When_Pipelined_With_Multiple_ReadDirBatches(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	const workers = 4

	pad := strings.Repeat("x", testNamePad)

	wantPaths := make([]string, 0, testNumFilesMed)
	for i := range testNumFilesMed {
		name := fmt.Sprintf("f-%04d-%s.txt", i, pad)
		writeFile(t, root, name, []byte("x"))
		wantPaths = append(wantPaths, name)
	}

	opts := fileproc.Options{
		Workers:            workers,
		SmallFileThreshold: 1, // force pipelined mode
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]int, testNumFilesMed)
		ok   struct{}
	)

	results, errs := fileproc.ProcessReader(t.Context(), root, func(path []byte, r io.Reader) (*struct{}, error) {
		// Touch the stream so we cover the reader path.
		_, _ = io.CopyN(io.Discard, r, 1)

		mu.Lock()

		seen[string(path)]++

		mu.Unlock()

		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), wantPaths)

	mu.Lock()
	defer mu.Unlock()

	if len(seen) != testNumFilesMed {
		t.Fatalf("unexpected callback set size: got=%d want=%d", len(seen), testNumFilesMed)
	}

	for p, c := range seen {
		if c != 1 {
			t.Fatalf("file processed %d times: %s", c, p)
		}
	}
}

// Sanity check that ProcessReader pipelined tests don't accidentally rely on cancellation.
func Test_ProcessReader_Completes_When_Pipelined_Without_Cancellation(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	const workers = 4

	pad := strings.Repeat("x", testNamePad)

	for i := range testNumFilesMed {
		name := fmt.Sprintf("f-%04d-%s.txt", i, pad)
		writeFile(t, root, name, []byte("x"))
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var ok struct{}

	_, errs := fileproc.ProcessReader(ctx, root, func(_ []byte, _ io.Reader) (*struct{}, error) {
		return &ok, nil
	}, fileproc.Options{Workers: workers, SmallFileThreshold: 1})

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
}
