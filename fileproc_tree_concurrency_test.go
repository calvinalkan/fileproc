package fileproc_test

import (
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/calvinalkan/fileproc"
)

func Test_Process_Processes_All_Files_When_Using_Recursive_Concurrent_Workers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	wantPaths := make([]string, 0, 128)

	// Root-level files.
	writeFile(t, root, "root1.txt", []byte("a"))
	writeFile(t, root, "root2.txt", []byte("b"))

	wantPaths = append(wantPaths, "root1.txt", "root2.txt")

	// Many subdirectories to exercise coordinator + multiple workers.
	for i := range 25 {
		dir := fmt.Sprintf("d%02d", i)
		file1 := filepath.Join(dir, fmt.Sprintf("f%02d.txt", i))
		file2 := filepath.Join(dir, "nested", "n.txt")

		writeFile(t, root, file1, []byte("x"))
		writeFile(t, root, file2, []byte("y"))

		wantPaths = append(wantPaths, file1, file2)
	}

	opts := fileproc.Options{
		Recursive:          true,
		Workers:            4,
		SmallFileThreshold: 1_000_000, // avoid within-directory pipelining; focus on tree concurrency
	}

	var (
		mu    sync.Mutex
		seen  = make(map[string]int)
		value struct{}
	)

	results, errs := fileproc.Process(t.Context(), root, func(path, _ []byte) (*struct{}, error) {
		mu.Lock()

		seen[string(path)]++

		mu.Unlock()

		return &value, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), wantPaths)

	mu.Lock()
	defer mu.Unlock()

	if len(seen) != len(wantPaths) {
		t.Fatalf("unexpected callback set size: got=%d want=%d", len(seen), len(wantPaths))
	}

	for p, c := range seen {
		if c != 1 {
			t.Fatalf("file processed %d times: %s", c, p)
		}
	}
}

func Test_ProcessReader_Processes_All_Files_When_Using_Recursive_Concurrent_Workers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	wantPaths := make([]string, 0, 128)

	writeFile(t, root, "root1.txt", []byte("a"))
	writeFile(t, root, "root2.txt", []byte("b"))

	wantPaths = append(wantPaths, "root1.txt", "root2.txt")

	for i := range 25 {
		dir := fmt.Sprintf("d%02d", i)
		file1 := filepath.Join(dir, fmt.Sprintf("f%02d.txt", i))
		file2 := filepath.Join(dir, "nested", "n.txt")

		writeFile(t, root, file1, []byte("x"))
		writeFile(t, root, file2, []byte("y"))

		wantPaths = append(wantPaths, file1, file2)
	}

	opts := fileproc.Options{
		Recursive:          true,
		Workers:            4,
		SmallFileThreshold: 1_000_000,
	}

	var (
		mu    sync.Mutex
		seen  = make(map[string]int)
		value struct{}
	)

	results, errs := fileproc.ProcessReader(t.Context(), root, func(path []byte, r io.Reader) (*struct{}, error) {
		// Drain to exercise streaming path.
		_, _ = io.Copy(io.Discard, r)

		mu.Lock()

		seen[string(path)]++

		mu.Unlock()

		return &value, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), wantPaths)

	mu.Lock()
	defer mu.Unlock()

	if len(seen) != len(wantPaths) {
		t.Fatalf("unexpected callback set size: got=%d want=%d", len(seen), len(wantPaths))
	}

	for p, c := range seen {
		if c != 1 {
			t.Fatalf("file processed %d times: %s", c, p)
		}
	}
}

func Test_Process_Traverses_Subdirs_When_Using_Recursive_LargeDir_Pipelined(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	wantPaths := make([]string, 0, 64)

	// A directory with enough files to exceed the threshold.
	for i := range 20 {
		p := filepath.Join("big", fmt.Sprintf("f%02d.txt", i))
		writeFile(t, root, p, []byte("x"))
		wantPaths = append(wantPaths, p)
	}

	// Nested directory under the large dir.
	nested := filepath.Join("big", "sub", "inner.txt")
	writeFile(t, root, nested, []byte("y"))
	wantPaths = append(wantPaths, nested)

	opts := fileproc.Options{
		Recursive:          true,
		Workers:            2,
		SmallFileThreshold: 1, // force pipelining in "big" within tree mode
	}

	var ok struct{}

	results, errs := fileproc.Process(t.Context(), root, func(_ []byte, _ []byte) (*struct{}, error) {
		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), wantPaths)
}

func Test_ProcessReader_Traverses_Subdirs_When_Using_Recursive_LargeDir_Pipelined(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	wantPaths := make([]string, 0, 64)

	for i := range 20 {
		p := filepath.Join("big", fmt.Sprintf("f%02d.txt", i))
		writeFile(t, root, p, []byte("x"))
		wantPaths = append(wantPaths, p)
	}

	nested := filepath.Join("big", "sub", "inner.txt")
	writeFile(t, root, nested, []byte("y"))
	wantPaths = append(wantPaths, nested)

	opts := fileproc.Options{
		Recursive:          true,
		Workers:            2,
		SmallFileThreshold: 1,
	}

	var ok struct{}

	results, errs := fileproc.ProcessReader(t.Context(), root, func(_ []byte, r io.Reader) (*struct{}, error) {
		_, _ = io.Copy(io.Discard, r)

		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), wantPaths)
}
