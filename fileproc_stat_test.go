package fileproc_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/calvinalkan/fileproc"
)

func Test_ProcessStat_Returns_TopLevel_Files_When_NonRecursive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.md", []byte("alpha"))
	writeFile(t, root, "b.txt", []byte("bravo"))
	writeFile(t, root, "empty.md", nil)
	writeFile(t, root, filepath.Join("sub", "c.md"), []byte("charlie"))
	writeSymlink(t, root, "a.md", "link.md")

	opts := fileproc.Options{
		Workers:            1,
		SmallFileThreshold: 1000,
	}

	var (
		mu   sync.Mutex
		got  = make(map[string]fileproc.Stat)
		ok   struct{}
		seen = make(map[string]struct{})
	)

	results, errs := fileproc.ProcessStat(t.Context(), root, func(path []byte, st fileproc.Stat, _ fileproc.LazyFile) (*struct{}, error) {
		mu.Lock()

		got[string(path)] = st
		seen[string(path)] = struct{}{}

		mu.Unlock()

		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	wantPaths := []string{"a.md", "b.txt", "empty.md"}
	assertStringSlicesEqual(t, resultPaths(results), wantPaths)

	expectedData := map[string][]byte{
		"a.md":     []byte("alpha"),
		"b.txt":    []byte("bravo"),
		"empty.md": {},
	}

	for path, want := range expectedData {
		mu.Lock()

		st, ok := got[path]

		mu.Unlock()

		if !ok {
			t.Fatalf("missing callback for %s", path)
		}

		info, err := os.Stat(filepath.Join(root, path))
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}

		if st.Size != int64(len(want)) {
			t.Fatalf("size mismatch for %s: got=%d want=%d", path, st.Size, len(want))
		}

		if st.ModTime != info.ModTime().UnixNano() {
			t.Fatalf("modtime mismatch for %s: got=%d want=%d", path, st.ModTime, info.ModTime().UnixNano())
		}
	}

	if _, ok := seen[filepath.Join("sub", "c.md")]; ok {
		t.Fatal("unexpected subdir file in non-recursive mode")
	}

	if _, ok := seen["link.md"]; ok {
		t.Fatal("unexpected symlink in results")
	}
}

func Test_ProcessStat_Applies_Suffix_Filter_When_Recursive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "keep.md", []byte("keep"))
	writeFile(t, root, "skip.txt", []byte("skip"))
	writeFile(t, root, filepath.Join("sub", "keep2.md"), []byte("keep2"))
	writeFile(t, root, filepath.Join("sub", "skip2.txt"), []byte("skip2"))
	writeSymlink(t, root, "sub", "sub_link")

	opts := fileproc.Options{
		Recursive:          true,
		Suffix:             ".md",
		Workers:            1,
		SmallFileThreshold: 1000,
	}

	var (
		mu  sync.Mutex
		got = make(map[string]struct{})
		ok  struct{}
	)

	results, errs := fileproc.ProcessStat(t.Context(), root, func(path []byte, _ fileproc.Stat, _ fileproc.LazyFile) (*struct{}, error) {
		mu.Lock()

		got[string(path)] = struct{}{}

		mu.Unlock()

		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	wantPaths := []string{"keep.md", filepath.Join("sub", "keep2.md")}
	assertStringSlicesEqual(t, resultPaths(results), wantPaths)

	if _, ok := got[filepath.Join("sub_link", "keep2.md")]; ok {
		t.Fatal("symlinked directory was traversed")
	}
}

func Test_ProcessStat_Reads_LazyFile_When_Used(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("hello")
	writeFile(t, root, "data.txt", content)

	opts := fileproc.Options{
		Workers: 1,
	}

	var (
		mu     sync.Mutex
		got    []byte
		called bool
		ok     struct{}
	)

	errMissingLazy := errors.New("missing lazy file")

	results, errs := fileproc.ProcessStat(t.Context(), root, func(_ []byte, _ fileproc.Stat, f fileproc.LazyFile) (*struct{}, error) {
		if f == nil {
			return nil, errMissingLazy
		}

		data, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("read lazy file: %w", err)
		}

		mu.Lock()

		got = append([]byte(nil), data...)
		called = true

		mu.Unlock()

		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if !called {
		t.Fatal("callback was not invoked")
	}

	if !bytes.Equal(got, content) {
		t.Fatalf("unexpected data: got=%q want=%q", got, content)
	}
}

func Test_ProcessStat_Does_Not_Open_LazyFile_When_Unused(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("chmod 000 unsupported on windows")
	}

	root := t.TempDir()
	writeFile(t, root, "secret.txt", []byte("secret"))

	filePath := filepath.Join(root, "secret.txt")

	err := os.Chmod(filePath, 0)
	if err != nil {
		t.Fatalf("chmod %s: %v", filePath, err)
	}

	opts := fileproc.Options{
		Workers: 1,
	}

	var called bool

	results, errs := fileproc.ProcessStat(t.Context(), root, func(_ []byte, _ fileproc.Stat, _ fileproc.LazyFile) (*struct{}, error) {
		called = true

		return &struct{}{}, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if !called {
		t.Fatal("callback was not invoked")
	}
}

func Test_ProcessStat_Returns_Error_When_Callback_Fails(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, testBadFile, []byte("bad"))

	opts := fileproc.Options{
		Workers: 1,
	}

	sentinel := errors.New("boom")

	_, errs := fileproc.ProcessStat(t.Context(), root, func(_ []byte, _ fileproc.Stat, _ fileproc.LazyFile) (*struct{}, error) {
		return nil, sentinel
	}, opts)

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	var procErr *fileproc.ProcessError
	if !errors.As(errs[0], &procErr) {
		t.Fatalf("expected ProcessError, got %T", errs[0])
	}

	if !errors.Is(procErr, sentinel) {
		t.Fatalf("unexpected error: %v", procErr)
	}

	if procErr.Path != testBadFile {
		t.Fatalf("unexpected path: %s", procErr.Path)
	}
}
