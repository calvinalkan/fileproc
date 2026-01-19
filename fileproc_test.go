package fileproc_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"fileproc"
)

const openOp = "open"

func Test_Process_Returns_TopLevel_Files_When_NonRecursive(t *testing.T) {
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
		mu  sync.Mutex
		got = make(map[string][]byte)
		ok  struct{}
	)

	results, errs := fileproc.Process(t.Context(), root, func(path, data []byte) (*struct{}, error) {
		mu.Lock()

		got[string(path)] = append([]byte(nil), data...)

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
		gotData, ok := got[path]
		if !ok {
			t.Fatalf("missing callback for %s", path)
		}

		if !bytes.Equal(gotData, want) {
			t.Fatalf("data mismatch for %s: got=%q want=%q", path, gotData, want)
		}
	}

	if _, ok := got[filepath.Join("sub", "c.md")]; ok {
		t.Fatal("unexpected subdir file in non-recursive mode")
	}

	if _, ok := got["link.md"]; ok {
		t.Fatal("unexpected symlink in results")
	}
}

func Test_Process_Applies_Suffix_Filter_When_Recursive(t *testing.T) {
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

	results, errs := fileproc.Process(t.Context(), root, func(path, _ []byte) (*struct{}, error) {
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

func Test_Process_Truncates_Prefix_When_ReadSize_Is_Small(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("0123456789")
	writeFile(t, root, "data.txt", content)

	opts := fileproc.Options{
		ReadSize: 4,
		Workers:  1,
	}

	var (
		mu     sync.Mutex
		got    []byte
		called bool
		ok     struct{}
	)

	results, errs := fileproc.Process(t.Context(), root, func(_ []byte, data []byte) (*struct{}, error) {
		mu.Lock()

		got = append([]byte(nil), data...)
		called = true

		mu.Unlock()

		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if !called {
		t.Fatal("callback was not invoked")
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if !bytes.Equal(got, content[:4]) {
		t.Fatalf("unexpected data: got=%q want=%q", got, content[:4])
	}
}

func Test_ProcessReader_Reads_Empty_And_Full_Files_When_Processing(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	full := []byte("full-content")
	writeFile(t, root, "full.txt", full)
	writeFile(t, root, "empty.txt", nil)

	opts := fileproc.Options{
		ReadSize: 1,
		Workers:  1,
	}

	var (
		mu    sync.Mutex
		got   = make(map[string][]byte)
		cbErr error
		ok    struct{}
	)

	setErr := func(err error) {
		mu.Lock()

		if cbErr == nil {
			cbErr = err
		}

		mu.Unlock()
	}

	results, errs := fileproc.ProcessReader(t.Context(), root, func(path []byte, r io.Reader) (*struct{}, error) {
		data, err := io.ReadAll(r)
		if err != nil {
			wrapped := fmt.Errorf("read all: %w", err)
			setErr(wrapped)

			return nil, wrapped
		}

		mu.Lock()

		got[string(path)] = data

		mu.Unlock()

		return &ok, nil
	}, opts)

	if cbErr != nil {
		t.Fatalf("callback error: %v", cbErr)
	}

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	wantPaths := []string{"empty.txt", "full.txt"}
	assertStringSlicesEqual(t, resultPaths(results), wantPaths)

	if !bytes.Equal(got["full.txt"], full) {
		t.Fatalf("full.txt data mismatch: got=%q want=%q", got["full.txt"], full)
	}

	if len(got["empty.txt"]) != 0 {
		t.Fatal("expected empty reader for empty.txt")
	}
}

func Test_Process_Collects_ProcessError_When_Callback_Returns_Error(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "bad.txt", []byte("boom"))

	opts := fileproc.Options{Workers: 1}
	sentinel := errors.New("fail")

	results, errs := fileproc.Process(t.Context(), root, func(_ []byte, _ []byte) (*struct{}, error) {
		return nil, sentinel
	}, opts)

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	var procErr *fileproc.ProcessError
	if !errors.As(errs[0], &procErr) {
		t.Fatalf("expected ProcessError, got %T", errs[0])
	}

	if procErr.Path != "bad.txt" {
		t.Fatalf("unexpected error path: %s", procErr.Path)
	}

	if !errors.Is(procErr, sentinel) {
		t.Fatalf("unexpected wrapped error: %v", procErr.Err)
	}
}

func Test_Process_Drops_Errors_When_OnError_Returns_False(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "bad.txt", []byte("boom"))

	var (
		mu     sync.Mutex
		counts []struct{ ioErrs, procErrs int }
	)

	opts := fileproc.Options{
		Workers: 1,
		OnError: func(_ error, ioErrs, procErrs int) bool {
			mu.Lock()

			counts = append(counts, struct{ ioErrs, procErrs int }{ioErrs: ioErrs, procErrs: procErrs})

			mu.Unlock()

			return false
		},
	}

	results, errs := fileproc.Process(t.Context(), root, func(_ []byte, _ []byte) (*struct{}, error) {
		return nil, errors.New("fail")
	}, opts)

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 0 {
		t.Fatalf("expected no collected errors, got %v", errs)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(counts) != 1 {
		t.Fatalf("expected OnError to be called once, got %d", len(counts))
	}

	if counts[0].ioErrs != 0 || counts[0].procErrs != 1 {
		t.Fatalf("unexpected counts: io=%d proc=%d", counts[0].ioErrs, counts[0].procErrs)
	}
}

func Test_Process_Reports_Dot_Path_When_Root_Open_Fails(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	missing := filepath.Join(root, "missing")

	var (
		mu     sync.Mutex
		counts []struct{ ioErrs, procErrs int }
	)

	opts := fileproc.Options{
		Workers: 1,
		OnError: func(_ error, ioErrs, procErrs int) bool {
			mu.Lock()

			counts = append(counts, struct{ ioErrs, procErrs int }{ioErrs: ioErrs, procErrs: procErrs})

			mu.Unlock()

			return true
		},
	}

	var ok struct{}

	results, errs := fileproc.Process(t.Context(), missing, func(_ []byte, _ []byte) (*struct{}, error) {
		return &ok, nil
	}, opts)

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	var ioErr *fileproc.IOError
	if !errors.As(errs[0], &ioErr) {
		t.Fatalf("expected IOError, got %T", errs[0])
	}

	if ioErr.Path != "." {
		t.Fatalf("unexpected error path: %s", ioErr.Path)
	}

	if ioErr.Op != openOp {
		t.Fatalf("unexpected error op: %s", ioErr.Op)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(counts) != 1 {
		t.Fatalf("expected OnError to be called once, got %d", len(counts))
	}

	if counts[0].ioErrs != 1 || counts[0].procErrs != 0 {
		t.Fatalf("unexpected counts: io=%d proc=%d", counts[0].ioErrs, counts[0].procErrs)
	}
}

func Test_Process_Returns_Error_When_Suffix_Has_Nul(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "file.txt", []byte("data"))

	opts := fileproc.Options{
		Suffix: "bad\x00suffix",
	}

	var ok struct{}

	_, errs := fileproc.Process(t.Context(), root, func(_ []byte, _ []byte) (*struct{}, error) {
		return &ok, nil
	}, opts)

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	msg := errs[0].Error()
	if !strings.Contains(msg, "invalid suffix") || !strings.Contains(msg, "contains NUL") {
		t.Fatalf("unexpected error: %v", errs[0])
	}
}

func Test_Process_Stops_Early_When_Context_Canceled(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "one.txt", []byte("one"))
	writeFile(t, root, "two.txt", []byte("two"))
	writeFile(t, root, "three.txt", []byte("three"))

	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })

	stopErr := errors.New("stop")
	count := 0

	var ok struct{}

	opts := fileproc.Options{
		Workers:            1,
		SmallFileThreshold: 1000,
	}

	results, errs := fileproc.Process(ctx, root, func(_ []byte, _ []byte) (*struct{}, error) {
		count++
		if count == 1 {
			cancel(stopErr)
		}

		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if context.Cause(ctx) == nil || !errors.Is(context.Cause(ctx), stopErr) {
		t.Fatalf("expected cancellation cause to be %v, got %v", stopErr, context.Cause(ctx))
	}

	if len(results) >= 3 {
		t.Fatalf("expected early stop, got %d results", len(results))
	}
}

func Test_Process_Returns_Same_Paths_When_CopyResultPath_Set(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("a"))
	writeFile(t, root, "b.txt", []byte("b"))

	var ok struct{}

	fn := func(_ []byte, _ []byte) (*struct{}, error) { return &ok, nil }

	optsA := fileproc.Options{Workers: 1}
	optsB := fileproc.Options{Workers: 1, CopyResultPath: true}

	resA, errsA := fileproc.Process(t.Context(), root, fn, optsA)
	resB, errsB := fileproc.Process(t.Context(), root, fn, optsB)

	if len(errsA) != 0 || len(errsB) != 0 {
		t.Fatalf("unexpected errors: errsA=%v errsB=%v", errsA, errsB)
	}

	assertStringSlicesEqual(t, resultPaths(resA), resultPaths(resB))
}

func Test_Process_Processes_All_Files_When_Pipelined_Workers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	wantPaths := make([]string, 0, 8)

	for i := range 8 {
		name := "f" + strings.Repeat("x", i) + ".txt"
		writeFile(t, root, name, []byte("data"))
		wantPaths = append(wantPaths, name)
	}

	opts := fileproc.Options{
		Workers:            4,
		SmallFileThreshold: 1,
	}

	var (
		mu  sync.Mutex
		got = make(map[string]struct{})
		ok  struct{}
	)

	results, errs := fileproc.Process(t.Context(), root, func(path, _ []byte) (*struct{}, error) {
		mu.Lock()

		got[string(path)] = struct{}{}

		mu.Unlock()

		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), wantPaths)

	for _, path := range wantPaths {
		if _, ok := got[path]; !ok {
			t.Fatalf("missing callback for %s", path)
		}
	}
}

func Test_Process_Skips_Result_When_Callback_Returns_Nil(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "skip.txt", []byte("data"))

	count := 0
	opts := fileproc.Options{Workers: 1}

	results, errs := fileproc.Process(t.Context(), root, func(_ []byte, _ []byte) (*struct{}, error) {
		count++

		var result *struct{}

		return result, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if count != 1 {
		t.Fatalf("expected callback count=1, got %d", count)
	}
}

func Test_ProcessReader_Collects_ProcessError_When_Callback_Returns_Error(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "bad.txt", []byte("boom"))

	sentinel := errors.New("read fail")
	results, errs := fileproc.ProcessReader(t.Context(), root, func(_ []byte, _ io.Reader) (*struct{}, error) {
		return nil, sentinel
	}, fileproc.Options{Workers: 1})

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	var procErr *fileproc.ProcessError
	if !errors.As(errs[0], &procErr) {
		t.Fatalf("expected ProcessError, got %T", errs[0])
	}

	if procErr.Path != "bad.txt" {
		t.Fatalf("unexpected error path: %s", procErr.Path)
	}

	if !errors.Is(procErr, sentinel) {
		t.Fatalf("unexpected wrapped error: %v", procErr.Err)
	}
}

func Test_ProcessReader_Drops_Errors_When_OnError_Returns_False(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "bad.txt", []byte("boom"))

	var (
		mu     sync.Mutex
		counts []struct{ ioErrs, procErrs int }
	)

	opts := fileproc.Options{
		Workers: 1,
		OnError: func(_ error, ioErrs, procErrs int) bool {
			mu.Lock()

			counts = append(counts, struct{ ioErrs, procErrs int }{ioErrs: ioErrs, procErrs: procErrs})

			mu.Unlock()

			return false
		},
	}

	results, errs := fileproc.ProcessReader(t.Context(), root, func(_ []byte, _ io.Reader) (*struct{}, error) {
		return nil, errors.New("fail")
	}, opts)

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 0 {
		t.Fatalf("expected no collected errors, got %v", errs)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(counts) != 1 {
		t.Fatalf("expected OnError to be called once, got %d", len(counts))
	}

	if counts[0].ioErrs != 0 || counts[0].procErrs != 1 {
		t.Fatalf("unexpected counts: io=%d proc=%d", counts[0].ioErrs, counts[0].procErrs)
	}
}

func Test_Process_Returns_No_Results_When_Context_Already_Canceled(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "data.txt", []byte("data"))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	count := 0

	var ok struct{}

	results, errs := fileproc.Process(ctx, root, func(_ []byte, _ []byte) (*struct{}, error) {
		count++

		return &ok, nil
	}, fileproc.Options{Workers: 1})

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}

	if count != 0 {
		t.Fatalf("expected no callbacks, got %d", count)
	}
}

func Test_ProcessReader_Returns_No_Results_When_Context_Already_Canceled(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "data.txt", []byte("data"))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	count := 0

	var ok struct{}

	results, errs := fileproc.ProcessReader(ctx, root, func(_ []byte, _ io.Reader) (*struct{}, error) {
		count++

		return &ok, nil
	}, fileproc.Options{Workers: 1})

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}

	if count != 0 {
		t.Fatalf("expected no callbacks, got %d", count)
	}
}

func Test_Process_Returns_IOError_When_Path_Contains_Nul(t *testing.T) {
	t.Parallel()

	count := 0

	var ok struct{}

	results, errs := fileproc.Process(t.Context(), "bad\x00path", func(_ []byte, _ []byte) (*struct{}, error) {
		count++

		return &ok, nil
	}, fileproc.Options{})

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	if count != 0 {
		t.Fatalf("expected no callbacks, got %d", count)
	}

	var ioErr *fileproc.IOError
	if !errors.As(errs[0], &ioErr) {
		t.Fatalf("expected IOError, got %T", errs[0])
	}

	if ioErr.Path != "." {
		t.Fatalf("unexpected error path: %s", ioErr.Path)
	}

	if ioErr.Op != openOp {
		t.Fatalf("unexpected error op: %s", ioErr.Op)
	}

	if !strings.Contains(ioErr.Error(), "contains NUL") {
		t.Fatalf("unexpected error: %v", ioErr)
	}
}

func Test_ProcessReader_Returns_IOError_When_Path_Contains_Nul(t *testing.T) {
	t.Parallel()

	count := 0

	var ok struct{}

	results, errs := fileproc.ProcessReader(t.Context(), "bad\x00path", func(_ []byte, _ io.Reader) (*struct{}, error) {
		count++

		return &ok, nil
	}, fileproc.Options{})

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	if count != 0 {
		t.Fatalf("expected no callbacks, got %d", count)
	}

	var ioErr *fileproc.IOError
	if !errors.As(errs[0], &ioErr) {
		t.Fatalf("expected IOError, got %T", errs[0])
	}

	if ioErr.Path != "." {
		t.Fatalf("unexpected error path: %s", ioErr.Path)
	}

	if ioErr.Op != openOp {
		t.Fatalf("unexpected error op: %s", ioErr.Op)
	}

	if !strings.Contains(ioErr.Error(), "contains NUL") {
		t.Fatalf("unexpected error: %v", ioErr)
	}
}

func Test_Process_Reports_IOError_When_File_Not_Readable(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	root := t.TempDir()
	writeFile(t, root, "good.txt", []byte("good"))
	writeFile(t, root, "secret.txt", []byte("secret"))

	secretPath := filepath.Join(root, "secret.txt")

	err := os.Chmod(secretPath, 0o000)
	if err != nil {
		t.Fatalf("chmod secret.txt: %v", err)
	}

	t.Cleanup(func() {
		_ = os.Chmod(secretPath, 0o600)
	})

	var (
		mu  sync.Mutex
		got = make(map[string]struct{})
		ok  struct{}
	)

	results, errs := fileproc.Process(t.Context(), root, func(path, _ []byte) (*struct{}, error) {
		mu.Lock()

		got[string(path)] = struct{}{}

		mu.Unlock()

		return &ok, nil
	}, fileproc.Options{Workers: 1})

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	var ioErr *fileproc.IOError
	if !errors.As(errs[0], &ioErr) {
		t.Fatalf("expected IOError, got %T", errs[0])
	}

	if ioErr.Path != "secret.txt" {
		t.Fatalf("unexpected error path: %s", ioErr.Path)
	}

	if ioErr.Op != openOp {
		t.Fatalf("unexpected error op: %s", ioErr.Op)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{"good.txt"})

	if _, ok := got["good.txt"]; !ok {
		t.Fatal("missing callback for good.txt")
	}
}

func Test_Process_Reports_IOError_When_Subdir_Not_Readable_Recursive(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	root := t.TempDir()
	writeFile(t, root, "root.txt", []byte("root"))
	writeFile(t, root, filepath.Join("blocked", "secret.txt"), []byte("secret"))

	blockedDir := filepath.Join(root, "blocked")

	info, err := os.Stat(blockedDir)
	if err != nil {
		t.Fatalf("stat blocked dir: %v", err)
	}

	origPerm := info.Mode().Perm()

	err = os.Chmod(blockedDir, 0o000)
	if err != nil {
		t.Fatalf("chmod blocked dir: %v", err)
	}

	t.Cleanup(func() {
		_ = os.Chmod(blockedDir, origPerm)
	})

	var (
		mu  sync.Mutex
		got = make(map[string]struct{})
		ok  struct{}
	)

	opts := fileproc.Options{
		Recursive:          true,
		Workers:            1,
		SmallFileThreshold: 1000,
	}

	results, errs := fileproc.Process(t.Context(), root, func(path, _ []byte) (*struct{}, error) {
		mu.Lock()

		got[string(path)] = struct{}{}

		mu.Unlock()

		return &ok, nil
	}, opts)

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	var ioErr *fileproc.IOError
	if !errors.As(errs[0], &ioErr) {
		t.Fatalf("expected IOError, got %T", errs[0])
	}

	if ioErr.Path != "blocked" {
		t.Fatalf("unexpected error path: %s", ioErr.Path)
	}

	if ioErr.Op != openOp {
		t.Fatalf("unexpected error op: %s", ioErr.Op)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{"root.txt"})

	if _, ok := got["root.txt"]; !ok {
		t.Fatal("missing callback for root.txt")
	}
}

func Test_Process_Skips_NonRegular_When_Fifo_Present(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "regular.txt", []byte("data"))

	fifoPath := filepath.Join(root, "pipe")

	err := syscall.Mkfifo(fifoPath, 0o600)
	if err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	var (
		mu  sync.Mutex
		got = make(map[string]struct{})
		ok  struct{}
	)

	results, errs := fileproc.Process(t.Context(), root, func(path, _ []byte) (*struct{}, error) {
		mu.Lock()

		got[string(path)] = struct{}{}

		mu.Unlock()

		return &ok, nil
	}, fileproc.Options{Workers: 1})

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{"regular.txt"})

	if _, ok := got["regular.txt"]; !ok {
		t.Fatal("missing callback for regular.txt")
	}
}

func Test_Process_Returns_No_Results_When_Directory_Empty(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	count := 0

	var ok struct{}

	results, errs := fileproc.Process(t.Context(), root, func(_ []byte, _ []byte) (*struct{}, error) {
		count++

		return &ok, nil
	}, fileproc.Options{Workers: 1})

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}

	if count != 0 {
		t.Fatalf("expected no callbacks, got %d", count)
	}
}

func Test_ProcessReader_Returns_No_Results_When_Directory_Empty(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	count := 0

	var ok struct{}

	results, errs := fileproc.ProcessReader(t.Context(), root, func(_ []byte, _ io.Reader) (*struct{}, error) {
		count++

		return &ok, nil
	}, fileproc.Options{Workers: 1})

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}

	if count != 0 {
		t.Fatalf("expected no callbacks, got %d", count)
	}
}

func Test_ProcessReader_Applies_Suffix_Filter_When_Recursive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "keep.md", []byte("keep"))
	writeFile(t, root, "skip.txt", []byte("skip"))
	writeFile(t, root, filepath.Join("sub", "keep2.md"), []byte("keep2"))
	writeFile(t, root, filepath.Join("sub", "skip2.txt"), []byte("skip2"))

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

	results, errs := fileproc.ProcessReader(t.Context(), root, func(path []byte, _ io.Reader) (*struct{}, error) {
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
}

func Test_Process_Uses_Default_ReadSize_When_ReadSize_Not_Set(t *testing.T) {
	t.Parallel()

	const defaultReadSize = 2048

	root := t.TempDir()
	content := bytes.Repeat([]byte("a"), defaultReadSize*2)
	writeFile(t, root, "data.txt", content)

	var (
		mu  sync.Mutex
		got []byte
		ok  struct{}
	)

	opts := fileproc.Options{ReadSize: 0, Workers: 1}

	results, errs := fileproc.Process(t.Context(), root, func(_ []byte, data []byte) (*struct{}, error) {
		mu.Lock()

		got = append([]byte(nil), data...)

		mu.Unlock()

		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if len(got) != defaultReadSize {
		t.Fatalf("expected %d bytes, got %d", defaultReadSize, len(got))
	}

	if !bytes.Equal(got, content[:defaultReadSize]) {
		t.Fatal("unexpected data prefix")
	}
}

func Test_Process_Propagates_Panic_When_Callback_Panics(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "panic.txt", []byte("panic"))

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic to propagate")
		}
	}()

	_, _ = fileproc.Process(t.Context(), root, func(_ []byte, _ []byte) (*struct{}, error) {
		panic("boom")
	}, fileproc.Options{Workers: 1})
}

func Test_ProcessReader_Propagates_Panic_When_Callback_Panics(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "panic.txt", []byte("panic"))

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic to propagate")
		}
	}()

	_, _ = fileproc.ProcessReader(t.Context(), root, func(_ []byte, _ io.Reader) (*struct{}, error) {
		panic("boom")
	}, fileproc.Options{Workers: 1})
}

func Test_ProcessReader_Skips_Result_When_Callback_Returns_Nil(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "skip.txt", []byte("data"))

	count := 0
	opts := fileproc.Options{Workers: 1}

	results, errs := fileproc.ProcessReader(t.Context(), root, func(_ []byte, _ io.Reader) (*struct{}, error) {
		count++

		var result *struct{}

		return result, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if count != 1 {
		t.Fatalf("expected callback count=1, got %d", count)
	}
}

func Test_ProcessReader_Stops_Early_When_Context_Canceled(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "one.txt", []byte("one"))
	writeFile(t, root, "two.txt", []byte("two"))
	writeFile(t, root, "three.txt", []byte("three"))

	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })

	stopErr := errors.New("stop")
	count := 0

	var ok struct{}

	opts := fileproc.Options{Workers: 1}

	results, errs := fileproc.ProcessReader(ctx, root, func(_ []byte, _ io.Reader) (*struct{}, error) {
		count++
		if count == 1 {
			cancel(stopErr)
		}

		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if context.Cause(ctx) == nil || !errors.Is(context.Cause(ctx), stopErr) {
		t.Fatalf("expected cancellation cause to be %v, got %v", stopErr, context.Cause(ctx))
	}

	if len(results) >= 3 {
		t.Fatalf("expected early stop, got %d results", len(results))
	}
}

func Test_Process_Continues_When_OnError_Drops_Errors(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "bad.txt", []byte("bad"))
	writeFile(t, root, "good.txt", []byte("good"))

	var (
		mu       sync.Mutex
		paths    []string
		ok       struct{}
		seenErrs int
	)

	opts := fileproc.Options{
		Workers: 1,
		OnError: func(_ error, _, _ int) bool {
			seenErrs++

			return false
		},
	}

	results, errs := fileproc.Process(t.Context(), root, func(path []byte, _ []byte) (*struct{}, error) {
		if string(path) == "bad.txt" {
			return nil, errors.New("fail")
		}

		mu.Lock()

		paths = append(paths, string(path))

		mu.Unlock()

		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("expected no collected errors, got %v", errs)
	}

	if seenErrs != 1 {
		t.Fatalf("expected OnError count=1, got %d", seenErrs)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{"good.txt"})
	assertStringSlicesEqual(t, paths, []string{"good.txt"})
}

func Test_ProcessReader_Returns_Same_Paths_When_CopyResultPath_Set(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("a"))
	writeFile(t, root, "b.txt", []byte("b"))

	var ok struct{}

	fn := func(_ []byte, _ io.Reader) (*struct{}, error) { return &ok, nil }

	optsA := fileproc.Options{Workers: 1}
	optsB := fileproc.Options{Workers: 1, CopyResultPath: true}

	resA, errsA := fileproc.ProcessReader(t.Context(), root, fn, optsA)
	resB, errsB := fileproc.ProcessReader(t.Context(), root, fn, optsB)

	if len(errsA) != 0 || len(errsB) != 0 {
		t.Fatalf("unexpected errors: errsA=%v errsB=%v", errsA, errsB)
	}

	assertStringSlicesEqual(t, resultPaths(resA), resultPaths(resB))
}

func Test_ProcessReader_Replays_Probe_When_File_Exceeds_Probe_Size(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := bytes.Repeat([]byte("z"), 6000)
	writeFile(t, root, "big.txt", content)

	var (
		mu  sync.Mutex
		got []byte
		ok  struct{}
	)

	results, errs := fileproc.ProcessReader(t.Context(), root, func(_ []byte, r io.Reader) (*struct{}, error) {
		data, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("read all: %w", err)
		}

		mu.Lock()

		got = append([]byte(nil), data...)

		mu.Unlock()

		return &ok, nil
	}, fileproc.Options{Workers: 1})

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if !bytes.Equal(got, content) {
		t.Fatalf("content mismatch: got=%d want=%d", len(got), len(content))
	}
}

func Test_ProcessReader_Continues_When_OnError_Drops_Errors(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "bad.txt", []byte("bad"))
	writeFile(t, root, "good.txt", []byte("good"))

	var (
		mu       sync.Mutex
		paths    []string
		ok       struct{}
		seenErrs int
	)

	opts := fileproc.Options{
		Workers: 1,
		OnError: func(_ error, _, _ int) bool {
			seenErrs++

			return false
		},
	}

	results, errs := fileproc.ProcessReader(t.Context(), root, func(path []byte, _ io.Reader) (*struct{}, error) {
		if string(path) == "bad.txt" {
			return nil, errors.New("fail")
		}

		mu.Lock()

		paths = append(paths, string(path))

		mu.Unlock()

		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("expected no collected errors, got %v", errs)
	}

	if seenErrs != 1 {
		t.Fatalf("expected OnError count=1, got %d", seenErrs)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{"good.txt"})
	assertStringSlicesEqual(t, paths, []string{"good.txt"})
}

func Test_Process_Returns_IOError_When_Path_Is_File(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	filePath := filepath.Join(root, "notadir.txt")
	writeFile(t, root, "notadir.txt", []byte("data"))

	count := 0

	var ok struct{}

	results, errs := fileproc.Process(t.Context(), filePath, func(_ []byte, _ []byte) (*struct{}, error) {
		count++

		return &ok, nil
	}, fileproc.Options{})

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	if count != 0 {
		t.Fatalf("expected no callbacks, got %d", count)
	}

	var ioErr *fileproc.IOError
	if !errors.As(errs[0], &ioErr) {
		t.Fatalf("expected IOError, got %T", errs[0])
	}

	if ioErr.Path != "." {
		t.Fatalf("unexpected error path: %s", ioErr.Path)
	}

	if ioErr.Op != openOp {
		t.Fatalf("unexpected error op: %s", ioErr.Op)
	}
}

func Test_Process_Returns_IOError_When_Path_Is_Symlink(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "target.txt", []byte("data"))

	symPath := filepath.Join(root, "link.txt")

	err := os.Symlink(filepath.Join(root, "target.txt"), symPath)
	if err != nil {
		t.Fatalf("symlink: %v", err)
	}

	count := 0

	var ok struct{}

	results, errs := fileproc.Process(t.Context(), symPath, func(_ []byte, _ []byte) (*struct{}, error) {
		count++

		return &ok, nil
	}, fileproc.Options{})

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	if count != 0 {
		t.Fatalf("expected no callbacks, got %d", count)
	}

	var ioErr *fileproc.IOError
	if !errors.As(errs[0], &ioErr) {
		t.Fatalf("expected IOError, got %T", errs[0])
	}

	if ioErr.Path != "." {
		t.Fatalf("unexpected error path: %s", ioErr.Path)
	}

	if ioErr.Op != openOp {
		t.Fatalf("unexpected error op: %s", ioErr.Op)
	}
}

func Test_Process_Returns_Result_Value_When_Callback_Returns_Value(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "value.txt", []byte("value"))

	value := 42
	results, errs := fileproc.Process(t.Context(), root, func(_ []byte, _ []byte) (*int, error) {
		return &value, nil
	}, fileproc.Options{Workers: 1})

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].Value != &value {
		t.Fatalf("unexpected value pointer: got=%p want=%p", results[0].Value, &value)
	}
}

func Test_ProcessReader_Returns_Result_Value_When_Callback_Returns_Value(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "value.txt", []byte("value"))

	value := 42
	results, errs := fileproc.ProcessReader(t.Context(), root, func(_ []byte, _ io.Reader) (*int, error) {
		return &value, nil
	}, fileproc.Options{Workers: 1})

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].Value != &value {
		t.Fatalf("unexpected value pointer: got=%p want=%p", results[0].Value, &value)
	}
}

func Test_Process_OnError_Counts_Are_Cumulative_When_Multiple_Errors(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "bad1.txt", []byte("bad1"))
	writeFile(t, root, "bad2.txt", []byte("bad2"))

	var (
		mu     sync.Mutex
		counts []struct{ ioErrs, procErrs int }
	)

	opts := fileproc.Options{
		Workers:            1,
		SmallFileThreshold: 1000,
		OnError: func(_ error, ioErrs, procErrs int) bool {
			mu.Lock()

			counts = append(counts, struct{ ioErrs, procErrs int }{ioErrs: ioErrs, procErrs: procErrs})

			mu.Unlock()

			return true
		},
	}

	results, errs := fileproc.Process(t.Context(), root, func(_ []byte, _ []byte) (*struct{}, error) {
		return nil, errors.New("fail")
	}, opts)

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 2 {
		t.Fatalf("expected 2 errors, got %d", len(errs))
	}

	mu.Lock()
	defer mu.Unlock()

	if len(counts) != 2 {
		t.Fatalf("expected OnError to be called twice, got %d", len(counts))
	}

	if counts[0].ioErrs != 0 || counts[0].procErrs != 1 {
		t.Fatalf("unexpected counts[0]: io=%d proc=%d", counts[0].ioErrs, counts[0].procErrs)
	}

	if counts[1].ioErrs != 0 || counts[1].procErrs != 2 {
		t.Fatalf("unexpected counts[1]: io=%d proc=%d", counts[1].ioErrs, counts[1].procErrs)
	}
}

func Test_ProcessReader_Skips_Symlink_When_File_Is_Symlink(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "real.txt", []byte("real"))
	writeSymlink(t, root, "real.txt", "link.txt")

	var (
		mu  sync.Mutex
		got = make(map[string]struct{})
		ok  struct{}
	)

	results, errs := fileproc.ProcessReader(t.Context(), root, func(path []byte, _ io.Reader) (*struct{}, error) {
		mu.Lock()

		got[string(path)] = struct{}{}

		mu.Unlock()

		return &ok, nil
	}, fileproc.Options{Workers: 1})

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{"real.txt"})

	if _, ok := got["link.txt"]; ok {
		t.Fatal("unexpected symlink in results")
	}
}

func Test_ProcessReader_Skips_NonRegular_When_Fifo_Present(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "regular.txt", []byte("data"))

	fifoPath := filepath.Join(root, "pipe")

	err := syscall.Mkfifo(fifoPath, 0o600)
	if err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	var (
		mu  sync.Mutex
		got = make(map[string]struct{})
		ok  struct{}
	)

	results, errs := fileproc.ProcessReader(t.Context(), root, func(path []byte, _ io.Reader) (*struct{}, error) {
		mu.Lock()

		got[string(path)] = struct{}{}

		mu.Unlock()

		return &ok, nil
	}, fileproc.Options{Workers: 1})

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{"regular.txt"})

	if _, ok := got["regular.txt"]; !ok {
		t.Fatal("missing callback for regular.txt")
	}
}

func Test_ProcessReader_Reports_IOError_When_File_Not_Readable(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	root := t.TempDir()
	writeFile(t, root, "good.txt", []byte("good"))
	writeFile(t, root, "secret.txt", []byte("secret"))

	secretPath := filepath.Join(root, "secret.txt")

	err := os.Chmod(secretPath, 0o000)
	if err != nil {
		t.Fatalf("chmod secret.txt: %v", err)
	}

	t.Cleanup(func() {
		_ = os.Chmod(secretPath, 0o600)
	})

	var (
		mu  sync.Mutex
		got = make(map[string]struct{})
		ok  struct{}
	)

	results, errs := fileproc.ProcessReader(t.Context(), root, func(path []byte, _ io.Reader) (*struct{}, error) {
		mu.Lock()

		got[string(path)] = struct{}{}

		mu.Unlock()

		return &ok, nil
	}, fileproc.Options{Workers: 1})

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	var ioErr *fileproc.IOError
	if !errors.As(errs[0], &ioErr) {
		t.Fatalf("expected IOError, got %T", errs[0])
	}

	if ioErr.Path != "secret.txt" {
		t.Fatalf("unexpected error path: %s", ioErr.Path)
	}

	if ioErr.Op != openOp {
		t.Fatalf("unexpected error op: %s", ioErr.Op)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{"good.txt"})

	if _, ok := got["good.txt"]; !ok {
		t.Fatal("missing callback for good.txt")
	}
}

func Test_ProcessReader_Reports_IOError_When_Subdir_Not_Readable_Recursive(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	root := t.TempDir()
	writeFile(t, root, "root.txt", []byte("root"))
	writeFile(t, root, filepath.Join("blocked", "secret.txt"), []byte("secret"))

	blockedDir := filepath.Join(root, "blocked")

	info, err := os.Stat(blockedDir)
	if err != nil {
		t.Fatalf("stat blocked dir: %v", err)
	}

	origPerm := info.Mode().Perm()

	err = os.Chmod(blockedDir, 0o000)
	if err != nil {
		t.Fatalf("chmod blocked dir: %v", err)
	}

	t.Cleanup(func() {
		_ = os.Chmod(blockedDir, origPerm)
	})

	var (
		mu  sync.Mutex
		got = make(map[string]struct{})
		ok  struct{}
	)

	opts := fileproc.Options{
		Recursive:          true,
		Workers:            1,
		SmallFileThreshold: 1000,
	}

	results, errs := fileproc.ProcessReader(t.Context(), root, func(path []byte, _ io.Reader) (*struct{}, error) {
		mu.Lock()

		got[string(path)] = struct{}{}

		mu.Unlock()

		return &ok, nil
	}, opts)

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	var ioErr *fileproc.IOError
	if !errors.As(errs[0], &ioErr) {
		t.Fatalf("expected IOError, got %T", errs[0])
	}

	if ioErr.Path != "blocked" {
		t.Fatalf("unexpected error path: %s", ioErr.Path)
	}

	if ioErr.Op != openOp {
		t.Fatalf("unexpected error op: %s", ioErr.Op)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{"root.txt"})

	if _, ok := got["root.txt"]; !ok {
		t.Fatal("missing callback for root.txt")
	}
}

func Test_ProcessReader_Returns_Error_When_Suffix_Has_Nul(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "file.txt", []byte("data"))

	count := 0

	var ok struct{}

	opts := fileproc.Options{Suffix: "bad\x00suffix"}

	_, errs := fileproc.ProcessReader(t.Context(), root, func(_ []byte, _ io.Reader) (*struct{}, error) {
		count++

		return &ok, nil
	}, opts)

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	if count != 0 {
		t.Fatalf("expected no callbacks, got %d", count)
	}

	msg := errs[0].Error()
	if !strings.Contains(msg, "invalid suffix") || !strings.Contains(msg, "contains NUL") {
		t.Fatalf("unexpected error: %v", errs[0])
	}
}

func Test_ProcessReader_Returns_IOError_When_Path_Is_File(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	filePath := filepath.Join(root, "notadir.txt")
	writeFile(t, root, "notadir.txt", []byte("data"))

	count := 0

	var ok struct{}

	results, errs := fileproc.ProcessReader(t.Context(), filePath, func(_ []byte, _ io.Reader) (*struct{}, error) {
		count++

		return &ok, nil
	}, fileproc.Options{})

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	if count != 0 {
		t.Fatalf("expected no callbacks, got %d", count)
	}

	var ioErr *fileproc.IOError
	if !errors.As(errs[0], &ioErr) {
		t.Fatalf("expected IOError, got %T", errs[0])
	}

	if ioErr.Path != "." {
		t.Fatalf("unexpected error path: %s", ioErr.Path)
	}

	if ioErr.Op != openOp {
		t.Fatalf("unexpected error op: %s", ioErr.Op)
	}
}

func Test_ProcessReader_Returns_IOError_When_Path_Is_Symlink(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "target.txt", []byte("data"))

	symPath := filepath.Join(root, "link.txt")

	err := os.Symlink(filepath.Join(root, "target.txt"), symPath)
	if err != nil {
		t.Fatalf("symlink: %v", err)
	}

	count := 0

	var ok struct{}

	results, errs := fileproc.ProcessReader(t.Context(), symPath, func(_ []byte, _ io.Reader) (*struct{}, error) {
		count++

		return &ok, nil
	}, fileproc.Options{})

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	if count != 0 {
		t.Fatalf("expected no callbacks, got %d", count)
	}

	var ioErr *fileproc.IOError
	if !errors.As(errs[0], &ioErr) {
		t.Fatalf("expected IOError, got %T", errs[0])
	}

	if ioErr.Path != "." {
		t.Fatalf("unexpected error path: %s", ioErr.Path)
	}

	if ioErr.Op != openOp {
		t.Fatalf("unexpected error op: %s", ioErr.Op)
	}
}
