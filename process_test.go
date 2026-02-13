package fileproc_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	osexec "os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/calvinalkan/fileproc"
)

// ============================================================================
// Process basics
// ============================================================================

func Test_Process_Returns_ProcessError_When_Callback_Fails(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, testBadFile, []byte("bad"))

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	sentinel := errors.New("boom")

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		return nil, sentinel
	}, opts...)

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

	if procErr.Path != filepath.Join(root, testBadFile) {
		t.Fatalf("unexpected path: %s", procErr.Path)
	}
}

func Test_Process_Closes_File_Handle_When_Callback_Errors_After_Read(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("alpha"))
	writeFile(t, root, "b.txt", []byte("bravo"))

	expected := map[string][]byte{
		filepath.Join(root, "a.txt"): []byte("alpha"),
		filepath.Join(root, "b.txt"): []byte("bravo"),
	}

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(1),
	}

	sentinel := errors.New("boom")
	mismatch := errors.New("mismatch")

	var (
		calls         int
		errorInjected bool
	)

	_, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		calls++

		data, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("read file: %w", err)
		}

		path := string(f.AbsPath())

		exp, ok := expected[path]
		if !ok {
			return nil, fmt.Errorf("unexpected path %q: %w", path, mismatch)
		}

		if !errorInjected {
			errorInjected = true

			if !bytes.Equal(data, exp) {
				return nil, fmt.Errorf("unexpected content for first file: %w", mismatch)
			}

			return nil, sentinel
		}

		if !bytes.Equal(data, exp) {
			return nil, fmt.Errorf("stale file handle: %w", mismatch)
		}

		return &struct{}{}, nil
	}, opts...)

	if calls != 2 {
		t.Fatalf("expected 2 callbacks, got %d", calls)
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}

	var procErr *fileproc.ProcessError
	if !errors.As(errs[0], &procErr) {
		t.Fatalf("expected ProcessError, got %T", errs[0])
	}

	if !errors.Is(procErr, sentinel) {
		t.Fatalf("unexpected error: %v", procErr)
	}

	if errors.Is(procErr, mismatch) {
		t.Fatalf("unexpected mismatch error: %v", procErr)
	}
}

func Test_Process_Closes_File_Handle_When_Callback_Errors_After_Bytes(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("alpha"))
	writeFile(t, root, "b.txt", []byte("bravo"))

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(1),
	}

	sentinel := errors.New("boom")

	var calls int

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		calls++
		_, _ = f.ReadAll()

		if calls == 1 {
			return nil, sentinel
		}

		return &struct{}{}, nil
	}, opts...)

	if calls != 2 {
		t.Fatalf("expected 2 callbacks, got %d", calls)
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_Process_Closes_File_Handle_When_Bytes_Returns_Error(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("chmod 000 unsupported on windows")
	}

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("alpha"))
	writeFile(t, root, "b.txt", []byte("bravo"))

	makeUnreadable(t, filepath.Join(root, "a.txt"))

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(1),
	}

	var (
		calls       int
		successData []byte
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		calls++

		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		successData = retainOwnedCopy(w, data)

		return &struct{}{}, nil
	}, opts...)

	if calls != 2 {
		t.Fatalf("expected 2 callbacks, got %d", calls)
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if string(successData) != "bravo" {
		t.Fatalf("expected 'bravo', got %q", successData)
	}
}

func Test_Process_Silently_Skips_When_File_Becomes_Directory(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "normal.txt", []byte("normal"))
	writeFile(t, root, "will_become_dir.txt", []byte("temp"))

	racePath := filepath.Join(root, "will_become_dir.txt")

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(1),
	}

	var callCount int

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		callCount++

		// On first call for the racy file, replace it with a directory
		if string(f.AbsPath()) == racePath {
			// Remove file and create directory with same name before reading
			err := os.Remove(racePath)
			if err != nil {
				return nil, fmt.Errorf("remove: %w", err)
			}

			err = os.Mkdir(racePath, 0o755)
			if err != nil {
				return nil, fmt.Errorf("mkdir: %w", err)
			}

			// Now try to read - should trigger skip
			_, err = f.ReadAll()
			if err != nil {
				return nil, fmt.Errorf("bytes: %w", err)
			}
		}

		return &struct{}{}, nil
	}, opts...)

	// Should have called callback twice (both files)
	if callCount != 2 {
		t.Fatalf("expected 2 callbacks, got %d", callCount)
	}

	// Only 1 result (normal.txt), the directory-race was silently skipped
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// No errors reported for the race condition
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors, got %d: %v", len(errs), errs)
	}
}

func Test_Process_Silently_Skips_When_File_Becomes_Symlink(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("symlinks unreliable on windows")
	}

	root := t.TempDir()
	writeFile(t, root, "normal.txt", []byte("normal"))
	writeFile(t, root, "will_become_symlink.txt", []byte("temp"))
	writeFile(t, root, "target.txt", []byte("target"))

	racePath := filepath.Join(root, "will_become_symlink.txt")
	targetPath := filepath.Join(root, "target.txt")

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(1),
	}

	var callCount int

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		callCount++

		// On the racy file, replace it with a symlink before reading
		if string(f.AbsPath()) == racePath {
			// Remove file and create symlink with same name
			err := os.Remove(racePath)
			if err != nil {
				return nil, fmt.Errorf("remove: %w", err)
			}

			err = os.Symlink(targetPath, racePath)
			if err != nil {
				return nil, fmt.Errorf("symlink: %w", err)
			}

			// Now try to read - should trigger skip (ELOOP from O_NOFOLLOW)
			_, err = f.ReadAll()
			if err != nil {
				return nil, fmt.Errorf("bytes: %w", err)
			}
		}

		return &struct{}{}, nil
	}, opts...)

	// Called for all 3 files that were regular at scan time
	if callCount != 3 {
		t.Fatalf("expected 3 callbacks, got %d", callCount)
	}

	// 2 results (normal.txt, target.txt), the symlink-race was silently skipped
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// No errors reported for the race condition
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors, got %d: %v", len(errs), errs)
	}
}

func Test_Process_Returns_No_Results_When_No_Work(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name  string
		setup func(t *testing.T) (context.Context, string)
	}

	tests := []testCase{
		{
			name: "ContextAlreadyCanceled",
			setup: func(t *testing.T) (context.Context, string) {
				t.Helper()

				root := t.TempDir()
				writeFile(t, root, "data.txt", []byte("data"))

				ctx, cancel := context.WithCancel(t.Context())
				cancel()

				return ctx, root
			},
		},
		{
			name: "DirectoryEmpty",
			setup: func(t *testing.T) (context.Context, string) {
				t.Helper()

				root := t.TempDir()

				return t.Context(), root
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, root := tt.setup(t)
			count := 0

			results, errs := fileproc.Process(ctx, root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
				count++

				return &struct{}{}, nil
			}, fileproc.WithFileWorkers(1))

			if len(results) != 0 {
				t.Fatalf("expected no results, got %d", len(results))
			}

			if len(errs) != 0 {
				t.Fatalf("expected no errors, got %v", errs)
			}

			if count != 0 {
				t.Fatalf("expected no callbacks, got %d", count)
			}
		})
	}
}

func Test_Process_Propagates_Panic_When_Callback_Panics(t *testing.T) {
	t.Parallel()

	if os.Getenv("FILEPROC_PANIC_TEST") == "1" {
		root := t.TempDir()
		writeFile(t, root, "panic.txt", []byte("panic"))

		_, _ = fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
			panic("boom")
		}, fileproc.WithFileWorkers(1))

		return
	}

	cmd := osexec.Command(os.Args[0], "-test.run=^Test_Process_Propagates_Panic_When_Callback_Panics$")

	cmd.Env = append(os.Environ(), "FILEPROC_PANIC_TEST=1")

	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected process to fail due to panic")
	}

	if !bytes.Contains(out, []byte("panic: boom")) {
		t.Fatalf("expected panic output, got: %s", out)
	}
}

func Test_Process_Skips_Result_When_Callback_Returns_ErrSkip(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "skip.txt", []byte("data"))

	count := 0

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		count++

		return nil, fileproc.ErrSkip
	}, fileproc.WithFileWorkers(1))

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

// ============================================================================
// Non-recursive traversal
// ============================================================================

func Test_Process_Returns_Result_Value_When_Callback_Returns_Value(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "value.txt", []byte("value"))

	value := 42

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*int, error) {
		return &value, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0] != &value {
		t.Fatalf("unexpected value pointer: got=%p want=%p", results[0], &value)
	}
}

func Test_Process_Returns_TopLevel_Files_And_Skips_Symlinks_When_NonRecursive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFiles(t, root, map[string][]byte{
		"a.md":                       []byte("alpha"),
		"b.txt":                      []byte("bravo"),
		"empty.md":                   nil,
		filepath.Join("sub", "c.md"): []byte("charlie"),
	})
	writeSymlink(t, root, "a.md", "link.md")

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(1),
	}

	var (
		mu   sync.Mutex
		got  = make(map[string]fileproc.Stat)
		seen = make(map[string]struct{})
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
		st, statErr := f.Stat()
		if statErr != nil {
			return nil, fmt.Errorf("stat: %w", statErr)
		}

		path := string(f.AbsPath())

		mu.Lock()

		got[path] = st
		seen[path] = struct{}{}

		mu.Unlock()

		return &path, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	wantPaths := []string{
		filepath.Join(root, "a.md"),
		filepath.Join(root, "b.txt"),
		filepath.Join(root, "empty.md"),
	}
	assertStringSlicesEqual(t, resultPaths(results), wantPaths)

	expectedData := map[string][]byte{
		filepath.Join(root, "a.md"):     []byte("alpha"),
		filepath.Join(root, "b.txt"):    []byte("bravo"),
		filepath.Join(root, "empty.md"): {},
	}

	for path, want := range expectedData {
		mu.Lock()

		st, ok := got[path]

		mu.Unlock()

		if !ok {
			t.Fatalf("missing callback for %s", path)
		}

		info, err := os.Stat(path)
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

	if _, ok := seen[filepath.Join(root, "sub", "c.md")]; ok {
		t.Fatal("unexpected subdir file in non-recursive mode")
	}

	if _, ok := seen[filepath.Join(root, "link.md")]; ok {
		t.Fatal("unexpected symlink in results")
	}
}

func Test_Process_Reads_Content_Via_File_When_Used(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("hello")
	writeFile(t, root, "data.txt", content)

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var (
		mu     sync.Mutex
		got    []byte
		called bool
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		data, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("read file: %w", err)
		}

		mu.Lock()

		got = append([]byte(nil), data...)
		called = true

		mu.Unlock()

		return &struct{}{}, nil
	}, opts...)

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

func Test_Process_Does_Not_Open_File_When_Unused(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("chmod 000 unsupported on windows")
	}

	root := t.TempDir()
	writeFile(t, root, "secret.txt", []byte("secret"))

	filePath := filepath.Join(root, "secret.txt")
	makeUnreadable(t, filePath)

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var called bool

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		called = true
		// Only access Stat(), don't open file
		_, _ = f.Stat()

		return &struct{}{}, nil
	}, opts...)

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

func Test_Process_Processes_All_Files_When_Using_Concurrent_Workers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	wantPaths := writeFlatFiles(t, root, 8, func(i int) string {
		return "f" + strings.Repeat("x", i) + ".txt"
	}, func(int) []byte { return []byte("data") })

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
		path := string(f.AbsPath())

		return &path, nil
	}, fileproc.WithFileWorkers(4))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), wantPaths)
}

func Test_Process_Processes_All_Files_When_Directory_Is_Large(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	const workers = 4

	pad := strings.Repeat("x", testNamePad)

	wantPaths := writeFlatFiles(t, root, testNumFilesMed, func(i int) string {
		return fmt.Sprintf("f-%04d-%s.txt", i, pad)
	}, nil)

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(workers),
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]int, testNumFilesMed)
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
		path := string(f.AbsPath())

		mu.Lock()

		seen[path]++

		mu.Unlock()

		return &path, nil
	}, opts...)

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

func Test_Process_Reads_Content_When_Using_Concurrent_Workers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	const files = 40

	wantData := make(map[string]string, files)
	wantPaths := writeFlatFiles(t, root, files, func(i int) string {
		return fmt.Sprintf("f-%03d.txt", i)
	}, func(i int) []byte {
		content := fmt.Sprintf("data-%03d", i)
		path := filepath.Join(root, fmt.Sprintf("f-%03d.txt", i))
		wantData[path] = content

		return []byte(content)
	})

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(4),
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]string, files)
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
		data, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("read: %w", err)
		}

		path := string(f.AbsPath())

		mu.Lock()

		seen[path] = string(data)

		mu.Unlock()

		return &path, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), wantPaths)

	mu.Lock()
	defer mu.Unlock()

	if len(seen) != files {
		t.Fatalf("unexpected callback set size: got=%d want=%d", len(seen), files)
	}

	for path, want := range wantData {
		if got, ok := seen[path]; !ok || got != want {
			t.Fatalf("unexpected content for %s: got=%q want=%q", path, got, want)
		}
	}
}

func Test_Process_Skips_NonRegular_When_Socket_Present(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "regular.txt", []byte("data"))

	socketPath := filepath.Join(root, "sock")

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer ln.Close()

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
		path := string(f.AbsPath())

		return &path, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{filepath.Join(root, "regular.txt")})
}

// ============================================================================
// Recursive traversal
// ============================================================================

func Test_Process_Applies_Suffix_Filter_And_Skips_Symlink_Dirs_When_Recursive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "keep.md", []byte("keep"))
	writeFile(t, root, "skip.txt", []byte("skip"))
	writeFile(t, root, filepath.Join("sub", "keep2.md"), []byte("keep2"))
	writeFile(t, root, filepath.Join("sub", "skip2.txt"), []byte("skip2"))
	writeSymlink(t, root, "sub", "sub_link")

	opts := []fileproc.Option{
		fileproc.WithRecursive(),
		fileproc.WithSuffix(".md"),
		fileproc.WithFileWorkers(1),
	}

	var (
		mu  sync.Mutex
		got = make(map[string]struct{})
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
		path := string(f.AbsPath())

		mu.Lock()

		got[path] = struct{}{}

		mu.Unlock()

		return &path, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	wantPaths := []string{
		filepath.Join(root, "keep.md"),
		filepath.Join(root, "sub", "keep2.md"),
	}
	assertStringSlicesEqual(t, resultPaths(results), wantPaths)

	if _, ok := got[filepath.Join(root, "sub_link", "keep2.md")]; ok {
		t.Fatal("symlinked directory was traversed")
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

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
		path := string(f.AbsPath())

		return &path, nil
	}, fileproc.WithRecursive(), fileproc.WithFileWorkers(1))

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	var ioErr *fileproc.IOError
	if !errors.As(errs[0], &ioErr) {
		t.Fatalf("expected IOError, got %T", errs[0])
	}

	if ioErr.Path != filepath.Join(root, "blocked") {
		t.Fatalf("unexpected error path: %s", ioErr.Path)
	}

	if ioErr.Op != openOp {
		t.Fatalf("unexpected error op: %s", ioErr.Op)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{filepath.Join(root, "root.txt")})
}

func Test_Process_Processes_All_Files_When_Using_Recursive_Concurrent_Workers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	wantPaths := make([]string, 0, 128)

	// Root-level files.
	writeFile(t, root, "root1.txt", []byte("a"))
	writeFile(t, root, "root2.txt", []byte("b"))

	wantPaths = append(wantPaths, filepath.Join(root, "root1.txt"), filepath.Join(root, "root2.txt"))

	// Many subdirectories to exercise coordinator + multiple workers.
	writeNestedFiles(t, root, 25, 2, func(dir, file int) string {
		if file == 0 {
			return filepath.Join(fmt.Sprintf("d%02d", dir), fmt.Sprintf("f%02d.txt", dir))
		}

		return filepath.Join(fmt.Sprintf("d%02d", dir), "nested", "n.txt")
	}, func(_, file int) []byte {
		if file == 0 {
			return []byte("x")
		}

		return []byte("y")
	})

	for i := range 25 {
		dir := fmt.Sprintf("d%02d", i)
		wantPaths = append(wantPaths,
			filepath.Join(root, dir, fmt.Sprintf("f%02d.txt", i)),
			filepath.Join(root, dir, "nested", "n.txt"),
		)
	}

	opts := []fileproc.Option{
		fileproc.WithRecursive(),
		fileproc.WithFileWorkers(4),
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]int)
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
		path := string(f.AbsPath())

		mu.Lock()

		seen[path]++

		mu.Unlock()

		return &path, nil
	}, opts...)

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

func Test_Process_Traverses_Subdirs_When_Directory_Is_Large(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	wantPaths := make([]string, 0, 64)

	// A directory with enough files to exceed the threshold.
	wantPaths = append(wantPaths, writeFlatFiles(t, root, 20, func(i int) string {
		return filepath.Join("big", fmt.Sprintf("f%02d.txt", i))
	}, nil)...)

	// Nested directory under the large dir.
	nested := filepath.Join("big", "sub", "inner.txt")
	writeFile(t, root, nested, []byte("y"))
	wantPaths = append(wantPaths, filepath.Join(root, nested))

	opts := []fileproc.Option{
		fileproc.WithRecursive(),
		fileproc.WithFileWorkers(2),
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
		path := string(f.AbsPath())

		return &path, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), wantPaths)
}

// ============================================================================
// Options
// ============================================================================

func Test_Process_Processes_All_Files_When_ChunkSize_Is_Configured(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	pad := strings.Repeat("x", 8)

	wantPaths := make([]string, 0, 64)

	wantPaths = append(wantPaths, writeFlatFiles(t, root, 64, func(i int) string {
		return fmt.Sprintf("c-%02d-%s.txt", i, pad)
	}, nil)...)

	for _, chunkSize := range []int{1, 32} {
		t.Run(fmt.Sprintf("chunk=%d", chunkSize), func(t *testing.T) {
			t.Parallel()

			var (
				mu   sync.Mutex
				seen = make(map[string]int, len(wantPaths))
			)

			results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
				path := string(f.AbsPath())

				mu.Lock()

				seen[path]++

				mu.Unlock()

				return &path, nil
			}, fileproc.WithFileWorkers(4), fileproc.WithChunkSize(chunkSize))

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
		})
	}
}

func Test_Process_Processes_All_Files_When_ScanWorkers_Are_Configured(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	wantPaths := make([]string, 0, 64)

	writeNestedFiles(t, root, 32, 2, func(dir, file int) string {
		if file == 0 {
			return filepath.Join(fmt.Sprintf("d%02d", dir), "a.txt")
		}

		return filepath.Join(fmt.Sprintf("d%02d", dir), "b.txt")
	}, func(_ int, file int) []byte {
		if file == 0 {
			return []byte("a")
		}

		return []byte("b")
	})

	for i := range 32 {
		dir := fmt.Sprintf("d%02d", i)
		wantPaths = append(wantPaths,
			filepath.Join(root, dir, "a.txt"),
			filepath.Join(root, dir, "b.txt"),
		)
	}

	for _, scanWorkers := range []int{1, 4} {
		t.Run(fmt.Sprintf("scan=%d", scanWorkers), func(t *testing.T) {
			t.Parallel()

			var (
				mu   sync.Mutex
				seen = make(map[string]int, len(wantPaths))
			)

			results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
				path := string(f.AbsPath())

				mu.Lock()

				seen[path]++

				mu.Unlock()

				return &path, nil
			}, fileproc.WithRecursive(), fileproc.WithFileWorkers(4), fileproc.WithScanWorkers(scanWorkers))

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
		})
	}
}
