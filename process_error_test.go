package fileproc_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/calvinalkan/fileproc"
)

func Test_Process_Returns_Error_When_Suffix_Has_Nul(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "file.txt", []byte("data"))

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		return &struct{}{}, nil
	}, fileproc.WithSuffix("bad\x00suffix"))

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	msg := errs[0].Error()
	if !strings.Contains(msg, "invalid suffix") || !strings.Contains(msg, "contains NUL") {
		t.Fatalf("unexpected error: %v", errs[0])
	}
}

func Test_Process_Returns_IOError_When_Root_Invalid(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		setup        func(t *testing.T, root string) (string, string)
		wantContains string
	}

	tests := []testCase{
		{
			name: "PathContainsNul",
			setup: func(t *testing.T, _ string) (string, string) {
				t.Helper()

				return "bad\x00path", "."
			},
			wantContains: "contains NUL",
		},
		{
			name: "PathIsFile",
			setup: func(t *testing.T, root string) (string, string) {
				t.Helper()

				filePath := filepath.Join(root, "notadir.txt")
				writeFile(t, root, "notadir.txt", []byte("data"))

				return filePath, filePath
			},
		},
		{
			name: "PathIsSymlink",
			setup: func(t *testing.T, root string) (string, string) {
				t.Helper()

				writeFile(t, root, "target.txt", []byte("data"))

				symPath := filepath.Join(root, "link.txt")

				err := os.Symlink(filepath.Join(root, "target.txt"), symPath)
				if err != nil {
					t.Fatalf("symlink: %v", err)
				}

				return symPath, symPath
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			path, wantPath := tt.setup(t, root)

			count := 0
			results, errs := fileproc.Process(t.Context(), path, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
				count++

				return &struct{}{}, nil
			})

			if len(results) != 0 {
				t.Fatalf("expected no results, got %d", len(results))
			}

			if len(errs) != 1 {
				t.Fatalf("expected 1 error, got %d", len(errs))
			}

			if count != 0 {
				t.Fatalf("expected no callbacks, got %d", count)
			}

			assertIOError(t, errs[0], wantPath, openOp)

			if tt.wantContains != "" && !strings.Contains(errs[0].Error(), tt.wantContains) {
				t.Fatalf("unexpected error: %v", errs[0])
			}
		})
	}
}

func Test_Process_Reports_Path_When_Root_Open_Fails(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	missing := filepath.Join(root, "missing")

	var (
		mu     sync.Mutex
		counts []struct{ ioErrs, procErrs int }
	)

	results, errs := fileproc.Process(t.Context(), missing, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		return &struct{}{}, nil
	}, fileproc.WithOnError(func(_ error, ioErrs, procErrs int) bool {
		mu.Lock()

		counts = append(counts, struct{ ioErrs, procErrs int }{ioErrs: ioErrs, procErrs: procErrs})

		mu.Unlock()

		return true
	}))

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	assertIOError(t, errs[0], missing, openOp)

	mu.Lock()
	defer mu.Unlock()

	if len(counts) != 1 {
		t.Fatalf("expected OnError to be called once, got %d", len(counts))
	}

	if counts[0].ioErrs != 1 || counts[0].procErrs != 0 {
		t.Fatalf("unexpected counts: io=%d proc=%d", counts[0].ioErrs, counts[0].procErrs)
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

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		return nil, errors.New("fail")
	}, fileproc.WithFileWorkers(1), fileproc.WithOnError(func(_ error, ioErrs, procErrs int) bool {
		mu.Lock()

		counts = append(counts, struct{ ioErrs, procErrs int }{ioErrs: ioErrs, procErrs: procErrs})

		mu.Unlock()

		return false
	}))

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

func Test_Process_Continues_When_OnError_Drops_Errors(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "bad.txt", []byte("bad"))
	writeFile(t, root, "good.txt", []byte("good"))

	var (
		mu       sync.Mutex
		paths    []string
		seenErrs int
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
		path := string(f.AbsPathBorrowed())
		if path == filepath.Join(root, "bad.txt") {
			return nil, errors.New("fail")
		}

		mu.Lock()

		paths = append(paths, path)

		mu.Unlock()

		return &path, nil
	}, fileproc.WithFileWorkers(1), fileproc.WithOnError(func(_ error, _, _ int) bool {
		seenErrs++

		return false
	}))

	if len(errs) != 0 {
		t.Fatalf("expected no collected errors, got %v", errs)
	}

	if seenErrs != 1 {
		t.Fatalf("expected OnError count=1, got %d", seenErrs)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{filepath.Join(root, "good.txt")})
	assertStringSlicesEqual(t, paths, []string{filepath.Join(root, "good.txt")})
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

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		return nil, errors.New("fail")
	}, fileproc.WithFileWorkers(1), fileproc.WithOnError(func(_ error, ioErrs, procErrs int) bool {
		mu.Lock()

		counts = append(counts, struct{ ioErrs, procErrs int }{ioErrs: ioErrs, procErrs: procErrs})

		mu.Unlock()

		return true
	}))

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

func Test_Process_Counts_Errors_Correctly_When_Mixed_IO_And_Process_Errors_Concurrent(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	root := t.TempDir()

	writeFile(t, root, "good.txt", []byte("good"))
	writeFile(t, root, testBadFile, []byte("bad"))
	writeFile(t, root, filepath.Join("blocked", testSecretFile), []byte("secret"))

	blockedPath := filepath.Join(root, "blocked")

	info, chmodErr := os.Stat(blockedPath)
	if chmodErr != nil {
		t.Fatalf("stat blocked dir: %v", chmodErr)
	}

	origPerm := info.Mode().Perm()

	chmodErr = os.Chmod(blockedPath, 0o000)
	if chmodErr != nil {
		t.Fatalf("chmod blocked dir: %v", chmodErr)
	}

	t.Cleanup(func() {
		_ = os.Chmod(blockedPath, origPerm)
	})

	sentinel := errors.New("fail")

	type call struct {
		errType  string
		ioErrs   int
		procErrs int
	}

	var (
		mu    sync.Mutex
		calls []call
	)

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(4),
		fileproc.WithRecursive(),
		fileproc.WithOnError(func(err error, ioErrs, procErrs int) bool {
			mu.Lock()
			defer mu.Unlock()

			c := call{ioErrs: ioErrs, procErrs: procErrs}

			{
				var (
					errCase0 *fileproc.IOError
					errCase1 *fileproc.ProcessError
				)

				switch {
				case errors.As(err, &errCase0):
					c.errType = "io"
				case errors.As(err, &errCase1):
					c.errType = "proc"
				default:
					c.errType = "other"
				}
			}

			calls = append(calls, c)

			return true
		}),
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
		path := string(f.AbsPathBorrowed())
		if path == filepath.Join(root, testBadFile) {
			return nil, sentinel
		}

		return &path, nil
	}, opts...)

	// Only good.txt should be successful.
	assertStringSlicesEqual(t, resultPaths(results), []string{filepath.Join(root, "good.txt")})

	if len(errs) != 2 {
		t.Fatalf("expected 2 errors, got %d (%v)", len(errs), errs)
	}

	var (
		ioSeen   bool
		procSeen bool
	)

	for _, err := range errs {
		var (
			ioErr   *fileproc.IOError
			procErr *fileproc.ProcessError
		)

		switch {
		case errors.As(err, &ioErr):
			ioSeen = true

			assertIOError(t, err, filepath.Join(root, "blocked"), openOp)
		case errors.As(err, &procErr):
			procSeen = true

			if procErr.Path != filepath.Join(root, testBadFile) {
				t.Fatalf("unexpected proc path: %s", procErr.Path)
			}

			if !errors.Is(procErr, sentinel) {
				t.Fatalf("unexpected wrapped proc err: %v", procErr.Err)
			}
		default:
			t.Fatalf("unexpected error type: %T", err)
		}
	}

	if !ioSeen || !procSeen {
		t.Fatalf("expected both IO and Process errors, got io=%v proc=%v", ioSeen, procSeen)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(calls) != 2 {
		t.Fatalf("expected OnError to be called twice, got %d", len(calls))
	}

	// We expect one call to see the full totals (1,1), and the other to see the
	// state after its own increment: (1,0) or (0,1).
	var (
		saw11     bool
		saw10or01 bool
	)

	for _, c := range calls {
		if c.ioErrs == 1 && c.procErrs == 1 {
			saw11 = true
		}

		if (c.ioErrs == 1 && c.procErrs == 0) || (c.ioErrs == 0 && c.procErrs == 1) {
			saw10or01 = true
		}

		// Type-specific sanity.
		switch c.errType {
		case "io":
			if c.ioErrs < 1 {
				t.Fatalf("io error call had ioErrs=%d", c.ioErrs)
			}
		case "proc":
			if c.procErrs < 1 {
				t.Fatalf("proc error call had procErrs=%d", c.procErrs)
			}
		}
	}

	if !saw11 {
		t.Fatalf("expected one OnError call with counts (io=1, proc=1), got %+v", calls)
	}

	if !saw10or01 {
		t.Fatalf("expected one OnError call with counts (1,0) or (0,1), got %+v", calls)
	}
}

func Test_Process_Drops_IOErrors_When_OnError_Returns_False(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	root := t.TempDir()
	writeFile(t, root, "good.txt", []byte("good"))
	writeFile(t, root, filepath.Join("blocked", testSecretFile), []byte("secret"))

	blockedPath := filepath.Join(root, "blocked")

	info, err := os.Stat(blockedPath)
	if err != nil {
		t.Fatalf("stat blocked dir: %v", err)
	}

	origPerm := info.Mode().Perm()

	err = os.Chmod(blockedPath, 0o000)
	if err != nil {
		t.Fatalf("chmod blocked dir: %v", err)
	}

	t.Cleanup(func() {
		_ = os.Chmod(blockedPath, origPerm)
	})

	var (
		mu        sync.Mutex
		onErrorN  int
		ioErrPath string
		ioErrOp   string
		ioErrWrap error
	)

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(1),
		fileproc.WithRecursive(),
		fileproc.WithOnError(func(err error, ioErrs, procErrs int) bool {
			mu.Lock()
			defer mu.Unlock()

			onErrorN++

			var ioErr *fileproc.IOError
			if errors.As(err, &ioErr) {
				ioErrPath = ioErr.Path
				ioErrOp = ioErr.Op
				ioErrWrap = ioErr.Err
			}

			if ioErrs != 1 || procErrs != 0 {
				t.Fatalf("unexpected counts in OnError: io=%d proc=%d", ioErrs, procErrs)
			}

			return false
		}),
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
		path := string(f.AbsPathBorrowed())

		return &path, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("expected no collected errors, got %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{filepath.Join(root, "good.txt")})

	mu.Lock()
	defer mu.Unlock()

	if onErrorN != 1 {
		t.Fatalf("expected OnError to be called once, got %d", onErrorN)
	}

	if ioErrPath != filepath.Join(root, "blocked") {
		t.Fatalf("unexpected IOError path: %q", ioErrPath)
	}

	if ioErrOp != openOp {
		t.Fatalf("unexpected IOError op: %q", ioErrOp)
	}

	if ioErrWrap == nil {
		t.Fatal("expected wrapped errno in IOError")
	}
}

func Test_Process_Skips_File_When_Callback_Returns_ErrSkip(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "keep.txt", []byte("keep"))
	writeFile(t, root, "skip.txt", []byte("skip"))

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
		path := string(f.AbsPathBorrowed())
		if strings.HasSuffix(path, "skip.txt") {
			return nil, fileproc.ErrSkip
		}

		return &path, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{filepath.Join(root, "keep.txt")})
}

func Test_Process_Does_Not_Report_Error_When_Callback_Returns_ErrSkip(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "skip.txt", []byte("skip"))

	onErrorCalled := false

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.FileWorker) (*string, error) {
		return nil, fileproc.ErrSkip
	}, fileproc.WithOnError(func(_ error, _, _ int) bool {
		onErrorCalled = true

		return true
	}))

	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}

	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}

	if onErrorCalled {
		t.Fatal("OnError should not be called for ErrSkip")
	}
}
