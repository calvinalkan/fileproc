package fileproc_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/calvinalkan/fileproc"
)

func Test_Process_Drops_IOErrors_When_OnError_Returns_False(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	root := t.TempDir()
	writeFile(t, root, "good.txt", []byte("good"))
	writeFile(t, root, testSecretFile, []byte("secret"))

	secretPath := filepath.Join(root, testSecretFile)

	err := os.Chmod(secretPath, 0o000)
	if err != nil {
		t.Fatalf("chmod secret.txt: %v", err)
	}

	t.Cleanup(func() {
		_ = os.Chmod(secretPath, 0o600)
	})

	var (
		mu        sync.Mutex
		onErrorN  int
		ioErrPath string
		ioErrOp   string
		ioErrWrap error
	)

	opts := fileproc.Options{
		Workers: 1,
		OnError: func(err error, ioErrs, procErrs int) bool {
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
		},
	}

	var ok struct{}

	results, errs := fileproc.Process(t.Context(), root, func(_ []byte, _ []byte) (*struct{}, error) {
		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("expected no collected errors, got %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{"good.txt"})

	mu.Lock()
	defer mu.Unlock()

	if onErrorN != 1 {
		t.Fatalf("expected OnError to be called once, got %d", onErrorN)
	}

	if ioErrPath != testSecretFile {
		t.Fatalf("unexpected IOError path: %q", ioErrPath)
	}

	if ioErrOp != openOp {
		t.Fatalf("unexpected IOError op: %q", ioErrOp)
	}

	if ioErrWrap == nil {
		t.Fatal("expected wrapped errno in IOError")
	}
}

func Test_ProcessReader_Drops_IOErrors_When_OnError_Returns_False(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	root := t.TempDir()
	writeFile(t, root, "good.txt", []byte("good"))
	writeFile(t, root, testSecretFile, []byte("secret"))

	secretPath := filepath.Join(root, testSecretFile)

	err := os.Chmod(secretPath, 0o000)
	if err != nil {
		t.Fatalf("chmod secret.txt: %v", err)
	}

	t.Cleanup(func() {
		_ = os.Chmod(secretPath, 0o600)
	})

	var (
		mu        sync.Mutex
		onErrorN  int
		ioErrPath string
		ioErrOp   string
		ioErrWrap error
	)

	opts := fileproc.Options{
		Workers: 1,
		OnError: func(err error, ioErrs, procErrs int) bool {
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
		},
	}

	var ok struct{}

	results, errs := fileproc.ProcessReader(t.Context(), root, func(_ []byte, _ io.Reader) (*struct{}, error) {
		return &ok, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("expected no collected errors, got %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{"good.txt"})

	mu.Lock()
	defer mu.Unlock()

	if onErrorN != 1 {
		t.Fatalf("expected OnError to be called once, got %d", onErrorN)
	}

	if ioErrPath != testSecretFile {
		t.Fatalf("unexpected IOError path: %q", ioErrPath)
	}

	if ioErrOp != openOp {
		t.Fatalf("unexpected IOError op: %q", ioErrOp)
	}

	if ioErrWrap == nil {
		t.Fatal("expected wrapped errno in IOError")
	}
}
