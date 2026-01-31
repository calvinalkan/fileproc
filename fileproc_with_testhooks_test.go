//go:build fileproc_testhooks

package fileproc

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func Test_ReaddirError_BestEffort_NonRecursive(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt")
	writeTestFile(t, root, "b.txt")

	errSentinel := errors.New("readdir boom")

	restore := setReadDirBatchHook(func(rh readdirHandle, buf []byte, suffix string, batch *nameBatch, reportSubdir func(nulTermName)) error {
		// Hook is global to this test binary; keep tests non-parallel to avoid interference.
		// Inject a non-EOF error after providing some names to test best-effort behavior.
		batch.addName(nulTermName(append([]byte("a.txt"), 0)))
		batch.addName(nulTermName(append([]byte("b.txt"), 0)))

		return errSentinel
	})
	defer restore()

	results, errs := Process(t.Context(), root, func(_ *File, _ *Worker) (*struct{}, error) {
		return &struct{}{}, nil
	})

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	assertIOError(t, errs[0], ".", "readdir")

	if !errors.Is(errs[0], errSentinel) {
		t.Fatalf("expected error to wrap %q, got %v", errSentinel, errs[0])
	}
}

func Test_ReaddirError_BestEffort_Pipelined(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt")
	writeTestFile(t, root, "b.txt")
	writeTestFile(t, root, "c.txt")

	errSentinel := errors.New("readdir boom")
	callCount := 0

	restore := setReadDirBatchHook(func(rh readdirHandle, buf []byte, suffix string, batch *nameBatch, reportSubdir func(nulTermName)) error {
		// Hook is global to this test binary; keep tests non-parallel to avoid interference.
		callCount++
		switch callCount {
		case 1:
			// Seed enough names to trigger pipelining without error.
			batch.addName(nulTermName(append([]byte("a.txt"), 0)))
			batch.addName(nulTermName(append([]byte("b.txt"), 0)))

			return nil
		case 2:
			// Next batch returns a non-EOF error but still provides names.
			batch.addName(nulTermName(append([]byte("c.txt"), 0)))

			return errSentinel
		default:
			return io.EOF
		}
	})
	defer restore()

	opts := []Option{
		WithSmallFileThreshold(1),
		WithWorkers(2),
	}

	results, errs := Process(t.Context(), root, func(_ *File, _ *Worker) (*struct{}, error) {
		return &struct{}{}, nil
	}, opts...)

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	assertIOError(t, errs[0], ".", "readdir")

	if !errors.Is(errs[0], errSentinel) {
		t.Fatalf("expected error to wrap %q, got %v", errSentinel, errs[0])
	}
}

func Test_ReaddirError_BestEffort_Recursive(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "a.txt")
	writeTestFile(t, root, "b.txt")

	errSentinel := errors.New("readdir boom")

	restore := setReadDirBatchHook(func(rh readdirHandle, buf []byte, suffix string, batch *nameBatch, reportSubdir func(nulTermName)) error {
		// Hook is global to this test binary; keep tests non-parallel to avoid interference.
		// Inject a non-EOF error after providing some names to test best-effort behavior.
		batch.addName(nulTermName(append([]byte("a.txt"), 0)))
		batch.addName(nulTermName(append([]byte("b.txt"), 0)))

		return errSentinel
	})
	defer restore()

	opts := []Option{
		WithRecursive(),
		WithWorkers(1),
		WithSmallFileThreshold(10),
	}

	results, errs := Process(t.Context(), root, func(_ *File, _ *Worker) (*struct{}, error) {
		return &struct{}{}, nil
	}, opts...)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	assertIOError(t, errs[0], ".", "readdir")

	if !errors.Is(errs[0], errSentinel) {
		t.Fatalf("expected error to wrap %q, got %v", errSentinel, errs[0])
	}
}

func writeTestFile(t *testing.T, root, name string) {
	t.Helper()

	path := filepath.Join(root, name)
	if err := os.WriteFile(path, []byte(name), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func assertIOError(t *testing.T, err error, path, op string) {
	t.Helper()

	var ioErr *IOError
	if !errors.As(err, &ioErr) {
		t.Fatalf("expected IOError, got %T", err)
	}

	if ioErr.Path != path {
		t.Fatalf("unexpected error path: got %q want %q", ioErr.Path, path)
	}

	if ioErr.Op != op {
		t.Fatalf("unexpected error op: got %q want %q", ioErr.Op, op)
	}
}
