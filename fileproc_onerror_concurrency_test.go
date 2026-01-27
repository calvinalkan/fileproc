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

func Test_Process_Counts_Errors_Correctly_When_Mixed_IO_And_Process_Errors_Concurrent(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	root := t.TempDir()

	writeFile(t, root, "good.txt", []byte("good"))
	writeFile(t, root, testBadFile, []byte("bad"))
	writeFile(t, root, testSecretFile, []byte("secret"))

	secretPath := filepath.Join(root, testSecretFile)

	chmodErr := os.Chmod(secretPath, 0o000)
	if chmodErr != nil {
		t.Fatalf("chmod secret.txt: %v", chmodErr)
	}

	t.Cleanup(func() {
		_ = os.Chmod(secretPath, 0o600)
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
		ok    struct{}
	)

	opts := fileproc.Options{
		Workers:            4,
		SmallFileThreshold: 1, // force pipelining (concurrent callbacks)
		OnError: func(err error, ioErrs, procErrs int) bool {
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
		},
	}

	results, errs := fileproc.Process(t.Context(), root, func(path, _ []byte) (*struct{}, error) {
		if string(path) == testBadFile {
			return nil, sentinel
		}

		return &ok, nil
	}, opts)

	// Only good.txt should be successful.
	assertStringSlicesEqual(t, resultPaths(results), []string{"good.txt"})

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

			if ioErr.Op != openOp {
				t.Fatalf("unexpected io op: %s", ioErr.Op)
			}

			if ioErr.Path != testSecretFile {
				t.Fatalf("unexpected io path: %s", ioErr.Path)
			}
		case errors.As(err, &procErr):
			procSeen = true

			if procErr.Path != testBadFile {
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

func Test_ProcessReader_Counts_Errors_Correctly_When_Mixed_IO_And_Process_Errors_Concurrent(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	root := t.TempDir()

	writeFile(t, root, "good.txt", []byte("good"))
	writeFile(t, root, testBadFile, []byte("bad"))
	writeFile(t, root, testSecretFile, []byte("secret"))

	secretPath := filepath.Join(root, testSecretFile)

	chmodErr := os.Chmod(secretPath, 0o000)
	if chmodErr != nil {
		t.Fatalf("chmod secret.txt: %v", chmodErr)
	}

	t.Cleanup(func() {
		_ = os.Chmod(secretPath, 0o600)
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
		ok    struct{}
	)

	opts := fileproc.Options{
		Workers:            4,
		SmallFileThreshold: 1,
		OnError: func(err error, ioErrs, procErrs int) bool {
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
		},
	}

	results, errs := fileproc.ProcessReader(t.Context(), root, func(path []byte, _ io.Reader) (*struct{}, error) {
		if string(path) == testBadFile {
			return nil, sentinel
		}

		return &ok, nil
	}, opts)

	assertStringSlicesEqual(t, resultPaths(results), []string{"good.txt"})

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

			if ioErr.Op != openOp {
				t.Fatalf("unexpected io op: %s", ioErr.Op)
			}

			if ioErr.Path != testSecretFile {
				t.Fatalf("unexpected io path: %s", ioErr.Path)
			}
		case errors.As(err, &procErr):
			procSeen = true

			if procErr.Path != testBadFile {
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
