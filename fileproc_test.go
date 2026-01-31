package fileproc_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/calvinalkan/fileproc"
)

const (
	windowsOS = "windows"
	invalidFD = ^uintptr(0)

	testNamePad     = 200
	testNumFilesBig = 500
	testNumFilesMed = 300
	testNumDirs     = 201
	testBadFile     = "bad.txt"
	testSecretFile  = "secret.txt"
	openOp          = "open"
)

// ============================================================================
// File.AbsPathBorrowed() tests
// ============================================================================

func Test_File_AbsPathBorrowed_Returns_Correct_Path_When_NonRecursive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "alpha.txt", []byte("alpha"))
	writeFile(t, root, "beta.md", []byte("beta"))

	opts := []fileproc.Option{
		fileproc.WithWorkers(1),
		fileproc.WithSmallFileThreshold(1000),
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]bool)
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		mu.Lock()

		seen[string(f.AbsPathBorrowed())] = true

		mu.Unlock()

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if !seen[filepath.Join(root, "alpha.txt")] || !seen[filepath.Join(root, "beta.md")] {
		t.Fatalf("missing expected paths: %v", seen)
	}
}

func Test_File_AbsPathBorrowed_Returns_Correct_Path_When_Recursive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "top.txt", []byte("top"))
	writeFile(t, root, filepath.Join("sub", "nested.txt"), []byte("nested"))
	writeFile(t, root, filepath.Join("sub", "deep", "file.txt"), []byte("deep"))

	opts := []fileproc.Option{
		fileproc.WithRecursive(),
		fileproc.WithWorkers(1),
		fileproc.WithSmallFileThreshold(1000),
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]bool)
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		mu.Lock()

		seen[string(f.AbsPathBorrowed())] = true

		mu.Unlock()

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	want := []string{
		filepath.Join(root, "top.txt"),
		filepath.Join(root, "sub", "nested.txt"),
		filepath.Join(root, "sub", "deep", "file.txt"),
	}
	for _, w := range want {
		if !seen[w] {
			t.Fatalf("missing expected path %q: %v", w, seen)
		}
	}
}

func Test_File_AbsPathBorrowed_Copy_Remains_Valid_When_Process_Returns(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	type pathHolder struct {
		path string
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*pathHolder, error) {
		return &pathHolder{path: string(f.AbsPathBorrowed())}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// The path should still be valid here (arena not released)
	if results[0].path != filepath.Join(root, "test.txt") {
		t.Fatalf("expected path %q, got %q", filepath.Join(root, "test.txt"), results[0].path)
	}
}

// ============================================================================
// File.Stat() tests
// ============================================================================

func Test_File_Stat_Returns_Correct_Metadata_When_Called(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("hello world")
	writeFile(t, root, "data.txt", content)

	info, err := os.Stat(filepath.Join(root, "data.txt"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var gotStat fileproc.Stat

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		var statErr error

		gotStat, statErr = f.Stat()
		if statErr != nil {
			return nil, fmt.Errorf("stat: %w", statErr)
		}

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if gotStat.Size != int64(len(content)) {
		t.Fatalf("size: got %d, want %d", gotStat.Size, len(content))
	}

	if gotStat.ModTime != info.ModTime().UnixNano() {
		t.Fatalf("modtime: got %d, want %d", gotStat.ModTime, info.ModTime().UnixNano())
	}
}

func Test_File_Stat_Is_Available_When_File_Unreadable(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("chmod 000 unsupported on windows")
	}

	root := t.TempDir()
	writeFile(t, root, "secret.txt", []byte("secret"))

	// Make file unreadable
	filePath := filepath.Join(root, "secret.txt")

	err := os.Chmod(filePath, 0)
	if err != nil {
		t.Fatalf("chmod: %v", err)
	}

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var gotStat fileproc.Stat

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		// Just access stat, don't try to read
		var statErr error

		gotStat, statErr = f.Stat()
		if statErr != nil {
			return nil, fmt.Errorf("stat: %w", statErr)
		}

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if gotStat.Size != int64(len("secret")) {
		t.Fatalf("expected size 6, got %d", gotStat.Size)
	}
}

// ============================================================================
// File.Bytes() tests
// ============================================================================

func Test_File_Bytes_Reads_Full_Content_When_Called(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("hello world, this is test content")
	writeFile(t, root, "data.txt", content)

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var gotData []byte

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = data

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if !bytes.Equal(gotData, content) {
		t.Fatalf("content mismatch: got %q, want %q", gotData, content)
	}
}

func Test_File_Bytes_Returns_Arena_Backed_Slice_When_Called(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("arena test content")
	writeFile(t, root, "test.txt", content)

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	type dataHolder struct {
		data []byte
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*dataHolder, error) {
		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}
		// Store the slice directly without copying
		return &dataHolder{data: data}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// The data should still be valid here (arena not released)
	if !bytes.Equal(results[0].data, content) {
		t.Fatalf("expected %q, got %q", content, results[0].data)
	}
}

func Test_File_Bytes_Returns_NonNil_Empty_Slice_When_File_Empty(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "empty.txt", nil)

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var (
		gotData []byte
		gotNil  bool
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = data
		gotNil = data == nil

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if gotNil {
		t.Fatal("expected non-nil slice for empty file")
	}

	if len(gotData) != 0 {
		t.Fatalf("expected empty slice, got len=%d", len(gotData))
	}
}

func Test_File_Bytes_Returns_Actual_Content_When_File_Shrunk_Since_Stat(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	filePath := filepath.Join(root, "shrinking.txt")

	// Write initial content
	initialContent := []byte("initial longer content here")

	err := os.WriteFile(filePath, initialContent, 0o644)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var (
		gotData  []byte
		statSize int64
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		st, statErr := f.Stat()
		if statErr != nil {
			return nil, fmt.Errorf("stat: %w", statErr)
		}

		statSize = st.Size

		// Shrink the file after stat but before Bytes()
		writeErr := os.WriteFile(filePath, []byte("short"), 0o644)
		if writeErr != nil {
			return nil, fmt.Errorf("write: %w", writeErr)
		}

		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = data

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// stat reported original size
	if statSize != int64(len(initialContent)) {
		t.Fatalf("stat size: got %d, want %d", statSize, len(initialContent))
	}

	// Bytes() returns actual (shorter) content
	if string(gotData) != "short" {
		t.Fatalf("data: got %q, want 'short'", gotData)
	}
}

func Test_File_Bytes_Returns_Full_Content_When_File_Grew_Since_Stat(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	filePath := filepath.Join(root, "growing.txt")

	// Write initial content
	initialContent := []byte("small")

	err := os.WriteFile(filePath, initialContent, 0o644)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var (
		gotData  []byte
		statSize int64
	)

	grownContent := []byte("this is much longer content that grew after stat")

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		st, statErr := f.Stat()
		if statErr != nil {
			return nil, fmt.Errorf("stat: %w", statErr)
		}

		statSize = st.Size

		// Grow the file after stat but before Bytes()
		writeErr := os.WriteFile(filePath, grownContent, 0o644)
		if writeErr != nil {
			return nil, fmt.Errorf("write: %w", writeErr)
		}

		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = data

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// stat reported original size
	if statSize != int64(len(initialContent)) {
		t.Fatalf("stat size: got %d, want %d", statSize, len(initialContent))
	}

	// Bytes() returns actual (larger) content
	if !bytes.Equal(gotData, grownContent) {
		t.Fatalf("data: got %q, want %q", gotData, grownContent)
	}
}

func Test_File_Bytes_Returns_Error_When_Called_Second_Time(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var firstErr, secondErr error

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		_, firstErr = f.Bytes()
		_, secondErr = f.Bytes()

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if firstErr != nil {
		t.Fatalf("first Bytes() should succeed: %v", firstErr)
	}

	if secondErr == nil {
		t.Fatal("second Bytes() should return error")
	}
}

func Test_File_Bytes_Returns_Error_When_Called_After_Read(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var readErr, bytesErr error

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		buf := make([]byte, 0, 4)
		buf = buf[:4]
		_, readErr = f.Read(buf)
		_, bytesErr = f.Bytes()

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if readErr != nil {
		t.Fatalf("Read() should succeed: %v", readErr)
	}

	if bytesErr == nil {
		t.Fatal("Bytes() after Read() should return error")
	}
}

func Test_File_Bytes_Opens_File_Lazily_When_Called(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("chmod 000 unsupported on windows")
	}

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	// Make file unreadable
	filePath := filepath.Join(root, "test.txt")

	err := os.Chmod(filePath, 0)
	if err != nil {
		t.Fatalf("chmod: %v", err)
	}

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var (
		statOK   bool
		bytesErr error
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		// Stat should work without opening file
		st, statErr := f.Stat()
		statOK = statErr == nil && st.Size > 0
		// Bytes() should fail because file is unreadable
		_, bytesErr = f.Bytes()

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if !statOK {
		t.Fatal("Stat() should work without opening file")
	}

	if bytesErr == nil {
		t.Fatal("Bytes() should fail for unreadable file")
	}
}

func Test_File_Bytes_Works_Correctly_When_File_Large(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	// Create a 1MB file
	content := make([]byte, 0, 1024*1024)
	content = content[:1024*1024]

	for i := range content {
		content[i] = byte(i % 256)
	}

	writeFile(t, root, "large.bin", content)

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var gotData []byte

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = data

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if !bytes.Equal(gotData, content) {
		t.Fatal("content mismatch for large file")
	}
}

// ============================================================================
// File.Read() tests
// ============================================================================

func Test_File_Read_Implements_IoReader_When_Called(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("hello world")
	writeFile(t, root, "test.txt", content)

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var gotData []byte

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		data, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = data

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if !bytes.Equal(gotData, content) {
		t.Fatalf("content mismatch: got %q, want %q", gotData, content)
	}
}

func Test_File_Read_Returns_Error_When_Called_After_Bytes(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var bytesErr, readErr error

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		_, bytesErr = f.Bytes()
		buf := make([]byte, 0, 4)
		buf = buf[:4]
		_, readErr = f.Read(buf)

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if bytesErr != nil {
		t.Fatalf("Bytes() should succeed: %v", bytesErr)
	}

	if readErr == nil {
		t.Fatal("Read() after Bytes() should return error")
	}
}

func Test_File_Read_Works_Correctly_When_Called_Multiple_Times(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("hello world")
	writeFile(t, root, "test.txt", content)

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var gotData []byte

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		buf := make([]byte, 0, 5)

		buf = buf[:5]
		for {
			n, err := f.Read(buf)
			if n > 0 {
				gotData = append(gotData, buf[:n]...)
			}

			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				return nil, fmt.Errorf("test: %w", err)
			}
		}

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if !bytes.Equal(gotData, content) {
		t.Fatalf("content mismatch: got %q, want %q", gotData, content)
	}
}

func Test_File_Read_Opens_File_Lazily_When_Called(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("chmod 000 unsupported on windows")
	}

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	// Make file unreadable
	filePath := filepath.Join(root, "test.txt")

	err := os.Chmod(filePath, 0)
	if err != nil {
		t.Fatalf("chmod: %v", err)
	}

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var (
		statOK  bool
		readErr error
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		// Stat should work without opening file
		st, statErr := f.Stat()
		statOK = statErr == nil && st.Size > 0
		// Read() should fail because file is unreadable
		buf := make([]byte, 0, 4)
		buf = buf[:4]
		_, readErr = f.Read(buf)

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if !statOK {
		t.Fatal("Stat() should work without opening file")
	}

	if readErr == nil {
		t.Fatal("Read() should fail for unreadable file")
	}
}

// ============================================================================
// File.Fd() tests
// ============================================================================

func Test_File_Fd_Returns_Valid_Fd_When_Called(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var fd uintptr

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		fd = f.Fd()

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if fd == invalidFD {
		t.Fatal("expected valid fd, got -1")
	}
}

func Test_File_Fd_Opens_File_When_Not_Already_Open(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var fd uintptr

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		// Don't call Bytes() or Read(), just Fd()
		fd = f.Fd()

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if fd == invalidFD {
		t.Fatal("expected valid fd, got -1")
	}
}

func Test_File_Fd_Works_When_Called_After_Bytes(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var fd uintptr

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		_, _ = f.Bytes()
		fd = f.Fd()

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if fd == invalidFD {
		t.Fatal("expected valid fd after Bytes(), got -1")
	}
}

func Test_File_Fd_Works_When_Called_After_Read(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var fd uintptr

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		buf := make([]byte, 0, 4)
		buf = buf[:4]
		_, _ = f.Read(buf)
		fd = f.Fd()

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if fd == invalidFD {
		t.Fatal("expected valid fd after Read(), got -1")
	}
}

// ============================================================================
// Worker.Buf tests
// ============================================================================

func Test_Worker_Buf_Returns_Zero_Len_With_Cap_When_Called(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var bufLen, bufCap int

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, scratch *fileproc.Worker) (*struct{}, error) {
		buf := scratch.Buf(4096)
		bufLen = len(buf)
		bufCap = cap(buf)

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if bufLen != 0 {
		t.Fatalf("expected len=0, got %d", bufLen)
	}

	if bufCap < 4096 {
		t.Fatalf("expected cap>=4096, got %d", bufCap)
	}
}

func Test_Worker_Buf_Grows_Capacity_When_Larger_Request(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("a"))
	writeFile(t, root, "b.txt", []byte("b"))

	opts := []fileproc.Option{
		fileproc.WithWorkers(1),
		fileproc.WithSmallFileThreshold(1000),
	}

	var caps []int

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, scratch *fileproc.Worker) (*struct{}, error) {
		// First file requests 1024, second requests 8192
		size := 1024
		if len(caps) > 0 {
			size = 8192
		}

		buf := scratch.Buf(size)
		caps = append(caps, cap(buf))

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if len(caps) != 2 {
		t.Fatalf("expected 2 capacity readings, got %d", len(caps))
	}

	if caps[0] < 1024 {
		t.Fatalf("first cap should be >= 1024, got %d", caps[0])
	}

	if caps[1] < 8192 {
		t.Fatalf("second cap should be >= 8192, got %d", caps[1])
	}
}

func Test_Worker_Buf_Reuses_Buffer_When_Same_Worker(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("a"))
	writeFile(t, root, "b.txt", []byte("b"))

	opts := []fileproc.Option{
		fileproc.WithWorkers(1),
		fileproc.WithSmallFileThreshold(1000),
	}

	var ptrs []uintptr

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, scratch *fileproc.Worker) (*struct{}, error) {
		buf := scratch.Buf(1024)
		// Expand to get actual backing array pointer
		buf = buf[:cap(buf)]
		if len(buf) > 0 {
			ptrs = append(ptrs, uintptr(unsafe.Pointer(&buf[0])))
		}

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if len(ptrs) != 2 {
		t.Fatalf("expected 2 pointers, got %d", len(ptrs))
	}

	// Same backing array should be reused
	if ptrs[0] != ptrs[1] {
		t.Fatal("worker buffer should be reused across files in same worker")
	}
}

// ============================================================================
// Arena lifetime tests
// ============================================================================

func Test_Arena_Multiple_Files_Dont_Interfere_When_Sequential(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("alpha"))
	writeFile(t, root, "b.txt", []byte("bravo"))
	writeFile(t, root, "c.txt", []byte("charlie"))

	opts := []fileproc.Option{
		fileproc.WithWorkers(1),
		fileproc.WithSmallFileThreshold(1000),
	}

	type fileData struct {
		path string
		data []byte
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*fileData, error) {
		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		return &fileData{path: string(f.AbsPathBorrowed()), data: data}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	expected := map[string]string{
		filepath.Join(root, "a.txt"): "alpha",
		filepath.Join(root, "b.txt"): "bravo",
		filepath.Join(root, "c.txt"): "charlie",
	}

	for _, r := range results {
		want, ok := expected[r.path]
		if !ok {
			t.Fatalf("unexpected path: %s", r.path)
		}

		if string(r.data) != want {
			t.Fatalf("data mismatch for %s: got %q, want %q", r.path, r.data, want)
		}
	}
}

func Test_Arena_Subslices_Remain_Valid_When_Process_Returns(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("prefix:middle:suffix")
	writeFile(t, root, "test.txt", content)

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	type parts struct {
		prefix []byte
		middle []byte
		suffix []byte
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*parts, error) {
		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}
		// Create subslices
		return &parts{
			prefix: data[0:6],   // "prefix"
			middle: data[7:13],  // "middle"
			suffix: data[14:20], // "suffix"
		}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Subslices should still be valid
	if string(results[0].prefix) != "prefix" {
		t.Fatalf("prefix: got %q", results[0].prefix)
	}

	if string(results[0].middle) != "middle" {
		t.Fatalf("middle: got %q", results[0].middle)
	}

	if string(results[0].suffix) != "suffix" {
		t.Fatalf("suffix: got %q", results[0].suffix)
	}
}

// ============================================================================
// Mutual exclusion tests
// ============================================================================

func Test_Mutual_Exclusion_Returns_Error_When_Bytes_Then_Read(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var bytesErr, readErr error

	_, _ = fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		_, bytesErr = f.Bytes()
		buf := make([]byte, 0, 4)
		buf = buf[:4]
		_, readErr = f.Read(buf)

		return &struct{}{}, nil
	}, opts...)

	if bytesErr != nil {
		t.Fatalf("Bytes() should succeed: %v", bytesErr)
	}

	if readErr == nil {
		t.Fatal("Read() after Bytes() should return error")
	}
}

func Test_Mutual_Exclusion_Returns_Error_When_Read_Then_Bytes(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var readErr, bytesErr error

	_, _ = fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		buf := make([]byte, 0, 4)
		buf = buf[:4]
		_, readErr = f.Read(buf)
		_, bytesErr = f.Bytes()

		return &struct{}{}, nil
	}, opts...)

	if readErr != nil {
		t.Fatalf("Read() should succeed: %v", readErr)
	}

	if bytesErr == nil {
		t.Fatal("Bytes() after Read() should return error")
	}
}

// ============================================================================
// Concurrency tests
// ============================================================================

func Test_Concurrency_Multiple_Workers_Have_Independent_Arenas_When_Parallel(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	// Create enough files to ensure multiple workers are used
	for i := range 100 {
		writeFile(t, root, filepath.Join("sub", "file"+string(rune('0'+i%10))+string(rune('0'+i/10))+".txt"), []byte("content"))
	}

	opts := []fileproc.Option{
		fileproc.WithRecursive(),
		fileproc.WithWorkers(4),
		fileproc.WithSmallFileThreshold(10),
	}

	type holder struct {
		data []byte
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*holder, error) {
		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		return &holder{data: data}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}

	// Verify all data is correct
	for _, r := range results {
		if string(r.data) != "content" {
			t.Fatalf("data mismatch: got %q", r.data)
		}
	}
}

func Test_Concurrency_Multiple_Workers_Have_Independent_WorkerBuf_When_Parallel(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	for i := range 50 {
		writeFile(t, root, "file"+string(rune('0'+i%10))+string(rune('0'+i/10))+".txt", []byte("content"))
	}

	opts := []fileproc.Option{
		fileproc.WithWorkers(4),
		fileproc.WithSmallFileThreshold(10),
	}

	type scratchResult struct {
		data []byte
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, scratch *fileproc.Worker) (*scratchResult, error) {
		// Write file path into scratch buffer
		buf := scratch.Buf(256)
		buf = append(buf, f.AbsPathBorrowed()...)

		// Copy to result (scratch only valid during callback)
		return &scratchResult{data: append([]byte(nil), buf...)}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 50 {
		t.Fatalf("expected 50 results, got %d", len(results))
	}

	// Verify each result has correct path (scratch wasn't corrupted by other workers)
	seen := make(map[string]bool)

	for _, r := range results {
		path := string(r.data)
		if seen[path] {
			t.Fatalf("duplicate path: %s", path)
		}

		seen[path] = true
	}
}

// ============================================================================
// Error handling tests
// ============================================================================

func Test_ErrorHandling_Bytes_IO_Error_Propagates_When_File_Unreadable(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("chmod 000 unsupported on windows")
	}

	root := t.TempDir()
	writeFile(t, root, "secret.txt", []byte("secret"))

	filePath := filepath.Join(root, "secret.txt")

	err := os.Chmod(filePath, 0)
	if err != nil {
		t.Fatalf("chmod: %v", err)
	}

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		_, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		return &struct{}{}, nil
	}, opts...)

	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	var procErr *fileproc.ProcessError
	if !errors.As(errs[0], &procErr) {
		t.Fatalf("expected ProcessError, got %T", errs[0])
	}
}

func Test_ErrorHandling_Read_IO_Error_Propagates_When_File_Unreadable(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("chmod 000 unsupported on windows")
	}

	root := t.TempDir()
	writeFile(t, root, "secret.txt", []byte("secret"))

	filePath := filepath.Join(root, "secret.txt")

	err := os.Chmod(filePath, 0)
	if err != nil {
		t.Fatalf("chmod: %v", err)
	}

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		buf := make([]byte, 0, 4)
		buf = buf[:4]

		_, err := f.Read(buf)
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		return &struct{}{}, nil
	}, opts...)

	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}
}

func Test_ErrorHandling_File_Handle_Closed_When_Callback_Errors(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("alpha"))
	writeFile(t, root, "b.txt", []byte("bravo"))

	opts := []fileproc.Option{
		fileproc.WithWorkers(1),
		fileproc.WithSmallFileThreshold(10),
	}

	sentinel := errors.New("boom")

	var calls int

	_, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		calls++
		// Open file via Bytes
		_, _ = f.Bytes()

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
}

func Test_ErrorHandling_File_Handle_Closed_When_Bytes_Errors(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("chmod 000 unsupported on windows")
	}

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("alpha"))
	writeFile(t, root, "b.txt", []byte("bravo"))

	// Make first file unreadable
	err := os.Chmod(filepath.Join(root, "a.txt"), 0)
	if err != nil {
		t.Fatalf("chmod: %v", err)
	}

	opts := []fileproc.Option{
		fileproc.WithWorkers(1),
		fileproc.WithSmallFileThreshold(10),
	}

	var (
		calls       int
		successData []byte
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		calls++

		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		successData = data

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

// ============================================================================
// Edge cases
// ============================================================================

func Test_EdgeCase_Zero_Size_File_Returns_Empty_Slice_When_Bytes_Called(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "empty.txt", nil)

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var gotData []byte

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		st, statErr := f.Stat()
		if statErr != nil {
			return nil, fmt.Errorf("stat: %w", statErr)
		}

		if st.Size != 0 {
			t.Errorf("expected size 0, got %d", st.Size)
		}

		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = data

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if gotData == nil {
		t.Fatal("expected non-nil slice")
	}

	if len(gotData) != 0 {
		t.Fatalf("expected empty slice, got len=%d", len(gotData))
	}
}

func Test_EdgeCase_File_Deleted_Returns_Error_When_Bytes_Called(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	filePath := filepath.Join(root, "doomed.txt")

	err := os.WriteFile(filePath, []byte("doomed"), 0o644)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		// Delete file after stat but before Bytes()
		removeErr := os.Remove(filePath)
		if removeErr != nil {
			return nil, fmt.Errorf("remove: %w", removeErr)
		}

		_, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		return &struct{}{}, nil
	}, opts...)

	// Should have an error (file not found)
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func Test_EdgeCase_File_Grew_From_Empty_Returns_Content_When_Bytes_Called(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	filePath := filepath.Join(root, "growing.txt")

	// Start with empty file
	err := os.WriteFile(filePath, nil, 0o644)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var (
		statSize int64
		gotData  []byte
	)

	grownContent := []byte("content that appeared after stat")

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		st, statErr := f.Stat()
		if statErr != nil {
			return nil, fmt.Errorf("stat: %w", statErr)
		}

		statSize = st.Size

		// Write content after stat
		writeErr := os.WriteFile(filePath, grownContent, 0o644)
		if writeErr != nil {
			return nil, fmt.Errorf("write: %w", writeErr)
		}

		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = data

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if statSize != 0 {
		t.Fatalf("stat size should be 0, got %d", statSize)
	}

	if !bytes.Equal(gotData, grownContent) {
		t.Fatalf("expected %q, got %q", grownContent, gotData)
	}
}

// ============================================================================
// Tests ported from old ProcessStat()
// ============================================================================

func Test_Process_Returns_TopLevel_Files_And_Skips_Symlinks_When_NonRecursive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.md", []byte("alpha"))
	writeFile(t, root, "b.txt", []byte("bravo"))
	writeFile(t, root, "empty.md", nil)
	writeFile(t, root, filepath.Join("sub", "c.md"), []byte("charlie"))
	writeSymlink(t, root, "a.md", "link.md")

	opts := []fileproc.Option{
		fileproc.WithWorkers(1),
		fileproc.WithSmallFileThreshold(1000),
	}

	var (
		mu   sync.Mutex
		got  = make(map[string]fileproc.Stat)
		seen = make(map[string]struct{})
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*string, error) {
		st, statErr := f.Stat()
		if statErr != nil {
			return nil, fmt.Errorf("stat: %w", statErr)
		}

		path := string(f.AbsPathBorrowed())

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
		fileproc.WithWorkers(1),
		fileproc.WithSmallFileThreshold(1000),
	}

	var (
		mu  sync.Mutex
		got = make(map[string]struct{})
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*string, error) {
		path := string(f.AbsPathBorrowed())

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

func Test_Process_Reads_Content_Via_File_When_Used(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("hello")
	writeFile(t, root, "data.txt", content)

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var (
		mu     sync.Mutex
		got    []byte
		called bool
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
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

	err := os.Chmod(filePath, 0)
	if err != nil {
		t.Fatalf("chmod %s: %v", filePath, err)
	}

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	var called bool

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
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

func Test_Process_Returns_ProcessError_When_Callback_Fails(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, testBadFile, []byte("bad"))

	opts := []fileproc.Option{fileproc.WithWorkers(1)}

	sentinel := errors.New("boom")

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
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
		fileproc.WithWorkers(1),
		fileproc.WithSmallFileThreshold(10),
	}

	sentinel := errors.New("boom")
	mismatch := errors.New("mismatch")

	var (
		calls         int
		errorInjected bool
	)

	_, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		calls++

		data, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("read file: %w", err)
		}

		path := string(f.AbsPathBorrowed())

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

func Test_Process_Silently_Skips_When_File_Becomes_Directory(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "normal.txt", []byte("normal"))
	writeFile(t, root, "will_become_dir.txt", []byte("temp"))

	racePath := filepath.Join(root, "will_become_dir.txt")

	opts := []fileproc.Option{
		fileproc.WithWorkers(1),
		fileproc.WithSmallFileThreshold(1000),
	}

	var callCount int

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		callCount++

		// On first call for the racy file, replace it with a directory
		if string(f.AbsPathBorrowed()) == racePath {
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
			_, err = f.Bytes()
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
		fileproc.WithWorkers(1),
		fileproc.WithSmallFileThreshold(1000),
	}

	var callCount int

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		callCount++

		// On the racy file, replace it with a symlink before reading
		if string(f.AbsPathBorrowed()) == racePath {
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
			_, err = f.Bytes()
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

func Test_Process_Returns_No_Results_When_Context_Already_Canceled(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "data.txt", []byte("data"))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	count := 0

	results, errs := fileproc.Process(ctx, root, func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		count++

		return &struct{}{}, nil
	}, fileproc.WithWorkers(1))

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

func Test_Process_Returns_No_Results_When_Directory_Empty(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	count := 0

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		count++

		return &struct{}{}, nil
	}, fileproc.WithWorkers(1))

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

func Test_Process_Returns_Error_When_Suffix_Has_Nul(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "file.txt", []byte("data"))

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
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

func Test_Process_Returns_IOError_When_Path_Contains_Nul(t *testing.T) {
	t.Parallel()

	count := 0

	results, errs := fileproc.Process(t.Context(), "bad\x00path", func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
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

func Test_Process_Returns_IOError_When_Path_Is_File(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	filePath := filepath.Join(root, "notadir.txt")
	writeFile(t, root, "notadir.txt", []byte("data"))

	count := 0

	results, errs := fileproc.Process(t.Context(), filePath, func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
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

	var ioErr *fileproc.IOError
	if !errors.As(errs[0], &ioErr) {
		t.Fatalf("expected IOError, got %T", errs[0])
	}

	if ioErr.Path != filePath {
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

	results, errs := fileproc.Process(t.Context(), symPath, func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
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

	var ioErr *fileproc.IOError
	if !errors.As(errs[0], &ioErr) {
		t.Fatalf("expected IOError, got %T", errs[0])
	}

	if ioErr.Path != symPath {
		t.Fatalf("unexpected error path: %s", ioErr.Path)
	}

	if ioErr.Op != openOp {
		t.Fatalf("unexpected error op: %s", ioErr.Op)
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

	results, errs := fileproc.Process(t.Context(), missing, func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
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

	var ioErr *fileproc.IOError
	if !errors.As(errs[0], &ioErr) {
		t.Fatalf("expected IOError, got %T", errs[0])
	}

	if ioErr.Path != missing {
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

	_, _ = fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		panic("boom")
	}, fileproc.WithWorkers(1))
}

func Test_Process_Skips_Result_When_Callback_Returns_Nil(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "skip.txt", []byte("data"))

	count := 0

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		count++

		var result *struct{}

		return result, nil
	}, fileproc.WithWorkers(1))

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
// Pipelined processing (non-recursive)
// ============================================================================

func Test_Process_Processes_All_Files_When_Pipelined_Workers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	wantPaths := make([]string, 0, 8)

	for i := range 8 {
		name := "f" + strings.Repeat("x", i) + ".txt"
		writeFile(t, root, name, []byte("data"))
		wantPaths = append(wantPaths, filepath.Join(root, name))
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*string, error) {
		path := string(f.AbsPathBorrowed())

		return &path, nil
	}, fileproc.WithWorkers(4), fileproc.WithSmallFileThreshold(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), wantPaths)
}

func Test_Process_Processes_All_Files_When_Pipelined_With_Multiple_ReadDirBatches(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	const workers = 4

	pad := strings.Repeat("x", testNamePad)

	wantPaths := make([]string, 0, testNumFilesMed)
	for i := range testNumFilesMed {
		name := fmt.Sprintf("f-%04d-%s.txt", i, pad)
		writeFile(t, root, name, []byte("x"))
		wantPaths = append(wantPaths, filepath.Join(root, name))
	}

	opts := []fileproc.Option{
		fileproc.WithWorkers(workers),
		fileproc.WithSmallFileThreshold(1),
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]int, testNumFilesMed)
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*string, error) {
		path := string(f.AbsPathBorrowed())

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

func Test_Process_Reads_Content_When_Pipelined(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	const files = 40

	wantPaths := make([]string, 0, files)
	wantData := make(map[string]string, files)

	for i := range files {
		name := fmt.Sprintf("f-%03d.txt", i)
		content := fmt.Sprintf("data-%03d", i)
		writeFile(t, root, name, []byte(content))
		fullPath := filepath.Join(root, name)
		wantPaths = append(wantPaths, fullPath)
		wantData[fullPath] = content
	}

	opts := []fileproc.Option{
		fileproc.WithWorkers(4),
		fileproc.WithSmallFileThreshold(1),
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]string, files)
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*string, error) {
		data, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("read: %w", err)
		}

		path := string(f.AbsPathBorrowed())

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

func Test_Process_Drops_Errors_When_OnError_Returns_False(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "bad.txt", []byte("boom"))

	var (
		mu     sync.Mutex
		counts []struct{ ioErrs, procErrs int }
	)

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		return nil, errors.New("fail")
	}, fileproc.WithWorkers(1), fileproc.WithOnError(func(_ error, ioErrs, procErrs int) bool {
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

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*string, error) {
		path := string(f.AbsPathBorrowed())
		if path == filepath.Join(root, "bad.txt") {
			return nil, errors.New("fail")
		}

		mu.Lock()

		paths = append(paths, path)

		mu.Unlock()

		return &path, nil
	}, fileproc.WithWorkers(1), fileproc.WithOnError(func(_ error, _, _ int) bool {
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

	_, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		return nil, errors.New("fail")
	}, fileproc.WithWorkers(1), fileproc.WithOnError(func(_ error, ioErrs, procErrs int) bool {
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

func Test_Process_Returns_Result_Value_When_Callback_Returns_Value(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "value.txt", []byte("value"))

	value := 42

	results, errs := fileproc.Process(t.Context(), root, func(_ *fileproc.File, _ *fileproc.Worker) (*int, error) {
		return &value, nil
	}, fileproc.WithWorkers(1))

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

func Test_Process_Skips_NonRegular_When_Fifo_Present(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("mkfifo unsupported on windows")
	}

	root := t.TempDir()
	writeFile(t, root, "regular.txt", []byte("data"))

	fifoPath := filepath.Join(root, "pipe")

	err := mkfifo(t, fifoPath, 0o600)
	if err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*string, error) {
		path := string(f.AbsPathBorrowed())

		return &path, nil
	}, fileproc.WithWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), []string{filepath.Join(root, "regular.txt")})
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

	results, errs := fileproc.Process(ctx, root, func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
		count++
		if count == 1 {
			cancel(stopErr)
		}

		return &struct{}{}, nil
	}, fileproc.WithWorkers(1))

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

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*string, error) {
		path := string(f.AbsPathBorrowed())

		return &path, nil
	}, fileproc.WithRecursive(), fileproc.WithWorkers(1), fileproc.WithSmallFileThreshold(1000))

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

func Test_Process_Does_Not_Hang_When_Cancelled_In_Pipelined_Mode(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	pad := strings.Repeat("x", testNamePad)

	for i := range testNumFilesMed {
		name := fmt.Sprintf("f-%04d-%s.txt", i, pad)
		writeFile(t, root, name, []byte("x"))
	}

	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })

	stopErr := errors.New("stop")

	started := make(chan struct{}, 1)
	release := make(chan struct{})

	var ok struct{}

	done := make(chan struct{})

	var (
		results []*struct{}
		errs    []error
	)

	opts := []fileproc.Option{
		fileproc.WithWorkers(1),
		fileproc.WithSmallFileThreshold(1), // force pipelined mode
	}

	go func() {
		results, errs = fileproc.Process(ctx, root, func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
			select {
			case started <- struct{}{}:
			default:
			}

			<-release

			return &ok, nil
		}, opts...)

		close(done)
	}()

	// Wait for at least one callback to start (worker is now blocked).
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timeout waiting for callback to start")
	}

	// Give the producer some time to fill the pipeline queue / potentially block.
	time.Sleep(50 * time.Millisecond)

	cancel(stopErr)
	close(release)

	select {
	case <-done:
		// ok
	case <-time.After(3 * time.Second):
		t.Fatal("Process did not return after cancellation (possible deadlock)")
	}

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if !errors.Is(context.Cause(ctx), stopErr) {
		t.Fatalf("unexpected cancellation cause: %v", context.Cause(ctx))
	}

	if len(results) == 0 {
		t.Fatal("expected at least 1 result before cancellation")
	}

	if len(results) >= testNumFilesMed {
		t.Fatalf("expected early stop, got %d results", len(results))
	}
}

func Test_Process_Does_Not_Hang_When_Cancelled_In_Recursive_Concurrent_Mode(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	for i := range testNumDirs {
		p := filepath.Join(fmt.Sprintf("d%03d", i), "f.txt")
		writeFile(t, root, p, []byte("x"))
	}

	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })

	stopErr := errors.New("stop")

	var started atomic.Int64

	release := make(chan struct{})

	var ok struct{}

	done := make(chan struct{})

	var (
		results []*struct{}
		errs    []error
	)

	workers := 4
	opts := []fileproc.Option{
		fileproc.WithRecursive(),
		fileproc.WithWorkers(workers),
		// Keep directories "small" so we focus on tree coordination, not within-dir pipelining.
		fileproc.WithSmallFileThreshold(1_000_000),
	}

	go func() {
		results, errs = fileproc.Process(ctx, root, func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
			started.Add(1)
			<-release

			return &ok, nil
		}, opts...)

		close(done)
	}()

	// Wait for all workers to be blocked in the callback.
	deadline := time.Now().Add(2 * time.Second)
	for started.Load() < int64(workers) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if started.Load() < int64(workers) {
		close(release)
		t.Fatalf("expected at least %d concurrent callbacks to start; got %d", workers, started.Load())
	}

	cancel(stopErr)
	close(release)

	select {
	case <-done:
		// ok
	case <-time.After(3 * time.Second):
		t.Fatal("Process did not return after recursive cancellation (possible deadlock)")
	}

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if !errors.Is(context.Cause(ctx), stopErr) {
		t.Fatalf("unexpected cancellation cause: %v", context.Cause(ctx))
	}

	if len(results) == 0 {
		t.Fatal("expected at least 1 result before cancellation")
	}

	if len(results) >= testNumDirs {
		t.Fatalf("expected early stop, got %d results", len(results))
	}
}

func Test_Process_Does_Not_Hang_When_Cancelled_In_Tree_LargeDir_Pipelined_Mode(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	pad := strings.Repeat("x", testNamePad)

	for i := range testNumFilesMed {
		name := filepath.Join("big", fmt.Sprintf("f-%04d-%s.txt", i, pad))
		writeFile(t, root, name, []byte("x"))
	}

	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })

	stopErr := errors.New("stop")

	started := make(chan struct{}, 1)
	release := make(chan struct{})

	var ok struct{}

	done := make(chan struct{})

	var (
		results []*struct{}
		errs    []error
	)

	opts := []fileproc.Option{
		fileproc.WithRecursive(),
		fileproc.WithWorkers(2),
		fileproc.WithSmallFileThreshold(1), // force within-tree pipelining for "big"
	}

	go func() {
		results, errs = fileproc.Process(ctx, root, func(_ *fileproc.File, _ *fileproc.Worker) (*struct{}, error) {
			select {
			case started <- struct{}{}:
			default:
			}

			<-release

			return &ok, nil
		}, opts...)

		close(done)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("timeout waiting for callback to start")
	}

	// Give the within-tree pipeline producer time to fill the queue / potentially block.
	time.Sleep(50 * time.Millisecond)

	cancel(stopErr)
	close(release)

	select {
	case <-done:
		// ok
	case <-time.After(3 * time.Second):
		t.Fatal("Process did not return after cancellation (possible deadlock in tree large-dir pipeline)")
	}

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if !errors.Is(context.Cause(ctx), stopErr) {
		t.Fatalf("unexpected cancellation cause: %v", context.Cause(ctx))
	}

	if len(results) == 0 {
		t.Fatal("expected at least 1 result before cancellation")
	}

	if len(results) >= testNumFilesMed {
		t.Fatalf("expected early stop, got %d results", len(results))
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
		fileproc.WithWorkers(4),
		fileproc.WithSmallFileThreshold(1), // force pipelining (concurrent callbacks)
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

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*string, error) {
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

			if ioErr.Op != openOp {
				t.Fatalf("unexpected io op: %s", ioErr.Op)
			}

			if ioErr.Path != filepath.Join(root, "blocked") {
				t.Fatalf("unexpected io path: %s", ioErr.Path)
			}
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
		fileproc.WithWorkers(1),
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

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*string, error) {
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

// ============================================================================
// Recursive traversal (tree workers + within-dir pipelining)
// ============================================================================

func Test_Process_Processes_All_Files_When_Using_Recursive_Concurrent_Workers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	wantPaths := make([]string, 0, 128)

	// Root-level files.
	writeFile(t, root, "root1.txt", []byte("a"))
	writeFile(t, root, "root2.txt", []byte("b"))

	wantPaths = append(wantPaths, filepath.Join(root, "root1.txt"), filepath.Join(root, "root2.txt"))

	// Many subdirectories to exercise coordinator + multiple workers.
	for i := range 25 {
		dir := fmt.Sprintf("d%02d", i)
		file1 := filepath.Join(dir, fmt.Sprintf("f%02d.txt", i))
		file2 := filepath.Join(dir, "nested", "n.txt")

		writeFile(t, root, file1, []byte("x"))
		writeFile(t, root, file2, []byte("y"))

		wantPaths = append(wantPaths, filepath.Join(root, file1), filepath.Join(root, file2))
	}

	opts := []fileproc.Option{
		fileproc.WithRecursive(),
		fileproc.WithWorkers(4),
		fileproc.WithSmallFileThreshold(1_000_000), // avoid within-directory pipelining; focus on tree concurrency
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]int)
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*string, error) {
		path := string(f.AbsPathBorrowed())

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

func Test_Process_Traverses_Subdirs_When_Using_Recursive_LargeDir_Pipelined(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	wantPaths := make([]string, 0, 64)

	// A directory with enough files to exceed the threshold.
	for i := range 20 {
		p := filepath.Join("big", fmt.Sprintf("f%02d.txt", i))
		writeFile(t, root, p, []byte("x"))
		wantPaths = append(wantPaths, filepath.Join(root, p))
	}

	// Nested directory under the large dir.
	nested := filepath.Join("big", "sub", "inner.txt")
	writeFile(t, root, nested, []byte("y"))
	wantPaths = append(wantPaths, filepath.Join(root, nested))

	opts := []fileproc.Option{
		fileproc.WithRecursive(),
		fileproc.WithWorkers(2),
		fileproc.WithSmallFileThreshold(1), // force pipelining in "big" within tree mode
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.Worker) (*string, error) {
		path := string(f.AbsPathBorrowed())

		return &path, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	assertStringSlicesEqual(t, resultPaths(results), wantPaths)
}

func Test_Process_Does_Not_Share_DataBuffers_When_Using_Concurrent_Tree_Workers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	// Create many directories so all workers can get a job.
	const (
		workers = 4
		dirs    = 64
	)

	for i := range dirs {
		p := filepath.Join(fmt.Sprintf("d%03d", i), "f.txt")
		writeFile(t, root, p, []byte("x"))
	}

	ptrCh := make(chan uintptr, workers*4)
	release := make(chan struct{})

	var ok struct{}

	opts := []fileproc.Option{
		fileproc.WithRecursive(),
		fileproc.WithWorkers(workers),
		fileproc.WithSmallFileThreshold(1_000_000),
	}

	done := make(chan struct{})

	var errs []error

	go func() {
		_, errs = fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.Worker) (*struct{}, error) {
			buf := w.Buf(1)

			buf = buf[:cap(buf)]
			if len(buf) == 0 {
				return nil, errors.New("unexpected empty buffer")
			}

			ptr := uintptr(unsafe.Pointer(&buf[0]))

			select {
			case ptrCh <- ptr:
			default:
			}

			<-release

			return &ok, nil
		}, opts...)

		close(done)
	}()

	// Wait until we have observed one buffer pointer per worker.
	ptrs := make(map[uintptr]struct{})
	deadline := time.Now().Add(2 * time.Second)

	for len(ptrs) < workers && time.Now().Before(deadline) {
		select {
		case p := <-ptrCh:
			ptrs[p] = struct{}{}
		case <-time.After(5 * time.Millisecond):
		}
	}

	close(release)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Process did not return (possible deadlock)")
	}

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(ptrs) != workers {
		t.Fatalf("expected %d distinct worker buffers across concurrent workers, got %d", workers, len(ptrs))
	}
}

func Test_Process_Does_Not_Share_DataBuffers_When_Using_Pipelined_Workers(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	const workers = 4

	pad := strings.Repeat("x", testNamePad)

	for i := range testNumFilesBig {
		name := fmt.Sprintf("f-%04d-%s.txt", i, pad)
		writeFile(t, root, name, []byte("x"))
	}

	ptrCh := make(chan uintptr, workers*8)
	release := make(chan struct{})

	var ok struct{}

	opts := []fileproc.Option{
		fileproc.WithWorkers(workers),
		fileproc.WithSmallFileThreshold(1),
	}

	done := make(chan struct{})

	var (
		results []*struct{}
		errs    []error
	)

	go func() {
		results, errs = fileproc.Process(t.Context(), root, func(_ *fileproc.File, w *fileproc.Worker) (*struct{}, error) {
			buf := w.Buf(1)

			buf = buf[:cap(buf)]
			if len(buf) == 0 {
				return nil, errors.New("unexpected empty buffer")
			}

			ptr := uintptr(unsafe.Pointer(&buf[0]))

			select {
			case ptrCh <- ptr:
			default:
			}

			<-release

			return &ok, nil
		}, opts...)

		close(done)
	}()

	ptrs := make(map[uintptr]struct{})
	deadline := time.Now().Add(2 * time.Second)

	for len(ptrs) < workers && time.Now().Before(deadline) {
		select {
		case p := <-ptrCh:
			ptrs[p] = struct{}{}
		case <-time.After(5 * time.Millisecond):
		}
	}

	close(release)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Process did not return (possible deadlock)")
	}

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) == 0 {
		t.Fatal("expected some results")
	}

	if len(ptrs) != workers {
		t.Fatalf("expected %d distinct data buffers across concurrent pipeline workers, got %d", workers, len(ptrs))
	}
}

func writeFile(t *testing.T, root, rel string, data []byte) {
	t.Helper()

	fullPath := filepath.Join(root, rel)
	parent := filepath.Dir(fullPath)

	err := os.MkdirAll(parent, 0o750)
	if err != nil {
		t.Fatalf("mkdir %s: %v", parent, err)
	}

	err = os.WriteFile(fullPath, data, 0o600)
	if err != nil {
		t.Fatalf("write %s: %v", fullPath, err)
	}
}

func writeSymlink(t *testing.T, root, targetRel, linkRel string) {
	t.Helper()

	target := filepath.Join(root, targetRel)
	link := filepath.Join(root, linkRel)

	parent := filepath.Dir(link)

	err := os.MkdirAll(parent, 0o750)
	if err != nil {
		t.Fatalf("mkdir %s: %v", parent, err)
	}

	err = os.Symlink(target, link)
	if err != nil {
		t.Fatalf("symlink %s -> %s: %v", link, target, err)
	}
}

func resultPaths(results []*string) []string {
	out := make([]string, 0, len(results))
	for _, r := range results {
		if r == nil {
			continue
		}

		out = append(out, *r)
	}

	sort.Strings(out)

	return out
}

func assertStringSlicesEqual(t *testing.T, got, want []string) {
	t.Helper()

	gotSorted := append([]string(nil), got...)
	wantSorted := append([]string(nil), want...)

	sort.Strings(gotSorted)
	sort.Strings(wantSorted)

	if len(gotSorted) != len(wantSorted) {
		t.Fatalf("slice length mismatch: got=%d want=%d (got=%v want=%v)", len(gotSorted), len(wantSorted), gotSorted, wantSorted)
	}

	for i := range gotSorted {
		if gotSorted[i] != wantSorted[i] {
			t.Fatalf("slice mismatch at %d: got=%v want=%v", i, gotSorted, wantSorted)
		}
	}
}
