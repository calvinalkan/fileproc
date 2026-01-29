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
	"unsafe"

	"github.com/calvinalkan/fileproc"
)

const windowsOS = "windows"

// ============================================================================
// File.RelPath() tests
// ============================================================================

func Test_File_RelPath_Returns_Correct_Path_When_NonRecursive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "alpha.txt", []byte("alpha"))
	writeFile(t, root, "beta.md", []byte("beta"))

	opts := fileproc.Options{
		Workers:            1,
		SmallFileThreshold: 1000,
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]bool)
	)

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		mu.Lock()

		seen[string(f.RelPath())] = true

		mu.Unlock()

		return &struct{}{}, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if !seen["alpha.txt"] || !seen["beta.md"] {
		t.Fatalf("missing expected paths: %v", seen)
	}
}

func Test_File_RelPath_Returns_Correct_Path_When_Recursive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "top.txt", []byte("top"))
	writeFile(t, root, filepath.Join("sub", "nested.txt"), []byte("nested"))
	writeFile(t, root, filepath.Join("sub", "deep", "file.txt"), []byte("deep"))

	opts := fileproc.Options{
		Recursive:          true,
		Workers:            1,
		SmallFileThreshold: 1000,
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]bool)
	)

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		mu.Lock()

		seen[string(f.RelPath())] = true

		mu.Unlock()

		return &struct{}{}, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	want := []string{"top.txt", filepath.Join("sub", "nested.txt"), filepath.Join("sub", "deep", "file.txt")}
	for _, w := range want {
		if !seen[w] {
			t.Fatalf("missing expected path %q: %v", w, seen)
		}
	}
}

func Test_File_RelPath_Remains_Valid_When_ProcessLazy_Returns(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := fileproc.Options{Workers: 1}

	type pathHolder struct {
		path []byte
	}

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*pathHolder, error) {
		// Store the slice directly without copying
		return &pathHolder{path: f.RelPath()}, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// The path should still be valid here (arena not released)
	if string(results[0].Value.path) != "test.txt" {
		t.Fatalf("expected path 'test.txt', got %q", results[0].Value.path)
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

	opts := fileproc.Options{Workers: 1}

	var gotStat fileproc.Stat

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		var statErr error

		gotStat, statErr = f.Stat()
		if statErr != nil {
			return nil, fmt.Errorf("stat: %w", statErr)
		}

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var gotStat fileproc.Stat

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		// Just access stat, don't try to read
		var statErr error

		gotStat, statErr = f.Stat()
		if statErr != nil {
			return nil, fmt.Errorf("stat: %w", statErr)
		}

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var gotData []byte

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = data

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	type dataHolder struct {
		data []byte
	}

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*dataHolder, error) {
		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}
		// Store the slice directly without copying
		return &dataHolder{data: data}, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// The data should still be valid here (arena not released)
	if !bytes.Equal(results[0].Value.data, content) {
		t.Fatalf("expected %q, got %q", content, results[0].Value.data)
	}
}

func Test_File_Bytes_Returns_NonNil_Empty_Slice_When_File_Empty(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "empty.txt", nil)

	opts := fileproc.Options{Workers: 1}

	var (
		gotData []byte
		gotNil  bool
	)

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = data
		gotNil = data == nil

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var (
		gotData  []byte
		statSize int64
	)

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
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
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var (
		gotData  []byte
		statSize int64
	)

	grownContent := []byte("this is much longer content that grew after stat")

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
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
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var firstErr, secondErr error

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		_, firstErr = f.Bytes()
		_, secondErr = f.Bytes()

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var readErr, bytesErr error

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		buf := make([]byte, 0, 4)
		buf = buf[:4]
		_, readErr = f.Read(buf)
		_, bytesErr = f.Bytes()

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var (
		statOK   bool
		bytesErr error
	)

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		// Stat should work without opening file
		st, statErr := f.Stat()
		statOK = statErr == nil && st.Size > 0
		// Bytes() should fail because file is unreadable
		_, bytesErr = f.Bytes()

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var gotData []byte

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = data

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var gotData []byte

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		data, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = data

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var bytesErr, readErr error

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		_, bytesErr = f.Bytes()
		buf := make([]byte, 0, 4)
		buf = buf[:4]
		_, readErr = f.Read(buf)

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var gotData []byte

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
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
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var (
		statOK  bool
		readErr error
	)

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		// Stat should work without opening file
		st, statErr := f.Stat()
		statOK = statErr == nil && st.Size > 0
		// Read() should fail because file is unreadable
		buf := make([]byte, 0, 4)
		buf = buf[:4]
		_, readErr = f.Read(buf)

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var fd uintptr

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		fd = f.Fd()

		return &struct{}{}, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if fd == 0 {
		t.Fatal("expected valid fd, got 0")
	}
}

func Test_File_Fd_Opens_File_When_Not_Already_Open(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := fileproc.Options{Workers: 1}

	var fd uintptr

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		// Don't call Bytes() or Read(), just Fd()
		fd = f.Fd()

		return &struct{}{}, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if fd == 0 {
		t.Fatal("expected valid fd, got 0")
	}
}

func Test_File_Fd_Works_When_Called_After_Bytes(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := fileproc.Options{Workers: 1}

	var fd uintptr

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		_, _ = f.Bytes()
		fd = f.Fd()

		return &struct{}{}, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if fd == 0 {
		t.Fatal("expected valid fd after Bytes(), got 0")
	}
}

func Test_File_Fd_Works_When_Called_After_Read(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := fileproc.Options{Workers: 1}

	var fd uintptr

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		buf := make([]byte, 0, 4)
		buf = buf[:4]
		_, _ = f.Read(buf)
		fd = f.Fd()

		return &struct{}{}, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if fd == 0 {
		t.Fatal("expected valid fd after Read(), got 0")
	}
}

// ============================================================================
// Scratch tests
// ============================================================================

func Test_Scratch_Get_Returns_Zero_Len_With_Cap_When_Called(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := fileproc.Options{Workers: 1}

	var bufLen, bufCap int

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(_ *fileproc.File, scratch *fileproc.Scratch) (*struct{}, error) {
		buf := scratch.Get(4096)
		bufLen = len(buf)
		bufCap = cap(buf)

		return &struct{}{}, nil
	}, opts)

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

func Test_Scratch_Get_Grows_Capacity_When_Larger_Request(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("a"))
	writeFile(t, root, "b.txt", []byte("b"))

	opts := fileproc.Options{
		Workers:            1,
		SmallFileThreshold: 1000,
	}

	var caps []int

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(_ *fileproc.File, scratch *fileproc.Scratch) (*struct{}, error) {
		// First file requests 1024, second requests 8192
		size := 1024
		if len(caps) > 0 {
			size = 8192
		}

		buf := scratch.Get(size)
		caps = append(caps, cap(buf))

		return &struct{}{}, nil
	}, opts)

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

func Test_Scratch_Get_Reuses_Buffer_When_Same_Worker(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("a"))
	writeFile(t, root, "b.txt", []byte("b"))

	opts := fileproc.Options{
		Workers:            1,
		SmallFileThreshold: 1000,
	}

	var ptrs []uintptr

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(_ *fileproc.File, scratch *fileproc.Scratch) (*struct{}, error) {
		buf := scratch.Get(1024)
		// Expand to get actual backing array pointer
		buf = buf[:cap(buf)]
		if len(buf) > 0 {
			ptrs = append(ptrs, uintptr(unsafe.Pointer(&buf[0])))
		}

		return &struct{}{}, nil
	}, opts)

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
		t.Fatal("scratch buffer should be reused across files in same worker")
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

	opts := fileproc.Options{
		Workers:            1,
		SmallFileThreshold: 1000,
	}

	type fileData struct {
		path string
		data []byte
	}

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*fileData, error) {
		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		return &fileData{path: string(f.RelPath()), data: data}, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	expected := map[string]string{
		"a.txt": "alpha",
		"b.txt": "bravo",
		"c.txt": "charlie",
	}

	for _, r := range results {
		want, ok := expected[r.Value.path]
		if !ok {
			t.Fatalf("unexpected path: %s", r.Value.path)
		}

		if string(r.Value.data) != want {
			t.Fatalf("data mismatch for %s: got %q, want %q", r.Value.path, r.Value.data, want)
		}
	}
}

func Test_Arena_Subslices_Remain_Valid_When_ProcessLazy_Returns(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("prefix:middle:suffix")
	writeFile(t, root, "test.txt", content)

	opts := fileproc.Options{Workers: 1}

	type parts struct {
		prefix []byte
		middle []byte
		suffix []byte
	}

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*parts, error) {
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
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Subslices should still be valid
	if string(results[0].Value.prefix) != "prefix" {
		t.Fatalf("prefix: got %q", results[0].Value.prefix)
	}

	if string(results[0].Value.middle) != "middle" {
		t.Fatalf("middle: got %q", results[0].Value.middle)
	}

	if string(results[0].Value.suffix) != "suffix" {
		t.Fatalf("suffix: got %q", results[0].Value.suffix)
	}
}

// ============================================================================
// Mutual exclusion tests
// ============================================================================

func Test_Mutual_Exclusion_Returns_Error_When_Bytes_Then_Read(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := fileproc.Options{Workers: 1}

	var bytesErr, readErr error

	_, _ = fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		_, bytesErr = f.Bytes()
		buf := make([]byte, 0, 4)
		buf = buf[:4]
		_, readErr = f.Read(buf)

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var readErr, bytesErr error

	_, _ = fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		buf := make([]byte, 0, 4)
		buf = buf[:4]
		_, readErr = f.Read(buf)
		_, bytesErr = f.Bytes()

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{
		Recursive:          true,
		Workers:            4,
		SmallFileThreshold: 10,
	}

	type holder struct {
		data []byte
	}

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*holder, error) {
		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		return &holder{data: data}, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 100 {
		t.Fatalf("expected 100 results, got %d", len(results))
	}

	// Verify all data is correct
	for _, r := range results {
		if string(r.Value.data) != "content" {
			t.Fatalf("data mismatch: got %q", r.Value.data)
		}
	}
}

func Test_Concurrency_Multiple_Workers_Have_Independent_Scratch_When_Parallel(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	for i := range 50 {
		writeFile(t, root, "file"+string(rune('0'+i%10))+string(rune('0'+i/10))+".txt", []byte("content"))
	}

	opts := fileproc.Options{
		Workers:            4,
		SmallFileThreshold: 10,
	}

	type scratchResult struct {
		data []byte
	}

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, scratch *fileproc.Scratch) (*scratchResult, error) {
		// Write file path into scratch buffer
		buf := scratch.Get(256)
		buf = append(buf, f.RelPath()...)

		// Copy to result (scratch only valid during callback)
		return &scratchResult{data: append([]byte(nil), buf...)}, nil
	}, opts)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 50 {
		t.Fatalf("expected 50 results, got %d", len(results))
	}

	// Verify each result has correct path (scratch wasn't corrupted by other workers)
	seen := make(map[string]bool)

	for _, r := range results {
		path := string(r.Value.data)
		if seen[path] {
			t.Fatalf("duplicate path: %s", path)
		}

		seen[path] = true

		// Verify path matches Result.Path
		if path != string(r.Path) {
			t.Fatalf("scratch data %q != result path %q", path, r.Path)
		}
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

	opts := fileproc.Options{Workers: 1}

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		_, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		buf := make([]byte, 0, 4)
		buf = buf[:4]

		_, err := f.Read(buf)
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{
		Workers:            1,
		SmallFileThreshold: 10,
	}

	sentinel := errors.New("boom")

	var calls int

	_, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		calls++
		// Open file via Bytes
		_, _ = f.Bytes()

		if calls == 1 {
			return nil, sentinel
		}

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{
		Workers:            1,
		SmallFileThreshold: 10,
	}

	var (
		calls       int
		successData []byte
	)

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		calls++

		data, err := f.Bytes()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		successData = data

		return &struct{}{}, nil
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var gotData []byte

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
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
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
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
	}, opts)

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

	opts := fileproc.Options{Workers: 1}

	var (
		statSize int64
		gotData  []byte
	)

	grownContent := []byte("content that appeared after stat")

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
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
	}, opts)

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

func Test_ProcessLazy_Returns_TopLevel_Files_And_Skips_Symlinks_When_NonRecursive(t *testing.T) {
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
		seen = make(map[string]struct{})
	)

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		st, statErr := f.Stat()
		if statErr != nil {
			return nil, fmt.Errorf("stat: %w", statErr)
		}

		mu.Lock()

		got[string(f.RelPath())] = st
		seen[string(f.RelPath())] = struct{}{}

		mu.Unlock()

		return &struct{}{}, nil
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

func Test_ProcessLazy_Applies_Suffix_Filter_And_Skips_Symlink_Dirs_When_Recursive(t *testing.T) {
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
	)

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		mu.Lock()

		got[string(f.RelPath())] = struct{}{}

		mu.Unlock()

		return &struct{}{}, nil
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

func Test_ProcessLazy_Reads_Content_Via_File_When_Used(t *testing.T) {
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
	)

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		data, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("read file: %w", err)
		}

		mu.Lock()

		got = append([]byte(nil), data...)
		called = true

		mu.Unlock()

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

	if !bytes.Equal(got, content) {
		t.Fatalf("unexpected data: got=%q want=%q", got, content)
	}
}

func Test_ProcessLazy_Does_Not_Open_File_When_Unused(t *testing.T) {
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

	opts := fileproc.Options{
		Workers: 1,
	}

	var called bool

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		called = true
		// Only access Stat(), don't open file
		_, _ = f.Stat()

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

func Test_ProcessLazy_Returns_ProcessError_When_Callback_Fails(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, testBadFile, []byte("bad"))

	opts := fileproc.Options{
		Workers: 1,
	}

	sentinel := errors.New("boom")

	_, errs := fileproc.ProcessLazy(t.Context(), root, func(_ *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
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

func Test_ProcessLazy_Closes_File_Handle_When_Callback_Errors_After_Read(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("alpha"))
	writeFile(t, root, "b.txt", []byte("bravo"))

	expected := map[string][]byte{
		"a.txt": []byte("alpha"),
		"b.txt": []byte("bravo"),
	}

	opts := fileproc.Options{
		Workers:            1,
		SmallFileThreshold: 10,
	}

	sentinel := errors.New("boom")
	mismatch := errors.New("mismatch")

	var (
		calls         int
		errorInjected bool
	)

	_, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		calls++

		data, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("read file: %w", err)
		}

		relPath := string(f.RelPath())

		exp, ok := expected[relPath]
		if !ok {
			return nil, fmt.Errorf("unexpected path %q: %w", relPath, mismatch)
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
	}, opts)

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

func Test_ProcessLazy_Silently_Skips_When_File_Becomes_Directory(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "normal.txt", []byte("normal"))
	writeFile(t, root, "will_become_dir.txt", []byte("temp"))

	racePath := filepath.Join(root, "will_become_dir.txt")

	opts := fileproc.Options{
		Workers:            1,
		SmallFileThreshold: 1000,
	}

	var callCount int

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		callCount++

		// On first call for the racy file, replace it with a directory
		if string(f.RelPath()) == "will_become_dir.txt" {
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
	}, opts)

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

func Test_ProcessLazy_Silently_Skips_When_File_Becomes_Symlink(t *testing.T) {
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

	opts := fileproc.Options{
		Workers:            1,
		SmallFileThreshold: 1000,
	}

	var callCount int

	results, errs := fileproc.ProcessLazy(t.Context(), root, func(f *fileproc.File, _ *fileproc.Scratch) (*struct{}, error) {
		callCount++

		// On the racy file, replace it with a symlink before reading
		if string(f.RelPath()) == "will_become_symlink.txt" {
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
	}, opts)

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
