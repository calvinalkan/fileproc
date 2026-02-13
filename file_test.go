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

func Test_File_AbsPath_Returns_Correct_Path_When_NonRecursive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "alpha.txt", []byte("alpha"))
	writeFile(t, root, "beta.md", []byte("beta"))

	opts := []fileproc.Option{
		fileproc.WithFileWorkers(1),
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]bool)
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		mu.Lock()

		seen[string(f.AbsPath())] = true

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

func Test_File_AbsPath_Returns_Correct_Path_When_Recursive(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "top.txt", []byte("top"))
	writeFile(t, root, filepath.Join("sub", "nested.txt"), []byte("nested"))
	writeFile(t, root, filepath.Join("sub", "deep", "file.txt"), []byte("deep"))

	opts := []fileproc.Option{
		fileproc.WithRecursive(),
		fileproc.WithFileWorkers(1),
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]bool)
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		mu.Lock()

		seen[string(f.AbsPath())] = true

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

func Test_File_AbsPath_Copy_Remains_Valid_When_Process_Returns(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	type pathHolder struct {
		path string
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*pathHolder, error) {
		return &pathHolder{path: string(f.AbsPath())}, nil
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

func Test_File_RelPath_Returns_Correct_Path(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		recursive bool
		files     map[string][]byte
		want      []string
	}{
		{
			name: "non_recursive",
			files: map[string][]byte{
				"alpha.txt":                        []byte("alpha"),
				"beta.md":                          []byte("beta"),
				filepath.Join("sub", "nested.txt"): []byte("nested"),
			},
			want: []string{
				"alpha.txt",
				"beta.md",
			},
		},
		{
			name:      "recursive",
			recursive: true,
			files: map[string][]byte{
				"top.txt":                                 []byte("top"),
				filepath.Join("sub", "nested.txt"):        []byte("nested"),
				filepath.Join("sub", "deep", "file.txt"):  []byte("deep"),
				filepath.Join("other", "branch", "x.txt"): []byte("x"),
			},
			want: []string{
				"top.txt",
				filepath.Join("sub", "nested.txt"),
				filepath.Join("sub", "deep", "file.txt"),
				filepath.Join("other", "branch", "x.txt"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			writeFiles(t, root, tc.files)

			opts := []fileproc.Option{fileproc.WithFileWorkers(1)}
			if tc.recursive {
				opts = append(opts, fileproc.WithRecursive())
			}

			var (
				mu   sync.Mutex
				seen = make(map[string]bool)
			)

			results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
				mu.Lock()

				seen[string(f.RelPath())] = true

				mu.Unlock()

				return &struct{}{}, nil
			}, opts...)

			if len(errs) != 0 {
				t.Fatalf("unexpected errors: %v", errs)
			}

			if len(results) != len(tc.want) {
				t.Fatalf("expected %d results, got %d; seen paths: %v", len(tc.want), len(results), seen)
			}

			for _, w := range tc.want {
				if !seen[w] {
					t.Fatalf("missing expected path %q in seen paths: %v", w, seen)
				}
			}
		})
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

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var gotStat fileproc.Stat

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
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

	makeUnreadable(t, filePath)

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var gotStat fileproc.Stat

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
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

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var gotData []byte

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = retainOwnedCopy(w, data)

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

func Test_File_Bytes_Remains_Valid_When_AllocateOwnedCopy_Called(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("arena test content")
	writeFile(t, root, "test.txt", content)

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	type dataHolder struct {
		data []byte
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*dataHolder, error) {
		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		return &dataHolder{data: retainOwnedCopy(w, data)}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// The data should still be valid here (retained).
	if !bytes.Equal(results[0].data, content) {
		t.Fatalf("expected %q, got %q", content, results[0].data)
	}
}

func Test_File_Bytes_Returns_NonNil_Empty_Slice_When_File_Empty(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "empty.txt", nil)

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var (
		gotLen int
		gotNil bool
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotLen = len(data)
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

	if gotLen != 0 {
		t.Fatalf("expected empty slice, got len=%d", gotLen)
	}
}

func Test_File_Bytes_Returns_Empty_Slice_When_File_Size_Is_Zero(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "empty.txt", nil)

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var (
		gotLen int
		gotNil bool
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		st, statErr := f.Stat()
		if statErr != nil {
			return nil, fmt.Errorf("stat: %w", statErr)
		}

		if st.Size != 0 {
			t.Errorf("expected size 0, got %d", st.Size)
		}

		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotLen = len(data)
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
		t.Fatal("expected non-nil slice")
	}

	if gotLen != 0 {
		t.Fatalf("expected empty slice, got len=%d", gotLen)
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

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var (
		gotData  []byte
		statSize int64
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
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

		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = retainOwnedCopy(w, data)

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

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var (
		gotData  []byte
		statSize int64
	)

	grownContent := []byte("this is much longer content that grew after stat")

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
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

		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = retainOwnedCopy(w, data)

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

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var firstErr, secondErr error

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		_, firstErr = f.ReadAll()
		_, secondErr = f.ReadAll()

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

func Test_File_Bytes_Opens_File_Lazily_When_Called(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("chmod 000 unsupported on windows")
	}

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	// Make file unreadable
	filePath := filepath.Join(root, "test.txt")

	makeUnreadable(t, filePath)

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var (
		statOK   bool
		bytesErr error
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		// Stat should work without opening file
		st, statErr := f.Stat()
		statOK = statErr == nil && st.Size > 0
		// Bytes() should fail because file is unreadable
		_, bytesErr = f.ReadAll()

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

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var gotData []byte

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = retainOwnedCopy(w, data)

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

func Test_File_Bytes_Returns_Error_When_File_Is_Deleted(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	filePath := filepath.Join(root, "doomed.txt")

	err := os.WriteFile(filePath, []byte("doomed"), 0o644)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		// Delete file after stat but before Bytes()
		removeErr := os.Remove(filePath)
		if removeErr != nil {
			return nil, fmt.Errorf("remove: %w", removeErr)
		}

		_, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		return &struct{}{}, nil
	}, opts...)

	// Should have an error (file not found)
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}

	var procErr *fileproc.ProcessError
	if !errors.As(errs[0], &procErr) {
		t.Fatalf("expected ProcessError, got %T", errs[0])
	}

	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func Test_File_Bytes_Returns_Content_When_File_Grows_From_Empty(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	filePath := filepath.Join(root, "growing.txt")

	// Start with empty file
	err := os.WriteFile(filePath, nil, 0o644)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var (
		statSize int64
		gotData  []byte
	)

	grownContent := []byte("content that appeared after stat")

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
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

		data, err := f.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		gotData = retainOwnedCopy(w, data)

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
// Bytes error propagation
// ============================================================================

func Test_File_Bytes_Propagates_Error_When_File_Unreadable(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("chmod 000 unsupported on windows")
	}

	root := t.TempDir()
	writeFile(t, root, "secret.txt", []byte("secret"))

	filePath := filepath.Join(root, "secret.txt")

	makeUnreadable(t, filePath)

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		_, err := f.ReadAll()
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

// ============================================================================
// File.Read() tests
// ============================================================================

func Test_File_Read_Implements_IoReader_When_Called(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("hello world")
	writeFile(t, root, "test.txt", content)

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var gotData []byte

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
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

func Test_File_Read_Works_Correctly_When_Called_Multiple_Times(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	content := []byte("hello world")
	writeFile(t, root, "test.txt", content)

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var gotData []byte

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
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

	makeUnreadable(t, filePath)

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var (
		statOK  bool
		readErr error
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
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

func Test_File_Read_Propagates_Error_When_File_Unreadable(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("chmod 000 unsupported on windows")
	}

	root := t.TempDir()
	writeFile(t, root, "secret.txt", []byte("secret"))

	filePath := filepath.Join(root, "secret.txt")

	makeUnreadable(t, filePath)

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
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

// ============================================================================
// File.Fd() tests
// ============================================================================

func Test_File_Fd_Returns_Valid_Fd_When_Called(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var fd uintptr

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
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

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var fd uintptr

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
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

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var fd uintptr

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		_, _ = f.ReadAll()
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

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var fd uintptr

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
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

// Mutual exclusion tests
// ============================================================================

func Test_File_Read_And_Bytes_Return_Error_When_Called_In_Different_Order(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	cases := []struct {
		name       string
		first      func(f *fileproc.File) error
		second     func(f *fileproc.File) error
		wantFirst  bool
		wantSecond bool
	}{
		{
			name: "BytesThenRead",
			first: func(f *fileproc.File) error {
				_, err := f.ReadAll()
				if err != nil {
					return fmt.Errorf("bytes: %w", err)
				}

				return nil
			},
			second: func(f *fileproc.File) error {
				var buf [4]byte

				_, err := f.Read(buf[:])
				if err != nil {
					return fmt.Errorf("read: %w", err)
				}

				return nil
			},
			wantFirst:  false,
			wantSecond: true,
		},
		{
			name: "ReadThenBytes",
			first: func(f *fileproc.File) error {
				var buf [4]byte

				_, err := f.Read(buf[:])
				if err != nil {
					return fmt.Errorf("read: %w", err)
				}

				return nil
			},
			second: func(f *fileproc.File) error {
				_, err := f.ReadAll()
				if err != nil {
					return fmt.Errorf("bytes: %w", err)
				}

				return nil
			},
			wantFirst:  false,
			wantSecond: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
				firstErr := tc.first(f)

				secondErr := tc.second(f)
				if (firstErr != nil) != tc.wantFirst {
					return nil, fmt.Errorf("first call error=%w", firstErr)
				}

				if (secondErr != nil) != tc.wantSecond {
					return nil, fmt.Errorf("second call error=%w", secondErr)
				}

				return &struct{}{}, nil
			}, opts...)

			if len(errs) != 0 {
				t.Fatalf("unexpected errors: %v", errs)
			}

			if len(results) != 1 {
				t.Fatalf("expected 1 result, got %d", len(results))
			}
		})
	}
}

// ============================================================================
// ReadAllIntoAt tests
// ============================================================================

func Test_File_ReadAllIntoAt_Returns_Error_When_Destination_Offset_Is_Invalid(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("abc"))

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		var dst [3]byte

		n, err := f.ReadAllIntoAt(dst[:], -1)
		if n != 0 || err == nil {
			return nil, fmt.Errorf("negative offset: n=%d err=%w", n, err)
		}

		n, err = f.ReadAllIntoAt(dst[:], len(dst)+1)
		if n != 0 || err == nil {
			return nil, fmt.Errorf("offset > len(dst): n=%d err=%w", n, err)
		}

		// Invalid offsets should not mutate File mode state.
		n, err = f.ReadAllIntoAt(dst[:], 0)
		if err != nil {
			return nil, fmt.Errorf("valid call after invalid offset should succeed: %w", err)
		}

		if n != 3 || !bytes.Equal(dst[:], []byte("abc")) {
			return nil, fmt.Errorf("unexpected valid call result: n=%d dst=%q", n, dst)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_File_ReadAllIntoAt_Writes_Content_When_Destination_Has_Space(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("hello"))

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		dst := []byte("__________")

		n, err := f.ReadAllIntoAt(dst, 2)
		if err != nil {
			return nil, fmt.Errorf("ReadAllIntoAt: %w", err)
		}

		if n != 5 {
			return nil, fmt.Errorf("unexpected n: got %d, want 5", n)
		}

		if !bytes.Equal(dst, []byte("__hello___")) {
			return nil, fmt.Errorf("unexpected dst: %q", dst)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_File_ReadAllIntoAt_Returns_ShortBuffer_When_Content_Exceeds_Remaining_Destination(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("hello"))

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		dst := []byte("____")

		n, err := f.ReadAllIntoAt(dst, 1)
		if !errors.Is(err, io.ErrShortBuffer) {
			return nil, fmt.Errorf("expected io.ErrShortBuffer, got n=%d err=%w", n, err)
		}

		if n != 3 {
			return nil, fmt.Errorf("unexpected n: got %d, want 3", n)
		}

		if !bytes.Equal(dst, []byte("_hel")) {
			return nil, fmt.Errorf("unexpected dst: %q", dst)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_File_ReadAllIntoAt_Returns_Nil_When_Content_Exactly_Fits_Remaining_Destination(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("hello"))

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		dst := []byte("______")

		n, err := f.ReadAllIntoAt(dst, 1)
		if err != nil {
			return nil, fmt.Errorf("expected nil error, got n=%d err=%w", n, err)
		}

		if n != 5 {
			return nil, fmt.Errorf("unexpected n: got %d, want 5", n)
		}

		if !bytes.Equal(dst, []byte("_hello")) {
			return nil, fmt.Errorf("unexpected dst: %q", dst)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_File_ReadAllIntoAt_Returns_Zero_When_File_Is_Empty(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "empty.txt", []byte{})

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		dst := []byte("___")

		n, err := f.ReadAllIntoAt(dst, len(dst))
		if err != nil {
			return nil, fmt.Errorf("expected nil error for empty file, got n=%d err=%w", n, err)
		}

		if n != 0 {
			return nil, fmt.Errorf("unexpected n: got %d, want 0", n)
		}

		if !bytes.Equal(dst, []byte("___")) {
			return nil, fmt.Errorf("destination should stay unchanged: %q", dst)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_File_Read_And_ReadAllIntoAt_Return_Error_When_Called_In_Different_Order(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	cases := []struct {
		name       string
		first      func(f *fileproc.File) error
		second     func(f *fileproc.File) error
		wantFirst  bool
		wantSecond bool
	}{
		{
			name: "ReadAllIntoAtThenRead",
			first: func(f *fileproc.File) error {
				var dst [16]byte

				_, err := f.ReadAllIntoAt(dst[:], 0)
				if err != nil {
					return fmt.Errorf("ReadAllIntoAt: %w", err)
				}

				return nil
			},
			second: func(f *fileproc.File) error {
				var buf [4]byte

				_, err := f.Read(buf[:])
				if err != nil {
					return fmt.Errorf("Read: %w", err)
				}

				return nil
			},
			wantFirst:  false,
			wantSecond: true,
		},
		{
			name: "ReadThenReadAllIntoAt",
			first: func(f *fileproc.File) error {
				var buf [4]byte

				_, err := f.Read(buf[:])
				if err != nil {
					return fmt.Errorf("Read: %w", err)
				}

				return nil
			},
			second: func(f *fileproc.File) error {
				var dst [16]byte

				_, err := f.ReadAllIntoAt(dst[:], 0)
				if err != nil {
					return fmt.Errorf("ReadAllIntoAt: %w", err)
				}

				return nil
			},
			wantFirst:  false,
			wantSecond: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
				firstErr := tc.first(f)
				secondErr := tc.second(f)

				if (firstErr != nil) != tc.wantFirst {
					return nil, fmt.Errorf("first call error=%w", firstErr)
				}

				if (secondErr != nil) != tc.wantSecond {
					return nil, fmt.Errorf("second call error=%w", secondErr)
				}

				return &struct{}{}, nil
			}, fileproc.WithFileWorkers(1))

			if len(errs) != 0 {
				t.Fatalf("unexpected errors: %v", errs)
			}

			if len(results) != 1 {
				t.Fatalf("expected 1 result, got %d", len(results))
			}
		})
	}
}
