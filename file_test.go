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
	"syscall"
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
// File.ReadAll() tests
// ============================================================================

func Test_File_ReadAll_Reads_Full_Content_When_Called(t *testing.T) {
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

func Test_File_ReadAll_Remains_Valid_When_AllocateOwnedCopy_Called(t *testing.T) {
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

func Test_File_ReadAll_Returns_NonNil_Empty_Slice_When_File_Empty(t *testing.T) {
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

func Test_File_ReadAll_Returns_Empty_Slice_When_File_Size_Is_Zero(t *testing.T) {
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

func Test_File_ReadAll_Returns_Actual_Content_When_File_Shrunk_Since_Stat(t *testing.T) {
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

		// Shrink the file after stat but before ReadAll()
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

	// ReadAll() returns actual (shorter) content
	if string(gotData) != "short" {
		t.Fatalf("data: got %q, want 'short'", gotData)
	}
}

func Test_File_ReadAll_Does_Not_Reject_From_Stale_Stat_Size_When_File_Shrinks(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	filePath := filepath.Join(root, "shrinking-max-bytes.txt")
	initialContent := []byte("0123456789")
	shrunkContent := []byte("1234")

	err := os.WriteFile(filePath, initialContent, 0o644)
	if err != nil {
		t.Fatalf("write initial: %v", err)
	}

	var gotData []byte

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		_, err = f.Stat()
		if err != nil {
			return nil, fmt.Errorf("stat: %w", err)
		}

		err = os.WriteFile(filePath, shrunkContent, 0o644)
		if err != nil {
			return nil, fmt.Errorf("write shrunk: %w", err)
		}

		data, err := f.ReadAll(fileproc.WithMaxBytes(len(shrunkContent)))
		if err != nil {
			return nil, fmt.Errorf("read all: %w", err)
		}

		gotData = retainOwnedCopy(w, data)

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if !bytes.Equal(gotData, shrunkContent) {
		t.Fatalf("data: got %q, want %q", gotData, shrunkContent)
	}
}

func Test_File_ReadAll_Returns_Full_Content_When_File_Grew_Since_Stat(t *testing.T) {
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

		// Grow the file after stat but before ReadAll()
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

	// ReadAll() returns actual (larger) content
	if !bytes.Equal(gotData, grownContent) {
		t.Fatalf("data: got %q, want %q", gotData, grownContent)
	}
}

func Test_File_ReadAll_Opens_File_Lazily_When_Called(t *testing.T) {
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
		// ReadAll() should fail because file is unreadable
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
		t.Fatal("ReadAll() should fail for unreadable file")
	}
}

func Test_File_ReadAll_Works_Correctly_When_File_Large(t *testing.T) {
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

func Test_File_ReadAll_Returns_Error_When_File_Is_Deleted(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	filePath := filepath.Join(root, "doomed.txt")

	err := os.WriteFile(filePath, []byte("doomed"), 0o644)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		// Delete file after stat but before ReadAll()
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

func Test_File_ReadAll_Returns_Content_When_File_Grows_From_Empty(t *testing.T) {
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
// ReadAll error propagation
// ============================================================================

func Test_File_ReadAll_Propagates_Error_When_File_Unreadable(t *testing.T) {
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

func Test_File_Fd_Returns_Same_Fd_When_Called_Multiple_Times(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	opts := []fileproc.Option{fileproc.WithFileWorkers(1)}

	var firstFD, secondFD uintptr

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		firstFD = f.Fd()
		secondFD = f.Fd()

		return &struct{}{}, nil
	}, opts...)

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if firstFD == invalidFD {
		t.Fatal("expected valid fd on first call, got -1")
	}

	if secondFD != firstFD {
		t.Fatalf("expected same fd across repeated calls, first=%d second=%d", firstFD, secondFD)
	}
}

func Test_File_Fd_Is_Usable_For_LinuxSyscalls_When_Called(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != "linux" {
		t.Skip("linux-specific fd syscall test")
	}

	root := t.TempDir()
	content := []byte("syscall-fd-check")
	writeFile(t, root, "test.txt", content)

	var (
		statSize int64
		gotData  []byte
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		fd := f.Fd()
		if fd == invalidFD {
			return nil, errors.New("expected valid fd, got -1")
		}

		var st syscall.Stat_t

		err := syscall.Fstat(int(fd), &st)
		if err != nil {
			return nil, fmt.Errorf("fstat: %w", err)
		}

		var buf [64]byte

		n, err := syscall.Pread(int(fd), buf[:len(content)], 0)
		if err != nil {
			return nil, fmt.Errorf("pread: %w", err)
		}

		statSize = st.Size

		gotData = append([]byte(nil), buf[:n]...)

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if statSize != int64(len(content)) {
		t.Fatalf("unexpected fstat size: got %d want %d", statSize, len(content))
	}

	if !bytes.Equal(gotData, content) {
		t.Fatalf("pread content mismatch: got %q want %q", gotData, content)
	}
}

func Test_File_Fd_Works_When_Called_After_ReadAll(t *testing.T) {
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
		t.Fatal("expected valid fd after ReadAll(), got -1")
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

// ============================================================================
// ReadAllOwned tests
// ============================================================================

func Test_File_ReadAllOwned_Returns_Error_When_Destination_Offset_Is_Invalid(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("abc"))

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		dst := w.AllocateOwned(3)

		_, n, err := f.ReadAllOwned(dst, -1)
		if n != 0 || err == nil {
			return nil, fmt.Errorf("negative offset: n=%d err=%w", n, err)
		}

		_, n, err = f.ReadAllOwned(dst, len(dst.Buf)+1)
		if n != 0 || err == nil {
			return nil, fmt.Errorf("offset > len(dst): n=%d err=%w", n, err)
		}

		// Invalid offsets should not mutate File mode state.
		got, n, err := f.ReadAllOwned(dst, 0)
		if err != nil {
			return nil, fmt.Errorf("valid call after invalid offset should succeed: %w", err)
		}

		if n != 3 || !bytes.Equal(got.Buf, []byte("abc")) {
			return nil, fmt.Errorf("unexpected valid call result: n=%d dst=%q", n, got.Buf)
		}

		got.Free()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_File_ReadAllOwned_Writes_Content_When_Destination_Has_Space(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("hello"))

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		dst := w.AllocateOwned(10)
		copy(dst.Buf, "__________")

		got, n, err := f.ReadAllOwned(dst, 2)
		if err != nil {
			return nil, fmt.Errorf("ReadAllOwned: %w", err)
		}

		if n != 5 {
			return nil, fmt.Errorf("unexpected n: got %d, want 5", n)
		}

		if !bytes.Equal(got.Buf, []byte("__hello___")) {
			return nil, fmt.Errorf("unexpected dst: %q", got.Buf)
		}

		got.Free()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_File_ReadAllOwned_Applies_MaxBytes_To_FileData_When_Offset_Is_NonZero(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("hello"))

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		dst := w.AllocateOwned(8)
		copy(dst.Buf, "________")

		got, n, err := f.ReadAllOwned(dst, 3, fileproc.WithMaxBytes(5))
		if err != nil {
			return nil, fmt.Errorf("ReadAllOwned: %w", err)
		}

		if n != 5 {
			return nil, fmt.Errorf("unexpected n: got %d, want 5", n)
		}

		if !bytes.Equal(got.Buf, []byte("___hello")) {
			return nil, fmt.Errorf("unexpected dst: %q", got.Buf)
		}

		got.Free()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_File_ReadAllOwned_Returns_Partial_Write_When_MaxBytes_Is_Exceeded(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("hello"))

	var (
		gotN   int
		gotErr error
		gotBuf []byte
	)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		dst := w.AllocateOwned(8)
		copy(dst.Buf, "________")

		got, n, err := f.ReadAllOwned(dst, 2, fileproc.WithMaxBytes(4))

		gotN = n
		gotErr = err

		gotBuf = append([]byte(nil), got.Buf...)

		got.Free()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if !errors.Is(gotErr, fileproc.ErrFileTooLarge) {
		t.Fatalf("expected ErrFileTooLarge, got %v", gotErr)
	}

	if gotN != 4 {
		t.Fatalf("expected partial write of 4 bytes, got %d", gotN)
	}

	if !bytes.Equal(gotBuf[:6], []byte("__hell")) {
		t.Fatalf("unexpected partial content: %q", gotBuf[:6])
	}
}

func Test_File_ReadAllOwned_Grows_Destination_When_Content_Exceeds_Remaining_Destination(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("hello"))

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		dst := w.AllocateOwned(4)
		copy(dst.Buf, "____")

		got, n, err := f.ReadAllOwned(dst, 1)
		if err != nil {
			return nil, fmt.Errorf("expected nil error, got n=%d err=%w", n, err)
		}

		if n != 5 {
			return nil, fmt.Errorf("unexpected n: got %d, want 5", n)
		}

		if len(got.Buf) < 6 {
			return nil, fmt.Errorf("expected grown destination, got len=%d", len(got.Buf))
		}

		if !bytes.Equal(got.Buf[:6], []byte("_hello")) {
			return nil, fmt.Errorf("unexpected dst prefix: %q", got.Buf[:6])
		}

		got.Free()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_File_ReadAllOwned_Returns_Nil_When_Content_Exactly_Fits_Remaining_Destination(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("hello"))

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		dst := w.AllocateOwned(6)
		copy(dst.Buf, "______")

		got, n, err := f.ReadAllOwned(dst, 1)
		if err != nil {
			return nil, fmt.Errorf("expected nil error, got n=%d err=%w", n, err)
		}

		if n != 5 {
			return nil, fmt.Errorf("unexpected n: got %d, want 5", n)
		}

		if !bytes.Equal(got.Buf, []byte("_hello")) {
			return nil, fmt.Errorf("unexpected dst: %q", got.Buf)
		}

		got.Free()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_File_ReadAllOwned_Returns_Zero_When_File_Is_Empty(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeFile(t, root, "empty.txt", []byte{})

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		dst := w.AllocateOwned(3)
		copy(dst.Buf, "___")

		got, n, err := f.ReadAllOwned(dst, len(dst.Buf))
		if err != nil {
			return nil, fmt.Errorf("expected nil error for empty file, got n=%d err=%w", n, err)
		}

		if n != 0 {
			return nil, fmt.Errorf("unexpected n: got %d, want 0", n)
		}

		if !bytes.Equal(got.Buf, []byte("___")) {
			return nil, fmt.Errorf("destination should stay unchanged: %q", got.Buf)
		}

		got.Free()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_File_ReadMethods_Return_Expected_Errors_When_Called_In_Invalid_Order(t *testing.T) {
	t.Parallel()

	type readCallResult struct {
		firstErr  error
		secondErr error
	}

	root := t.TempDir()
	writeFile(t, root, "test.txt", []byte("content"))

	cases := []struct {
		name string
		run  func(f *fileproc.File, w *fileproc.FileWorker) readCallResult
	}{
		{
			name: "ReadThenReadAll",
			run: func(f *fileproc.File, _ *fileproc.FileWorker) readCallResult {
				var buf [1]byte

				_, firstErr := f.Read(buf[:])
				_, secondErr := f.ReadAll()

				return readCallResult{
					firstErr:  firstErr,
					secondErr: secondErr,
				}
			},
		},
		{
			name: "ReadThenReadAllOwned",
			run: func(f *fileproc.File, w *fileproc.FileWorker) readCallResult {
				var buf [1]byte

				_, firstErr := f.Read(buf[:])

				dst := w.AllocateOwned(16)

				got, _, secondErr := f.ReadAllOwned(dst, 0)
				if secondErr == nil {
					got.Free()
				}

				return readCallResult{
					firstErr:  firstErr,
					secondErr: secondErr,
				}
			},
		},
		{
			name: "ReadAllThenRead",
			run: func(f *fileproc.File, _ *fileproc.FileWorker) readCallResult {
				_, firstErr := f.ReadAll()

				var buf [1]byte

				_, secondErr := f.Read(buf[:])

				return readCallResult{
					firstErr:  firstErr,
					secondErr: secondErr,
				}
			},
		},
		{
			name: "ReadAllOwnedThenRead",
			run: func(f *fileproc.File, w *fileproc.FileWorker) readCallResult {
				dst := w.AllocateOwned(16)

				got, _, firstErr := f.ReadAllOwned(dst, 0)
				if firstErr == nil {
					got.Free()
				}

				var buf [1]byte

				_, secondErr := f.Read(buf[:])

				return readCallResult{
					firstErr:  firstErr,
					secondErr: secondErr,
				}
			},
		},
		{
			name: "ReadAllThenReadAll",
			run: func(f *fileproc.File, _ *fileproc.FileWorker) readCallResult {
				_, firstErr := f.ReadAll()
				_, secondErr := f.ReadAll()

				return readCallResult{
					firstErr:  firstErr,
					secondErr: secondErr,
				}
			},
		},
		{
			name: "ReadAllOwnedThenReadAllOwned",
			run: func(f *fileproc.File, w *fileproc.FileWorker) readCallResult {
				firstDst := w.AllocateOwned(16)

				firstGot, _, firstErr := f.ReadAllOwned(firstDst, 0)
				if firstErr == nil {
					firstGot.Free()
				}

				secondDst := w.AllocateOwned(16)

				secondGot, _, secondErr := f.ReadAllOwned(secondDst, 0)
				if secondErr == nil {
					secondGot.Free()
				}

				return readCallResult{
					firstErr:  firstErr,
					secondErr: secondErr,
				}
			},
		},
		{
			name: "ReadAllThenReadAllOwned",
			run: func(f *fileproc.File, w *fileproc.FileWorker) readCallResult {
				_, firstErr := f.ReadAll()

				dst := w.AllocateOwned(16)

				got, _, secondErr := f.ReadAllOwned(dst, 0)
				if secondErr == nil {
					got.Free()
				}

				return readCallResult{
					firstErr:  firstErr,
					secondErr: secondErr,
				}
			},
		},
		{
			name: "ReadAllOwnedThenReadAll",
			run: func(f *fileproc.File, w *fileproc.FileWorker) readCallResult {
				dst := w.AllocateOwned(16)

				got, _, firstErr := f.ReadAllOwned(dst, 0)
				if firstErr == nil {
					got.Free()
				}

				_, secondErr := f.ReadAll()

				return readCallResult{
					firstErr:  firstErr,
					secondErr: secondErr,
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var callResult readCallResult

			results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
				callResult = tc.run(f, w)

				return &struct{}{}, nil
			}, fileproc.WithFileWorkers(1))

			if len(errs) != 0 {
				t.Fatalf("unexpected errors: %v", errs)
			}

			if len(results) != 1 {
				t.Fatalf("expected 1 result, got %d", len(results))
			}

			if callResult.firstErr != nil {
				t.Fatalf("unexpected first-call error: %v", callResult.firstErr)
			}

			if callResult.secondErr == nil {
				t.Fatal("expected second call to fail")
			}
		})
	}
}

func Test_File_ReadVariants_Apply_Options_When_ReadAllOptions_Are_Provided(t *testing.T) {
	t.Parallel()

	type readVariant struct {
		name string
		call func(f *fileproc.File, w *fileproc.FileWorker, opts ...fileproc.ReadAllOption) ([]byte, error)
	}

	readVariants := []readVariant{
		{
			name: "ReadAll",
			call: func(f *fileproc.File, _ *fileproc.FileWorker, opts ...fileproc.ReadAllOption) ([]byte, error) {
				data, err := f.ReadAll(opts...)
				if err != nil {
					return nil, fmt.Errorf("read all: %w", err)
				}

				return append([]byte(nil), data...), nil
			},
		},
		{
			name: "ReadAllOwned",
			call: func(f *fileproc.File, w *fileproc.FileWorker, opts ...fileproc.ReadAllOption) ([]byte, error) {
				dst := w.AllocateOwned(0)

				got, n, err := f.ReadAllOwned(dst, 0, opts...)
				if err != nil {
					return nil, fmt.Errorf("read all owned: %w", err)
				}

				out := append([]byte(nil), got.Buf[:n]...)
				got.Free()

				return out, nil
			},
		},
	}

	maxInt := int(^uint(0) >> 1)

	cases := []struct {
		name            string
		content         []byte
		opts            []fileproc.ReadAllOption
		wantErr         bool
		wantErrTooLarge bool
	}{
		{
			name:            "MaxBytesExceeded",
			content:         []byte("hello"),
			opts:            []fileproc.ReadAllOption{fileproc.WithMaxBytes(4)},
			wantErrTooLarge: true,
		},
		{
			name:    "LastMaxBytesWins",
			content: []byte("hello"),
			opts: []fileproc.ReadAllOption{
				fileproc.WithMaxBytes(2),
				fileproc.WithMaxBytes(8),
			},
		},
		{
			name:    "NegativeMaxBytesReturnsError",
			content: []byte("abc"),
			opts: []fileproc.ReadAllOption{
				fileproc.WithMaxBytes(-1),
			},
			wantErr: true,
		},
		{
			name:    "PlatformIntLimitIsEnforced",
			content: []byte("abc"),
			opts: []fileproc.ReadAllOption{
				fileproc.WithMaxBytes(maxInt),
			},
			wantErr: true,
		},
	}

	for _, variant := range readVariants {
		for _, tc := range cases {
			t.Run(variant.name+"_"+tc.name, func(t *testing.T) {
				t.Parallel()

				root := t.TempDir()
				writeFile(t, root, "test.txt", tc.content)

				var (
					gotData []byte
					gotErr  error
				)

				results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
					gotData, gotErr = variant.call(f, w, tc.opts...)

					return &struct{}{}, nil
				}, fileproc.WithFileWorkers(1))

				if len(errs) != 0 {
					t.Fatalf("unexpected process errors: %v", errs)
				}

				if len(results) != 1 {
					t.Fatalf("expected 1 result, got %d", len(results))
				}

				if tc.wantErrTooLarge {
					if !errors.Is(gotErr, fileproc.ErrFileTooLarge) {
						t.Fatalf("expected ErrFileTooLarge, got %v", gotErr)
					}

					return
				}

				if tc.wantErr {
					if gotErr == nil {
						t.Fatal("expected read error, got nil")
					}

					return
				}

				if gotErr != nil {
					t.Fatalf("unexpected read error: %v", gotErr)
				}

				if !bytes.Equal(gotData, tc.content) {
					t.Fatalf("content mismatch: got %q want %q", gotData, tc.content)
				}
			})
		}
	}
}

func Test_File_ReadAll_Applies_SizeHint_Rules_When_SizeHint_Options_Are_Provided(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		content       []byte
		callStatFirst bool
		opts          []fileproc.ReadAllOption
	}{
		{
			name:    "HintExceedsMaxWithoutStatIsClamped",
			content: []byte("a"),
			opts:    []fileproc.ReadAllOption{fileproc.WithSizeHint(1024), fileproc.WithMaxBytes(1)},
		},
		{
			name:          "HintExceedsMaxWithPriorStatIsClamped",
			content:       []byte("abc"),
			callStatFirst: true,
			opts:          []fileproc.ReadAllOption{fileproc.WithSizeHint(8), fileproc.WithMaxBytes(4)},
		},
		{
			name:    "LastSizeHintWins",
			content: []byte("abc"),
			opts: []fileproc.ReadAllOption{
				fileproc.WithSizeHint(8),
				fileproc.WithSizeHint(3),
				fileproc.WithMaxBytes(4),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			writeFile(t, root, "test.txt", tc.content)

			var (
				gotData []byte
				gotErr  error
			)

			results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
				if tc.callStatFirst {
					_, statErr := f.Stat()
					if statErr != nil {
						return nil, fmt.Errorf("stat: %w", statErr)
					}
				}

				gotData, gotErr = f.ReadAll(tc.opts...)

				return &struct{}{}, nil
			}, fileproc.WithFileWorkers(1))

			if len(errs) != 0 {
				t.Fatalf("unexpected process errors: %v", errs)
			}

			if len(results) != 1 {
				t.Fatalf("expected 1 result, got %d", len(results))
			}

			if gotErr != nil {
				t.Fatalf("unexpected read error: %v", gotErr)
			}

			if !bytes.Equal(gotData, tc.content) {
				t.Fatalf("content mismatch: got %q want %q", gotData, tc.content)
			}
		})
	}
}

func Test_File_ReadVariants_Do_Not_Change_Mode_When_Options_Are_Invalid(t *testing.T) {
	t.Parallel()

	type readVariant struct {
		name string
		call func(f *fileproc.File, w *fileproc.FileWorker, opts ...fileproc.ReadAllOption) ([]byte, error)
	}

	readVariants := []readVariant{
		{
			name: "ReadAll",
			call: func(f *fileproc.File, _ *fileproc.FileWorker, opts ...fileproc.ReadAllOption) ([]byte, error) {
				data, err := f.ReadAll(opts...)
				if err != nil {
					return nil, fmt.Errorf("read all: %w", err)
				}

				return append([]byte(nil), data...), nil
			},
		},
		{
			name: "ReadAllOwned",
			call: func(f *fileproc.File, w *fileproc.FileWorker, opts ...fileproc.ReadAllOption) ([]byte, error) {
				dst := w.AllocateOwned(0)

				got, n, err := f.ReadAllOwned(dst, 0, opts...)
				if err != nil {
					return nil, fmt.Errorf("read all owned: %w", err)
				}

				out := append([]byte(nil), got.Buf[:n]...)
				got.Free()

				return out, nil
			},
		},
	}

	for _, variant := range readVariants {
		t.Run(variant.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			content := []byte("content")
			writeFile(t, root, "test.txt", content)

			var (
				firstErr  error
				second    []byte
				secondErr error
			)

			results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
				_, firstErr = variant.call(f, w, fileproc.WithMaxBytes(-1))
				second, secondErr = variant.call(f, w)

				return &struct{}{}, nil
			}, fileproc.WithFileWorkers(1))

			if len(errs) != 0 {
				t.Fatalf("unexpected process errors: %v", errs)
			}

			if len(results) != 1 {
				t.Fatalf("expected 1 result, got %d", len(results))
			}

			if firstErr == nil {
				t.Fatal("expected first call to fail with invalid options")
			}

			if secondErr != nil {
				t.Fatalf("second call should succeed after options parse failure: %v", secondErr)
			}

			if !bytes.Equal(second, content) {
				t.Fatalf("unexpected second call content: got %q want %q", second, content)
			}
		})
	}
}

func Test_File_Fd_Returns_InvalidFd_When_File_Is_Removed_Before_Call(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	filePath := filepath.Join(root, "test.txt")
	writeFile(t, root, "test.txt", []byte("content"))

	var fd uintptr

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		removeErr := os.Remove(filePath)
		if removeErr != nil {
			return nil, fmt.Errorf("remove: %w", removeErr)
		}

		fd = f.Fd()

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if fd != invalidFD {
		t.Fatalf("expected invalid fd, got %d", fd)
	}
}

func Test_File_Stat_Returns_Fresh_Metadata_When_File_Changes_Between_Calls(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	filePath := filepath.Join(root, "test.txt")
	initialContent := []byte("small")
	updatedContent := []byte("this content is larger than before")

	writeFile(t, root, "test.txt", initialContent)

	var first, second fileproc.Stat

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, _ *fileproc.FileWorker) (*struct{}, error) {
		var err error

		first, err = f.Stat()
		if err != nil {
			return nil, fmt.Errorf("first stat: %w", err)
		}

		writeErr := os.WriteFile(filePath, updatedContent, 0o644)
		if writeErr != nil {
			return nil, fmt.Errorf("write: %w", writeErr)
		}

		second, err = f.Stat()
		if err != nil {
			return nil, fmt.Errorf("second stat: %w", err)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if first == second {
		t.Fatalf("expected second stat to refresh after file change, first=%+v second=%+v", first, second)
	}

	if first.Size != int64(len(initialContent)) {
		t.Fatalf("unexpected first size: got %d want %d", first.Size, len(initialContent))
	}

	if second.Size != int64(len(updatedContent)) {
		t.Fatalf("unexpected second size: got %d want %d", second.Size, len(updatedContent))
	}
}

func Test_File_Methods_Skip_File_When_Path_Becomes_Directory_Before_Access(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		call func(f *fileproc.File, w *fileproc.FileWorker) error
	}{
		{
			name: "Stat",
			call: func(f *fileproc.File, _ *fileproc.FileWorker) error {
				_, err := f.Stat()
				if err != nil {
					return fmt.Errorf("stat: %w", err)
				}

				return nil
			},
		},
		{
			name: "ReadAll",
			call: func(f *fileproc.File, _ *fileproc.FileWorker) error {
				_, err := f.ReadAll()
				if err != nil {
					return fmt.Errorf("read all: %w", err)
				}

				return nil
			},
		},
		{
			name: "Read",
			call: func(f *fileproc.File, _ *fileproc.FileWorker) error {
				var buf [1]byte

				_, err := f.Read(buf[:])
				if err != nil {
					return fmt.Errorf("read: %w", err)
				}

				return nil
			},
		},
		{
			name: "ReadAllOwned",
			call: func(f *fileproc.File, w *fileproc.FileWorker) error {
				dst := w.AllocateOwned(1)

				got, _, err := f.ReadAllOwned(dst, 0)
				if err == nil {
					got.Free()
				}

				if err != nil {
					return fmt.Errorf("read all owned: %w", err)
				}

				return nil
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			filePath := filepath.Join(root, "test.txt")
			writeFile(t, root, "test.txt", []byte("content"))

			callbackCalls := 0

			results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
				callbackCalls++

				removeErr := os.Remove(filePath)
				if removeErr != nil {
					return nil, fmt.Errorf("remove: %w", removeErr)
				}

				mkdirErr := os.Mkdir(filePath, 0o755)
				if mkdirErr != nil {
					return nil, fmt.Errorf("mkdir: %w", mkdirErr)
				}

				callErr := tc.call(f, w)
				if callErr != nil {
					return nil, callErr
				}

				return &struct{}{}, nil
			}, fileproc.WithFileWorkers(1))

			if len(errs) != 0 {
				t.Fatalf("expected no process errors for skip path, got %v", errs)
			}

			if len(results) != 0 {
				t.Fatalf("expected file to be skipped, got %d results", len(results))
			}

			if callbackCalls != 1 {
				t.Fatalf("expected callback to run exactly once, got %d", callbackCalls)
			}
		})
	}
}

func Test_File_Methods_Skip_File_When_Path_Becomes_SelfReferentialSymlink_Before_Access(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("symlink tests skipped on windows")
	}

	cases := []struct {
		name string
		call func(f *fileproc.File, w *fileproc.FileWorker) error
	}{
		{
			name: "Stat",
			call: func(f *fileproc.File, _ *fileproc.FileWorker) error {
				_, err := f.Stat()
				if err != nil {
					return fmt.Errorf("stat: %w", err)
				}

				return nil
			},
		},
		{
			name: "ReadAll",
			call: func(f *fileproc.File, _ *fileproc.FileWorker) error {
				_, err := f.ReadAll()
				if err != nil {
					return fmt.Errorf("read all: %w", err)
				}

				return nil
			},
		},
		{
			name: "Read",
			call: func(f *fileproc.File, _ *fileproc.FileWorker) error {
				var buf [1]byte

				_, err := f.Read(buf[:])
				if err != nil {
					return fmt.Errorf("read: %w", err)
				}

				return nil
			},
		},
		{
			name: "ReadAllOwned",
			call: func(f *fileproc.File, w *fileproc.FileWorker) error {
				dst := w.AllocateOwned(1)

				got, _, err := f.ReadAllOwned(dst, 0)
				if err == nil {
					got.Free()
				}

				if err != nil {
					return fmt.Errorf("read all owned: %w", err)
				}

				return nil
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			filePath := filepath.Join(root, "test.txt")
			writeFile(t, root, "test.txt", []byte("content"))

			callbackCalls := 0

			results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
				callbackCalls++

				removeErr := os.Remove(filePath)
				if removeErr != nil {
					return nil, fmt.Errorf("remove: %w", removeErr)
				}

				// Relative target to the same name creates a self-referential symlink.
				linkErr := os.Symlink("test.txt", filePath)
				if linkErr != nil {
					return nil, fmt.Errorf("symlink: %w", linkErr)
				}

				callErr := tc.call(f, w)
				if callErr != nil {
					return nil, callErr
				}

				return &struct{}{}, nil
			}, fileproc.WithFileWorkers(1))

			if len(errs) != 0 {
				t.Fatalf("expected no process errors for skip path, got %v", errs)
			}

			if len(results) != 0 {
				t.Fatalf("expected file to be skipped, got %d results", len(results))
			}

			if callbackCalls != 1 {
				t.Fatalf("expected callback to run exactly once, got %d", callbackCalls)
			}
		})
	}
}

func Test_File_ReadAllOwned_Propagates_Error_When_File_Unreadable(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip("chmod 000 unsupported on windows")
	}

	root := t.TempDir()
	writeFile(t, root, "secret.txt", []byte("secret"))

	filePath := filepath.Join(root, "secret.txt")
	makeUnreadable(t, filePath)

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		dst := w.AllocateOwned(16)

		_, _, err := f.ReadAllOwned(dst, 0)
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

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

func Test_File_ReadAllOwned_Returns_Error_When_File_Is_Deleted_Before_Read(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	filePath := filepath.Join(root, "doomed.txt")

	err := os.WriteFile(filePath, []byte("doomed"), 0o644)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	results, errs := fileproc.Process(t.Context(), root, func(f *fileproc.File, w *fileproc.FileWorker) (*struct{}, error) {
		removeErr := os.Remove(filePath)
		if removeErr != nil {
			return nil, fmt.Errorf("remove: %w", removeErr)
		}

		dst := w.AllocateOwned(16)

		_, _, err := f.ReadAllOwned(dst, 0)
		if err != nil {
			return nil, fmt.Errorf("test: %w", err)
		}

		return &struct{}{}, nil
	}, fileproc.WithFileWorkers(1))

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
