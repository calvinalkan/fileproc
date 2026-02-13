package fileproc_test

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"testing"

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

func writeFiles(t *testing.T, root string, files map[string][]byte) {
	t.Helper()

	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}

	sort.Strings(names)

	for _, name := range names {
		writeFile(t, root, name, files[name])
	}
}

func writeFlatFiles(
	t *testing.T,
	root string,
	n int,
	pathFn func(i int) string,
	contentFn func(i int) []byte,
) []string {
	t.Helper()

	paths := make([]string, 0, n)

	for i := range n {
		rel := pathFn(i)
		if rel == "" {
			continue
		}

		data := []byte("x")
		if contentFn != nil {
			data = contentFn(i)
		}

		writeFile(t, root, rel, data)
		paths = append(paths, filepath.Join(root, rel))
	}

	return paths
}

func writeNestedFiles(
	t *testing.T,
	root string,
	dirs int,
	filesPerDir int,
	pathFn func(dir, file int) string,
	contentFn func(dir, file int) []byte,
) {
	t.Helper()

	for dir := range dirs {
		for file := range filesPerDir {
			rel := pathFn(dir, file)
			if rel == "" {
				continue
			}

			data := []byte("x")
			if contentFn != nil {
				data = contentFn(dir, file)
			}

			writeFile(t, root, rel, data)
		}
	}
}

func mustStat(t *testing.T, path string) fileproc.Stat {
	t.Helper()

	var st syscall.Stat_t

	err := syscall.Stat(path, &st)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}

	return fileproc.Stat{
		Size:    st.Size,
		ModTime: st.Mtim.Nano(),
		Mode:    st.Mode,
		Inode:   st.Ino,
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

func makeUnreadable(t *testing.T, path string) {
	t.Helper()

	err := os.Chmod(path, 0)
	if err != nil {
		t.Fatalf("chmod: %v", err)
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

func assertIOError(t *testing.T, err error, wantPath, wantOp string) {
	t.Helper()

	var ioErr *fileproc.IOError
	if !errors.As(err, &ioErr) {
		t.Fatalf("expected IOError, got %T", err)
	}

	if ioErr.Path != wantPath {
		t.Fatalf("unexpected error path: %s", ioErr.Path)
	}

	if ioErr.Op != wantOp {
		t.Fatalf("unexpected error op: %s", ioErr.Op)
	}
}

func retainOwnedCopy(w *fileproc.FileWorker, src []byte) []byte {
	owned := w.AllocateOwned(len(src))
	copy(owned.Buf, src)

	return owned.Buf
}
