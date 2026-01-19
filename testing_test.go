package fileproc_test

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"fileproc"
)

// Shared test constants.
const (
	testNamePad     = 200
	testNumFilesBig = 500
	testNumFilesMed = 300
	testNumDirs     = 201
	testBadFile     = "bad.txt"
	testSecretFile  = "secret.txt"
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

func resultPaths[T any](results []fileproc.Result[T]) []string {
	out := make([]string, 0, len(results))
	for _, r := range results {
		out = append(out, string(r.Path))
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
