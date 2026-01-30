//go:build windows

package fileproc_test

import "testing"

func mkfifo(t *testing.T, _ string, _ uint32) error {
	t.Helper()
	t.Skip("mkfifo unsupported on windows")
	return nil
}
