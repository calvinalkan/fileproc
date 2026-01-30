//go:build !windows

package fileproc_test

import (
	"syscall"
	"testing"
)

func mkfifo(_ *testing.T, path string, mode uint32) error {
	return syscall.Mkfifo(path, mode)
}
