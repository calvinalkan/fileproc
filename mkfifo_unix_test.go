//go:build !windows

package fileproc_test

import (
	"fmt"
	"syscall"
	"testing"
)

func mkfifo(_ *testing.T, path string, mode uint32) error {
	err := syscall.Mkfifo(path, mode)
	if err != nil {
		return fmt.Errorf("mkfifo: %w", err)
	}

	return nil
}
