//go:build !windows
// +build !windows

package local

import (
	"errors"
	"os"
	"syscall"

	"github.com/wplapper/restic/library/fs"
)

// fsyncDir flushes changes to the directory dir.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}

	err = d.Sync()
	if errors.Is(err, syscall.ENOTSUP) || errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.EINVAL) {
		err = nil
	}

	cerr := d.Close()
	if err == nil {
		err = cerr
	}

	return err
}

// set file to readonly
func setFileReadonly(f string, mode os.FileMode) error {
	return fs.Chmod(f, mode&^0222)
}
