//go:build !fileproc_testhooks

package fileproc

func readDirBatch(dh dirHandle, dirPath nulTermPath, buf []byte, suffix string, batch *pathArena, reportSubdir func(nulTermName)) error {
	return readDirBatchImpl(dh, dirPath, buf, suffix, batch, reportSubdir)
}

// Compile-time guard: wrapper signature must match the backend contract.
var _ func(dirHandle, nulTermPath, []byte, string, *pathArena, func(nulTermName)) error = readDirBatch
