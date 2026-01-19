package fileproc_test

// Shared test constants to avoid goconst duplication warnings.
const (
	testNamePad     = 200
	testNumFilesBig = 500
	testNumFilesMed = 300
	testNumDirs     = 201 // slightly different from testNamePad to avoid goconst false positive
	testBadFile     = "bad.txt"
	testSecretFile  = "secret.txt"
)
