package fileproc

// Export internal symbols for white-box tests in fileproc package.
var (
	WithDefaults              = withDefaults
	DefaultReadSize           = defaultReadSize
	DefaultSmallFileThreshold = defaultSmallFileThreshold
	DefaultWorkers            = defaultWorkers
	DefaultTreeWorkers        = defaultTreeWorkers
	MaxWorkers                = maxWorkers
)
