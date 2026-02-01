# AGENTS.md

`fileproc` is a Go library for ultra-fast parallel file processing with low allocations and platform IO fast paths.

## Architecture

- `fileproc.go` - Public API (`Process`, options), docs.
- `processor.go` - Core pipeline: coordinator, scan workers, file workers.
- `file.go` - `File` methods (AbsPath/RelPath/Stat/Read/ReadAll/Fd).
- `io*.go` - Platform-specific IO (linux/unix/other) and syscall wrappers.
- `path_helpers.go` - NUL-terminated path types, arenas, helpers.
- `options.go` - Option parsing and defaults.
- `*_test.go` - Tests; focus on `Process` behavior and `File` API.

## Commands

We use `make` in this project.

```bash
make check # Runs everything, use this before committing
make build # Build the binary
make fmt # format code
make test # Run tests with race detector
make lint # Run all linters

# Targeted test
GOFLAGS="-run=TestName" make test
GOFLAGS="-run=TestPattern.*" make test
```
