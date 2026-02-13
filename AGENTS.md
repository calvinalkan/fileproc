# AGENTS.md

`fileproc` is a Go library for ultra-fast parallel file processing with low allocations and platform IO fast paths.

## Architecture

- `fileproc.go` - Public API (`Process`, `ProcessFunc`, top-level docs).
- `process.go` - Core processing pipeline: coordinator, scanner, worker dispatch.
- `file.go` - `File` API (`AbsPath`/`RelPath`/`Stat`/`ReadAll`/`ReadAllOwned`/`Read`/`Fd`).
- `file_worker.go` - `FileWorker` memory helpers (`AllocateScratch`, `AllocateOwned`, IDs).
- `io*.go` - Platform-specific IO handles/syscall wrappers (`linux`/`unix`/`other`).
- `path_helpers.go` - Path arenas, NUL-terminated path helpers.
- `options.go` - Option parsing/defaults for process and worker behavior.
- `watcher*.go` - File watcher and path-tracking/compaction logic.

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
