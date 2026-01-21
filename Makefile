SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables --no-builtin-rules -j
.SUFFIXES:
.DELETE_ON_ERROR:
.DEFAULT_GOAL := build

.PHONY: build lint clean fmt bench check test

GO := go

build:
	@for dir in ./cmd/*/; do (cd "$$dir" && $(GO) build . && echo "$$dir built"); done

check: lint test

lint:
	golangci-lint config verify
	@for script in ./backpressure/*.sh; do "$$script"; done
	golangci-lint run --fix ./...

test:
	go test -race ./...

clean:
	rm -f $(shell find cmd -maxdepth 2 -type f -executable 2>/dev/null)
	rm -rf .benchmarks/history.jsonl .benchmarks/profiles .benchmarks/regress-*

fmt:
	golangci-lint run --fix --enable-only=modernize
	golangci-lint fmt

bench:
	@bash .pi/skills/fileproc-benchmark/scripts/bench_regress.sh
	./cmd/benchreport/benchreport compare --against avg --n 5 --fail-above 1
	./cmd/benchreport/benchreport compare --against avg --n 5 --fail-above 1 --process stat
