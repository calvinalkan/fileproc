#!/bin/bash
# Enforces that test helper functions appear after Test* functions in _test.go files.
#
# Rule:
#   - Helper functions (lowercase, e.g. `func writeFile`) must appear AFTER all
#     Test*/Benchmark*/Example*/Fuzz* functions in the same file.
#   - Files with no test functions are skipped (pure helper files are allowed).
#
# Output:
#   - Reports ALL violations across all files (not just first error).
#   - Each violation shows: file:startLine-endLine: funcName
#   - Results are sorted by file, then by line number DESCENDING so you can
#     move helpers bottom-to-top without invalidating subsequent line numbers.
#
# Usage:
#   ./test-helper-order.sh              # Check all *_test.go files in repo
#   ./test-helper-order.sh file_test.go # Check specific file
#
# Dependencies: ast-grep (sg), jq, GNU parallel
set -euo pipefail

# Check dependencies
for cmd in sg jq parallel; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: Required tool '$cmd' not found" >&2
        exit 1
    fi
done

# Determine input files
if [ $# -gt 0 ]; then
    # Validate all files exist first
    for file in "$@"; do
        if [ ! -f "$file" ]; then
            echo "Error: File not found: $file" >&2
            exit 1
        fi
    done

    # Filter to only _test.go files, rg returns 1 if no matches
    files=$(printf "%s\n" "$@" | rg '_test\.go$') || {
        # No test files in input - nothing to check
        exit 0
    }
else
    files=$(git ls-files '*_test.go') || {
        echo "Error: git ls-files failed" >&2
        exit 1
    }
fi

if [ -z "$files" ]; then
    exit 0
fi

# Wrapper that handles sg's exit codes properly:
# - exit 0: matches found (outputs JSON array)
# - exit 1: no matches (outputs []) - this is valid, not an error
# - exit 2+: actual error
run_sg() {
    local file="$1"
    local output
    local stderr_file
    local exit_code

    stderr_file=$(mktemp)
    trap 'rm -f "$stderr_file"' RETURN

    output=$(sg --pattern 'func $NAME($$$) { $$$ }' --lang go --json "$file" 2>"$stderr_file") && exit_code=0 || exit_code=$?

    case $exit_code in
        0|1)
            # Validate JSON before outputting (catches corruption)
            if [ -n "$output" ] && ! echo "$output" | jq -e . >/dev/null 2>&1; then
                echo "Error: sg produced invalid JSON for $file" >&2
                echo "$output" | head -5 >&2
                return 1
            fi
            echo "$output"
            ;;
        *)
            echo "Error: sg failed on $file (exit $exit_code): $(cat "$stderr_file")" >&2
            return 1
            ;;
    esac
}
export -f run_sg

# Run sg on all files in parallel
# parallel returns 0 if all jobs succeed, non-zero otherwise
sg_output=$(echo "$files" | parallel -j "$(nproc)" --halt soon,fail=1 run_sg {}) || {
    echo "Error: Failed to parse test files" >&2
    exit 1
}

# Process the JSON output with jq
# jq exits 0 on success, non-zero on parse errors
jq_stderr=$(mktemp)
trap 'rm -f "$jq_stderr"' EXIT
FOUND=$(echo "$sg_output" | jq -n '[inputs[]]' 2>"$jq_stderr") || {
    echo "Error: jq failed to parse sg output: $(cat "$jq_stderr")" >&2
    exit 1
}

FOUND=$(echo "$FOUND" | jq -r '
    group_by(.file) | .[] |
    (.[0].file) as $file |
    [.[] | {
        name: .metaVariables.single.NAME.text,
        start: (.range.start.line + 1),
        end: (.range.end.line + 1)
    }] |
    (map(select(.name | test("^(Test|Benchmark|Example|Fuzz)"))) | min_by(.start) | .start) as $first_test |
    if $first_test == null then empty else
    [.[] | select((.name | test("^[a-z]")) and (.start < $first_test))] |
    sort_by(-.start) |
    if length == 0 then empty else
    $file, (.[] | "  \(.name) (\(.start)-\(.end))")
    end end
' 2>"$jq_stderr") || {
    echo "Error: jq processing failed: $(cat "$jq_stderr")" >&2
    exit 1
}

if [ -n "$FOUND" ]; then
    echo "Error: Helper functions must appear after test functions."
    echo "Move to bottom of file:"
    echo ""
    echo "$FOUND"
    exit 1
fi
