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
set -eu

# Determine input files
if [ $# -gt 0 ]; then
    files=$(printf "%s\n" "$@" | rg '_test\.go$' || true)
else
    files=$(git ls-files '*_test.go')
fi

if [ -z "$files" ]; then
    exit 0
fi

# Use ast-grep to get all functions with their line ranges
# Run in parallel, output reverse line order so moving bottom-up keeps line numbers valid
FOUND=$(echo "$files" | parallel -j "$(nproc)" 'sg --pattern "func \$NAME(\$\$\$) { \$\$\$ }" --lang go --json {} 2>/dev/null' | \
    jq -n '[inputs[]]' | jq -r '
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
' 2>/dev/null)

if [ -n "$FOUND" ]; then
    echo "Error: Helper functions must appear after test functions."
    echo "Move to bottom of file:"
    echo ""
    echo "$FOUND"
    exit 1
fi
