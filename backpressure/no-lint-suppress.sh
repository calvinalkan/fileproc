#!/bin/bash
# Prevents new lint suppression directives from being added to the codebase.
#
# Usage:
#   ./no-lint-suppress.sh              # Check git diff (changed/untracked files)
#   ./no-lint-suppress.sh file.go      # Check specific file
#   cat file.go | ./no-lint-suppress.sh -   # Read from stdin
set -eou pipefail

# Patterns to block:
#   //nolint, // nolint - golangci-lint suppression
#   #nosec, // #nosec  - gosec suppression
#   //lint:ignore     - staticcheck/golint ignore
#   /*nolint*/, /* nolint */ - block comment style
RG_PATTERN='//\s*nolint|#nosec|//\s*lint:ignore|/\*\s*nolint'

get_suppressions_from_git() {
    local result=""

    # Check changed lines in tracked files
    local changed=$(git diff HEAD -U0 -- '*.go' | rg "^\+\+\+ b/|^@@|^\+.*($RG_PATTERN)" | awk '
        /^\+\+\+ b\//{file=substr($0,7)}
        /^@@/{split($3,a,","); gsub(/\+/,"",a[1]); line=a[1]}
        /^\+.*(\/\/.*nolint|#nosec|\/\/.*lint:ignore|\/\*.*nolint)/{gsub(/^\+/, "", $0); print file":"line":"$0}
    ' || true)

    # Check untracked .go files
    local untracked=$(git ls-files --others --exclude-standard '*.go' | \
        xargs -r rg -n "$RG_PATTERN" 2>/dev/null || true)

    result="${changed}${changed:+$'\n'}${untracked}"
    echo "$result" | sed '/^$/d'
}

get_suppressions_from_files() {
    rg -n --with-filename "$RG_PATTERN" "$@" 2>/dev/null || true
}

get_suppressions_from_stdin() {
    rg -n "$RG_PATTERN" | sed 's|^|<stdin>:|' || true
}

# Determine input source
if [ $# -gt 0 ]; then
    if [ "$1" = "-" ]; then
        if [ $# -gt 1 ]; then
            echo "Error: '-' cannot be combined with file paths." >&2
            exit 1
        fi
        FOUND=$(get_suppressions_from_stdin)
    else
        for file in "$@"; do
            if [ ! -f "$file" ]; then
                echo "Error: File not found: $file" >&2
                exit 1
            fi
        done
        FOUND=$(get_suppressions_from_files "$@")
    fi
else
    FOUND=$(get_suppressions_from_git)
fi

FOUND=$(echo "$FOUND" | sed '/^$/d')

if [ -n "$FOUND" ]; then
    echo "Error: Lint suppression directives are forbidden (nolint, nosec, lint:ignore)."
    echo "Fix the underlying issues, otherwise you will not be able to commit."
    echo ""
    echo "$FOUND" | while IFS=: read -r file line rest; do
        printf "  %s:%s: %s\n" "$file" "$line" "$rest"
    done
    exit 1
fi
