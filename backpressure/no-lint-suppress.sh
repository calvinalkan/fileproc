#!/bin/bash
# Prevents new lint suppression directives from being added to the codebase.
#
# Usage:
#   ./no-lint-suppress.sh              # Check git diff (changed/untracked files)
#   ./no-lint-suppress.sh file.go      # Check specific file
#   cat file.go | ./no-lint-suppress.sh -   # Read from stdin
set -euo pipefail

# Check dependencies
for cmd in rg git; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: Required tool '$cmd' not found" >&2
        exit 1
    fi
done

# Patterns to block:
#   //nolint, // nolint - golangci-lint suppression
#   #nosec, // #nosec  - gosec suppression
#   //lint:ignore     - staticcheck/golint ignore
#   /*nolint*/, /* nolint */ - block comment style
RG_PATTERN='//\s*nolint|#nosec|//\s*lint:ignore|/\*\s*nolint'

get_suppressions_from_git() {
    local result=""

    # Check changed lines in tracked files
    local diff_output
    diff_output=$(git diff HEAD -U0 -- '*.go') || {
        echo "Error: git diff failed" >&2
        return 1
    }

    # rg returns 1 when no matches - that's valid here
    local changed
    changed=$(echo "$diff_output" | rg "^\+\+\+ b/|^@@|^\+.*($RG_PATTERN)" | awk '
        /^\+\+\+ b\//{file=substr($0,7)}
        /^@@/{split($3,a,","); gsub(/\+/,"",a[1]); line=a[1]}
        /^\+.*(\/\/.*nolint|#nosec|\/\/.*lint:ignore|\/\*.*nolint)/{gsub(/^\+/, "", $0); print file":"line":"$0}
    ') || true

    # Check untracked .go files
    # rg returns 1 when no matches - that's valid here
    local untracked_files
    untracked_files=$(git ls-files --others --exclude-standard '*.go') || {
        echo "Error: git ls-files failed" >&2
        return 1
    }

    local untracked=""
    if [ -n "$untracked_files" ]; then
        # rg returns 1 when no matches - that's valid here
        local rg_exit
        untracked=$(echo "$untracked_files" | xargs -r rg -n "$RG_PATTERN" 2>&1) && rg_exit=0 || rg_exit=$?
        if [ "$rg_exit" -gt 1 ] && [ "$rg_exit" -ne 123 ]; then
            echo "Error: rg failed on untracked files: $untracked" >&2
            return 1
        fi
    fi

    result="${changed}${changed:+$'\n'}${untracked}"
    echo "$result" | sed '/^$/d'
}

get_suppressions_from_files() {
    # rg returns 1 when no matches - that's valid here
    # rg returns 2 on errors (file not found, etc) - we want that to fail
    local output stderr_file exit_code
    stderr_file=$(mktemp)
    trap 'rm -f "$stderr_file"' RETURN

    output=$(rg -n --with-filename "$RG_PATTERN" "$@" 2>"$stderr_file") && exit_code=0 || exit_code=$?

    case $exit_code in
        0) echo "$output" ;;  # matches found
        1) ;;                  # no matches - valid
        *)
            echo "Error: rg failed: $(cat "$stderr_file")" >&2
            return 1
            ;;
    esac
}

get_suppressions_from_stdin() {
    # rg returns 1 when no matches - that's valid here
    local output stderr_file exit_code
    stderr_file=$(mktemp)
    trap 'rm -f "$stderr_file"' RETURN

    output=$(rg -n "$RG_PATTERN" 2>"$stderr_file") && exit_code=0 || exit_code=$?

    case $exit_code in
        0) echo "$output" | sed 's|^|<stdin>:|' ;;  # matches found
        1) ;;                                        # no matches - valid
        *)
            echo "Error: rg failed: $(cat "$stderr_file")" >&2
            return 1
            ;;
    esac
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
