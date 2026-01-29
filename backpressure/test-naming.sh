#!/bin/bash
# Enforces Go test function naming convention:
#   - Pattern: Test_<Subject>_<Action/State>_When_<Condition>
#   - Must use underscore-separated PascalCase words
#   - Must contain "_When_" to describe the condition
#
# Usage:
#   ./test-naming.sh              # Check git diff (changed/untracked files)
#   ./test-naming.sh file.go      # Check specific file
#   cat file.go | ./test-naming.sh -   # Read from stdin
set -euo pipefail

# Check dependencies
for cmd in rg git; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: Required tool '$cmd' not found" >&2
        exit 1
    fi
done

# Pattern for VALID test names (after file:line: prefix):
#   Test_ + PascalWord(s) + _When_ + PascalWord(s)
VALID_PATTERN='func Test(_[A-Z][a-zA-Z0-9]*)+_When(_[A-Z][a-zA-Z0-9]*)+\('

get_test_funcs_from_git() {
    local result=""

    # Check changed lines in tracked files
    local diff_output
    diff_output=$(git diff HEAD -U0 -- '*_test.go') || {
        echo "Error: git diff failed" >&2
        return 1
    }

    # rg returns 1 when no matches - that's valid here
    local changed
    changed=$(echo "$diff_output" | rg "^\+\+\+ b/|^@@|^\+.*func Test" | awk '
        /^\+\+\+ b\//{file=substr($0,7)}
        /^@@/{split($3,a,","); gsub(/\+/,"",a[1]); line=a[1]}
        /^\+.*func Test/{gsub(/^\+/, "", $0); print file":"line":"$0}
    ') || true

    # Check untracked *_test.go files
    local untracked_files
    untracked_files=$(git ls-files --others --exclude-standard '*_test.go') || {
        echo "Error: git ls-files failed" >&2
        return 1
    }

    local untracked=""
    if [ -n "$untracked_files" ]; then
        # rg returns 1 when no matches - that's valid here
        local rg_exit
        untracked=$(echo "$untracked_files" | xargs -r rg -n '^func Test' 2>&1) && rg_exit=0 || rg_exit=$?
        if [ "$rg_exit" -gt 1 ] && [ "$rg_exit" -ne 123 ]; then
            echo "Error: rg failed on untracked files: $untracked" >&2
            return 1
        fi
    fi

    result="${changed}${changed:+$'\n'}${untracked}"
    echo "$result" | sed '/^$/d'
}

get_test_funcs_from_files() {
    # rg returns 1 when no matches - that's valid here
    # rg returns 2 on errors - we want that to fail
    local output stderr_file exit_code
    stderr_file=$(mktemp)
    trap 'rm -f "$stderr_file"' RETURN

    output=$(rg -n --with-filename '^func Test' "$@" 2>"$stderr_file") && exit_code=0 || exit_code=$?

    case $exit_code in
        0) echo "$output" ;;  # matches found
        1) ;;                  # no matches - valid
        *)
            echo "Error: rg failed: $(cat "$stderr_file")" >&2
            return 1
            ;;
    esac
}

get_test_funcs_from_stdin() {
    # rg returns 1 when no matches - that's valid here
    local output stderr_file exit_code
    stderr_file=$(mktemp)
    trap 'rm -f "$stderr_file"' RETURN

    output=$(rg -n '^func Test' 2>"$stderr_file") && exit_code=0 || exit_code=$?

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
        ALL_TESTS=$(get_test_funcs_from_stdin)
    else
        test_files=()
        for file in "$@"; do
            if [ ! -f "$file" ]; then
                echo "Error: File not found: $file" >&2
                exit 1
            fi
            if [[ "$file" == *_test.go ]]; then
                test_files+=("$file")
            fi
        done
        if [ ${#test_files[@]} -eq 0 ]; then
            ALL_TESTS=""
        else
            ALL_TESTS=$(get_test_funcs_from_files "${test_files[@]}")
        fi
    fi
else
    ALL_TESTS=$(get_test_funcs_from_git)
fi

# Filter to only INVALID test names (those not matching the valid pattern)
# rg -v returns 1 when all lines match (meaning no invalid names) - that's valid
# Exclude TestMain - it's Go's special test entry point and cannot be renamed
FOUND=""
if [ -n "$ALL_TESTS" ]; then
    FOUND=$(echo "$ALL_TESTS" | grep -v 'func TestMain(' | rg -v "$VALID_PATTERN") || true
fi

if [ -n "$FOUND" ]; then
    echo "Error: Test function names must follow the pattern: Test_<Subject>_<Action/State>_When_<Condition>"
    echo ""
    echo "Rules:"
    echo "  - Use underscore-separated PascalCase words"
    echo "  - Must contain '_When_' to describe the condition"
    echo ""
    echo "Examples:"
    echo "  ✓ Test_Parser_Returns_Error_When_Input_Is_Empty"
    echo "  ✓ Test_Cache_Evicts_Entry_When_TTL_Expires"
    echo "  ✗ TestParserReturnsError"
    echo "  ✗ Test_Parser_Returns_Error"
    echo ""
    echo "$FOUND" | while IFS=: read -r file line rest; do
        printf "  %s:%s: %s\n" "$file" "$line" "$rest"
    done
    exit 1
fi
