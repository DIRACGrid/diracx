#!/bin/bash
# E2E tests for CWL job submission, input/output sandboxes, and status reporting.
# Run from the directory containing the CWL files.
#
# Usage:
#   ./run-e2e.sh              # submit + wait + verify
#   ./run-e2e.sh --submit     # submit only (prints job IDs)
#   ./run-e2e.sh --verify ID  # verify a single completed job
set -euo pipefail
cd "$(dirname "$0")"

POLL_INTERVAL=30
MAX_WAIT=600  # 10 minutes

# ── Helpers ──────────────────────────────────────────────────────────────────

submit_job() {
    # Submit and extract job ID from output (last number on any line)
    local output
    output=$(dirac job submit "$@" 2>&1)
    echo "$output" >&2
    echo "$output" | grep -oE '[0-9]+' | tail -1
}

wait_for_jobs() {
    # Wait until all given job IDs reach a terminal state (Done/Failed)
    local ids=("$@")
    local elapsed=0
    echo "Waiting for ${#ids[@]} jobs to finish (max ${MAX_WAIT}s)..."
    while (( elapsed < MAX_WAIT )); do
        local pending=0
        for id in "${ids[@]}"; do
            local status
            status=$(dirac job search --condition "JobID eq $id" --parameter Status 2>/dev/null \
                | grep -oE '(Waiting|Matched|Running|Submitting|Received|Checking|Staging|Done|Failed|Killed|Deleted|Stalled|Completing|Rescheduled)' \
                | head -1 || echo "Unknown")
            case "$status" in
                Done|Failed|Killed|Deleted|Stalled) ;;
                *) pending=$((pending + 1)) ;;
            esac
        done
        if (( pending == 0 )); then
            echo "All jobs reached terminal state."
            return 0
        fi
        echo "  $pending job(s) still running (${elapsed}s elapsed)..."
        sleep "$POLL_INTERVAL"
        elapsed=$((elapsed + POLL_INTERVAL))
    done
    echo "ERROR: Timed out waiting for jobs after ${MAX_WAIT}s"
    return 1
}

get_job_status() {
    dirac job search --condition "JobID eq $1" --parameter Status 2>/dev/null \
        | grep -oE '(Done|Failed|Killed|Deleted|Stalled|Waiting|Matched|Running|Completing)' \
        | head -1 || echo "Unknown"
}

# ── Verify functions ─────────────────────────────────────────────────────────

verify_echo_tool() {
    local job_id=$1
    echo "--- Verifying echo-tool (Job $job_id) ---"
    local status
    status=$(get_job_status "$job_id")
    if [[ "$status" != "Done" ]]; then
        echo "FAIL: Expected Done, got $status"
        return 1
    fi

    # Output sandbox should contain stdout.log and stderr.log
    local listing
    listing=$(dirac job sandbox list "$job_id" 2>&1) || true
    echo "$listing"
    if ! echo "$listing" | grep -q "stdout.log"; then
        echo "FAIL: stdout.log not in output sandbox"
        return 1
    fi
    if ! echo "$listing" | grep -q "stderr.log"; then
        echo "FAIL: stderr.log not in output sandbox"
        return 1
    fi

    # Peek stdout.log — should contain the echo message
    local content
    content=$(dirac job sandbox peek "$job_id" stdout.log 2>&1) || true
    echo "stdout.log content: $content"
    if ! echo "$content" | grep -qi "hello"; then
        echo "FAIL: stdout.log should contain 'hello'"
        return 1
    fi

    echo "PASS: echo-tool"
}

verify_fail_tool() {
    local job_id=$1
    echo "--- Verifying fail-tool (Job $job_id) ---"
    local status
    status=$(get_job_status "$job_id")
    if [[ "$status" != "Failed" ]]; then
        echo "FAIL: Expected Failed, got $status"
        return 1
    fi

    # Check ApplicationStatus shows failure
    local app_status
    app_status=$(dirac job search --condition "JobID eq $job_id" --parameter ApplicationStatus 2>/dev/null) || true
    echo "ApplicationStatus: $app_status"
    if ! echo "$app_status" | grep -qi "fail"; then
        echo "FAIL: ApplicationStatus should contain 'fail'"
        return 1
    fi

    echo "PASS: fail-tool"
}

verify_sandbox_tool() {
    local job_id=$1
    echo "--- Verifying sandbox-tool (Job $job_id) ---"
    local status
    status=$(get_job_status "$job_id")
    if [[ "$status" != "Done" ]]; then
        echo "FAIL: Expected Done, got $status"
        return 1
    fi

    # Output sandbox should have stdout.log and stderr.log
    local listing
    listing=$(dirac job sandbox list "$job_id" 2>&1) || true
    echo "$listing"
    if ! echo "$listing" | grep -q "stdout.log"; then
        echo "FAIL: stdout.log not in output sandbox"
        return 1
    fi
    if ! echo "$listing" | grep -q "stderr.log"; then
        echo "FAIL: stderr.log not in output sandbox"
        return 1
    fi

    # stdout.log should contain output from hello.sh (input sandbox script)
    local content
    content=$(dirac job sandbox peek "$job_id" stdout.log 2>&1) || true
    echo "stdout.log content: $content"
    if ! echo "$content" | grep -q "Hello from sandbox script"; then
        echo "FAIL: stdout.log should contain 'Hello from sandbox script' (from hello.sh input sandbox)"
        return 1
    fi

    echo "PASS: sandbox-tool"
}

verify_cmd_tool() {
    local job_id=$1
    echo "--- Verifying cmd tool (Job $job_id) ---"
    local status
    status=$(get_job_status "$job_id")
    if [[ "$status" != "Done" ]]; then
        echo "FAIL: Expected Done, got $status"
        return 1
    fi

    # Output sandbox should have stdout.log and stderr.log (auto-generated CWL)
    local listing
    listing=$(dirac job sandbox list "$job_id" 2>&1) || true
    echo "$listing"
    if ! echo "$listing" | grep -q "stdout.log"; then
        echo "FAIL: stdout.log not in output sandbox"
        return 1
    fi

    # Peek stdout.log
    local content
    content=$(dirac job sandbox peek "$job_id" stdout.log 2>&1) || true
    echo "stdout.log content: $content"
    if ! echo "$content" | grep -q "hello from cmd"; then
        echo "FAIL: stdout.log should contain 'hello from cmd'"
        return 1
    fi

    echo "PASS: cmd tool"
}

verify_download() {
    # Test `dirac job sandbox get` for one job — downloads to a temp dir
    local job_id=$1
    echo "--- Verifying sandbox download (Job $job_id) ---"
    local tmpdir
    tmpdir=$(mktemp -d)
    trap "rm -rf $tmpdir" RETURN

    dirac job sandbox get "$job_id" -o "$tmpdir" 2>&1
    echo "Downloaded files:"
    ls -la "$tmpdir"

    if [[ ! -f "$tmpdir/stdout.log" ]]; then
        echo "FAIL: stdout.log not downloaded"
        return 1
    fi

    echo "PASS: sandbox download"
}

# ── Main ─────────────────────────────────────────────────────────────────────

if [[ "${1:-}" == "--verify" ]]; then
    # Verify a single job (for manual use)
    shift
    job_id=$1
    verify_echo_tool "$job_id"
    exit $?
fi

# ── Submit phase ─────────────────────────────────────────────────────────────

echo "=== Submitting E2E test jobs ==="
echo

echo "Test 1: echo-tool (stdout/stderr capture)"
ECHO_JOB=$(submit_job cwl echo-tool.cwl echo-tool-input.yml)
echo "  Job ID: $ECHO_JOB"
echo

echo "Test 2: fail-tool (failure status)"
FAIL_JOB=$(submit_job cwl fail-tool.cwl)
echo "  Job ID: $FAIL_JOB"
echo

echo "Test 3: sandbox-tool (input sandbox + output sandbox)"
SANDBOX_JOB=$(submit_job cwl sandbox-tool.cwl sandbox-tool-input.yml)
echo "  Job ID: $SANDBOX_JOB"
echo

echo "Test 4: cmd (auto-generated CWL)"
CMD_JOB=$(submit_job cmd echo hello from cmd)
echo "  Job ID: $CMD_JOB"
echo

ALL_JOBS=("$ECHO_JOB" "$FAIL_JOB" "$SANDBOX_JOB" "$CMD_JOB")
echo "Submitted jobs: ${ALL_JOBS[*]}"
echo

if [[ "${1:-}" == "--submit" ]]; then
    echo "Submit-only mode. Verify later with:"
    echo "  ./run-e2e.sh --verify <job_id>"
    exit 0
fi

# ── Wait phase ───────────────────────────────────────────────────────────────

wait_for_jobs "${ALL_JOBS[@]}"
echo

# ── Verify phase ─────────────────────────────────────────────────────────────

echo "=== Verifying results ==="
echo
FAILURES=0

verify_echo_tool "$ECHO_JOB" || FAILURES=$((FAILURES + 1))
echo
verify_fail_tool "$FAIL_JOB" || FAILURES=$((FAILURES + 1))
echo
verify_sandbox_tool "$SANDBOX_JOB" || FAILURES=$((FAILURES + 1))
echo
verify_cmd_tool "$CMD_JOB" || FAILURES=$((FAILURES + 1))
echo

# Also test the download command on the echo job
verify_download "$ECHO_JOB" || FAILURES=$((FAILURES + 1))
echo

# ── Summary ──────────────────────────────────────────────────────────────────

echo "=== Summary ==="
TOTAL=5
PASSED=$((TOTAL - FAILURES))
echo "$PASSED/$TOTAL tests passed"

if (( FAILURES > 0 )); then
    echo "SOME TESTS FAILED"
    exit 1
fi
echo "ALL TESTS PASSED"
