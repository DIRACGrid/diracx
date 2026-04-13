# CWL E2E Test Files

Test CWL workflows for verifying job submission, input/output sandboxes, and status reporting on the grid.

## Running

```bash
./run-e2e.sh              # submit + wait + verify (full run)
./run-e2e.sh --submit     # submit only (prints job IDs)
./run-e2e.sh --verify ID  # verify a single completed job
```

## Tests

### 1. echo-tool — stdout/stderr capture

Submits `echo "Hello from E2E test"`. Verifies:

- Job completes with status Done
- Output sandbox contains `stdout.log` and `stderr.log`
- `stdout.log` contains the echo message

### 2. fail-tool — failure status

Runs `exit 42`. Verifies:

- Job status is Failed
- ApplicationStatus contains "fail"

### 3. sandbox-tool — input sandbox + output sandbox

Uploads `hello.sh` as an input sandbox, runs it on the worker. Verifies:

- Job completes with status Done
- Output sandbox contains `stdout.log` and `stderr.log`
- `stdout.log` contains output from `hello.sh` (proving the input sandbox was downloaded and executed)

### 4. cmd — auto-generated CWL

Uses `dirac job submit cmd -- echo "hello from cmd"`. Verifies:

- Job completes with status Done
- Auto-generated CWL produces `stdout.log` in output sandbox
- `stdout.log` contains the command output

### 5. sandbox download

Tests `dirac job sandbox get` — downloads output sandbox files to a temp directory and verifies `stdout.log` exists on disk.
