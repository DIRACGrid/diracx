#!/usr/bin/env bash
# Run the full DiracX stack locally: seaweedfs, Redis, uvicorn, scheduler, workers
set -euo pipefail
IFS=$'\n\t'

tmp_dir=$(mktemp -d)
echo "Using temp dir: ${tmp_dir}"
mkdir -p "${tmp_dir}/keystore" "${tmp_dir}/cs_store/" "${tmp_dir}/seaweedfs" "${tmp_dir}/logs"

state_key="$(head -c 32 /dev/urandom | base64)"

function log_prefix() {
  local prefix=$1
  while IFS= read -r line; do
    printf "[%-10s] [%s] %s\n" "$prefix" "$(date +%H:%M:%S)" "$line"
  done
}

# Rewrite the pinned banner area (below the scroll region)
function update_banner() {
  local text=$1
  current_banner="$text"
  if [ "$use_alt_screen" = true ]; then
    # Save cursor, move to banner area, clear it, print new banner, restore cursor
    printf '\033[s'
    local i
    for i in $(seq "$((scroll_end + 1))" "$term_height"); do
      printf '\033[%d;1H\033[2K' "$i"
    done
    printf '\033[%d;1H' "$((scroll_end + 1))"
    printf '%s\n' "$text"
    printf '\033[u'
  fi
}

# Pre-flight check: ensure a port is available before starting services
function check_port() {
  local port=$1
  local service=$2
  if python -c "import socket; s=socket.socket(); s.settimeout(0.5); exit(0 if s.connect_ex(('localhost',$port))==0 else 1)"; then
    echo "❌ Port $port is already in use (needed by $service)"
    echo "   To find what's using it:  lsof -i :$port"
    exit 1
  fi
}

# Health check with process liveness detection
function wait_for_service() {
  local name=$1 pid=$2 log_file=$3
  shift 3
  for _ in $(seq 1 10); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "❌ $name (PID $pid) exited during startup"
      echo "   Last lines of $log_file:"
      tail -20 "$log_file" 2>/dev/null | sed 's/^/   /'
      return 1
    fi
    if "$@" > /dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "❌ $name did not become ready within 10s"
  echo "   Last lines of $log_file:"
  tail -20 "$log_file" 2>/dev/null | sed 's/^/   /'
  return 1
}

function redis_ready() { redis-cli -p 6379 ping 2>/dev/null | grep -q PONG; }

# Make a keystore
keystore="${tmp_dir}/keystore/jwks.json"
python -m diracx.logic rotate-jwk --jwks-path "${keystore}"

# Make a fake CS
dirac internal generate-cs "${tmp_dir}/cs_store/initialRepo"

dirac internal add-vo "${tmp_dir}/cs_store/initialRepo" \
    --vo=diracAdmin \
    --idp-url=runlocal.diracx.invalid \
    --idp-client-id="idp-client-id" \
    --default-group=admin

dirac internal add-user "${tmp_dir}/cs_store/initialRepo" \
  --vo=diracAdmin --group=admin \
  --sub=75212b23-14c2-47be-9374-eb0113b0575e \
  --preferred-username=localuser

export DIRACX_CONFIG_BACKEND_URL="git+file://${tmp_dir}/cs_store/initialRepo"
# Dynamically discover all registered databases from entry points and configure
# them with sqlite URLs. OS DBs use a JSON wrapper because the testing setup
# monkey-patches them to use sqlite instead of OpenSearch.
eval "$(python -m diracx.db generate-local-urls "${tmp_dir}")"
export DIRACX_SERVICE_AUTH_TOKEN_KEYSTORE="file://${keystore}"
export DIRACX_SERVICE_AUTH_STATE_KEY="${state_key}"
hostname_lower=$(hostname | tr -s '[:upper:]' '[:lower:]')
export DIRACX_SERVICE_AUTH_TOKEN_ISSUER="http://${hostname_lower}:8000"
export DIRACX_SERVICE_AUTH_ALLOWED_REDIRECTS='["http://'"$hostname_lower"':8000/docs/oauth2-redirect"]'
export DIRACX_SANDBOX_STORE_BUCKET_NAME=sandboxes
export DIRACX_SANDBOX_STORE_AUTO_CREATE_BUCKET=true
export DIRACX_SANDBOX_STORE_S3_CLIENT_KWARGS='{"endpoint_url": "http://localhost:8333", "aws_access_key_id": "console", "aws_secret_access_key": "console123"}'
export DIRACX_TASKS_REDIS_URL="redis://localhost:6379"

# Write all DIRACX env vars to a sourceable file for use in other terminals
script_dir="$(cd "$(dirname "$0")" && pwd)"
env_file="${tmp_dir}/env.sh"
cat > "$env_file" <<ENVEOF
# Guard: ensure this is sourced in the same pixi environment
if [ "\$CONDA_PREFIX" != "$CONDA_PREFIX" ]; then
  echo "❌ Environment mismatch: local-start was run in $(basename "$CONDA_PREFIX"),"
  echo "   but this shell is using \$(basename "\$CONDA_PREFIX" 2>/dev/null || echo "no environment")."
  echo "   Use the same pixi environment (e.g. pixi run -e default-gubbins local-shell)"
  return 1 2>/dev/null || exit 1
fi
ENVEOF
env | grep '^DIRACX_' | while read -r line; do
  name="${line%%=*}"
  value="${line#*=}"
  printf 'export %s=%q\n' "$name" "$value" >> "$env_file"
done
echo "export DIRACX_URL=http://${hostname_lower}:8000" >> "$env_file"

# Write pointer so local-shell and local-tasks can find env.sh
echo "$env_file" > "${script_dir}/.run-local-env"

# Configure seaweedfs S3 credentials
cat > "${tmp_dir}/seaweedfs_s3.json" <<'EOF'
{
  "identities": [
    {
      "name": "admin",
      "credentials": [
        {
          "accessKey": "console",
          "secretKey": "console123"
        }
      ],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}
EOF

echo "🔍 Checking ports..."
check_port 6379 "Redis"
check_port 8333 "SeaweedFS S3"
check_port 8000 "uvicorn"

# Start all services, directing output to log files
weed mini -dir="${tmp_dir}/seaweedfs" -s3.config="${tmp_dir}/seaweedfs_s3.json" > "${tmp_dir}/logs/seaweedfs.log" 2>&1 &
seaweedfs_pid=$!
redis-server --port 6379 --save "" --appendonly no > "${tmp_dir}/logs/redis.log" 2>&1 &
redis_pid=$!

# Ensure infrastructure is cleaned up if startup fails
function cleanup_infra() {
  kill "$seaweedfs_pid" "$redis_pid" 2>/dev/null || true
  rm -f "${script_dir}/.run-local-env"
  rm -rf "${tmp_dir}"
}
trap cleanup_infra EXIT

# Wait for infrastructure before starting app services
echo "⏳ Waiting for infrastructure..."
if ! wait_for_service "SeaweedFS" "$seaweedfs_pid" "${tmp_dir}/logs/seaweedfs.log" \
     curl --silent --max-time 2 --head http://localhost:8333; then
  exit 1
fi
if ! wait_for_service "Redis" "$redis_pid" "${tmp_dir}/logs/redis.log" \
     redis_ready; then
  exit 1
fi

# Start application services
uvicorn --factory diracx.testing.routers.factory:create_app --reload > "${tmp_dir}/logs/uvicorn.log" 2>&1 &
diracx_pid=$!
diracx-task-run scheduler > "${tmp_dir}/logs/scheduler.log" 2>&1 &
scheduler_pid=$!
diracx-task-run worker --worker-size small --max-concurrent-tasks 1 > "${tmp_dir}/logs/worker-sm.log" 2>&1 &
worker_small_pid=$!
diracx-task-run worker --worker-size medium --max-concurrent-tasks 1 > "${tmp_dir}/logs/worker-md.log" 2>&1 &
worker_medium_pid=$!
diracx-task-run worker --worker-size large --max-concurrent-tasks 1 > "${tmp_dir}/logs/worker-lg.log" 2>&1 &
worker_large_pid=$!

all_pid_names=(seaweedfs redis uvicorn scheduler worker-sm worker-md worker-lg)
all_pid_values=($seaweedfs_pid $redis_pid $diracx_pid $scheduler_pid $worker_small_pid $worker_medium_pid $worker_large_pid)
all_log_files=(
  "${tmp_dir}/logs/seaweedfs.log"
  "${tmp_dir}/logs/redis.log"
  "${tmp_dir}/logs/uvicorn.log"
  "${tmp_dir}/logs/scheduler.log"
  "${tmp_dir}/logs/worker-sm.log"
  "${tmp_dir}/logs/worker-md.log"
  "${tmp_dir}/logs/worker-lg.log"
)
all_commands=(
  "weed mini -dir=${tmp_dir}/seaweedfs -s3.config=${tmp_dir}/seaweedfs_s3.json"
  "redis-server --port 6379 --save \"\" --appendonly no"
  "uvicorn --factory diracx.testing.routers.factory:create_app --reload"
  "diracx-task-run scheduler"
  "diracx-task-run worker --worker-size small --max-concurrent-tasks 1"
  "diracx-task-run worker --worker-size medium --max-concurrent-tasks 1"
  "diracx-task-run worker --worker-size large --max-concurrent-tasks 1"
)
all_restart_counts=(0 0 0 0 0 0 0)

function restart_process() {
  local i=$1
  local name=${all_pid_names[$i]}
  local log=${all_log_files[$i]}
  local cmd=${all_commands[$i]}
  local count=${all_restart_counts[$i]}
  count=$((count + 1))
  all_restart_counts[$i]=$count
  eval "$cmd" >> "$log" 2>&1 &
  all_pid_values[$i]=$!
}

# Wait for uvicorn
if wait_for_service "uvicorn" "$diracx_pid" "${tmp_dir}/logs/uvicorn.log" \
     curl --silent --max-time 2 --head http://localhost:8000; then
  status_line="✅ DiracX is running on http://localhost:8000"
else
  status_line="❌ Failed to start DiracX — check ${tmp_dir}/logs/uvicorn.log"
fi

# Build the services status line from current process state
function build_services_line() {
  local line="📊 Services:"
  for i in "${!all_pid_names[@]}"; do
    local name=${all_pid_names[$i]}
    local pid=${all_pid_values[$i]}
    local restarts=${all_restart_counts[$i]}
    if kill -0 "$pid" 2>/dev/null; then
      if [ "$restarts" -gt 0 ]; then
        line+=" ✅ ${name}(↻${restarts})"
      else
        line+=" ✅ ${name}"
      fi
    else
      line+=" ❌ ${name}"
    fi
  done
  echo "$line"
}

# Static part of the banner (services line is appended dynamically)
banner_static="
${status_line}
📋 To interact with DiracX you can:
  1️⃣  Open a configured shell:  pixi run local-shell
  2️⃣  Submit a task:  pixi run local-tasks submit <entry_point> [--args JSON]
  3️⃣  Swagger UI: http://localhost:8000/api/docs

SERVICES_PLACEHOLDER
📁 Logs: ${tmp_dir}/logs/
⚡ Press Ctrl+C to stop"

function build_banner() {
  echo "${banner_static/SERVICES_PLACEHOLDER/$(build_services_line)}"
}

banner="$(build_banner)"

banner_height=$(printf '%s\n' "$banner" | wc -l | tr -d ' ')
term_height=$(tput lines 2>/dev/null || echo 0)
use_alt_screen=false
scroll_end=0

if [ "$term_height" -gt 0 ] && [ "$((term_height - banner_height))" -ge 5 ]; then
  use_alt_screen=true
  # Save terminal settings and disable keyboard echo
  original_stty=$(stty -g)
  stty -echo
  # Enter alternate screen buffer (like vim/less/htop)
  tput smcup 2>/dev/null || use_alt_screen=false
fi

if [ "$use_alt_screen" = true ]; then
  # Pin banner at the bottom using a terminal scroll region
  scroll_end=$((term_height - banner_height - 1))
  printf '\033[1;%dr' "$scroll_end"
  printf '\033[%d;1H' "$((scroll_end + 1))"
  printf '%s\n' "$banner"
  printf '\033[%d;1H' "$scroll_end"
else
  echo ""
  printf '%s\n' "$banner"
fi

# Current banner text (updated on shutdown)
current_banner="$banner"

min_banner_lines=5  # minimum log lines visible above the banner

function handle_resize() {
  if [ "$use_alt_screen" != true ]; then return; fi
  term_height=$(tput lines 2>/dev/null || echo 0)
  # Reset everything first
  printf '\033[r'
  printf '\033[2J'
  if [ "$term_height" -le "$((banner_height + min_banner_lines))" ]; then
    # Too small for pinned banner — show a compact message and full-width logs
    scroll_end=0
    printf '\033[1;1H'
    printf '\033[1;7m ⚡ DiracX running — resize terminal for full banner \033[0m\n'
  else
    scroll_end=$((term_height - banner_height - 1))
    printf '\033[1;%dr' "$scroll_end"
    printf '\033[%d;1H' "$((scroll_end + 1))"
    printf '%s\n' "$current_banner"
    printf '\033[%d;1H' "$scroll_end"
  fi
}

trap handle_resize SIGWINCH

# Now start tailing log files — these are the only terminal writers,
# so they scroll cleanly within the scroll region.
tail -f "${tmp_dir}/logs/seaweedfs.log" 2>/dev/null | log_prefix "seaweedfs" &
tail -f "${tmp_dir}/logs/redis.log" 2>/dev/null | log_prefix "redis" &
tail -f "${tmp_dir}/logs/uvicorn.log" 2>/dev/null | log_prefix "uvicorn" &
tail -f "${tmp_dir}/logs/scheduler.log" 2>/dev/null | log_prefix "scheduler" &
tail -f "${tmp_dir}/logs/worker-sm.log" 2>/dev/null | log_prefix "worker-sm" &
tail -f "${tmp_dir}/logs/worker-md.log" 2>/dev/null | log_prefix "worker-md" &
tail -f "${tmp_dir}/logs/worker-lg.log" 2>/dev/null | log_prefix "worker-lg" &

# Ctrl+C handling: first press updates banner with shutdown message,
# second press actually stops everything.
shutting_down=false

function handle_interrupt() {
  if [ "$shutting_down" = true ]; then
    # Second Ctrl+C — force kill
    kill -9 "${all_pid_values[@]}" 2>/dev/null || true
    exit 0
  fi
  shutting_down=true

  # Build shutdown banner with PIDs
  local pid_list=""
  local i
  for i in "${!all_pid_names[@]}"; do
    local name=${all_pid_names[$i]}
    local pid=${all_pid_values[$i]}
    if kill -0 "$pid" 2>/dev/null; then
      pid_list+="    ${name} (PID ${pid})"$'\n'
    fi
  done

  local shutdown_banner="🛑 Shutting down DiracX...

⏳ Waiting for processes to exit — press Ctrl+C again to force kill (SIGKILL)

${pid_list}"

  update_banner "$shutdown_banner"

  # Gracefully stop all services
  kill "${all_pid_values[@]}" 2>/dev/null || true
}

trap handle_interrupt SIGINT

function cleanup(){
  trap - SIGINT SIGTERM SIGHUP
  kill "${all_pid_values[@]}" 2>/dev/null || true
  # Kill tail processes
  kill $(jobs -p) 2>/dev/null || true
  wait 2>/dev/null || true
  # Leave alternate screen — restores the original terminal content
  if [ "${use_alt_screen}" = true ]; then
    printf '\033[r'
    tput rmcup 2>/dev/null || true
    stty "${original_stty}" 2>/dev/null || true
  fi
  echo "🧹 Cleaning up"
  rm -f "${script_dir}/.run-local-env"
  rm -rf "${tmp_dir}"
}

trap "cleanup" EXIT

# Poll process status and update the banner periodically
while true; do
  # Short wait so we can poll — signals like SIGWINCH also interrupt this
  sleep 5 &
  wait $! 2>/dev/null || true

  # Break once all service processes have exited
  alive=false
  for pid in "${all_pid_values[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then alive=true; break; fi
  done
  if [ "$alive" = false ]; then break; fi

  # Restart any dead processes and refresh the banner
  if [ "$shutting_down" = false ]; then
    for i in "${!all_pid_values[@]}"; do
      if ! kill -0 "${all_pid_values[$i]}" 2>/dev/null; then
        restart_process "$i"
      fi
    done
    update_banner "$(build_banner)"
  fi
done
