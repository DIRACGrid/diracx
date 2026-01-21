#!/usr/bin/env bash
# Collect Kubernetes debug information with GitHub Actions formatting
# Usage: collect-k8s-debug-info.sh <demo_dir> [service_prefix]
#   demo_dir: Path to demo directory containing kube.conf
#   service_prefix: Pod name prefix for main services (default: diracx)

set -euo pipefail

DEMO_DIR="${1:?Usage: $0 <demo_dir> [service_prefix]}"
SERVICE_PREFIX="${2:-diracx}"

export KUBECONFIG="${DEMO_DIR}/kube.conf"
export PATH="${DEMO_DIR}:$PATH"

INFRA_PODS="(dex|minio|mysql|rabbitmq|opensearch)"

# Function to output all info for a single pod
output_pod_info() {
  local pod_name="$1"
  local tail_arg=("${@:2}")

  echo "=== Pod Description ==="
  kubectl describe pod/"${pod_name}" 2>&1 || true

  # Init containers
  init_containers=$(kubectl get pods "$pod_name" -o jsonpath='{.spec.initContainers[*].name}' 2>/dev/null || true)
  if [ -n "$init_containers" ]; then
    echo ""
    echo "=== Init Container Logs ==="
    for container_name in $init_containers; do
      echo "--- $container_name ---"
      kubectl logs "${pod_name}" -c "${container_name}" "${tail_arg[@]}" 2>&1 || true
    done
  fi

  # Main containers
  echo ""
  echo "=== Container Logs ==="
  for container_name in $(kubectl get pods "$pod_name" -o jsonpath='{.spec.containers[*].name}'); do
    echo "--- $container_name ---"
    kubectl logs "${pod_name}" -c "${container_name}" "${tail_arg[@]}" 2>&1 || true
  done
}

# === JOB SUMMARY ===
{
  echo "## 🔍 Integration Test Debug Summary"
  echo ""
  echo "### Pod Status"
  echo '```'
  kubectl get pods -o wide
  echo '```'
  echo ""
} >> "${GITHUB_STEP_SUMMARY:-/dev/null}"

# === POD STATUS OVERVIEW ===
echo "::group::📋 Pod Status Overview"
kubectl get pods -o wide
echo "::endgroup::"

# === MAIN SERVICE PODS (shown first) ===
for pod_name in $(kubectl get pods -o json | jq -r '.items[] | .metadata.name' | grep -E "^${SERVICE_PREFIX}-" || true); do
  echo "::group::🚀 ${pod_name}"
  output_pod_info "$pod_name"
  echo "::endgroup::"
done

# === OTHER APPLICATION PODS ===
for pod_name in $(kubectl get pods -o json | jq -r '.items[] | .metadata.name' | grep -vE "$INFRA_PODS" | grep -vE "^${SERVICE_PREFIX}-" || true); do
  if [ -n "$pod_name" ]; then
    echo "::group::📦 ${pod_name}"
    output_pod_info "$pod_name"
    echo "::endgroup::"
  fi
done

# === INFRASTRUCTURE PODS ===
for pod_name in $(kubectl get pods -o json | jq -r '.items[] | .metadata.name' | grep -E "$INFRA_PODS" || true); do
  echo "::group::🏗️ ${pod_name}"
  output_pod_info "$pod_name" --tail=100
  echo "::endgroup::"
done

# === ERROR SUMMARY WITH ANNOTATIONS ===
echo "::group::⚠️ Error Summary"
error_count=0
for pod_name in $(kubectl get pods -o json | jq -r '.items[] | .metadata.name' | grep -E "^${SERVICE_PREFIX}-" || true); do
  for container_name in $(kubectl get pods "$pod_name" -o jsonpath='{.spec.containers[*].name}'); do
    errors=$(kubectl logs "${pod_name}" -c "${container_name}" 2>&1 | grep -iE "(ERROR|CRITICAL|Exception|Traceback)" | head -5 || true)
    if [ -n "$errors" ]; then
      echo "Errors in ${pod_name}/${container_name}:"
      echo "$errors"
      echo ""
      # Add annotation (limited to avoid exceeding quota)
      if [ $error_count -lt 8 ]; then
        first_error=$(echo "$errors" | head -1 | cut -c1-200)
        echo "::warning title=Error in ${pod_name}/${container_name}::${first_error}"
        ((error_count++)) || true
      fi
    fi
  done
done
if [ $error_count -eq 0 ]; then
  echo "No errors found in ${SERVICE_PREFIX} service logs"
fi
echo "::endgroup::"

# === CHECK FOR DEMO SUCCESS ===
if [ ! -f "${DEMO_DIR}/.success" ]; then
  echo "::error title=Demo Failed::Demo did not complete successfully"
  {
    echo ""
    echo "## ❌ Demo Failed"
    echo '```'
    cat "${DEMO_DIR}/.failed" 2>/dev/null || echo "No failure details available"
    echo '```'
  } >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
  cat "${DEMO_DIR}/.failed" 2>/dev/null || true
  exit 1
fi

echo "✅ Demo completed successfully"
