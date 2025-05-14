#!/usr/bin/env bash
# Enable the unofficial bash strict mode
# See: http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail
IFS=$'\n\t'

usage() {
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo "Collect coverage data from a demo run."
    echo
    echo "Options:"
    echo "  --demo-dir <path>         Path to the demo directory."
    echo "  --diracx-repo <path>     Path to the DiracX repository."
    echo "  --extension-name <name>  Name of the extension to collect coverage for."
    echo "  --extension-repo <path>  Path to the extension repository."
    echo "  -h, --help              Show this help message and exit."
    echo
    exit 1
}

# Parse command line arguments
demo_dir=""
diracx_repo=""
extension_name=""
extension_repo=""

while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --demo-dir)
            if [[ -n "${2:-}" ]]; then
                demo_dir="$2"
                shift 2
            else
                echo "Error: --demo-dir requires a value."
                usage
            fi
            ;;
        --diracx-repo)
            if [[ -n "${2:-}" ]]; then
                diracx_repo=$(realpath "$2")
                shift 2
            else
                echo "Error: --diracx-repo requires a value."
                usage
            fi
            ;;
        --extension-name)
            if [[ -n "${2:-}" ]]; then
                extension_name="$2"
                shift 2
            else
                echo "Error: --extension-name requires a value."
                usage
            fi
            ;;
        --extension-repo)
            if [[ -n "${2:-}" ]]; then
                extension_repo=$(realpath "$2")
                shift 2
            else
                echo "Error: --extension-repo requires a value."
                usage
            fi
            ;;
        *)
            echo "Unknown parameter passed: $1"
            usage
            ;;
    esac
done

# Argument validation
if [[ -z "$demo_dir" ]]; then
    echo "Error: --demo-dir is required."
    usage
fi
if [[ -z "${diracx_repo:-}" && -z "${extension_repo:-}" ]]; then
    echo "Error: At least one of --diracx-repo or --extension-repo is required."
    usage
fi
if [[ -n "${extension_name:-}" && -z "${extension_repo:-}" ]]; then
    echo "Error: --extension-name is required when --extension-repo is provided."
    usage
fi
if [[ ! -f ".coveragerc" ]]; then
    echo "Error: Expected .coveragerc file not found in the current directory."
    exit 1
fi

# Set up the environment
export KUBECONFIG=${demo_dir}/kube.conf
export PATH=${demo_dir}:$PATH

if ! kubectl cluster-info > /dev/null 2>&1; then
    echo "Error: The demo does not appear to be running."
    exit 1
fi

# Shutdown the pods so we collect coverage data
pods=$(kubectl get pods -o json | jq -r '.items[] | .metadata.name')
for pod_name in $(echo "${pods}" | grep -vE '(dex|minio|mysql|rabbitmq|opensearch)'); do
    kubectl delete pod/"${pod_name}"
done

# Combine the coverage data from the demo and make an XML report
coverage_data=$(mktemp)
echo "Changing ownership of coverage data to $(id -u)"
sudo chown -R "$(id -u)" "${demo_dir}"/coverage-reports/
coverage combine --keep --data-file "${coverage_data}" "${demo_dir}"/coverage-reports/*

# coverage can't handle having multiple src directories, so we need to make a fake one with symlinks
fake_module=$(mktemp -d)

# Ensure we clean up the fake module and restore the original .coveragerc
cleanup() {
    rm -rf "${fake_module}"
    if [[ -f ".coveragerc.bak" ]]; then
        mv ".coveragerc.bak" .coveragerc
    fi
}
trap cleanup EXIT

# Symlink DiracX into the fake module (if provided)
if [[ -n "${diracx_repo:-}" ]]; then
    mkdir -p "${fake_module}/src/diracx"
    for fn in "${diracx_repo}"/*/src/diracx/*; do
        ln -sf "${fn}" "${fake_module}/src/diracx/$(basename "${fn}")"
    done
fi

# Symlink the extension into the fake module (if provided)
if [[ -n "${extension_repo:-}" ]]; then
    mkdir -p "${fake_module}/src/${extension_name}/extensions"
    for fn in "${extension_repo}"/*/src/"${extension_name}"/*; do
        ln -sf "${fn}" "${fake_module}/src/${extension_name}/$(basename "${fn}")"
    done
fi

# Edit the .coveragerc file to point to the fake module
sed -i.bak "s@source =@source =\n    ${fake_module}/src@g" .coveragerc
if diff -q .coveragerc .coveragerc.bak > /dev/null; then
    echo "Error: .coveragerc was not modified."
    exit 1
fi

# Make the actual coverage report
coverage xml -o coverage-demo.xml --data-file "${coverage_data}"
