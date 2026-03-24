#!/bin/bash
set -e

source /activate.sh

function install_sources() {
    extension_name=$1
    source_prefix=$2
    image_packages=$3

    IFS=','
    to_install=()
    for dir in ${!source_prefix}; do
        for package_name in ${!image_packages}; do
            if [[ "${package_name}" == "." ]]; then
                wheel_name="${extension_name}"
            else
                wheel_name="${extension_name}_${package_name}"
            fi
            wheels=( $(find "${dir}" -name "${wheel_name}-*.whl") )
            if [[ ${#wheels[@]} -gt 1 ]]; then
                echo "ERROR: Multiple wheels found for ${package_name} in ${dir}"
                exit 1
            elif [[ ${#wheels[@]} -eq 1 ]]; then
                to_install+=("${wheels[0]}")
            else
                if [[ "${package_name}" == "." ]]; then
                    src_dir=("${dir}")
                else
                    src_dir=("${dir}-${package_name}")
                fi
                if [[ -f "${src_dir}/pyproject.toml" ]]; then
                    to_install+=("${src_dir}")
                fi
            fi
        done
    done
    if [[ ${#to_install[@]} -gt 0 ]]; then
        pip install --no-deps "${to_install[@]}"
    fi
}


if [[ -n "${DIRACX_EXTENSIONS:-}" ]]; then
    # Install extensions in reverse order so that the base (diracx) is
    # installed last, allowing extension packages to override base packages.
    IFS=', ' read -r -a extension_array <<< "$DIRACX_EXTENSIONS"
    for (( idx=${#extension_array[@]}-1 ; idx>=0 ; idx-- )) ; do
        extension_name="${extension_array[idx]}"
        source_prefix="${extension_name^^}_CUSTOM_SOURCE_PREFIXES"
        image_packages="${extension_name^^}_IMAGE_PACKAGES"

        if [[ -n "${!source_prefix:-}" ]]; then
            install_sources "${extension_name}" "${source_prefix}" "${image_packages}"
        fi
    done
elif [[ -n "${DIRACX_CUSTOM_SOURCE_PREFIXES:-}" ]]; then
    # No extensions, just diracx
    install_sources "diracx" "DIRACX_CUSTOM_SOURCE_PREFIXES" "DIRACX_IMAGE_PACKAGES"
fi


exec "$@"
