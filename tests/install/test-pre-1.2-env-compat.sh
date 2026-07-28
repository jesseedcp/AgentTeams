#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
INSTALLER="${ROOT_DIR}/install/agentteams-install.sh"
AGENTTEAMS_KNOWN_STABLE_VERSION="latest"

eval "$(
    sed -n \
        -e '/^_normalize_version()/,/^}/p' \
        -e '/^_ver_lt()/,/^}/p' \
        -e '/^_use_legacy_image_env()/,/^}/p' \
        -e '/^_controller_env_prefix()/,/^}/p' \
        "${INSTALLER}"
)"

assert_normalized() {
    local input="$1" expected="$2" actual
    actual="$(_normalize_version "${input}")"
    if [ "${actual}" != "${expected}" ]; then
        echo "FAIL: ${input} normalized to ${actual}, expected ${expected}" >&2
        exit 1
    fi
}

assert_normalized "1.2.0.beta.1" "v1.2.0-beta.1"
assert_normalized "v1.2.0-beta.1" "v1.2.0-beta.1"
assert_normalized "1.1.2+build.7" "v1.1.2+build.7"
assert_normalized "1.1" "v1.1.0"
assert_normalized "latest" "latest"

assert_legacy() {
    _use_legacy_image_env "$1" || {
        echo "FAIL: expected legacy env compatibility for $1" >&2
        exit 1
    }
}

assert_current() {
    if _use_legacy_image_env "$1"; then
        echo "FAIL: did not expect legacy env compatibility for $1" >&2
        exit 1
    fi
}

assert_legacy "v1.0.0"
assert_legacy "v1.1.2"
assert_legacy "v1.1.9+build.7"
assert_current "v1.2.0"
assert_current "v1.2.0-rc.1"
assert_current "v1.3.0"
assert_current "garbage"

AGENTTEAMS_KNOWN_STABLE_VERSION="v1.1.2"
assert_legacy "latest"
AGENTTEAMS_KNOWN_STABLE_VERSION="v1.2.0"
assert_current "latest"
AGENTTEAMS_KNOWN_STABLE_VERSION="latest"
assert_current "latest"

legacy_prefix='HIC''LAW_'
for pair in \
    "v1.1.2:${legacy_prefix}" \
    "v1.2.0:AGENTTEAMS_" \
    "v1.2.0-beta.1:AGENTTEAMS_" \
    "garbage:AGENTTEAMS_"
do
    version="${pair%%:*}"
    expected="${pair#*:}"
    actual="$(_controller_env_prefix "${version}")"
    if [ "${actual}" != "${expected}" ]; then
        echo "FAIL: ${version} prefix ${actual}, expected ${expected}" >&2
        exit 1
    fi
done

controller_env_block="$(
    sed -n \
        '/        # Controller env args/,/        # Timezone/p' \
        "${INSTALLER}"
)"

for suffix in \
    REGISTRATION_TOKEN \
    MINIO_USER \
    MINIO_PASSWORD \
    MANAGER_IMAGE \
    WORKER_IMAGE \
    COPAW_WORKER_IMAGE \
    HERMES_WORKER_IMAGE \
    QWENPAW_WORKER_IMAGE \
    MATRIX_DOMAIN \
    MATRIX_URL \
    MINIO_ENDPOINT \
    CONTROLLER_URL \
    DOCKER_NETWORK
do
    if ! grep -Fq "\${_ctrl_env_prefix}${suffix}=" <<<"${controller_env_block}"; then
        echo "FAIL: controller env ${suffix} does not use the selected prefix" >&2
        exit 1
    fi
    if grep -Fq "\"AGENTTEAMS_${suffix}=" <<<"${controller_env_block}" ||
        grep -Fq "\"${legacy_prefix}${suffix}=" <<<"${controller_env_block}"; then
        echo "FAIL: controller env ${suffix} also has a fixed prefix" >&2
        exit 1
    fi
done

echo "PASS: Bash installer selects one controller env contract by image version"
