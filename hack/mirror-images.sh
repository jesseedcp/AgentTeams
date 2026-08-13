#!/bin/bash
# mirror-images.sh - Mirror upstream images to Higress registry (multi-arch)
#
# 镜像镜像（mirror）是把相同 digest/多架构 manifest 复制到更近的 registry，不是
# 重新编译代码。脚本只应写入明确的 TARGET_REGISTRY/TARGET_NS；先用 DRY_RUN 核对
# 源/目标和 tag，避免覆盖正式仓库。registry 登录文件含凭据，不能提交或打印。
#
# Uses skopeo to copy multi-arch manifests from upstream registries to
# the cn-hangzhou PRIMARY registry. Regional mirrors auto-sync from it:
#
#   Primary (push target): higress-registry.cn-hangzhou.cr.aliyuncs.com
#   Mirror (North America): higress-registry.us-west-1.cr.aliyuncs.com
#   Mirror (Southeast Asia): higress-registry.ap-southeast-7.cr.aliyuncs.com
#
# Prerequisites:
#   - skopeo installed (or use the container mode below)
#   - Logged in to the target registry
#
# Usage:
#   # Mirror all images (interactive login prompt)
#   ./hack/mirror-images.sh
#
#   # Mirror a single image by name
#   ./hack/mirror-images.sh tuwunel
#
#   # Dry-run (show commands without executing)
#   DRY_RUN=1 ./hack/mirror-images.sh
#
#   # Use skopeo container instead of local binary
#   USE_CONTAINER=1 ./hack/mirror-images.sh
#
#   # Override target registry / namespace
#   TARGET_REGISTRY=my-registry.example.com TARGET_NS=myns ./hack/mirror-images.sh
#
#   # Override the persistent auth file used in container mode
#   SKOPEO_AUTH_FILE=/path/to/auth.json USE_CONTAINER=1 ./hack/mirror-images.sh

set -euo pipefail

# ============================================================
# Configuration
# ============================================================

TARGET_REGISTRY="${TARGET_REGISTRY:-higress-registry.cn-hangzhou.cr.aliyuncs.com}"
TARGET_NS="${TARGET_NS:-higress}"
TARGET_PREFIX="docker://${TARGET_REGISTRY}/${TARGET_NS}"

DATE_TAG="${DATE_TAG:-$(date +%Y%m%d)}"
DRY_RUN="${DRY_RUN:-}"
USE_CONTAINER="${USE_CONTAINER:-}"
SKOPEO_IMAGE="${SKOPEO_IMAGE:-quay.io/skopeo/stable:latest}"
SKOPEO_AUTH_FILE="${SKOPEO_AUTH_FILE:-${HOME}/.config/containers/auth.json}"

# ============================================================
# Image mapping: SOURCE -> TARGET_NAME:TAG
#
# Format: "source_image|target_name|target_tag"
# ============================================================

IMAGES=(
    "ghcr.io/matrix-construct/tuwunel:main|tuwunel|${DATE_TAG}"
    "quay.io/minio/minio:latest|minio|${DATE_TAG}"
    "quay.io/minio/mc:latest|mc|${DATE_TAG}"
    "ghcr.io/cinnyapp/cinny:v4.12.3|cinny|v4.12.3"
    "docker.io/library/node:20-slim|node|20-slim"
    "docker.io/library/ubuntu:24.04|ubuntu|24.04"
    "docker.io/library/golang:1.23-alpine|golang|1.23-alpine"
    "docker.io/library/alpine:3.20|alpine|3.20"
)

# ============================================================
# Helpers
# ============================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 逻辑说明：输出镜像同步过程信息；这些日志不应包含 registry 密码或认证文件内容。
log()   { echo -e "${CYAN}[mirror]${NC} $1"; }
# 逻辑说明：标记单项复制或认证检查成功，供人工从批量输出中快速定位结果。
ok()    { echo -e "${GREEN}[  OK  ]${NC} $1"; }
# 逻辑说明：记录单项失败；是否终止由调用该函数的控制流决定，以便批量同步可以汇总。
fail()  { echo -e "${RED}[ FAIL ]${NC} $1"; }
# 逻辑说明：报告不阻断后续复制的风险或降级路径。
warn()  { echo -e "${YELLOW}[ WARN ]${NC} $1"; }

# 逻辑说明：返回宿主认证文件目录，容器运行 skopeo 时只挂载这个精确目录而不是整个 HOME。
container_auth_dir() {
    dirname "${SKOPEO_AUTH_FILE}"
}

# 逻辑说明：把宿主认证文件映射为容器内 `/auth` 路径，文件名保持稳定供 skopeo 使用。
container_auth_file() {
    printf '/auth/%s' "$(basename "${SKOPEO_AUTH_FILE}")"
}

# 逻辑说明：优先调用本机 skopeo；缺失时在受限挂载的容器中执行同一参数，并传递认证文件。
run_skopeo() {
    if [ -n "${USE_CONTAINER}" ]; then
        local auth_dir auth_file
        auth_dir=$(container_auth_dir)
        auth_file=$(container_auth_file)
        mkdir -p "${auth_dir}"

        docker run --rm \
            -e "REGISTRY_AUTH_FILE=${auth_file}" \
            -v "${auth_dir}:/auth:ro" \
            "${SKOPEO_IMAGE}" \
            "$@"
    else
        skopeo "$@"
    fi
}

# ============================================================
# Login check / prompt
# ============================================================

# 逻辑说明：先复用已有登录，缺失时按环境凭据登录；认证失败在复制镜像前终止，避免产生误导性部分结果。
check_login() {
    log "Checking authentication to ${TARGET_REGISTRY}..."

    if run_skopeo login --get-login "${TARGET_REGISTRY}" > /dev/null 2>&1; then
        ok "Already authenticated to ${TARGET_REGISTRY}"
        return 0
    fi

    warn "Not authenticated. Please login:"
    if [ -n "${USE_CONTAINER}" ]; then
        local auth_dir auth_file
        auth_dir=$(container_auth_dir)
        auth_file=$(container_auth_file)
        mkdir -p "${auth_dir}"

        echo "  docker run -it --rm -e REGISTRY_AUTH_FILE=${auth_file} -v ${auth_dir}:/auth ${SKOPEO_IMAGE} login ${TARGET_REGISTRY}"
        echo ""
        echo "Or set USE_CONTAINER= and login locally:"
        echo "  skopeo login ${TARGET_REGISTRY}"
    else
        echo "  skopeo login ${TARGET_REGISTRY}"
    fi

    read -p "Attempt login now? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if [ -n "${USE_CONTAINER}" ]; then
            docker run -it --rm \
                -e "REGISTRY_AUTH_FILE=${auth_file}" \
                -v "${auth_dir}:/auth" \
                "${SKOPEO_IMAGE}" \
                login "${TARGET_REGISTRY}"
        else
            skopeo login "${TARGET_REGISTRY}"
        fi
    else
        fail "Login required. Exiting."
        exit 1
    fi
}

# ============================================================
# Copy a single image
# ============================================================

# 逻辑说明：解析单个源/目标/tag 映射并用 skopeo 复制完整多架构 manifest；dry-run 只展示而不写 registry。
copy_image() {
    local entry="$1"
    local source_image target_name target_tag

    IFS='|' read -r source_image target_name target_tag <<< "${entry}"

    local target="${TARGET_PREFIX}/${target_name}:${target_tag}"

    log "Copying: docker://${source_image}"
    log "     -> ${target}"

    if [ -n "${DRY_RUN}" ]; then
        warn "[dry-run] skopeo copy --multi-arch all docker://${source_image} ${target}"
        return 0
    fi

    if run_skopeo copy --multi-arch all \
        "docker://${source_image}" \
        "${target}"; then
        ok "${target_name}:${target_tag} mirrored successfully"
    else
        fail "${target_name}:${target_tag} FAILED"
        return 1
    fi
}

# ============================================================
# Main
# ============================================================

# 逻辑说明：完成认证后按清单逐项复制并汇总失败；入口统一决定最终退出码供 CI/人工判断。
main() {
    echo ""
    echo "============================================"
    echo "  AgentTeams Image Mirror (skopeo multi-arch)"
    echo "============================================"
    echo "  Target:  ${TARGET_REGISTRY}/${TARGET_NS}/"
    echo "  Date tag: ${DATE_TAG}"
    [ -n "${DRY_RUN}" ] && echo "  Mode:    DRY RUN"
    [ -n "${USE_CONTAINER}" ] && echo "  Skopeo:  container (${SKOPEO_IMAGE})"
    echo "============================================"
    echo ""

    # Filter by name if argument given
    local filter="${1:-}"
    local to_copy=()

    if [ -n "${filter}" ]; then
        for entry in "${IMAGES[@]}"; do
            local name
            name=$(echo "${entry}" | cut -d'|' -f2)
            if [ "${name}" = "${filter}" ]; then
                to_copy+=("${entry}")
            fi
        done
        if [ ${#to_copy[@]} -eq 0 ]; then
            fail "No image matching '${filter}'. Available:"
            for entry in "${IMAGES[@]}"; do
                echo "  - $(echo "${entry}" | cut -d'|' -f2)"
            done
            exit 1
        fi
    else
        to_copy=("${IMAGES[@]}")
    fi

    # Show plan
    log "Images to mirror:"
    for entry in "${to_copy[@]}"; do
        local src tgt tag
        IFS='|' read -r src tgt tag <<< "${entry}"
        echo "  ${src}  ->  ${TARGET_NS}/${tgt}:${tag}"
    done
    echo ""

    if [ -z "${DRY_RUN}" ]; then
        check_login
    fi

    # Copy
    local total=${#to_copy[@]}
    local passed=0
    local failed=0

    for entry in "${to_copy[@]}"; do
        if copy_image "${entry}"; then
            passed=$((passed + 1))
        else
            failed=$((failed + 1))
        fi
        echo ""
    done

    # Summary
    echo "============================================"
    echo "  Mirror Summary"
    echo "============================================"
    echo -e "  Total:  ${total}"
    echo -e "  ${GREEN}Passed: ${passed}${NC}"
    [ "${failed}" -gt 0 ] && echo -e "  ${RED}Failed: ${failed}${NC}"
    echo "============================================"

    if [ "${failed}" -gt 0 ]; then
        echo ""
        warn "Some images failed. Re-run with the image name to retry:"
        echo "  ./hack/mirror-images.sh <image-name>"
        exit 1
    fi

    echo ""
    ok "All images mirrored. Update Dockerfiles if DATE_TAG changed from the current value."
}

main "$@"
