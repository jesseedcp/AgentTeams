# ============================================================
# agentteams-controller - Standalone build for Go binaries
# ============================================================
# Produces: /agentteams-controller, /agt, /kube-apiserver, /opt/agentteams/config/crd/
# Manager Dockerfile copies these artifacts via: COPY --from=agentteams-controller ...
#
# Since v0.2: includes merged worker lifecycle, auth, credentials, and proxy.
#
# 初学者说明：builder 负责编译 Controller 与 typed `agt` CLI；mc/kube-bin 阶段只
# 提供现成二进制；最终镜像把它们组合起来。多阶段 `FROM` 不是启动多个容器。
# Controller 是一个 Manager、多 Worker 的控制面：它根据 CR 创建/更新各 Agent
# Pod，自己不承担 LLM 对话。Controller 开启 CGO 是为了 SQLite/kine 依赖；agt
# 不需要 CGO，因此可构建为更易移植的静态工具。

ARG HIGRESS_REGISTRY=higress-registry.cn-hangzhou.cr.aliyuncs.com

# ============ Stage 1: Build Go binaries ============
FROM ${HIGRESS_REGISTRY}/higress/golang:1.25-alpine AS builder

ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}
RUN apk add --no-cache gcc musl-dev

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download || true

COPY . .
RUN CGO_ENABLED=1 go build -ldflags '-extldflags "-static"' -o /agentteams-controller ./cmd/controller/
RUN CGO_ENABLED=0 go build -o /agt ./cmd/agt/

# ============ Stage 2: mc (MinIO Client) ============
FROM ${HIGRESS_REGISTRY}/higress/mc:20260216 AS mc

# ============ Stage 3: Download kube-apiserver (native arch to avoid QEMU TLS bugs) ============
FROM --platform=$BUILDPLATFORM ${HIGRESS_REGISTRY}/higress/alpine:3.20 AS kube-bin
ARG KUBE_VERSION=v1.31.3
# TARGETARCH is auto-injected by buildx for multi-arch builds. For plain
# `docker build` (no buildx) it is unset, so fall back to `uname -m` —
# safe here because the stage is pinned to BUILDPLATFORM and a non-buildx
# build is always single-arch == host arch.
ARG TARGETARCH
RUN apk add --no-cache curl && \
    ARCH="${TARGETARCH:-$(uname -m | sed 's/x86_64/amd64/; s/aarch64/arm64/')}" && \
    curl -fSL -o /kube-apiserver \
      "https://dl.k8s.io/release/${KUBE_VERSION}/bin/linux/${ARCH}/kube-apiserver" && \
    chmod +x /kube-apiserver

# ============ Final: Minimal image with all artifacts ============
FROM ${HIGRESS_REGISTRY}/higress/alpine:3.20

RUN apk add --no-cache bash jq curl unzip kubectl

COPY --from=mc /usr/bin/mc /usr/local/bin/mc
COPY --from=builder /agentteams-controller /usr/local/bin/agentteams-controller
COPY --from=builder /agt /usr/local/bin/agt
COPY --from=kube-bin /kube-apiserver /usr/local/bin/kube-apiserver
COPY config/crd/ /opt/agentteams/config/crd/
COPY agent/ /opt/agentteams/agent/

# Default endpoint + token-file for the bundled `agt` CLI. The controller process mints a
# long-lived admin SA token at startup (see internal/app.bootstrapAdminCLIToken)
# and writes it to AGENTTEAMS_AUTH_TOKEN_FILE; the CLI's discoverToken() then
# auto-loads it so operators don't need to pass `-e AGENTTEAMS_AUTH_TOKEN=…` on
# every exec. Both vars are container-local defaults — `docker exec -e` still
# overrides them, and the controller binary itself does not read them.
ENV AGENTTEAMS_CONTROLLER_URL=http://127.0.0.1:8090 \
    AGENTTEAMS_AUTH_TOKEN_FILE=/var/run/agentteams/cli-token

ENTRYPOINT ["agentteams-controller"]
