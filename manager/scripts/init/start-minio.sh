#!/bin/bash
# start-minio.sh - Start MinIO object storage (single node, single disk)
# embedded 本机模式用单节点 MinIO 保存共享文件。9000 是程序使用的 S3 API，9001
# 是人工 Console；/data/minio 必须挂载持久卷，否则删除 Controller 容器会丢数据。
# 生产环境不应依赖默认 admin 回退密码，凭据应由安装器生成并通过 env 注入。

export MINIO_ROOT_USER="${AGENTTEAMS_MINIO_USER:-${AGENTTEAMS_ADMIN_USER:-admin}}"
export MINIO_ROOT_PASSWORD="${AGENTTEAMS_MINIO_PASSWORD:-${AGENTTEAMS_ADMIN_PASSWORD:-admin}}"

mkdir -p /data/minio

exec minio server /data/minio --console-address ":9001" --address ":9000"
