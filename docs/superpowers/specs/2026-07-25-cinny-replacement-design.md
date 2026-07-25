# Cinny 完全替换 Element 设计

日期：2026-07-25

## 目标

把 AgentTeams Manager 内置的 Element Web 完整替换为 Cinny，同时保持现有
Matrix 服务、Agent 管理能力、数据卷以及用户访问地址不变。

- Docker 单机部署继续通过 `http://127.0.0.1:18388` 访问聊天界面。
- 当前 Kubernetes 测试部署继续通过 `http://127.0.0.1:18480` 访问聊天界面。
- 新安装的默认端口仍为 `18088`，避免因为更换客户端而改变网络契约。
- Matrix 账号、房间、消息、媒体、Manager/Worker 身份和运行状态均不迁移、不重建。

## 方案

### 1. 客户端镜像和静态配置

固定使用 Cinny `v4.12.3` 官方容器镜像，避免 `latest` 带来的不可重复部署。
Cinny 通过运行时生成的 `config.json` 连接现有 Higress/Matrix 公网入口：

- 当前网关 URL 作为默认 homeserver；
- 允许用户在确有需要时输入自定义 homeserver；
- 启用 hash router，使静态 Nginx 部署无需额外的路径重写规则；
- 保持单页应用回退和 `config.json` 禁止缓存。

### 2. Embedded Docker 部署

在 `agentteams-embedded` 镜像中：

- 用 Cinny 镜像阶段替换 Element Web 镜像阶段；
- 将静态资源放入 `/opt/cinny`；
- 以 `start-cinny.sh` 生成配置并启动同一个 Nginx；
- Supervisor 程序名、日志名和注释全部改成 Cinny；
- 内部继续监听 `8088`，因此宿主机现有的 `18388:8088` 映射无需改变；
- Higress WASM 插件静态服务仍由同一个 Nginx 在 `8002` 提供。

### 3. Kubernetes/Helm 部署

Helm Chart 的公开配置从 `elementWeb` 改为 `cinny`，资源名称改为：

- `agentteams-cinny` Deployment；
- `agentteams-cinny` Service；
- `agentteams-cinny-config` ConfigMap。

Controller 接收内部 `AGENTTEAMS_CINNY_URL`，并在 Higress 中注册 `cinny`
服务源和根路径路由。原网关地址和 NodePort 不变。

### 4. 配置兼容

新项目的规范变量使用：

- `AGENTTEAMS_PORT_CINNY`
- `AGENTTEAMS_CINNY_HOMESERVER_URL`
- `AGENTTEAMS_CINNY_URL`

安装器读取已有部署配置时，允许旧的
`AGENTTEAMS_PORT_ELEMENT_WEB` 和 `AGENTTEAMS_ELEMENT_HOMESERVER_URL`
作为一次性兼容回退。写回的新配置只使用 Cinny 变量。此兼容层不会下载、
启动或路由 Element。

Controller 配置层同样只把旧 URL 作为回退输入，确保正在升级的环境不会因
变量改名中断；所有新渲染和新部署均使用 Cinny 名称。

### 5. 数据和登录行为

Element 与 Cinny 是两个独立的 Matrix 网页客户端。替换客户端不会修改
Tuwunel 数据，但浏览器本地存储格式不同，因此 Element 的浏览器会话不能直接
交给 Cinny。管理员首次打开 Cinny 时需要使用原 Matrix 用户名和密码登录一次。
登录后会看到原来的房间、消息和成员。

## 测试与验收

1. 先更新测试，使其要求 Cinny 资源、镜像、配置与路由，并拒绝残留的 Element
   运行资源。
2. 验证 Shell/PowerShell 安装器语法和变量迁移。
3. 运行 Helm lint、模板渲染和项目现有相关测试。
4. 构建新的 embedded 镜像，复用当前 Docker 数据卷和环境配置替换容器。
5. 升级当前 Kind/Helm 测试部署，确认所有 Pod Ready，Manager 与 Worker
   保持运行。
6. 对 `18388` 和 `18480` 验证 Cinny 页面、`config.json`、Matrix
   `/_matrix/client/versions` 接口，并确认页面不再包含 Element 资源。
7. 使用现有管理员账号完成一次浏览器登录回归，不在日志或报告中输出密码。

## 不在本次范围

- 不更换 Matrix 服务端 Tuwunel。
- 不改变 AgentScope Manager 或 Worker 运行时。
- 不迁移或清空任何 Matrix/MinIO/Controller 数据。
- 不引入新的数据库、Redis 或会话服务。
- 不重做 Cinny 自身 UI；本次使用上游正式发布版。

## 风险与缓解

- **上游镜像拉取受限**：源码固定正式版本；当前 Kind 环境可先在 Docker 中拉取
  并本地导入，避免集群重复联网。
- **旧变量升级断裂**：仅在读取阶段提供旧变量回退，生成结果统一为 Cinny。
- **浏览器旧缓存**：`config.json` 和入口 HTML 禁止缓存，部署验证包含内容特征。
- **会话看似丢失**：文档明确说明需要重新登录；服务端数据不变并通过 Matrix
  API 与房间登录回归验证。
