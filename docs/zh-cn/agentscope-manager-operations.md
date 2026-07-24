# AgentScope Manager 运维手册

本手册只适用于生产 `agentscope` Manager。OpenClaw、CoPaw、Hermes、
QwenPaw 和 OpenHuman 都是 Worker 运行时，拥有各自独立的运行状态。

## 服务检查

Manager 在容器端口 `18799` 提供运维 HTTP；嵌入式安装默认只映射到宿主机
回环端口 `18888`。

```bash
curl -fsS http://127.0.0.1:18888/healthz
curl -fsS http://127.0.0.1:18888/readyz
curl -fsS http://127.0.0.1:18888/metrics
docker logs --tail 200 agentteams-manager
```

`/healthz` 只表示事件循环仍然存活。只有 `/readyz` 中数据库、恢复、运行时
配置、Matrix 传输和心跳均已就绪，Manager 才算可用。

重点指标包括：

- `agentteams_manager_runtime_revision`；
- `agentteams_manager_runtime_reloads_total`；
- `agentteams_manager_matrix_events_total`；
- `agentteams_manager_tool_calls_total`；
- `agentteams_manager_recovery_reconciled_total`；
- `agentteams_manager_recovery_errors_total`。

收集支持包时使用带脱敏的导出器：

```bash
python scripts/export-debug-log.py \
  --range 1h \
  --container agentteams-manager
```

导出器会在 `debug-log/` 下创建带时间戳的调试包。分享前仍要人工检查输出。
自动脱敏是一层保护，不能代替对调试包的敏感数据管理。

## 状态与备份边界

活跃数据库位于：

```text
<AGENTTEAMS_WORKSPACE_DIR>/state/manager.db
```

SQLite WAL 是本地事务权威。MinIO/S3 在 `manager/journal/` 保存不可变操作
事件，在 `manager/snapshots/` 保存带校验的数据库快照。Matrix 是消息和
房间事实的权威；Controller 是 Manager、Worker、Team、Human 期望状态的
权威。

备份时必须同时备份 Manager 工作空间和对象存储桶。只备份工作空间会缺少
远端操作 journal；只备份对象存储可能只能恢复到最近一次已发布快照，而
不是最近一次本地对话状态。

## 恢复损坏的本地 SQLite

不要删除对象存储 journal，也不要关闭快照校验。

1. 停止 Manager，并先保存诊断信息：

   ```bash
   docker logs agentteams-manager > manager-before-recovery.log 2>&1
   docker stop agentteams-manager
   ```

2. 从受保护的 `agentteams-manager.env` 中解析准确的工作空间路径。移动文件
   前，确认该路径确实属于本次 Manager 安装。

3. 在 `state/` 同级创建带时间戳的隔离目录，只移动以下实际存在的文件：

   ```text
   state/manager.db
   state/manager.db-wal
   state/manager.db-shm
   ```

   恢复验证完成前保留隔离文件。不要删除整个工作空间、Matrix E2EE 目录、
   对象存储桶或 Controller 数据。

4. 确认配置桶中存在 `manager/snapshots/latest.json`，并且其 `key` 字段
   指向的快照对象也存在。Manager 使用前会同时校验字节长度和 SHA-256。

5. 使用最初创建它的同一种安装器、Compose 或 Kubernetes 部署重新启动
   Manager。启动过程会创建干净的本地 schema，恢复最新有效快照，重放其后
   的不可变 journal，再根据 Controller、Matrix、MinIO 和 Higress 的事实
   对账未完成副作用。

6. 等待 `/readyz`，然后检查恢复指标和日志：

   ```bash
   curl -fsS http://127.0.0.1:18888/readyz
   curl -fsS http://127.0.0.1:18888/metrics |
     grep 'agentteams_manager_recovery_'
   docker logs --tail 200 agentteams-manager
   ```

如果快照校验失败，应保持 Manager 停止，从已知正常的存储桶备份恢复相关
对象，或先排查存储故障；不要编辑 `latest.json` 来绕过校验。无法自动对账
的操作会暴露为恢复错误或 `needs_attention`，而不是被盲目重复执行。

## 轮换密钥

密钥只能放在本地受保护的环境文件或 Kubernetes Secret 中，不能写入
Controller 生成的 Manager 运行时文档、SQLite payload、skill 文档或聊天
消息。

嵌入式安装：

1. 以仅属主可读权限备份 `agentteams-manager.env`；
2. 替换其中受影响的值；
3. 运行安装器的升级流程，让 Controller 和 Manager 容器使用新环境重建；
4. 等待 Manager 就绪并验证受影响的集成；
5. 新凭据验证成功后，再撤销旧凭据。

Helm 安装：

1. 更新受保护的 values 来源或 External Secret 输入；
2. 使用该来源执行 `helm upgrade --install`；
3. 等待 Controller 和 Manager 完成 rollout；
4. 验证 `/readyz`、调和状态和受影响的集成；
5. 撤销旧凭据。

模型、Matrix、MinIO、Higress、GitHub MCP 和 Controller 凭据都是进程环境
输入，仅更新运行时文档 revision 不会替换它们。`env:NAME` 中只能写大写
环境变量名，绝不能把真实密钥当作引用字符串；AgentScope 会在工具执行时
解析对应环境变量。

## 运行时 revision

模型、MCP、服务和 Manager 身份变更都通过 typed Manager 工具和 Controller
资源提交。Controller 发布更高 revision 的不可变、无密钥运行时文档。当前
AgentScope 回合仍按原配置结束，新 revision 在两个回合之间生效，不需要
替换 Manager 容器。

revision 未生效时：

1. 对比 Controller 报告的 revision 与
   `agentteams_manager_runtime_revision`；
2. 检查 `agentteams_manager_runtime_reloads_total`；
3. 验证 Manager 能否读取配置的 MinIO 运行时文档 key；
4. 检查 Manager 日志中的配置校验错误；
5. 通过 `agt` 或 typed Manager 工具修正期望状态，不要进入容器直接改文件。
