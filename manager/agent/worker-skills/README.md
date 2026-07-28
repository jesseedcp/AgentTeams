# Worker 按需技能目录

这个目录保存 AgentTeams 随镜像发布、可按需分配给 Worker 的技能。它是
Controller 的只读发布源，不是 AgentScope Manager 的运行时工作区。

## 工作方式

1. 用户或 AgentScope Manager 通过 Worker/Team 资源的 `spec.skills` 声明技能。
2. Controller 校验技能名，并从本目录读取对应内容。
3. Controller 将技能发布到 MinIO 的
   `agents/<worker>/skills/<skill-name>/`。
4. Worker 的同步机制将技能拉取到本地。

Controller 资源是期望状态的唯一来源。Manager 不修改 JSON registry，也不调用
技能分发脚本。Controller 用
`controller/worker-skills/<worker>/state.json` 记录自己发布的技能集合；更新或
清空 `spec.skills` 时，会删除已经取消分配的 Controller-managed 技能。自定义
package 不应与这些技能重名。

## 目录结构

```text
worker-skills/
├── README.md
└── <skill-name>/
    ├── SKILL.md
    └── scripts/        # 可选
```

每个技能必须包含普通文件 `SKILL.md`。推荐使用以下 frontmatter：

```yaml
---
name: <skill-name>
description: <一句话说明>
assign_when: <什么职责的 Worker 需要这个技能>
---
```

技能名只能包含小写字母、数字和内部连字符，最长 128 个字符。符号链接不会被
发布。

## 分配技能

```bash
# 创建 Worker 时分配
agt create worker --name alice --skills github-operations

# 替换已有 Worker 的按需技能列表
agt update worker --name alice --skills git-delegation

# 查看 Controller 中的期望状态
agt get workers alice -o json
```

AgentScope Manager 的 `create_worker`、`update_worker`、`create_team` 和
`update_team` typed tools 使用同一套 Controller API。

远程动态技能使用资源的 `spec.remoteSkills`，不放入本目录。

## 新增目录内技能

1. 新建 `<skill-name>/SKILL.md`，需要时再添加脚本或参考文件。
2. 为 Controller 发布逻辑补充测试。
3. 重建并发布 Controller 镜像；运行中的 Controller 不接受对本目录的动态写入。

Worker 镜像自带的基础技能由各 runtime 模板维护，不需要在 `spec.skills` 中重复
声明。

当前目录内的按需技能：

| Skill | 用途 |
| --- | --- |
| `github-operations` | GitHub 仓库、Issue、PR 等操作 |
| `git-delegation` | 受控的 Git 任务委派 |
| `coding-cli` | 为 Manager 的受限 Coding CLI 准备提示词并复核结果 |
