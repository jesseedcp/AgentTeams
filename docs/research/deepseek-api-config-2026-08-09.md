# DeepSeek 官方 API 配置核验（2026-08-09）

## 结论先行

截至 2026-08-09，DeepSeek 官方托管 API 的 OpenAI 兼容 Base URL 是 `https://api.deepseek.com`，鉴权使用 HTTP Bearer：`Authorization: Bearer <API_KEY>`。官方当前列出的可用 API model ID 只有 `deepseek-v4-flash` 和 `deepseek-v4-pro`。

“DeepSeek V4 Flash”不是第三方杜撰的模型名。DeepSeek 已于 2026-04-24 正式发布 **DeepSeek-V4-Flash**，其第一方托管 API 的精确 model ID 是 **`deepseek-v4-flash`**。配置的 `model` 字段应使用这个小写、连字符形式，而不是产品展示名、仓库名或带 provider 前缀的选择器名。

本笔记只使用 DeepSeek 官方文档与官方 API Reference。没有调用任何需要鉴权的接口，也没有读取、记录或复述任何实际 API key。

## 1. 第一方 API 配置

| 配置项 | 截至 2026-08-09 的官方值 | 官方依据 |
| --- | --- | --- |
| OpenAI 兼容 Base URL | `https://api.deepseek.com` | [Your First API Call](https://api-docs.deepseek.com/)；[Models & Pricing](https://api-docs.deepseek.com/quick_start/pricing/) |
| Chat Completions 路径 | `POST /chat/completions`，完整 URL 为 `https://api.deepseek.com/chat/completions` | [Your First API Call](https://api-docs.deepseek.com/) |
| 模型列表路径 | `GET /models`，完整 URL 为 `https://api.deepseek.com/models` | [Lists Models](https://api-docs.deepseek.com/api/list-models/) |
| 鉴权方案 | HTTP Bearer | [DeepSeek API Reference — Authentication](https://api-docs.deepseek.com/api/deepseek-api/) |
| 鉴权头 | `Authorization: Bearer <API_KEY>` | [Your First API Call](https://api-docs.deepseek.com/) |
| 请求内容类型 | `Content-Type: application/json` | [Your First API Call](https://api-docs.deepseek.com/) |

当前通用 Quick Start 和 Models & Pricing 都把 OpenAI 格式 Base URL 写成不带 `/v1` 的 `https://api.deepseek.com`。DeepSeek 的官方 Oh My Pi 接入页进一步明确说明该配置不要追加 `/v1`。因此，对于可设置 `base_url` 的 OpenAI 兼容 SDK 或通用客户端，应优先填写官方根地址；不要仅因为 OpenAI 自身常见路径带 `/v1` 就自行改写。若某个特定客户端要求填写“完整请求 URL”而不是 Base URL，则应以该客户端对应的官方接入说明为准。[Using DeepSeek with Oh My Pi](https://api-docs.deepseek.com/quick_start/agent_integrations/oh_my_pi/)

## 2. OpenAI 兼容调用方式

DeepSeek 官方说明其 API 格式兼容 OpenAI，并直接给出了使用 OpenAI Python/Node SDK 和 Chat Completions 的示例。下面是等价的最小 Python 形式；只从环境变量读取凭据，占位符不代表任何实际密钥：

```python
import os
from openai import OpenAI

client = OpenAI(
    api_key=os.environ["DEEPSEEK_API_KEY"],
    base_url="https://api.deepseek.com",
)

response = client.chat.completions.create(
    model="deepseek-v4-flash",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Hello!"},
    ],
    stream=False,
)
```

对应的 HTTP 形态是：

```text
POST https://api.deepseek.com/chat/completions
Content-Type: application/json
Authorization: Bearer <API_KEY>

{
  "model": "deepseek-v4-flash",
  "messages": [
    {"role": "user", "content": "Hello!"}
  ],
  "stream": false
}
```

“OpenAI 兼容”不等于支持 OpenAI API 的每一个新字段。DeepSeek 当前官方集成说明列出的重要差异包括：系统提示使用 `system` 角色而不是 `developer`；输出上限字段使用 `max_tokens` 而不是 `max_completion_tokens`。V4 的思考模式还使用 DeepSeek 扩展字段 `thinking` 和 `reasoning_effort`；在 OpenAI Python SDK 中，官方示例把 `thinking` 放入 `extra_body`。这些差异不会改变上面的最小非流式调用，但在接入 Agent、工具调用或思考模式时需要单独处理。[Your First API Call](https://api-docs.deepseek.com/)；[Thinking Mode](https://api-docs.deepseek.com/guides/thinking_mode/)；[Using DeepSeek with Oh My Pi](https://api-docs.deepseek.com/quick_start/agent_integrations/oh_my_pi/)

## 3. 当前官方 model ID

DeepSeek 当前的 Models & Pricing 页面和 `GET /models` API Reference 示例一致，只列出以下两个可用 ID：

| 应填入 `model` 的精确值 | 官方显示/版本 | 状态与说明 |
| --- | --- | --- |
| `deepseek-v4-flash` | 产品名 `DeepSeek-V4-Flash`；当前服务版本 `DeepSeek-V4-Flash-0731` | 当前可用。官方说明 0731 更新后调用方式不变，仍使用稳定 ID `deepseek-v4-flash`。 |
| `deepseek-v4-pro` | 产品名/服务版本 `DeepSeek-V4-Pro` | 当前可用。 |

来源：[Lists Models](https://api-docs.deepseek.com/api/list-models/)；[Models & Pricing](https://api-docs.deepseek.com/quick_start/pricing/)；[Your First API Call](https://api-docs.deepseek.com/)

旧别名 `deepseek-chat` 和 `deepseek-reasoner` 不应再用于 2026-08-09 的新配置。DeepSeek 官方发布公告说明，这两个旧 API 模型名已在 2026-07-24 15:59 UTC 后完全退役并不可访问；退役前它们曾分别路由到 `deepseek-v4-flash` 的非思考和思考模式。当前官方 `/models` 示例也已不再列出它们。[DeepSeek V4 Preview Release](https://api-docs.deepseek.com/news/news260424/)；[Change Log](https://api-docs.deepseek.com/updates/)；[Lists Models](https://api-docs.deepseek.com/api/list-models/)

若需要在部署时核对实时目录，官方定义的权威入口是带 Bearer 鉴权的 `GET https://api.deepseek.com/models`。本次研究没有执行该鉴权请求；上述列表来自 2026-08-09 查阅到的官方 Reference 示例和官方 Models & Pricing 页面。

## 4. “DeepSeek V4 Flash”到底是不是官方名称

是，而且有三条相互独立的官方证据：

1. DeepSeek 的 2026-04-24 官方发布页明确宣布 **DeepSeek-V4-Flash**，并说明 API 当天可用。
2. 同一发布页明确要求 API 的 `model` 改为 `deepseek-v4-flash`（或 Pro 的 `deepseek-v4-pro`）。
3. 当前官方 `GET /models` Reference 示例返回 `deepseek-v4-flash`，`owned_by` 为 `deepseek`。

来源：[DeepSeek V4 Preview Release](https://api-docs.deepseek.com/news/news260424/)；[Lists Models](https://api-docs.deepseek.com/api/list-models/)

名称需要按用途区分：

| 名称形态 | 它是什么 | 能否直接填入 DeepSeek 第一方 API 的 `model` |
| --- | --- | --- |
| `DeepSeek V4 Flash` | 官方集成页使用的人类可读展示名 | 不建议；它是显示标签，不是 Reference 给出的精确 ID |
| `DeepSeek-V4-Flash` | 官方产品/发布名称 | 不应据此猜测 API 值；使用下方精确 ID |
| `deepseek-v4-flash` | DeepSeek 第一方托管 API model ID | **可以，且应使用这个值** |
| `DeepSeek-V4-Flash-0731` | 当前后端模型版本标签 | **不要当作 API ID**；官方明确说升级后仍用 `deepseek-v4-flash` |
| `deepseek/deepseek-v4-flash` | 某些客户端使用的 `provider/model` 选择器；DeepSeek 官方 Oh My Pi 指南中的 CLI 就采用这种外层命名 | **不能直接照搬到 `api.deepseek.com` 的 `model` 字段**；该指南内部模型 ID 仍是 `deepseek-v4-flash` |
| [`deepseek-ai/DeepSeek-V4-Flash`](https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash) | DeepSeek 官方开放权重仓库/组织命名空间形式 | 属于模型制品仓库标识，不是第一方托管 API ID |
| [`DeepSeek-V4-Flash-Base`](https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash) | 官方开放权重中的基础模型制品 | 不是当前托管 API ID |
| [`DeepSeek-V4-Flash-Max`](https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash) | 官方模型卡用于描述 Flash 的最大推理强度模式 | 不是独立 API ID；托管 API 仍用 `deepseek-v4-flash`，并通过思考/effort 参数选模式 |

这也给出了判断第三方平台展示名的方法：只要名称包含 provider 前缀、仓库组织名、额外版本后缀，或只是带空格的 UI 标签，就不能自动假定它是 `api.deepseek.com` 接受的 model ID。第三方平台可以为同一模型定义自己的路由别名；该别名只对其自身 endpoint 有效。直接调用 DeepSeek 官方 endpoint 时，应以官方 `GET /models` 返回的 `id` 为准。DeepSeek 官方接入文档也明确提醒，非官方 provider 的 OpenAI 兼容行为可能不同，并建议优先使用官方 `api.deepseek.com` endpoint。[Using DeepSeek with Oh My Pi](https://api-docs.deepseek.com/quick_start/agent_integrations/oh_my_pi/)

## 5. 可直接交给实现者的配置结论

```text
provider: DeepSeek official
base_url: https://api.deepseek.com
auth: Authorization: Bearer <API_KEY>
api_style: OpenAI Chat Completions
model_fast: deepseek-v4-flash
model_strong: deepseek-v4-pro
do_not_use_as_current_ids: deepseek-chat, deepseek-reasoner
```

如果配置界面同时有“显示名称”和“model ID”两个字段，可以把显示名称写成 `DeepSeek V4 Flash`，但 model ID 必须写成 `deepseek-v4-flash`。如果配置界面只有一个模型字段，则直接写精确 ID。

## 官方来源索引

- [DeepSeek API Docs — Your First API Call](https://api-docs.deepseek.com/)
- [DeepSeek API Docs — Models & Pricing](https://api-docs.deepseek.com/quick_start/pricing/)
- [DeepSeek API Reference — Lists Models](https://api-docs.deepseek.com/api/list-models/)
- [DeepSeek API Reference — Authentication](https://api-docs.deepseek.com/api/deepseek-api/)
- [DeepSeek V4 Preview Release（2026-04-24）](https://api-docs.deepseek.com/news/news260424/)
- [DeepSeek API Change Log](https://api-docs.deepseek.com/updates/)
- [DeepSeek API Docs — Thinking Mode](https://api-docs.deepseek.com/guides/thinking_mode/)
- [DeepSeek API Docs — Using DeepSeek with Oh My Pi](https://api-docs.deepseek.com/quick_start/agent_integrations/oh_my_pi/)
- [DeepSeek 官方开放权重仓库 — DeepSeek-V4-Flash](https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash)
