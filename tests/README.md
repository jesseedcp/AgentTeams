# AgentTeams Integration Tests

Automated integration and Kubernetes behavioral acceptance suites for the
AgentScope Manager fork.

> 初学者说明：这里的根目录 Shell 用例多数是黑盒端到端测试，会把系统当成真实用户正在使用的产品；
> `manager-agentscope/tests` 和 Controller 的 Go 测试则更偏向快速、隔离的单元/组件测试。
> 端到端测试中的等待与重试用于观察 Matrix、Controller 等异步组件最终收敛，超时就是失败信号，
> 不能用无限等待掩盖。缺少真实 LLM/GitHub Secret 的用例会标记跳过，跳过不等于已经验收通过。

## Architecture

Tests simulate human interaction by calling the Matrix API directly, then verify system responses and side effects:

```
Test Script                     AgentTeams System
    |                               |
    ├── Matrix API: send message ──>| Manager Agent processes
    |                               │ (creates Worker, assigns task, etc.)
    ├── poll Matrix API for reply <─|
    ├── verify reply content        |
    ├── verify Higress Console ────>| (Consumer created? Route updated?)
    ├── verify MinIO files ────────>| (SOUL.md written? task/spec.md?)
    └── PASS / FAIL                 |
```

## Test Cases

| Test | POC Case | Description |
|------|----------|-------------|
| test-01 | Case 1 | Manager boot, all services healthy, IM login |
| test-02 | Case 2 | Create Worker Alice via Matrix conversation |
| test-03 | Case 3 | Assign task, Worker completes |
| test-04 | Case 4 | Human intervenes with supplementary instructions |
| test-05 | Case 5 | Heartbeat triggers Manager inquiry |
| test-06 | Case 6 | Create Bob, collaborative task |
| test-07 | Case 7 | Credential smooth rotation (TODO) |
| test-08 | Case 8 | GitHub operations via MCP Server |
| test-09 | Case 9 | Multi-Worker GitHub collaboration |
| test-10 | Case 10 | MCP permission dynamic revoke/restore |
| test-11 | Feature | Multi-round GitHub PR collaboration |
| test-28 | Parity | Fork release, 18 typed skills, upstream baseline, image/runtime contract |

The Python Kubernetes suite additionally verifies writable Worker/Team/Project
administration, confirmation gates, Worker console rollouts, Matrix session
commands, Cinny routing, and Manager SQLite persistence.

## Running Tests

### Via Makefile (Recommended)

```bash
# Full test flow (auto-creates and cleans up test container)
AGENTTEAMS_LLM_API_KEY=sk-xxx make test

# Skip image rebuild
make test SKIP_BUILD=1

# Run specific tests
make test TEST_FILTER="01 02"

# Test an existing Manager installation
make test SKIP_INSTALL=1
```

### Direct Script Execution

```bash
# Build + run all tests
./tests/run-all-tests.sh

# Use existing images
./tests/run-all-tests.sh --skip-build

# Run specific tests only
./tests/run-all-tests.sh --test-filter "01 02 03"

# Run against an already-installed Manager
./tests/run-all-tests.sh --use-existing

# Use a custom container name
./tests/run-all-tests.sh --container my-test-container
```

### Kubernetes behavioral acceptance

The Kubernetes tests are opt-in because they create short-lived Worker, Team,
Project, Matrix, and confirmation state in the selected namespace.

```bash
# Structural fallback only; safe in ordinary CI
python -m pytest manager-agentscope/tests/e2e -q

# Live Kind/Kubernetes behavior, preserving the existing namespace data
AGENTTEAMS_E2E_K8S=1 \
AGENTTEAMS_E2E_NAMESPACE=agentteams-k8s-b35deb9 \
AGENTTEAMS_E2E_GATEWAY_URL=http://127.0.0.1:18388 \
python -m pytest manager-agentscope/tests/e2e -q

# Also restart the Manager pod and prove the SQLite sentinel survives
AGENTTEAMS_E2E_K8S=1 AGENTTEAMS_E2E_RESTART=1 \
python -m pytest manager-agentscope/tests/e2e -q

# Also run one real LLM-generated, Matrix-confirmed create_worker tool call
AGENTTEAMS_E2E_K8S=1 AGENTTEAMS_E2E_LLM=1 \
python -m pytest \
  manager-agentscope/tests/e2e/test_k8s_matrix_commands.py -q
```

`AGENTTEAMS_E2E_LLM=1` requires the deployed Manager's configured model to be
reachable. The test does not read or print the LLM key. It cleans up its
temporary Worker even when the assertion fails.

## Required Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `AGENTTEAMS_LLM_API_KEY` | Yes | LLM API key for Agent behavior |
| `AGENTTEAMS_GITHUB_TOKEN` | No | GitHub PAT for tests 08-11 |
| `AGENTTEAMS_E2E_K8S` | No | Set to `1` to mutate and verify a live Kubernetes namespace |
| `AGENTTEAMS_E2E_RESTART` | No | Set to `1` with the K8s gate to restart Manager and verify PVC persistence |
| `AGENTTEAMS_E2E_LLM` | No | Set to `1` with the K8s gate for a real confirmed Matrix tool call |

## Helper Libraries

- `lib/test-helpers.sh`: Assertions, lifecycle, logging, Docker helpers
- `lib/matrix-client.sh`: Matrix API wrapper (register, login, send/read messages)
- `lib/higress-client.sh`: Higress Console API wrapper (consumers, routes, MCP)
- `lib/minio-client.sh`: MinIO verification (file existence, content, listing)
