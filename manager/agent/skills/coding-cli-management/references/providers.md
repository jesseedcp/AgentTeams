# Provider configuration

Coding CLI delegation is disabled by default. Operators enable it with the
Manager deployment values, choose an exact provider allowlist, and mount the
vendor executables read-only below the configured trusted directory.

| Provider | Executable | Supported non-interactive policy |
| --- | --- | --- |
| Claude Code | `claude` | print mode, workspace edits, Bash/web tools denied |
| Gemini CLI | `gemini` | headless stdin, `auto_edit`, other approvals fail closed |
| Qoder CLI | `qodercli` | print mode, `accept_edits`, file tools only |

The runtime never searches arbitrary PATH entries and never interpolates the
prompt into argv. It launches an absolute executable path without a shell and
passes the prompt through stdin.

Only a provider-specific credential allowlist is inherited. Examples include
`ANTHROPIC_API_KEY`, `GEMINI_API_KEY`, and
`QODER_PERSONAL_ACCESS_TOKEN`. AgentTeams Matrix, storage, gateway, and
Controller credentials are excluded. Prefer Kubernetes Secret references in
the Manager Pod template or a read-only vendor login mount; never place
credentials in Helm values, a Manager resource, a prompt, or a task artifact.

`coding_cli_status` distinguishes:

- configured: selected in desired deployment state;
- available: the executable exists inside the trusted read-only mount;
- enabled: the workflow feature flag permits execution.

Configured but unavailable is a normal state when the base Manager image is
used without an operator-provided vendor CLI layer.
