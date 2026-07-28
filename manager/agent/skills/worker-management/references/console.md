# Worker Web Console and Service Exposure

CoPaw and QwenPaw Workers have an optional web console. It is declarative
Worker state, not an ad-hoc container edit:

1. Call `get_worker` and check `runtime` plus `spec.console`.
2. Enable it with `update_worker` using `console_enabled: true`. The default
   container port is 8088; set `console_port` only when a different port is
   required.
3. Disable it with `console_enabled: false`.
4. Call `get_worker` again and report the observed `spec.console` state.

Examples:

```json
{"name":"researcher","console_enabled":true,"console_port":8088}
{"name":"researcher","console_enabled":false}
```

The console switch only starts or stops the service inside the Worker.
Publishing it through Higress is separate: pass the complete desired `expose`
array, such as `{"name":"researcher","expose":[8088]}`. Passing an empty
`expose` array removes all published ports.

OpenClaw and Hermes do not support this console switch. Do not promise a
browser URL from a container port alone; a URL exists only after the approved
deployment routing layer has reconciled the corresponding exposed port.
