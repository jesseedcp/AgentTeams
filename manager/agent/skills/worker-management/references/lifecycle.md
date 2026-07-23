# Worker Lifecycle

## Inspect

- Call `list_workers` to see Workers allowed by the current Human scope.
- Call `get_worker` with `name` for the authoritative phase, container state,
  runtime, Team placement, and room identity.

## Sleep

Before calling `sleep_worker`, verify that the Worker has no active task,
unfinished handoff, or irreplaceable in-memory work. Then pass:

```json
{"name":"researcher"}
```

A successful receipt proves the Controller reports a stopped, sleeping, or
exited container state. Sleeping preserves the Worker resource and durable
data.

## Wake

Call `wake_worker` with the same name. A successful receipt proves the
Controller reports a running or ready container state. Do not create a
second Worker when an existing one is merely asleep.

## Update or replace

Use `update_worker` for typed desired-state changes. Runtime, image, or
package changes can replace the managed container while preserving the
Controller identity and durable external data. Confirm interruption is
acceptable first.

## Delete

Use `delete_worker` only when permanent removal is intended. State the exact
Worker name and wait for the confirmation gate. The receipt is successful
only after Controller absence is proven.

If a mutation is interrupted, let heartbeat resume its journaled operation.
Do not issue an unrelated create request to force recovery.
