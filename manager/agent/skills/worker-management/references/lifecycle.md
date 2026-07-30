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

## Delete completion boundary

After deleting a Worker, verify once with `list_workers` or `get_worker`. If the
target Worker is absent, deletion is complete. Do not probe stale Matrix rooms:
they are client history and can be hidden or left in Cinny.

If verification is inconclusive, retry the same diagnostic at most once. After
two empty, identical, or malformed results, stop running tools and report the
confirmed state instead of entering a diagnostic loop.

## Update or replace

Use `update_worker` for typed desired-state changes. Runtime, image, or
package changes can replace the managed container while preserving the
Controller identity and durable external data. Confirm interruption is
acceptable first.

## Reset

Use `reset_worker` when the same Worker identity must be recreated without
changing its desired model, runtime, image, identity, soul, skills, package,
or exposed ports. The workflow records that complete desired state before
deletion, recreates from the saved copy, verifies Controller readiness, and
refreshes Matrix topology. A retry resumes the saved reset operation rather
than issuing a second independent delete.

## Delete

Use `delete_worker` only when permanent removal is intended. State the exact
Worker name and wait for the confirmation gate. The receipt is successful
only after Controller absence is proven.
That receipt is terminal: deletion has no sleep prerequisite or post-delete
sleep step, and the same deletion must not be submitted for another approval.

If a mutation is interrupted, let heartbeat resume its journaled operation.
Do not issue an unrelated create request to force recovery.
