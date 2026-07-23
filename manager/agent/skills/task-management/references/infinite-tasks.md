# Recurring Task Workflow

Use `schedule_task` for work that repeats without a natural terminal result.
Provide exactly five cron fields and an IANA timezone.

The durable status remains `active`. Heartbeat dispatches at most one
occurrence for the current `next_scheduled_at`; a delay beyond 30 minutes is a
warning, not permission to send catch-up bursts.

When the Worker reports `executed`, call `complete_task` or `update_task` with
the recurring execution action. This records `last_executed_at`, calculates
one future schedule, and sends no new Worker message.

Never turn an execution acknowledgement into an immediate trigger. The next
dispatch belongs exclusively to a later deterministic heartbeat.

Use `get_task` to inspect `last_executed_at` and `next_scheduled_at`. Use
`delete_task` to cancel the schedule while retaining its history and objects.
