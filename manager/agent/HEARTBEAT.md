# Deterministic Heartbeat Contract

The Python Heartbeat scheduler runs without a model call. This document tells
the conversational Agent how to interpret and summarize its typed report; it
does not instruct the Agent to perform reconciliation itself.

## Scheduler order

Each heartbeat performs the following bounded steps:

1. Recover incomplete resource operations and compare Controller facts.
2. Refresh Controller and Matrix topology.
3. Continue Worker readiness, greeting, and deletion cleanup.
4. Recover task, project, artifact, and Git operations.
5. Reclaim only provably expired processing leases.
6. Dispatch due recurring-task occurrences exactly once.
7. Reconcile finite-task completion artifacts.
8. Activate valid model and MCP runtime generations between turns.
9. Reconcile Higress MCP and published-service desired state.
10. Deliver unsent terminal notifications exactly once.
11. Create a verified SQLite snapshot and advance the immutable remote journal
    watermark when the configured threshold is reached.

The scheduler uses stable operation IDs, Matrix transaction IDs, object
checksums, and compare-and-swap state transitions. It never assumes a timeout
means an effect did not happen.

## Conversational summary

When the room policy requests a heartbeat summary, call the read-only
`inspect_heartbeat_status` tool. Summarize:

- operations reconciled;
- due occurrences dispatched;
- terminal successes and failures;
- resources requiring admin attention;
- snapshot and runtime generation status.

Do not re-run mutations from the summary. Do not include credentials, raw
authorization failures, or internal payloads. If there is nothing actionable,
reply with a short healthy status.

## Escalation

Escalate to the Admin DM when deterministic recovery reports corruption,
exhausted bounded retries, a policy conflict, or an external fact that cannot
be reconciled safely. Include the operation ID, affected resource, redacted
error class, and a concrete recovery action.
