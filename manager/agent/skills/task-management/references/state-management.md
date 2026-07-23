# Durable Task State

SQLite is the local scheduler and operation ledger. MinIO stores canonical
human-readable task and project artifacts plus the immutable recovery journal.
Matrix carries assignments and reports; it is not the scheduler.

Read state with `list_tasks` and `get_task`. Mutate it only through
`create_task`, `schedule_task`, `update_task`, `complete_task`, or
`delete_task`.

Every cross-system mutation follows:

1. deterministic operation identity;
2. durable intent;
3. effect-planned journal entry;
4. external effect with a stable idempotency key or conditional version;
5. verified receipt;
6. local transition;
7. terminal journal state.

Timeout means “unknown,” not “did not happen.” Recovery compares SQLite,
MinIO, Matrix, Controller, and process evidence before continuing.
