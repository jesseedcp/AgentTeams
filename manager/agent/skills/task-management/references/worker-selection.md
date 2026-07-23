# Worker Selection

When no assignee is named:

1. Inspect Teams first and prefer a matching Team for multi-skill work.
2. Inspect Workers and compare role, skills, runtime phase, and current tasks
   from `list_tasks`.
3. Prefer an idle capable Worker. Do not assign to a missing, sleeping, or
   unready resource.
4. If only busy Workers exist, expose workload and the scheduling tradeoff.
5. Import or create a Worker only through the resource-management tools and
   only when policy allows that material change.

After selection:

- use `delegate_team_task` for a Team and its authoritative Leader Room;
- use `delegate_task` for an individual Worker;
- use `get_task` to verify the durable assignment.

Self-execution is not a hidden fallback. It requires an explicit request or a
task that is itself Manager administration.
