# Worker-to-Worker Coordination

The Manager does not edit runtime files to enable broad peer mentions.
Worker collaboration is represented by Controller Team membership and
Manager-owned task handoffs.

1. Use `get_worker` to confirm each participant and its current Team.
2. Use the Team management tools when membership or leadership must change.
3. Route a handoff through the task workflow so ownership, status, and
   completion evidence remain auditable.

If an administrator requests unrestricted peer-triggering, explain the loop
risk: acknowledgment messages can recursively trigger more responses.
Require an explicit bounded policy such as “mentions only for blocking
handoffs,” then implement it through a future typed policy resource rather
than modifying a Worker's runtime configuration ad hoc.

`update_worker` changes supported Worker desired-state fields; it is not a
back door for untyped mention rules.
