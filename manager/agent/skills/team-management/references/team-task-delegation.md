# Delegate Work Through a Team Leader

Use this only for a finite task that benefits from a Team.

1. Call `get_team` and require an active Team with a ready Leader Room.
2. Create the task through the task-management workflow.
3. Call `delegate_team_task` with the task and Team name.
4. Send instructions only to the Team Leader. The Leader owns breakdown,
   Worker assignment, aggregation, and escalation.
5. Record Team attribution on the task and keep all updates in its Matrix
   thread.

The Manager must not join the Team-private room, message Team Workers directly,
or bypass the Leader when a Worker appears idle. If the Leader is unavailable,
report the Team as unavailable and ask whether to repair the Team or choose a
standalone Worker.
