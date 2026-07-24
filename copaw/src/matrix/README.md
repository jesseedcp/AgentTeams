# AgentTeams CoPaw Worker Matrix Overlay

This directory contains the Matrix channel overlay installed into both the
standard and lite CoPaw Worker environments.

It is Worker-only. The AgentScope Manager has its own Matrix adapter under
`manager-agentscope/src/agentteams_manager/matrix/` and must not import this
overlay.

## Files

### `channel.py` and `__init__.py`

The overlay adds:

- end-to-end encryption through `matrix-nio` and `libolm`;
- bounded group-room history buffering;
- display-name and structured mention handling;
- Markdown-to-HTML rendering;
- direct-room detection;
- typing-indicator renewal for long operations.

### `config.py`

The configuration overlay adds AgentTeams-required CoPaw fields, including
Matrix mention controls, OneBot settings, stable agent ordering, and video
analysis configuration.

## Installation

`copaw/Dockerfile` replaces the corresponding CoPaw modules in both Worker
virtual environments:

```dockerfile
for SITE in \
  /opt/venv/standard/lib/python3.11/site-packages \
  /opt/venv/lite/lib/python3.11/site-packages; do
  rm -rf "$SITE/copaw/app/channels/matrix"
  mkdir -p "$SITE/copaw/app/channels/matrix"
  cp src/matrix/channel.py src/matrix/__init__.py \
    "$SITE/copaw/app/channels/matrix/"
done
```

The Controller remains authoritative for the Worker Matrix identity and
allow-lists. The CoPaw bridge translates the generated Worker
`openclaw.json` into the native CoPaw configuration consumed by this channel.

## Maintenance

When changing the overlay:

1. update or add tests under `copaw/tests/`;
2. run the CoPaw Worker test suite;
3. rebuild `agentteams/copaw-worker`;
4. verify both standard and lite modes.

Do not add a Manager build path here. Once upstream CoPaw contains every
required field and behavior, remove the overlay after verifying equivalent
Worker behavior.
