# Worker Discovery and Import Contract

## Search

Call `find_worker`:

```json
{"query":"production Python coder"}
```

Return the candidates to the admin with display name, description, runtime,
version, and package URI. Keep the complete discovery receipt for the selected
candidate.

## Import a Search Result

After the admin chooses a candidate and local Worker name, call
`import_worker`:

```json
{
  "discovery": "<the complete typed find_worker receipt>",
  "candidate_name": "remote-coder",
  "worker_name": "alice"
}
```

AgentScope confirmation binds the full receipt and local name. A changed or
fabricated receipt is rejected.

## Import a Direct URI

Do not call `find_worker`. Restate the exact URI and proposed name, then call
`import_worker`:

```json
{
  "package_uri": "nacos://registry.example/public/remote-coder/1.4.0",
  "worker_name": "alice"
}
```

The configured registry must own the URI. The workflow reads the AgentSpec,
binds its digest, and the Controller verifies that digest after download.

## Result Handling

A success receipt includes the Worker, package URI, digest, room, and
Controller phase. A failure is terminal for that import attempt. Show its
redacted reason; do not call `create_worker` unless the admin separately asks
to abandon the Nacos path.
