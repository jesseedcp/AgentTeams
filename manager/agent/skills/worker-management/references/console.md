# Worker Service Exposure

The new Manager does not maintain a separate runtime-specific console switch.
It represents desired service ports on the Controller Worker resource.

1. Call `get_worker` with `name` and inspect the runtime, current phase, and
   exposed ports.
2. If the runtime supports the requested service, call `update_worker` with
   the Worker `name` and the complete desired `expose` array.
3. Call `get_worker` again and report the reconciled status. An exposed port
   is not proof that a public route or firewall rule exists.

Example input shape:

```json
{"name":"researcher","expose":[8080]}
```

To close all Worker service ports, pass an empty `expose` array. This is an
explicit configuration change, not an omitted field.

Do not promise a browser URL from a port number alone. Publishing a service
outside the Worker is a separate infrastructure operation and must use the
deployment environment's approved routing controls.
