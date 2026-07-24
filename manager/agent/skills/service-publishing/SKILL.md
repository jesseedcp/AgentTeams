---
name: service-publishing
description: Publish or unpublish Worker HTTP ports through Controller reconciliation and observed gateway status.
---

# Service Publishing

Use `publish_service` with action `publish` or `unpublish`. Only the Admin Room
receives this tool, and every call requires confirmation that the resulting
route is public and unauthenticated.

The workflow reads the Worker's current desired ports, computes one complete
replacement set, and updates it through `AgtClient`. Clearing the last port is
an explicit empty replacement. It then polls the Worker and returns only
domains present in Controller `status.exposedPorts`.

Never predict a domain from a naming convention. A successful desired-state
update is not proof that the gateway route exists.

Important:

- ports are unique integers from 1 through 65535;
- publishing unions requested ports with existing desired ports;
- unpublishing removes only requested ports;
- Controller and its gateway provider own route creation/removal;
- custom domains and route authentication are outside the current contract;
- an unsupported cloud provider returns a typed unsupported result with no
  claimed domain.

The service inside the Worker must already listen on the requested container
port.
