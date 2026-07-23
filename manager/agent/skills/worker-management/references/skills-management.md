# Worker Skills and Configuration

Skills are part of the Worker's Controller desired state.

## Inspect

Call `get_worker` and read the current `skills` field before changing it.

## Replace the desired skill set

Call `update_worker` with `name` and the complete desired `skills` array:

```json
{
  "name": "researcher",
  "skills": ["web-research", "source-verification"]
}
```

An empty array explicitly removes all optional skills. Omitting `skills`
leaves the current set unchanged.

The same typed tool can change `model`, `runtime`, `image`, `identity`,
`soul`, `package_uri`, and `expose`. Include only fields intentionally being
changed, but when a field is an array, pass its complete desired value.

For skills supplied by a Nacos Worker package, prefer a confirmed package
import or package-version update so the package digest and provenance stay
auditable. Do not copy definitions into a Manager-local directory.

After the receipt, call `get_worker` when the administrator needs to see the
reconciled configuration. Do not tell the Worker to synchronize local files;
Controller reconciliation owns distribution.
