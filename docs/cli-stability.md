# CLI stability

dryrun groups its command surface into stability tiers. Stable commands keep their flags and output shape across minor releases. Experimental commands may change while the design settles, so pin a version if you script against them.

## Experimental: OCI remotes

OCI registry storage shipped recently. The flags and the on-registry layout may change before they are marked stable.

| Surface | Purpose |
|---------|---------|
| `dryrun remote add\|list\|rm` | Manage `[[remote]]` entries in dryrun.toml. |
| `dryrun snapshot push --oci\|--remote` | Push snapshots to an OCI registry. |
| `dryrun snapshot pull --oci\|--remote` | Pull snapshots from an OCI registry. |
| `dryrun snapshot take --push [--remote]` | Capture and push in one step. |

`--oci <ref>` targets a registry base path directly. `--remote <name>` resolves the base and credentials from a configured `[[remote]]`. The `--all` flag on push and pull syncs every key in the source rather than the resolved profile's.

The artifact format carries its version in the media type (`application/vnd.dryrun.bundle.v1+zstd`). A future format would use a new version rather than reinterpreting v1, so older snapshots stay readable.

See [dryrun-toml.md](dryrun-toml.md) for the `[[remote]]` block and the per-profile `remote` and `stream` keys.
