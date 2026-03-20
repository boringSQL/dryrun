# dryRun PostgreSQL MCP Server

## Lint Configuration

Configure linting rules in `dry_run.toml` under the `[conventions]` section.

### `min_severity`

Controls the minimum severity level reported. Rules with severity below this threshold are excluded from the report. Default: `"warning"`.

- `"info"` — show all violations (Info-level rules like `naming/fk_pattern`, `naming/index_pattern`, `constraints/unnamed` are included)
- `"warning"` — hide Info-level violations (default)
- `"error"` — only show errors

### `disabled_rules`

Disables specific rules by name regardless of severity.

### Example

```toml
[conventions]
min_severity = "info"  # default: "warning". Options: "info", "warning", "error"

[conventions.disabled_rules]
rules = ["naming/table_style", "constraints/unnamed"]
```


