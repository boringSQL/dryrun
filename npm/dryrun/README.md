# dryrun

PostgreSQL schema intelligence from a committed snapshot. No connection, no
credentials. Capture the schema once, and your AI assistant, IDE, and CI get
full schema awareness — tables, indexes, constraints, statistics — without a
connection string on any developer machine.

This npm package is a thin launcher. The real binary is written in Go and
ships as a platform-specific dependency that npm selects automatically.

## Use as an MCP server

No install needed. Add to your MCP client config:

```json
{
  "mcpServers": {
    "dryrun": {
      "command": "npx",
      "args": ["-y", "@boringsql/dryrun", "mcp-serve"]
    }
  }
}
```

`npx` fetches the package on first run and caches it; subsequent launches are
local. Pin a version for reproducibility: `"@boringsql/dryrun@0.15.0"`.

Or let dryrun write the config. `setup` detects Claude Code, Cursor, Codex,
and Zed, and adds a directive to `AGENTS.md`/`CLAUDE.md` so the agent checks
the schema before writing SQL:

```sh
npx @boringsql/dryrun setup
```

The server reads the newest snapshot from `.dryrun/history.db` in the current
project. Without one, its tools answer that no schema is loaded: capture with
`dryrun init --db "$DATABASE_URL"`, or pull a snapshot a teammate pushed with
`dryrun snapshot pull`. It is picked up on the next tool call, no restart needed.

## CLI

```sh
npx @boringsql/dryrun --help
```

Or install globally: `npm i -g @boringsql/dryrun` then `dryrun --help`.

## Supported platforms

`darwin-arm64`, `darwin-x64`, `linux-x64`, `linux-arm64`, `win32-x64`. On other
platforms, grab a prebuilt binary from the
[GitHub Releases](https://github.com/boringSQL/dryrun/releases) or install via
Homebrew (`brew install boringsql/boringsql/dryrun`).

## More

- [GitHub README](https://github.com/boringSQL/dryrun#readme) — quickstart,
  team snapshot sharing, OCI registry push/pull
- [Tutorial](https://github.com/boringSQL/dryrun/blob/master/TUTORIAL.md) —
  offline, online, and multi-node workflows with the full tool reference
- [boringsql.com/products/dryrun](https://boringsql.com/products/dryrun/) —
  project page
