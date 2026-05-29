# dryrun

PostgreSQL schema/query advisor with a built-in MCP server. This npm package is
a thin launcher — the real binary is written in Go and ships as a
platform-specific dependency that npm selects automatically.

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
local. Pin a version for reproducibility: `"@boringsql/dryrun@0.8.2"`.

## CLI

```sh
npx @boringsql/dryrun --help
```

Or install globally: `npm i -g @boringsql/dryrun` then `dryrun --help`.

## Supported platforms

`darwin-arm64`, `linux-x64`, `linux-arm64`. On other platforms, grab a prebuilt
binary from the [GitHub Releases](https://github.com/boringSQL/dryrun/releases).
