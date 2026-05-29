#!/usr/bin/env node
"use strict";

// Launcher for the dryrun Go binary. The actual binary ships inside a
// platform-specific optionalDependency (@boringsql/dryrun-<os>-<cpu>); npm
// installs only the one matching the host. We resolve it and exec, forwarding
// argv + stdio verbatim so MCP stdio transport passes straight through.

const { spawnSync } = require("node:child_process");

// node arch/platform -> our published platform packages
const SUPPORTED = {
  "darwin arm64": "@boringsql/dryrun-darwin-arm64",
  "darwin x64": "@boringsql/dryrun-darwin-x64",
  "linux x64": "@boringsql/dryrun-linux-x64",
  "linux arm64": "@boringsql/dryrun-linux-arm64",
  "win32 x64": "@boringsql/dryrun-win32-x64",
};

function resolveBinary() {
  const key = `${process.platform} ${process.arch}`;
  const pkg = SUPPORTED[key];
  if (!pkg) {
    fail(
      `unsupported platform ${key}.\n` +
        `Supported: ${Object.keys(SUPPORTED).join(", ")}.\n` +
        `Install a prebuilt binary from https://github.com/boringSQL/dryrun/releases instead.`
    );
  }
  try {
    const binName = process.platform === "win32" ? "dryrun.exe" : "dryrun";
    return require.resolve(`${pkg}/bin/${binName}`);
  } catch (_e) {
    fail(
      `the platform package ${pkg} is not installed.\n` +
        `This usually means npm skipped optional dependencies. Reinstall without\n` +
        `--no-optional (e.g. \`npm i @boringsql/dryrun\`), or grab a binary from\n` +
        `https://github.com/boringSQL/dryrun/releases.`
    );
  }
}

function fail(msg) {
  process.stderr.write(`dryrun: ${msg}\n`);
  process.exit(1);
}

const result = spawnSync(resolveBinary(), process.argv.slice(2), {
  stdio: "inherit",
});

if (result.error) fail(result.error.message);
// propagate signal-kills as the conventional 128+signal exit code
if (result.signal) process.exit(1);
process.exit(result.status === null ? 1 : result.status);
