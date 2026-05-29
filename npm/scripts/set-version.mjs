// Stamp one version across the main package, its platform packages, and the
// optionalDependencies pins (which must match exactly). Run in CI from the tag:
//   node npm/scripts/set-version.mjs 0.8.2
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const version = process.argv[2];
if (!version) {
  console.error("usage: set-version.mjs <version>");
  process.exit(1);
}

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const platforms = ["darwin-arm64", "darwin-x64", "linux-x64", "linux-arm64", "win32-x64"];

function patch(path, fn) {
  const pkg = JSON.parse(readFileSync(path, "utf8"));
  fn(pkg);
  writeFileSync(path, JSON.stringify(pkg, null, 2) + "\n");
}

for (const p of platforms) {
  patch(join(root, "packages", p, "package.json"), (pkg) => {
    pkg.version = version;
  });
}

patch(join(root, "dryrun", "package.json"), (pkg) => {
  pkg.version = version;
  for (const p of platforms) {
    pkg.optionalDependencies[`@boringsql/dryrun-${p}`] = version;
  }
});

console.log(`set npm packages to ${version}`);
