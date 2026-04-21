use std::path::Path;

use crate::error::{Error, Result};
use crate::schema::types::{NodeStats, SchemaSnapshot};

/// Load a schema.json file, validate it is structural-only, and merge sibling
/// `*-stats.json` files (each containing a single NodeStats) as node_stats.
///
/// Legacy combined files (inline stats or embedded node_stats) are rejected;
/// the caller is pointed at `dryrun dump-schema`.
pub fn load_schema_file(path: &Path) -> Result<SchemaSnapshot> {
    let json = std::fs::read_to_string(path).map_err(|e| {
        Error::Config(format!("failed to read {}: {e}", path.display()))
    })?;
    let mut snapshot: SchemaSnapshot = serde_json::from_str(&json).map_err(|e| {
        Error::Config(format!("invalid schema JSON in {}: {e}", path.display()))
    })?;

    snapshot
        .validate_structural_only()
        .map_err(|msg| Error::Config(format!("{}: {msg}", path.display())))?;

    if let Some(dir) = path.parent() {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return Ok(snapshot),
        };
        let mut discovered: Vec<(String, NodeStats)> = Vec::new();
        for entry in entries.flatten() {
            let p = entry.path();
            let fname = match p.file_name().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            if !fname.ends_with("-stats.json") {
                continue;
            }
            let txt = std::fs::read_to_string(&p).map_err(|e| {
                Error::Config(format!("failed to read {}: {e}", p.display()))
            })?;
            let ns: NodeStats = serde_json::from_str(&txt).map_err(|e| {
                Error::Config(format!("invalid stats JSON in {}: {e}", p.display()))
            })?;
            discovered.push((fname.to_string(), ns));
        }
        discovered.sort_by(|a, b| a.0.cmp(&b.0));
        snapshot.node_stats.extend(discovered.into_iter().map(|(_, ns)| ns));
    }

    Ok(snapshot)
}
