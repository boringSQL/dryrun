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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::*;

    fn tmpdir() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "dryrun-load-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn minimal_snapshot() -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "16.0".into(),
            database: "t".into(),
            timestamp: chrono::Utc::now(),
            content_hash: "abc".into(),
            source: None,
            tables: vec![],
            enums: vec![],
            domains: vec![],
            composites: vec![],
            views: vec![],
            functions: vec![],
            extensions: vec![],
            gucs: vec![],
            node_stats: vec![],
        }
    }

    #[test]
    fn loads_schema_and_merges_sibling_stats() {
        let dir = tmpdir();
        let schema_path = dir.join("schema.json");
        std::fs::write(
            &schema_path,
            serde_json::to_string(&minimal_snapshot()).unwrap(),
        )
        .unwrap();

        let ns_a = NodeStats {
            source: "a".into(),
            timestamp: chrono::Utc::now(),
            is_standby: false,
            table_stats: vec![],
            index_stats: vec![],
            column_stats: vec![],
        };
        let ns_b = NodeStats {
            source: "b".into(),
            timestamp: chrono::Utc::now(),
            is_standby: true,
            table_stats: vec![],
            index_stats: vec![],
            column_stats: vec![],
        };
        std::fs::write(dir.join("a-stats.json"), serde_json::to_string(&ns_a).unwrap()).unwrap();
        std::fs::write(dir.join("b-stats.json"), serde_json::to_string(&ns_b).unwrap()).unwrap();

        let loaded = load_schema_file(&schema_path).unwrap();
        assert_eq!(loaded.node_stats.len(), 2);
        // deterministic order (filename sort)
        assert_eq!(loaded.node_stats[0].source, "a");
        assert_eq!(loaded.node_stats[1].source, "b");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn rejects_legacy_combined_schema() {
        let dir = tmpdir();
        let schema_path = dir.join("schema.json");

        let mut snap = minimal_snapshot();
        snap.node_stats.push(NodeStats {
            source: "legacy".into(),
            timestamp: chrono::Utc::now(),
            is_standby: false,
            table_stats: vec![],
            index_stats: vec![],
            column_stats: vec![],
        });
        std::fs::write(&schema_path, serde_json::to_string(&snap).unwrap()).unwrap();

        let err = load_schema_file(&schema_path).unwrap_err().to_string();
        assert!(err.contains("dump-schema"), "{err}");

        std::fs::remove_dir_all(&dir).ok();
    }
}
