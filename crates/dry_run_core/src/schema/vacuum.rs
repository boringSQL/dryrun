use serde::{Deserialize, Serialize};

use super::types::{GucSetting, SchemaSnapshot, effective_table_stats};

#[derive(Debug, Clone)]
pub struct AutovacuumDefaults {
    pub enabled: bool,
    pub vacuum_threshold: i64,
    pub vacuum_scale_factor: f64,
    pub analyze_threshold: i64,
    pub analyze_scale_factor: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VacuumHealth {
    pub schema: String,
    pub table: String,
    pub reltuples: f64,
    pub dead_tuples: i64,
    pub vacuum_trigger_at: f64,
    pub vacuum_progress: f64,
    pub has_overrides: bool,
    pub effective_threshold: i64,
    pub effective_scale_factor: f64,
    pub effective_analyze_threshold: i64,
    pub effective_analyze_scale_factor: f64,
    pub analyze_trigger_at: f64,
    pub autovacuum_enabled: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recommendations: Vec<String>,
}

pub fn parse_autovacuum_defaults(gucs: &[GucSetting]) -> AutovacuumDefaults {
    let mut d = AutovacuumDefaults {
        enabled: true,
        vacuum_threshold: 50,
        vacuum_scale_factor: 0.2,
        analyze_threshold: 50,
        analyze_scale_factor: 0.1,
    };

    for g in gucs {
        match g.name.as_str() {
            "autovacuum" => d.enabled = g.setting == "on",
            "autovacuum_vacuum_threshold" => {
                if let Ok(v) = g.setting.parse::<i64>() {
                    d.vacuum_threshold = v;
                }
            }
            "autovacuum_vacuum_scale_factor" => {
                if let Ok(v) = g.setting.parse::<f64>() {
                    d.vacuum_scale_factor = v;
                }
            }
            "autovacuum_analyze_threshold" => {
                if let Ok(v) = g.setting.parse::<i64>() {
                    d.analyze_threshold = v;
                }
            }
            "autovacuum_analyze_scale_factor" => {
                if let Ok(v) = g.setting.parse::<f64>() {
                    d.analyze_scale_factor = v;
                }
            }
            _ => {}
        }
    }
    d
}

fn parse_reloptions(reloptions: &[String]) -> std::collections::HashMap<String, String> {
    reloptions
        .iter()
        .filter_map(|opt| {
            opt.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
        })
        .collect()
}

pub fn analyze_vacuum_health(snap: &SchemaSnapshot) -> Vec<VacuumHealth> {
    let defaults = parse_autovacuum_defaults(&snap.gucs);
    let mut results = Vec::new();

    for table in &snap.tables {
        let stats = match effective_table_stats(table, snap) {
            Some(s) if s.reltuples >= 10_000.0 => s,
            _ => continue,
        };

        let opts = parse_reloptions(&table.reloptions);
        let has_overrides = opts.keys().any(|k| k.starts_with("autovacuum_"));

        let mut threshold = defaults.vacuum_threshold;
        let mut scale_factor = defaults.vacuum_scale_factor;
        let mut analyze_threshold = defaults.analyze_threshold;
        let mut analyze_scale_factor = defaults.analyze_scale_factor;
        let mut av_enabled = defaults.enabled;

        if let Some(v) = opts.get("autovacuum_vacuum_threshold") {
            if let Ok(parsed) = v.parse::<i64>() {
                threshold = parsed;
            }
        }
        if let Some(v) = opts.get("autovacuum_vacuum_scale_factor") {
            if let Ok(parsed) = v.parse::<f64>() {
                scale_factor = parsed;
            }
        }
        if let Some(v) = opts.get("autovacuum_analyze_threshold") {
            if let Ok(parsed) = v.parse::<i64>() {
                analyze_threshold = parsed;
            }
        }
        if let Some(v) = opts.get("autovacuum_analyze_scale_factor") {
            if let Ok(parsed) = v.parse::<f64>() {
                analyze_scale_factor = parsed;
            }
        }
        if let Some(v) = opts.get("autovacuum_enabled") {
            av_enabled = v == "on" || v == "true";
        }

        let trigger_at = threshold as f64 + scale_factor * stats.reltuples;
        let analyze_trigger = analyze_threshold as f64 + analyze_scale_factor * stats.reltuples;
        let progress = if trigger_at > 0.0 {
            stats.dead_tuples as f64 / trigger_at
        } else {
            0.0
        };

        let mut recommendations = Vec::new();

        if !av_enabled {
            recommendations.push(
                "autovacuum is disabled for this table! This won't end good; you've been warned"
                    .into(),
            );
        }

        if stats.reltuples >= 1_000_000.0 && !has_overrides {
            let mut suggested_vac_sf = 100_000.0 / stats.reltuples;
            suggested_vac_sf = (suggested_vac_sf * 1000.0).round() / 1000.0;
            if suggested_vac_sf < 0.001 {
                suggested_vac_sf = 0.001;
            }
            let suggested_az_sf = (suggested_vac_sf / 2.0 * 1000.0).round() / 1000.0;

            // threshold: ~1% of rows, clamped to 500..5000
            let suggested_vac_thresh = ((stats.reltuples * 0.01) as i64).clamp(500, 5000);
            let suggested_az_thresh = (suggested_vac_thresh / 2).max(250);

            recommendations.push(format!(
                "large table ({}k rows) using default autovacuum settings; consider: \
                 autovacuum_vacuum_scale_factor={suggested_vac_sf}, \
                 autovacuum_vacuum_threshold={suggested_vac_thresh}, \
                 autovacuum_analyze_scale_factor={suggested_az_sf}, \
                 autovacuum_analyze_threshold={suggested_az_thresh}",
                stats.reltuples as i64 / 1000
            ));
        }

        if stats.reltuples > 0.0
            && stats.dead_tuples as f64 / stats.reltuples > 0.10
        {
            recommendations.push(format!(
                "high dead tuple ratio: {} dead / {}k live ({:.1}%)",
                stats.dead_tuples,
                stats.reltuples as i64 / 1000,
                stats.dead_tuples as f64 / stats.reltuples * 100.0
            ));
        }

        if trigger_at > 10_000_000.0 {
            recommendations.push(format!(
                "vacuum won't trigger until {}k dead tuples. Threshold is very high",
                trigger_at as i64 / 1000
            ));
        }

        results.push(VacuumHealth {
            schema: table.schema.clone(),
            table: table.name.clone(),
            reltuples: stats.reltuples,
            dead_tuples: stats.dead_tuples,
            vacuum_trigger_at: trigger_at,
            vacuum_progress: progress,
            has_overrides,
            effective_threshold: threshold,
            effective_scale_factor: scale_factor,
            effective_analyze_threshold: analyze_threshold,
            effective_analyze_scale_factor: analyze_scale_factor,
            analyze_trigger_at: analyze_trigger,
            autovacuum_enabled: av_enabled,
            recommendations,
        });
    }

    results.sort_by(|a, b| {
        b.vacuum_progress
            .partial_cmp(&a.vacuum_progress)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;

    fn make_table_with_stats(name: &str, reltuples: f64, dead: i64) -> Table {
        Table {
            oid: 0,
            schema: "public".into(),
            name: name.into(),
            columns: vec![],
            constraints: vec![],
            indexes: vec![],
            comment: None,
            stats: Some(TableStats {
                reltuples,
                relpages: 1000,
                dead_tuples: dead,
                last_vacuum: None,
                last_autovacuum: None,
                last_analyze: None,
                last_autoanalyze: None,
                seq_scan: 0,
                idx_scan: 0,
                table_size: 0,
            }),
            partition_info: None,
            policies: vec![],
            triggers: vec![],
            reloptions: vec![],
            rls_enabled: false,
        }
    }

    fn make_snap(tables: Vec<Table>) -> SchemaSnapshot {
        SchemaSnapshot {
            pg_version: "16.0".into(),
            database: "test".into(),
            timestamp: chrono::Utc::now(),
            content_hash: String::new(),
            source: None,
            tables,
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
    fn skips_small_tables() {
        let snap = make_snap(vec![make_table_with_stats("tiny", 100.0, 10)]);
        let results = analyze_vacuum_health(&snap);
        assert!(results.is_empty());
    }

    #[test]
    fn reports_large_table_with_defaults() {
        let snap = make_snap(vec![make_table_with_stats("big", 5_000_000.0, 100)]);
        let results = analyze_vacuum_health(&snap);
        assert_eq!(results.len(), 1);
        assert!(results[0].recommendations.iter().any(|r| r.contains("large table")));
    }

    #[test]
    fn reports_high_dead_ratio() {
        let snap = make_snap(vec![make_table_with_stats("dirty", 100_000.0, 20_000)]);
        let results = analyze_vacuum_health(&snap);
        assert_eq!(results.len(), 1);
        assert!(results[0].recommendations.iter().any(|r| r.contains("high dead tuple")));
    }

    #[test]
    fn disabled_autovacuum_warns() {
        let mut table = make_table_with_stats("bad", 100_000.0, 100);
        table.reloptions = vec!["autovacuum_enabled=false".into()];
        let snap = make_snap(vec![table]);
        let results = analyze_vacuum_health(&snap);
        assert_eq!(results.len(), 1);
        assert!(results[0].recommendations.iter().any(|r| r.contains("disabled")));
        assert!(!results[0].autovacuum_enabled);
    }

    #[test]
    fn parses_defaults_from_gucs() {
        let gucs = vec![
            GucSetting { name: "autovacuum_vacuum_threshold".into(), setting: "100".into(), unit: None },
            GucSetting { name: "autovacuum_vacuum_scale_factor".into(), setting: "0.05".into(), unit: None },
            GucSetting { name: "autovacuum_analyze_threshold".into(), setting: "200".into(), unit: None },
            GucSetting { name: "autovacuum_analyze_scale_factor".into(), setting: "0.02".into(), unit: None },
        ];
        let d = parse_autovacuum_defaults(&gucs);
        assert_eq!(d.vacuum_threshold, 100);
        assert!((d.vacuum_scale_factor - 0.05).abs() < f64::EPSILON);
        assert_eq!(d.analyze_threshold, 200);
        assert!((d.analyze_scale_factor - 0.02).abs() < f64::EPSILON);
    }
}
