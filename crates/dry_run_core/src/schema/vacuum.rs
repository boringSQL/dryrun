use serde::{Deserialize, Serialize};

use super::types::{AnnotatedSchema, GucSetting, QualifiedName};

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

pub fn analyze_vacuum_health(annotated: &AnnotatedSchema<'_>) -> Vec<VacuumHealth> {
    let defaults = parse_autovacuum_defaults(&annotated.schema.gucs);
    let mut results = Vec::new();

    for table in &annotated.schema.tables {
        let qn = QualifiedName::new(&table.schema, &table.name);
        let reltuples = match annotated.reltuples(&qn) {
            Some(r) if r >= 10_000.0 => r,
            _ => continue,
        };
        let dead_tuples = annotated.n_dead_tup_sum(&qn);

        let opts = parse_reloptions(&table.reloptions);
        let has_overrides = opts.keys().any(|k| k.starts_with("autovacuum_"));

        let mut threshold = defaults.vacuum_threshold;
        let mut scale_factor = defaults.vacuum_scale_factor;
        let mut analyze_threshold = defaults.analyze_threshold;
        let mut analyze_scale_factor = defaults.analyze_scale_factor;
        let mut av_enabled = defaults.enabled;

        if let Some(v) = opts.get("autovacuum_vacuum_threshold")
            && let Ok(parsed) = v.parse::<i64>()
        {
            threshold = parsed;
        }
        if let Some(v) = opts.get("autovacuum_vacuum_scale_factor")
            && let Ok(parsed) = v.parse::<f64>()
        {
            scale_factor = parsed;
        }
        if let Some(v) = opts.get("autovacuum_analyze_threshold")
            && let Ok(parsed) = v.parse::<i64>()
        {
            analyze_threshold = parsed;
        }
        if let Some(v) = opts.get("autovacuum_analyze_scale_factor")
            && let Ok(parsed) = v.parse::<f64>()
        {
            analyze_scale_factor = parsed;
        }
        if let Some(v) = opts.get("autovacuum_enabled") {
            av_enabled = v == "on" || v == "true";
        }

        let trigger_at = threshold as f64 + scale_factor * reltuples;
        let analyze_trigger = analyze_threshold as f64 + analyze_scale_factor * reltuples;
        let progress = if trigger_at > 0.0 {
            dead_tuples as f64 / trigger_at
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

        if reltuples >= 1_000_000.0 && !has_overrides {
            let mut suggested_vac_sf = 100_000.0 / reltuples;
            suggested_vac_sf = (suggested_vac_sf * 1000.0).round() / 1000.0;
            if suggested_vac_sf < 0.001 {
                suggested_vac_sf = 0.001;
            }
            let suggested_az_sf = (suggested_vac_sf / 2.0 * 1000.0).round() / 1000.0;

            // threshold: ~1% of rows, clamped to 500..5000
            let suggested_vac_thresh = ((reltuples * 0.01) as i64).clamp(500, 5000);
            let suggested_az_thresh = (suggested_vac_thresh / 2).max(250);

            recommendations.push(format!(
                "large table ({}k rows) using default autovacuum settings; consider: \
                 autovacuum_vacuum_scale_factor={suggested_vac_sf}, \
                 autovacuum_vacuum_threshold={suggested_vac_thresh}, \
                 autovacuum_analyze_scale_factor={suggested_az_sf}, \
                 autovacuum_analyze_threshold={suggested_az_thresh}",
                reltuples as i64 / 1000
            ));
        }

        if reltuples > 0.0 && dead_tuples as f64 / reltuples > 0.10 {
            recommendations.push(format!(
                "high dead tuple ratio: {} dead / {}k live ({:.1}%)",
                dead_tuples,
                reltuples as i64 / 1000,
                dead_tuples as f64 / reltuples * 100.0
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
            reltuples,
            dead_tuples,
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
    use std::collections::BTreeMap;

    use super::*;
    use crate::schema::types::{
        ActivityStatsSnapshot, AnnotatedSnapshot, IndexActivityEntry, NodeIdentity,
        PlannerStatsSnapshot, TableActivity, TableActivityEntry, TableSizing, TableSizingEntry,
    };
    use crate::schema::*;

    fn ddl_table(name: &str) -> Table {
        Table {
            oid: 0,
            schema: "public".into(),
            name: name.into(),
            columns: vec![],
            constraints: vec![],
            indexes: vec![],
            comment: None,
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
        }
    }

    fn annotated(
        tables: Vec<Table>,
        sizing: Vec<(&str, f64, i64)>,
        dead_by_table: Vec<(&str, i64)>,
    ) -> AnnotatedSnapshot {
        let schema = make_snap(tables);
        let planner = PlannerStatsSnapshot {
            pg_version: "16.0".into(),
            database: "test".into(),
            timestamp: chrono::Utc::now(),
            content_hash: "ph".into(),
            schema_ref_hash: "sh".into(),
            tables: sizing
                .into_iter()
                .map(|(name, reltuples, table_size)| TableSizingEntry {
                    table: QualifiedName::new("public", name),
                    sizing: TableSizing {
                        reltuples,
                        relpages: 1000,
                        table_size,
                        total_size: None,
                        index_size: None,
                    },
                })
                .collect(),
            columns: vec![],
            indexes: vec![],
        };
        let activity = ActivityStatsSnapshot {
            pg_version: "16.0".into(),
            database: "test".into(),
            timestamp: chrono::Utc::now(),
            content_hash: "ah".into(),
            schema_ref_hash: "sh".into(),
            node: NodeIdentity {
                label: "primary".into(),
                host: "p".into(),
                is_standby: false,
                replication_lag_bytes: None,
                stats_reset: None,
            },
            tables: dead_by_table
                .into_iter()
                .map(|(name, dead)| TableActivityEntry {
                    table: QualifiedName::new("public", name),
                    activity: TableActivity {
                        seq_scan: 0,
                        idx_scan: 0,
                        n_live_tup: 0,
                        n_dead_tup: dead,
                        last_vacuum: None,
                        last_autovacuum: None,
                        last_analyze: None,
                        last_autoanalyze: None,
                        vacuum_count: 0,
                        autovacuum_count: 0,
                        analyze_count: 0,
                        autoanalyze_count: 0,
                    },
                })
                .collect(),
            indexes: Vec::<IndexActivityEntry>::new(),
        };
        let mut activity_by_node = BTreeMap::new();
        activity_by_node.insert("primary".into(), activity);
        AnnotatedSnapshot {
            schema,
            planner: Some(planner),
            activity_by_node,
        }
    }

    #[test]
    fn skips_small_tables() {
        let snap = annotated(
            vec![ddl_table("tiny")],
            vec![("tiny", 100.0, 0)],
            vec![("tiny", 10)],
        );
        let results = analyze_vacuum_health(&snap.view(None));
        assert!(results.is_empty());
    }

    #[test]
    fn reports_large_table_with_defaults() {
        let snap = annotated(
            vec![ddl_table("big")],
            vec![("big", 5_000_000.0, 0)],
            vec![("big", 100)],
        );
        let results = analyze_vacuum_health(&snap.view(None));
        assert_eq!(results.len(), 1);
        assert!(
            results[0]
                .recommendations
                .iter()
                .any(|r| r.contains("large table"))
        );
    }

    #[test]
    fn reports_high_dead_ratio() {
        let snap = annotated(
            vec![ddl_table("dirty")],
            vec![("dirty", 100_000.0, 0)],
            vec![("dirty", 20_000)],
        );
        let results = analyze_vacuum_health(&snap.view(None));
        assert_eq!(results.len(), 1);
        assert!(
            results[0]
                .recommendations
                .iter()
                .any(|r| r.contains("high dead tuple"))
        );
    }

    #[test]
    fn disabled_autovacuum_warns() {
        let mut table = ddl_table("bad");
        table.reloptions = vec!["autovacuum_enabled=false".into()];
        let snap = annotated(vec![table], vec![("bad", 100_000.0, 0)], vec![("bad", 100)]);
        let results = analyze_vacuum_health(&snap.view(None));
        assert_eq!(results.len(), 1);
        assert!(
            results[0]
                .recommendations
                .iter()
                .any(|r| r.contains("disabled"))
        );
        assert!(!results[0].autovacuum_enabled);
    }

    #[test]
    fn skipped_when_planner_absent() {
        // Degradation case: schema has the table but planner is None → reltuples
        // returns None → skipped. Pins the new "no data → no findings" path.
        let snap = AnnotatedSnapshot {
            schema: make_snap(vec![ddl_table("big")]),
            planner: None,
            activity_by_node: BTreeMap::new(),
        };
        assert!(analyze_vacuum_health(&snap.view(None)).is_empty());
    }

    #[test]
    fn dead_tuples_summed_across_replicas() {
        // 3-node cluster, dead_tuples reported per node. Cluster sum drives the
        // ratio check.
        let schema = make_snap(vec![ddl_table("hot")]);
        let planner = PlannerStatsSnapshot {
            pg_version: "16.0".into(),
            database: "test".into(),
            timestamp: chrono::Utc::now(),
            content_hash: "ph".into(),
            schema_ref_hash: "sh".into(),
            tables: vec![TableSizingEntry {
                table: QualifiedName::new("public", "hot"),
                sizing: TableSizing {
                    reltuples: 100_000.0,
                    relpages: 1000,
                    table_size: 0,
                    total_size: None,
                    index_size: None,
                },
            }],
            columns: vec![],
            indexes: vec![],
        };
        let mut activity_by_node = BTreeMap::new();
        for (label, dead) in [
            ("primary", 8_000_i64),
            ("replica1", 7_000),
            ("replica2", 6_000),
        ] {
            activity_by_node.insert(
                label.into(),
                ActivityStatsSnapshot {
                    pg_version: "16.0".into(),
                    database: "test".into(),
                    timestamp: chrono::Utc::now(),
                    content_hash: format!("h-{label}"),
                    schema_ref_hash: "sh".into(),
                    node: NodeIdentity {
                        label: label.into(),
                        host: label.into(),
                        is_standby: label != "primary",
                        replication_lag_bytes: None,
                        stats_reset: None,
                    },
                    tables: vec![TableActivityEntry {
                        table: QualifiedName::new("public", "hot"),
                        activity: TableActivity {
                            seq_scan: 0,
                            idx_scan: 0,
                            n_live_tup: 0,
                            n_dead_tup: dead,
                            last_vacuum: None,
                            last_autovacuum: None,
                            last_analyze: None,
                            last_autoanalyze: None,
                            vacuum_count: 0,
                            autovacuum_count: 0,
                            analyze_count: 0,
                            autoanalyze_count: 0,
                        },
                    }],
                    indexes: vec![],
                },
            );
        }
        let snap = AnnotatedSnapshot {
            schema,
            planner: Some(planner),
            activity_by_node,
        };
        let results = analyze_vacuum_health(&snap.view(None));
        assert_eq!(results.len(), 1);
        // 8k+7k+6k = 21k dead vs 100k live → 21% > 10% threshold
        assert_eq!(results[0].dead_tuples, 21_000);
        assert!(
            results[0]
                .recommendations
                .iter()
                .any(|r| r.contains("high dead tuple"))
        );
    }

    #[test]
    fn parses_defaults_from_gucs() {
        let gucs = vec![
            GucSetting {
                name: "autovacuum_vacuum_threshold".into(),
                setting: "100".into(),
                unit: None,
            },
            GucSetting {
                name: "autovacuum_vacuum_scale_factor".into(),
                setting: "0.05".into(),
                unit: None,
            },
            GucSetting {
                name: "autovacuum_analyze_threshold".into(),
                setting: "200".into(),
                unit: None,
            },
            GucSetting {
                name: "autovacuum_analyze_scale_factor".into(),
                setting: "0.02".into(),
                unit: None,
            },
        ];
        let d = parse_autovacuum_defaults(&gucs);
        assert_eq!(d.vacuum_threshold, 100);
        assert!((d.vacuum_scale_factor - 0.05).abs() < f64::EPSILON);
        assert_eq!(d.analyze_threshold, 200);
        assert!((d.analyze_scale_factor - 0.02).abs() < f64::EPSILON);
    }
}
