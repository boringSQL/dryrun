use serde::{Deserialize, Serialize};

use super::snapshot::{AnnotatedSchema, QualifiedName};
use super::types::GucSetting;

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
#[path = "vacuum_tests.rs"]
mod tests;
