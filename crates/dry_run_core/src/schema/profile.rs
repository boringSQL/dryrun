use serde::Serialize;

use super::types::{Column, ColumnStats};

#[derive(Debug, Clone, Serialize)]
pub struct ColumnProfile {
    pub cardinality: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distribution: Option<String>,
    pub nulls: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_order: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_range: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub top_values: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Build a human-readable profile for a single column.
pub fn profile_column(col: &Column, table_rows: f64) -> Option<ColumnProfile> {
    let s = col.stats.as_ref()?;

    Some(ColumnProfile {
        cardinality: profile_cardinality(s, table_rows),
        distribution: profile_distribution(s),
        nulls: profile_nulls(s, table_rows),
        physical_order: profile_correlation(s),
        value_range: profile_range(s),
        top_values: parse_top_values(s, 5),
        note: profile_note(col, s, table_rows),
    })
}

/// Estimated selectivity (0..1) for equality predicate on this column.
pub fn column_selectivity(col: &Column, table_rows: f64) -> f64 {
    let s = match col.stats.as_ref() {
        Some(s) => s,
        None => return 0.5,
    };

    let n_distinct = match s.n_distinct {
        Some(v) => v,
        None => return 0.5,
    };

    let distinct_count = if n_distinct < 0.0 {
        // negative means fraction of rows
        (-n_distinct * table_rows).max(1.0)
    } else if n_distinct > 0.0 {
        n_distinct
    } else {
        return 0.5;
    };

    1.0 / distinct_count
}

/// Returns Some((dominant_value, frequency)) when a single value exceeds the
/// given frequency threshold.
pub fn has_skewed_distribution(
    stats: &ColumnStats,
    threshold: f64,
) -> Option<(String, f64)> {
    let vals = stats.most_common_vals.as_deref().map(parse_pg_array)?;
    let freqs = stats.most_common_freqs.as_deref().map(parse_pg_array)?;

    for (v, f_str) in vals.iter().zip(freqs.iter()) {
        if let Ok(f) = f_str.parse::<f64>() {
            if f > threshold {
                return Some((v.clone(), f));
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// private helpers
// ---------------------------------------------------------------------------

fn profile_nulls(s: &ColumnStats, table_rows: f64) -> String {
    let frac = s.null_frac.unwrap_or(0.0);
    if frac <= 0.0 {
        return "none".to_string();
    }
    let pct = frac * 100.0;
    if table_rows > 0.0 {
        let rows = (frac * table_rows).round() as i64;
        format!("{pct:.1}% (~{rows} rows)")
    } else {
        format!("{pct:.1}%")
    }
}

fn profile_cardinality(s: &ColumnStats, table_rows: f64) -> String {
    let nd = match s.n_distinct {
        Some(v) => v,
        None => return "unknown".to_string(),
    };

    if nd == 1.0 {
        return "constant (1 value)".to_string();
    }

    let abs_count = if nd < 0.0 {
        (-nd * table_rows).round() as i64
    } else {
        nd.round() as i64
    };

    let ratio = if table_rows > 0.0 {
        abs_count as f64 / table_rows
    } else {
        0.0
    };

    let label = if nd == -1.0 || ratio >= 0.95 {
        "unique"
    } else if ratio >= 0.5 {
        "high"
    } else if ratio >= 0.1 {
        "medium"
    } else if ratio >= 0.01 {
        "low"
    } else {
        "very low"
    };

    format!("{label} ({abs_count} distinct)")
}

fn profile_distribution(s: &ColumnStats) -> Option<String> {
    let raw = s.most_common_freqs.as_deref()?;
    let freqs: Vec<f64> = parse_pg_array(raw)
        .iter()
        .filter_map(|v| v.parse::<f64>().ok())
        .collect();

    if freqs.is_empty() {
        return Some("uniform".to_string());
    }

    let min = freqs.iter().copied().fold(f64::INFINITY, f64::min);
    let max = freqs.iter().copied().fold(f64::NEG_INFINITY, f64::max);

    if min <= 0.0 {
        return Some("uniform".to_string());
    }

    let ratio = max / min;

    let label = if ratio > 3.0 && max > 0.5 {
        "heavily skewed"
    } else if ratio > 2.0 {
        "skewed"
    } else {
        "uniform"
    };

    Some(label.to_string())
}

fn profile_correlation(s: &ColumnStats) -> Option<String> {
    let corr = s.correlation?;
    let abs = corr.abs();
    let label = if abs > 0.99 {
        "perfectly ordered".to_string()
    } else if abs > 0.9 {
        "well ordered".to_string()
    } else if abs > 0.5 {
        "partially ordered".to_string()
    } else {
        format!("random (correlation: {corr:.2})")
    };
    Some(label)
}

fn profile_range(s: &ColumnStats) -> Option<String> {
    let raw = s.histogram_bounds.as_deref()?;
    let bounds = parse_pg_array(raw);
    if bounds.len() < 2 {
        return None;
    }
    let first = &bounds[0];
    let last = &bounds[bounds.len() - 1];
    Some(format!("{first} to {last}"))
}

fn parse_top_values(s: &ColumnStats, limit: usize) -> Vec<String> {
    let vals = match s.most_common_vals.as_deref().map(parse_pg_array) {
        Some(v) if !v.is_empty() => v,
        _ => return Vec::new(),
    };
    let freqs = s
        .most_common_freqs
        .as_deref()
        .map(parse_pg_array)
        .unwrap_or_default();

    vals.iter()
        .zip(freqs.iter())
        .take(limit)
        .filter_map(|(v, f_str)| {
            let f: f64 = f_str.parse().ok()?;
            Some(format!("{v} ({:.0}%)", f * 100.0))
        })
        .collect()
}

fn profile_note(col: &Column, s: &ColumnStats, table_rows: f64) -> Option<String> {
    // low-cardinality text column -> suggest enum
    if let Some(nd) = s.n_distinct {
        if nd > 0.0 && nd <= 10.0 {
            let t = col.type_name.to_lowercase();
            if t.contains("text") || t.contains("varchar") || t.contains("character varying") {
                return Some("Consider using an enum type".to_string());
            }
        }
    }

    // very high null ratio
    if let Some(nf) = s.null_frac {
        if nf > 0.8 {
            return Some(
                "Very high null ratio; partial index WHERE col IS NOT NULL recommended"
                    .to_string(),
            );
        }
    }

    // low physical correlation on large table
    if let Some(corr) = s.correlation {
        if corr.abs() < 0.3 && table_rows > 100_000.0 {
            return Some(
                "Low physical correlation; BRIN index will be ineffective, use btree".to_string(),
            );
        }
    }

    None
}

fn parse_pg_array(s: &str) -> Vec<String> {
    let s = s.trim();
    // strip outer braces
    let inner = if s.starts_with('{') && s.ends_with('}') {
        &s[1..s.len() - 1]
    } else {
        s
    };

    if inner.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::new();
    let mut chars = inner.chars().peekable();

    loop {
        // skip whitespace before value
        while chars.peek() == Some(&' ') {
            chars.next();
        }

        if chars.peek().is_none() {
            break;
        }

        if chars.peek() == Some(&'"') {
            // quoted value
            chars.next(); // consume opening quote
            let mut val = String::new();
            loop {
                match chars.next() {
                    Some('\\') => {
                        // escaped character
                        if let Some(c) = chars.next() {
                            val.push(c);
                        }
                    }
                    Some('"') => break,
                    Some(c) => val.push(c),
                    None => break,
                }
            }
            result.push(val);
        } else {
            // unquoted value
            let mut val = String::new();
            loop {
                match chars.peek() {
                    Some(&',') | None => break,
                    Some(_) => val.push(chars.next().unwrap()),
                }
            }
            result.push(val.trim_end().to_string());
        }

        // consume comma separator
        if chars.peek() == Some(&',') {
            chars.next();
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_stats(n_distinct: Option<f64>) -> ColumnStats {
        ColumnStats {
            null_frac: Some(0.0),
            n_distinct,
            most_common_vals: None,
            most_common_freqs: None,
            histogram_bounds: None,
            correlation: None,
        }
    }

    fn make_col(type_name: &str, stats: Option<ColumnStats>) -> Column {
        Column {
            name: "test_col".to_string(),
            ordinal: 1,
            type_name: type_name.to_string(),
            nullable: true,
            default: None,
            identity: None,
            generated: None,
            comment: None,
            statistics_target: None,
            stats,
        }
    }

    #[test]
    fn test_parse_pg_array_simple() {
        let vals = parse_pg_array("{a,b,c}");
        assert_eq!(vals, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_pg_array_quoted() {
        let vals = parse_pg_array(r#"{hello,"world with spaces","escaped\"quote"}"#);
        assert_eq!(vals, vec!["hello", "world with spaces", r#"escaped"quote"#]);
    }

    #[test]
    fn test_parse_pg_array_empty() {
        assert!(parse_pg_array("{}").is_empty());
    }

    #[test]
    fn test_profile_nulls_none() {
        let s = make_stats(None);
        assert_eq!(profile_nulls(&s, 1000.0), "none");
    }

    #[test]
    fn test_profile_nulls_with_rows() {
        let mut s = make_stats(None);
        s.null_frac = Some(0.25);
        assert_eq!(profile_nulls(&s, 1000.0), "25.0% (~250 rows)");
    }

    #[test]
    fn test_profile_cardinality_unique() {
        let s = make_stats(Some(-1.0));
        let result = profile_cardinality(&s, 5000.0);
        assert!(result.starts_with("unique"), "got: {result}");
    }

    #[test]
    fn test_profile_cardinality_constant() {
        let s = make_stats(Some(1.0));
        assert_eq!(profile_cardinality(&s, 1000.0), "constant (1 value)");
    }

    #[test]
    fn test_profile_cardinality_low_positive() {
        let s = make_stats(Some(5.0));
        let result = profile_cardinality(&s, 10000.0);
        assert!(result.contains("very low"), "got: {result}");
        assert!(result.contains("5 distinct"));
    }

    #[test]
    fn test_column_selectivity_negative_distinct() {
        let col = make_col("integer", Some(make_stats(Some(-0.5))));
        let sel = column_selectivity(&col, 10000.0);
        // -0.5 -> 5000 distinct -> selectivity 0.0002
        assert!((sel - 0.0002).abs() < 0.0001);
    }

    #[test]
    fn test_column_selectivity_positive_distinct() {
        let col = make_col("integer", Some(make_stats(Some(100.0))));
        let sel = column_selectivity(&col, 10000.0);
        assert!((sel - 0.01).abs() < 0.0001);
    }

    #[test]
    fn test_column_selectivity_no_stats() {
        let col = make_col("integer", None);
        assert_eq!(column_selectivity(&col, 1000.0), 0.5);
    }

    #[test]
    fn test_has_skewed_distribution_found() {
        let stats = ColumnStats {
            null_frac: None,
            n_distinct: None,
            most_common_vals: Some("{active,inactive}".to_string()),
            most_common_freqs: Some("{0.85,0.15}".to_string()),
            histogram_bounds: None,
            correlation: None,
        };
        let result = has_skewed_distribution(&stats, 0.8);
        assert!(result.is_some());
        let (val, freq) = result.unwrap();
        assert_eq!(val, "active");
        assert!((freq - 0.85).abs() < 0.001);
    }

    #[test]
    fn test_has_skewed_distribution_not_found() {
        let stats = ColumnStats {
            null_frac: None,
            n_distinct: None,
            most_common_vals: Some("{a,b}".to_string()),
            most_common_freqs: Some("{0.5,0.5}".to_string()),
            histogram_bounds: None,
            correlation: None,
        };
        assert!(has_skewed_distribution(&stats, 0.8).is_none());
    }

    #[test]
    fn test_profile_range_extracts_bounds() {
        let stats = ColumnStats {
            null_frac: None,
            n_distinct: None,
            most_common_vals: None,
            most_common_freqs: None,
            histogram_bounds: Some("{1,50,100,200,500}".to_string()),
            correlation: None,
        };
        assert_eq!(profile_range(&stats), Some("1 to 500".to_string()));
    }

    #[test]
    fn test_profile_correlation_well_ordered() {
        let stats = ColumnStats {
            null_frac: None,
            n_distinct: None,
            most_common_vals: None,
            most_common_freqs: None,
            histogram_bounds: None,
            correlation: Some(0.95),
        };
        assert_eq!(profile_correlation(&stats), Some("well ordered".to_string()));
    }

    #[test]
    fn test_profile_note_enum_suggestion() {
        let mut s = make_stats(Some(3.0));
        s.null_frac = Some(0.0);
        let col = make_col("text", Some(s));
        let note = profile_note(&col, col.stats.as_ref().unwrap(), 1000.0);
        assert_eq!(note, Some("Consider using an enum type".to_string()));
    }

    #[test]
    fn test_profile_note_high_nulls() {
        let mut s = make_stats(Some(100.0));
        s.null_frac = Some(0.9);
        let col = make_col("integer", Some(s));
        let note = profile_note(&col, col.stats.as_ref().unwrap(), 1000.0);
        assert!(note.unwrap().contains("partial index"));
    }

    #[test]
    fn test_profile_column_returns_none_without_stats() {
        let col = make_col("integer", None);
        assert!(profile_column(&col, 1000.0).is_none());
    }

    #[test]
    fn test_profile_column_returns_some_with_stats() {
        let s = ColumnStats {
            null_frac: Some(0.1),
            n_distinct: Some(-0.8),
            most_common_vals: Some("{foo,bar}".to_string()),
            most_common_freqs: Some("{0.6,0.4}".to_string()),
            histogram_bounds: Some("{1,100}".to_string()),
            correlation: Some(0.99),
        };
        let col = make_col("integer", Some(s));
        let p = profile_column(&col, 10000.0).unwrap();
        assert!(p.cardinality.contains("high"));
        assert_eq!(p.nulls, "10.0% (~1000 rows)");
        assert!(p.physical_order.is_some());
        assert!(p.value_range.is_some());
        assert!(!p.top_values.is_empty());
    }

    #[test]
    fn test_parse_top_values_limit() {
        let s = ColumnStats {
            null_frac: None,
            n_distinct: None,
            most_common_vals: Some("{a,b,c,d,e,f}".to_string()),
            most_common_freqs: Some("{0.3,0.2,0.15,0.1,0.1,0.05}".to_string()),
            histogram_bounds: None,
            correlation: None,
        };
        let vals = parse_top_values(&s, 3);
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[0], "a (30%)");
    }
}
