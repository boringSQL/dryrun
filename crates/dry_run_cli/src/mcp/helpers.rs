use dry_run_core::schema::{AnnotatedSnapshot, QualifiedName};
use rmcp::ErrorData as McpError;

pub fn to_mcp_err(e: dry_run_core::Error) -> McpError {
    McpError::internal_error(e.to_string(), None)
}

pub fn format_number(n: i64) -> String {
    if n.abs() < 1_000 {
        return n.to_string();
    }
    let s = n.abs().to_string();
    let mut result = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    if n < 0 {
        result.push('-');
    }
    result.chars().rev().collect()
}

// Render a per-node activity table for one (schema, table) pair, attached
// as a trailer to MCP tool output.
//
// Sizing columns (`reltuples`, `relpages`, `table_size`) come from the
// planner snapshot — those are byte-identical across replicas (they're
// replicated via WAL), so it would be misleading to render one column per
// node. Counter columns (`seq_scan`, `idx_scan`) come from each node's
// activity row and naturally vary node-to-node.
//
// Returns None when there's no activity at all (single-node, no captures
// yet); the caller skips the section in that case.
pub fn format_node_table_breakdown(
    annotated: &AnnotatedSnapshot,
    schema: &str,
    table: &str,
) -> Option<String> {
    if annotated.activity_by_node.is_empty() {
        return None;
    }

    let qn = QualifiedName::new(schema, table);
    let view = annotated.view();

    // Pull sizing once — it's the same regardless of which node we're
    // displaying. `unwrap_or` zeros so the table still renders cleanly
    // when the planner snapshot is missing.
    let reltuples = view.reltuples(&qn).unwrap_or(0.0);
    let relpages = view.relpages(&qn).unwrap_or(0);
    let table_size = view.table_size(&qn).unwrap_or(0);

    // Stale = "this node's activity capture is more than 7 days older
    // than the freshest one in the bundle." Surfaces forgotten replicas.
    let newest = annotated
        .activity_by_node
        .values()
        .map(|a| a.timestamp)
        .max();
    let stale_threshold = newest.map(|t| t - chrono::TimeDelta::days(7));

    let mut lines: Vec<String> = Vec::new();
    lines.push(format!(
        "\nPer-node breakdown ({} node(s)):\n",
        annotated.activity_by_node.len()
    ));
    lines.push(format!(
        "{:<16} {:>12} {:>10} {:>10} {:>10} {:>12}  {}",
        "", "reltuples", "relpages", "seq_scan", "idx_scan", "table_size", "collected"
    ));

    for (label, activity) in &annotated.activity_by_node {
        let ta = activity.tables.iter().find(|e| e.table == qn);
        if let Some(ta) = ta {
            let size_mb = table_size / (1024 * 1024);
            let collected = activity.timestamp.format("%Y-%m-%d %H:%M");
            let stale = stale_threshold.is_some_and(|threshold| activity.timestamp < threshold);
            // idx_scan_sum on a single index would be ambiguous here —
            // the table-level row aggregates across all indexes already
            // (TableActivity.idx_scan), so we read it directly off the
            // entry.
            lines.push(format!(
                "{:<16} {:>12} {:>10} {:>10} {:>10} {:>9} MB  {}{}",
                label,
                format_number(reltuples as i64),
                format_number(relpages),
                format_number(ta.activity.seq_scan),
                format_number(ta.activity.idx_scan),
                format_number(size_mb),
                collected,
                if stale { " (stale)" } else { "" },
            ));
        } else {
            lines.push(format!("{:<16} (no data for this table)", label));
        }
    }

    Some(lines.join("\n"))
}
