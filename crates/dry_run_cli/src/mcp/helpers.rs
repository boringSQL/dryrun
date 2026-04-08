use dry_run_core::schema::NodeStats;
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

pub fn format_node_table_breakdown(node_stats: &[NodeStats], schema: &str, table: &str) -> Option<String> {
    if node_stats.is_empty() {
        return None;
    }

    let newest = node_stats.iter().map(|ns| ns.timestamp).max();
    let stale_threshold = newest.map(|t| t - chrono::TimeDelta::days(7));

    let mut lines: Vec<String> = Vec::new();
    lines.push(format!(
        "\nPer-node breakdown ({} node(s)):\n",
        node_stats.len()
    ));
    lines.push(format!(
        "{:<16} {:>12} {:>10} {:>10} {:>10} {:>12}  {}",
        "", "reltuples", "relpages", "seq_scan", "idx_scan", "table_size", "collected"
    ));

    for ns in node_stats {
        let ts = ns
            .table_stats
            .iter()
            .find(|t| t.table == table && t.schema == schema);

        if let Some(ts) = ts {
            let size_mb = ts.stats.table_size / (1024 * 1024);
            let collected = ns.timestamp.format("%Y-%m-%d %H:%M");
            let stale = stale_threshold
                .is_some_and(|threshold| ns.timestamp < threshold);
            lines.push(format!(
                "{:<16} {:>12} {:>10} {:>10} {:>10} {:>9} MB  {}{}",
                ns.source,
                format_number(ts.stats.reltuples as i64),
                format_number(ts.stats.relpages),
                format_number(ts.stats.seq_scan),
                format_number(ts.stats.idx_scan),
                format_number(size_mb),
                collected,
                if stale { " (stale)" } else { "" },
            ));
        } else {
            lines.push(format!("{:<16} (no data for this table)", ns.source));
        }
    }

    Some(lines.join("\n"))
}
