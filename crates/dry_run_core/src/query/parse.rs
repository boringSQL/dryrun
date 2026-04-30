use std::collections::HashSet;

use pg_query::NodeRef;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedQuery {
    pub sql: String,
    pub info: QueryInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryInfo {
    pub tables: Vec<ReferencedTable>,
    pub filter_columns: Vec<(Option<String>, String)>,
    pub func_wrapped_columns: Vec<FuncWrappedColumn>,
    pub update_targets: Vec<String>,
    pub has_select_star: bool,
    pub has_limit: bool,
    pub has_where: bool,
    pub has_join: bool,
    pub statement_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuncWrappedColumn {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    pub column: String,
    pub func_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferencedTable {
    pub schema: Option<String>,
    pub name: String,
    pub alias: Option<String>,
    pub context: String,
}

pub fn parse_sql(sql: &str) -> Result<ParsedQuery> {
    let result =
        pg_query::parse(sql).map_err(|e| Error::Introspection(format!("SQL parse error: {e}")))?;

    let mut tables = Vec::new();
    let mut has_select_star = false;
    let mut has_join = false;
    let mut has_where = false;
    let mut has_limit = false;
    let mut statement_type = String::new();

    let mut func_wrapped_columns = Vec::new();
    let mut update_targets = Vec::new();

    let mut seen_tables: HashSet<String> = HashSet::new();
    for (table_name, context) in &result.tables {
        let ctx_str = match context {
            pg_query::Context::Select => "select",
            pg_query::Context::DML => "dml",
            pg_query::Context::DDL => "ddl",
            _ => "other",
        };
        if seen_tables.insert(format!("{table_name}:{ctx_str}")) {
            let (schema, name) = split_qualified(table_name);
            let alias = result
                .aliases
                .iter()
                .find(|(_, v)| v.as_str() == table_name)
                .map(|(k, _)| k.clone());
            tables.push(ReferencedTable {
                schema,
                name,
                alias,
                context: ctx_str.to_string(),
            });
        }
    }

    for (node, _depth, _context, _) in result.protobuf.nodes() {
        match node {
            NodeRef::SelectStmt(s) => {
                if statement_type.is_empty() {
                    statement_type = "SELECT".into();
                }
                if s.where_clause.is_some() {
                    has_where = true;
                    collect_func_wrapped_columns(
                        s.where_clause.as_deref(),
                        &mut func_wrapped_columns,
                    );
                }
                if s.limit_count.is_some() || s.limit_offset.is_some() {
                    has_limit = true;
                }
                for target in &s.target_list {
                    if let Some(pg_query::protobuf::node::Node::ResTarget(rt)) = &target.node
                        && let Some(val) = &rt.val
                            && let Some(pg_query::protobuf::node::Node::ColumnRef(cr)) = &val.node {
                                for field in &cr.fields {
                                    if let Some(pg_query::protobuf::node::Node::AStar(_)) =
                                        &field.node
                                    {
                                        has_select_star = true;
                                    }
                                }
                            }
                }
            }
            NodeRef::InsertStmt(_)
                if statement_type.is_empty() => {
                    statement_type = "INSERT".into();
                }
            NodeRef::UpdateStmt(u) => {
                if statement_type.is_empty() {
                    statement_type = "UPDATE".into();
                }
                if u.where_clause.is_some() {
                    has_where = true;
                    collect_func_wrapped_columns(
                        u.where_clause.as_deref(),
                        &mut func_wrapped_columns,
                    );
                }
                for tl in &u.target_list {
                    if let Some(pg_query::protobuf::node::Node::ResTarget(rt)) = &tl.node
                        && !rt.name.is_empty() {
                            update_targets.push(rt.name.clone());
                        }
                }
            }
            NodeRef::DeleteStmt(d) => {
                if statement_type.is_empty() {
                    statement_type = "DELETE".into();
                }
                if d.where_clause.is_some() {
                    has_where = true;
                }
            }
            NodeRef::JoinExpr(_) => {
                has_join = true;
            }
            _ => {}
        }
    }

    let filter_columns: Vec<(Option<String>, String)> = result
        .filter_columns
        .into_iter()
        .map(|(tbl, col)| (tbl.map(|s| s.to_string()), col.to_string()))
        .collect();

    Ok(ParsedQuery {
        sql: sql.to_string(),
        info: QueryInfo {
            tables,
            filter_columns,
            func_wrapped_columns,
            update_targets,
            has_select_star,
            has_limit,
            has_where,
            has_join,
            statement_type,
        },
    })
}

fn split_qualified(name: &str) -> (Option<String>, String) {
    if let Some((schema, table)) = name.rsplit_once('.') {
        (Some(schema.to_string()), table.to_string())
    } else {
        (None, name.to_string())
    }
}

fn collect_func_wrapped_columns(
    node: Option<&pg_query::protobuf::Node>,
    out: &mut Vec<FuncWrappedColumn>,
) {
    let node = match node {
        Some(n) => n,
        None => return,
    };
    let inner = match &node.node {
        Some(n) => n,
        None => return,
    };

    match inner {
        pg_query::protobuf::node::Node::FuncCall(fc) => {
            let func_name = extract_func_name(&fc.funcname);
            for arg in &fc.args {
                if let Some(col) = as_column_ref(arg) {
                    out.push(FuncWrappedColumn {
                        table: col.0,
                        column: col.1,
                        func_name: func_name.clone(),
                    });
                } else {
                    collect_func_wrapped_columns(Some(arg), out);
                }
            }
        }
        pg_query::protobuf::node::Node::TypeCast(tc) => {
            if let Some(arg) = &tc.arg {
                if let Some(col) = as_column_ref(arg) {
                    let type_name = tc
                        .type_name
                        .as_ref()
                        .map(|tn| format!("::{}", extract_type_name(tn)))
                        .unwrap_or_default();
                    out.push(FuncWrappedColumn {
                        table: col.0,
                        column: col.1,
                        func_name: type_name,
                    });
                } else {
                    collect_func_wrapped_columns(Some(arg), out);
                }
            }
        }
        pg_query::protobuf::node::Node::BoolExpr(be) => {
            for arg in &be.args {
                collect_func_wrapped_columns(Some(arg), out);
            }
        }
        pg_query::protobuf::node::Node::AExpr(ae) => {
            collect_func_wrapped_columns(ae.lexpr.as_deref(), out);
            collect_func_wrapped_columns(ae.rexpr.as_deref(), out);
        }
        pg_query::protobuf::node::Node::SubLink(sl) => {
            collect_func_wrapped_columns(sl.testexpr.as_deref(), out);
        }
        _ => {}
    }
}

fn as_column_ref(node: &pg_query::protobuf::Node) -> Option<(Option<String>, String)> {
    if let Some(pg_query::protobuf::node::Node::ColumnRef(cr)) = &node.node {
        let fields: Vec<String> = cr
            .fields
            .iter()
            .filter_map(|f| {
                if let Some(pg_query::protobuf::node::Node::String(s)) = &f.node {
                    Some(s.sval.clone())
                } else {
                    None
                }
            })
            .collect();
        match fields.len() {
            1 => Some((None, fields[0].clone())),
            2 => Some((Some(fields[0].clone()), fields[1].clone())),
            _ => None,
        }
    } else {
        None
    }
}

fn extract_func_name(funcname: &[pg_query::protobuf::Node]) -> String {
    funcname
        .last()
        .and_then(|n| {
            if let Some(pg_query::protobuf::node::Node::String(s)) = &n.node {
                Some(s.sval.to_lowercase())
            } else {
                None
            }
        })
        .unwrap_or_default()
}

fn extract_type_name(tn: &pg_query::protobuf::TypeName) -> String {
    tn.names
        .last()
        .and_then(|n| {
            if let Some(pg_query::protobuf::node::Node::String(s)) = &n.node {
                Some(s.sval.clone())
            } else {
                None
            }
        })
        .unwrap_or_default()
}
