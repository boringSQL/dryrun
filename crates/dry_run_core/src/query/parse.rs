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
    pub has_select_star: bool,
    pub has_limit: bool,
    pub has_where: bool,
    pub has_join: bool,
    pub statement_type: String,
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
                }
                if s.limit_count.is_some() || s.limit_offset.is_some() {
                    has_limit = true;
                }
                for target in &s.target_list {
                    if let Some(pg_query::protobuf::node::Node::ResTarget(rt)) = &target.node {
                        if let Some(val) = &rt.val {
                            if let Some(pg_query::protobuf::node::Node::ColumnRef(cr)) = &val.node {
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
                }
            }
            NodeRef::InsertStmt(_) => {
                if statement_type.is_empty() {
                    statement_type = "INSERT".into();
                }
            }
            NodeRef::UpdateStmt(u) => {
                if statement_type.is_empty() {
                    statement_type = "UPDATE".into();
                }
                if u.where_clause.is_some() {
                    has_where = true;
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_select() {
        let q = parse_sql("SELECT id, name FROM users WHERE id = 1").unwrap();
        assert_eq!(q.info.statement_type, "SELECT");
        assert!(q.info.has_where);
        assert!(!q.info.has_select_star);
        assert!(!q.info.has_join);
        assert_eq!(q.info.tables.len(), 1);
        assert_eq!(q.info.tables[0].name, "users");
    }

    #[test]
    fn detect_select_star() {
        let q = parse_sql("SELECT * FROM orders").unwrap();
        assert!(q.info.has_select_star);
        assert!(!q.info.has_where);
        assert!(!q.info.has_limit);
    }

    #[test]
    fn detect_join() {
        let q = parse_sql(
            "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total > 100",
        )
        .unwrap();
        assert!(q.info.has_join);
        assert!(q.info.has_where);
        assert_eq!(q.info.tables.len(), 2);
    }

    #[test]
    fn detect_limit() {
        let q = parse_sql("SELECT * FROM users LIMIT 10").unwrap();
        assert!(q.info.has_limit);
    }

    #[test]
    fn parse_error() {
        let result = parse_sql("SELEC broken");
        assert!(result.is_err());
    }

    #[test]
    fn detect_update_without_where() {
        let q = parse_sql("UPDATE users SET name = 'test'").unwrap();
        assert_eq!(q.info.statement_type, "UPDATE");
        assert!(!q.info.has_where);
    }

    #[test]
    fn detect_delete_with_where() {
        let q = parse_sql("DELETE FROM users WHERE id = 1").unwrap();
        assert_eq!(q.info.statement_type, "DELETE");
        assert!(q.info.has_where);
    }
}
