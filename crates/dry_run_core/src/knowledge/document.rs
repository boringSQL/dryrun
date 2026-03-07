use serde::{Deserialize, Serialize};

use crate::version::PgVersion;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeCategory {
    MigrationSafety,
    IndexDecisions,
    SchemaConventions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeDoc {
    pub name: String,
    pub category: KnowledgeCategory,
    pub title: String,
    pub keywords: Vec<String>,
    pub min_pg_version: Option<u32>,
    pub max_pg_version: Option<u32>,
    pub safety: String,
    pub body: String,
}

impl KnowledgeDoc {
    pub fn parse(name: &str, category: KnowledgeCategory, content: &str) -> Option<Self> {
        let content = content.trim();
        if !content.starts_with("---") {
            return None;
        }

        let end = content[3..].find("---")?;
        let frontmatter = &content[3..3 + end];
        let body = content[3 + end + 3..].trim().to_string();

        let mut title = String::new();
        let mut keywords = Vec::new();
        let mut min_pg_version = None;
        let mut max_pg_version = None;
        let mut safety = "caution".to_string();

        for line in frontmatter.lines() {
            let line = line.trim();
            if let Some((key, value)) = line.split_once(':') {
                let key = key.trim();
                let value = value.trim();
                match key {
                    "title" => title = value.to_string(),
                    "keywords" => {
                        keywords = value.split(',').map(|s| s.trim().to_lowercase()).collect();
                    }
                    "min_pg_version" => min_pg_version = value.parse().ok(),
                    "max_pg_version" => max_pg_version = value.parse().ok(),
                    "safety" => safety = value.to_string(),
                    _ => {}
                }
            }
        }

        Some(KnowledgeDoc {
            name: name.to_string(),
            category,
            title,
            keywords,
            min_pg_version,
            max_pg_version,
            safety,
            body,
        })
    }

    pub fn applies_to_version(&self, ver: &PgVersion) -> bool {
        if let Some(min) = self.min_pg_version {
            if ver.major < min {
                return false;
            }
        }
        if let Some(max) = self.max_pg_version {
            if ver.major > max {
                return false;
            }
        }
        true
    }
}
