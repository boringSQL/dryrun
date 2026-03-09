use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::lint::LintConfig;

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    url: String,
}

impl ConnectionConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectConfig {
    #[serde(default)]
    pub default: Option<DefaultConfig>,

    #[serde(default)]
    pub profiles: HashMap<String, ProfileConfig>,

    #[serde(default)]
    pub conventions: Option<ConventionsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultConfig {
    pub profile: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileConfig {
    pub db_url: Option<String>,
    pub schema_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConventionsConfig {
    pub table_name: Option<String>,
    pub column_name: Option<String>,
    pub pk_type: Option<String>,
    pub fk_pattern: Option<String>,
    pub index_pattern: Option<String>,
    pub require_timestamps: Option<bool>,
    pub timestamp_type: Option<String>,
    pub prefer_text_over_varchar: Option<bool>,

    #[serde(default)]
    pub disabled_rules: Option<DisabledRulesConfig>,

    #[serde(default)]
    pub custom: Option<CustomPatternsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisabledRulesConfig {
    #[serde(default)]
    pub rules: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomPatternsConfig {
    pub table_name_regex: Option<String>,
    pub column_name_regex: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedProfile {
    pub name: String,
    pub db_url: Option<String>,
    pub schema_file: Option<PathBuf>,
}

impl ProjectConfig {
    pub fn parse(content: &str) -> Result<Self> {
        toml::from_str(content).map_err(|e| Error::Config(format!("invalid dry_run.toml: {e}")))
    }

    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("cannot read {}: {e}", path.display())))?;
        Self::parse(&content)
    }

    pub fn discover(start_dir: &Path) -> Option<(PathBuf, Self)> {
        let mut dir = start_dir.to_path_buf();
        loop {
            let candidate = dir.join("dry_run.toml");
            if candidate.is_file() {
                if let Ok(config) = Self::load(&candidate) {
                    return Some((candidate, config));
                }
            }
            if dir.join(".git").exists() {
                return None;
            }
            if !dir.pop() {
                return None;
            }
        }
    }

    // resolution order:
    // 1. explicit cli_db or cli_schema (CLI flags)
    // 2. cli_profile flag (--profile)
    // 3. DRY_RUN_PROFILE env var
    // 4. [default].profile in toml
    // 5. auto-discovery of .dry_run/schema.json
    pub fn resolve_profile(
        &self,
        cli_db: Option<&str>,
        cli_schema: Option<&Path>,
        cli_profile: Option<&str>,
        project_root: &Path,
    ) -> Result<ResolvedProfile> {
        if let Some(db) = cli_db {
            return Ok(ResolvedProfile {
                name: "<cli>".into(),
                db_url: Some(expand_env_vars(db)),
                schema_file: None,
            });
        }
        if let Some(schema) = cli_schema {
            return Ok(ResolvedProfile {
                name: "<cli>".into(),
                db_url: None,
                schema_file: Some(schema.to_path_buf()),
            });
        }

        let profile_name = cli_profile
            .map(|s| s.to_string())
            .or_else(|| std::env::var("DRY_RUN_PROFILE").ok())
            .or_else(|| self.default.as_ref().and_then(|d| d.profile.clone()));

        if let Some(name) = profile_name {
            let profile = self.profiles.get(&name).ok_or_else(|| {
                Error::Config(format!("profile '{name}' not found in dry_run.toml"))
            })?;
            return Ok(resolve_profile_config(&name, profile, project_root));
        }

        let auto_schema = project_root.join(".dry_run/schema.json");
        if auto_schema.is_file() {
            return Ok(ResolvedProfile {
                name: "<auto>".into(),
                db_url: None,
                schema_file: Some(auto_schema),
            });
        }

        Err(Error::Config(
            "no profile found: specify --profile, set DRY_RUN_PROFILE, \
             configure [default].profile in dry_run.toml, \
             or place a schema at .dry_run/schema.json"
                .into(),
        ))
    }

    pub fn lint_config(&self) -> LintConfig {
        let Some(conv) = &self.conventions else {
            return LintConfig::default();
        };

        let mut config = LintConfig::default();

        if let Some(v) = &conv.table_name {
            config.table_name_style = v.clone();
        }
        if let Some(v) = &conv.column_name {
            config.column_name_style = v.clone();
        }
        if let Some(v) = &conv.pk_type {
            config.pk_type = v.clone();
        }
        if let Some(v) = &conv.fk_pattern {
            config.fk_pattern = v.clone();
        }
        if let Some(v) = &conv.index_pattern {
            config.index_pattern = v.clone();
        }
        if let Some(v) = conv.require_timestamps {
            config.require_timestamps = v;
        }
        if let Some(v) = &conv.timestamp_type {
            config.timestamp_type = v.clone();
        }
        if let Some(v) = conv.prefer_text_over_varchar {
            config.prefer_text_over_varchar = v;
        }

        if let Some(disabled) = &conv.disabled_rules {
            config.disabled_rules = disabled.rules.clone();
        }

        if let Some(custom) = &conv.custom {
            config.table_name_regex = custom.table_name_regex.clone();
            config.column_name_regex = custom.column_name_regex.clone();
        }

        config
    }
}

fn resolve_profile_config(
    name: &str,
    profile: &ProfileConfig,
    project_root: &Path,
) -> ResolvedProfile {
    let db_url = profile.db_url.as_ref().map(|u| expand_env_vars(u));
    let schema_file = profile.schema_file.as_ref().map(|p| {
        let path = PathBuf::from(p);
        if path.is_absolute() {
            path
        } else {
            project_root.join(path)
        }
    });

    ResolvedProfile {
        name: name.to_string(),
        db_url,
        schema_file,
    }
}

pub fn expand_env_vars(input: &str) -> String {
    let mut result = input.to_string();
    loop {
        let Some(start) = result.find("${") else {
            break;
        };
        let Some(end) = result[start..].find('}') else {
            break;
        };
        let end = start + end;
        let var_name = &result[start + 2..end];
        let value = std::env::var(var_name).unwrap_or_default();
        result = format!("{}{}{}", &result[..start], value, &result[end + 1..]);
    }
    result
}
