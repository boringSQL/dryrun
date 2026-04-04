use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::lint::{LintConfig, Severity};

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

    #[serde(default)]
    pub services: Option<ServicesConfig>,
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
    pub min_severity: Option<String>,

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServicesConfig {
    pub pgmustard_api_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedProfile {
    pub name: String,
    pub db_url: Option<String>,
    pub schema_file: Option<PathBuf>,
}

impl ProjectConfig {
    pub fn parse(content: &str) -> Result<Self> {
        toml::from_str(content).map_err(|e| Error::Config(format!("invalid dryrun.toml: {e}")))
    }

    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("cannot read {}: {e}", path.display())))?;
        Self::parse(&content)
    }

    pub fn discover(start_dir: &Path) -> Option<(PathBuf, Self)> {
        let mut dir = start_dir.to_path_buf();
        loop {
            let candidate = dir.join("dryrun.toml");
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
    // 3. PROFILE env var
    // 4. [default].profile in toml
    // 5. auto-discovery of .dryrun/schema.json
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
            .or_else(|| std::env::var("PROFILE").ok())
            .or_else(|| self.default.as_ref().and_then(|d| d.profile.clone()));

        if let Some(name) = profile_name {
            let profile = self.profiles.get(&name).ok_or_else(|| {
                Error::Config(format!("profile '{name}' not found in dryrun.toml"))
            })?;
            return Ok(resolve_profile_config(&name, profile, project_root));
        }

        let auto_schema = project_root.join(".dryrun/schema.json");
        if auto_schema.is_file() {
            return Ok(ResolvedProfile {
                name: "<auto>".into(),
                db_url: None,
                schema_file: Some(auto_schema),
            });
        }

        Err(Error::Config(
            "no profile found: specify --profile, set PROFILE, \
             configure [default].profile in dryrun.toml, \
             or place a schema at .dryrun/schema.json"
                .into(),
        ))
    }

    pub fn pgmustard_api_key(&self) -> Option<String> {
        self.services
            .as_ref()
            .and_then(|s| s.pgmustard_api_key.as_ref())
            .map(|k| expand_env_vars(k))
            .filter(|k| !k.is_empty())
            .or_else(|| std::env::var("PGMUSTARD_API_KEY").ok())
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

        if let Some(v) = &conv.min_severity {
            match v.as_str() {
                "info" => config.min_severity = Severity::Info,
                "warning" => config.min_severity = Severity::Warning,
                "error" => config.min_severity = Severity::Error,
                _ => {} // keep default
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_config() {
        let toml = r#"
[default]
profile = "production"

[profiles.development]
db_url = "${DEV_DATABASE_URL}"

[profiles.staging]
schema_file = ".dryrun/staging-schema.json"

[profiles.production]
schema_file = ".dryrun/schema.json"

[conventions]
table_name = "snake_singular"
column_name = "snake_case"
pk_type = "bigint_identity"
require_timestamps = true
prefer_text_over_varchar = true

[conventions.disabled_rules]
rules = ["naming/table_style"]

[conventions.custom]
table_name_regex = "^[a-z][a-z0-9_]*$"
"#;

        let config = ProjectConfig::parse(toml).unwrap();
        assert_eq!(config.default.as_ref().unwrap().profile.as_deref(), Some("production"));
        assert_eq!(config.profiles.len(), 3);
        assert!(config.profiles.contains_key("development"));
        assert!(config.profiles.contains_key("staging"));
        assert!(config.profiles.contains_key("production"));

        let conv = config.conventions.as_ref().unwrap();
        assert_eq!(conv.table_name.as_deref(), Some("snake_singular"));
        assert_eq!(conv.require_timestamps, Some(true));

        let disabled = conv.disabled_rules.as_ref().unwrap();
        assert_eq!(disabled.rules, vec!["naming/table_style"]);
    }

    #[test]
    fn parse_empty_config() {
        let config = ProjectConfig::parse("").unwrap();
        assert!(config.default.is_none());
        assert!(config.profiles.is_empty());
        assert!(config.conventions.is_none());
    }

    #[test]
    fn parse_invalid_config() {
        let result = ProjectConfig::parse("not valid toml [[[");
        assert!(result.is_err());
    }

    #[test]
    fn expand_env_vars_basic() {
        // SAFETY: test-only, single-threaded test runner
        unsafe { std::env::set_var("DRYRUN_TEST_VAR", "hello") };
        assert_eq!(expand_env_vars("${DRYRUN_TEST_VAR}"), "hello");
        assert_eq!(
            expand_env_vars("postgres://${DRYRUN_TEST_VAR}:5432/db"),
            "postgres://hello:5432/db"
        );
        unsafe { std::env::remove_var("DRYRUN_TEST_VAR") };
    }

    #[test]
    fn expand_env_vars_missing() {
        // SAFETY: test-only, single-threaded test runner
        unsafe { std::env::remove_var("DRYRUN_MISSING_VAR") };
        assert_eq!(expand_env_vars("${DRYRUN_MISSING_VAR}"), "");
    }

    #[test]
    fn expand_env_vars_no_vars() {
        assert_eq!(expand_env_vars("just a string"), "just a string");
    }

    #[test]
    fn lint_config_from_conventions() {
        let toml = r#"
[conventions]
table_name = "snake_plural"
prefer_text_over_varchar = false

[conventions.disabled_rules]
rules = ["pk/exists"]
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        let lint = config.lint_config();
        assert_eq!(lint.table_name_style, "snake_plural");
        assert!(!lint.prefer_text_over_varchar);
        assert_eq!(lint.disabled_rules, vec!["pk/exists"]);
    }

    #[test]
    fn lint_config_defaults_without_conventions() {
        let config = ProjectConfig::parse("").unwrap();
        let lint = config.lint_config();
        assert_eq!(lint.table_name_style, "auto");
        assert!(lint.prefer_text_over_varchar);
    }

    #[test]
    fn resolve_profile_cli_db_wins() {
        let config = ProjectConfig::parse("[default]\nprofile = \"prod\"").unwrap();
        let resolved = config
            .resolve_profile(Some("postgres://localhost/test"), None, None, Path::new("/tmp"))
            .unwrap();
        assert_eq!(resolved.name, "<cli>");
        assert_eq!(resolved.db_url.as_deref(), Some("postgres://localhost/test"));
    }

    #[test]
    fn resolve_profile_by_name() {
        let toml = r#"
[profiles.staging]
schema_file = ".dryrun/staging.json"
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        let resolved = config
            .resolve_profile(None, None, Some("staging"), Path::new("/project"))
            .unwrap();
        assert_eq!(resolved.name, "staging");
        assert_eq!(resolved.schema_file.unwrap(), PathBuf::from("/project/.dryrun/staging.json"));
    }

    #[test]
    fn discover_returns_none_for_nonexistent() {
        let result = ProjectConfig::discover(Path::new("/nonexistent/path/that/doesnt/exist"));
        assert!(result.is_none());
    }
}
