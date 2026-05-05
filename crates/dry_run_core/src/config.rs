use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::history::{DatabaseId, ProjectId};
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
    pub project: Option<ProjectMeta>,

    #[serde(default)]
    pub default: Option<DefaultConfig>,

    #[serde(default)]
    pub profiles: HashMap<String, ProfileConfig>,

    #[serde(default)]
    pub conventions: Option<ConventionsConfig>,

    #[serde(default)]
    pub services: Option<ServicesConfig>,

    #[serde(default)]
    pub telemetry_enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectMeta {
    #[serde(default)]
    pub id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultConfig {
    pub profile: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileConfig {
    pub db_url: Option<String>,
    pub schema_file: Option<String>,
    #[serde(default)]
    pub database_id: Option<String>,
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
    pub project_id: ProjectId,
    pub database_id: Option<DatabaseId>,
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
            if candidate.is_file()
                && let Ok(config) = Self::load(&candidate)
            {
                return Some((candidate, config));
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
    // 1. cli_profile flag (--profile)
    // 2. PROFILE env var
    // 3. [default].profile in toml
    // 4. auto-discovery of .dryrun/schema.json
    //
    // CLI flags (cli_db, cli_schema) override the resolved profile's matching
    // fields for the current invocation. So `--profile billing --db $OTHER`
    // connects to $OTHER but keeps billing's database_id for snapshot keying.
    pub fn resolve_profile(
        &self,
        cli_db: Option<&str>,
        cli_schema: Option<&Path>,
        cli_profile: Option<&str>,
        project_root: &Path,
    ) -> Result<ResolvedProfile> {
        let project_id = self.project_id(project_root);

        let explicit_profile = cli_profile
            .map(|s| s.to_string())
            .or_else(|| std::env::var("PROFILE").ok());
        let default_profile = self.default.as_ref().and_then(|d| d.profile.clone());
        let profile_name = explicit_profile.clone().or(default_profile);

        if let Some(name) = profile_name {
            if let Some(profile) = self.profiles.get(&name) {
                let mut resolved = resolve_profile_config(&name, profile, project_root, project_id);
                if let Some(db) = cli_db {
                    resolved.db_url = Some(expand_env_vars(db));
                }
                if let Some(schema) = cli_schema {
                    resolved.schema_file = Some(schema.to_path_buf());
                }
                return Ok(resolved);
            }

            // Missing profile causes error.
            if explicit_profile.is_some() || (cli_db.is_none() && cli_schema.is_none()) {
                return Err(Error::Config(format!(
                    "profile '{name}' not found in dryrun.toml"
                )));
            }
        }

        // No profile resolved: fall back to <cli> or <auto>.
        if let Some(db) = cli_db {
            return Ok(ResolvedProfile {
                name: "<cli>".into(),
                db_url: Some(expand_env_vars(db)),
                schema_file: None,
                project_id,
                database_id: None,
            });
        }
        if let Some(schema) = cli_schema {
            return Ok(ResolvedProfile {
                name: "<cli>".into(),
                db_url: None,
                schema_file: Some(schema.to_path_buf()),
                project_id,
                database_id: None,
            });
        }

        let auto_schema = project_root.join(".dryrun/schema.json");
        if auto_schema.is_file() {
            return Ok(ResolvedProfile {
                name: "<auto>".into(),
                db_url: None,
                schema_file: Some(auto_schema),
                project_id,
                database_id: None,
            });
        }

        Err(Error::Config(
            "no profile found: specify --profile, set PROFILE, \
             configure [default].profile in dryrun.toml, \
             or place a schema at .dryrun/schema.json"
                .into(),
        ))
    }

    pub fn project_id(&self, project_root: &Path) -> ProjectId {
        if let Some(meta) = &self.project
            && let Some(id) = &meta.id
            && !id.is_empty()
        {
            return ProjectId(id.clone());
        }
        default_project_id(project_root)
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
    project_id: ProjectId,
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
    let database_id = Some(DatabaseId(
        profile
            .database_id
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| name.to_string()),
    ));

    ResolvedProfile {
        name: name.to_string(),
        db_url,
        schema_file,
        project_id,
        database_id,
    }
}

fn default_project_id(project_root: &Path) -> ProjectId {
    project_root
        .file_name()
        .map(|n| ProjectId(n.to_string_lossy().into_owned()))
        .unwrap_or_else(|| ProjectId("default".into()))
}

pub fn expand_env_vars(input: &str) -> String {
    let mut result = input.to_string();
    while let Some(start) = result.find("${") {
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
        assert_eq!(
            config.default.as_ref().unwrap().profile.as_deref(),
            Some("production")
        );
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
            .resolve_profile(
                Some("postgres://localhost/test"),
                None,
                None,
                Path::new("/tmp"),
            )
            .unwrap();
        assert_eq!(resolved.name, "<cli>");
        assert_eq!(
            resolved.db_url.as_deref(),
            Some("postgres://localhost/test")
        );
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
        assert_eq!(
            resolved.schema_file.unwrap(),
            PathBuf::from("/project/.dryrun/staging.json")
        );
    }

    #[test]
    fn discover_returns_none_for_nonexistent() {
        let result = ProjectConfig::discover(Path::new("/nonexistent/path/that/doesnt/exist"));
        assert!(result.is_none());
    }

    #[test]
    fn parse_telemetry_enabled() {
        let config = ProjectConfig::parse("telemetry_enabled = true\n").unwrap();
        assert_eq!(config.telemetry_enabled, Some(true));

        let config = ProjectConfig::parse("telemetry_enabled = false\n").unwrap();
        assert_eq!(config.telemetry_enabled, Some(false));

        let config = ProjectConfig::parse("").unwrap();
        assert_eq!(config.telemetry_enabled, None);
    }

    #[test]
    fn parse_with_project_section() {
        let toml = r#"
[project]
id = "myapp"

[profiles.dev]
schema_file = ".dryrun/schema.json"
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        assert_eq!(config.project.unwrap().id.as_deref(), Some("myapp"));
    }

    #[test]
    fn parse_with_database_id_per_profile() {
        let toml = r#"
[profiles.prod-auth]
schema_file = ".dryrun/auth.json"
database_id = "auth"

[profiles.prod-billing]
schema_file = ".dryrun/billing.json"
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        assert_eq!(
            config.profiles["prod-auth"].database_id.as_deref(),
            Some("auth")
        );
        assert!(config.profiles["prod-billing"].database_id.is_none());
    }

    #[test]
    fn resolve_profile_uses_configured_project_id() {
        let toml = r#"
[project]
id = "myapp"

[profiles.dev]
schema_file = ".dryrun/schema.json"
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        let resolved = config
            .resolve_profile(None, None, Some("dev"), Path::new("/tmp/some-folder"))
            .unwrap();
        assert_eq!(resolved.project_id.0, "myapp");
    }

    #[test]
    fn resolve_profile_falls_back_to_cwd_basename() {
        let toml = r#"
[profiles.dev]
schema_file = ".dryrun/schema.json"
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        let resolved = config
            .resolve_profile(None, None, Some("dev"), Path::new("/tmp/test-myapp"))
            .unwrap();
        assert_eq!(resolved.project_id.0, "test-myapp");
    }

    #[test]
    fn resolve_profile_database_id_defaults_to_profile_name() {
        let toml = r#"
[profiles.staging]
schema_file = ".dryrun/staging.json"
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        let resolved = config
            .resolve_profile(None, None, Some("staging"), Path::new("/project"))
            .unwrap();
        assert_eq!(
            resolved.database_id.as_ref().map(|d| d.0.as_str()),
            Some("staging")
        );
    }

    #[test]
    fn resolve_profile_database_id_from_config() {
        let toml = r#"
[profiles.prod-auth]
schema_file = ".dryrun/auth.json"
database_id = "auth"
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        let resolved = config
            .resolve_profile(None, None, Some("prod-auth"), Path::new("/project"))
            .unwrap();
        assert_eq!(
            resolved.database_id.as_ref().map(|d| d.0.as_str()),
            Some("auth")
        );
    }

    #[test]
    fn cli_profile_has_no_database_id() {
        let config = ProjectConfig::parse("").unwrap();
        let resolved = config
            .resolve_profile(
                Some("postgres://localhost/test"),
                None,
                None,
                Path::new("/tmp/myproj"),
            )
            .unwrap();
        assert_eq!(resolved.name, "<cli>");
        assert!(resolved.database_id.is_none());
        assert_eq!(resolved.project_id.0, "myproj");
    }

    #[test]
    fn cli_db_overrides_profile_db_url_keeps_database_id() {
        let toml = r#"
[profiles.billing]
db_url = "postgres://prod/billing"
database_id = "billing"
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        let resolved = config
            .resolve_profile(
                Some("postgres://localhost/other"),
                None,
                Some("billing"),
                Path::new("/project"),
            )
            .unwrap();
        assert_eq!(resolved.name, "billing");
        assert_eq!(
            resolved.db_url.as_deref(),
            Some("postgres://localhost/other")
        );
        assert_eq!(
            resolved.database_id.as_ref().map(|d| d.0.as_str()),
            Some("billing")
        );
    }

    #[test]
    fn cli_schema_overrides_profile_schema_file_keeps_database_id() {
        let toml = r#"
[profiles.staging]
schema_file = ".dryrun/staging.json"
database_id = "stg"
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        let override_path = PathBuf::from("/tmp/other-schema.json");
        let resolved = config
            .resolve_profile(
                None,
                Some(&override_path),
                Some("staging"),
                Path::new("/project"),
            )
            .unwrap();
        assert_eq!(resolved.name, "staging");
        assert_eq!(
            resolved.schema_file.as_deref(),
            Some(override_path.as_path())
        );
        assert_eq!(
            resolved.database_id.as_ref().map(|d| d.0.as_str()),
            Some("stg")
        );
    }

    #[test]
    fn explicit_profile_missing_errors() {
        let config = ProjectConfig::parse("").unwrap();
        let result = config.resolve_profile(None, None, Some("nope"), Path::new("/tmp"));
        let err = result.unwrap_err().to_string();
        assert!(err.contains("'nope'"), "got: {err}");
    }

    #[test]
    fn default_profile_missing_with_cli_db_falls_back_to_cli() {
        let config = ProjectConfig::parse("[default]\nprofile = \"prod\"").unwrap();
        let resolved = config
            .resolve_profile(
                Some("postgres://localhost/x"),
                None,
                None,
                Path::new("/tmp"),
            )
            .unwrap();
        assert_eq!(resolved.name, "<cli>");
        assert!(resolved.database_id.is_none());
    }

    #[test]
    fn default_profile_missing_without_cli_args_errors() {
        let config = ProjectConfig::parse("[default]\nprofile = \"missing\"").unwrap();
        let result = config.resolve_profile(None, None, None, Path::new("/tmp"));
        let err = result.unwrap_err().to_string();
        assert!(err.contains("'missing'"), "got: {err}");
    }

    #[test]
    fn project_id_falls_back_to_default_for_root_path() {
        let config = ProjectConfig::parse("").unwrap();
        // root path has no file_name; falls back to "default"
        assert_eq!(config.project_id(Path::new("/")).0, "default");
    }

    #[test]
    fn explicit_profile_overrides_default_profile() {
        let toml = r#"
[default]
profile = "prod"

[profiles.prod]
schema_file = "prod.json"

[profiles.dev]
schema_file = "dev.json"
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        let resolved = config
            .resolve_profile(None, None, Some("dev"), Path::new("/p"))
            .unwrap();
        assert_eq!(resolved.name, "dev");
        assert_eq!(resolved.schema_file.unwrap(), PathBuf::from("/p/dev.json"));
    }

    #[test]
    fn resolve_profile_absolute_schema_path_kept_as_is() {
        let toml = r#"
[profiles.dev]
schema_file = "/abs/schema.json"
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        let resolved = config
            .resolve_profile(None, None, Some("dev"), Path::new("/project"))
            .unwrap();
        assert_eq!(
            resolved.schema_file.unwrap(),
            PathBuf::from("/abs/schema.json")
        );
    }

    #[test]
    fn resolve_profile_empty_database_id_falls_back_to_profile_name() {
        let toml = r#"
[profiles.staging]
schema_file = "x.json"
database_id = ""
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        let resolved = config
            .resolve_profile(None, None, Some("staging"), Path::new("/p"))
            .unwrap();
        assert_eq!(
            resolved.database_id.as_ref().map(|d| d.0.as_str()),
            Some("staging")
        );
    }

    #[test]
    fn resolve_profile_auto_discovers_schema_json() {
        let dir = tempfile::TempDir::new().unwrap();
        let dryrun_dir = dir.path().join(".dryrun");
        std::fs::create_dir_all(&dryrun_dir).unwrap();
        std::fs::write(dryrun_dir.join("schema.json"), "{}").unwrap();

        let config = ProjectConfig::parse("").unwrap();
        let resolved = config
            .resolve_profile(None, None, None, dir.path())
            .unwrap();
        assert_eq!(resolved.name, "<auto>");
        assert!(resolved.database_id.is_none());
        assert_eq!(
            resolved.schema_file.unwrap(),
            dir.path().join(".dryrun/schema.json")
        );
    }

    #[test]
    fn resolve_profile_cli_schema_without_profile_falls_back() {
        let config = ProjectConfig::parse("").unwrap();
        let p = PathBuf::from("/some/where.json");
        let resolved = config
            .resolve_profile(None, Some(&p), None, Path::new("/p"))
            .unwrap();
        assert_eq!(resolved.name, "<cli>");
        assert_eq!(resolved.schema_file.as_deref(), Some(p.as_path()));
        assert!(resolved.db_url.is_none());
    }

    #[test]
    fn resolve_profile_no_profile_no_schema_no_cli_errors() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = ProjectConfig::parse("").unwrap();
        let result = config.resolve_profile(None, None, None, dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn expand_env_vars_multiple_in_one_string() {
        // SAFETY: test-only, single-threaded test runner
        unsafe {
            std::env::set_var("DRYRUN_A", "alpha");
            std::env::set_var("DRYRUN_B", "beta");
        }
        assert_eq!(expand_env_vars("${DRYRUN_A}-${DRYRUN_B}"), "alpha-beta");
        unsafe {
            std::env::remove_var("DRYRUN_A");
            std::env::remove_var("DRYRUN_B");
        }
    }

    #[test]
    fn expand_env_vars_unterminated_brace_left_alone() {
        // no closing brace — should not loop forever, return as-is
        assert_eq!(expand_env_vars("foo ${UNCLOSED bar"), "foo ${UNCLOSED bar");
    }

    #[test]
    fn discover_finds_config_in_parent() {
        let dir = tempfile::TempDir::new().unwrap();
        // simulate repo root
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        std::fs::write(
            dir.path().join("dryrun.toml"),
            "[profiles.dev]\nschema_file = \"x.json\"\n",
        )
        .unwrap();

        let nested = dir.path().join("a").join("b");
        std::fs::create_dir_all(&nested).unwrap();
        let (path, config) = ProjectConfig::discover(&nested).unwrap();
        assert_eq!(path, dir.path().join("dryrun.toml"));
        assert!(config.profiles.contains_key("dev"));
    }

    #[test]
    fn discover_stops_at_git_root() {
        let dir = tempfile::TempDir::new().unwrap();
        // .git in inner dir, dryrun.toml only above it — discovery must NOT cross the boundary
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        std::fs::write(
            dir.path().parent().unwrap().join("dryrun.toml"),
            "[profiles.dev]\n",
        )
        .ok();
        // discovery from the git root should not find the parent's dryrun.toml
        assert!(ProjectConfig::discover(dir.path()).is_none());
    }

    #[test]
    fn pgmustard_api_key_from_config_expands_env() {
        // SAFETY: test-only, single-threaded test runner
        unsafe { std::env::set_var("DRYRUN_PGM_KEY", "sk-test-123") };
        let toml = r#"
[services]
pgmustard_api_key = "${DRYRUN_PGM_KEY}"
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        assert_eq!(config.pgmustard_api_key().as_deref(), Some("sk-test-123"));
        unsafe { std::env::remove_var("DRYRUN_PGM_KEY") };
    }

    #[test]
    fn pgmustard_api_key_empty_after_expansion_falls_through() {
        // SAFETY: test-only, single-threaded test runner
        unsafe {
            std::env::remove_var("DRYRUN_PGM_MISSING");
            std::env::remove_var("PGMUSTARD_API_KEY");
        }
        let toml = r#"
[services]
pgmustard_api_key = "${DRYRUN_PGM_MISSING}"
"#;
        let config = ProjectConfig::parse(toml).unwrap();
        assert!(config.pgmustard_api_key().is_none());
    }
}
