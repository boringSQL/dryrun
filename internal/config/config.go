package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"

	"github.com/boringsql/dryrun/internal/lint"
)

type (
	ProjectConfig struct {
		Default     *DefaultConfig           `toml:"default"`
		Profiles    map[string]ProfileConfig `toml:"profiles"`
		Conventions *ConventionsConfig       `toml:"conventions"`
		Services    *ServicesConfig          `toml:"services"`
	}

	ServicesConfig struct {
		PgMustardAPIKey *string `toml:"pgmustard_api_key"`
	}

	DefaultConfig struct {
		Profile *string `toml:"profile"`
	}

	ProfileConfig struct {
		DBURL      *string `toml:"db_url"`
		SchemaFile *string `toml:"schema_file"`
	}

	ConventionsConfig struct {
		TableName             *string               `toml:"table_name"`
		ColumnName            *string               `toml:"column_name"`
		PKType                *string               `toml:"pk_type"`
		FKPattern             *string               `toml:"fk_pattern"`
		IndexPattern          *string               `toml:"index_pattern"`
		RequireTimestamps     *bool                 `toml:"require_timestamps"`
		TimestampType         *string               `toml:"timestamp_type"`
		PreferTextOverVarchar *bool                 `toml:"prefer_text_over_varchar"`
		DisabledRules         *DisabledRulesConfig  `toml:"disabled_rules"`
		Custom                *CustomPatternsConfig `toml:"custom"`
	}

	DisabledRulesConfig struct {
		Rules []string `toml:"rules"`
	}

	CustomPatternsConfig struct {
		TableNameRegex  *string `toml:"table_name_regex"`
		ColumnNameRegex *string `toml:"column_name_regex"`
	}

	ResolvedProfile struct {
		Name       string
		DBURL      *string
		SchemaFile *string
	}
)

func Parse(content string) (*ProjectConfig, error) {
	var cfg ProjectConfig
	if _, err := toml.Decode(content, &cfg); err != nil {
		return nil, fmt.Errorf("invalid dryrun.toml: %w", err)
	}
	if cfg.Profiles == nil {
		cfg.Profiles = make(map[string]ProfileConfig)
	}
	return &cfg, nil
}

func Load(path string) (*ProjectConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", path, err)
	}
	return Parse(string(data))
}

// Walks up from startDir looking for dryrun.toml, stops at .git
func Discover(startDir string) (string, *ProjectConfig, bool) {
	dir := startDir
	for {
		candidate := filepath.Join(dir, "dryrun.toml")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			cfg, err := Load(candidate)
			if err == nil {
				return candidate, cfg, true
			}
		}
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return "", nil, false
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", nil, false
		}
		dir = parent
	}
}

// Priority: CLI flags > env var > config default > auto-discovery
func (c *ProjectConfig) ResolveProfile(cliDB, cliSchema, cliProfile *string, projectRoot string) (*ResolvedProfile, error) {
	if cliDB != nil {
		expanded := ExpandEnvVars(*cliDB)
		return &ResolvedProfile{Name: "<cli>", DBURL: &expanded}, nil
	}
	if cliSchema != nil {
		return &ResolvedProfile{Name: "<cli>", SchemaFile: cliSchema}, nil
	}

	var profileName string
	if cliProfile != nil {
		profileName = *cliProfile
	} else if env := os.Getenv("PROFILE"); env != "" {
		profileName = env
	} else if c.Default != nil && c.Default.Profile != nil {
		profileName = *c.Default.Profile
	}

	if profileName != "" {
		profile, ok := c.Profiles[profileName]
		if !ok {
			return nil, fmt.Errorf("profile '%s' not found in dryrun.toml", profileName)
		}
		return resolveProfileConfig(profileName, &profile, projectRoot), nil
	}

	autoSchema := filepath.Join(projectRoot, ".dryrun", "schema.json")
	if info, err := os.Stat(autoSchema); err == nil && !info.IsDir() {
		return &ResolvedProfile{Name: "<auto>", SchemaFile: &autoSchema}, nil
	}

	return nil, fmt.Errorf("no profile found: specify --profile, set PROFILE, " +
		"configure [default].profile in dryrun.toml, " +
		"or place a schema at .dryrun/schema.json")
}

func (c *ProjectConfig) LintConfig() lint.Config {
	cfg := lint.DefaultConfig()

	conv := c.Conventions
	if conv == nil {
		return cfg
	}

	if conv.TableName != nil {
		cfg.TableNameStyle = *conv.TableName
	}
	if conv.ColumnName != nil {
		cfg.ColumnNameStyle = *conv.ColumnName
	}
	if conv.PKType != nil {
		cfg.PKType = *conv.PKType
	}
	if conv.FKPattern != nil {
		cfg.FKPattern = *conv.FKPattern
	}
	if conv.IndexPattern != nil {
		cfg.IndexPattern = *conv.IndexPattern
	}
	if conv.RequireTimestamps != nil {
		cfg.RequireTimestamps = *conv.RequireTimestamps
	}
	if conv.TimestampType != nil {
		cfg.TimestampType = *conv.TimestampType
	}
	if conv.PreferTextOverVarchar != nil {
		cfg.PreferTextOverVarchar = *conv.PreferTextOverVarchar
	}
	if conv.DisabledRules != nil {
		cfg.DisabledRules = conv.DisabledRules.Rules
	}
	if conv.Custom != nil {
		cfg.TableNameRegex = conv.Custom.TableNameRegex
		cfg.ColumnNameRegex = conv.Custom.ColumnNameRegex
	}

	return cfg
}

func resolveProfileConfig(name string, profile *ProfileConfig, projectRoot string) *ResolvedProfile {
	rp := &ResolvedProfile{Name: name}
	if profile.DBURL != nil {
		expanded := ExpandEnvVars(*profile.DBURL)
		rp.DBURL = &expanded
	}
	if profile.SchemaFile != nil {
		p := *profile.SchemaFile
		if !filepath.IsAbs(p) {
			p = filepath.Join(projectRoot, p)
		}
		rp.SchemaFile = &p
	}
	return rp
}

// Expands ${VAR} from environment
func ExpandEnvVars(input string) string {
	result := input
	for {
		start := strings.Index(result, "${")
		if start < 0 {
			break
		}
		end := strings.Index(result[start:], "}")
		if end < 0 {
			break
		}
		end += start
		varName := result[start+2 : end]
		value := os.Getenv(varName)
		result = result[:start] + value + result[end+1:]
	}
	return result
}
