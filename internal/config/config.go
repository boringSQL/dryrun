package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/pkg/lint"
)

type (
	ProjectConfig struct {
		Project      *ProjectMeta             `toml:"project"`
		Default      *DefaultConfig           `toml:"default"`
		Profiles     map[string]ProfileConfig `toml:"profiles"`
		Conventions  *ConventionsConfig       `toml:"conventions"`
		Services     *ServicesConfig          `toml:"services"`
		RequireMasks *bool                    `toml:"require_masks"`
		Remotes      []RemoteConfig           `toml:"remote"`
	}

	// [[remote]] block; Ref is the registry base, e.g. ghcr.io/org/dryrun
	RemoteConfig struct {
		Name     string `toml:"name"`
		Type     string `toml:"type"`
		Ref      string `toml:"ref"`
		TokenEnv string `toml:"token_env"`
		Auth     string `toml:"auth"` // "" docker creds; "gcp" GAR/GCR via ADC
		Default  bool   `toml:"default"`
	}

	ProjectMeta struct {
		ID *string `toml:"id"`
	}

	ServicesConfig struct {
		PgMustardAPIKey *string `toml:"pgmustard_api_key"`
	}

	DefaultConfig struct {
		Profile *string `toml:"profile"`
	}

	ProfileConfig struct {
		DBURL        *string  `toml:"db_url"`
		SchemaFile   *string  `toml:"schema_file"`
		DatabaseID   *string  `toml:"database_id"`
		MasksFile    *string  `toml:"masks_file"`
		MaskPolicies []string `toml:"mask_policies"`
		Remote       *string  `toml:"remote"`
		Stream       *string  `toml:"stream"`
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
		Name           string
		DBURL          *string
		SchemaFile     *string
		ProjectID      history.ProjectId
		DatabaseID     *history.DatabaseId
		MasksFile      *string
		MaskPolicies   []string
		Remote         *string
		streamOverride *string
	}
)

// Stream is the remote repo-path suffix: explicit override or the default
// <project>/<database> (shared with BundleDir so remote/local layouts match).
func (r *ResolvedProfile) Stream() string {
	if r.streamOverride != nil && *r.streamOverride != "" {
		return *r.streamOverride
	}
	return history.StreamSuffix(r.SnapshotKey())
}

// ResolveRemote: by name, else the sole remote, else the sole default.
func (c *ProjectConfig) ResolveRemote(name string) (*RemoteConfig, error) {
	if name != "" {
		for i := range c.Remotes {
			if c.Remotes[i].Name == name {
				return &c.Remotes[i], nil
			}
		}
		return nil, fmt.Errorf("remote %q not found in dryrun.toml", name)
	}
	switch len(c.Remotes) {
	case 0:
		return nil, fmt.Errorf("no remotes configured in dryrun.toml")
	case 1:
		return &c.Remotes[0], nil
	}
	var def *RemoteConfig
	for i := range c.Remotes {
		if c.Remotes[i].Default {
			if def != nil {
				return nil, fmt.Errorf("multiple default remotes; pass --remote")
			}
			def = &c.Remotes[i]
		}
	}
	if def == nil {
		return nil, fmt.Errorf("multiple remotes and no default; pass --remote")
	}
	return def, nil
}

func (r *ResolvedProfile) SnapshotKey() history.SnapshotKey {
	did := history.DatabaseId(string(r.ProjectID))
	if r.DatabaseID != nil {
		did = *r.DatabaseID
	}
	return history.SnapshotKey{ProjectID: r.ProjectID, DatabaseID: did}
}

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
	projectID := c.ProjectID(projectRoot)

	if cliDB != nil {
		expanded := ExpandEnvVars(*cliDB)
		return &ResolvedProfile{Name: "<cli>", DBURL: &expanded, ProjectID: projectID}, nil
	}
	if cliSchema != nil {
		return &ResolvedProfile{Name: "<cli>", SchemaFile: cliSchema, ProjectID: projectID}, nil
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
		return resolveProfileConfig(profileName, &profile, projectRoot, projectID), nil
	}

	// one profile defined, treat it as the default
	if len(c.Profiles) == 1 {
		for name, profile := range c.Profiles {
			return resolveProfileConfig(name, &profile, projectRoot, projectID), nil
		}
	}

	autoSchema := filepath.Join(projectRoot, ".dryrun", "schema.json")
	if info, err := os.Stat(autoSchema); err == nil && !info.IsDir() {
		return &ResolvedProfile{Name: "<auto>", SchemaFile: &autoSchema, ProjectID: projectID}, nil
	}

	return nil, fmt.Errorf("no profile found: specify --profile, set PROFILE, " +
		"configure [default].profile in dryrun.toml, " +
		"or place a schema at .dryrun/schema.json")
}

func (c *ProjectConfig) ProjectID(projectRoot string) history.ProjectId {
	if c.Project != nil && c.Project.ID != nil && *c.Project.ID != "" {
		return history.ProjectId(*c.Project.ID)
	}
	base := filepath.Base(projectRoot)
	if base == "" || base == "." || base == string(filepath.Separator) {
		return history.ProjectId("default")
	}
	return history.ProjectId(base)
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

func resolveProfileConfig(name string, profile *ProfileConfig, projectRoot string, projectID history.ProjectId) *ResolvedProfile {
	rp := &ResolvedProfile{Name: name, ProjectID: projectID}
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
	did := name
	if profile.DatabaseID != nil && *profile.DatabaseID != "" {
		did = *profile.DatabaseID
	}
	d := history.DatabaseId(did)
	rp.DatabaseID = &d
	if profile.MasksFile != nil {
		p := *profile.MasksFile
		if !filepath.IsAbs(p) {
			p = filepath.Join(projectRoot, p)
		}
		rp.MasksFile = &p
	}
	rp.MaskPolicies = profile.MaskPolicies
	rp.Remote = profile.Remote
	rp.streamOverride = profile.Stream
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
