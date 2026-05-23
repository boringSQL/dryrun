package datamask

import (
	"fmt"

	"github.com/boringsql/dryrun/internal/history"
)

// Flags is the CLI-side masking surface; populated from cobra in main.
type Flags struct {
	MasksFile      string
	MaskPolicies   []string
	NoMasks        bool
	AllowMissingDB bool
}

// ProfileMasks is the masking slice of a resolved profile. Kept here so
// datamask stays independent of internal/config.
type ProfileMasks struct {
	File     string
	Policies []string
}

// MaskConfig is the pure overlay of flags onto profile. No filesystem reads.
type MaskConfig struct {
	Path           string
	Policies       []string
	Disabled       bool
	AllowMissingDB bool
}

// BuildConfig: CLI beats profile. Pure.
func BuildConfig(flags Flags, profile ProfileMasks) MaskConfig {
	cfg := MaskConfig{
		Path:           flags.MasksFile,
		Policies:       flags.MaskPolicies,
		Disabled:       flags.NoMasks,
		AllowMissingDB: flags.AllowMissingDB,
	}
	if cfg.Path == "" {
		cfg.Path = profile.File
	}
	if len(cfg.Policies) == 0 {
		cfg.Policies = profile.Policies
	}
	return cfg
}

// Refuses NullMasker silently: unmasked init permanently writes raw stats to history.db
func (c MaskConfig) BuildMasker(dbID history.DatabaseId, cwd string) (Masker, error) {
	if c.Disabled {
		return NullMasker{}, nil
	}
	path := c.Path
	if path == "" {
		if d, err := Discover(cwd); err == nil {
			path = d
		}
	}
	if path == "" {
		return nil, fmt.Errorf("no data-masking-policy.yml found; pass --masks-file=PATH, set masks_file in the profile, or --no-masks to capture without masking")
	}
	return Load(path, dbID, c.Policies, LoadOptions{AllowMissingDatabase: c.AllowMissingDB})
}
