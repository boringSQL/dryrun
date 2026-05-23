package datamask

import (
	"fmt"

	"github.com/boringsql/fixturize/masking"

	"github.com/boringsql/dryrun/internal/history"
)

// Flags is the CLI-side masking surface; populated from cobra in main.
type Flags struct {
	MasksFile    string
	MaskPolicies []string
	NoMasks      bool
}

// ProfileMasks is the masking slice of a resolved profile. Kept here so
// datamask stays independent of internal/config.
type ProfileMasks struct {
	File     string
	Policies []string
}

// MaskConfig is the pure overlay of flags onto profile. No filesystem reads.
type MaskConfig struct {
	Path     string
	Policies []string
	Disabled bool
}

// BuildConfig: CLI beats profile. Pure.
func BuildConfig(flags Flags, profile ProfileMasks) MaskConfig {
	cfg := MaskConfig{
		Path:     flags.MasksFile,
		Policies: flags.MaskPolicies,
		Disabled: flags.NoMasks,
	}
	if cfg.Path == "" {
		cfg.Path = profile.File
	}
	if len(cfg.Policies) == 0 {
		cfg.Policies = profile.Policies
	}
	return cfg
}

// nil return = masking disabled; unmasked init permanently writes raw stats to history.db
func (c MaskConfig) BuildMasker(dbID history.DatabaseId, cwd string) (*masking.Policy, error) {
	if c.Disabled {
		return nil, nil
	}
	path := c.Path
	if path == "" {
		if d, err := masking.DiscoverMasksFile(cwd); err == nil {
			path = d
		}
	}
	if path == "" {
		return nil, fmt.Errorf("no data-masking-policy.yml found; pass --masks-file=PATH, set masks_file in the profile, or --no-masks to capture without masking")
	}
	return Load(path, dbID, c.Policies)
}
