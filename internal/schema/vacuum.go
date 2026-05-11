package schema

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

type (
	AutovacuumDefaults struct {
		Enabled               bool
		VacuumThreshold       int64
		VacuumScaleFactor     float64
		AnalyzeThreshold      int64
		AnalyzeScaleFactor    float64
		VacuumCostDelay       int // ms
		VacuumCostLimit       int
		FreezeMaxAge          int64
		MultixactFreezeMaxAge int64
	}

	VacuumHealth struct {
		Schema                    string   `json:"schema"`
		Table                     string   `json:"table"`
		Reltuples                 float64  `json:"reltuples"`
		DeadTuples                int64    `json:"dead_tuples"`
		VacuumTriggerAt           float64  `json:"vacuum_trigger_at"`
		VacuumProgress            float64  `json:"vacuum_progress"`
		HasOverrides              bool     `json:"has_overrides"`
		EffectiveThreshold        int64    `json:"effective_threshold"`
		EffectiveScale            float64  `json:"effective_scale_factor"`
		EffectiveAnalyzeThreshold int64    `json:"effective_analyze_threshold"`
		EffectiveAnalyzeScale     float64  `json:"effective_analyze_scale_factor"`
		AnalyzeTriggerAt          float64  `json:"analyze_trigger_at"`
		AutovacuumEnabled         bool     `json:"autovacuum_enabled"`
		Recommendations           []string `json:"recommendations,omitempty"`
	}
)

// Reads autovacuum GUCs, falling back to PG defaults
func ParseAutovacuumDefaults(gucs []GucSetting) AutovacuumDefaults {
	d := AutovacuumDefaults{
		Enabled:               true,
		VacuumThreshold:       50,
		VacuumScaleFactor:     0.2,
		AnalyzeThreshold:      50,
		AnalyzeScaleFactor:    0.1,
		VacuumCostDelay:       2,
		VacuumCostLimit:       -1,
		FreezeMaxAge:          200_000_000,
		MultixactFreezeMaxAge: 400_000_000,
	}

	for _, g := range gucs {
		switch g.Name {
		case "autovacuum":
			d.Enabled = g.Setting == "on"
		case "autovacuum_vacuum_threshold":
			if v, err := strconv.ParseInt(g.Setting, 10, 64); err == nil {
				d.VacuumThreshold = v
			}
		case "autovacuum_vacuum_scale_factor":
			if v, err := strconv.ParseFloat(g.Setting, 64); err == nil {
				d.VacuumScaleFactor = v
			}
		case "autovacuum_analyze_threshold":
			if v, err := strconv.ParseInt(g.Setting, 10, 64); err == nil {
				d.AnalyzeThreshold = v
			}
		case "autovacuum_analyze_scale_factor":
			if v, err := strconv.ParseFloat(g.Setting, 64); err == nil {
				d.AnalyzeScaleFactor = v
			}
		case "autovacuum_vacuum_cost_delay":
			if v, err := strconv.Atoi(g.Setting); err == nil {
				d.VacuumCostDelay = v
			}
		case "autovacuum_vacuum_cost_limit":
			if v, err := strconv.Atoi(g.Setting); err == nil {
				d.VacuumCostLimit = v
			}
		case "autovacuum_freeze_max_age":
			if v, err := strconv.ParseInt(g.Setting, 10, 64); err == nil {
				d.FreezeMaxAge = v
			}
		case "autovacuum_multixact_freeze_max_age":
			if v, err := strconv.ParseInt(g.Setting, 10, 64); err == nil {
				d.MultixactFreezeMaxAge = v
			}
		}
	}
	return d
}

func parseReloptions(reloptions []string) map[string]string {
	opts := make(map[string]string, len(reloptions))
	for _, opt := range reloptions {
		if k, v, ok := strings.Cut(opt, "="); ok {
			opts[k] = v
		}
	}
	return opts
}

func AnalyzeVacuumHealth(a *AnnotatedSchema) []VacuumHealth {
	if a == nil || a.Schema == nil {
		return nil
	}
	defaults := ParseAutovacuumDefaults(a.Schema.GUCs)

	var results []VacuumHealth
	for i := range a.Schema.Tables {
		t := &a.Schema.Tables[i]
		qual := t.Qual()
		sizing := a.SizingFor(qual)
		activity := a.PrimaryActivity(qual)
		if sizing == nil || sizing.Reltuples < 10_000 {
			continue
		}
		var reltuples float64 = sizing.Reltuples
		var deadTuples int64
		if activity != nil {
			deadTuples = activity.NDeadTup
		}

		opts := parseReloptions(t.Reloptions)
		hasOverrides := false
		for k := range opts {
			if strings.HasPrefix(k, "autovacuum_") {
				hasOverrides = true
				break
			}
		}

		// effective settings
		threshold := defaults.VacuumThreshold
		scaleFactor := defaults.VacuumScaleFactor
		analyzeThreshold := defaults.AnalyzeThreshold
		analyzeScaleFactor := defaults.AnalyzeScaleFactor
		avEnabled := defaults.Enabled

		if v, ok := opts["autovacuum_vacuum_threshold"]; ok {
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				threshold = parsed
			}
		}
		if v, ok := opts["autovacuum_vacuum_scale_factor"]; ok {
			if parsed, err := strconv.ParseFloat(v, 64); err == nil {
				scaleFactor = parsed
			}
		}
		if v, ok := opts["autovacuum_analyze_threshold"]; ok {
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				analyzeThreshold = parsed
			}
		}
		if v, ok := opts["autovacuum_analyze_scale_factor"]; ok {
			if parsed, err := strconv.ParseFloat(v, 64); err == nil {
				analyzeScaleFactor = parsed
			}
		}
		if v, ok := opts["autovacuum_enabled"]; ok {
			avEnabled = v == "on" || v == "true"
		}

		triggerAt := float64(threshold) + scaleFactor*reltuples
		analyzeTrigger := float64(analyzeThreshold) + analyzeScaleFactor*reltuples
		var progress float64
		if triggerAt > 0 {
			progress = float64(deadTuples) / triggerAt
		}

		vh := VacuumHealth{
			Schema:                    t.Schema,
			Table:                     t.Name,
			Reltuples:                 reltuples,
			DeadTuples:                deadTuples,
			VacuumTriggerAt:           triggerAt,
			VacuumProgress:            progress,
			HasOverrides:              hasOverrides,
			EffectiveThreshold:        threshold,
			EffectiveScale:            scaleFactor,
			EffectiveAnalyzeThreshold: analyzeThreshold,
			EffectiveAnalyzeScale:     analyzeScaleFactor,
			AnalyzeTriggerAt:          analyzeTrigger,
			AutovacuumEnabled:         avEnabled,
		}

		if !avEnabled {
			vh.Recommendations = append(vh.Recommendations,
				"autovacuum is disabled for this table! This won't end good; you've been warned")
		}
		if reltuples >= 1_000_000 && !hasOverrides {
			vacSF, vacThresh, azSF, azThresh := suggestedVacuumKnobs(reltuples)
			vh.Recommendations = append(vh.Recommendations,
				fmt.Sprintf("large table (%dk rows) using default autovacuum settings; consider: "+
					"autovacuum_vacuum_scale_factor=%g, autovacuum_vacuum_threshold=%d, "+
					"autovacuum_analyze_scale_factor=%g, autovacuum_analyze_threshold=%d",
					int64(reltuples)/1000, vacSF, vacThresh, azSF, azThresh))
		}
		if reltuples > 0 && float64(deadTuples)/reltuples > 0.10 {
			vh.Recommendations = append(vh.Recommendations,
				fmt.Sprintf("high dead tuple ratio: %d dead / %dk live (%.1f%%)",
					deadTuples, int64(reltuples)/1000,
					float64(deadTuples)/reltuples*100))
		}
		if triggerAt > 10_000_000 {
			vh.Recommendations = append(vh.Recommendations,
				fmt.Sprintf("vacuum won't trigger until %dk dead tuples. Threshold is very high",
					int64(triggerAt)/1000))
		}

		results = append(results, vh)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].VacuumProgress > results[j].VacuumProgress
	})
	return results
}

// Shared with audit/rules.go so both sides recommend the same numbers.
func SuggestedVacuumKnobs(reltuples float64) (vacSF float64, vacThresh int64, azSF float64, azThresh int64) {
	return suggestedVacuumKnobs(reltuples)
}

func suggestedVacuumKnobs(reltuples float64) (vacSF float64, vacThresh int64, azSF float64, azThresh int64) {
	vacSF = 100_000.0 / reltuples
	vacSF = math.Round(vacSF*1000) / 1000
	if vacSF < 0.001 {
		vacSF = 0.001
	}
	azSF = math.Round(vacSF/2*1000) / 1000

	vacThresh = int64(reltuples * 0.01)
	if vacThresh < 500 {
		vacThresh = 500
	} else if vacThresh > 5000 {
		vacThresh = 5000
	}
	azThresh = vacThresh / 2
	if azThresh < 250 {
		azThresh = 250
	}
	return
}
