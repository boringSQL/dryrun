package vacuum

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

type Severity string

const (
	SeverityInfo   Severity = "info"
	SeverityLow    Severity = "low"
	SeverityMedium Severity = "medium"
	SeverityHigh   Severity = "high"
)

// Typed concern code; the cloud pattern-matches on Code+Severity instead of
// parsing Message, and reads magnitudes off the parent VacuumHealth.
type VacuumCode string

const (
	CodeAutovacuumDisabled     VacuumCode = "autovacuum_disabled"
	CodeDefaultKnobsLargeTable VacuumCode = "default_knobs_large_table"
	CodeHighDeadTupleRatio     VacuumCode = "high_dead_tuple_ratio"
	CodeHighBloat              VacuumCode = "high_bloat"
	CodeVacuumThresholdHigh    VacuumCode = "vacuum_threshold_too_high"
	CodeFreezeAgeHigh          VacuumCode = "freeze_age_high"
	CodeMxidAgeHigh            VacuumCode = "mxid_age_high"
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
		FailsafeAge           int64 // vacuum_failsafe_age; global GUC, no per-table override
		MultixactFailsafeAge  int64
	}

	VacuumFinding struct {
		Code     VacuumCode `json:"code"`
		Severity Severity   `json:"severity"`
		Message  string     `json:"message"`
	}

	VacuumHealth struct {
		Schema                    string                  `json:"schema"`
		Table                     string                  `json:"table"`
		Reltuples                 float64                 `json:"reltuples"`
		DeadTuples                int64                   `json:"dead_tuples"`
		ModSinceAnalyze           int64                   `json:"mod_since_analyze"`
		VacuumTriggerAt           float64                 `json:"vacuum_trigger_at"`
		VacuumProgress            float64                 `json:"vacuum_progress"`
		HasOverrides              bool                    `json:"has_overrides"`
		EffectiveThreshold        int64                   `json:"effective_threshold"`
		EffectiveScale            float64                 `json:"effective_scale_factor"`
		EffectiveAnalyzeThreshold int64                   `json:"effective_analyze_threshold"`
		EffectiveAnalyzeScale     float64                 `json:"effective_analyze_scale_factor"`
		AnalyzeTriggerAt          float64                 `json:"analyze_trigger_at"`
		AnalyzeProgress           float64                 `json:"analyze_progress"`
		AutovacuumEnabled         bool                    `json:"autovacuum_enabled"`
		Bloat                     *snapshot.BloatEstimate `json:"bloat,omitempty"`
		XidAge                    int64                   `json:"xid_age,omitempty"`
		FreezeMaxAge              int64                   `json:"freeze_max_age,omitempty"`
		FreezeProgress            float64                 `json:"freeze_progress,omitempty"`
		FailsafeAge               int64                   `json:"failsafe_age,omitempty"`
		MxidAge                   int64                   `json:"mxid_age,omitempty"`
		MultixactFreezeMaxAge     int64                   `json:"multixact_freeze_max_age,omitempty"`
		MultixactFreezeProgress   float64                 `json:"multixact_freeze_progress,omitempty"`
		MultixactFailsafeAge      int64                   `json:"multixact_failsafe_age,omitempty"`
		LastVacuum                *time.Time              `json:"last_vacuum,omitempty"`
		LastAutovacuum            *time.Time              `json:"last_autovacuum,omitempty"`
		LastAnalyze               *time.Time              `json:"last_analyze,omitempty"`
		LastAutoanalyze           *time.Time              `json:"last_autoanalyze,omitempty"`
		VacuumCount               int64                   `json:"vacuum_count"`
		AutovacuumCount           int64                   `json:"autovacuum_count"`
		AnalyzeCount              int64                   `json:"analyze_count"`
		AutoanalyzeCount          int64                   `json:"autoanalyze_count"`
		Findings                  []VacuumFinding         `json:"findings,omitempty"`
		Recommendations           []string                `json:"recommendations,omitempty"` // Message of each Finding, in order
	}
)

// One concern: typed for the cloud, human string for the MCP tool. Single source.
func (vh *VacuumHealth) add(code VacuumCode, sev Severity, msg string) {
	vh.Findings = append(vh.Findings, VacuumFinding{Code: code, Severity: sev, Message: msg})
	vh.Recommendations = append(vh.Recommendations, msg)
}

// Reads autovacuum GUCs, falling back to PG defaults
func ParseAutovacuumDefaults(gucs []snapshot.GucSetting) AutovacuumDefaults {
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
		FailsafeAge:           1_600_000_000,
		MultixactFailsafeAge:  1_600_000_000,
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
		case "vacuum_failsafe_age":
			if v, err := strconv.ParseInt(g.Setting, 10, 64); err == nil {
				d.FailsafeAge = v
			}
		case "vacuum_multixact_failsafe_age":
			if v, err := strconv.ParseInt(g.Setting, 10, 64); err == nil {
				d.MultixactFailsafeAge = v
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

func AnalyzeVacuumHealth(a *snapshot.AnnotatedSchema) []VacuumHealth {
	if a == nil || a.Schema == nil {
		return nil
	}
	defaults := ParseAutovacuumDefaults(a.Schema.GUCs)
	caps := a.Schema.Flavor.Capabilities()

	var results []VacuumHealth
	for i := range a.Schema.Tables {
		t := &a.Schema.Tables[i]

		// skip partitioned table as they have no heap of its own
		if t.PartitionInfo != nil {
			continue
		}
		qual := t.Qual()
		sizing := a.SizingFor(qual)
		bloat := a.TableBloatFor(qual)
		activity := a.PrimaryActivity(qual)
		if sizing == nil || sizing.Reltuples < 10_000 {
			continue
		}
		var reltuples float64 = sizing.Reltuples
		var deadTuples, modSinceAnalyze int64
		if activity != nil {
			deadTuples = activity.NDeadTup
			modSinceAnalyze = activity.NModSinceAnalyze
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
		var analyzeProgress float64
		if analyzeTrigger > 0 {
			analyzeProgress = float64(modSinceAnalyze) / analyzeTrigger
		}

		vh := VacuumHealth{
			Schema:                    t.Schema,
			Table:                     t.Name,
			Reltuples:                 reltuples,
			DeadTuples:                deadTuples,
			ModSinceAnalyze:           modSinceAnalyze,
			VacuumTriggerAt:           triggerAt,
			VacuumProgress:            progress,
			HasOverrides:              hasOverrides,
			EffectiveThreshold:        threshold,
			EffectiveScale:            scaleFactor,
			EffectiveAnalyzeThreshold: analyzeThreshold,
			EffectiveAnalyzeScale:     analyzeScaleFactor,
			AnalyzeTriggerAt:          analyzeTrigger,
			AnalyzeProgress:           analyzeProgress,
			AutovacuumEnabled:         avEnabled,
			Bloat:                     bloat,
		}
		// anti-wraparound: age(relfrozenxid) vs the (possibly overridden) freeze_max_age.
		// ok=false (partitioned parents, pre-feature snapshots) skips freeze analysis.
		freezeMaxAge := defaults.FreezeMaxAge
		if v, ok := opts["autovacuum_freeze_max_age"]; ok {
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				freezeMaxAge = parsed
			}
		}
		if age, ok := sizing.FrozenXidAge(a.Planner.DatabaseXid); ok {
			vh.XidAge = age
			vh.FreezeMaxAge = freezeMaxAge
			vh.FailsafeAge = defaults.FailsafeAge
			if freezeMaxAge > 0 {
				vh.FreezeProgress = float64(age) / float64(freezeMaxAge)
			}
			if sev := freezeSeverity(age, freezeMaxAge, defaults.FailsafeAge); sev != "" {
				vh.add(CodeFreezeAgeHigh, sev,
					freezeMessage("relfrozenxid", "autovacuum_freeze_max_age", "vacuum_failsafe_age",
						age, freezeMaxAge, defaults.FailsafeAge))
			}
		}

		mxidFreezeMaxAge := defaults.MultixactFreezeMaxAge
		if v, ok := opts["autovacuum_multixact_freeze_max_age"]; ok {
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				mxidFreezeMaxAge = parsed
			}
		}
		if age, ok := sizing.MinMxidAge(a.Planner.DatabaseMxid); ok {
			vh.MxidAge = age
			vh.MultixactFreezeMaxAge = mxidFreezeMaxAge
			vh.MultixactFailsafeAge = defaults.MultixactFailsafeAge
			if mxidFreezeMaxAge > 0 {
				vh.MultixactFreezeProgress = float64(age) / float64(mxidFreezeMaxAge)
			}
			if sev := freezeSeverity(age, mxidFreezeMaxAge, defaults.MultixactFailsafeAge); sev != "" {
				vh.add(CodeMxidAgeHigh, sev,
					freezeMessage("relminmxid", "autovacuum_multixact_freeze_max_age", "vacuum_multixact_failsafe_age",
						age, mxidFreezeMaxAge, defaults.MultixactFailsafeAge))
			}
		}

		if activity != nil {
			vh.LastVacuum = activity.LastVacuum
			vh.LastAutovacuum = activity.LastAutovacuum
			vh.LastAnalyze = activity.LastAnalyze
			vh.LastAutoanalyze = activity.LastAutoanalyze
			vh.VacuumCount = activity.VacuumCount
			vh.AutovacuumCount = activity.AutovacuumCount
			vh.AnalyzeCount = activity.AnalyzeCount
			vh.AutoanalyzeCount = activity.AutoanalyzeCount
		}

		if !avEnabled {
			vh.add(CodeAutovacuumDisabled, SeverityHigh,
				"autovacuum is disabled for this table! This won't end good; you've been warned")
		}
		if reltuples >= 1_000_000 && !hasOverrides {
			if caps.AdaptiveAutovacuum {
				// AlloyDB schedules autovacuum by instance load, so the fixed scale-factor runbook misleads
				msg := fmt.Sprintf("large table (%dk rows) on default autovacuum settings; %s runs adaptive autovacuum, which schedules by load rather than the fixed scale-factor knobs",
					int64(reltuples)/1000, a.Schema.Flavor.Display())
				if !caps.ConfigTunable {
					msg += ", and those knobs are managed here, not yours to set"
				}
				vh.add(CodeDefaultKnobsLargeTable, SeverityInfo, msg)
			} else {
				vacSF, vacThresh, azSF, azThresh := suggestedVacuumKnobs(reltuples)
				vh.add(CodeDefaultKnobsLargeTable, SeverityMedium,
					fmt.Sprintf("large table (%dk rows) using default autovacuum settings; consider: "+
						"autovacuum_vacuum_scale_factor=%g, autovacuum_vacuum_threshold=%d, "+
						"autovacuum_analyze_scale_factor=%g, autovacuum_analyze_threshold=%d",
						int64(reltuples)/1000, vacSF, vacThresh, azSF, azThresh))
			}
		}
		if reltuples > 0 && float64(deadTuples)/reltuples > 0.10 {
			vh.add(CodeHighDeadTupleRatio, SeverityMedium,
				fmt.Sprintf("high dead tuple ratio: %d dead / %dk live (%.1f%%)",
					deadTuples, int64(reltuples)/1000,
					float64(deadTuples)/reltuples*100))
		}
		if triggerAt > 10_000_000 {
			vh.add(CodeVacuumThresholdHigh, SeverityMedium,
				fmt.Sprintf("vacuum won't trigger until %dk dead tuples. Threshold is very high",
					int64(triggerAt)/1000))
		}
		if bloat != nil && bloat.BloatRatio >= 4.0 {
			sev := SeverityMedium
			if bloat.BloatRatio >= 10.0 {
				sev = SeverityHigh
			}
			vh.add(CodeHighBloat, sev,
				fmt.Sprintf("table is %.1fx bloated: %d actual pages vs %d expected; "+
					"autovacuum cannot reclaim this — consider VACUUM FULL or pg_repack",
					bloat.BloatRatio, bloat.ActualPages, bloat.ExpectedPages))
		}

		results = append(results, vh)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].VacuumProgress > results[j].VacuumProgress
	})
	return results
}

// Grade past the escalation points, not the routine freeze_max_age trigger (healthy
// tables ride to it and reset): >=failsafe high, >=2x freeze_max_age medium, else "".
func freezeSeverity(age, freezeMaxAge, failsafeAge int64) Severity {
	switch {
	case failsafeAge > 0 && age >= failsafeAge:
		return SeverityHigh
	case freezeMaxAge > 0 && age >= 2*freezeMaxAge:
		return SeverityMedium
	default:
		return ""
	}
}

func freezeMessage(ageName, maxName, failsafeName string, age, maxAge, failsafeAge int64) string {
	if failsafeAge > 0 && age >= failsafeAge {
		return fmt.Sprintf("%s age is %d, at or past %s (%d); anti-wraparound VACUUM is in "+
			"last-resort mode and autovacuum is not keeping up — check for long-running "+
			"transactions, replication slots holding xmin, or a stuck/disabled autovacuum",
			ageName, age, failsafeName, failsafeAge)
	}
	return fmt.Sprintf("%s age is %d, %.1fx %s (%d) and not being frozen; the anti-wraparound "+
		"autovacuum isn't keeping up",
		ageName, age, float64(age)/float64(maxAge), maxName, maxAge)
}

// Shared with audit so both sides recommend the same numbers.
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
