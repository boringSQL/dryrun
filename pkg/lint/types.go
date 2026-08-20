package lint

type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
	SeverityInfo    Severity = "info"
)

type (
	Finding struct {
		Rule           string   `json:"rule"`
		Severity       Severity `json:"severity"`
		Tables         []string `json:"tables"`
		Column         *string  `json:"column,omitempty"`
		Message        string   `json:"message"`
		Recommendation string   `json:"recommendation"`
		ConventionDoc  string   `json:"convention_doc,omitempty"`
		DDLFix         *string  `json:"ddl_fix,omitempty"`
		MinPgVersion   *int     `json:"min_pg_version,omitempty"`
		// estimate-based finding whose math is a lower bound (expression/partial index)
		Approximate    bool   `json:"approximate,omitempty"`
		ApproximateWhy string `json:"approximate_why,omitempty"`
	}

	Summary struct {
		Errors   int `json:"errors"`
		Warnings int `json:"warnings"`
		Info     int `json:"info"`
	}

	Report struct {
		Findings      []Finding `json:"findings"`
		TablesChecked int       `json:"tables_checked"`
		Summary       Summary   `json:"summary"`
		ConfigSource  string    `json:"config_source,omitempty"`
	}

	Config struct {
		MinSeverity           Severity `json:"min_severity" toml:"min_severity"`
		TableNameStyle        string   `json:"table_name_style" toml:"table_name"`
		ColumnNameStyle       string   `json:"column_name_style" toml:"column_name"`
		PKType                string   `json:"pk_type" toml:"pk_type"`
		FKPattern             string   `json:"fk_pattern" toml:"fk_pattern"`
		IndexPattern          string   `json:"index_pattern" toml:"index_pattern"`
		RequireTimestamps     bool     `json:"require_timestamps" toml:"require_timestamps"`
		TimestampType         string   `json:"timestamp_type" toml:"timestamp_type"`
		PreferTextOverVarchar bool     `json:"prefer_text_over_varchar" toml:"prefer_text_over_varchar"`
		DisabledRules         []string `json:"disabled_rules" toml:"disabled_rules"`
		TableNameRegex        *string  `json:"table_name_regex,omitempty" toml:"table_name_regex"`
		ColumnNameRegex       *string  `json:"column_name_regex,omitempty" toml:"column_name_regex"`
	}
)

func NewReport(findings []Finding, tablesChecked int, configSource string) Report {
	s := countSeverities(findings)
	return Report{
		Findings:      findings,
		TablesChecked: tablesChecked,
		Summary:       s,
		ConfigSource:  configSource,
	}
}

func countSeverities(findings []Finding) Summary {
	var s Summary
	for _, f := range findings {
		switch f.Severity {
		case SeverityError:
			s.Errors++
		case SeverityWarning:
			s.Warnings++
		case SeverityInfo:
			s.Info++
		}
	}
	return s
}

// Findings grouped by rule for concise MCP output
type (
	CompactReport struct {
		RuleGroups    []RuleGroup `json:"rule_groups"`
		TablesChecked int         `json:"tables_checked"`
		Summary       Summary     `json:"summary"`
	}

	RuleGroup struct {
		Rule     string           `json:"rule"`
		Severity Severity         `json:"severity"`
		Message  string           `json:"message"`
		Count    int              `json:"count"`
		Items    []CompactFinding `json:"items"`
	}

	CompactFinding struct {
		Tables []string `json:"tables"`
		Column *string  `json:"column,omitempty"`
	}
)

// maxExamples=0 keeps all items
func CompactReportFromReportN(r Report, maxExamples int) CompactReport {
	groups := make(map[string]*RuleGroup)
	var order []string

	for _, f := range r.Findings {
		g, ok := groups[f.Rule]
		if !ok {
			g = &RuleGroup{Rule: f.Rule, Severity: f.Severity, Message: f.Message}
			groups[f.Rule] = g
			order = append(order, f.Rule)
		}
		g.Count++
		if maxExamples == 0 || len(g.Items) < maxExamples {
			g.Items = append(g.Items, CompactFinding{
				Tables: f.Tables, Column: f.Column,
			})
		}
	}

	ruleGroups := make([]RuleGroup, 0, len(order))
	for _, rule := range order {
		ruleGroups = append(ruleGroups, *groups[rule])
	}

	return CompactReport{
		RuleGroups:    ruleGroups,
		TablesChecked: r.TablesChecked,
		Summary:       r.Summary,
	}
}

func DefaultConfig() Config {
	return Config{
		TableNameStyle:        "auto",
		ColumnNameStyle:       "snake_case",
		PKType:                "bigint_identity",
		FKPattern:             "fk_{table}_{column}",
		IndexPattern:          "idx_{table}_{columns}",
		RequireTimestamps:     true,
		TimestampType:         "timestamptz",
		PreferTextOverVarchar: true,
	}
}
