package schema

import (
	"fmt"
	"math"
	"strings"
)

// Human-readable interpretation of pg_stats for one column
type ColumnProfile struct {
	Cardinality   string   `json:"cardinality"`
	Distribution  string   `json:"distribution,omitempty"`
	Nulls         string   `json:"nulls"`
	PhysicalOrder string   `json:"physical_order,omitempty"`
	ValueRange    string   `json:"value_range,omitempty"`
	TopValues     []string `json:"top_values,omitempty"`
	Note          string   `json:"note,omitempty"`
}

func ProfileColumn(col Column, tableRows float64) *ColumnProfile {
	if col.Stats == nil {
		return nil
	}
	s := col.Stats

	p := &ColumnProfile{
		Nulls: profileNulls(s, tableRows),
	}

	p.Cardinality = profileCardinality(s, tableRows)
	p.Distribution = profileDistribution(s)
	p.PhysicalOrder = profileCorrelation(s)
	p.ValueRange = profileRange(s)
	p.TopValues = parseTopValues(s, 8)
	p.Note = profileNote(col, s, tableRows)

	return p
}

func profileNulls(s *ColumnStats, tableRows float64) string {
	if s.NullFrac == nil || *s.NullFrac == 0 {
		return "none"
	}
	frac := *s.NullFrac
	if tableRows > 0 {
		return fmt.Sprintf("%.0f%% (~%d rows)", frac*100, int64(frac*tableRows))
	}
	return fmt.Sprintf("%.0f%%", frac*100)
}

func profileCardinality(s *ColumnStats, tableRows float64) string {
	if s.NDistinct == nil {
		return "unknown"
	}
	nd := *s.NDistinct

	// negative n_distinct = fraction of rows that are distinct
	if nd < 0 {
		ratio := -nd
		if ratio >= 0.99 {
			return "unique"
		}
		if tableRows > 0 {
			return fmt.Sprintf("high (~%d distinct in ~%d rows)", int64(ratio*tableRows), int64(tableRows))
		}
		return fmt.Sprintf("high (~%.0f%% distinct)", ratio*100)
	}

	// positive n_distinct = actual count
	distinct := int64(nd)
	if distinct <= 1 {
		return "constant (1 value)"
	}

	rowStr := ""
	if tableRows > 0 {
		rowStr = fmt.Sprintf(" in ~%d rows", int64(tableRows))
	}

	switch {
	case distinct <= 5:
		return fmt.Sprintf("very low (%d distinct%s)", distinct, rowStr)
	case distinct <= 20:
		return fmt.Sprintf("low (%d distinct%s)", distinct, rowStr)
	case distinct <= 200:
		return fmt.Sprintf("medium (%d distinct%s)", distinct, rowStr)
	default:
		return fmt.Sprintf("high (%d distinct%s)", distinct, rowStr)
	}
}

func profileDistribution(s *ColumnStats) string {
	if s.MostCommonFreqs == nil {
		return ""
	}
	freqs := parsePgArray(*s.MostCommonFreqs)
	if len(freqs) == 0 {
		return ""
	}

	var floats []float64
	for _, f := range freqs {
		var v float64
		if _, err := fmt.Sscanf(f, "%f", &v); err == nil {
			floats = append(floats, v)
		}
	}
	if len(floats) == 0 {
		return ""
	}

	maxFreq := floats[0]
	minFreq := floats[0]
	for _, f := range floats[1:] {
		if f > maxFreq {
			maxFreq = f
		}
		if f < minFreq {
			minFreq = f
		}
	}

	if maxFreq-minFreq < 0.02 {
		return fmt.Sprintf("uniform (each ~%.0f%%)", floats[0]*100)
	}
	if maxFreq > 0.5 {
		vals := parseTopValues(s, 1)
		if len(vals) > 0 {
			return fmt.Sprintf("heavily skewed (dominant value '%s' at ~%.0f%%)", vals[0], maxFreq*100)
		}
		return fmt.Sprintf("heavily skewed (top value at ~%.0f%%)", maxFreq*100)
	}
	return fmt.Sprintf("skewed (top ~%.0f%%, bottom ~%.0f%%)", maxFreq*100, minFreq*100)
}

func profileCorrelation(s *ColumnStats) string {
	if s.Correlation == nil {
		return ""
	}
	c := math.Abs(*s.Correlation)
	switch {
	case c >= 0.99:
		return "perfectly ordered (correlation: 1.0)"
	case c >= 0.9:
		return fmt.Sprintf("well ordered (correlation: %.2f)", *s.Correlation)
	case c >= 0.5:
		return fmt.Sprintf("partially ordered (correlation: %.2f)", *s.Correlation)
	default:
		return fmt.Sprintf("random (correlation: %.2f)", *s.Correlation)
	}
}

func profileRange(s *ColumnStats) string {
	if s.HistogramBounds == nil {
		return ""
	}
	bounds := parsePgArray(*s.HistogramBounds)
	if len(bounds) < 2 {
		return ""
	}
	return fmt.Sprintf("%s to %s", bounds[0], bounds[len(bounds)-1])
}

func parseTopValues(s *ColumnStats, limit int) []string {
	if s.MostCommonVals == nil {
		return nil
	}
	vals := parsePgArray(*s.MostCommonVals)
	if len(vals) > limit {
		vals = vals[:limit]
	}
	return vals
}

func profileNote(col Column, s *ColumnStats, tableRows float64) string {
	var notes []string

	nd := float64(0)
	if s.NDistinct != nil {
		nd = *s.NDistinct
	}
	typeLower := strings.ToLower(col.TypeName)

	if nd > 0 && nd <= 10 && !strings.Contains(typeLower, "bool") {
		notes = append(notes, "Enum-like column. Consider a PostgreSQL enum or a lookup table for referential integrity.")
	}

	if s.NullFrac != nil && *s.NullFrac > 0.5 {
		notes = append(notes, fmt.Sprintf("Mostly NULL (%.0f%%); a partial index WHERE %s IS NOT NULL would be compact and efficient.", *s.NullFrac*100, col.Name))
	}

	if s.Correlation != nil && math.Abs(*s.Correlation) < 0.3 && tableRows > 10_000 {
		notes = append(notes, "Low physical correlation; index range scans will cause random I/O. Consider CLUSTER or a BRIN index if sequential access patterns dominate.")
	}

	if nd == -1 && col.Identity == nil {
		notes = append(notes, "Unique values but no identity. Natural key candidate, or missing unique constraint?")
	}

	if len(notes) == 0 {
		return ""
	}
	return strings.Join(notes, " ")
}

// Parses {a,b,c} into ["a","b","c"]
func parsePgArray(s string) []string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil
	}
	s = s[1 : len(s)-1]
	if s == "" {
		return nil
	}

	var result []string
	var current strings.Builder
	inQuote := false
	escaped := false

	for _, r := range s {
		if escaped {
			current.WriteRune(r)
			escaped = false
			continue
		}
		switch {
		case r == '\\':
			escaped = true
		case r == '"':
			inQuote = !inQuote
		case r == ',' && !inQuote:
			result = append(result, current.String())
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		result = append(result, current.String())
	}
	return result
}

// Estimated selectivity for equality on column, in [0..1] (lower = more selective)
func ColumnSelectivity(col Column, tableRows float64) float64 {
	if col.Stats == nil || col.Stats.NDistinct == nil || tableRows <= 0 {
		return 0.5 // unknown, assume moderate
	}
	nd := *col.Stats.NDistinct
	if nd < 0 {
		// negative = fraction of rows that are distinct
		distinct := -nd * tableRows
		if distinct <= 0 {
			return 0.5
		}
		return 1.0 / distinct
	}
	if nd <= 0 {
		return 0.5
	}
	return 1.0 / nd
}

// True if dominant value covers more than threshold of rows
func HasSkewedDistribution(s *ColumnStats, threshold float64) (dominant string, freq float64, skewed bool) {
	if s == nil || s.MostCommonFreqs == nil || s.MostCommonVals == nil {
		return "", 0, false
	}
	freqs := parsePgArray(*s.MostCommonFreqs)
	vals := parsePgArray(*s.MostCommonVals)
	if len(freqs) == 0 || len(vals) == 0 {
		return "", 0, false
	}

	var f float64
	if _, err := fmt.Sscanf(freqs[0], "%f", &f); err != nil {
		return "", 0, false
	}
	if f >= threshold {
		return vals[0], f, true
	}
	return "", 0, false
}
