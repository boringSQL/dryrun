package bloat

import (
	"math"
	"strings"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

const (
	pageSize        = 8192
	btreeFillfactor = 0.9
	tupleOverhead   = 8 // item pointer + tuple header alignment, bytes
	defaultWidth    = 32
)

// Avg byte widths per type for btree tuple sizing
var typeWidths = map[string]int{
	"smallint":                    2,
	"int2":                        2,
	"integer":                     4,
	"int":                         4,
	"int4":                        4,
	"bigint":                      8,
	"int8":                        8,
	"real":                        4,
	"float4":                      4,
	"double precision":            8,
	"float8":                      8,
	"boolean":                     1,
	"bool":                        1,
	"date":                        4,
	"timestamp without time zone": 8,
	"timestamp":                   8,
	"timestamp with time zone":    8,
	"timestamptz":                 8,
	"uuid":                        16,
	"inet":                        19,
	"cidr":                        19,
	"macaddr":                     6,
	"oid":                         4,
	"numeric":                     16,
	"text":                        32,
	"character varying":           32,
	"varchar":                     32,
	"character":                   32,
	"char":                        32,
	"bpchar":                      32,
	"bytea":                       32,
	"jsonb":                       64,
	"json":                        64,
	"xml":                         64,
}

type BloatEstimate struct {
	BloatRatio    float64 `json:"bloat_ratio"`
	ExpectedPages int64   `json:"expected_pages"`
	ActualPages   int64   `json:"actual_pages"`
	AvgKeyWidth   int     `json:"avg_key_width"`
	SizeBytes     int64   `json:"size_bytes"`
}

func EstimateIndexBloat(sizing snapshot.IndexSizing, columns []string, table snapshot.Table, indexType string) (BloatEstimate, bool) {
	if indexType != "btree" {
		return BloatEstimate{}, false
	}
	if sizing.Reltuples <= 0 || sizing.Relpages <= 0 {
		return BloatEstimate{}, false
	}

	colTypes := make(map[string]string, len(table.Columns))
	for _, c := range table.Columns {
		colTypes[c.Name] = c.TypeName
	}

	avgKeyWidth := 0
	for _, col := range columns {
		typeName, ok := colTypes[col]
		if !ok {
			// Expression column (e.g. lower(email)) - use default
			avgKeyWidth += defaultWidth
			continue
		}
		avgKeyWidth += lookupTypeWidth(typeName)
	}

	if avgKeyWidth == 0 {
		return BloatEstimate{}, false
	}

	usable := float64(pageSize) * btreeFillfactor
	tupleSize := float64(tupleOverhead + avgKeyWidth)
	tuplesPerPage := usable / tupleSize
	expectedPages := int64(math.Ceil(sizing.Reltuples / tuplesPerPage))
	if expectedPages < 1 {
		expectedPages = 1
	}

	return BloatEstimate{
		BloatRatio:    float64(sizing.Relpages) / float64(expectedPages),
		ExpectedPages: expectedPages,
		ActualPages:   sizing.Relpages,
		AvgKeyWidth:   avgKeyWidth,
		SizeBytes:     sizing.Size,
	}, true
}

// lookupTypeWidth returns the estimated byte width for a PostgreSQL type name.
func lookupTypeWidth(typeName string) int {
	normalized := strings.ToLower(strings.TrimSpace(typeName))

	// Strip parenthesized suffixes: varchar(255) -> varchar, numeric(10,2) -> numeric
	if idx := strings.IndexByte(normalized, '('); idx >= 0 {
		normalized = strings.TrimSpace(normalized[:idx])
	}

	// Strip array suffix
	normalized = strings.TrimSuffix(normalized, "[]")

	if w, ok := typeWidths[normalized]; ok {
		return w
	}
	return defaultWidth
}
