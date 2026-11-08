package lint

import (
	"fmt"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

// key defaults to "RANGE (k)" where k is a column of keyType
func rangeGapFindings(t *testing.T, keyType, key string, bounds ...string) []Finding {
	t.Helper()
	children := make([]snapshot.PartitionChild, len(bounds))
	for i, b := range bounds {
		children[i] = snapshot.PartitionChild{Schema: "public", Name: fmt.Sprintf("events_%d", i), Bound: b}
	}
	tbl := makePartitionedTable(children)
	tbl.Columns = append(tbl.Columns, makeCol("k", keyType))
	tbl.PartitionInfo.Key = "RANGE (k)"
	if key != "" {
		tbl.PartitionInfo.Key = key
	}
	snap := emptySnapshot()
	snap.Tables = []snapshot.Table{tbl}

	config := DefaultConfig()
	var out []Finding
	for _, f := range LintSchema(snap, &config).Findings {
		if f.Rule == "partition/range_gaps" {
			out = append(out, f)
		}
	}
	return out
}

func TestPartitionRangeGapBoundTypes(t *testing.T) {
	tests := []struct {
		name    string
		keyType string
		key     string
		bounds  []string
		wantGap int
	}{
		{
			name:    "int4 unquoted, contiguous",
			keyType: "integer",
			bounds: []string{
				"FOR VALUES FROM (1) TO (10)",
				"FOR VALUES FROM (10) TO (20)",
			},
		},
		{
			name:    "int4 unquoted, gap",
			keyType: "integer",
			bounds: []string{
				"FOR VALUES FROM (1) TO (10)",
				"FOR VALUES FROM (20) TO (30)",
			},
			wantGap: 1,
		},
		{
			// pg quotes negative int4 bounds but not the non-negative ones
			name:    "negative int4 next to non-negative, contiguous",
			keyType: "integer",
			bounds: []string{
				"FOR VALUES FROM ('-10') TO (0)",
				"FOR VALUES FROM (0) TO (10)",
			},
		},
		{
			name:    "negative int4 next to non-negative, gap",
			keyType: "integer",
			bounds: []string{
				"FOR VALUES FROM (0) TO (10)",
				"FOR VALUES FROM ('-10') TO ('-5')",
			},
			wantGap: 1,
		},
		{
			// as strings 10 < 2, which used to invent a gap
			name:    "bigint quoted, contiguous across digit counts",
			keyType: "bigint",
			bounds: []string{
				"FOR VALUES FROM ('1') TO ('2')",
				"FOR VALUES FROM ('2') TO ('10')",
				"FOR VALUES FROM ('10') TO ('20')",
			},
		},
		{
			name:    "bigint quoted, gap across digit counts",
			keyType: "bigint",
			bounds: []string{
				"FOR VALUES FROM ('20') TO ('100')",
				"FOR VALUES FROM ('1') TO ('2')",
				"FOR VALUES FROM ('2') TO ('10')",
			},
			wantGap: 1,
		},
		{
			name:    "numeric, equal values with different spelling",
			keyType: "numeric(10,2)",
			bounds: []string{
				"FOR VALUES FROM (0.5) TO (1.0)",
				"FOR VALUES FROM (1) TO (1.5)",
			},
		},
		{
			name:    "minvalue and maxvalue, contiguous",
			keyType: "integer",
			bounds: []string{
				"FOR VALUES FROM (MINVALUE) TO (100)",
				"FOR VALUES FROM (100) TO (200)",
				"FOR VALUES FROM (200) TO (MAXVALUE)",
			},
		},
		{
			name:    "minvalue and maxvalue, gap",
			keyType: "integer",
			bounds: []string{
				"FOR VALUES FROM (200) TO (MAXVALUE)",
				"FOR VALUES FROM (MINVALUE) TO (100)",
			},
			wantGap: 1,
		},
		{
			name:    "date, contiguous",
			keyType: "date",
			bounds: []string{
				"FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')",
				"FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')",
			},
		},
		{
			name:    "timestamp without time zone, contiguous",
			keyType: "timestamp without time zone",
			bounds: []string{
				"FOR VALUES FROM ('2025-01-01 00:00:00') TO ('2025-02-01 00:00:00')",
				"FOR VALUES FROM ('2025-02-01 00:00:00') TO ('2025-03-01 00:00:00')",
			},
		},
		{
			name:    "timestamptz, same instant in different offsets",
			keyType: "timestamp with time zone",
			bounds: []string{
				"FOR VALUES FROM ('2025-01-01 00:00:00+00') TO ('2025-02-01 00:00:00+00')",
				"FOR VALUES FROM ('2025-02-01 01:00:00+01') TO ('2025-03-01 00:00:00+00')",
			},
		},
		{
			name:    "timestamptz with hour-only negative offset, contiguous",
			keyType: "timestamp with time zone",
			bounds: []string{
				"FOR VALUES FROM ('2025-01-01 00:00:00-07') TO ('2025-02-01 00:00:00-07')",
				"FOR VALUES FROM ('2025-02-01 00:00:00-07') TO ('2025-03-01 00:00:00-07')",
			},
		},
		{
			name:    "timestamptz with fraction and minutes offset, gap",
			keyType: "timestamp with time zone",
			bounds: []string{
				"FOR VALUES FROM ('2025-01-01 00:00:00.5+05:30') TO ('2025-02-01 00:00:00+05:30')",
				"FOR VALUES FROM ('2025-03-01 00:00:00+05:30') TO ('2025-04-01 00:00:00+05:30')",
			},
			wantGap: 1,
		},
		{
			name:    "default partition is ignored",
			keyType: "integer",
			bounds: []string{
				"FOR VALUES FROM (1) TO (10)",
				"DEFAULT",
				"FOR VALUES FROM (20) TO (30)",
			},
			wantGap: 1,
		},
		{
			name:    "only a default partition",
			keyType: "integer",
			bounds:  []string{"DEFAULT"},
		},
		{
			name:    "single partition",
			keyType: "integer",
			bounds:  []string{"FOR VALUES FROM (1) TO (10)"},
		},
		{
			name:    "no partitions",
			keyType: "integer",
		},
		{
			// can't judge gaps without the column collation
			name:    "text bounds are skipped",
			keyType: "text",
			bounds: []string{
				"FOR VALUES FROM ('a') TO ('b')",
				"FOR VALUES FROM ('x') TO ('y')",
			},
		},
		{
			// contiguous as text (0 < 10 < 2 < 3), a gap if read as numbers
			name:    "text key with digit-only bounds is skipped",
			keyType: "text",
			bounds: []string{
				"FOR VALUES FROM ('0') TO ('10')",
				"FOR VALUES FROM ('10') TO ('2')",
				"FOR VALUES FROM ('2') TO ('3')",
			},
		},
		{
			name:    "varchar key with digit-only bounds is skipped",
			keyType: "character varying(20)",
			bounds: []string{
				"FOR VALUES FROM ('0') TO ('10')",
				"FOR VALUES FROM ('10') TO ('2')",
				"FOR VALUES FROM ('2') TO ('3')",
			},
		},
		{
			name:    "expression key is skipped",
			keyType: "timestamp with time zone",
			key:     "RANGE (date_trunc('day'::text, k))",
			bounds: []string{
				"FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')",
				"FOR VALUES FROM ('2025-03-01') TO ('2025-04-01')",
			},
		},
		{
			name:    "multi-column key is skipped",
			keyType: "integer",
			key:     "RANGE (k, id)",
			bounds: []string{
				"FOR VALUES FROM (1, 1) TO (1, 7)",
				"FOR VALUES FROM (1, 9) TO (2, 1)",
			},
		},
		{
			name:    "multi-column bounds are skipped",
			keyType: "integer",
			bounds: []string{
				"FOR VALUES FROM (2024, 1) TO (2024, 7)",
				"FOR VALUES FROM (2024, 9) TO (2025, 1)",
			},
		},
		{
			// one unreadable child would otherwise look like a hole
			name:    "one unparseable child skips the table",
			keyType: "integer",
			bounds: []string{
				"FOR VALUES FROM (1) TO (10)",
				"FOR VALUES FROM ('infinity') TO ('x')",
				"FOR VALUES FROM (20) TO (30)",
			},
		},
		{
			name:    "BC dates are skipped",
			keyType: "date",
			bounds: []string{
				"FOR VALUES FROM ('0001-01-01 BC') TO ('0001-02-01 BC')",
				"FOR VALUES FROM ('0001-04-01 BC') TO ('0001-05-01 BC')",
			},
		},
		{
			name:    "mixed bound types are skipped",
			keyType: "integer",
			bounds: []string{
				"FOR VALUES FROM (1) TO (10)",
				"FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := rangeGapFindings(t, tc.keyType, tc.key, tc.bounds...)
			if len(got) != tc.wantGap {
				t.Fatalf("got %d range_gaps findings, want %d: %+v", len(got), tc.wantGap, got)
			}
		})
	}
}

func TestPartitionRangeGapMessageNamesBounds(t *testing.T) {
	got := rangeGapFindings(t, "integer", "",
		"FOR VALUES FROM (1) TO (10)",
		"FOR VALUES FROM (20) TO (30)",
	)
	if len(got) != 1 {
		t.Fatalf("got %d findings, want 1", len(got))
	}
	if !strings.Contains(got[0].Message, "'10'") || !strings.Contains(got[0].Message, "'20'") {
		t.Errorf("message should name the bounds around the gap: %q", got[0].Message)
	}
	if got[0].DDLFix == nil || !strings.Contains(*got[0].DDLFix, "FROM ('10') TO ('20')") {
		t.Errorf("fix should create a partition spanning the gap: %v", got[0].DDLFix)
	}
}
