package query

import (
	"fmt"
	"strings"

	"github.com/boringsql/dryrun/internal/schema"
)

const largeTableThreshold = 10_000.0

func detectAntipatterns(parsed *ParsedQuery, snap *schema.SchemaSnapshot, warnings *[]ValidationWarning) {
	detectSelectStar(parsed, warnings)
	detectUnboundedQuery(parsed, snap, warnings)
	detectCartesianJoin(parsed, warnings)
	detectDMLWithoutWhere(parsed, warnings)
	detectPartitionKeyAntipatterns(parsed, snap, warnings)
	detectPartitionKeyUpdate(parsed, snap, warnings)
}

func detectSelectStar(parsed *ParsedQuery, warnings *[]ValidationWarning) {
	if parsed.Info.HasSelectStar {
		*warnings = append(*warnings, ValidationWarning{
			Severity: SeverityWarning,
			Message:  "SELECT * - consider listing columns explicitly to avoid extra I/O and breakage when columns change",
		})
	}
}

func detectUnboundedQuery(parsed *ParsedQuery, snap *schema.SchemaSnapshot, warnings *[]ValidationWarning) {
	if parsed.Info.StatementType != "SELECT" {
		return
	}
	if parsed.Info.HasWhere || parsed.Info.HasLimit {
		return
	}

	for _, ref := range parsed.Info.Tables {
		schemaName := "public"
		if ref.Schema != nil {
			schemaName = *ref.Schema
		}
		// table-size refinement requires AnnotatedSchema; ValidateQuery doesn't carry one
		_ = schemaName
		_ = ref
	}
}

func detectCartesianJoin(parsed *ParsedQuery, warnings *[]ValidationWarning) {
	if parsed.Info.StatementType != "SELECT" {
		return
	}

	var selectTables []string
	for _, t := range parsed.Info.Tables {
		if t.Context == "select" {
			selectTables = append(selectTables, t.Name)
		}
	}

	if len(selectTables) > 1 && !parsed.Info.HasJoin {
		*warnings = append(*warnings, ValidationWarning{
			Severity: SeverityWarning,
			Message:  fmt.Sprintf("possible Cartesian join between %s - missing JOIN condition", strings.Join(selectTables, ", ")),
		})
	}
}

func detectDMLWithoutWhere(parsed *ParsedQuery, warnings *[]ValidationWarning) {
	isDML := parsed.Info.StatementType == "UPDATE" || parsed.Info.StatementType == "DELETE"
	if isDML && !parsed.Info.HasWhere {
		*warnings = append(*warnings, ValidationWarning{
			Severity: SeverityError,
			Message:  fmt.Sprintf("%s without WHERE clause - this will affect ALL rows", parsed.Info.StatementType),
		})
	}
}

func detectPartitionKeyAntipatterns(parsed *ParsedQuery, snap *schema.SchemaSnapshot, warnings *[]ValidationWarning) {
	for _, ref := range parsed.Info.Tables {
		schemaName := "public"
		if ref.Schema != nil {
			schemaName = *ref.Schema
		}

		for i := range snap.Tables {
			t := &snap.Tables[i]
			if t.Name != ref.Name || t.Schema != schemaName {
				continue
			}
			if t.PartitionInfo == nil {
				break
			}

			keyColumns := parsePartitionKeyColumns(t.PartitionInfo.Key)
			found := false

			for _, kc := range keyColumns {
				for _, fc := range parsed.Info.FilterColumns {
					if strings.EqualFold(fc.Column, kc) {
						found = true
						break
					}
				}
				if found {
					break
				}
			}

			if !found {
				*warnings = append(*warnings, ValidationWarning{
					Severity: SeverityWarning,
					Message: fmt.Sprintf(
						"query on partitioned table '%s.%s' (%s on '%s', %d partitions) "+
							"does not filter on partition key; all partitions will be scanned",
						t.Schema, t.Name, t.PartitionInfo.Strategy, t.PartitionInfo.Key,
						len(t.PartitionInfo.Children)),
				})
			}

			for _, kc := range keyColumns {
				for _, fwc := range parsed.Info.FuncWrappedColumns {
					if strings.EqualFold(fwc.Column, kc) {
						*warnings = append(*warnings, ValidationWarning{
							Severity: SeverityWarning,
							Message: fmt.Sprintf(
								"partition key '%s' on '%s.%s' is wrapped in %s - this prevents partition pruning. %s",
								kc, t.Schema, t.Name, fwc.FuncName, funcWrapRewriteHint(fwc.FuncName, kc)),
						})
					}
				}
			}
			break
		}
	}
}

func detectPartitionKeyUpdate(parsed *ParsedQuery, snap *schema.SchemaSnapshot, warnings *[]ValidationWarning) {
	if parsed.Info.StatementType != "UPDATE" || len(parsed.Info.UpdateTargets) == 0 {
		return
	}

	for _, ref := range parsed.Info.Tables {
		schemaName := "public"
		if ref.Schema != nil {
			schemaName = *ref.Schema
		}

		for i := range snap.Tables {
			t := &snap.Tables[i]
			if t.Name != ref.Name || t.Schema != schemaName {
				continue
			}
			if t.PartitionInfo == nil {
				break
			}

			keyColumns := parsePartitionKeyColumns(t.PartitionInfo.Key)
			for _, kc := range keyColumns {
				for _, ut := range parsed.Info.UpdateTargets {
					if strings.EqualFold(ut, kc) {
						*warnings = append(*warnings, ValidationWarning{
							Severity: SeverityWarning,
							Message: fmt.Sprintf(
								"UPDATE changes partition key '%s' on partitioned table '%s.%s'. This causes cross-partition row movement (DELETE + INSERT)", kc, t.Schema, t.Name),
						})
					}
				}
			}
			break
		}
	}
}

func funcWrapRewriteHint(funcName, col string) string {
	switch funcName {
	case "extract", "::date", "to_char":
		return fmt.Sprintf("Rewrite as: WHERE %s >= '2025-01-01' AND %s < '2026-01-01'", col, col)
	case "date_trunc":
		return fmt.Sprintf(
			"Rewrite as: WHERE %s >= date_trunc('month', target) AND %s < date_trunc('month', target) + interval '1 month'",
			col, col)
	default:
		return fmt.Sprintf("Rewrite using a direct range comparison on %s instead.", col)
	}
}

func parsePartitionKeyColumns(key string) []string {
	parts := strings.Split(key, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
