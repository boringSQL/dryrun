package lint

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/boringsql/dryrun/pkg/jit"
	"github.com/boringsql/dryrun/pkg/snapshot"
)

func runAllRules(snap *snapshot.SchemaSnapshot, config *Config) []Finding {
	var violations []Finding

	// collect partition children so we skip them - parent violations cover them
	partitionChildren := make(map[string]bool)
	for _, t := range snap.Tables {
		if t.PartitionInfo != nil {
			for _, child := range t.PartitionInfo.Children {
				partitionChildren[child.Schema+"."+child.Name] = true
			}
		}
	}

	// resolve "auto" table name style
	effectiveConfig := *config
	if effectiveConfig.TableNameStyle == "auto" {
		effectiveConfig.TableNameStyle = autoDetectTableNameStyle(snap.Tables)
	}

	for i := range snap.Tables {
		t := &snap.Tables[i]
		qualified := t.Schema + "." + t.Name

		if partitionChildren[qualified] {
			continue
		}

		rules := []struct {
			name string
			fn   func(*snapshot.Table, string, *Config, *snapshot.SchemaSnapshot, *[]Finding)
		}{
			{"naming/table_style", func(t *snapshot.Table, q string, c *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkTableNameStyle(t, q, c, v)
			}},
			{"naming/column_style", func(t *snapshot.Table, q string, c *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkColumnNameStyle(t, q, c, v)
			}},
			{"naming/fk_pattern", func(t *snapshot.Table, q string, c *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkFKNaming(t, q, c, v)
			}},
			{"naming/index_pattern", func(t *snapshot.Table, q string, c *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkIndexNaming(t, q, c, v)
			}},
			{"pk/exists", func(t *snapshot.Table, q string, _ *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkPKExists(t, q, v)
			}},
			{"pk/bigint_identity", func(t *snapshot.Table, q string, c *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkPKType(t, q, c, v)
			}},
			{"types/text_over_varchar", func(t *snapshot.Table, q string, c *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkTextOverVarchar(t, q, c, v)
			}},
			{"types/timestamptz", func(t *snapshot.Table, q string, _ *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkTimestamptz(t, q, v)
			}},
			{"types/no_serial", func(t *snapshot.Table, q string, _ *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkNoSerial(t, q, v)
			}},
			{"types/bigint_pk_fk", func(t *snapshot.Table, q string, c *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkBigintPKFK(t, q, c, v)
			}},
			{"constraints/fk_has_index", func(t *snapshot.Table, q string, _ *Config, s *snapshot.SchemaSnapshot, v *[]Finding) {
				checkFKHasIndex(t, q, s, v)
			}},
			{"constraints/unnamed", func(t *snapshot.Table, q string, _ *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkUnnamedConstraints(t, q, v)
			}},
			{"timestamps/has_created_at", func(t *snapshot.Table, q string, c *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkHasCreatedAt(t, q, c, v)
			}},
			{"timestamps/has_updated_at", func(t *snapshot.Table, q string, c *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkHasUpdatedAt(t, q, c, v)
			}},
			{"timestamps/correct_type", func(t *snapshot.Table, q string, c *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkTimestampType(t, q, c, v)
			}},
			{"partition/too_many_children", func(t *snapshot.Table, q string, _ *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkPartitionTooManyChildren(t, q, v)
			}},
			{"partition/range_gaps", func(t *snapshot.Table, q string, _ *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkPartitionRangeGaps(t, q, v)
			}},
			{"partition/no_default", func(t *snapshot.Table, q string, _ *Config, _ *snapshot.SchemaSnapshot, v *[]Finding) {
				checkPartitionNoDefault(t, q, v)
			}},
		}

		for _, r := range rules {
			if !isDisabled(&effectiveConfig, r.name) {
				r.fn(t, qualified, &effectiveConfig, snap, &violations)
			}
		}
	}

	checkPartitionGUCs(snap, &effectiveConfig, &violations)

	return violations
}

func isDisabled(config *Config, rule string) bool {
	for _, r := range config.DisabledRules {
		if r == rule {
			return true
		}
	}
	return false
}

// Guess dominant naming convention from existing tables
func autoDetectTableNameStyle(tables []snapshot.Table) string {
	var plural, singular int
	for _, t := range tables {
		if !isSnakeCase(t.Name) {
			continue
		}
		if looksPlural(t.Name) {
			plural++
		} else {
			singular++
		}
	}
	if plural > singular {
		return "snake_plural"
	}
	return "snake_singular"
}

func checkTableNameStyle(t *snapshot.Table, qualified string, config *Config, violations *[]Finding) {
	name := t.Name
	valid := true

	style := config.TableNameStyle
	if style == "auto" {
		// resolved by caller already
		return
	}

	switch style {
	case "snake_singular":
		valid = isSnakeCase(name) && !looksPlural(name)
	case "snake_plural":
		valid = isSnakeCase(name)
	case "camelCase":
		valid = regexp.MustCompile(`^[a-z][a-zA-Z0-9]*$`).MatchString(name)
	case "PascalCase":
		valid = regexp.MustCompile(`^[A-Z][a-zA-Z0-9]*$`).MatchString(name)
	case "custom_regex":
		if config.TableNameRegex != nil {
			re, err := regexp.Compile(*config.TableNameRegex)
			if err == nil {
				valid = re.MatchString(name)
			}
		}
	}

	if !valid {
		*violations = append(*violations, Finding{
			Rule:           "naming/table_style",
			Severity:       SeverityWarning,
			Tables:         []string{qualified},
			Message:        fmt.Sprintf("table name '%s' does not match style '%s'", name, config.TableNameStyle),
			Recommendation: fmt.Sprintf("rename to match %s convention", config.TableNameStyle),
			ConventionDoc:  "naming",
		})
	}
}

func checkColumnNameStyle(t *snapshot.Table, qualified string, config *Config, violations *[]Finding) {
	var camelRe *regexp.Regexp
	var customRe *regexp.Regexp

	if config.ColumnNameStyle == "camelCase" {
		camelRe = regexp.MustCompile(`^[a-z][a-zA-Z0-9]*$`)
	}
	if config.ColumnNameRegex != nil {
		if re, err := regexp.Compile(*config.ColumnNameRegex); err == nil {
			customRe = re
		}
	}

	for _, col := range t.Columns {
		valid := true
		switch config.ColumnNameStyle {
		case "snake_case":
			valid = isSnakeCase(col.Name)
		case "camelCase":
			valid = camelRe.MatchString(col.Name)
		case "custom_regex":
			if customRe != nil {
				valid = customRe.MatchString(col.Name)
			}
		}
		if !valid {
			*violations = append(*violations, Finding{
				Rule:           "naming/column_style",
				Severity:       SeverityWarning,
				Tables:         []string{qualified},
				Column:         new(col.Name),
				Message:        fmt.Sprintf("column '%s' does not match style '%s'", col.Name, config.ColumnNameStyle),
				Recommendation: fmt.Sprintf("rename to match %s convention", config.ColumnNameStyle),
				ConventionDoc:  "naming",
			})
		}
	}
}

func checkFKNaming(t *snapshot.Table, qualified string, config *Config, violations *[]Finding) {
	for _, con := range t.Constraints {
		if con.Kind != snapshot.ConstraintForeignKey {
			continue
		}
		expected := strings.ReplaceAll(config.FKPattern, "{table}", t.Name)
		expected = strings.ReplaceAll(expected, "{column}", strings.Join(con.Columns, "_"))
		if con.Name != expected {
			*violations = append(*violations, Finding{
				Rule:           "naming/fk_pattern",
				Severity:       SeverityInfo,
				Tables:         []string{qualified},
				Message:        fmt.Sprintf("FK constraint '%s' doesn't match pattern '%s' (expected '%s')", con.Name, config.FKPattern, expected),
				Recommendation: fmt.Sprintf("rename constraint to '%s'", expected),
				ConventionDoc:  "naming",
			})
		}
	}
}

func checkIndexNaming(t *snapshot.Table, qualified string, config *Config, violations *[]Finding) {
	for _, idx := range t.Indexes {
		if idx.IsPrimary {
			continue
		}
		expected := strings.ReplaceAll(config.IndexPattern, "{table}", t.Name)
		expected = strings.ReplaceAll(expected, "{columns}", strings.Join(idx.Columns, "_"))
		if idx.Name != expected {
			*violations = append(*violations, Finding{
				Rule:           "naming/index_pattern",
				Severity:       SeverityInfo,
				Tables:         []string{qualified},
				Message:        fmt.Sprintf("index '%s' doesn't match pattern '%s' (expected '%s')", idx.Name, config.IndexPattern, expected),
				Recommendation: fmt.Sprintf("rename index to '%s'", expected),
				ConventionDoc:  "naming",
			})
		}
	}
}

func checkPKExists(t *snapshot.Table, qualified string, violations *[]Finding) {
	for _, c := range t.Constraints {
		if c.Kind == snapshot.ConstraintPrimaryKey {
			return
		}
	}
	e := jit.MissingPrimaryKey(qualified)
	*violations = append(*violations, Finding{
		Rule:           "pk/exists",
		Severity:       SeverityError,
		Tables:         []string{qualified},
		Message:        "table has no primary key",
		Recommendation: e.Reason,
		DDLFix:         strp(e.Fix),
		ConventionDoc:  "primary_keys",
	})
}

func checkPKType(t *snapshot.Table, qualified string, config *Config, violations *[]Finding) {
	var acceptedTypes map[string]bool
	var recommend string

	switch config.PKType {
	case "bigint_identity":
		acceptedTypes = map[string]bool{"bigint": true, "int8": true}
		recommend = "use bigint GENERATED ALWAYS AS IDENTITY for primary keys"
	case "int_identity":
		acceptedTypes = map[string]bool{
			"bigint": true, "int8": true,
			"integer": true, "int4": true, "int": true,
		}
		recommend = "use integer GENERATED ALWAYS AS IDENTITY for primary keys"
	default:
		return
	}

	var pk *snapshot.Constraint
	for i := range t.Constraints {
		if t.Constraints[i].Kind == snapshot.ConstraintPrimaryKey {
			pk = &t.Constraints[i]
			break
		}
	}
	if pk == nil {
		return
	}

	for _, pkColName := range pk.Columns {
		var col *snapshot.Column
		for i := range t.Columns {
			if t.Columns[i].Name == pkColName {
				col = &t.Columns[i]
				break
			}
		}
		if col == nil {
			continue
		}

		typeLower := strings.ToLower(col.TypeName)
		isAccepted := acceptedTypes[typeLower]
		isIdentity := col.Identity != nil

		if !isAccepted || !isIdentity {
			identityStr := ""
			if isIdentity {
				identityStr = "(identity) "
			}
			*violations = append(*violations, Finding{
				Rule:           "pk/bigint_identity",
				Severity:       SeverityWarning,
				Tables:         []string{qualified},
				Column:         new(pkColName),
				Message:        fmt.Sprintf("PK column '%s' is %s %s- expected %s with identity", pkColName, col.TypeName, identityStr, config.PKType),
				Recommendation: recommend,
				ConventionDoc:  "primary_keys",
			})
		}
	}
}

func checkTextOverVarchar(t *snapshot.Table, qualified string, config *Config, violations *[]Finding) {
	if !config.PreferTextOverVarchar {
		return
	}
	for _, col := range t.Columns {
		typeLower := strings.ToLower(col.TypeName)
		if strings.HasPrefix(typeLower, "character varying") || strings.HasPrefix(typeLower, "varchar") {
			e := jit.TextOverVarchar(qualified, col.Name)
			*violations = append(*violations, Finding{
				Rule:           "types/text_over_varchar",
				Severity:       SeverityWarning,
				Tables:         []string{qualified},
				Column:         new(col.Name),
				Message:        fmt.Sprintf("column '%s' uses %s - prefer text", col.Name, col.TypeName),
				Recommendation: e.Reason,
				DDLFix:         strp(e.Fix),
				ConventionDoc:  "types",
			})
		}
	}
}

func checkTimestamptz(t *snapshot.Table, qualified string, violations *[]Finding) {
	for _, col := range t.Columns {
		typeLower := strings.ToLower(col.TypeName)
		if typeLower == "timestamp without time zone" || typeLower == "timestamp" {
			e := jit.TimestampToTimestamptz(qualified, col.Name)
			*violations = append(*violations, Finding{
				Rule:           "types/timestamptz",
				Severity:       SeverityWarning,
				Tables:         []string{qualified},
				Column:         new(col.Name),
				Message:        fmt.Sprintf("column '%s' uses timestamp without time zone", col.Name),
				Recommendation: e.Reason + "\n" + e.Note,
				DDLFix:         strp(e.Fix),
				ConventionDoc:  "types",
			})
		}
	}
}

func checkNoSerial(t *snapshot.Table, qualified string, violations *[]Finding) {
	for _, col := range t.Columns {
		if col.Default != nil && strings.Contains(strings.ToLower(*col.Default), "nextval(") {
			*violations = append(*violations, Finding{
				Rule:           "types/no_serial",
				Severity:       SeverityWarning,
				Tables:         []string{qualified},
				Column:         new(col.Name),
				Message:        fmt.Sprintf("column '%s' uses serial/sequence default (%s)", col.Name, *col.Default),
				Recommendation: "use bigint GENERATED ALWAYS AS IDENTITY instead of serial",
				ConventionDoc:  "types",
			})
		}
	}
}

func checkBigintPKFK(t *snapshot.Table, qualified string, config *Config, violations *[]Finding) {
	pkCols := make(map[string]bool)
	fkCols := make(map[string]bool)
	for _, c := range t.Constraints {
		if c.Kind == snapshot.ConstraintPrimaryKey {
			for _, col := range c.Columns {
				pkCols[col] = true
			}
		}
		if c.Kind == snapshot.ConstraintForeignKey {
			for _, col := range c.Columns {
				fkCols[col] = true
			}
		}
	}

	// integer/int4 acceptable when int_identity is configured
	intAllowed := config.PKType == "int_identity"

	for _, col := range t.Columns {
		if !pkCols[col.Name] && !fkCols[col.Name] {
			continue
		}
		typeLower := strings.ToLower(col.TypeName)
		isSmallint := typeLower == "smallint" || typeLower == "int2"
		isInteger := typeLower == "integer" || typeLower == "int4" || typeLower == "int"

		if isSmallint || (isInteger && !intAllowed) {
			*violations = append(*violations, Finding{
				Rule:           "types/bigint_pk_fk",
				Severity:       SeverityWarning,
				Tables:         []string{qualified},
				Column:         new(col.Name),
				Message:        fmt.Sprintf("PK/FK column '%s' uses %s - risk of 32-bit overflow", col.Name, col.TypeName),
				Recommendation: "use bigint for PK and FK columns",
				ConventionDoc:  "types",
			})
		}
	}
}

func checkFKHasIndex(t *snapshot.Table, qualified string, _ *snapshot.SchemaSnapshot, violations *[]Finding) {
	for _, con := range t.Constraints {
		if con.Kind != snapshot.ConstraintForeignKey || len(con.Columns) == 0 {
			continue
		}

		hasCovering := false
		for _, idx := range t.Indexes {
			if len(idx.Columns) < len(con.Columns) {
				continue
			}
			match := true
			for i, fkCol := range con.Columns {
				if idx.Columns[i] != fkCol {
					match = false
					break
				}
			}
			if match {
				hasCovering = true
				break
			}
		}

		if !hasCovering {
			colList := strings.Join(con.Columns, ", ")
			ddl := fmt.Sprintf("CREATE INDEX CONCURRENTLY idx_%s_%s ON %s(%s);",
				t.Name, strings.Join(con.Columns, "_"), qualified, colList)
			*violations = append(*violations, Finding{
				Rule:           "constraints/fk_has_index",
				Severity:       SeverityError,
				Tables:         []string{qualified},
				Column:         new(colList),
				Message:        fmt.Sprintf("FK '%s' on column(s) (%s) has no covering index", con.Name, colList),
				Recommendation: fmt.Sprintf("Add an index on FK columns to avoid sequential scans on DELETE/UPDATE of the referenced table."),
				DDLFix:         strp(ddl),
				ConventionDoc:  "constraints",
			})
		}
	}
}

func checkUnnamedConstraints(t *snapshot.Table, qualified string, violations *[]Finding) {
	for _, con := range t.Constraints {
		isAuto := strings.HasSuffix(con.Name, "_pkey") ||
			strings.HasSuffix(con.Name, "_fkey") ||
			strings.HasSuffix(con.Name, "_key") ||
			strings.HasSuffix(con.Name, "_check") ||
			strings.HasSuffix(con.Name, "_excl")

		if isAuto {
			*violations = append(*violations, Finding{
				Rule:           "constraints/unnamed",
				Severity:       SeverityInfo,
				Tables:         []string{qualified},
				Message:        fmt.Sprintf("constraint '%s' appears to be auto-generated", con.Name),
				Recommendation: "name constraints explicitly for readable error messages",
				ConventionDoc:  "constraints",
			})
		}
	}
}

func checkHasCreatedAt(t *snapshot.Table, qualified string, config *Config, violations *[]Finding) {
	if !config.RequireTimestamps {
		return
	}
	for _, col := range t.Columns {
		if col.Name == "created_at" {
			return
		}
	}
	e := jit.MissingTimestamp(qualified, "created_at")
	*violations = append(*violations, Finding{
		Rule:           "timestamps/has_created_at",
		Severity:       SeverityWarning,
		Tables:         []string{qualified},
		Message:        "table is missing 'created_at' column",
		Recommendation: e.Reason,
		DDLFix:         strp(e.Fix),
		ConventionDoc:  "timestamps",
	})
}

func checkHasUpdatedAt(t *snapshot.Table, qualified string, config *Config, violations *[]Finding) {
	if !config.RequireTimestamps {
		return
	}
	for _, col := range t.Columns {
		if col.Name == "updated_at" {
			return
		}
	}
	e := jit.MissingTimestamp(qualified, "updated_at")
	*violations = append(*violations, Finding{
		Rule:           "timestamps/has_updated_at",
		Severity:       SeverityWarning,
		Tables:         []string{qualified},
		Message:        "table is missing 'updated_at' column",
		Recommendation: e.Reason,
		DDLFix:         strp(e.Fix),
		ConventionDoc:  "timestamps",
	})
}

func checkTimestampType(t *snapshot.Table, qualified string, config *Config, violations *[]Finding) {
	if config.TimestampType != "timestamptz" {
		return
	}
	tsCols := map[string]bool{"created_at": true, "updated_at": true, "deleted_at": true}
	for _, col := range t.Columns {
		if !tsCols[col.Name] {
			continue
		}
		typeLower := strings.ToLower(col.TypeName)
		if typeLower == "timestamp without time zone" || typeLower == "timestamp" {
			*violations = append(*violations, Finding{
				Rule:           "timestamps/correct_type",
				Severity:       SeverityWarning,
				Tables:         []string{qualified},
				Column:         new(col.Name),
				Message:        fmt.Sprintf("timestamp column '%s' uses %s instead of timestamptz", col.Name, col.TypeName),
				Recommendation: "use timestamptz for timestamp columns",
				ConventionDoc:  "timestamps",
			})
		}
	}
}

func checkPartitionTooManyChildren(t *snapshot.Table, qualified string, violations *[]Finding) {
	if t.PartitionInfo == nil {
		return
	}
	n := len(t.PartitionInfo.Children)
	if n > 500 {
		e := jit.PartitionTooManyChildren(qualified, n)
		*violations = append(*violations, Finding{
			Rule:           "partition/too_many_children",
			Severity:       SeverityWarning,
			Tables:         []string{qualified},
			Message:        fmt.Sprintf("table has %d partitions; planning overhead may be significant", n),
			Recommendation: e.Reason + "\n" + e.Note,
			ConventionDoc:  "partitioning",
		})
	}
}

var rangeBoundRe = regexp.MustCompile(`FROM \('([^']+)'\) TO \('([^']+)'\)`)

func checkPartitionRangeGaps(t *snapshot.Table, qualified string, violations *[]Finding) {
	if t.PartitionInfo == nil || t.PartitionInfo.Strategy != snapshot.PartitionRange {
		return
	}

	type bound struct {
		lower, upper string
	}
	var bounds []bound
	for _, child := range t.PartitionInfo.Children {
		m := rangeBoundRe.FindStringSubmatch(child.Bound)
		if m == nil {
			continue
		}
		bounds = append(bounds, bound{lower: m[1], upper: m[2]})
	}

	// sort by lower bound - string compare works for ISO dates and numbers
	for i := 0; i < len(bounds); i++ {
		for j := i + 1; j < len(bounds); j++ {
			if bounds[j].lower < bounds[i].lower {
				bounds[i], bounds[j] = bounds[j], bounds[i]
			}
		}
	}

	for i := 0; i < len(bounds)-1; i++ {
		if bounds[i].upper != bounds[i+1].lower {
			e := jit.PartitionRangeGap(t.Name, bounds[i].upper, bounds[i+1].lower)
			*violations = append(*violations, Finding{
				Rule:     "partition/range_gaps",
				Severity: SeverityWarning,
				Tables:   []string{qualified},
				Message: fmt.Sprintf(
					"gap in partition range between '%s' and '%s'; INSERTs for values in this gap will fail without a DEFAULT partition",
					bounds[i].upper, bounds[i+1].lower),
				Recommendation: e.Reason,
				DDLFix:         strp(e.Fix),
				ConventionDoc:  "partitioning",
			})
		}
	}
}

func checkPartitionNoDefault(t *snapshot.Table, qualified string, violations *[]Finding) {
	if t.PartitionInfo == nil {
		return
	}
	for _, child := range t.PartitionInfo.Children {
		if strings.Contains(strings.ToUpper(child.Bound), "DEFAULT") {
			return
		}
	}
	e := jit.PartitionNoDefault(t.Name)
	*violations = append(*violations, Finding{
		Rule:           "partition/no_default",
		Severity:       SeverityInfo,
		Tables:         []string{qualified},
		Message:        "partitioned table has no DEFAULT partition - INSERTs for unmapped values will fail (might be expected behaviour)",
		Recommendation: e.Reason,
		DDLFix:         strp(e.Fix),
		ConventionDoc:  "partitioning",
	})
}

func parsePartitionKey(key string) []string {
	parts := strings.Split(key, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func checkPartitionGUCs(snap *snapshot.SchemaSnapshot, config *Config, violations *[]Finding) {
	var partitionedCount int
	for _, t := range snap.Tables {
		if t.PartitionInfo != nil {
			partitionedCount++
		}
	}
	if partitionedCount == 0 {
		return
	}

	if findGUC(snap, "enable_partition_pruning") == "off" {
		*violations = append(*violations, Finding{
			Rule:           "partition/pruning_disabled",
			Severity:       SeverityError,
			Tables:         []string{"[global]"},
			Message:        "enable_partition_pruning is OFF; queries on partitioned tables will scan every partition",
			Recommendation: "SET enable_partition_pruning = on",
			ConventionDoc:  "partitioning",
		})
	}

	if partitionedCount > 1 && findGUC(snap, "enable_partitionwise_join") == "off" {
		*violations = append(*violations, Finding{
			Rule:           "partition/partitionwise_join",
			Severity:       SeverityInfo,
			Tables:         []string{"[global]"},
			Message:        "enable_partitionwise_join is OFF (default) causing joins between co-partitioned tables won't use per-partition joins",
			Recommendation: "consider SET enable_partitionwise_join = on",
			ConventionDoc:  "partitioning",
		})
	}

	if findGUC(snap, "enable_partitionwise_aggregate") == "off" {
		*violations = append(*violations, Finding{
			Rule:           "partition/partitionwise_aggregate",
			Severity:       SeverityInfo,
			Tables:         []string{"[global]"},
			Message:        "enable_partitionwise_aggregate is OFF (default); aggregates on partitioned tables won't use per-partition aggregation",
			Recommendation: "consider SET enable_partitionwise_aggregate = on",
			ConventionDoc:  "partitioning",
		})
	}
}

func findGUC(snap *snapshot.SchemaSnapshot, name string) string {
	for _, g := range snap.GUCs {
		if g.Name == name {
			return g.Setting
		}
	}
	return ""
}

func strp(s string) *string { return &s }

var snakeCaseRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func isSnakeCase(s string) bool {
	return snakeCaseRe.MatchString(s)
}

func looksPlural(name string) bool {
	if strings.HasSuffix(name, "s") &&
		!strings.HasSuffix(name, "ss") &&
		!strings.HasSuffix(name, "us") &&
		!strings.HasSuffix(name, "is") &&
		!strings.HasSuffix(name, "ies") {
		return true
	}
	if strings.HasSuffix(name, "ies") && name != "series" {
		return true
	}
	return false
}
