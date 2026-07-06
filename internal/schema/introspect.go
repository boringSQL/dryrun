package schema

import (
	"context"
	"embed"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/boringsql/queries"
	"github.com/jackc/pgx/v5"
)

// scanAll wraps the standard rows.Next loop. The scan callback receives the
// already-positioned rows and returns the decoded value.
func scanAll[T any](rows pgx.Rows, scan func(pgx.Rows) (T, error)) ([]T, error) {
	defer rows.Close()
	var out []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func query(ctx context.Context, pool Querier, name string) (pgx.Rows, error) {
	return pool.Query(ctx, q(name))
}

//go:embed sql/*.sql
var sqlFS embed.FS

var store *queries.QueryStore

func init() {
	store = queries.NewQueryStore()
	if err := store.LoadFromEmbed(sqlFS, "sql"); err != nil {
		panic(fmt.Sprintf("failed to load embedded SQL: %v", err))
	}
}

func q(name string) string {
	return store.MustHaveQuery(name).Query()
}

// DDL-only introspection; planner/activity stats now flow through CapturePlannerStats / CaptureActivityStats
func IntrospectSchema(ctx context.Context, pool Querier) (*SchemaSnapshot, error) {
	var pgVersion string
	if err := pool.QueryRow(ctx, "SELECT version()").Scan(&pgVersion); err != nil {
		return nil, fmt.Errorf("query pg version: %w", err)
	}

	var database string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&database); err != nil {
		return nil, fmt.Errorf("query current database: %w", err)
	}

	// table-centric
	rawTables, err := fetchTables(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch tables: %w", err)
	}
	rawColumns, err := fetchColumns(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch columns: %w", err)
	}
	rawConstraints, err := fetchConstraints(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch constraints: %w", err)
	}
	tableComments, err := fetchTableComments(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch table comments: %w", err)
	}
	columnComments, err := fetchColumnComments(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch column comments: %w", err)
	}
	rawIndexes, err := fetchIndexes(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch indexes: %w", err)
	}
	rawPartitions, err := fetchPartitionInfo(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch partition info: %w", err)
	}
	rawPartitionChildren, err := fetchPartitionChildren(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch partition children: %w", err)
	}
	rawPolicies, err := fetchPolicies(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch policies: %w", err)
	}
	rawTriggers, err := fetchTriggers(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch triggers: %w", err)
	}

	// top-level objects
	enums, err := fetchEnums(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch enums: %w", err)
	}
	domains, err := fetchDomains(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch domains: %w", err)
	}
	composites, err := fetchComposites(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch composites: %w", err)
	}
	views, err := fetchViews(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch views: %w", err)
	}
	functions, err := fetchFunctions(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch functions: %w", err)
	}
	extensions, err := fetchExtensions(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch extensions: %w", err)
	}
	gucs, err := fetchGUCs(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch gucs: %w", err)
	}

	flavor := detectFlavor(ctx, pool, extensions)

	tables := assembleTables(
		rawTables,
		rawColumns,
		rawConstraints,
		tableComments,
		columnComments,
		rawIndexes,
		rawPartitions,
		rawPartitionChildren,
		rawPolicies,
		rawTriggers,
	)

	snap := &SchemaSnapshot{
		FormatVersion: FormatVersion,
		PgVersion:     pgVersion,
		Flavor:        flavor,
		Database:      database,
		Timestamp:     time.Now().UTC(),
		Tables:        tables,
		Enums:         enums,
		Domains:       domains,
		Composites:    composites,
		Views:         views,
		Functions:     functions,
		Extensions:    extensions,
		GUCs:          gucs,
	}
	snap.ContentHash = ComputeContentHash(snap)
	return snap, nil
}

// Raw row types for intermediate grouping

type (
	rawTable struct {
		oid        uint32
		schema     string
		name       string
		rlsEnabled bool
		reloptions []string
	}

	rawColumn struct {
		tableOID         uint32
		name             string
		ordinal          int16
		typeName         string
		nullable         bool
		dflt             *string
		identity         *string
		statisticsTarget *int16
		generated        *string
	}

	rawConstraint struct {
		tableOID     uint32
		name         string
		contype      string
		columns      []string
		definition   *string
		fkTable      *string
		fkColumns    []string
		backingIndex *string
		comment      *string
	}

	rawTableComment struct {
		tableOID uint32
		comment  string
	}

	rawColumnComment struct {
		tableOID   uint32
		columnName string
		comment    string
	}

	rawIndex struct {
		tableOID        uint32
		name            string
		columns         []string
		includeColumns  []string
		indexType       string
		isUnique        bool
		isPrimary       bool
		predicate       *string
		definition      string
		isValid         bool
		isReady         bool
		backsConstraint bool
		hasExpressions  bool
	}

	rawPartitionInfo struct {
		tableOID uint32
		strategy string
		key      string
	}

	rawPartitionChild struct {
		parentOID uint32
		schema    string
		name      string
		bound     string
	}

	rawPolicy struct {
		tableOID      uint32
		name          string
		command       string
		permissive    bool
		roles         []string
		usingExpr     *string
		withCheckExpr *string
	}

	rawTrigger struct {
		tableOID   uint32
		name       string
		definition string
	}
)

// Fetchers - each uses a named query from sql/introspect.sql

func fetchTables(ctx context.Context, pool Querier) ([]rawTable, error) {
	rows, err := query(ctx, pool, "fetch-tables")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (rawTable, error) {
		var oid int32
		var rt rawTable
		err := r.Scan(&oid, &rt.schema, &rt.name, &rt.rlsEnabled, &rt.reloptions)
		rt.oid = uint32(oid)
		return rt, err
	})
}

func fetchColumns(ctx context.Context, pool Querier) ([]rawColumn, error) {
	rows, err := query(ctx, pool, "fetch-columns")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (rawColumn, error) {
		var oid int32
		var rc rawColumn
		err := r.Scan(&oid, &rc.name, &rc.ordinal, &rc.typeName, &rc.nullable, &rc.dflt, &rc.identity, &rc.statisticsTarget, &rc.generated)
		rc.tableOID = uint32(oid)
		return rc, err
	})
}

func fetchConstraints(ctx context.Context, pool Querier) ([]rawConstraint, error) {
	rows, err := query(ctx, pool, "fetch-constraints")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (rawConstraint, error) {
		var oid int32
		var rc rawConstraint
		err := r.Scan(&oid, &rc.name, &rc.contype, &rc.definition, &rc.columns, &rc.fkTable, &rc.fkColumns, &rc.backingIndex, &rc.comment)
		rc.tableOID = uint32(oid)
		return rc, err
	})
}

func fetchTableComments(ctx context.Context, pool Querier) ([]rawTableComment, error) {
	rows, err := query(ctx, pool, "fetch-table-comments")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (rawTableComment, error) {
		var oid int32
		var tc rawTableComment
		err := r.Scan(&oid, &tc.comment)
		tc.tableOID = uint32(oid)
		return tc, err
	})
}

func fetchColumnComments(ctx context.Context, pool Querier) ([]rawColumnComment, error) {
	rows, err := query(ctx, pool, "fetch-column-comments")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (rawColumnComment, error) {
		var oid int32
		var cc rawColumnComment
		err := r.Scan(&oid, &cc.columnName, &cc.comment)
		cc.tableOID = uint32(oid)
		return cc, err
	})
}

func fetchEnums(ctx context.Context, pool Querier) ([]EnumType, error) {
	rows, err := query(ctx, pool, "fetch-enums")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (EnumType, error) {
		var e EnumType
		err := r.Scan(&e.Schema, &e.Name, &e.Labels)
		return e, err
	})
}

func fetchDomains(ctx context.Context, pool Querier) ([]DomainType, error) {
	rows, err := query(ctx, pool, "fetch-domains")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (DomainType, error) {
		var d DomainType
		var notnull bool
		err := r.Scan(&d.Schema, &d.Name, &d.BaseType, &notnull, &d.Default, &d.CheckConstraints)
		d.Nullable = !notnull
		return d, err
	})
}

func fetchComposites(ctx context.Context, pool Querier) ([]CompositeType, error) {
	rows, err := pool.Query(ctx, q("fetch-composites"))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type compKey struct {
		schema, name string
	}
	fieldsByKey := make(map[compKey][]CompositeField)
	var order []compKey
	seen := make(map[compKey]bool)

	for rows.Next() {
		var (
			schemaName string
			typeName   string
			f          CompositeField
		)
		if err := rows.Scan(&schemaName, &typeName, &f.Name, &f.TypeName); err != nil {
			return nil, err
		}
		k := compKey{schemaName, typeName}
		fieldsByKey[k] = append(fieldsByKey[k], f)
		if !seen[k] {
			seen[k] = true
			order = append(order, k)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]CompositeType, 0, len(order))
	for _, k := range order {
		out = append(out, CompositeType{
			Schema: k.schema,
			Name:   k.name,
			Fields: fieldsByKey[k],
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Schema != out[j].Schema {
			return out[i].Schema < out[j].Schema
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func fetchIndexes(ctx context.Context, pool Querier) ([]rawIndex, error) {
	rows, err := query(ctx, pool, "fetch-indexes")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (rawIndex, error) {
		var (
			oid      int32
			ri       rawIndex
			nKeyAtts int16
			allCols  []string
		)
		if err := r.Scan(
			&oid, &ri.name, &ri.indexType,
			&ri.isUnique, &ri.isPrimary, &ri.predicate,
			&ri.definition, &nKeyAtts, &ri.isValid, &ri.isReady,
			&ri.backsConstraint, &ri.hasExpressions, &allCols,
		); err != nil {
			return ri, err
		}
		ri.tableOID = uint32(oid)
		n := int(nKeyAtts)
		if n > 0 && n <= len(allCols) {
			ri.columns = allCols[:n]
			ri.includeColumns = allCols[n:]
		} else {
			ri.columns = allCols
		}
		return ri, nil
	})
}

func fetchPartitionInfo(ctx context.Context, pool Querier) ([]rawPartitionInfo, error) {
	rows, err := query(ctx, pool, "fetch-partition-info")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (rawPartitionInfo, error) {
		var oid int32
		var rp rawPartitionInfo
		err := r.Scan(&oid, &rp.strategy, &rp.key)
		rp.tableOID = uint32(oid)
		return rp, err
	})
}

func fetchPartitionChildren(ctx context.Context, pool Querier) ([]rawPartitionChild, error) {
	rows, err := query(ctx, pool, "fetch-partition-children")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (rawPartitionChild, error) {
		var oid int32
		var pc rawPartitionChild
		var bound *string
		if err := r.Scan(&oid, &pc.schema, &pc.name, &bound); err != nil {
			return pc, err
		}
		pc.parentOID = uint32(oid)
		if bound != nil {
			pc.bound = *bound
		}
		return pc, nil
	})
}

func fetchPolicies(ctx context.Context, pool Querier) ([]rawPolicy, error) {
	rows, err := query(ctx, pool, "fetch-policies")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (rawPolicy, error) {
		var oid int32
		var rp rawPolicy
		err := r.Scan(&oid, &rp.name, &rp.command, &rp.permissive, &rp.roles, &rp.usingExpr, &rp.withCheckExpr)
		rp.tableOID = uint32(oid)
		return rp, err
	})
}

func fetchTriggers(ctx context.Context, pool Querier) ([]rawTrigger, error) {
	rows, err := query(ctx, pool, "fetch-triggers")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (rawTrigger, error) {
		var oid int32
		var rt rawTrigger
		err := r.Scan(&oid, &rt.name, &rt.definition)
		rt.tableOID = uint32(oid)
		return rt, err
	})
}

func fetchViews(ctx context.Context, pool Querier) ([]View, error) {
	rows, err := query(ctx, pool, "fetch-views")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (View, error) {
		var v View
		var def *string
		if err := r.Scan(&v.Schema, &v.Name, &v.IsMaterialized, &def, &v.Comment); err != nil {
			return v, err
		}
		if def != nil {
			v.Definition = *def
		}
		return v, nil
	})
}

func fetchFunctions(ctx context.Context, pool Querier) ([]Function, error) {
	rows, err := query(ctx, pool, "fetch-functions")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (Function, error) {
		var f Function
		var volStr string
		var returnType *string
		if err := r.Scan(
			&f.Schema, &f.Name, &f.IdentityArgs,
			&returnType, &f.Language, &volStr,
			&f.SecurityDefiner, &f.Comment,
		); err != nil {
			return f, err
		}
		if returnType != nil {
			f.ReturnType = *returnType
		}
		if vol, ok := VolatilityFromPg(volStr); ok {
			f.Volatility = vol
		} else {
			f.Volatility = VolatilityVolatile
		}
		return f, nil
	})
}

// alloydb_omni* is Omni-only; do NOT broaden to omni_%, managed has an unrelated
// omni_enable_post_startup_helper GUC that would misclassify it as Omni.
// Shared as a bare expression so the probe and detectFlavor queries stay in sync.
const omniMarkerExpr = `EXISTS (
	SELECT 1 FROM pg_catalog.pg_settings WHERE name LIKE 'alloydb\_omni%'
)`

// advisory: a probe failure degrades to the fail-safe default, not an error
func detectFlavor(ctx context.Context, pool Querier, exts []Extension) Flavor {
	hasColumnar := false
	for _, e := range exts {
		if e.Name == "google_columnar_engine" {
			hasColumnar = true
			break
		}
	}

	var omniMarker bool
	if hasColumnar {
		if err := pool.QueryRow(ctx, "SELECT "+omniMarkerExpr).Scan(&omniMarker); err != nil {
			slog.Warn("omni-marker probe failed, treating AlloyDB as managed", "err", err)
		}
	}

	return DetectFlavor(FlavorSignals{HasColumnarEngine: hasColumnar, OmniMarker: omniMarker})
}

// one-round-trip flavor of a live target, for callers without a fetched snapshot
func DetectFlavorLive(ctx context.Context, q Querier) (Flavor, error) {
	var hasColumnar, omniMarker bool
	if err := q.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'google_columnar_engine'), `+omniMarkerExpr,
	).Scan(&hasColumnar, &omniMarker); err != nil {
		return "", err
	}
	return DetectFlavor(FlavorSignals{HasColumnarEngine: hasColumnar, OmniMarker: omniMarker}), nil
}

func fetchExtensions(ctx context.Context, pool Querier) ([]Extension, error) {
	rows, err := query(ctx, pool, "fetch-extensions")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (Extension, error) {
		var e Extension
		err := r.Scan(&e.Name, &e.Version, &e.Schema)
		return e, err
	})
}

func fetchGUCs(ctx context.Context, pool Querier) ([]GucSetting, error) {
	rows, err := query(ctx, pool, "fetch-gucs")
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (GucSetting, error) {
		var g GucSetting
		err := r.Scan(&g.Name, &g.Setting, &g.Unit)
		return g, err
	})
}

// Assembly: merge parts into Table structs

type colKey struct {
	oid  uint32
	name string
}

func assembleTables(
	rawTables []rawTable,
	rawColumns []rawColumn,
	rawConstraints []rawConstraint,
	tableComments []rawTableComment,
	columnComments []rawColumnComment,
	rawIndexes []rawIndex,
	rawPartitions []rawPartitionInfo,
	rawPartitionChildren []rawPartitionChild,
	rawPolicies []rawPolicy,
	rawTriggers []rawTrigger,
) []Table {
	// Columns
	columnsByOID := make(map[uint32][]Column)
	for _, rc := range rawColumns {
		columnsByOID[rc.tableOID] = append(columnsByOID[rc.tableOID], Column{
			Name:             rc.name,
			Ordinal:          rc.ordinal,
			TypeName:         rc.typeName,
			Nullable:         rc.nullable,
			Default:          rc.dflt,
			Identity:         rc.identity,
			StatisticsTarget: rc.statisticsTarget,
			Generated:        rc.generated,
		})
	}

	// Constraints
	constraintsByOID := make(map[uint32][]Constraint)
	for _, rc := range rawConstraints {
		kind, ok := ConstraintKindFromPg(rc.contype)
		if !ok {
			continue
		}
		constraintsByOID[rc.tableOID] = append(constraintsByOID[rc.tableOID], Constraint{
			Name:         rc.name,
			Kind:         kind,
			Columns:      rc.columns,
			Definition:   rc.definition,
			FKTable:      rc.fkTable,
			FKColumns:    rc.fkColumns,
			BackingIndex: rc.backingIndex,
			Comment:      rc.comment,
		})
	}

	// Table comments
	tableCommentMap := make(map[uint32]string, len(tableComments))
	for _, tc := range tableComments {
		tableCommentMap[tc.tableOID] = tc.comment
	}

	// Column comments
	colCommentMap := make(map[colKey]string, len(columnComments))
	for _, cc := range columnComments {
		colCommentMap[colKey{cc.tableOID, cc.columnName}] = cc.comment
	}
	for oid, cols := range columnsByOID {
		for i := range cols {
			if comment, ok := colCommentMap[colKey{oid, cols[i].Name}]; ok {
				columnsByOID[oid][i].Comment = &comment
			}
		}
	}

	// Indexes
	indexesByOID := make(map[uint32][]Index)
	for _, ri := range rawIndexes {
		indexesByOID[ri.tableOID] = append(indexesByOID[ri.tableOID], Index{
			Name:            ri.name,
			Columns:         ri.columns,
			IncludeColumns:  ri.includeColumns,
			IndexType:       ri.indexType,
			IsUnique:        ri.isUnique,
			IsPrimary:       ri.isPrimary,
			Predicate:       ri.predicate,
			Definition:      ri.definition,
			IsValid:         ri.isValid,
			IsReady:         ri.isReady,
			BacksConstraint: ri.backsConstraint,
			HasExpressions:  ri.hasExpressions,
		})
	}

	// Partition info
	childrenByParent := make(map[uint32][]PartitionChild)
	for _, pc := range rawPartitionChildren {
		childrenByParent[pc.parentOID] = append(childrenByParent[pc.parentOID], PartitionChild{
			Schema: pc.schema,
			Name:   pc.name,
			Bound:  pc.bound,
		})
	}

	partInfoByOID := make(map[uint32]PartitionInfo)
	for _, rp := range rawPartitions {
		strategy, ok := PartitionStrategyFromPg(rp.strategy)
		if !ok {
			continue
		}
		partInfoByOID[rp.tableOID] = PartitionInfo{
			Strategy: strategy,
			Key:      rp.key,
			Children: childrenByParent[rp.tableOID],
		}
	}

	// Policies
	policiesByOID := make(map[uint32][]RlsPolicy)
	for _, rp := range rawPolicies {
		policiesByOID[rp.tableOID] = append(policiesByOID[rp.tableOID], RlsPolicy{
			Name:          rp.name,
			Command:       rp.command,
			Permissive:    rp.permissive,
			Roles:         rp.roles,
			UsingExpr:     rp.usingExpr,
			WithCheckExpr: rp.withCheckExpr,
		})
	}

	// Triggers
	triggersByOID := make(map[uint32][]Trigger)
	for _, rt := range rawTriggers {
		triggersByOID[rt.tableOID] = append(triggersByOID[rt.tableOID], Trigger{
			Name:       rt.name,
			Definition: rt.definition,
		})
	}

	// Assemble
	tables := make([]Table, 0, len(rawTables))
	for _, rt := range rawTables {
		t := Table{
			OID:         rt.oid,
			Schema:      rt.schema,
			Name:        rt.name,
			Columns:     columnsByOID[rt.oid],
			Constraints: constraintsByOID[rt.oid],
			Indexes:     indexesByOID[rt.oid],
			Policies:    policiesByOID[rt.oid],
			Triggers:    triggersByOID[rt.oid],
			RLSEnabled:  rt.rlsEnabled,
			Reloptions:  rt.reloptions,
		}
		if comment, ok := tableCommentMap[rt.oid]; ok {
			t.Comment = &comment
		}
		if pi, ok := partInfoByOID[rt.oid]; ok {
			t.PartitionInfo = &pi
		}
		tables = append(tables, t)
	}
	return tables
}
