package query

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/boringsql/dryrun/internal/schema"
)

func vacuumIsFull(v *pg_query.VacuumStmt) bool {
	for _, o := range v.GetOptions() {
		d := o.GetDefElem()
		if d == nil || !strings.EqualFold(d.GetDefname(), "full") {
			continue
		}
		// VACUUM (FULL false) is a plain vacuum
		if b := d.GetArg().GetBoolean(); b != nil && !b.GetBoolval() {
			return false
		}
		if s := d.GetArg().GetString_(); s != nil && (strings.EqualFold(s.GetSval(), "false") || strings.EqualFold(s.GetSval(), "off")) {
			return false
		}
		return true
	}
	return false
}

func analyzeVacuum(v *pg_query.VacuumStmt, a *schema.AnnotatedSchema, node *pg_query.Node) MigrationCheck {
	statement := topLevelStatement(node)
	if !vacuumIsFull(v) {
		const rec = "VACUUM takes SHARE UPDATE EXCLUSIVE, so reads and writes continue, but it cannot run inside a transaction block: goose, dbmate and tern wrap a migration in one by default, and the statement then fails at apply. Run it out-of-band."
		return MigrationCheck{
			Operation: "VACUUM", Safety: SafetyCaution,
			LockType: "SHARE UPDATE EXCLUSIVE", LockDuration: "non-blocking",
			Recommendation: rec,
			Rationale:      &Rationale{Reason: rec},
			Statement:      statement,
		}
	}
	return rewriteMaintenance("VACUUM FULL", "VACUUM FULL rewrites the table into a new file under ACCESS EXCLUSIVE: all reads and writes block for the whole rewrite, and disk holds both copies until it finishes. Prefer pg_repack, or plain VACUUM when the goal is only dead-tuple cleanup.",
		relationsOf(v.GetRels()), a, statement)
}

func analyzeCluster(c *pg_query.ClusterStmt, a *schema.AnnotatedSchema, node *pg_query.Node) MigrationCheck {
	var rels []*pg_query.RangeVar
	if c.GetRelation() != nil {
		rels = append(rels, c.GetRelation())
	}
	return rewriteMaintenance("CLUSTER", "CLUSTER rewrites the table in index order under ACCESS EXCLUSIVE: all reads and writes block for the whole rewrite, and disk holds both copies until it finishes. It also takes the table out of service for every table when run without a name. Prefer pg_repack.",
		rels, a, topLevelStatement(node))
}

// The rewrite holds ACCESS EXCLUSIVE for the whole read: rate it by size like CREATE INDEX.
func rewriteMaintenance(operation, reason string, rels []*pg_query.RangeVar, a *schema.AnnotatedSchema, statement string) MigrationCheck {
	check := MigrationCheck{
		Operation: operation, Safety: SafetyDangerous,
		LockType: "ACCESS EXCLUSIVE", LockDuration: "proportional to table size (full rewrite)",
		Statement: statement,
	}
	rec := reason
	note := ""
	if len(rels) == 1 {
		qual := schema.QualifiedName{Schema: schemaOf(rels[0]), Name: rels[0].GetRelname()}
		size, rows, small, sizing := lookupTableStats(a, qual)
		check.Table = strp(relationName(rels[0]))
		check.TableSize, check.RowEstimate = size, rows
		if small {
			check.Safety = SafetyCaution
			lead := smallTableNote(*rows, *size)
			rec = lead + "\n\n" + reason
			note = lead
		} else if ctx, caveat := sizingCaveat(sizing); ctx != "" {
			check.SizingContext = ctx
			note = caveat
		}
	} else {
		note = "No single target table: every table in scope is rewritten in turn."
	}
	check.Recommendation = rec
	check.Rationale = &Rationale{Reason: reason, Note: note}
	return check
}

func analyzeTruncate(t *pg_query.TruncateStmt, node *pg_query.Node) MigrationCheck {
	rels := relationsOf(t.GetRelations())
	names := make([]string, 0, len(rels))
	for _, r := range rels {
		names = append(names, relationName(r))
	}
	safety := SafetyCaution
	rec := "TRUNCATE is instant but removes every row under ACCESS EXCLUSIVE, and cannot be undone once committed. Confirm the target before applying."
	if t.GetBehavior() == pg_query.DropBehavior_DROP_CASCADE {
		safety = SafetyDangerous
		rec = "TRUNCATE ... CASCADE also empties every table with a foreign key to the target, and those are not named here. Truncate the referencing tables explicitly instead."
	}
	check := MigrationCheck{
		Operation: "TRUNCATE", Safety: safety,
		LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (data removed, no scan)",
		Recommendation: rec,
		Rationale:      &Rationale{Reason: rec},
		Statement:      topLevelStatement(node),
	}
	if len(names) > 0 {
		check.Table = strp(strings.Join(names, ", "))
	}
	return check
}

func analyzeCreateTrigger(t *pg_query.CreateTrigStmt, node *pg_query.Node) MigrationCheck {
	const rec = "CREATE TRIGGER takes SHARE ROW EXCLUSIVE: writes to the table block until it commits, reads continue. No table scan, so the lock is brief unless it queues behind a long transaction; set lock_timeout and retry."
	check := MigrationCheck{
		Operation: "CREATE TRIGGER", Safety: SafetySafe,
		LockType: "SHARE ROW EXCLUSIVE", LockDuration: "brief (metadata-only)",
		Recommendation: rec,
		Rationale:      &Rationale{Reason: rec},
		Statement:      topLevelStatement(node),
	}
	if t.GetRelation() != nil {
		check.Table = strp(relationName(t.GetRelation()))
	}
	return check
}

func analyzeRefreshMatView(r *pg_query.RefreshMatViewStmt, node *pg_query.Node) MigrationCheck {
	check := MigrationCheck{Operation: "REFRESH MATERIALIZED VIEW", Statement: topLevelStatement(node)}
	if r.GetRelation() != nil {
		check.Table = strp(relationName(r.GetRelation()))
	}
	var rec string
	switch {
	case r.GetSkipData():
		check.Safety = SafetyCaution
		check.LockType, check.LockDuration = "ACCESS EXCLUSIVE", "brief (data dropped, no scan)"
		rec = "WITH NO DATA empties the view and makes it unscannable until the next refresh: every query against it errors."
	case r.GetConcurrent():
		check.Safety = SafetyCaution
		check.LockType, check.LockDuration = "EXCLUSIVE", "proportional to the view's query (reads continue, writes to the view block)"
		rec = "REFRESH ... CONCURRENTLY keeps the view readable but needs a unique index on it and rebuilds into a temporary table first. It runs for the full cost of the defining query."
	default:
		check.Safety = SafetyDangerous
		check.LockType, check.LockDuration = "ACCESS EXCLUSIVE", "proportional to the view's query"
		rec = "REFRESH MATERIALIZED VIEW blocks every read of the view for as long as the defining query runs. Use CONCURRENTLY (needs a unique index) for a view that is read while it refreshes."
	}
	check.Recommendation = rec
	check.Rationale = &Rationale{Reason: rec}
	return check
}

func relationsOf(nodes []*pg_query.Node) []*pg_query.RangeVar {
	var out []*pg_query.RangeVar
	for _, n := range nodes {
		switch v := n.GetNode().(type) {
		case *pg_query.Node_RangeVar:
			out = append(out, v.RangeVar)
		case *pg_query.Node_VacuumRelation:
			if v.VacuumRelation.GetRelation() != nil {
				out = append(out, v.VacuumRelation.GetRelation())
			}
		}
	}
	return out
}
