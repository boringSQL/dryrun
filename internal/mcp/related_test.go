package mcp

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

func fk(name string, cols []string, refTable string, refCols []string, action string) schema.Constraint {
	def := fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s(%s)",
		strings.Join(cols, ", "), refTable, strings.Join(refCols, ", "))
	if action != "" {
		def += " " + action
	}
	return schema.Constraint{
		Name: name, Kind: schema.ConstraintForeignKey,
		Columns: cols, FKTable: &refTable, FKColumns: refCols, Definition: &def,
	}
}

func col(name string) schema.Column { return schema.Column{Name: name, TypeName: "bigint"} }

// One parent with children covering every ON DELETE action, a composite key, a
// partitioned child, and a name that has to be quoted.
func relatedSnap() *schema.SchemaSnapshot {
	parts := make([]schema.PartitionChild, 0, 4)
	tables := []schema.Table{
		{
			Schema: "public", Name: "users",
			Columns:     []schema.Column{col("user_id"), col("tenant_id")},
			Constraints: []schema.Constraint{fk("users_org_fkey", []string{"org_id"}, "public.orgs", []string{"org_id"}, "ON DELETE RESTRICT ON UPDATE CASCADE")},
		},
		{Schema: "public", Name: "orgs", Columns: []schema.Column{col("org_id")}},
		{Schema: "public", Name: "loners", Columns: []schema.Column{col("id")}},
		{
			Schema: "public", Name: "sessions",
			Columns:     []schema.Column{col("user_id")},
			Constraints: []schema.Constraint{fk("sessions_user_fkey", []string{"user_id"}, "public.users", []string{"user_id"}, "ON DELETE CASCADE")},
		},
		{
			Schema: "public", Name: "invoices",
			Columns:     []schema.Column{col("user_id")},
			Constraints: []schema.Constraint{fk("invoices_user_fkey", []string{"user_id"}, "public.users", []string{"user_id"}, "ON DELETE SET NULL")},
		},
		{
			Schema: "public", Name: "audits",
			Columns:     []schema.Column{col("user_id")},
			Constraints: []schema.Constraint{fk("audits_user_fkey", []string{"user_id"}, "public.users", []string{"user_id"}, "")},
		},
		{
			// composite, and a target that needs quoting
			Schema: "app", Name: "Memberships",
			Columns: []schema.Column{col("user_id"), col("tenant_id")},
			Constraints: []schema.Constraint{
				fk("memberships_user_fkey", []string{"user_id", "tenant_id"}, "public.users", []string{"user_id", "tenant_id"}, "ON DELETE CASCADE"),
			},
		},
		{
			Schema: "public", Name: "events",
			Columns:     []schema.Column{col("user_id")},
			Constraints: []schema.Constraint{fk("events_user_fkey", []string{"user_id"}, "public.users", []string{"user_id"}, "ON DELETE CASCADE")},
		},
	}
	// every partition carries its own copy of the parent's foreign key, and they
	// are listed ahead of the parent so folding cannot depend on table order
	var partTables []schema.Table
	for i := 1; i <= 4; i++ {
		name := fmt.Sprintf("events_p%d", i)
		parts = append(parts, schema.PartitionChild{Schema: "public", Name: name})
		partTables = append(partTables, schema.Table{
			Schema: "public", Name: name,
			Columns:     []schema.Column{col("user_id")},
			Constraints: []schema.Constraint{fk(name+"_user_fkey", []string{"user_id"}, "public.users", []string{"user_id"}, "ON DELETE CASCADE")},
		})
	}
	for i := range tables {
		if tables[i].Name == "events" {
			tables[i].PartitionInfo = &schema.PartitionInfo{
				Strategy: schema.PartitionRange, Key: "RANGE (created_at)", Children: parts,
			}
		}
	}
	tables = append(partTables, tables...)
	return &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "test",
		Timestamp: time.Now().UTC(), ContentHash: "test", Tables: tables,
	}
}

// relations used to be its own find_related tool; it is now describe_table's
// relations section, with the same shape one level down.
func relationsSectionOf(t *testing.T, out string) findRelatedResult {
	t.Helper()
	var envelope struct {
		Relations findRelatedResult `json:"relations"`
		Meta      *toolMeta         `json:"_meta"`
	}
	if err := json.Unmarshal([]byte(out), &envelope); err != nil {
		t.Fatalf("expected the declared shape: %v\n%s", err, out)
	}
	// the hint and next now ride the describe_table envelope, one level up from
	// the section; lift them so the assertions below still read one object
	res := envelope.Relations
	res.Meta = envelope.Meta
	return res
}

func relationArgs(args map[string]any) map[string]any {
	merged := map[string]any{"detail": "relations"}
	for k, v := range args {
		merged[k] = v
	}
	return merged
}

func related(t *testing.T, args map[string]any) findRelatedResult {
	t.Helper()
	c := serveOffline(t, NewOfflineServerAnnotated(&schema.AnnotatedSchema{Schema: relatedSnap()}, lint.DefaultConfig()))
	return relationsSectionOf(t, callTool(t, c, "describe_table", relationArgs(args)))
}

func edgeFor(t *testing.T, edges []relatedEdge, table string) relatedEdge {
	t.Helper()
	for _, e := range edges {
		if e.Table == table {
			return e
		}
	}
	t.Fatalf("no edge for %s in %+v", table, edges)
	return relatedEdge{}
}

func TestFindRelatedEdges(t *testing.T) {
	res := related(t, map[string]any{"table": "users"})

	if res.Table != "public.users" {
		t.Errorf("table = %q", res.Table)
	}
	if len(res.Outgoing) != 1 || res.OutgoingCount != 1 {
		t.Fatalf("outgoing = %+v", res.Outgoing)
	}

	out := res.Outgoing[0]
	if out.Table != "public.orgs" || out.Constraint != "users_org_fkey" {
		t.Errorf("outgoing edge = %+v", out)
	}
	if out.Join != "JOIN public.orgs ON public.orgs.org_id = public.users.org_id" {
		t.Errorf("outgoing join = %q", out.Join)
	}
	// the action reads off the definition, and both clauses are present here
	if out.OnDelete != "RESTRICT" || out.OnUpdate != "CASCADE" {
		t.Errorf("outgoing actions = %s / %s", out.OnDelete, out.OnUpdate)
	}

	in := edgeFor(t, res.Incoming, "public.sessions")
	if in.OnDelete != "CASCADE" {
		t.Errorf("sessions on_delete = %q", in.OnDelete)
	}
	if in.Join != "JOIN public.sessions ON public.sessions.user_id = public.users.user_id" {
		t.Errorf("sessions join = %q", in.Join)
	}
	// columns are the ones on the table asked about, ref_columns on the other
	if strings.Join(in.Columns, ",") != "user_id" || strings.Join(in.RefColumns, ",") != "user_id" {
		t.Errorf("sessions columns = %v / %v", in.Columns, in.RefColumns)
	}
}

// Postgres leaves the clause out when it is the default; an agent should not
// have to know that absent means NO ACTION.
func TestFindRelatedDefaultsToNoAction(t *testing.T) {
	res := related(t, map[string]any{"table": "users"})
	e := edgeFor(t, res.Incoming, "public.audits")
	if e.OnDelete != "NO ACTION" || e.OnUpdate != "NO ACTION" {
		t.Errorf("audits actions = %s / %s", e.OnDelete, e.OnUpdate)
	}
}

func TestFindRelatedCompositeAndQuotedNames(t *testing.T) {
	res := related(t, map[string]any{"table": "users"})
	e := edgeFor(t, res.Incoming, `app."Memberships"`)
	want := `JOIN app."Memberships" ON app."Memberships".user_id = public.users.user_id AND app."Memberships".tenant_id = public.users.tenant_id`
	if e.Join != want {
		t.Errorf("composite join:\n got: %s\nwant: %s", e.Join, want)
	}
}

// The one question this tool exists to answer.
func TestFindRelatedSaysWhatADeleteDoes(t *testing.T) {
	c := serveOffline(t, NewOfflineServerAnnotated(&schema.AnnotatedSchema{Schema: relatedSnap()}, lint.DefaultConfig()))
	out := callTool(t, c, "describe_table", map[string]any{"table": "users", "detail": "relations"})

	var decoded map[string]any
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatal(err)
	}
	meta, _ := decoded["_meta"].(map[string]any)
	hint, _ := meta["hint"].(string)

	for _, want := range []string{
		`deletes matching rows in app."Memberships", public.events, public.sessions`,
		"clears the referencing column in public.invoices",
		"fails while rows exist in public.audits",
	} {
		if !strings.Contains(hint, want) {
			t.Errorf("hint missing %q:\n%s", want, hint)
		}
	}

	// one hop only, so the next hop is offered rather than implied
	next, _ := meta["next"].([]any)
	if len(next) != 1 {
		t.Fatalf("expected a follow-up, got %v", meta["next"])
	}
	call, _ := next[0].(map[string]any)
	args, _ := call["args"].(map[string]any)
	if call["tool"] != "describe_table" || args["table"] != "Memberships" ||
		args["schema"] != "app" || args["detail"] != "relations" {
		t.Errorf("follow-up should chase a cascade target, got %v", call)
	}

	// the follow-up is advertised as pre-validated, so it has to resolve
	chased := callTool(t, c, "describe_table", args)
	if !strings.Contains(chased, `"table": "app.\"Memberships\""`) {
		t.Errorf("the follow-up call does not resolve:\n%.400s", chased)
	}
}

func TestFindRelatedQuietWithoutForeignKeys(t *testing.T) {
	c := serveOffline(t, NewOfflineServerAnnotated(&schema.AnnotatedSchema{Schema: relatedSnap()}, lint.DefaultConfig()))
	out := callTool(t, c, "describe_table", map[string]any{"table": "loners", "detail": "relations"})
	if !strings.Contains(out, "Relations enforced in application code do not appear") {
		t.Errorf("expected the no-FK note:\n%s", out)
	}
}

// A partitioned table declares the key once per partition; the relationship is
// one relationship.
func TestFindRelatedFoldsPartitionsIntoTheParent(t *testing.T) {
	res := related(t, map[string]any{"table": "users"})
	for _, e := range res.Incoming {
		if strings.Contains(e.Table, "events_p") {
			t.Fatalf("a partition appears as its own relation: %+v", res.Incoming)
		}
	}
	e := edgeFor(t, res.Incoming, "public.events")
	if e.PartitionsFolded != 4 {
		t.Errorf("partitions_folded = %d, want 4", e.PartitionsFolded)
	}
	if res.IncomingCount != 5 {
		t.Errorf("incoming_count = %d, want 5 (4 partitions folded)", res.IncomingCount)
	}
}

// A hub table's incoming fan-out is unbounded, and the keys that act on delete
// are the ones that must survive the cap.
func TestFindRelatedCapsAndKeepsTheDestructiveKeys(t *testing.T) {
	res := related(t, map[string]any{"table": "users", "limit": float64(1)})
	if res.IncomingCount != 5 {
		t.Fatalf("incoming_count = %d, want the pre-cap total", res.IncomingCount)
	}
	if res.IncomingOmitted != 1 {
		t.Errorf("incoming_omitted = %d, want 1 (only audits is safe to drop)", res.IncomingOmitted)
	}
	for _, want := range []string{"public.sessions", "public.invoices", "public.events", `app."Memberships"`} {
		edgeFor(t, res.Incoming, want)
	}
	for _, e := range res.Incoming {
		if e.Table == "public.audits" {
			t.Errorf("a NO ACTION key survived the cap while destructive ones were counted")
		}
	}
	if res.Meta == nil || !strings.Contains(res.Meta.Hint, "Omitted 1 of 6 relations") {
		t.Errorf("hint should report the cap: %+v", res.Meta)
	}
}

func TestFindRelatedLimitZeroReturnsEverything(t *testing.T) {
	res := related(t, map[string]any{"table": "users", "limit": float64(0)})
	if len(res.Incoming) != 5 || res.IncomingOmitted != 0 {
		t.Errorf("limit=0 should return all 5, got %d (omitted %d)", len(res.Incoming), res.IncomingOmitted)
	}
}

func relatedFor(t *testing.T, snap *schema.SchemaSnapshot, args map[string]any) findRelatedResult {
	t.Helper()
	c := serveOffline(t, NewOfflineServerAnnotated(&schema.AnnotatedSchema{Schema: snap}, lint.DefaultConfig()))
	return relationsSectionOf(t, callTool(t, c, "describe_table", relationArgs(args)))
}

// A table that references itself is one relation, and Postgres will not accept
// an unaliased self-join.
func TestFindRelatedSelfReference(t *testing.T) {
	snap := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "t", Timestamp: time.Now().UTC(), ContentHash: "t",
		Tables: []schema.Table{{
			Schema: "public", Name: "employees",
			Columns: []schema.Column{col("employee_id"), col("manager_id")},
			Constraints: []schema.Constraint{
				fk("employees_manager_fkey", []string{"manager_id"}, "public.employees", []string{"employee_id"}, "ON DELETE SET NULL"),
			},
		}},
	}
	res := relatedFor(t, snap, map[string]any{"table": "employees"})

	if len(res.Incoming) != 0 || res.IncomingCount != 0 {
		t.Errorf("the outgoing key came back as an incoming one too: %+v", res.Incoming)
	}
	if len(res.Outgoing) != 1 {
		t.Fatalf("outgoing = %+v", res.Outgoing)
	}
	want := "JOIN public.employees AS parent ON parent.employee_id = public.employees.manager_id"
	if res.Outgoing[0].Join != want {
		t.Errorf("self join:\n got: %s\nwant: %s", res.Outgoing[0].Join, want)
	}
}

// Partitioning nests, so folding has to reach the root, not the table one level up.
func TestFindRelatedFoldsNestedPartitions(t *testing.T) {
	child := func(name string) schema.Table {
		return schema.Table{
			Schema: "public", Name: name,
			Columns:     []schema.Column{col("user_id")},
			Constraints: []schema.Constraint{fk(name+"_u_fkey", []string{"user_id"}, "public.users", []string{"user_id"}, "ON DELETE CASCADE")},
		}
	}
	mid := child("mid")
	mid.PartitionInfo = &schema.PartitionInfo{Strategy: schema.PartitionRange, Key: "RANGE (b)",
		Children: []schema.PartitionChild{{Schema: "public", Name: "leaf1"}, {Schema: "public", Name: "leaf2"}}}
	root := child("root")
	root.PartitionInfo = &schema.PartitionInfo{Strategy: schema.PartitionRange, Key: "RANGE (a)",
		Children: []schema.PartitionChild{{Schema: "public", Name: "mid"}}}

	snap := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "t", Timestamp: time.Now().UTC(), ContentHash: "t",
		Tables: []schema.Table{
			child("leaf1"), child("leaf2"), mid, root,
			{Schema: "public", Name: "users", Columns: []schema.Column{col("user_id")}},
		},
	}
	res := relatedFor(t, snap, map[string]any{"table": "users"})

	if len(res.Incoming) != 1 {
		t.Fatalf("nested partitions did not fold to one relation: %+v", res.Incoming)
	}
	e := res.Incoming[0]
	if e.Table != "public.root" || e.PartitionsFolded != 3 {
		t.Errorf("want public.root with 3 folded, got %s with %d", e.Table, e.PartitionsFolded)
	}
}

// A snapshot with no constraint definition cannot say what a delete does, and
// answering that question by omission is the failure to avoid.
func TestFindRelatedUnrecordedAction(t *testing.T) {
	bare := schema.Constraint{
		Name: "x_user_fkey", Kind: schema.ConstraintForeignKey,
		Columns: []string{"user_id"}, FKColumns: []string{"user_id"},
		FKTable: strPtr("public.users"),
	}
	snap := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "t", Timestamp: time.Now().UTC(), ContentHash: "t",
		Tables: []schema.Table{
			{Schema: "public", Name: "users", Columns: []schema.Column{col("user_id")}},
			{Schema: "public", Name: "x", Columns: []schema.Column{col("user_id")}, Constraints: []schema.Constraint{bare}},
		},
	}
	res := relatedFor(t, snap, map[string]any{"table": "users"})
	if len(res.Incoming) != 1 || res.Incoming[0].OnDelete != actionUnknown {
		t.Fatalf("want an unrecorded action, got %+v", res.Incoming)
	}
	if res.Meta == nil || !strings.Contains(res.Meta.Hint, "unrecorded in this snapshot to public.x") {
		t.Errorf("hint should say it cannot answer: %+v", res.Meta)
	}
	// and it must not be the first thing the cap drops
	if !destructive(res.Incoming[0]) {
		t.Error("an unrecorded action should be kept past the cap")
	}
}

func TestFindRelatedNotValid(t *testing.T) {
	snap := relatedSnap()
	for i := range snap.Tables {
		if snap.Tables[i].Name == "sessions" {
			def := "FOREIGN KEY (user_id) REFERENCES public.users(user_id) ON DELETE CASCADE NOT VALID"
			snap.Tables[i].Constraints[0].Definition = &def
		}
	}
	e := edgeFor(t, relatedFor(t, snap, map[string]any{"table": "users"}).Incoming, "public.sessions")
	if !e.NotValid {
		t.Error("NOT VALID should be reported: existing rows may violate the key")
	}
}

// SET DEFAULT is the third destructive action and the hint groups it with SET NULL.
func TestFindRelatedSetDefault(t *testing.T) {
	snap := relatedSnap()
	for i := range snap.Tables {
		if snap.Tables[i].Name == "invoices" {
			def := "FOREIGN KEY (user_id) REFERENCES public.users(user_id) ON DELETE SET DEFAULT"
			snap.Tables[i].Constraints[0].Definition = &def
		}
	}
	res := relatedFor(t, snap, map[string]any{"table": "users"})
	e := edgeFor(t, res.Incoming, "public.invoices")
	if e.OnDelete != "SET DEFAULT" || !destructive(e) {
		t.Errorf("invoices = %+v", e)
	}
	if !strings.Contains(res.Meta.Hint, "clears the referencing column in public.invoices") {
		t.Errorf("hint: %s", res.Meta.Hint)
	}
}

// A deferred constraint lets the DELETE through and raises at COMMIT.
func TestFindRelatedDeferredBlockingKey(t *testing.T) {
	snap := relatedSnap()
	for i := range snap.Tables {
		if snap.Tables[i].Name == "audits" {
			def := "FOREIGN KEY (user_id) REFERENCES public.users(user_id) DEFERRABLE INITIALLY DEFERRED"
			snap.Tables[i].Constraints[0].Definition = &def
		}
	}
	res := relatedFor(t, snap, map[string]any{"table": "users"})
	if !strings.Contains(res.Meta.Hint, "at COMMIT, where the constraint is deferred") {
		t.Errorf("hint should not claim the DELETE itself fails: %s", res.Meta.Hint)
	}
}

// A hub table would otherwise inline every referencing name into one sentence,
// which is the fan-out the cap exists for.
func TestFindRelatedHintNamesAreBounded(t *testing.T) {
	tables := []schema.Table{{Schema: "public", Name: "users", Columns: []schema.Column{col("user_id")}}}
	for i := 0; i < 40; i++ {
		name := fmt.Sprintf("child%02d", i)
		tables = append(tables, schema.Table{
			Schema: "public", Name: name, Columns: []schema.Column{col("user_id")},
			Constraints: []schema.Constraint{fk(name+"_u_fkey", []string{"user_id"}, "public.users", []string{"user_id"}, "ON DELETE CASCADE")},
		})
	}
	snap := &schema.SchemaSnapshot{PgVersion: "PostgreSQL 17.0", Database: "t",
		Timestamp: time.Now().UTC(), ContentHash: "t", Tables: tables}

	res := relatedFor(t, snap, map[string]any{"table": "users", "limit": float64(0)})
	if !strings.Contains(res.Meta.Hint, "and 35 more") {
		t.Errorf("hint should bound the names: %s", res.Meta.Hint)
	}
	if strings.Contains(res.Meta.Hint, "child39") {
		t.Errorf("hint inlined every name: %s", res.Meta.Hint)
	}
	// the count itself stays exact
	if res.IncomingCount != 40 {
		t.Errorf("incoming_count = %d, want 40", res.IncomingCount)
	}
}

func TestFindRelatedCapsOutgoingToo(t *testing.T) {
	cons := []schema.Constraint{}
	tables := []schema.Table{}
	for i := 0; i < 6; i++ {
		name := fmt.Sprintf("parent%d", i)
		tables = append(tables, schema.Table{Schema: "public", Name: name, Columns: []schema.Column{col("id")}})
		cons = append(cons, fk(fmt.Sprintf("c%d", i), []string{"id"}, "public."+name, []string{"id"}, ""))
	}
	tables = append(tables, schema.Table{Schema: "public", Name: "hub",
		Columns: []schema.Column{col("id")}, Constraints: cons})
	snap := &schema.SchemaSnapshot{PgVersion: "PostgreSQL 17.0", Database: "t",
		Timestamp: time.Now().UTC(), ContentHash: "t", Tables: tables}

	res := relatedFor(t, snap, map[string]any{"table": "hub", "limit": float64(2)})
	if res.OutgoingCount != 6 || res.OutgoingOmitted != 4 || len(res.Outgoing) != 2 {
		t.Errorf("outgoing cap: kept %d, count %d, omitted %d", len(res.Outgoing), res.OutgoingCount, res.OutgoingOmitted)
	}
}

func TestJoinClauseRefusesMismatchedKeys(t *testing.T) {
	if got := joinClause("public.a", "public.b", "child", []string{"x", "y"}, []string{"x"}); got != "" {
		t.Errorf("a partial join predicate is a wrong result set, got %q", got)
	}
	if got := joinClause("public.a", "public.b", "child", nil, nil); got != "" {
		t.Errorf("want no clause without columns, got %q", got)
	}
}

// A partition can carry a locally-added key alongside the inherited one. Same
// columns, different action, so folding it into the parent would hide it.
func TestFindRelatedKeepsAPartitionWithItsOwnAction(t *testing.T) {
	parent := schema.Table{
		Schema: "public", Name: "events",
		Columns:     []schema.Column{col("user_id")},
		Constraints: []schema.Constraint{fk("events_u_fkey", []string{"user_id"}, "public.users", []string{"user_id"}, "ON DELETE NO ACTION")},
		PartitionInfo: &schema.PartitionInfo{Strategy: schema.PartitionRange, Key: "RANGE (a)",
			Children: []schema.PartitionChild{{Schema: "public", Name: "events_p1"}, {Schema: "public", Name: "events_p2"}}},
	}
	snap := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "t", Timestamp: time.Now().UTC(), ContentHash: "t",
		Tables: []schema.Table{
			{Schema: "public", Name: "events_p1", Columns: []schema.Column{col("user_id")},
				Constraints: []schema.Constraint{fk("p1_u_fkey", []string{"user_id"}, "public.users", []string{"user_id"}, "ON DELETE NO ACTION")}},
			{Schema: "public", Name: "events_p2", Columns: []schema.Column{col("user_id")},
				Constraints: []schema.Constraint{fk("p2_u_fkey", []string{"user_id"}, "public.users", []string{"user_id"}, "ON DELETE CASCADE")}},
			parent,
			{Schema: "public", Name: "users", Columns: []schema.Column{col("user_id")}},
		},
	}
	res := relatedFor(t, snap, map[string]any{"table": "users"})

	if len(res.Incoming) != 2 {
		t.Fatalf("the diverging partition was folded away: %+v", res.Incoming)
	}
	inherited := edgeFor(t, res.Incoming, "public.events")
	if inherited.PartitionsFolded != 1 || inherited.OnDelete != "NO ACTION" {
		t.Errorf("inherited edge = %+v", inherited)
	}
	if !strings.Contains(res.Meta.Hint, "deletes matching rows in public.events") {
		t.Errorf("the cascading partition should still reach the hint: %s", res.Meta.Hint)
	}
}

// Table is the quoted SQL identity, for pasting into a query. A consumer that
// wants to join these edges to anything else -- a cascade cost annotator, say --
// needs the catalog identity, and un-quoting Table gets mixed-case and dotted
// names wrong. Both must ride the wire.
func TestRelatedEdgeCarriesCatalogIdentityBesideTheQuotedOne(t *testing.T) {
	res := related(t, map[string]any{"table": "users", "limit": 0})

	e := edgeFor(t, res.Incoming, `app."Memberships"`)
	if e.TableSchema != "app" || e.TableName != "Memberships" {
		t.Errorf("want the unquoted catalog identity, got schema=%q name=%q", e.TableSchema, e.TableName)
	}
	if e.Table != `app."Memberships"` {
		t.Errorf("the quoted identity must stay as it was: %q", e.Table)
	}
	// and it must be the identity a follow-up call actually resolves
	chased := relatedFor(t, relatedSnap(), map[string]any{
		"schema": e.TableSchema, "table": e.TableName,
	})
	if chased.Table != `app."Memberships"` {
		t.Errorf("catalog identity does not resolve: %q", chased.Table)
	}
}
