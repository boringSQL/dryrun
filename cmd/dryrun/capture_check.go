package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/boringsql/dryrun/internal/config"
	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// what --check reports per node. only Label is always set; an unreachable
// node still gets a row
type (
	checkResult struct {
		Label    string
		Fail     error
		Warnings []string

		Reached bool
		Role    string // detected: primary | standby
		// version() as reported; printed short
		PgVersion  string
		Database   string
		ServerAddr string
		// identifies the actual server; two labels sharing one = bad wiring
		Started *time.Time
		Streams []string
		// pool label: landing on the same server as another label is by design
		Pool bool
	}
)

const checkTimeoutDefault = 10 * time.Second

// SELECT 1 per node, then the same checks capture would run, in the same
// order, nothing written. a node failing doesn't stop the rest: one report
// should show every broken node
func runCaptureCheck(ctx context.Context, store *history.Store, key history.SnapshotKey, targets []captureTarget, opts captureRunOptions, timeout time.Duration, out io.Writer) error {
	// same answer for every node, resolve once
	ref, schemaErr := resolveSchemaRef(ctx, store, key, opts.AllowOrphan)
	hasSchema := schemaErr == nil && ref != ""

	results := make([]checkResult, 0, len(targets))
	for _, t := range targets {
		results = append(results, applySchemaBinding(checkNode(ctx, store, key, t, opts, timeout), schemaErr, hasSchema))
	}

	printCheckTable(out, results)
	fleetFatal, warn := fleetIssues(results)
	fatal := slices.Clone(fleetFatal)
	failed := 0
	for _, r := range results {
		if r.Fail != nil {
			failed++
			fatal = append(fatal, fmt.Sprintf("%s: %s", r.Label, redactSecrets(r.Fail.Error())))
		}
		for _, w := range r.Warnings {
			warn = append(warn, fmt.Sprintf("%s: %s", r.Label, redactSecrets(w)))
		}
	}

	if len(warn) > 0 {
		fmt.Fprintln(out)
		for _, w := range warn {
			fmt.Fprintf(out, "warning: %s\n", w)
		}
	}
	if len(fatal) > 0 {
		fmt.Fprintln(out)
		for _, f := range fatal {
			fmt.Fprintf(out, "error: %s\n", f)
		}
	}

	fmt.Fprintf(out, "\n%d of %d node(s) ready to capture\n", len(results)-failed, len(results))
	// fleet problems belong to no row; print them apart or the run looks clean
	if len(fleetFatal) > 0 {
		fmt.Fprintf(out, "%d fleet-level problem(s) to fix first\n", len(fleetFatal))
	}
	if len(fatal) > 0 {
		return errors.New("preflight failed; nothing was captured")
	}
	return nil
}

// every stream binds to a schema ref (--allow-orphan waives that). planner
// also needs the snapshot itself to annotate against, orphan or not. a node
// that already failed keeps its own error, it's the more useful one
func applySchemaBinding(r checkResult, schemaErr error, hasSchema bool) checkResult {
	if r.Fail != nil {
		return r
	}
	if schemaErr != nil {
		r.Fail = schemaErr
		return r
	}
	if !hasSchema && slices.Contains(r.Streams, "planner") {
		r.Fail = fmt.Errorf("planner stats need a schema snapshot to annotate against; run `dryrun snapshot take` first")
	}
	return r
}

func checkNode(ctx context.Context, store *history.Store, key history.SnapshotKey, t captureTarget, opts captureRunOptions, timeout time.Duration) checkResult {
	r := checkResult{Label: t.Label, Streams: t.Streams, Pool: t.Pool || opts.AllowRotation}

	url := t.URL
	if t.node != nil {
		var err error
		// unset url_env: this node's problem, report without connecting
		if url, err = t.node.URL(); err != nil {
			r.Fail = err
			return r
		}
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := connectForCheck(ctx, url)
	if err != nil {
		r.Fail = err
		return r
	}
	defer conn.Close()

	// connect alone can succeed against a pooler with no backend behind it;
	// only a query proves there is one
	var one int
	if err := conn.Pool().QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		r.Fail = fmt.Errorf("connected, but SELECT 1 failed: %w", err)
		return r
	}
	r.Reached = true

	// capture refuses a privileged role before reading anything; same here
	report, err := conn.RoleReport(ctx)
	if err != nil {
		r.Fail = err
		return r
	}
	if err := rolePreflight(report, flagAllowPrivileged); err != nil {
		r.Fail = err
		return r
	}
	if report.Privileged() {
		r.Warnings = append(r.Warnings, fmt.Sprintf("role %q is privileged (%s), allowed by --allow-privileged",
			report.Rolname, strings.Join(report.Privileges(), ", ")))
	}

	node, err := schema.CaptureNodeIdentity(ctx, conn.Pool(), t.Label)
	if err != nil {
		r.Fail = err
		return r
	}
	r.PgVersion, r.ServerAddr, r.Started = node.PgVersion, node.ServerAddr, node.PostmasterStartTime
	if v, err := dryrun.ParsePgVersion(node.PgVersion); err == nil && schema.BelowSupportedFloor(v) {
		r.Warnings = append(r.Warnings, schema.FloorWarning(v))
	}
	r.Role = history.NodeRolePrimary
	if node.IsStandby {
		r.Role = history.NodeRoleStandby
	}
	if db, err := schema.FetchCurrentDatabase(ctx, conn.Pool()); err == nil {
		r.Database = db
	}

	if t.Role != "" && t.Role != "auto" && t.Role != r.Role {
		r.Fail = fmt.Errorf("[[node]] %s declares role %s, but this node is a %s;\n"+
			"  swap the roles in dryrun.toml, or set role = auto", t.Label, t.Role, r.Role)
		return r
	}
	// same role guard capture applies
	if err := guardNodeRole(ctx, store, key, captureOptions{
		Label: t.Label, AllowRoleChange: opts.AllowRoleChange,
	}, r.Role); err != nil {
		r.Fail = err
		return r
	}

	if len(r.Streams) == 0 {
		r.Streams = config.DefaultStreamsFor(r.Role)
	}
	if slices.Contains(r.Streams, "query") {
		switch ok, err := schema.QueryStatsAvailable(ctx, conn.Pool()); {
		case err != nil:
			r.Warnings = append(r.Warnings, "could not check pg_stat_statements: "+err.Error())
		case !ok:
			r.Warnings = append(r.Warnings, pgssUnavailable+"; the query stream will be skipped")
		}
	}
	privs, err := conn.CheckPrivileges(ctx)
	if err != nil {
		r.Warnings = append(r.Warnings, "could not check catalog privileges: "+err.Error())
	}
	if err == nil {
		if !privs.PgStatUserTables {
			r.Warnings = append(r.Warnings, "pg_stat_user_tables is not readable; activity stats will be empty")
		}
		if !privs.PgCatalog {
			r.Warnings = append(r.Warnings, "pg_catalog is not readable by this role")
		}
	}
	// capture --due decides cadence before connecting; here we connected anyway
	// (reachability is the point), so report what a due run would skip
	if opts.Due && t.Interval > 0 {
		run, skipped, err := dueStreams(ctx, store, key, t, r.Streams, true)
		switch {
		case err != nil:
			r.Warnings = append(r.Warnings, "could not read this node's last capture: "+err.Error())
		case len(run) == 0:
			r.Warnings = append(r.Warnings, "nothing due; `capture --due` would skip this node entirely ("+strings.Join(skipped, ", ")+")")
		case len(skipped) > 0:
			r.Warnings = append(r.Warnings, "not due: "+strings.Join(skipped, ", "))
		}
	}
	warnIfLabelMoved(ctx, store, key, &r)
	return r
}

// capture prints this as a notice after the fact; in the preflight it's the
// actual question
func warnIfLabelMoved(ctx context.Context, store *history.Store, key history.SnapshotKey, r *checkResult) {
	if r.Started == nil || r.Pool {
		return
	}
	seen, err := store.RecentNodeFingerprints(ctx, key, r.Label)
	if err != nil || len(seen) == 0 || seen[0].StartedAt.Equal(*r.Started) {
		return
	}
	r.Warnings = append(r.Warnings, fmt.Sprintf(
		"this label was last captured from a server started %s (addr %s); it is now %s (addr %s). "+
			"A restart, or the label is aimed at a different node.",
		seen[0].StartedAt.Format(time.RFC3339Nano), addrOrUnknown(seen[0].ServerAddr),
		r.Started.Format(time.RFC3339Nano), addrOrUnknown(r.ServerAddr)))
}

func connectForCheck(ctx context.Context, url string) (*schema.DryRun, error) {
	guards := schema.DefaultSessionGuards()
	guards.StatementTimeout = flagStmtTimeout
	guards.LockTimeout = flagLockTimeout
	guards.IdleInTxTimeout = flagIdleTxTimeout
	return schema.ConnectWithGuards(ctx, url, guards)
}

// cross-node problems no single node can see: two labels aimed at one server,
// a fleet split across databases. both usually a copy-pasted URL, both silent
// at capture time
func fleetIssues(results []checkResult) (fatal, warn []string) {
	byServer := map[string][]checkResult{}
	dbs := map[string][]string{}
	for _, r := range results {
		if !r.Reached || r.Started == nil {
			continue
		}
		k := r.Started.UTC().Format(time.RFC3339Nano)
		byServer[k] = append(byServer[k], r)
		if r.Database != "" {
			dbs[r.Database] = append(dbs[r.Database], r.Label)
		}
	}

	for _, k := range sortedKeys(byServer) {
		group := byServer[k]
		if len(group) < 2 {
			continue
		}
		where := sharedAddr(group)
		// two labels on one server is only fine when they're different
		// databases on it
		byDB := map[string][]string{}
		for _, r := range group {
			byDB[r.Database] = append(byDB[r.Database], r.Label)
		}
		for _, db := range sortedKeys(byDB) {
			labels := byDB[db]
			if len(labels) < 2 {
				continue
			}
			msg := fmt.Sprintf(
				"%s point at %s (database %s): one server under %d labels. --all captures it "+
					"once per label, and the fleet view shows nodes that are the same server.",
				joinAnd(labels), where, orDash(db), len(labels))
			// a pool label lands on a real member by design, so this proves nothing
			if anyPool(group) {
				warn = append(warn, msg+" One of them names a pool, so this may be that pool's current member.")
				continue
			}
			fatal = append(fatal, msg)
		}
		if len(byDB) > 1 {
			warn = append(warn, fmt.Sprintf(
				"%s are the same server (%s), different databases.",
				joinAnd(labelsOf(group)), where))
		}
	}

	if len(dbs) > 1 {
		var parts []string
		for _, db := range sortedKeys(dbs) {
			parts = append(parts, fmt.Sprintf("%s (%s)", db, strings.Join(dbs[db], ", ")))
		}
		warn = append(warn, fmt.Sprintf(
			"the fleet spans %d databases: %s. Every node's stats land under one database_id, "+
				"so they will be compared as if they were one database.", len(dbs), strings.Join(parts, "; ")))
	}
	return fatal, warn
}

// inet_server_addr() is the interface that took the connection, so one server
// can report two; the boot time is the identity
func sharedAddr(group []checkResult) string {
	addr := ""
	for _, r := range group {
		if r.ServerAddr == "" {
			continue
		}
		if addr != "" && addr != r.ServerAddr {
			return "the same server, reached at two addresses"
		}
		addr = r.ServerAddr
	}
	if addr == "" {
		return "the same server"
	}
	return addr
}

func anyPool(group []checkResult) bool {
	for _, r := range group {
		if r.Pool {
			return true
		}
	}
	return false
}

func labelsOf(results []checkResult) []string {
	out := make([]string, len(results))
	for i, r := range results {
		out[i] = r.Label
	}
	return out
}

func joinAnd(items []string) string {
	switch len(items) {
	case 0:
		return ""
	case 1:
		return items[0]
	}
	return strings.Join(items[:len(items)-1], ", ") + " and " + items[len(items)-1]
}

func sortedKeys[V any](m map[string]V) []string { return slices.Sorted(maps.Keys(m)) }

func printCheckTable(out io.Writer, results []checkResult) {
	labelW := len("NODE")
	for _, r := range results {
		if len(r.Label) > labelW {
			labelW = len(r.Label)
		}
	}
	fmt.Fprintf(out, "%-*s  %-6s  %-8s  %-6s  %-12s  %-15s  %s\n",
		labelW, "NODE", "STATUS", "ROLE", "PG", "DATABASE", "SERVER", "STREAMS")
	for _, r := range results {
		status := "ok"
		switch {
		case r.Fail != nil:
			status = "FAIL"
		case len(r.Warnings) > 0:
			status = "warn"
		}
		fmt.Fprintf(out, "%-*s  %-6s  %-8s  %-6s  %-12s  %-15s  %s\n",
			labelW, r.Label, status,
			orDash(r.Role), orDash(shortPgVersion(r.PgVersion)), orDash(r.Database),
			orDash(r.ServerAddr), orDash(strings.Join(r.Streams, ",")))
	}
	// the address the server reports for itself; tunnel/pooler/NAT can make it
	// differ from the URL host
	fmt.Fprintln(out, "\nSERVER is the node's own inet_server_addr(), not the host you connected to.")
}

func orDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return nodeNoCapture
	}
	return s
}
