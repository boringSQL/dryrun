package history

import (
	"sort"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

const (
	// a server unseen this long, measured from the label's newest row, has left
	// the pool; its counters would only inflate the totals
	poolMemberMaxAge = 7 * 24 * time.Hour
)

// mergeNodeRow files row under its label and reports whether the bundle changed.
// A row not declared a pool keeps the one-row-per-label rule.
func mergeNodeRow[T any](head map[string]T, members map[string][]T, row T, nodeOf func(T) schema.NodeIdentity, hashOf func(T) string) bool {
	node := nodeOf(row)
	label := node.Source
	cur, hasHead := head[label]
	if hasHead && hashOf(cur) == hashOf(row) {
		return false
	}
	if !node.Pool {
		// a replay (--full) must not walk the label back or evict its servers.
		// Timestamps are whole seconds, so ties happen; break them on hash the
		// way newestPerNode does, or store and sync pick different heads.
		if hasHead && (nodeOf(cur).Timestamp.After(node.Timestamp) ||
			(nodeOf(cur).Timestamp.Equal(node.Timestamp) && hashOf(cur) > hashOf(row))) {
			return false
		}
		head[label] = row
		delete(members, label)
		return true
	}

	all := make([]T, 0, len(members[label])+2)
	all = append(all, row)
	if hasHead {
		all = append(all, cur)
	}
	for _, m := range members[label] {
		if hashOf(m) == hashOf(row) {
			return false
		}
		all = append(all, m)
	}

	kept := newestPerServer(all, nodeOf, hashOf)
	// an older capture of a server already held changes nothing
	found := false
	for _, k := range kept {
		if hashOf(k) == hashOf(row) {
			found = true
			break
		}
	}
	if !found {
		return false
	}
	head[label] = kept[0]
	if len(kept) > 1 {
		members[label] = kept[1:]
	} else {
		delete(members, label)
	}
	return true
}

// newest first, one row per server, aged-out servers dropped. Ties break on hash
// the way newestPerNode does, so store and sync agree on a label's newest row.
func newestPerServer[T any](rows []T, nodeOf func(T) schema.NodeIdentity, hashOf func(T) string) []T {
	sort.SliceStable(rows, func(i, j int) bool {
		ti, tj := nodeOf(rows[i]).Timestamp, nodeOf(rows[j]).Timestamp
		if !ti.Equal(tj) {
			return ti.After(tj)
		}
		return hashOf(rows[i]) > hashOf(rows[j])
	})
	newest := nodeOf(rows[0]).Timestamp
	var seen []time.Time
	out := make([]T, 0, len(rows))
	for _, r := range rows {
		n := nodeOf(r)
		// no fingerprint: zero time, so such rows share a single slot
		var started time.Time
		if n.PostmasterStartTime != nil {
			started = *n.PostmasterStartTime
		}
		if containsTime(seen, started) || newest.Sub(n.Timestamp) > poolMemberMaxAge {
			continue
		}
		seen = append(seen, started)
		out = append(out, r)
	}
	return out
}

// A stateless writer leaves one server per bundle, so a label's servers are
// spread over bundles and schema versions. The label's newest row decides
// whether it is a pool; another bundle's head counts only if marked, as in
// the SQL store, while stored members were already vetted on merge.
func poolMembers[T any](bundles []*Bundle, kind SnapshotKind,
	heads func(*Bundle) map[string]T, members func(*Bundle) map[string][]T,
	nodeOf func(T) schema.NodeIdentity, hashOf, refOf func(T) string) []SnapshotSummary {

	var (
		head  T
		found bool
		rows  []T
	)
	for _, b := range bundles {
		h, ok := heads(b)[kind.NodeLabel]
		if !ok {
			continue
		}
		if !found || newerRow(nodeOf(h), nodeOf(head), hashOf(h), hashOf(head)) {
			head, found = h, true
		}
		if n := nodeOf(h); n.Pool && n.PostmasterStartTime != nil {
			rows = append(rows, h)
		}
		for _, m := range members(b)[kind.NodeLabel] {
			if nodeOf(m).PostmasterStartTime != nil {
				rows = append(rows, m)
			}
		}
	}
	if !found || !nodeOf(head).Pool || nodeOf(head).PostmasterStartTime == nil {
		return nil
	}
	var out []SnapshotSummary
	for _, r := range newestPerServer(rows, nodeOf, hashOf) {
		if nodeOf(r).PostmasterStartTime.Equal(*nodeOf(head).PostmasterStartTime) {
			continue
		}
		out = append(out, SnapshotSummary{
			Kind: kind, Timestamp: nodeOf(r).Timestamp,
			ContentHash: hashOf(r), SchemaRefHash: refOf(r), NodeLabel: kind.NodeLabel,
		})
	}
	return out
}

func newerRow(a, b schema.NodeIdentity, hashA, hashB string) bool {
	return a.Timestamp.After(b.Timestamp) || (a.Timestamp.Equal(b.Timestamp) && hashA > hashB)
}
