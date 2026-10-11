package main

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/diff"
)

const (
	// capture --all is sequential; a big-stats replica lands ~80s after the primary
	captureGap      = 2 * time.Minute
	defaultLogLimit = 20
)

type (
	schemaVersion struct {
		Row      history.SnapshotSummary
		Ref      int // latest~Ref: offset in the unfiltered schema list
		Lived    time.Duration
		Oldest   bool // no parent kept: first capture, or pruned
		Delta    *diff.SchemaDelta
		DiffErr  error
		Captures []capture // newest first
	}

	capture struct {
		At    time.Time
		Nodes []string // empty for a planner-only capture
	}

	schemaLog struct {
		Versions []schemaVersion
		Orphans  []capture // stats whose schema is not in the store
		Older    int
	}

	logFilter struct {
		Since *time.Time
		Kind  string
		Node  string
		Limit int
	}
)

// An unmatched --node/--kind returns an empty log so the caller prints "matched
// nothing" rather than a log of empty versions.
func buildLog(ctx context.Context, store *history.Store, key history.SnapshotKey, f logFilter) (schemaLog, error) {
	rows, err := store.ListSchema(ctx, key, history.TimeRange{})
	if err != nil {
		return schemaLog{}, err
	}

	var stats []history.SnapshotSummary
	if f.Kind != "schema" {
		kinds, err := store.ListKinds(ctx, key)
		if err != nil {
			return schemaLog{}, err
		}
		for _, k := range kinds {
			if k.Tag == history.KindSchema || !kindMatches(k, f.Kind, f.Node) {
				continue
			}
			list, err := store.List(ctx, key, k, history.TimeRange{From: f.Since})
			if err != nil {
				return schemaLog{}, err
			}
			for i := range list {
				list[i].Kind = k
			}
			stats = append(stats, list...)
		}
		if len(stats) == 0 && (f.Node != "" || f.Kind != "") {
			return schemaLog{}, nil
		}
	}

	perVersion, orphans := assignVersions(rows, stats)
	var lg schemaLog
	for i, row := range rows {
		caps := clusterCaptures(perVersion[i])
		if f.Since != nil && row.Timestamp.Before(*f.Since) && len(caps) == 0 {
			continue
		}
		v := schemaVersion{Row: row, Ref: i, Oldest: i == len(rows)-1, Captures: caps}
		if i > 0 {
			v.Lived = rows[i-1].Timestamp.Sub(row.Timestamp)
		}
		lg.Versions = append(lg.Versions, v)
	}
	if f.Limit > 0 && len(lg.Versions) > f.Limit {
		lg.Older = len(lg.Versions) - f.Limit
		lg.Versions = lg.Versions[:f.Limit]
	}
	lg.Orphans = clusterCaptures(orphans)

	diffVersions(ctx, store, key, rows, lg.Versions)
	return lg, nil
}

// A version's parent is the next version's child; one cached parse keeps at
// most two schemas alive at once.
func diffVersions(ctx context.Context, store *history.Store, key history.SnapshotKey, rows []history.SnapshotSummary, versions []schemaVersion) {
	var (
		cachedRef = -1
		cached    *schema.SchemaSnapshot
	)
	load := func(ref int) (*schema.SchemaSnapshot, error) {
		if ref == cachedRef {
			return cached, nil
		}
		return store.GetSchemaByExactHash(ctx, key, rows[ref].ContentHash)
	}
	for i := range versions {
		v := &versions[i]
		// a nil Delta without DiffErr would render as "no schema change"
		if err := ctx.Err(); err != nil {
			v.DiffErr = err
			continue
		}
		if v.Oldest {
			continue
		}
		child, err := load(v.Ref)
		if err != nil {
			v.DiffErr = err
			continue
		}
		parent, err := load(v.Ref + 1)
		if err != nil {
			v.DiffErr = err
			continue
		}
		cachedRef, cached = v.Ref+1, parent
		v.Delta, v.DiffErr = diff.DiffSchema(parent, child)
	}
}

// A revert (A -> B -> A) stores A twice, so a stats row belongs to the newest
// twin at or before it; schemas is newest first.
func assignVersions(schemas, stats []history.SnapshotSummary) (perVersion [][]history.SnapshotSummary, orphans []history.SnapshotSummary) {
	twins := make(map[string][]int, len(schemas))
	for i, s := range schemas {
		twins[s.ContentHash] = append(twins[s.ContentHash], i)
	}
	perVersion = make([][]history.SnapshotSummary, len(schemas))
	for _, r := range stats {
		idx := twins[r.SchemaRefHash]
		pick := -1
		for _, i := range idx {
			if !schemas[i].Timestamp.After(r.Timestamp) {
				pick = i
				break
			}
		}
		// tolerate clock skew within one capture; an older stat whose own twin
		// was pruned still lands here
		if pick < 0 && len(idx) > 0 {
			if oldest := idx[len(idx)-1]; schemas[oldest].Timestamp.Sub(r.Timestamp) <= captureGap {
				pick = oldest
			}
		}
		if pick < 0 {
			orphans = append(orphans, r)
			continue
		}
		perVersion[pick] = append(perVersion[pick], r)
	}
	return perVersion, orphans
}

// Rows within captureGap of the previous one join a capture; chaining keeps a
// slow fleet together without merging hourly captures.
func clusterCaptures(rows []history.SnapshotSummary) []capture {
	slices.SortFunc(rows, func(a, b history.SnapshotSummary) int {
		return cmp.Or(a.Timestamp.Compare(b.Timestamp), cmp.Compare(a.Kind.Tag, b.Kind.Tag), cmp.Compare(a.NodeLabel, b.NodeLabel))
	})
	var (
		caps []capture
		last time.Time
	)
	for _, r := range rows {
		if len(caps) == 0 || r.Timestamp.Sub(last) > captureGap {
			caps = append(caps, capture{At: r.Timestamp})
		}
		last = r.Timestamp
		c := &caps[len(caps)-1]
		if r.NodeLabel != "" && !slices.Contains(c.Nodes, r.NodeLabel) {
			c.Nodes = append(c.Nodes, r.NodeLabel)
		}
	}
	slices.Reverse(caps)
	return caps
}
