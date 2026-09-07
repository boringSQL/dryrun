package mcp

// The first thing an agent reads, and until now it carried no selection
// guidance at all -- which is why a question explicitly about change over time
// was answered from single-snapshot tools while snapshot_diff, the only tool
// that differences anything, went uncalled in three measured sessions out of
// three.
//
// Both halves are gated on the history store: pkg/mcp serves the same
// instructions with those tools UNregistered and no store to read, so naming
// either there would promise a tool that is not registered and a field that
// never appears -- the defect this note exists to remove, not add. The
// predicate is a proxy for "Register, not RegisterOffline"; the two cannot
// diverge today, since RegisterOffline is reachable only through pkg/mcp,
// which has no way to set a store.
func selectionNote(hasHistory bool) string {
	if !hasHistory {
		return ""
	}
	return "When the question is about change over time rather than the current schema, reach for the history tools first: `snapshot_diff` differences two captures — schema DDL, planner sizing, activity, and per-shape query deltas over the same window. `list_top_queries` ranks the captured pg_stat_statements shapes, cumulative since their last reset rather than over a window. How much history is held is in _meta.history, as counts and spans only; `dryrun snapshot list` and `dryrun snapshot nodes` print it per node, with roles and staleness the field does not carry, and are what to run when it is absent.\n\n"
}

// The field's semantics, beside the other _meta fields rather than in the
// selection sentence: this is where a reader looks for what a key means, and
// the three time vocabularies (*_captured_at, newest, last_attempt) have to be
// reconcilable in one place.
func historyMetaNote(hasHistory bool) string {
	if !hasHistory {
		return ""
	}
	return " _meta.history reports, per stream, the rows the local store holds, their span, and last_attempt: when this host last captured that stream successfully. Schema, planner and query rows dedup on content — activity rows do not — so on those three newest is when the content last CHANGED rather than when it was last captured, and an absent last_attempt is unknown rather than never. The field is absent when there is no readable store, when it holds another database, or when it holds nothing for this one; _meta.history_unavailable: true is the separate case of a store that is there and would not answer."
}
