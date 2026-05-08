# DryRun vs pganalyze MCP — comparison

Written 2026-05-01, in response to pganalyze's MCP server public preview announcement
(https://pganalyze.com/blog/mcp-server-public-preview).

They look superficially similar (both "Postgres MCP servers") but they're solving
fundamentally different problems for different buyers.

## Different center of gravity

| | **pganalyze MCP** | **DryRun** |
|---|---|---|
| **Source of truth** | Hosted SaaS (their GraphQL API on top of pganalyze collector data) | Local schema snapshot (`schema.json`) + optional live PG |
| **What it knows** | Production runtime: `pg_stat_statements`, top queries, EXPLAINs, alerts, autovacuum, replication | Schema structure, multi-node table/index stats, lint rules, migration safety, knowledge base |
| **Account / install** | Requires pganalyze account + their collector running in prod | Local binary, no account, offline mode works without DB creds |
| **Audience** | DBA / SRE looking at an existing prod fleet | Dev / SWE designing schemas and shipping migrations |
| **Phase of work** | Reactive — "investigate this slow query / alert" | Proactive — "is this migration safe before I merge?" |
| **Safety story** | No DB connection at all (read-only GraphQL, redaction, rate limits) | Read-mostly; runs against snapshots or the user's own DB; sandbox only matters once shadow-EXPLAIN lands |

## pganalyze MCP tool surface (for reference)

- `get_query_stats` — top queries by runtime
- `get_query_explains`, `get_query_explain`, `get_query_explain_from_trace` — EXPLAIN access, including from OTel traces
- `get_checkup_status` — unresolved alerts (vacuum frequency, unused indexes, …)
- `run_index_selection` — Index Advisor recommendations

Data sources: aggregated `pg_stat_statements`, `pg_stat_user_tables`,
`pg_stat_user_indexes`, autovacuum activity, connection state, replication
health — all surfaced via their GraphQL API. No arbitrary SQL execution.
Access tiers (full vs basic) gate query text and EXPLAIN content.
Rate-limited at 100 calls/hour/billable server.

## Where they overlap (small)

- Both want to give an LLM a curated tool surface instead of raw SQL.
- Both expose query investigation: `get_query_explains` ↔ `explain_query` / `analyze_plan`.
- Index advice: `run_index_selection` ↔ `advise` / `detect`.
- Both deliberately avoid arbitrary SQL execution as the tool surface.

## Where they don't overlap at all

- pganalyze has **no schema design / migration / lint surface**. No
  `check_migration`, no `lint_schema`, no `schema_diff`, no `find_related`.
  That's most of DryRun.
- DryRun has **no production query telemetry** today. PLAN-statements.md is
  the closest analogue — and notably, it's collected via `--include-queries`
  dumps, not a hosted collector. That's a big gap relative to pganalyze's
  bread and butter (which is *based on* collecting `pg_stat_statements` and
  EXPLAINs at scale across customer fleets).
- pganalyze has nothing like ROADMAP Phase 1 "Shadow EXPLAIN" or Phase 2
  "vibe-coding / scaffold / validate_schema_design." Their MCP is a thin
  window onto an existing product, not a design assistant.

## Honest read on positioning

1. **pganalyze launching this is validation, not competition.** It says
   LLM-driven Postgres workflows are real, and it specifically validates the
   "don't let the model run SQL on prod, give it a curated tool surface"
   pattern that DryRun is already built around. The `mcp-improvements.md`
   thesis (tool descriptions are a token tax; consolidate; project responses)
   holds either way.

2. **The defensible wedge is what pganalyze structurally cannot do without a
   major product pivot:** offline / pre-merge / design-time work. DryRun
   analyzes a migration *before it runs*, validates a schema *before it
   exists*, and works *without* a hosted collector. pganalyze needs prod
   data to say anything; DryRun needs only a `schema.json`. That's the
   ROADMAP Phase 0–2 story and it's the right one.

3. **The PLAN-statements.md gap is now more strategic, not less.** Once
   DryRun ingests `pg_stat_statements` from a snapshot, it covers ~70% of
   pganalyze MCP's read surface (top queries, missing indexes, cache ratio,
   unused indexes) — but for users who don't want a SaaS collector. Combined
   with shadow-EXPLAIN, that's a coherent "pganalyze-lite, runs from your
   laptop, plus migration safety they don't have" story. Worth pulling
   earlier than its current position behind Phase 0/1 if pganalyze MCP gets
   traction.

4. **Pricing reality check.** pganalyze MCP is bundled into an existing $$$
   product aimed at teams already paying for observability. DryRun's $49 Pro
   / $200 Enterprise tiers are aimed at a different buyer (dev tooling
   budget, not infra observability budget). Don't try to win the same deals
   — different procurement path.

5. **Tactical: the boringSQL launch article (Phase 0) should explicitly
   contrast the two.** Not "we're better," but "design-time vs runtime,
   local vs hosted, no account needed." pganalyze's announcement gives the
   comparison hook for free.

## What not to change in response to this

- Don't chase parity with `get_query_stats` / `get_query_explains` as
  standalone hosted endpoints. That fight is theirs to win.
- Don't add an account / hosted collector to DryRun just because they have
  one. The "no signup, runs offline" property is a real differentiator;
  Phase 3 "Hosted MCP" already covers users who do want managed.
- Don't sprout a parallel set of pg_stat_statements tools — the
  `mcp-improvements.md` consolidation argument applies even harder now.
  Fold workload signals into `advise` / `detect` / `analyze_plan` as
  planned.

## Net

pganalyze validated the shape of the market; DryRun's roadmap is aimed at
an adjacent, larger, and structurally different audience. The one
prioritization nudge is to consider moving PLAN-statements.md collection in
front of (or in parallel with) Phase 1 Shadow EXPLAIN, since it's what
makes the comparison story concrete.
