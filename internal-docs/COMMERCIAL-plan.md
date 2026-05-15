# COMMERCIAL-plan.md (internal, binding)

**Status:** binding internal guide. Written 2026-04-30. Update only with explicit deliberation; do not let scope-creep edits drift it.

**Audience:** future me, and anyone I trust enough to share commercial direction with.

**Purpose:** When a question like "should this feature be OSS or commercial?" / "should I relicense X?" / "should I accept this contribution?" / "should I lower the price?" comes up — read this doc first. It captures the decisions already made and the reasoning, so I don't re-litigate them under pressure.

This document deliberately repeats parts of the public `PLAN-SHARED-STORAGE.md` so it stands alone as a single document to consult before any licensing, pricing, or boundary decision.

---

## 1. The one-line strategy

> *The CLI, MCP server, analysis engine, and snapshot protocol are MIT-licensed forever. The hosted/scheduled storage server is a closed commercial product distributed as a signed binary. The managed cloud is a SaaS. Pick the layer that fits.*

If a decision contradicts that line, the decision is wrong by default. Override only with written justification logged in this doc.

---

## 2. The open / closed boundary (binding)

| Component | License | Distribution | Status |
|---|---|---|---|
| `dry_run_core` (analysis engine, lint, audit, migration safety, query advise) | **MIT forever** | GitHub source | Locked. Never relicense. |
| `dry_run_cli` (CLI + MCP server) | **MIT forever** | GitHub source + binaries | Locked. Never relicense. |
| Snapshot wire protocol (OpenAPI spec) | **MIT / CC-BY** | `docs/protocol/` | Locked. Public spec is the seam. |
| Filesystem/Git snapshot backend | **MIT forever** | GitHub source | Locked. This is the free-floor for shared storage. |
| HTTP snapshot client (`HttpClientStore`) | **MIT forever** | GitHub source | Locked. |
| Current 25 knowledge-base docs (as of v1.0) | **MIT forever** | Embedded in `dry_run_core` | Locked. |
| **NEW knowledge rules added after v1.0** | **Closed**, bundled into server | Auto-update channel | Commercial moat. |
| `dryrun-server` (Phase D) | **Closed commercial, no source published** | Signed Docker image + license key + online activation | Commercial product. |
| `dryrun-cloud` (Phase E SaaS) | Closed, hosted only | dryrun.dev / cloud.dryrun.dev | Commercial product. |
| Prediction engine | Closed | Bundled into server + cloud | Commercial moat. |

### Hard rules

- **Never relicense MIT code.** If it ever shipped under MIT, it stays MIT. New closed code goes in a new crate with the closed license from day one. (`README.md` already states MIT — this rule freezes that choice.)
- **Never take away a free feature.** Anything currently free in OSS stays free in OSS. Commercial features must be additive, not extractive.
- **The protocol is public.** Anyone can implement a competing server. The moat is operational quality + prediction engine, not protocol obscurity. Don't break the protocol to lock customers in.
- **Knowledge base v1.0 stays MIT.** Future rules are commercial. Don't quietly move a v1.0 rule into the closed set.

---

## 3. The vendor case studies (why we're doing it this way)

These three cases are the reference points. Re-read before any major licensing decision.

### pganalyze — the model to copy
- OSS BSD collector + closed Docker Enterprise Server (license-key + online activation against `enterprise-license-check.pganalyze.com`) + SaaS cloud.
- **Years in production. Zero community blowback. Zero forks.**
- Why it works: closed product was *born* commercial; OSS never lost anything; air-gap handled by special license terms, not engineering.
- **This is our template.** Same niche. Same shape. Already proven.

### HashiCorp BSL (Aug 2023) — the cautionary tale
- Relicensed Apache Terraform/Vault/Consul/Nomad to BUSL-1.1.
- Within 30 days: OpenTF manifesto 33k+ stars, fork became OpenTofu (Linux Foundation).
- "Competitive use" clause triggered legal review at customers; routine upgrades became compliance projects.
- **Lesson: never relicense existing OSS. The damage is permanent.**

### Sentry FSL (Nov 2023) — the fallback
- 2-year non-compete, auto-converts to Apache/MIT.
- Better received than BSL: shorter cliff, plain-English non-compete, drafted in public.
- **Use only as a fallback** if a regulated enterprise customer demands source for audit. Customer-driven, not speculative.

---

## 4. Pricing tiers (binding defaults)

| Tier | Target | Price | What's included |
|---|---|---|---|
| **Community** | Individual devs, OSS users | **Free, forever** | OSS CLI + MCP + filesystem/Git snapshot backend + protocol spec + v1.0 knowledge base |
| **Pro** | Startups, small teams | **$49/mo** | Hosted snapshot store (cloud), scheduled snapshots, retention, web UI, single project, ≤5 databases, post-v1.0 knowledge updates |
| **Enterprise** | Scaled teams, regulated industries | **$200+/mo** | Self-hosted `dryrun-server` license, OIDC/SAML, multi-project, unlimited databases, prediction engine, support SLA, post-v1.0 knowledge updates |
| **Toggl (special)** | While I'm on staff | **$0** | Enterprise tier. License key tied to employment. Cutover terms documented before Phase D ships. |

### Pricing rules
- **Don't discount Pro below $49 to chase volume.** The differentiation is Enterprise; Pro is the trial path.
- **Enterprise floor is $200/mo.** Below that, the support burden eats the margin.
- **No per-seat pricing initially.** Per-project / per-database keeps it simple. Revisit only if a customer's usage is dramatically asymmetric.
- **No free Enterprise except Toggl.** Charity/OSS-project licenses come *after* the first 5 paying customers, not before.

---

## 5. Toggl-specific commercial terms (binding)

- **License:** free Enterprise tier while I'm on staff at Toggl.
- **Tied to employment:** when I leave, license terminates per the cutover terms below.
- **Cutover terms (must be documented in writing before Phase D deployment):**
  - 90-day frozen-license fallback after employment ends — binary keeps running, no online activation required.
  - At end of frozen period, Toggl chooses: (a) sign a paid Enterprise contract, (b) migrate to OSS-only with filesystem/Git backend, (c) export snapshots to S3 dump.
  - Snapshot data export tool (`dryrun-server export --format filesystem`) ships in Phase D, specifically so the migration path (b/c) is operational, not theoretical.
- **No support obligations beyond best-effort while employed.** Toggl is the design partner, not a paying customer.
- **No exclusive features for Toggl.** Anything I build for them ships in the standard Enterprise tier.

---

## 6. Decision rules (apply when in doubt)

When a new feature, contribution, or commercial question lands, work through these in order:

1. **Does it contradict the one-line strategy in §1?** If yes, default = no.
2. **Would shipping this require relicensing MIT code?** If yes, default = no. Find another design.
3. **Would this take away something that's currently free?** If yes, default = no.
4. **Is this a new knowledge rule?** Then it goes in the closed post-v1.0 set, bundled with server.
5. **Is this a new analysis capability that runs locally on the user's data?** Then it's MIT, in `dry_run_core`. The OSS tool must keep getting smarter.
6. **Is this a feature about coordination, scheduling, retention, multi-tenancy, or web UI?** Then it's commercial, in `dryrun-server` or `dryrun-cloud`.
7. **Is this an audit/compliance feature for a regulated customer?** Negotiate FSL or source-available addendum, customer-by-customer. Don't make it a default.

---

## 7. Things explicitly off-limits

- **Source-available licenses on `dry_run_core` or `dry_run_cli`.** MIT forever.
- **Telemetry from the OSS CLI.** No phone-home, no anonymous metrics, no opt-in dialogs. The CLI is silent. (Server telemetry via license activation is fine — that's a paid product.)
- **Feature flags in OSS that gate functionality on a license check.** The OSS tool either has a feature or doesn't. No "buy to unlock" UX.
- **Buying a competing OSS project to shut it down.** Bad-faith move, kills trust permanently.
- **Surprise license changes.** Any license change announced publicly with ≥30 days notice and a clear rationale, even for closed components.
- **Marketing OSS as "limited" or "lite."** Market the server as managed/scheduled/predictive — different product, different value, not a crippled version.

---

## 8. Trademark and IP hygiene (binding)

- **Register "dryRun" / "dry-run" trademarks** before `dryrun-server` launches.
- **Trademark policy:** forks of the OSS code are fine; forks calling themselves dryRun are not. Standard, non-controversial.
- **CLA required on all OSS contributions to the dry-run repos.** Use a standard MIT-compatible CLA template. Lets us accept community PRs without future re-licensing landmines.
- **No third-party copyrighted code in closed components without an explicit license.** Audit before each `dryrun-server` release.
- **Public `LICENSING.md`** in the OSS repo, explicitly listing what is MIT forever and what is commercial. Keep it current; this is the trust document.

---

## 9. Operational levers (closed-binary mechanics)

- **Distribution:** signed container image on `ghcr.io/clusterity/dryrun-server` (or private registry); customers `docker pull` with credentials issued at purchase.
- **License key:** `DRYRUN_LICENSE_KEY` env var, required at container start.
- **Online activation:** outbound HTTPS to `license.dryrun.dev/v1/check` on boot and every 24h.
- **Grace period:** 7 days unreachable → read-only mode, not hard-shutdown. Hard-shutdown loses customer trust during outages.
- **Air-gapped customers:** handled by special license terms (offline activation token, manual renewal). Don't engineer a generic offline mode — it's a long-tail enterprise sales conversation, not a default feature.
- **Telemetry collected at activation:** server version, customer ID, snapshot count (cardinality only, not contents). No DDL, no snapshot data, ever.

---

## 10. Launch checklist (before `dryrun-server` Phase D ships)

- [ ] `LICENSING.md` written and merged into OSS repo
- [ ] Trademark filed
- [ ] CLA in place on OSS repos
- [ ] FSL fallback license drafted (for the regulated-customer scenario)
- [ ] Toggl cutover terms documented and signed
- [ ] Two trusted Postgres-community voices have read the launch announcement
- [ ] Announcement leads with the *product*, not the license
- [ ] Pricing page lives at dryrun.dev/pricing with all three tiers and clear "what's free forever" copy
- [ ] FAQ written: "Can I run this internally?" (yes), "Can I host for my customers?" (no), "What about the OSS CLI?" (MIT forever, never changing)
- [ ] First external design partner (non-Toggl) signed up for Phase D pilot

---

## 11. Review cadence

- **Re-read this doc before any licensing or pricing decision.**
- **Quarterly review** — am I drifting from the binding rules? Log violations and fix.
- **Annual revision** — explicit decision: ratify, amend, or replace. Track each amendment with date and rationale.

**Last ratified:** 2026-04-30 (initial draft).
