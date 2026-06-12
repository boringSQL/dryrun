package history

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	dryrun "github.com/boringsql/dryrun/pkg/snapshot"

	"github.com/boringsql/dryrun/internal/schema"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

type (
	// fakePredict is a minimal in-memory stand-in for predictd's /v1 surface: it
	// validates blobs the same way (decompress, parse, recompute content_hash),
	// enforces the orphan invariant on manifests, and dedups by digest. Enough to
	// prove HTTPStore speaks the protocol; not the real server.
	fakePredict struct {
		mu        sync.Mutex
		token     string
		blobs     map[string][]byte           // keyed by digest
		manifests map[string][]storedManifest // keyed by "proj/db", append-only
		putBlobs  int                         // PUT /blobs that actually stored
	}

	storedManifest struct {
		digest    string
		body      manifestBody
		createdAt time.Time
	}
)

func newFakePredict(token string) *fakePredict {
	return &fakePredict{
		token:     token,
		blobs:     map[string][]byte{},
		manifests: map[string][]storedManifest{},
	}
}

func (f *fakePredict) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("HEAD /v1/blobs/{digest}", f.headBlob)
	mux.HandleFunc("GET /v1/blobs/{digest}", f.getBlob)
	mux.HandleFunc("PUT /v1/blobs/{digest}", f.putBlob)
	mux.HandleFunc("PUT /v1/db/{project}/{database}/manifests", f.putManifest)
	mux.HandleFunc("GET /v1/db/{project}/{database}/manifests/{digest}", f.getManifest)
	mux.HandleFunc("GET /v1/db/{project}/{database}/manifests", f.listManifests)
	return f.auth(mux)
}

func (f *fakePredict) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+f.token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (f *fakePredict) headBlob(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	_, ok := f.blobs[r.PathValue("digest")]
	f.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (f *fakePredict) getBlob(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	b, ok := f.blobs[r.PathValue("digest")]
	f.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	_, _ = w.Write(b)
}

func (f *fakePredict) putBlob(w http.ResponseWriter, r *http.Request) {
	digest := r.PathValue("digest")
	kind, ok := kindFromContentType(r.Header.Get("Content-Type"))
	if !ok {
		w.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	raw := readBody(r)
	if recomputeDigest(kind, raw) != digest { // typed accept, like the real server
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.blobs[digest]; exists {
		writeJSON(w, http.StatusOK, map[string]string{"status": "deduped", "digest": digest})
		return
	}
	f.blobs[digest] = raw
	f.putBlobs++
	writeJSON(w, http.StatusCreated, map[string]string{"status": "inserted", "digest": digest})
}

func (f *fakePredict) putManifest(w http.ResponseWriter, r *http.Request) {
	var body manifestBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	// orphan invariant: every referenced digest must already be a stored blob.
	for _, d := range manifestDigests(body) {
		if _, ok := f.blobs[d]; !ok {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "referenced digest absent: " + d})
			return
		}
	}

	key := r.PathValue("project") + "/" + r.PathValue("database")
	digest := computeManifestDigest(body)
	for _, m := range f.manifests[key] {
		if m.digest == digest { // identical commit dedups, taken_at refreshed
			writeJSON(w, http.StatusOK, manifestRespJSON(digest, body))
			return
		}
	}
	f.manifests[key] = append(f.manifests[key], storedManifest{digest: digest, body: body, createdAt: time.Now()})
	writeJSON(w, http.StatusCreated, manifestRespJSON(digest, body))
}

func (f *fakePredict) getManifest(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("project") + "/" + r.PathValue("database")
	digest := r.PathValue("digest")
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range f.manifests[key] {
		if m.digest == digest {
			writeJSON(w, http.StatusOK, manifestRespJSON(m.digest, m.body))
			return
		}
	}
	w.WriteHeader(http.StatusNotFound)
}

func (f *fakePredict) listManifests(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("project") + "/" + r.PathValue("database")
	f.mu.Lock()
	defer f.mu.Unlock()
	ms, ok := f.manifests[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	out := make([]map[string]any, 0, len(ms))
	for _, m := range ms {
		out = append(out, map[string]any{
			"manifest_digest": m.digest,
			"taken_at":        m.body.TakenAt,
			"created_at":      m.createdAt,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// --- helpers mirroring the predict wire contract ---

func kindFromContentType(ct string) (string, bool) {
	switch ct {
	case mediaTypeSchemaBlob:
		return "schema", true
	case mediaTypePlannerBlob:
		return "planner", true
	case mediaTypeActivityBlob:
		return "activity", true
	}
	return "", false
}

func recomputeDigest(kind string, raw []byte) string {
	switch kind {
	case "schema":
		var s dryrun.SchemaSnapshot
		if decodeSnapshotBlob(raw, &s) != nil {
			return ""
		}
		return dryrun.ComputeContentHash(&s)
	case "planner":
		var p dryrun.PlannerStatsSnapshot
		if decodeSnapshotBlob(raw, &p) != nil {
			return ""
		}
		return dryrun.ComputePlannerContentHash(&p)
	case "activity":
		var a dryrun.ActivityStatsSnapshot
		if decodeSnapshotBlob(raw, &a) != nil {
			return ""
		}
		return dryrun.ComputeActivityContentHash(&a)
	}
	return ""
}

func manifestDigests(b manifestBody) []string {
	out := []string{b.Schema.Digest}
	if b.Planner != nil {
		out = append(out, b.Planner.Digest)
	}
	for _, r := range b.Activity {
		out = append(out, r.Digest)
	}
	return out
}

// computeManifestDigest mirrors predict's ComputeDigest: sha256 over the ref set
// only (schema + planner + activity), excluding taken_at.
func computeManifestDigest(b manifestBody) string {
	canon := struct {
		Schema   manifestRef            `json:"schema"`
		Planner  *manifestRef           `json:"planner,omitempty"`
		Activity map[string]manifestRef `json:"activity,omitempty"`
	}{b.Schema, b.Planner, b.Activity}
	raw, _ := json.Marshal(canon)
	return sha256Hex(raw)
}

func manifestRespJSON(digest string, b manifestBody) map[string]any {
	m := map[string]any{
		"manifest_digest": digest,
		"schema":          b.Schema,
		"taken_at":        b.TakenAt,
	}
	if b.Planner != nil {
		m["planner"] = b.Planner
	}
	if len(b.Activity) > 0 {
		m["activity"] = b.Activity
	}
	return m
}

func readBody(r *http.Request) []byte {
	b, _ := io.ReadAll(r.Body)
	return b
}

// --- the round-trip test ---

func TestHTTPStorePushPullRoundTrip(t *testing.T) {
	const token = "prk_test"
	fake := newFakePredict(token)
	srv := httptest.NewServer(fake.handler())
	t.Cleanup(srv.Close)

	store, err := NewHTTPStore(HTTPConfig{BaseURL: srv.URL, Token: token, Client: srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := SnapshotKey{ProjectID: "billing", DatabaseID: "prod"}

	// one commit: schema + planner + two activity nodes, all bound by schema_ref.
	sch := httpTestSchema()
	pl := httpTestPlanner(sch.ContentHash)
	a1 := httpTestActivity(sch.ContentHash, "node-a")
	a2 := httpTestActivity(sch.ContentHash, "node-b")

	for _, snap := range []StoredSnapshot{WrapSchema(sch), WrapPlanner(pl), WrapActivity(a1), WrapActivity(a2)} {
		if out, err := store.Put(ctx, key, snap); err != nil {
			t.Fatalf("put %s: %v", snap.Kind(), err)
		} else if out != PutInserted {
			t.Fatalf("put %s: want inserted, got %v", snap.Kind(), out)
		}
	}
	if fake.putBlobs != 4 {
		t.Fatalf("want 4 blobs stored, got %d", fake.putBlobs)
	}

	// pull side: List each kind, then Get every referenced blob and assert it
	// recomputes to the content_hash it was listed under (the sync oracle).
	assertKindRoundTrips(t, ctx, store, key, SchemaKind(), []string{sch.ContentHash})
	assertKindRoundTrips(t, ctx, store, key, PlannerKind(), []string{pl.ContentHash})
	assertKindRoundTrips(t, ctx, store, key, ActivityKind(""), []string{a1.ContentHash, a2.ContentHash})

	// schema_ref_hash is reconstructed from the manifest for planner/activity.
	plList, _ := store.List(ctx, key, PlannerKind(), TimeRange{})
	if len(plList) != 1 || plList[0].SchemaRefHash != sch.ContentHash {
		t.Fatalf("planner schema_ref: %+v", plList)
	}

	// a second push of the same commit copies zero blobs and writes no new manifest.
	before := fake.putBlobs
	store2, _ := NewHTTPStore(HTTPConfig{BaseURL: srv.URL, Token: token, Client: srv.Client()})
	for _, snap := range []StoredSnapshot{WrapSchema(sch), WrapPlanner(pl), WrapActivity(a1), WrapActivity(a2)} {
		if out, err := store2.Put(ctx, key, snap); err != nil {
			t.Fatalf("re-put %s: %v", snap.Kind(), err)
		} else if out != PutDeduped {
			t.Fatalf("re-put %s: want deduped, got %v", snap.Kind(), out)
		}
	}
	if fake.putBlobs != before {
		t.Fatalf("second push stored %d new blobs, want 0", fake.putBlobs-before)
	}
}

// TestHTTPStorePerTakeTimestamps is the regression guard for the bug where every
// kind in a commit inherited the schema's taken_at. Two takes share one schema
// (the steady state: schema dedups, stats re-sampled) but their planner/activity
// are captured later, at distinct times. List must report each ref's own time so
// the sync loop's newestPerNode / --since windowing select the right take.
func TestHTTPStorePerTakeTimestamps(t *testing.T) {
	const token = "prk_test"
	fake := newFakePredict(token)
	srv := httptest.NewServer(fake.handler())
	t.Cleanup(srv.Close)

	store, _ := NewHTTPStore(HTTPConfig{BaseURL: srv.URL, Token: token, Client: srv.Client()})
	ctx := context.Background()
	key := SnapshotKey{ProjectID: "billing", DatabaseID: "prod"}

	base := time.Now().UTC().Truncate(time.Second)
	t0 := base                     // schema captured once, long before the stats
	t1 := base.Add(24 * time.Hour) // take 1 stats
	t2 := base.Add(48 * time.Hour) // take 2 stats

	sch := httpTestSchemaAt(t0)
	p1 := httpTestPlannerAt(sch.ContentHash, t1, 100)
	p2 := httpTestPlannerAt(sch.ContentHash, t2, 200)
	a1 := httpTestActivityAt(sch.ContentHash, "node-a", t1, 1)
	a2 := httpTestActivityAt(sch.ContentHash, "node-a", t2, 2)
	if p1.ContentHash == p2.ContentHash || a1.ContentHash == a2.ContentHash {
		t.Fatal("fixtures must differ in content so they are distinct blobs")
	}

	for _, snap := range []StoredSnapshot{
		WrapSchema(sch),
		WrapPlanner(p1), WrapPlanner(p2),
		WrapActivity(a1), WrapActivity(a2),
	} {
		if _, err := store.Put(ctx, key, snap); err != nil {
			t.Fatalf("put %s: %v", snap.Kind(), err)
		}
	}

	// each planner reports its own capture time and the right schema_ref, not
	// the schema's t0.
	wantPlanner := map[string]time.Time{p1.ContentHash: t1, p2.ContentHash: t2}
	plList, err := store.List(ctx, key, PlannerKind(), TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(plList) != 2 {
		t.Fatalf("want 2 planner summaries, got %d: %+v", len(plList), plList)
	}
	for _, s := range plList {
		if !s.Timestamp.Equal(wantPlanner[s.ContentHash]) {
			t.Fatalf("planner %s: timestamp %s, want %s", s.ContentHash, s.Timestamp, wantPlanner[s.ContentHash])
		}
		if s.SchemaRefHash != sch.ContentHash {
			t.Fatalf("planner %s: schema_ref %s, want %s", s.ContentHash, s.SchemaRefHash, sch.ContentHash)
		}
	}

	// newest-first ordering puts the later take's planner at the head, which is
	// what newestPerNode relies on.
	if plList[0].ContentHash != p2.ContentHash {
		t.Fatalf("planner list not newest-first: head is %s, want %s", plList[0].ContentHash, p2.ContentHash)
	}

	// --since windowing keys off the real per-take time: only take 2 is in range.
	since := store2List(t, store, ctx, key, PlannerKind(), TimeRange{From: &t2})
	if len(since) != 1 || since[0].ContentHash != p2.ContentHash {
		t.Fatalf("--since t2: want only p2, got %+v", since)
	}

	// same for activity on a single node: both takes present with their own times.
	wantActivity := map[string]time.Time{a1.ContentHash: t1, a2.ContentHash: t2}
	acList, err := store.List(ctx, key, ActivityKind(""), TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(acList) != 2 {
		t.Fatalf("want 2 activity summaries, got %d: %+v", len(acList), acList)
	}
	for _, s := range acList {
		if !s.Timestamp.Equal(wantActivity[s.ContentHash]) {
			t.Fatalf("activity %s: timestamp %s, want %s", s.ContentHash, s.Timestamp, wantActivity[s.ContentHash])
		}
		if s.NodeLabel != "node-a" {
			t.Fatalf("activity %s: node %q, want node-a", s.ContentHash, s.NodeLabel)
		}
	}

	// the schema is referenced by every manifest ({X}, {X,P1}, {X,P2}, {X,A1},
	// {X,A2}); dedupSummaries must collapse it to a single summary.
	schList, err := store.List(ctx, key, SchemaKind(), TimeRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(schList) != 1 || schList[0].ContentHash != sch.ContentHash {
		t.Fatalf("want exactly one schema summary, got %+v", schList)
	}
}

func store2List(t *testing.T, s *HTTPStore, ctx context.Context, key SnapshotKey, kind SnapshotKind, rng TimeRange) []SnapshotSummary {
	t.Helper()
	list, err := s.List(ctx, key, kind, rng)
	if err != nil {
		t.Fatal(err)
	}
	return list
}

func TestHTTPStoreOrphanManifestConflict(t *testing.T) {
	const token = "prk_test"
	fake := newFakePredict(token)
	srv := httptest.NewServer(fake.handler())
	t.Cleanup(srv.Close)

	store, _ := NewHTTPStore(HTTPConfig{BaseURL: srv.URL, Token: token, Client: srv.Client()})
	ctx := context.Background()
	key := SnapshotKey{ProjectID: "billing", DatabaseID: "prod"}

	// a planner whose schema blob was never pushed must be rejected as an orphan.
	pl := httpTestPlanner("0000000000000000000000000000000000000000000000000000000000000000")
	_, err := store.Put(ctx, key, WrapPlanner(pl))
	if err != ErrOrphanSnapshot {
		t.Fatalf("want ErrOrphanSnapshot, got %v", err)
	}
}

func assertKindRoundTrips(t *testing.T, ctx context.Context, store *HTTPStore, key SnapshotKey, kind SnapshotKind, wantHashes []string) {
	t.Helper()
	list, err := store.List(ctx, key, kind, TimeRange{})
	if err != nil {
		t.Fatalf("list %s: %v", kind, err)
	}
	got := make([]string, 0, len(list))
	for _, s := range list {
		stored, err := store.Get(ctx, key, s.Kind, NewRefHash(s.ContentHash))
		if err != nil {
			t.Fatalf("get %s %s: %v", kind, s.ContentHash, err)
		}
		if stored.ContentHash() != s.ContentHash {
			t.Fatalf("%s blob recomputed to %s, listed as %s", kind, stored.ContentHash(), s.ContentHash)
		}
		got = append(got, s.ContentHash)
	}
	sort.Strings(got)
	want := append([]string(nil), wantHashes...)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("%s: got %v, want %v", kind, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: got %v, want %v", kind, got, want)
		}
	}
}

// --- fixtures: real snapshots with real content hashes ---

func httpTestSchema() *schema.SchemaSnapshot {
	s := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0",
		Database:  "billing",
		Timestamp: time.Now().UTC().Truncate(time.Second),
		Tables:    []schema.Table{{Schema: "public", Name: "invoices"}},
	}
	s.ContentHash = dryrun.ComputeContentHash(s)
	return s
}

func httpTestPlanner(schemaRef string) *schema.PlannerStatsSnapshot {
	p := &schema.PlannerStatsSnapshot{
		SchemaRefHash: schemaRef,
		Database:      "billing",
		Timestamp:     time.Now().UTC().Truncate(time.Second),
		Tables:        []schema.TableSizingEntry{},
		Indexes:       []schema.IndexSizingEntry{},
		Columns:       []schema.ColumnStatsEntry{},
	}
	p.ContentHash = dryrun.ComputePlannerContentHash(p)
	return p
}

func httpTestActivity(schemaRef, source string) *schema.ActivityStatsSnapshot {
	a := &schema.ActivityStatsSnapshot{
		SchemaRefHash: schemaRef,
		Node:          schema.NodeIdentity{Source: source, PgVersion: "17.0", Timestamp: time.Now().UTC().Truncate(time.Second)},
		Tables:        []schema.TableActivityEntry{},
		Indexes:       []schema.IndexActivityEntry{},
	}
	a.ContentHash = dryrun.ComputeActivityContentHash(a)
	return a
}

// parameterized variants for the multi-take timestamp test: explicit capture
// time, and a seed that changes a hashed field so distinct takes are distinct
// blobs.

func httpTestSchemaAt(ts time.Time) *schema.SchemaSnapshot {
	s := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0",
		Database:  "billing",
		Timestamp: ts,
		Tables:    []schema.Table{{Schema: "public", Name: "invoices"}},
	}
	s.ContentHash = dryrun.ComputeContentHash(s)
	return s
}

func httpTestPlannerAt(schemaRef string, ts time.Time, reltuples float64) *schema.PlannerStatsSnapshot {
	p := &schema.PlannerStatsSnapshot{
		SchemaRefHash: schemaRef,
		Database:      "billing",
		Timestamp:     ts,
		Tables: []schema.TableSizingEntry{{
			Table:  schema.QualifiedName{Schema: "public", Name: "invoices"},
			Sizing: schema.TableSizing{Reltuples: reltuples},
		}},
		Indexes: []schema.IndexSizingEntry{},
		Columns: []schema.ColumnStatsEntry{},
	}
	p.ContentHash = dryrun.ComputePlannerContentHash(p)
	return p
}

func httpTestActivityAt(schemaRef, source string, ts time.Time, seed int) *schema.ActivityStatsSnapshot {
	a := &schema.ActivityStatsSnapshot{
		SchemaRefHash: schemaRef,
		Node:          schema.NodeIdentity{Source: source, PgVersion: "17.0", Timestamp: ts},
		Tables: []schema.TableActivityEntry{{
			Table: schema.QualifiedName{Schema: "public", Name: fmt.Sprintf("invoices_%d", seed)},
		}},
		Indexes: []schema.IndexActivityEntry{},
	}
	a.ContentHash = dryrun.ComputeActivityContentHash(a)
	return a
}
