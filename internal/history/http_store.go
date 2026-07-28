package history

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/boringsql/dryrun/internal/schema"
)

type (
	// HTTPStore is a SnapshotStore over the remote /v1 protocol; runSync drives it
	// unchanged. One manifest per Put, stamped with the snapshot's own timestamp.
	HTTPStore struct {
		base   string
		token  string
		client *http.Client
	}

	HTTPConfig struct {
		BaseURL string // scheme + host
		Token   string // predict API token (prk_…), sent as a bearer
		Client  *http.Client
	}
)

const (
	mediaTypeSchemaBlob     = "application/vnd.predict.schema+zstd"
	mediaTypePlannerBlob    = "application/vnd.predict.planner+zstd"
	mediaTypeActivityBlob   = "application/vnd.predict.activity+zstd"
	mediaTypeQueryStatsBlob = "application/vnd.predict.query_stats+zstd"
)

// wire shapes mirror predict's domain/manifest.Body and the flattened
// manifestResponse its handlers return.
type (
	manifestRef struct {
		Digest string `json:"digest"`
	}

	manifestBody struct {
		Schema     manifestRef            `json:"schema"`
		Planner    *manifestRef           `json:"planner,omitempty"`
		Activity   map[string]manifestRef `json:"activity,omitempty"`
		QueryStats map[string]manifestRef `json:"query_stats,omitempty"`
		TakenAt    time.Time              `json:"taken_at"`
	}

	manifestResponse struct {
		ManifestDigest string                 `json:"manifest_digest"`
		Schema         manifestRef            `json:"schema"`
		Planner        *manifestRef           `json:"planner,omitempty"`
		Activity       map[string]manifestRef `json:"activity,omitempty"`
		QueryStats     map[string]manifestRef `json:"query_stats,omitempty"`
		TakenAt        time.Time              `json:"taken_at"`
	}

	manifestSummary struct {
		ManifestDigest string    `json:"manifest_digest"`
		TakenAt        time.Time `json:"taken_at"`
		CreatedAt      time.Time `json:"created_at"`
	}
)

func NewHTTPStore(cfg HTTPConfig) (*HTTPStore, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("http store: empty base URL")
	}
	if cfg.Token == "" {
		return nil, fmt.Errorf("http store: empty token")
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	return &HTTPStore{
		base:   strings.TrimRight(cfg.BaseURL, "/"),
		token:  cfg.Token,
		client: client,
	}, nil
}

var _ SnapshotStore = (*HTTPStore)(nil)

// Put uploads the blob, then a manifest referencing it. Planner/activity/query
// stats attach the schema by ref; it is pushed first, so a missing one means
// predict returns 409, surfaced as ErrOrphanSnapshot.
func (h *HTTPStore) Put(ctx context.Context, key SnapshotKey, snap StoredSnapshot) (PutOutcome, error) {
	// The digest is recomputed from the snapshot we are about to serialize, not
	// read from the stored ContentHash. predict re-derives the hash from the
	// posted body, so a stored hash captured by an older binary (before an
	// additive field existed) would no longer reproduce and the PUT would 422.
	// Recomputing here keeps digest == hash(body) by construction.
	switch {
	case snap.AsSchema() != nil:
		s := snap.AsSchema()
		digest := schema.DigestFor(s)
		out, err := h.putBlob(ctx, digest, mediaTypeSchemaBlob, s)
		if err != nil {
			return out, err
		}
		body := manifestBody{
			Schema:  manifestRef{Digest: digest},
			TakenAt: s.Timestamp,
		}
		return out, h.putManifest(ctx, key, body)
	case snap.AsPlanner() != nil:
		p := snap.AsPlanner()
		digest := schema.ComputePlannerContentHash(p)
		out, err := h.putBlob(ctx, digest, mediaTypePlannerBlob, p)
		if err != nil {
			return out, err
		}
		body := manifestBody{
			Schema:  manifestRef{Digest: p.SchemaRefHash},
			Planner: &manifestRef{Digest: digest},
			TakenAt: p.Timestamp,
		}
		return out, h.putManifest(ctx, key, body)
	case snap.AsActivity() != nil:
		a := snap.AsActivity()
		digest := schema.ComputeActivityContentHash(a)
		out, err := h.putBlob(ctx, digest, mediaTypeActivityBlob, a)
		if err != nil {
			return out, err
		}
		body := manifestBody{
			Schema:   manifestRef{Digest: a.SchemaRefHash},
			Activity: map[string]manifestRef{activityLabel(a): {Digest: digest}},
			TakenAt:  a.Node.Timestamp,
		}
		return out, h.putManifest(ctx, key, body)
	case snap.AsQueryStats() != nil:
		q := snap.AsQueryStats()
		digest := schema.ComputeQueryStatsContentHash(q)
		out, err := h.putBlob(ctx, digest, mediaTypeQueryStatsBlob, q)
		if err != nil {
			// A predict that predates query_stats answers 415 on the blob PUT;
			// skip the kind for this remote instead of failing the whole push.
			if isUnsupportedBlobKindStatus(err) {
				return out, ErrKindUnsupported
			}
			return out, err
		}
		body := manifestBody{
			Schema:     manifestRef{Digest: q.SchemaRefHash},
			QueryStats: map[string]manifestRef{queryStatsLabel(q): {Digest: digest}},
			TakenAt:    q.Node.Timestamp,
		}
		if err := h.putManifest(ctx, key, body); err != nil {
			return out, err
		}
		return out, nil
	}
	return PutInserted, fmt.Errorf("http store: empty StoredSnapshot")
}

// putBlob HEAD-gates the upload (skip re-sending a present blob), then PUTs the
// canonical zstd(json(snapshot)). 200 = server already had it.
func (h *HTTPStore) putBlob(ctx context.Context, digest, mediaType string, v any) (PutOutcome, error) {
	has, err := h.hasBlob(ctx, digest)
	if err != nil {
		return PutInserted, err
	}
	if has {
		return PutDeduped, nil
	}

	body, err := encodeSnapshotBlob(v)
	if err != nil {
		return PutInserted, err
	}
	resp, err := h.do(ctx, http.MethodPut, h.blobURL(digest), mediaType, body)
	if err != nil {
		return PutInserted, err
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusCreated:
		return PutInserted, nil
	case http.StatusOK:
		return PutDeduped, nil
	default:
		return PutInserted, httpError(resp)
	}
}

func (h *HTTPStore) hasBlob(ctx context.Context, digest string) (bool, error) {
	resp, err := h.do(ctx, http.MethodHead, h.blobURL(digest), "", nil)
	if err != nil {
		return false, err
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, httpError(resp)
	}
}

// activityLabel mirrors predict's activityNodeLabel: explicit label, else node
// source. Keeps the manifest's activity map keyed the way the server projects.
func activityLabel(a *schema.ActivityStatsSnapshot) string {
	if a.Node.Label != nil && *a.Node.Label != "" {
		return *a.Node.Label
	}
	return a.Node.Source
}

// queryStatsLabel mirrors predict's queryStatsNodeLabel: explicit label, else
// node source.
func queryStatsLabel(q *schema.QueryStatsSnapshot) string {
	if q.Node.Label != nil && *q.Node.Label != "" {
		return *q.Node.Label
	}
	return q.Node.Source
}

func (h *HTTPStore) putManifest(ctx context.Context, key SnapshotKey, body manifestBody) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := h.do(ctx, http.MethodPut, h.manifestsURL(key), "application/json", raw)
	if err != nil {
		return err
	}
	defer drain(resp)
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		return nil
	case http.StatusConflict:
		// a referenced digest is absent (same invariant as a missing schema bundle)
		return ErrOrphanSnapshot
	default:
		return httpError(resp)
	}
}

func (h *HTTPStore) Get(ctx context.Context, key SnapshotKey, kind SnapshotKind, at SnapshotRef) (StoredSnapshot, error) {
	hash := at.Hash
	if at.Kind != RefHash {
		// sync always fetches by hash; resolve other refs via List for completeness
		resolved, err := h.resolveRef(ctx, key, kind, at)
		if err != nil {
			return StoredSnapshot{}, err
		}
		hash = resolved
	}
	return h.getByHash(ctx, kind, hash)
}

func (h *HTTPStore) getByHash(ctx context.Context, kind SnapshotKind, hash string) (StoredSnapshot, error) {
	resp, err := h.do(ctx, http.MethodGet, h.blobURL(hash), "", nil)
	if err != nil {
		return StoredSnapshot{}, err
	}
	defer drain(resp)
	if resp.StatusCode == http.StatusNotFound {
		return StoredSnapshot{}, fmt.Errorf("%w (hash %s)", ErrSnapshotNotFound, hash)
	}
	if resp.StatusCode != http.StatusOK {
		return StoredSnapshot{}, httpError(resp)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return StoredSnapshot{}, err
	}

	switch kind.Tag {
	case KindSchema:
		var s schema.SchemaSnapshot
		if err := decodeSnapshotBlob(raw, &s); err != nil {
			return StoredSnapshot{}, err
		}
		return WrapSchema(&s), nil
	case KindPlanner:
		var p schema.PlannerStatsSnapshot
		if err := decodeSnapshotBlob(raw, &p); err != nil {
			return StoredSnapshot{}, err
		}
		return WrapPlanner(&p), nil
	case KindActivity:
		var a schema.ActivityStatsSnapshot
		if err := decodeSnapshotBlob(raw, &a); err != nil {
			return StoredSnapshot{}, err
		}
		return WrapActivity(&a), nil
	case KindQuery:
		var q schema.QueryStatsSnapshot
		if err := decodeSnapshotBlob(raw, &q); err != nil {
			return StoredSnapshot{}, err
		}
		return WrapQueryStats(&q), nil
	}
	return StoredSnapshot{}, fmt.Errorf("http store: unknown SnapshotKind tag: %d", kind.Tag)
}

func (h *HTTPStore) resolveRef(ctx context.Context, key SnapshotKey, kind SnapshotKind, at SnapshotRef) (string, error) {
	list, err := h.List(ctx, key, kind, TimeRange{})
	if err != nil {
		return "", err
	}
	switch at.Kind {
	case RefLatest:
		if len(list) == 0 {
			return "", fmt.Errorf("%w (latest)", ErrSnapshotNotFound)
		}
		return list[0].ContentHash, nil
	case RefAt:
		for _, s := range list { // list is newest-first
			if !s.Timestamp.After(at.At) {
				return s.ContentHash, nil
			}
		}
		return "", fmt.Errorf("%w (at-or-before %s)", ErrSnapshotNotFound, at.At.Format(time.RFC3339))
	}
	return "", fmt.Errorf("http store: unknown SnapshotRef kind: %d", at.Kind)
}

// List expands every manifest into per-kind summaries (no blob fetch): the
// manifest's schema digest is each planner/activity's schema_ref_hash, taken_at
// is the timestamp, enough for the content_hash-keyed sync diff.
func (h *HTTPStore) List(ctx context.Context, key SnapshotKey, kind SnapshotKind, rng TimeRange) ([]SnapshotSummary, error) {
	commits, err := h.loadManifests(ctx, key)
	if err != nil {
		return nil, err
	}
	var out []SnapshotSummary
	for _, m := range commits {
		out = append(out, summariesFromManifest(m, kind, rng)...)
	}
	out = dedupSummaries(out)
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.After(out[j].Timestamp) })
	return out, nil
}

func summariesFromManifest(m manifestResponse, kind SnapshotKind, rng TimeRange) []SnapshotSummary {
	var out []SnapshotSummary
	switch kind.Tag {
	case KindSchema:
		if inRange(m.TakenAt, rng) {
			out = append(out, SnapshotSummary{
				Kind: SchemaKind(), Timestamp: m.TakenAt,
				ContentHash: m.Schema.Digest, SchemaRefHash: m.Schema.Digest,
			})
		}
	case KindPlanner:
		if m.Planner != nil && inRange(m.TakenAt, rng) {
			out = append(out, SnapshotSummary{
				Kind: PlannerKind(), Timestamp: m.TakenAt,
				ContentHash: m.Planner.Digest, SchemaRefHash: m.Schema.Digest,
			})
		}
	case KindActivity:
		for label, ref := range m.Activity {
			if kind.NodeLabel != "" && kind.NodeLabel != label {
				continue
			}
			if !inRange(m.TakenAt, rng) {
				continue
			}
			out = append(out, SnapshotSummary{
				Kind: ActivityKind(label), Timestamp: m.TakenAt,
				ContentHash: ref.Digest, SchemaRefHash: m.Schema.Digest, NodeLabel: label,
			})
		}
	case KindQuery:
		for label, ref := range m.QueryStats {
			if kind.NodeLabel != "" && kind.NodeLabel != label {
				continue
			}
			if !inRange(m.TakenAt, rng) {
				continue
			}
			out = append(out, SnapshotSummary{
				Kind: QueryKind(label), Timestamp: m.TakenAt,
				ContentHash: ref.Digest, SchemaRefHash: m.Schema.Digest, NodeLabel: label,
			})
		}
	}
	return out
}

// one blob (e.g. a schema) is referenced by many manifests; collapse to one
// summary per content_hash, keeping the newest timestamp.
func dedupSummaries(in []SnapshotSummary) []SnapshotSummary {
	best := map[string]SnapshotSummary{}
	order := make([]string, 0, len(in))
	for _, s := range in {
		cur, ok := best[s.ContentHash]
		if !ok {
			order = append(order, s.ContentHash)
			best[s.ContentHash] = s
			continue
		}
		if s.Timestamp.After(cur.Timestamp) {
			best[s.ContentHash] = s
		}
	}
	out := make([]SnapshotSummary, 0, len(order))
	for _, h := range order {
		out = append(out, best[h])
	}
	return out
}

func (h *HTTPStore) Latest(ctx context.Context, key SnapshotKey, kind SnapshotKind) (*SnapshotSummary, error) {
	list, err := h.List(ctx, key, kind, TimeRange{})
	if err != nil || len(list) == 0 {
		return nil, err
	}
	first := list[0]
	return &first, nil
}

func (h *HTTPStore) ListKinds(ctx context.Context, key SnapshotKey) ([]SnapshotKind, error) {
	commits, err := h.loadManifests(ctx, key)
	if err != nil {
		return nil, err
	}
	var hasSchema, hasPlanner bool
	labels := map[string]struct{}{}
	queryLabels := map[string]struct{}{}
	for _, m := range commits {
		if m.Schema.Digest != "" {
			hasSchema = true
		}
		if m.Planner != nil {
			hasPlanner = true
		}
		for label := range m.Activity {
			labels[label] = struct{}{}
		}
		for label := range m.QueryStats {
			queryLabels[label] = struct{}{}
		}
	}
	var out []SnapshotKind
	if hasSchema {
		out = append(out, SchemaKind())
	}
	if hasPlanner {
		out = append(out, PlannerKind())
	}
	sorted := make([]string, 0, len(labels))
	for label := range labels {
		sorted = append(sorted, label)
	}
	sort.Strings(sorted)
	for _, label := range sorted {
		out = append(out, ActivityKind(label))
	}
	// Separate label set from activity's: the same label must stay two kinds.
	sortedQuery := make([]string, 0, len(queryLabels))
	for label := range queryLabels {
		sortedQuery = append(sortedQuery, label)
	}
	sort.Strings(sortedQuery)
	for _, label := range sortedQuery {
		out = append(out, QueryKind(label))
	}
	return out, nil
}

// DeleteBefore is unsupported: predict owns retention (server-side GC) and the
// protocol exposes no delete. Sync never calls it on a remote.
func (h *HTTPStore) DeleteBefore(context.Context, SnapshotKey, SnapshotKind, time.Time) (int64, error) {
	return 0, fmt.Errorf("http store: DeleteBefore is not supported (server-side GC manages retention)")
}

// ListKeys is unsupported: /v1 has no workspace-wide database listing, so
// `pull --all` is unavailable (push --all enumerates the local source instead).
func (h *HTTPStore) ListKeys(context.Context) ([]SnapshotKey, error) {
	return nil, fmt.Errorf("http store: ListKeys is not supported; sync a specific profile/key (pull --all is unavailable for http remotes)")
}

// loadManifests fetches every manifest of a database, newest-first. An unknown
// database (never pushed to) reads as empty, mirroring OCIStore's absent repo.
func (h *HTTPStore) loadManifests(ctx context.Context, key SnapshotKey) ([]manifestResponse, error) {
	resp, err := h.do(ctx, http.MethodGet, h.manifestsURL(key), "", nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		drain(resp)
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		defer drain(resp)
		return nil, httpError(resp)
	}
	var summaries []manifestSummary
	if err := json.NewDecoder(resp.Body).Decode(&summaries); err != nil {
		drain(resp)
		return nil, err
	}
	drain(resp)

	out := make([]manifestResponse, 0, len(summaries))
	for _, s := range summaries {
		m, err := h.getManifest(ctx, key, s.ManifestDigest)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

func (h *HTTPStore) getManifest(ctx context.Context, key SnapshotKey, digest string) (manifestResponse, error) {
	u := h.manifestsURL(key) + "/" + url.PathEscape(digest)
	resp, err := h.do(ctx, http.MethodGet, u, "", nil)
	if err != nil {
		return manifestResponse{}, err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return manifestResponse{}, httpError(resp)
	}
	var m manifestResponse
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return manifestResponse{}, err
	}
	return m, nil
}

// URL builders. project/database are path-escaped; the digest is bare hex.

func (h *HTTPStore) blobURL(digest string) string {
	return h.base + "/v1/blobs/" + url.PathEscape(digest)
}

func (h *HTTPStore) manifestsURL(key SnapshotKey) string {
	return h.base + "/v1/db/" + url.PathEscape(string(key.ProjectID)) +
		"/" + url.PathEscape(string(key.DatabaseID)) + "/manifests"
}

func (h *HTTPStore) do(ctx context.Context, method, u, contentType string, body []byte) (*http.Response, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+h.token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return h.client.Do(req)
}

// blob wire format: zstd(json(snapshot)). content_hash is over a structural
// subset, so predict's canonical re-serialization recomputes the same digest.

func encodeSnapshotBlob(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	defer enc.Close()
	return enc.EncodeAll(raw, nil), nil
}

func decodeSnapshotBlob(raw []byte, v any) error {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	defer dec.Close()
	plain, err := dec.DecodeAll(raw, nil)
	if err != nil {
		return fmt.Errorf("http store: decompress blob: %w", err)
	}
	if err := json.Unmarshal(plain, v); err != nil {
		return fmt.Errorf("http store: parse blob: %w", err)
	}
	return nil
}

func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	_ = resp.Body.Close()
}

// httpStatusError keeps the status code so callers can branch on it.
type httpStatusError struct {
	status int
	msg    string
}

func (e *httpStatusError) Error() string { return e.msg }

func httpError(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	msg := strings.TrimSpace(string(b))
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}
	return &httpStatusError{
		status: resp.StatusCode,
		msg:    fmt.Sprintf("http store: %s %s: %d %s", resp.Request.Method, resp.Request.URL.Path, resp.StatusCode, msg),
	}
}

// isUnsupportedBlobKindStatus reports whether err is a 415 from the blob PUT:
// a predict that predates this kind. Blob-PUT errors only — a manifest-PUT
// failure past that point is a real error, not version skew.
func isUnsupportedBlobKindStatus(err error) bool {
	var se *httpStatusError
	if !errors.As(err, &se) {
		return false
	}
	return se.status == http.StatusUnsupportedMediaType
}
