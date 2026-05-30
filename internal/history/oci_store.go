package history

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	oras "oras.land/oras-go/v2"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/boringsql/dryrun/internal/schema"
)

const (
	mediaTypeBundle      = "application/vnd.dryrun.bundle.v1+zstd"
	artifactTypeSnapshot = "application/vnd.dryrun.snapshot.v1+json"
)

// OCIStore persists each schema bundle as one OCI artifact (manifest + a single
// zstd layer). Planner/activity merge into the matching bundle by
// schema_ref_hash, mirroring FilesystemStore.
type (
	OCIStore struct {
		base      string
		client    remote.Client
		plainHTTP bool
		streamFor func(SnapshotKey) string
	}

	OCIConfig struct {
		Base      string // registry + repo prefix, e.g. us-docker.pkg.dev/proj/dryrun
		Client    remote.Client
		PlainHTTP bool
		StreamFor func(SnapshotKey) string // default StreamSuffix
	}

	ociBundle struct {
		manifest ocispec.Descriptor
		bundle   *Bundle
	}
)

var _ SnapshotStore = (*OCIStore)(nil)

func NewOCIStore(cfg OCIConfig) (*OCIStore, error) {
	if cfg.Base == "" {
		return nil, fmt.Errorf("oci store: empty base reference")
	}
	streamFor := cfg.StreamFor
	if streamFor == nil {
		streamFor = StreamSuffix
	}
	return &OCIStore{
		base:      strings.TrimRight(cfg.Base, "/"),
		client:    cfg.Client,
		plainHTTP: cfg.PlainHTTP,
		streamFor: streamFor,
	}, nil
}

func (o *OCIStore) repo(key SnapshotKey) (*remote.Repository, error) {
	ref := o.base + "/" + o.streamFor(key)
	r, err := remote.NewRepository(ref)
	if err != nil {
		return nil, fmt.Errorf("oci store: bad reference %q: %w", ref, err)
	}
	r.Client = o.client
	r.PlainHTTP = o.plainHTTP
	return r, nil
}

// version tag embeds ts+hash like the filesystem filename; ref tag is hash-only
// so planner/activity locate their schema bundle by schema_ref_hash alone
func versionTag(ts time.Time, contentHash string) string {
	return fmt.Sprintf("%s-%s", ts.UTC().Format(bundleTimeLayout), contentHash)
}

func refTag(schemaHash string) string {
	return "ref-" + schemaHash
}

func (o *OCIStore) Put(ctx context.Context, key SnapshotKey, snap StoredSnapshot) (PutOutcome, error) {
	switch {
	case snap.AsSchema() != nil:
		return o.putSchema(ctx, key, snap.AsSchema())
	case snap.AsPlanner() != nil:
		return o.putPlanner(ctx, key, snap.AsPlanner())
	case snap.AsActivity() != nil:
		return o.putActivity(ctx, key, snap.AsActivity())
	}
	return PutInserted, fmt.Errorf("empty StoredSnapshot")
}

func (o *OCIStore) putSchema(ctx context.Context, key SnapshotKey, s *schema.SchemaSnapshot) (PutOutcome, error) {
	repo, err := o.repo(key)
	if err != nil {
		return PutInserted, err
	}
	vtag := versionTag(s.Timestamp, s.ContentHash)
	if _, ok, err := resolveTag(ctx, repo, vtag); err != nil {
		return PutInserted, err
	} else if ok {
		return PutDeduped, nil
	}
	b := &Bundle{Schema: s, Activity: map[string]*schema.ActivityStatsSnapshot{}}
	return o.pushTagged(ctx, repo, b)
}

func (o *OCIStore) putPlanner(ctx context.Context, key SnapshotKey, p *schema.PlannerStatsSnapshot) (PutOutcome, error) {
	repo, err := o.repo(key)
	if err != nil {
		return PutInserted, err
	}
	b, ok, err := o.findBySchemaRef(ctx, repo, p.SchemaRefHash)
	if err != nil {
		return PutInserted, err
	}
	if !ok {
		return PutInserted, ErrOrphanSnapshot
	}
	if b.Planner != nil && b.Planner.ContentHash == p.ContentHash {
		return PutDeduped, nil
	}
	b.Planner = p
	return o.pushTagged(ctx, repo, b)
}

func (o *OCIStore) putActivity(ctx context.Context, key SnapshotKey, a *schema.ActivityStatsSnapshot) (PutOutcome, error) {
	repo, err := o.repo(key)
	if err != nil {
		return PutInserted, err
	}
	b, ok, err := o.findBySchemaRef(ctx, repo, a.SchemaRefHash)
	if err != nil {
		return PutInserted, err
	}
	if !ok {
		return PutInserted, ErrOrphanSnapshot
	}
	if b.Activity == nil {
		b.Activity = map[string]*schema.ActivityStatsSnapshot{}
	}
	if existing, ok := b.Activity[a.Node.Source]; ok && existing.ContentHash == a.ContentHash {
		return PutDeduped, nil
	}
	b.Activity[a.Node.Source] = a
	return o.pushTagged(ctx, repo, b)
}

// merge re-pushes under the same (schema-keyed) tags; old manifest is left for
// registry cleanup
func (o *OCIStore) pushTagged(ctx context.Context, repo *remote.Repository, b *Bundle) (PutOutcome, error) {
	man, err := o.pushBundle(ctx, repo, b)
	if err != nil {
		return PutInserted, err
	}
	if err := tagBundle(ctx, repo, man, b); err != nil {
		return PutInserted, err
	}
	return PutInserted, nil
}

func tagBundle(ctx context.Context, repo *remote.Repository, man ocispec.Descriptor, b *Bundle) error {
	for _, t := range []string{
		versionTag(b.Schema.Timestamp, b.Schema.ContentHash),
		refTag(b.Schema.ContentHash),
	} {
		if err := repo.Tag(ctx, man, t); err != nil {
			return fmt.Errorf("oci store: tag %q: %w", t, err)
		}
	}
	return nil
}

func (o *OCIStore) pushBundle(ctx context.Context, repo *remote.Repository, b *Bundle) (ocispec.Descriptor, error) {
	raw, err := EncodeBundle(b)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	layer := ocispec.Descriptor{
		MediaType: mediaTypeBundle,
		Digest:    digest.FromBytes(raw),
		Size:      int64(len(raw)),
	}
	if err := pushIfAbsent(ctx, repo, layer, raw); err != nil {
		return ocispec.Descriptor{}, err
	}
	return oras.PackManifest(ctx, repo, oras.PackManifestVersion1_1, artifactTypeSnapshot, oras.PackManifestOptions{
		Layers: []ocispec.Descriptor{layer},
		// pin created to the snapshot ts so identical bundles pack to identical manifests
		ManifestAnnotations: map[string]string{ocispec.AnnotationCreated: b.Schema.Timestamp.UTC().Format(time.RFC3339)},
	})
}

func (o *OCIStore) findBySchemaRef(ctx context.Context, repo *remote.Repository, schemaHash string) (*Bundle, bool, error) {
	desc, ok, err := resolveTag(ctx, repo, refTag(schemaHash))
	if err != nil || !ok {
		return nil, false, err
	}
	b, err := fetchBundle(ctx, repo, desc)
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func pushIfAbsent(ctx context.Context, repo *remote.Repository, desc ocispec.Descriptor, data []byte) error {
	ok, err := repo.Exists(ctx, desc)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	return repo.Push(ctx, desc, bytes.NewReader(data))
}

func resolveTag(ctx context.Context, repo *remote.Repository, tag string) (ocispec.Descriptor, bool, error) {
	desc, err := repo.Resolve(ctx, tag)
	if err != nil {
		if errors.Is(err, errdef.ErrNotFound) {
			return ocispec.Descriptor{}, false, nil
		}
		return ocispec.Descriptor{}, false, err
	}
	return desc, true, nil
}

func fetchBundle(ctx context.Context, repo *remote.Repository, manifest ocispec.Descriptor) (*Bundle, error) {
	mr, err := repo.Fetch(ctx, manifest)
	if err != nil {
		return nil, err
	}
	defer mr.Close()
	mbytes, err := io.ReadAll(mr)
	if err != nil {
		return nil, err
	}
	var man ocispec.Manifest
	if err := json.Unmarshal(mbytes, &man); err != nil {
		return nil, fmt.Errorf("oci store: parse manifest: %w", err)
	}
	if len(man.Layers) == 0 {
		return nil, fmt.Errorf("oci store: manifest has no layers")
	}
	lr, err := repo.Fetch(ctx, man.Layers[0])
	if err != nil {
		return nil, err
	}
	defer lr.Close()
	raw, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	return DecodeBundle(raw)
}

// inverse of versionTag; ref-* and other tags fail the time parse and are skipped
func parseVersionTag(tag string) (time.Time, string, bool) {
	i := strings.IndexByte(tag, '-')
	if i < 0 || i+1 >= len(tag) {
		return time.Time{}, "", false
	}
	ts, err := time.Parse(bundleTimeLayout, tag[:i])
	if err != nil {
		return time.Time{}, "", false
	}
	return ts, tag[i+1:], true
}

// load fetches every version-tagged bundle newest-first, mirroring
// FilesystemStore.loadBundles so the pick*/summary helpers behave identically
func (o *OCIStore) load(ctx context.Context, key SnapshotKey) (*remote.Repository, []ociBundle, error) {
	repo, err := o.repo(key)
	if err != nil {
		return nil, nil, err
	}
	var items []ociBundle
	err = repo.Tags(ctx, "", func(tags []string) error {
		for _, t := range tags {
			if _, _, ok := parseVersionTag(t); !ok {
				continue
			}
			desc, ok, err := resolveTag(ctx, repo, t)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			b, err := fetchBundle(ctx, repo, desc)
			if err != nil {
				return err
			}
			items = append(items, ociBundle{manifest: desc, bundle: b})
		}
		return nil
	})
	if err != nil {
		// an absent repo (never pushed to) reads as empty, not an error
		if isRepoAbsent(err) {
			return repo, nil, nil
		}
		return nil, nil, err
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].bundle.Schema.Timestamp.After(items[j].bundle.Schema.Timestamp)
	})
	return repo, items, nil
}

// a never-pushed repo answers tags/list with 404 NAME_UNKNOWN, not ErrNotFound
func isRepoAbsent(err error) bool {
	if errors.Is(err, errdef.ErrNotFound) {
		return true
	}
	var resp *errcode.ErrorResponse
	return errors.As(err, &resp) && resp.StatusCode == http.StatusNotFound
}

func (o *OCIStore) loadBundles(ctx context.Context, key SnapshotKey) ([]*Bundle, error) {
	_, items, err := o.load(ctx, key)
	if err != nil {
		return nil, err
	}
	out := make([]*Bundle, len(items))
	for i, it := range items {
		out[i] = it.bundle
	}
	return out, nil
}

func (o *OCIStore) Get(ctx context.Context, key SnapshotKey, kind SnapshotKind, at SnapshotRef) (StoredSnapshot, error) {
	bundles, err := o.loadBundles(ctx, key)
	if err != nil {
		return StoredSnapshot{}, err
	}
	switch kind.Tag {
	case KindSchema:
		b, err := pickSchemaBundle(bundles, at)
		if err != nil {
			return StoredSnapshot{}, err
		}
		return WrapSchema(b.Schema), nil
	case KindPlanner:
		b, err := pickPlannerBundle(bundles, at)
		if err != nil {
			return StoredSnapshot{}, err
		}
		return WrapPlanner(b.Planner), nil
	case KindActivity:
		a, err := pickActivity(bundles, kind.NodeLabel, at)
		if err != nil {
			return StoredSnapshot{}, err
		}
		return WrapActivity(a), nil
	}
	return StoredSnapshot{}, fmt.Errorf("unknown SnapshotKind tag: %d", kind.Tag)
}

func (o *OCIStore) List(ctx context.Context, key SnapshotKey, kind SnapshotKind, rng TimeRange) ([]SnapshotSummary, error) {
	bundles, err := o.loadBundles(ctx, key)
	if err != nil {
		return nil, err
	}
	var out []SnapshotSummary
	for _, b := range bundles {
		ss, err := bundleSummaries(b, kind, rng)
		if err != nil {
			return nil, err
		}
		out = append(out, ss...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.After(out[j].Timestamp) })
	return out, nil
}

func (o *OCIStore) Latest(ctx context.Context, key SnapshotKey, kind SnapshotKind) (*SnapshotSummary, error) {
	list, err := o.List(ctx, key, kind, TimeRange{})
	if err != nil || len(list) == 0 {
		return nil, err
	}
	first := list[0]
	return &first, nil
}

func (o *OCIStore) DeleteBefore(ctx context.Context, key SnapshotKey, kind SnapshotKind, cutoff time.Time) (int64, error) {
	if kind.Tag != KindSchema {
		return 0, fmt.Errorf("oci store: DeleteBefore supports schema only, got %s", kind)
	}
	repo, items, err := o.load(ctx, key)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, it := range items {
		if it.bundle.Schema != nil && it.bundle.Schema.Timestamp.Before(cutoff) {
			if err := repo.Delete(ctx, it.manifest); err != nil {
				return n, err
			}
			n++
		}
	}
	return n, nil
}

func (o *OCIStore) ListKinds(ctx context.Context, key SnapshotKey) ([]SnapshotKind, error) {
	bundles, err := o.loadBundles(ctx, key)
	if err != nil {
		return nil, err
	}
	return bundleKinds(bundles), nil
}

func (o *OCIStore) ListKeys(ctx context.Context) ([]SnapshotKey, error) {
	host, prefix, ok := strings.Cut(o.base, "/")
	if !ok {
		return nil, fmt.Errorf("oci store: base %q has no repo path", o.base)
	}
	reg, err := remote.NewRegistry(host)
	if err != nil {
		return nil, err
	}
	reg.Client = o.client
	reg.PlainHTTP = o.plainHTTP

	prefix += "/"
	var out []SnapshotKey
	err = reg.Repositories(ctx, "", func(repos []string) error {
		for _, r := range repos {
			suffix, ok := strings.CutPrefix(r, prefix)
			if !ok {
				continue
			}
			proj, db, ok := strings.Cut(suffix, "/")
			if !ok || strings.Contains(db, "/") {
				continue
			}
			out = append(out, SnapshotKey{ProjectID: ProjectId(proj), DatabaseID: DatabaseId(db)})
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, errdef.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ProjectID != out[j].ProjectID {
			return out[i].ProjectID < out[j].ProjectID
		}
		return out[i].DatabaseID < out[j].DatabaseID
	})
	return out, nil
}
