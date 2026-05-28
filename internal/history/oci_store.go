package history

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	oras "oras.land/oras-go/v2"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote"

	"github.com/boringsql/dryrun/internal/schema"
)

const (
	mediaTypeBundle      = "application/vnd.dryrun.bundle.v1+zstd"
	artifactTypeSnapshot = "application/vnd.dryrun.snapshot.v1+json"
)

// OCIStore persists each schema bundle as one OCI artifact (manifest + a single
// zstd layer). Planner/activity merge into the matching bundle by
// schema_ref_hash, mirroring FilesystemStore.
type OCIStore struct {
	base      string
	client    remote.Client
	plainHTTP bool
	streamFor func(SnapshotKey) string
}

type OCIConfig struct {
	Base      string // registry + repo prefix, e.g. us-docker.pkg.dev/proj/dryrun
	Client    remote.Client
	PlainHTTP bool
	StreamFor func(SnapshotKey) string // default StreamSuffix
}

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

// merging planner/activity keeps the same version/ref tags (schema ts+hash are
// unchanged); the old manifest is left dangling for registry GC
func (o *OCIStore) pushTagged(ctx context.Context, repo *remote.Repository, b *Bundle) (PutOutcome, error) {
	man, err := o.pushBundle(ctx, repo, b)
	if err != nil {
		return PutInserted, err
	}
	tags := []string{
		versionTag(b.Schema.Timestamp, b.Schema.ContentHash),
		refTag(b.Schema.ContentHash),
	}
	for _, t := range tags {
		if err := repo.Tag(ctx, man, t); err != nil {
			return PutInserted, fmt.Errorf("oci store: tag %q: %w", t, err)
		}
	}
	return PutInserted, nil
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
