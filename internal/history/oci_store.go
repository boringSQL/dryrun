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
)

const (
	mediaTypeBundle      = "application/vnd.dryrun.bundle.v1+zstd"
	artifactTypeSnapshot = "application/vnd.dryrun.snapshot.v1+json"
)

// ociBackend persists each schema bundle as one OCI artifact (manifest + a
// single zstd layer). Planner/activity merge into the matching bundle by
// schema_ref_hash, mirroring the filesystem backend.
type (
	ociBackend struct {
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
)

var _ bundleBackend = (*ociBackend)(nil)

func newOCIBackend(cfg OCIConfig) (*ociBackend, error) {
	if cfg.Base == "" {
		return nil, fmt.Errorf("oci store: empty base reference")
	}
	streamFor := cfg.StreamFor
	if streamFor == nil {
		streamFor = StreamSuffix
	}
	return &ociBackend{
		base:      strings.TrimRight(cfg.Base, "/"),
		client:    cfg.Client,
		plainHTTP: cfg.PlainHTTP,
		streamFor: streamFor,
	}, nil
}

func NewOCIStore(cfg OCIConfig) (SnapshotStore, error) {
	backend, err := newOCIBackend(cfg)
	if err != nil {
		return nil, err
	}
	return &bundleStore{backend: backend}, nil
}

func (o *ociBackend) repo(key SnapshotKey) (*remote.Repository, error) {
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
	return formatVersionedName(ts, contentHash)
}

func refTag(schemaHash string) string {
	return "ref-" + schemaHash
}

func (o *ociBackend) list(ctx context.Context, key SnapshotKey) ([]*Bundle, error) {
	repo, err := o.repo(key)
	if err != nil {
		return nil, err
	}
	var items []*Bundle
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
			items = append(items, b)
		}
		return nil
	})
	if err != nil {
		// an absent repo (never pushed to) reads as empty, not an error
		if isRepoAbsent(err) {
			return nil, nil
		}
		return nil, err
	}
	sort.SliceStable(items, func(i, j int) bool {
		if !items[i].Schema.Timestamp.Equal(items[j].Schema.Timestamp) {
			return items[i].Schema.Timestamp.After(items[j].Schema.Timestamp)
		}
		return items[i].Schema.ContentHash < items[j].Schema.ContentHash
	})
	return items, nil
}

func (o *ociBackend) load(ctx context.Context, key SnapshotKey, schemaHash string) (*Bundle, bool, error) {
	repo, err := o.repo(key)
	if err != nil {
		return nil, false, err
	}
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

// save re-pushes under the same (schema-keyed) tags; old manifest is left for
// registry cleanup
func (o *ociBackend) save(ctx context.Context, key SnapshotKey, b *Bundle) error {
	repo, err := o.repo(key)
	if err != nil {
		return err
	}
	man, err := o.pushBundle(ctx, repo, b)
	if err != nil {
		return err
	}
	return tagBundle(ctx, repo, man, b)
}

func (o *ociBackend) listKeys(ctx context.Context) ([]SnapshotKey, error) {
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

func (o *ociBackend) pushBundle(ctx context.Context, repo *remote.Repository, b *Bundle) (ocispec.Descriptor, error) {
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
	return parseVersionedName(tag)
}

// a never-pushed repo answers tags/list with 404 NAME_UNKNOWN, not ErrNotFound
func isRepoAbsent(err error) bool {
	if errors.Is(err, errdef.ErrNotFound) {
		return true
	}
	var resp *errcode.ErrorResponse
	return errors.As(err, &resp) && resp.StatusCode == http.StatusNotFound
}
