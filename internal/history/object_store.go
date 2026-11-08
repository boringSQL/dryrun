package history

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"golang.org/x/sync/errgroup"
)

// objectReader is the per-cloud transport; gs:// and s3:// differ only in list/get.
type (
	objectReader interface {
		// list returns every object name under prefix, across all pages, in any order.
		list(ctx context.Context, prefix string) ([]string, error)
		// get returns an object's bytes; a missing object wraps errObjectNotFound.
		get(ctx context.Context, name string) ([]byte, error)
	}

	// objectBackend reads the fs layout from a bucket, as `gcloud storage cp -r`
	// of a `push --to-path` dir leaves it.
	objectBackend struct {
		reader    objectReader
		base      string // gs://bucket, for error messages
		prefix    string // "" or ends in "/"
		streamFor func(SnapshotKey) string
	}

	ObjectURL struct {
		Scheme string // "gs" or "s3"
		Bucket string
		Prefix string // "" or ends in "/"
	}
)

var (
	_ bundleBackend = (*objectBackend)(nil)

	// push to a bucket isn't supported yet
	ErrReadOnlyStore = errors.New("store is read-only")

	errObjectNotFound = errors.New("object not found")
)

const (
	// bounds GETs in flight per list
	objectFetchConcurrency = 8

	// rsync DIR's contents; `cp -r` would nest it one level too deep
	BucketUploadHint = "push to a directory with `dryrun snapshot push --to-path DIR`, then `gcloud storage rsync -r DIR gs://bucket/prefix` or `aws s3 sync DIR s3://bucket/prefix`"
)

func ParseObjectURL(raw string) (ObjectURL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return ObjectURL{}, fmt.Errorf("bad store URL %q: %w", raw, err)
	}
	if u.Scheme != "gs" && u.Scheme != "s3" {
		return ObjectURL{}, fmt.Errorf("unsupported store URL scheme %q; supported: gs://, s3://", u.Scheme)
	}
	if u.Host == "" {
		return ObjectURL{}, fmt.Errorf("store URL %q has no bucket", raw)
	}
	if strings.Contains(u.Host, ":") {
		return ObjectURL{}, fmt.Errorf("store URL %q: a bucket takes no port", raw)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return ObjectURL{}, fmt.Errorf("store URL %q: only %s://bucket[/prefix] is supported", raw, u.Scheme)
	}
	return ObjectURL{Scheme: u.Scheme, Bucket: u.Host, Prefix: normalizePrefix(u.Path)}, nil
}

func (u ObjectURL) String() string {
	return strings.TrimSuffix(u.Scheme+"://"+u.Bucket+"/"+u.Prefix, "/")
}

// "/a//b" and "a/b/" both become "a/b/"; "" and "/" mean the bucket root
func normalizePrefix(p string) string {
	var parts []string
	for _, s := range strings.Split(p, "/") {
		if s != "" {
			parts = append(parts, s)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "/") + "/"
}

func newObjectStore(r objectReader, u ObjectURL, streamFor func(SnapshotKey) string) SnapshotStore {
	if streamFor == nil {
		streamFor = StreamSuffix
	}
	return &bundleStore{backend: &objectBackend{
		reader:    r,
		base:      u.Scheme + "://" + u.Bucket,
		prefix:    normalizePrefix(u.Prefix),
		streamFor: streamFor,
	}}
}

func (o *objectBackend) keyPrefix(key SnapshotKey) string {
	return o.prefix + normalizePrefix(o.streamFor(key))
}

// entries lists bundle objects directly under key, newest first
func (o *objectBackend) entries(ctx context.Context, key SnapshotKey) ([]bundleEntry, error) {
	dir := o.keyPrefix(key)
	names, err := o.reader.list(ctx, dir)
	if err != nil {
		return nil, err
	}
	var out []bundleEntry
	for _, n := range names {
		rest, ok := strings.CutPrefix(n, dir)
		if !ok || strings.Contains(rest, "/") {
			continue
		}
		ts, hash, ok := ParseBundleFilename(rest)
		if !ok {
			continue
		}
		out = append(out, bundleEntry{name: n, timestamp: ts, contentHash: hash})
	}
	sortBundleEntries(out)
	return out, nil
}

func (o *objectBackend) list(ctx context.Context, key SnapshotKey) ([]*Bundle, error) {
	entries, err := o.entries(ctx, key)
	if err != nil {
		return nil, err
	}
	got := make([]*Bundle, len(entries))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(objectFetchConcurrency)
	for i, e := range entries {
		g.Go(func() error {
			b, err := o.fetch(gctx, e.name)
			if errors.Is(err, errObjectNotFound) {
				return nil // expired by a lifecycle rule between list and get
			}
			got[i] = b
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	out := got[:0]
	for _, b := range got {
		if b != nil {
			out = append(out, b)
		}
	}
	return out, nil
}

func (o *objectBackend) load(ctx context.Context, key SnapshotKey, schemaRefHash string) (*Bundle, bool, error) {
	entries, err := o.entries(ctx, key)
	if err != nil {
		return nil, false, err
	}
	for _, e := range entries {
		if e.contentHash != schemaRefHash {
			continue
		}
		b, err := o.fetch(ctx, e.name)
		if errors.Is(err, errObjectNotFound) {
			continue // an older twin may still be there
		}
		if err != nil {
			return nil, false, err
		}
		return b, true, nil
	}
	return nil, false, nil
}

func (o *objectBackend) save(context.Context, SnapshotKey, *Bundle) error {
	return fmt.Errorf("%w: %s", ErrReadOnlyStore, BucketUploadHint)
}

// a key counts once it holds a bundle, as on fs; stream overrides aren't reversible.
func (o *objectBackend) listKeys(ctx context.Context) ([]SnapshotKey, error) {
	names, err := o.reader.list(ctx, o.prefix)
	if err != nil {
		return nil, err
	}
	seen := map[SnapshotKey]bool{}
	var out []SnapshotKey
	for _, n := range names {
		rest, ok := strings.CutPrefix(n, o.prefix)
		if !ok {
			continue
		}
		parts := strings.Split(rest, "/")
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
			continue
		}
		if _, _, ok := ParseBundleFilename(parts[2]); !ok {
			continue
		}
		k := SnapshotKey{ProjectID: ProjectId(parts[0]), DatabaseID: DatabaseId(parts[1])}
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ProjectID != out[j].ProjectID {
			return out[i].ProjectID < out[j].ProjectID
		}
		return out[i].DatabaseID < out[j].DatabaseID
	})
	return out, nil
}

func (o *objectBackend) fetch(ctx context.Context, name string) (*Bundle, error) {
	raw, err := o.reader.get(ctx, name)
	if err != nil {
		return nil, err
	}
	b, err := DecodeBundle(raw)
	if err != nil {
		return nil, fmt.Errorf("%s/%s: %w", o.base, name, err)
	}
	return b, nil
}
