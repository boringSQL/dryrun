package history

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	gcsEndpoint = "https://storage.googleapis.com"
	gcsAttempts = 3
)

// gcsReader is only objects.list and objects.get: objectViewer grants no
// bucket-level calls.
type (
	gcsReader struct {
		bucket   string
		endpoint string
		client   *http.Client
		auth     *gcsAuth // nil against an emulator
		backoff  time.Duration
	}

	gcsListPage struct {
		Items []struct {
			Name string `json:"name"`
		} `json:"items"`
		NextPageToken string `json:"nextPageToken"`
	}
)

var _ objectReader = (*gcsReader)(nil)

// NewGCSStore opens gs://bucket[/prefix] read-only. STORAGE_EMULATOR_HOST
// points it at a fake-gcs-server without credentials.
func NewGCSStore(ctx context.Context, u ObjectURL, streamFor func(SnapshotKey) string) (SnapshotStore, error) {
	if u.Scheme != "gs" {
		return nil, fmt.Errorf("not a gs:// URL: %s", u)
	}
	// a stalled connection must not hang the pull: the header timeout is the
	// one that matters
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ResponseHeaderTimeout = 30 * time.Second
	endpoint := gcsEndpoint
	var auth *gcsAuth
	if host := os.Getenv("STORAGE_EMULATOR_HOST"); host != "" {
		if !strings.Contains(host, "://") {
			host = "http://" + host
		}
		endpoint = strings.TrimRight(host, "/")
	} else {
		var err error
		if auth, err = newGCSAuth(ctx); err != nil {
			return nil, err
		}
	}
	r := &gcsReader{bucket: u.Bucket, endpoint: endpoint, client: &http.Client{Transport: tr, Timeout: 5 * time.Minute}, auth: auth, backoff: 500 * time.Millisecond}
	return newObjectStore(r, u, streamFor), nil
}

func (g *gcsReader) list(ctx context.Context, prefix string) ([]string, error) {
	var names []string
	token := ""
	for {
		q := url.Values{"fields": {"items(name),nextPageToken"}}
		if prefix != "" {
			q.Set("prefix", prefix)
		}
		if token != "" {
			q.Set("pageToken", token)
		}
		u := g.endpoint + "/storage/v1/b/" + url.PathEscape(g.bucket) + "/o?" + q.Encode()
		body, err := g.do(ctx, u, prefix, false)
		if err != nil {
			return nil, err
		}
		var page gcsListPage
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("gs://%s/%s: list: %w", g.bucket, prefix, err)
		}
		for _, it := range page.Items {
			names = append(names, it.Name)
		}
		if page.NextPageToken == "" {
			return names, nil
		}
		token = page.NextPageToken
	}
}

func (g *gcsReader) get(ctx context.Context, name string) ([]byte, error) {
	// the name is one path segment: "/" must go as %2F, "+" as %2B.
	escaped := strings.ReplaceAll(url.PathEscape(name), "+", "%2B")
	u := g.endpoint + "/storage/v1/b/" + url.PathEscape(g.bucket) + "/o/" + escaped + "?alt=media"
	return g.do(ctx, u, name, true)
}

// do GETs u, retrying with a fresh token on 401 and backing off on 408/429/5xx.
func (g *gcsReader) do(ctx context.Context, u, obj string, isGet bool) ([]byte, error) {
	where := "gs://" + g.bucket + "/" + obj
	refreshed := false
	for attempt := 1; ; attempt++ {
		tok := ""
		if g.auth != nil {
			// a credential problem isn't transient: no backoff
			var err error
			if tok, err = g.auth.token(ctx); err != nil {
				return nil, err
			}
		}
		status, body, err := g.once(ctx, u, tok)
		switch {
		case err != nil:
			if ctx.Err() != nil || attempt >= gcsAttempts {
				return nil, fmt.Errorf("%s: %w", where, err)
			}
		case status == http.StatusOK:
			return body, nil
		case status == http.StatusUnauthorized && g.auth != nil && !refreshed:
			g.auth.invalidate(tok)
			refreshed = true
			continue
		case status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500:
			if attempt >= gcsAttempts {
				return nil, fmt.Errorf("%s: %s", where, gcsStatusText(status, body))
			}
		default:
			return nil, g.statusError(where, status, body, isGet)
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%s: %w", where, ctx.Err())
		// jitter so fetches that hit a 429 together don't retry together
		case <-time.After(g.backoff<<(attempt-1) + rand.N(g.backoff)):
		}
	}
}

func (g *gcsReader) once(ctx context.Context, u, tok string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("User-Agent", "dryrun")
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}

// GCS's own 403 message names the principal it refused
func (g *gcsReader) statusError(where string, status int, body []byte, isGet bool) error {
	detail := gcsStatusText(status, body)
	switch status {
	case http.StatusNotFound:
		// list only 404s for the bucket; a get means the object went
		if isGet {
			return fmt.Errorf("%s: %w", where, errObjectNotFound)
		}
		return fmt.Errorf("bucket gs://%s not found: %s", g.bucket, detail)
	case http.StatusUnauthorized:
		return fmt.Errorf("%s: Google credentials rejected (%s): %s", where, gcsLoginHint, detail)
	case http.StatusForbidden:
		return fmt.Errorf("no read access to %s (needs roles/storage.objectViewer): %s", where, detail)
	}
	return fmt.Errorf("%s: %s", where, detail)
}

// GCS errors are {"error":{"message":"..."}}; fall back to the status text
func gcsStatusText(status int, body []byte) string {
	var e struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error.Message != "" {
		return fmt.Sprintf("%d %s", status, e.Error.Message)
	}
	return fmt.Sprintf("%d %s", status, http.StatusText(status))
}
