package history

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// fakeGCS serves the two JSON API calls gcsReader makes, with the paging,
// escaping and error bodies of the real one. Any other path is recorded: a
// reader with objectViewer only must never need it.
type fakeGCS struct {
	mu          sync.Mutex
	bucket      string
	objects     map[string][]byte
	token       string // required bearer; "" accepts anything
	failList    int    // status code to answer lists with
	failGet     int    // status code to answer gets with
	unavailable int    // 503s to answer before serving
	otherPaths  []string
	pagedLists  int // lists that carried a pageToken
}

// small enough that the contract's 1200 objects span several pages
const fakeGCSPageSize = 500

func newFakeGCS(t *testing.T, objects map[string][]byte) (*fakeGCS, *httptest.Server) {
	t.Helper()
	f := &fakeGCS{bucket: "b", objects: objects}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv
}

func gcsError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": msg}})
}

func (f *fakeGCS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// the escaped path, as GCS reads it: an object name is a single segment, so a
	// raw "/" in it is a different (missing) object, not the same one
	rest, ok := strings.CutPrefix(r.URL.EscapedPath(), "/storage/v1/b/")
	bucket, objPath, _ := strings.Cut(rest, "/")
	escName, isGet := strings.CutPrefix(objPath, "o/")
	if !ok || r.Method != http.MethodGet || (objPath != "o" && !isGet) || strings.Contains(escName, "/") {
		f.otherPaths = append(f.otherPaths, r.Method+" "+r.URL.EscapedPath())
		gcsError(w, http.StatusBadRequest, "unexpected call")
		return
	}
	bucket, _ = url.PathUnescape(bucket)
	if f.unavailable > 0 {
		f.unavailable--
		gcsError(w, http.StatusServiceUnavailable, "backend error")
		return
	}
	if f.token != "" && r.Header.Get("Authorization") != "Bearer "+f.token {
		gcsError(w, http.StatusUnauthorized, "Invalid Credentials")
		return
	}
	if bucket != f.bucket {
		gcsError(w, http.StatusNotFound, "The specified bucket does not exist.")
		return
	}

	if objPath == "o" {
		if code := f.failList; code != 0 {
			gcsError(w, code, "reader@example.com does not have storage.objects.list access to the Google Cloud Storage bucket.")
			return
		}
		if r.URL.Query().Get("fields") == "" {
			gcsError(w, http.StatusBadRequest, "test fake: list without fields")
			return
		}
		prefix := r.URL.Query().Get("prefix")
		var names []string
		for n := range f.objects {
			if strings.HasPrefix(n, prefix) {
				names = append(names, n)
			}
		}
		sort.Strings(names)
		start := 0
		if tok := r.URL.Query().Get("pageToken"); tok != "" {
			start, _ = strconv.Atoi(tok)
			f.pagedLists++
		}
		end := min(start+fakeGCSPageSize, len(names))
		var page gcsListPage
		for _, n := range names[start:end] {
			page.Items = append(page.Items, struct {
				Name string `json:"name"`
			}{n})
		}
		if end < len(names) {
			page.NextPageToken = strconv.Itoa(end)
		}
		_ = json.NewEncoder(w).Encode(page)
		return
	}

	name, err := url.PathUnescape(escName)
	if err != nil {
		gcsError(w, http.StatusBadRequest, "bad escape")
		return
	}
	if r.URL.Query().Get("alt") != "media" {
		gcsError(w, http.StatusBadRequest, "test fake: get without alt=media")
		return
	}
	if code := f.failGet; code != 0 {
		gcsError(w, code, "reader@example.com does not have storage.objects.get access to the Google Cloud Storage object.")
		return
	}
	raw, ok := f.objects[name]
	if !ok {
		gcsError(w, http.StatusNotFound, "No such object: "+f.bucket+"/"+name)
		return
	}
	_, _ = w.Write(raw)
}

func staticGCSAuth(tokens ...string) (*gcsAuth, *int) {
	n := 0
	return &gcsAuth{sources: []func(context.Context) (*oauth2.Token, error){
		func(context.Context) (*oauth2.Token, error) {
			tok := tokens[min(n, len(tokens)-1)]
			n++
			return &oauth2.Token{AccessToken: tok}, nil
		},
	}}, &n
}

func testGCSReader(srv *httptest.Server, auth *gcsAuth) *gcsReader {
	return &gcsReader{bucket: "b", endpoint: srv.URL, client: srv.Client(), auth: auth, backoff: time.Millisecond}
}

func TestGCSReaderContract(t *testing.T) {
	var f *fakeGCS
	testObjectReaderContract(t, func(t *testing.T, objects map[string][]byte) objectReader {
		var srv *httptest.Server
		f, srv = newFakeGCS(t, objects)
		f.token = "tok"
		auth, _ := staticGCSAuth("tok")
		return testGCSReader(srv, auth)
	})
	if len(f.otherPaths) != 0 {
		t.Errorf("calls outside objects.list/get: %v", f.otherPaths)
	}
	if f.pagedLists < 2 {
		t.Errorf("paged lists = %d; the contract's 1200 objects span 3 pages of 500", f.pagedLists)
	}
	testObjectReaderErrors(t, func(t *testing.T, fail readerFailure, objects map[string][]byte) objectReader {
		f, srv := newFakeGCS(t, objects)
		r := testGCSReader(srv, nil)
		switch fail {
		case failListDenied:
			f.failList = http.StatusForbidden
		case failBucketMissing:
			r.bucket = "typo"
		case failGetDenied:
			f.failGet = http.StatusForbidden
		}
		return r
	})
}

// the production layout through the real constructor, as pull will open it
func TestGCSStoreReadsLikeFilesystem(t *testing.T) {
	ctx := context.Background()
	root := seedParityDir(t)
	f, srv := newFakeGCS(t, bucketFromDir(t, root, "dryrun/us/").objects)
	t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(srv.URL, "http://"))

	u, err := ParseObjectURL("gs://b/dryrun/us")
	if err != nil {
		t.Fatal(err)
	}
	gcs, err := NewGCSStore(ctx, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	fsStore, err := NewFilesystemStore(root)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := gcs.ListKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 {
		t.Fatalf("ListKeys = %v, want 3", keys)
	}
	for _, k := range keys {
		assertSameReads(t, fsStore, gcs, k)
	}
	if len(f.otherPaths) != 0 {
		t.Fatalf("calls outside objects.list/get: %v", f.otherPaths)
	}
	if _, err := gcs.Put(ctx, keys[0], WrapSchema(testSnapshot("sh-new", "appdb"))); !errors.Is(err, ErrReadOnlyStore) {
		t.Fatalf("put: err = %v, want ErrReadOnlyStore", err)
	}
}

func TestGCSReaderRefreshesTokenOn401(t *testing.T) {
	ctx := context.Background()
	f, srv := newFakeGCS(t, map[string][]byte{"acme/primary/a": []byte("a")})
	f.token = "fresh"

	auth, fetches := staticGCSAuth("revoked", "fresh")
	got, err := testGCSReader(srv, auth).get(ctx, "acme/primary/a")
	if err != nil || string(got) != "a" {
		t.Fatalf("get = %q, %v; want a after one refresh", got, err)
	}
	if *fetches != 2 {
		t.Fatalf("token fetches = %d, want 2", *fetches)
	}

	// the source hands the revoked token back: say how to log in, don't loop
	auth, fetches = staticGCSAuth("revoked")
	_, err = testGCSReader(srv, auth).list(ctx, "acme/")
	if err == nil || !strings.Contains(err.Error(), "rejected") || !strings.Contains(err.Error(), "gcloud auth application-default login") {
		t.Fatalf("err = %v, want the login hint", err)
	}
	if *fetches != 2 {
		t.Fatalf("token fetches = %d, want exactly one refresh", *fetches)
	}
}

func TestGCSReaderRetriesUnavailable(t *testing.T) {
	ctx := context.Background()
	f, srv := newFakeGCS(t, map[string][]byte{"acme/primary/a": []byte("a")})

	f.unavailable = gcsAttempts - 1
	if got, err := testGCSReader(srv, nil).get(ctx, "acme/primary/a"); err != nil || string(got) != "a" {
		t.Fatalf("get after %d 503s = %q, %v", gcsAttempts-1, got, err)
	}

	f.unavailable = gcsAttempts
	_, err := testGCSReader(srv, nil).get(ctx, "acme/primary/a")
	if err == nil || !strings.Contains(err.Error(), "503 backend error") || !strings.Contains(err.Error(), "gs://b/acme/primary/a") {
		t.Fatalf("err = %v, want the 503 for gs://b/acme/primary/a", err)
	}
}

// --- auth ---

func stubADC(t *testing.T, creds *google.Credentials, err error) {
	t.Helper()
	prev := findGoogleADC
	findGoogleADC = func(context.Context) (*google.Credentials, error) { return creds, err }
	t.Cleanup(func() { findGoogleADC = prev })
}

// fakeGcloud puts a gcloud first on PATH that logs each call to $LOG
func fakeGcloud(t *testing.T, script string) string {
	t.Helper()
	dir := t.TempDir()
	log := filepath.Join(dir, "calls.log")
	body := "#!/bin/sh\nLOG=" + log + "\necho \"$*\" >> \"$LOG\"\n" + script
	if err := os.WriteFile(filepath.Join(dir, "gcloud"), []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+"/usr/bin:/bin")
	return log
}

func gcloudCalls(t *testing.T, log string) []string {
	t.Helper()
	raw, err := os.ReadFile(log)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(raw)), "\n")
}

func TestGCSAuthPrefersADC(t *testing.T) {
	ctx := context.Background()
	stubADC(t, &google.Credentials{
		TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "adc-token"}),
	}, nil)
	log := fakeGcloud(t, "echo gcloud-token\n")

	a, err := newGCSAuth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tok, err := a.token(ctx); err != nil || tok != "adc-token" {
		t.Fatalf("token = %q, %v; want the ADC token", tok, err)
	}
	if calls := gcloudCalls(t, log); len(calls) != 0 {
		t.Fatalf("gcloud ran with ADC present: %v", calls)
	}
}

func TestGCSAuthFallsBackToGcloud(t *testing.T) {
	ctx := context.Background()
	stubADC(t, nil, errors.New("could not find default credentials"))
	log := fakeGcloud(t, `echo "gcloud-token-$(grep -c print-access-token "$LOG")"
`)

	a, err := newGCSAuth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first, err := a.token(ctx)
	if err != nil || !strings.HasPrefix(first, "gcloud-token-") {
		t.Fatalf("token = %q, %v", first, err)
	}
	if again, _ := a.token(ctx); again != first {
		t.Fatalf("second token = %q, want the cached %q", again, first)
	}
	a.invalidate("someone-elses-token") // not the cached one: keep it
	if again, _ := a.token(ctx); again != first {
		t.Fatalf("token after a stale invalidate = %q, want the cached %q", again, first)
	}
	a.invalidate(first)
	if fresh, _ := a.token(ctx); fresh == first {
		t.Fatalf("token after invalidate = %q, want a fresh one", fresh)
	}
	want := []string{"auth print-access-token", "auth print-access-token"}
	if got := gcloudCalls(t, log); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("gcloud calls = %q, want %q", got, want)
	}
}

// the invalid_rapt case: ADC present but stale, gcloud's own login fresh
func TestGCSAuthStaleADCFallsBackToGcloud(t *testing.T) {
	ctx := context.Background()
	stubADC(t, &google.Credentials{
		TokenSource: oauth2.ReuseTokenSource(nil, failingTokenSource{errors.New(`oauth2: "invalid_grant" "reauth related error (invalid_rapt)"`)}),
	}, nil)
	fakeGcloud(t, "echo gcloud-token\n")
	a, err := newGCSAuth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tok, err := a.token(ctx); err != nil || tok != "gcloud-token" {
		t.Fatalf("token = %q, %v; want gcloud's", tok, err)
	}

	// both stale: say both commands, keep both causes
	fakeGcloud(t, "echo 'Reauthentication failed.' >&2\nexit 1\n")
	a, err = newGCSAuth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.token(ctx)
	for _, want := range []string{"invalid_rapt", "gcloud auth application-default login", "Reauthentication failed", "gcloud auth login"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v, want it to contain %q", err, want)
		}
	}
}

type failingTokenSource struct{ err error }

func (f failingTokenSource) Token() (*oauth2.Token, error) { return nil, f.err }
