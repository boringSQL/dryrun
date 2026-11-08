package history

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

const (
	fakeS3Bucket   = "b"
	fakeS3PageSize = 500
)

type (
	// fakeS3 serves path-style ListObjectsV2 and GetObject with S3's XML bodies
	// and error codes. Any other call is recorded: a reader needs no more.
	fakeS3 struct {
		mu         sync.Mutex
		objects    map[string][]byte
		failList   int // 403 AccessDenied on list
		failGet    int // 403 AccessDenied on get
		otherCalls []string
	}

	// only what the SDK's parser reads back
	s3ListResult struct {
		XMLName               xml.Name `xml:"ListBucketResult"`
		IsTruncated           bool
		NextContinuationToken string `xml:",omitempty"`
		Contents              []s3ListEntry
	}

	s3ListEntry struct {
		Key string
	}

	s3ErrorBody struct {
		XMLName xml.Name `xml:"Error"`
		Code    string
		Message string
	}
)

func newFakeS3(t *testing.T, objects map[string][]byte) (*fakeS3, *httptest.Server) {
	t.Helper()
	f := &fakeS3{objects: objects}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv
}

func s3Error(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(s3ErrorBody{Code: code, Message: msg})
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
		s3Error(w, http.StatusForbidden, "AccessDenied", "unsigned request")
		return
	}
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	q := r.URL.Query()
	isList := key == "" && q.Get("list-type") == "2"
	if r.Method != http.MethodGet || (key == "" && !isList) {
		f.otherCalls = append(f.otherCalls, r.Method+" "+r.URL.RequestURI())
		s3Error(w, http.StatusBadRequest, "InvalidRequest", "unexpected call")
		return
	}
	if bucket != fakeS3Bucket {
		switch bucket {
		case "elsewhere":
			w.Header().Set("X-Amz-Bucket-Region", "eu-west-1")
			s3Error(w, http.StatusMovedPermanently, "PermanentRedirect", "The bucket you are attempting to access must be addressed using the specified endpoint.")
			return
		case "wrongsig":
			w.Header().Set("X-Amz-Bucket-Region", "eu-west-1")
			s3Error(w, http.StatusBadRequest, "AuthorizationHeaderMalformed", "the region 'us-east-1' is wrong; expecting 'eu-west-1'")
			return
		}
		s3Error(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}

	if isList {
		if f.failList != 0 {
			s3Error(w, f.failList, "AccessDenied", "Access Denied")
			return
		}
		prefix := q.Get("prefix")
		var names []string
		for n := range f.objects {
			if strings.HasPrefix(n, prefix) {
				names = append(names, n)
			}
		}
		sort.Strings(names)
		start := 0
		if tok := q.Get("continuation-token"); tok != "" {
			start, _ = strconv.Atoi(tok)
		}
		end := min(start+fakeS3PageSize, len(names))
		var res s3ListResult
		for _, n := range names[start:end] {
			res.Contents = append(res.Contents, s3ListEntry{Key: n})
		}
		if end < len(names) {
			res.IsTruncated, res.NextContinuationToken = true, strconv.Itoa(end)
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(res)
		return
	}

	if f.failGet != 0 {
		s3Error(w, f.failGet, "AccessDenied", "Access Denied")
		return
	}
	raw, ok := f.objects[key]
	if !ok {
		s3Error(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	_, _ = w.Write(raw)
}

// static keys, and nothing from the developer's ~/.aws or an IMDS probe
func isolateAWS(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDTEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/none")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/none")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	for _, v := range []string{"AWS_DEFAULT_REGION", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_CA_BUNDLE", "AWS_USE_FIPS_ENDPOINT", "AWS_USE_DUALSTACK_ENDPOINT", "AWS_SESSION_TOKEN"} {
		t.Setenv(v, "")
	}
}

func testS3Reader(t *testing.T, srv *httptest.Server, bucket string) *s3Reader {
	t.Helper()
	isolateAWS(t)
	r, err := newS3Reader(context.Background(), bucket, S3Options{Endpoint: srv.URL, PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestS3ReaderContract(t *testing.T) {
	var f *fakeS3
	testObjectReaderContract(t, func(t *testing.T, objects map[string][]byte) objectReader {
		var srv *httptest.Server
		f, srv = newFakeS3(t, objects)
		return testS3Reader(t, srv, "b")
	})
	if len(f.otherCalls) != 0 {
		t.Errorf("calls outside ListObjectsV2/GetObject: %v", f.otherCalls)
	}
	testObjectReaderErrors(t, func(t *testing.T, fail readerFailure, objects map[string][]byte) objectReader {
		f, srv := newFakeS3(t, objects)
		bucket := "b"
		switch fail {
		case failListDenied:
			f.failList = http.StatusForbidden
		case failBucketMissing:
			bucket = "typo"
		case failGetDenied:
			f.failGet = http.StatusForbidden
		}
		return testS3Reader(t, srv, bucket)
	})
}

// the production layout through the real constructor
func TestS3StoreReadsLikeFilesystem(t *testing.T) {
	ctx := context.Background()
	root := seedParityDir(t)
	f, srv := newFakeS3(t, bucketFromDir(t, root, "dryrun/us/").objects)
	isolateAWS(t)

	u, err := ParseObjectURL("s3://b/dryrun/us")
	if err != nil {
		t.Fatal(err)
	}
	s3store, err := NewS3Store(ctx, u, S3Options{Endpoint: srv.URL, PathStyle: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	fsStore, err := NewFilesystemStore(root)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := s3store.ListKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 {
		t.Fatalf("ListKeys = %v, want 3", keys)
	}
	for _, k := range keys {
		assertSameReads(t, fsStore, s3store, k)
	}
	if len(f.otherCalls) != 0 {
		t.Fatalf("calls outside ListObjectsV2/GetObject: %v", f.otherCalls)
	}
	if _, err := s3store.Put(ctx, keys[0], WrapSchema(testSnapshot("sh-new", "appdb"))); !errors.Is(err, ErrReadOnlyStore) {
		t.Fatalf("put: err = %v, want ErrReadOnlyStore", err)
	}
}

func TestS3ReaderErrorMessages(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, bucket string
		want         []string
	}{
		{name: "wrong region", bucket: "elsewhere", want: []string{"s3://elsewhere is in region eu-west-1, not us-east-1", "AWS_REGION=eu-west-1"}},
		{name: "wrong region signed", bucket: "wrongsig", want: []string{"s3://wrongsig is in region eu-west-1", "AWS_REGION=eu-west-1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, srv := newFakeS3(t, map[string][]byte{"acme/primary/a": []byte("a")})
			_, err := testS3Reader(t, srv, tc.bucket).list(ctx, "acme/")
			for _, w := range tc.want {
				if err == nil || !strings.Contains(err.Error(), w) {
					t.Errorf("err = %v, want it to contain %q", err, w)
				}
			}
		})
	}
}

func TestS3StoreNeedsCredentials(t *testing.T) {
	isolateAWS(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	_, err := newS3Reader(context.Background(), "b", S3Options{})
	if err == nil || !strings.Contains(err.Error(), "no usable AWS credentials for s3://b") || !strings.Contains(err.Error(), "aws sso login") {
		t.Fatalf("err = %v, want the credentials hint", err)
	}
}

func TestS3StoreNeedsARegion(t *testing.T) {
	isolateAWS(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	u, _ := ParseObjectURL("s3://b")
	_, err := NewS3Store(context.Background(), u, S3Options{}, nil)
	if err == nil || !strings.Contains(err.Error(), "no AWS region") {
		t.Fatalf("err = %v, want the region hint", err)
	}
}
