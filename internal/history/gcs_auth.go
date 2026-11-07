package history

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const (
	gcsReadScope = "https://www.googleapis.com/auth/devstorage.read_only"

	// gcloud reports no expiry; re-asking returns a fresh token near the end
	gcloudTokenTTL = 10 * time.Minute

	gcsLoginHint = "run `gcloud auth login` or `gcloud auth application-default login`"
)

// gcsAuth hands out a bearer token from the first working source.
type gcsAuth struct {
	mu       sync.Mutex
	tok      *oauth2.Token
	rejected string // rejected by GCS; skip a source handing it back
	sources  []func(ctx context.Context) (*oauth2.Token, error)
}

// swapped by tests
var findGoogleADC = func(ctx context.Context) (*google.Credentials, error) {
	return google.FindDefaultCredentials(ctx, gcsReadScope)
}

// ADC first, then gcloud's token: `gcloud auth login` alone leaves no ADC.
func newGCSAuth(ctx context.Context) (*gcsAuth, error) {
	var sources []func(ctx context.Context) (*oauth2.Token, error)

	// ADC keeps this ctx for later refreshes: it must outlive the caller
	creds, adcErr := findGoogleADC(context.WithoutCancel(ctx))
	if adcErr == nil {
		sources = append(sources, func(context.Context) (*oauth2.Token, error) {
			tok, err := creds.TokenSource.Token()
			if err != nil {
				return nil, fmt.Errorf("application default credentials: %w (run `gcloud auth application-default login`)", err)
			}
			return tok, nil
		})
	}
	if gcloud, err := exec.LookPath("gcloud"); err == nil {
		sources = append(sources, func(ctx context.Context) (*oauth2.Token, error) {
			out, err := runGcloud(ctx, gcloud, "auth", "print-access-token")
			if err != nil {
				return nil, fmt.Errorf("gcloud auth print-access-token: %w (run `gcloud auth login`)", err)
			}
			return &oauth2.Token{AccessToken: out, Expiry: time.Now().Add(gcloudTokenTTL)}, nil
		})
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("no Google credentials: run `gcloud auth application-default login` (or `gcloud auth login` with gcloud on PATH): %w", adcErr)
	}

	return &gcsAuth{sources: sources}, nil
}

func (a *gcsAuth) token(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.tok.Valid() {
		return a.tok.AccessToken, nil
	}
	var errs []error
	for _, fetch := range a.sources {
		tok, err := fetch(ctx)
		switch {
		case err != nil:
		case tok.AccessToken == "":
			err = errors.New("google credentials returned an empty access token")
		case tok.AccessToken == a.rejected:
			// ADC hands a revoked token back until it expires
			err = errors.New("token rejected by GCS (" + gcsLoginHint + ")")
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		a.tok = tok
		return tok.AccessToken, nil
	}
	return "", errors.Join(errs...)
}

// invalidate drops stale after a 401. Only that token: concurrent GETs that
// saw the same 401 must not discard the one already refetched.
func (a *gcsAuth) invalidate(stale string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.tok != nil && a.tok.AccessToken == stale {
		a.rejected, a.tok = stale, nil
	}
}

func runGcloud(ctx context.Context, gcloud string, args ...string) (string, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, gcloud, args...)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return "", fmt.Errorf("%w: %s", err, msg)
		}
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}
