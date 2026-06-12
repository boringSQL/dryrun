package history

import (
	"context"
	"strings"
	"testing"

	"oras.land/oras-go/v2/registry/remote/auth"
)

// NewAuthClient is the single chokepoint that turns a remote's [[remote]] auth
// config into an oras remote.Client. The dispatch order matters for security and
// for not silently falling back to anonymous access, so these hermetic tests pin
// each branch: token_env wins first, then the gcp/docker mode switch, then the
// default docker-credentials path. The one branch we deliberately do NOT exercise
// is a *successful* gcp (ADC) build, because google.FindDefaultCredentials reaches
// out to the ambient environment (metadata server, GOOGLE_APPLICATION_CREDENTIALS,
// gcloud config) and there is no hermetic, injectable seam for it here. We assert
// the things we can pin without standing up Google credentials: the token_env and
// unknown-mode error paths, and that an explicitly-set token actually flows through
// to the bearer credential.

// A token_env that names a variable which is empty (or unset) must be a hard
// error, never a silent fall-through to anonymous or docker creds. The error has
// to name the offending variable so the user can fix their config.
func TestNewAuthClient_TokenEnvEmptyIsError(t *testing.T) {
	t.Setenv("DRYRUN_TEST_TOKEN", "")

	_, err := NewAuthClient(AuthConfig{TokenEnv: "DRYRUN_TEST_TOKEN"})
	if err == nil {
		t.Fatal("expected an error for an empty token_env variable, got nil")
	}
	if !strings.Contains(err.Error(), "DRYRUN_TEST_TOKEN") {
		t.Errorf("error should name the empty variable, got: %v", err)
	}
}

// When token_env names a populated variable, NewAuthClient returns a static
// bearer client and that exact token has to surface as the credential's
// AccessToken for every registry host. token_env also wins over Mode, so a
// nonsense Mode alongside a good token must not change the outcome.
func TestNewAuthClient_TokenEnvProducesBearer(t *testing.T) {
	const want = "s3cr3t-bearer-token"
	t.Setenv("DRYRUN_TEST_TOKEN", want)

	client, err := NewAuthClient(AuthConfig{TokenEnv: "DRYRUN_TEST_TOKEN", Mode: "this-would-error-without-token"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ac, ok := client.(*auth.Client)
	if !ok {
		t.Fatalf("expected *auth.Client, got %T", client)
	}
	cred, err := ac.Credential(context.Background(), "us-docker.pkg.dev")
	if err != nil {
		t.Fatalf("credential callback errored: %v", err)
	}
	if cred.AccessToken != want {
		t.Errorf("AccessToken = %q, want %q", cred.AccessToken, want)
	}
}

// An unrecognized auth mode must fail loudly rather than quietly degrading to the
// docker-credentials default; a typo like "gpc" should never widen access.
func TestNewAuthClient_UnknownModeIsError(t *testing.T) {
	_, err := NewAuthClient(AuthConfig{Mode: "gpc"})
	if err == nil {
		t.Fatal("expected an error for an unknown auth mode, got nil")
	}
	if !strings.Contains(err.Error(), "gpc") {
		t.Errorf("error should name the bad mode, got: %v", err)
	}
}

// The empty and "docker" modes are the same default path: load docker
// credentials. NewStoreFromDocker is happy with no config file present (it just
// yields an empty store), so this must succeed and hand back a usable client
// without reaching for any Google or token_env machinery.
func TestNewAuthClient_DefaultDockerModes(t *testing.T) {
	for _, mode := range []string{"", "docker"} {
		client, err := NewAuthClient(AuthConfig{Mode: mode})
		if err != nil {
			t.Fatalf("mode %q: unexpected error: %v", mode, err)
		}
		if client == nil {
			t.Fatalf("mode %q: expected a non-nil client", mode)
		}
	}
}
