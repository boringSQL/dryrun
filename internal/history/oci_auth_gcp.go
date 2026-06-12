package history

import (
	"context"
	"fmt"

	"golang.org/x/oauth2/google"

	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/retry"
)

// GAR/GCR accept Application Default Credentials as a bearer token; the
// TokenSource auto-refreshes the ~1h token mid-operation.
func gcpADCClient(ctx context.Context) (*auth.Client, error) {
	creds, err := google.FindDefaultCredentials(ctx, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, fmt.Errorf("oci auth: google ADC: %w", err)
	}
	ts := creds.TokenSource
	c := &auth.Client{
		Client: retry.DefaultClient,
		Cache:  auth.NewCache(),
		Credential: func(context.Context, string) (auth.Credential, error) {
			tok, err := ts.Token()
			if err != nil {
				return auth.Credential{}, fmt.Errorf("oci auth: google token: %w", err)
			}
			return auth.Credential{AccessToken: tok.AccessToken}, nil
		},
	}
	c.SetUserAgent("dryrun")
	return c, nil
}
