package history

import (
	"context"
	"fmt"
	"os"

	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"
	"oras.land/oras-go/v2/registry/remote/retry"
)

type AuthConfig struct {
	TokenEnv string
	Mode     string // "" docker creds; "gcp" GAR/GCR via ADC
}

// token_env wins; then mode=gcp (ADC); else docker creds (GAR/GHCR/ECR/Hub after login)
func NewAuthClient(cfg AuthConfig) (remote.Client, error) {
	if cfg.TokenEnv != "" {
		token := os.Getenv(cfg.TokenEnv)
		if token == "" {
			return nil, fmt.Errorf("oci auth: token_env %q is set but the variable is empty", cfg.TokenEnv)
		}
		return staticBearerClient(token), nil
	}

	switch cfg.Mode {
	case "", "docker":
	case "gcp":
		return gcpADCClient(context.Background())
	default:
		return nil, fmt.Errorf("oci auth: unknown auth mode %q", cfg.Mode)
	}

	store, err := credentials.NewStoreFromDocker(credentials.StoreOptions{})
	if err != nil {
		return nil, fmt.Errorf("oci auth: load docker credentials: %w", err)
	}
	c := &auth.Client{
		Client:     retry.DefaultClient,
		Cache:      auth.NewCache(),
		Credential: credentials.Credential(store),
	}
	c.SetUserAgent("dryrun")
	return c, nil
}

func staticBearerClient(token string) *auth.Client {
	c := &auth.Client{
		Client: retry.DefaultClient,
		Cache:  auth.NewCache(),
		Credential: func(context.Context, string) (auth.Credential, error) {
			return auth.Credential{AccessToken: token}, nil
		},
	}
	c.SetUserAgent("dryrun")
	return c
}
