package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// a dryrun.toml whose single profile pins the remote, so pull/push need no flags
func withBucketRemote(t *testing.T, remote string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dryrun.toml")
	toml := "[project]\nid = \"acme\"\n\n[profiles.prod]\ndatabase_id = \"app\"\nremote = \"prod\"\n\n[[remote]]\nname = \"prod\"\n" + remote
	if err := os.WriteFile(path, []byte(toml), 0o644); err != nil {
		t.Fatal(err)
	}
	oldCfg, oldProfile := flagConfig, flagProfile
	flagConfig, flagProfile = path, ""
	t.Cleanup(func() { flagConfig, flagProfile = oldCfg, oldProfile })
}

// opening touches no network: the emulator skips auth, static keys skip IMDS
func offlineClouds(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "127.0.0.1:1")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDTEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "none"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "none"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("PROFILE", "")
}

func TestResolveSyncStoreBucketPaths(t *testing.T) {
	offlineClouds(t)
	withBucketRemote(t, "type = \"oci\"\nref = \"ghcr.io/org/dryrun\"\n")
	ctx := context.Background()
	for _, tc := range []struct {
		name, path string
		push       bool
		wantErr    string
	}{
		{name: "pull gs", path: "gs://snaps/us"},
		{name: "pull s3", path: "s3://snaps"},
		{name: "push gs", path: "gs://snaps", push: true, wantErr: "push to gs:// is not supported yet"},
		{name: "other scheme", path: "ftp://snaps", wantErr: `unsupported store URL scheme "ftp"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := resolveSyncStore(ctx, tc.path, "", "", tc.push)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || s == nil {
				t.Fatalf("store = %v, %v", s, err)
			}
		})
	}
}

func TestResolveSyncStoreBucketRemote(t *testing.T) {
	offlineClouds(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name, remote string
		push         bool
		wantErr      string
	}{
		{name: "gcs pull", remote: "type = \"gcs\"\nref = \"gs://snaps/us\"\n"},
		{name: "s3 pull", remote: "type = \"s3\"\nref = \"s3://snaps\"\nregion = \"eu-west-1\"\n"},
		{name: "gcs push", remote: "type = \"gcs\"\nref = \"gs://snaps\"\n", push: true, wantErr: "push to gs:// is not supported yet"},
		{name: "oci", remote: "type = \"oci\"\nref = \"ghcr.io/org/dryrun\"\n"},
		{name: "untyped oci", remote: "ref = \"ghcr.io/org/dryrun\"\n"},
		{name: "untyped bucket", remote: "ref = \"gs://snaps\"\n", wantErr: "is a bucket URL; set type"},
		{name: "unknown type", remote: "type = \"azure\"\nref = \"az://snaps\"\n", wantErr: "supported: oci, gcs, s3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withBucketRemote(t, tc.remote)
			// no flags: the profile's remote
			s, err := resolveSyncStore(ctx, "", "", "", tc.push)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || s == nil {
				t.Fatalf("store = %v, %v", s, err)
			}
		})
	}
}

// capture --push to a bucket must fail before any node is touched
func TestCapturePushToBucketRefusedUpFront(t *testing.T) {
	offlineClouds(t)
	withBucketRemote(t, "type = \"gcs\"\nref = \"gs://snaps\"\n")
	db := filepath.Join(t.TempDir(), "history.db")
	cmd := snapshotCaptureCmd(&db)
	// nothing listens on port 1: a capture attempt would fail differently
	cmd.SetArgs([]string{"--from", "postgres://127.0.0.1:1/x", "--label", "primary", "--push"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	if err := cmd.ExecuteContext(context.Background()); err == nil || !strings.Contains(err.Error(), "push to gs:// is not supported yet") {
		t.Fatalf("err = %v, want the push refusal", err)
	}
}
