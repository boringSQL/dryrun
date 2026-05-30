package buildinfo

import "testing"

func TestGetReturnsLdflagsVersion(t *testing.T) {
	// Get() should hand back whatever ldflags injected, untouched.
	old := Version
	t.Cleanup(func() { Version = old })

	Version = "v0.9.0"
	if got := Get(); got != "v0.9.0" {
		t.Fatalf("Get() = %q, want v0.9.0", got)
	}
}

func TestPseudoVersionMatches(t *testing.T) {
	// these are the build-info Main.Version strings we must NOT report as a real
	// version; matching them makes Get() fall through to "dev".
	pseudo := []string{
		"v0.0.0-20260530120000-abc123456789",   // untagged repo
		"v0.9.0-0.20260530120000-abc123456789", // tagged ancestor, newer commit
		"v1.2.3-0.20260530120000-abc123456789+incompatible",
	}
	for _, v := range pseudo {
		if !pseudoVersion.MatchString(v) {
			t.Errorf("expected %q to be detected as a pseudo-version", v)
		}
	}

	real := []string{
		"v0.9.0",
		"v0.8.2",
		"v1.2.3-rc1",
		"v2.0.0+incompatible",
	}
	for _, v := range real {
		if pseudoVersion.MatchString(v) {
			t.Errorf("did not expect %q to match the pseudo-version pattern", v)
		}
	}
}
