package pgmustard

import "testing"

func TestNewClientFromEnv(t *testing.T) {
	t.Setenv("PGMUSTARD_API_KEY", "test-key")
	c := NewClient("")
	if !c.HasKey() {
		t.Error("expected HasKey=true with env var")
	}
}

func TestNewClientExplicitKey(t *testing.T) {
	c := NewClient("explicit-key")
	if !c.HasKey() {
		t.Error("expected HasKey=true with explicit key")
	}
	if c.apiKey != "explicit-key" {
		t.Errorf("got key %q", c.apiKey)
	}
}

func TestNewClientNoKey(t *testing.T) {
	t.Setenv("PGMUSTARD_API_KEY", "")
	c := NewClient("")
	if c.HasKey() {
		t.Error("expected HasKey=false with no key")
	}
}

func TestAnalyzePlanNoKey(t *testing.T) {
	c := NewClient("")
	t.Setenv("PGMUSTARD_API_KEY", "")
	c.apiKey = ""
	_, err := c.AnalyzePlan([]byte(`{"Plan": {}}`))
	if err == nil {
		t.Error("expected error without API key")
	}
}
