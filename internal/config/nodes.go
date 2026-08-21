package config

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

type ResolvedNode struct {
	Name string
	// primary | standby | auto; asserted against the node at capture
	Role string
	// nil when the block did not name any: the capture picks by detected role
	Streams  []string
	Interval time.Duration
	Pool     bool

	// resolved on demand: an unset variable is one node's problem, not the
	// whole fleet's
	url    string
	urlEnv string
}

// URL reads the connection string from the environment at the point of use.
func (n ResolvedNode) URL() (string, error) {
	if n.urlEnv != "" {
		v := os.Getenv(n.urlEnv)
		if v == "" {
			return "", fmt.Errorf("url_env %s is unset in this environment", n.urlEnv)
		}
		return v, nil
	}
	v := ExpandEnvVars(n.url)
	if strings.Contains(n.url, "${") && v == "" {
		return "", fmt.Errorf("url %q expanded to nothing", n.url)
	}
	return v, nil
}

// Streams a node can feed. schema and planner are primary-only in practice but
// the check belongs at capture, where the role is known.
var knownStreams = []string{"planner", "activity", "query"}

// Config declares intent; nothing here connects. A bad block should fail the
// command that reads it, not the capture that is already halfway through a
// fleet.
func (c *ProjectConfig) ResolveNodes() ([]ResolvedNode, error) {
	seen := map[string]bool{}
	out := make([]ResolvedNode, 0, len(c.Nodes))
	for i, n := range c.Nodes {
		r, err := resolveNode(n)
		if err != nil {
			return nil, fmt.Errorf("[[node]] #%d (%s): %w", i+1, n.Name, err)
		}
		if seen[r.Name] {
			return nil, fmt.Errorf("[[node]] %q is defined twice; one label is one series", r.Name)
		}
		seen[r.Name] = true
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (c *ProjectConfig) ResolveNode(name string) (*ResolvedNode, error) {
	nodes, err := c.ResolveNodes()
	if err != nil {
		return nil, err
	}
	for i := range nodes {
		if nodes[i].Name == name {
			return &nodes[i], nil
		}
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no [[node]] blocks in dryrun.toml")
	}
	return nil, fmt.Errorf("no [[node]] named %q; defined: %s", name, strings.Join(nodeNames(nodes), ", "))
}

func nodeNames(nodes []ResolvedNode) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.Name
	}
	return out
}

// resolveNode's stream rules for names from the CLI instead of a [[node]]
// block: fail before anything connects
func ValidateStreams(streams []string) error {
	for _, s := range streams {
		if err := validateStream(strings.TrimSpace(s)); err != nil {
			return err
		}
	}
	return nil
}

func validateStream(s string) error {
	if s == "schema" {
		return fmt.Errorf("stream \"schema\": captured by `dryrun snapshot take`, which guards that it runs on a primary")
	}
	if !hasString(knownStreams, s) {
		return fmt.Errorf("stream %q: want one of %s", s, strings.Join(knownStreams, ", "))
	}
	return nil
}

func resolveNode(n NodeConfig) (ResolvedNode, error) {
	r := ResolvedNode{Name: strings.TrimSpace(n.Name), Pool: n.Pool}
	if r.Name == "" {
		return r, fmt.Errorf("name is required; it becomes the node label and every counter is differenced per label")
	}

	switch strings.TrimSpace(n.Role) {
	case "", "auto":
		r.Role = "auto"
	case "primary", "standby":
		r.Role = strings.TrimSpace(n.Role)
	default:
		return r, fmt.Errorf("role %q: want primary, standby, or auto", n.Role)
	}

	if n.URL != "" && n.URLEnv != "" {
		return r, fmt.Errorf("set url or url_env, not both")
	}
	if n.URL == "" && n.URLEnv == "" {
		return r, fmt.Errorf("one of url or url_env is required")
	}
	r.url, r.urlEnv = n.URL, n.URLEnv

	for _, s := range n.Streams {
		s = strings.TrimSpace(s)
		if err := validateStream(s); err != nil {
			return r, err
		}
		if !hasString(r.Streams, s) {
			r.Streams = append(r.Streams, s)
		}
	}

	if n.Interval != "" {
		d, err := time.ParseDuration(n.Interval)
		if err != nil {
			return r, fmt.Errorf("interval %q: %w", n.Interval, err)
		}
		if d <= 0 {
			return r, fmt.Errorf("interval %q must be positive", n.Interval)
		}
		r.Interval = d
	}
	return r, nil
}

// DefaultStreamsFor is what a node captures when its block names none.
// A standby has no schema to write and its planner stats mirror the primary's.
func DefaultStreamsFor(role string) []string {
	if role == "standby" {
		return []string{"activity", "query"}
	}
	return []string{"planner", "activity", "query"}
}

func hasString(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
