package history

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var latestRefRe = regexp.MustCompile(`^latest(?:~(\d+))?$`)

// schema|planner|activity|query; activity/query default to the sole node, else errors listing them
// One place that knows the kind names, so `--kind` means the same thing to
// every command that takes it.
func ParseKindTag(kind string) (SnapshotKindTag, error) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "", "schema":
		return KindSchema, nil
	case "planner":
		return KindPlanner, nil
	case "activity":
		return KindActivity, nil
	case "query":
		return KindQuery, nil
	}
	return 0, fmt.Errorf("unknown kind %q (use schema|planner|activity|query)", kind)
}

func (s *Store) ResolveKindFlag(ctx context.Context, key SnapshotKey, kindFlag, nodeFlag string) (SnapshotKind, error) {
	tag, err := ParseKindTag(kindFlag)
	if err != nil {
		return SnapshotKind{}, err
	}
	switch tag {
	case KindSchema:
		return SchemaKind(), nil
	case KindPlanner:
		return PlannerKind(), nil
	case KindActivity:
		return s.resolveNodeKind(ctx, key, nodeFlag, ActivityKind)
	default:
		return s.resolveNodeKind(ctx, key, nodeFlag, QueryKind)
	}
}

// resolveNodeKind picks the sole node for a per-node kind (activity/query), or
// errors listing the available nodes when there's more than one.
func (s *Store) resolveNodeKind(ctx context.Context, key SnapshotKey, nodeFlag string, mk func(string) SnapshotKind) (SnapshotKind, error) {
	if nodeFlag != "" {
		return mk(nodeFlag), nil
	}
	kinds, err := s.ListKinds(ctx, key)
	if err != nil {
		return SnapshotKind{}, err
	}
	tag := mk("").Tag
	var nodes []string
	for _, k := range kinds {
		if k.Tag == tag {
			nodes = append(nodes, k.NodeLabel)
		}
	}
	switch len(nodes) {
	case 0:
		return SnapshotKind{}, fmt.Errorf("no %s snapshots in history", mk(""))
	case 1:
		return mk(nodes[0]), nil
	default:
		return SnapshotKind{}, fmt.Errorf("multiple %s nodes (%s); pass a node to pick one", mk(""), strings.Join(nodes, ", "))
	}
}

// latest/latest~N take kind from kindFlag; a hash prefix carries its own. Shared by CLI and MCP.
func (s *Store) ResolveToken(ctx context.Context, key SnapshotKey, token, kindFlag, nodeFlag string) (SnapshotKind, SnapshotRef, error) {
	if m := latestRefRe.FindStringSubmatch(token); m != nil {
		kind, err := s.ResolveKindFlag(ctx, key, kindFlag, nodeFlag)
		if err != nil {
			return SnapshotKind{}, SnapshotRef{}, err
		}
		if m[1] == "" {
			return kind, NewRefLatest(), nil
		}
		n, _ := strconv.Atoi(m[1])
		list, err := s.List(ctx, key, kind, TimeRange{})
		if err != nil {
			return SnapshotKind{}, SnapshotRef{}, err
		}
		if n >= len(list) {
			return SnapshotKind{}, SnapshotRef{},
				fmt.Errorf("latest~%d: only %d %s snapshot(s) in history", n, len(list), kind)
		}
		return kind, NewRefHash(list[n].ContentHash), nil
	}
	kind, err := s.ResolveKind(ctx, key, token)
	if err != nil {
		return SnapshotKind{}, SnapshotRef{}, err
	}
	return kind, NewRefHash(token), nil
}
