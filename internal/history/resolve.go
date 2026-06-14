package history

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var latestRefRe = regexp.MustCompile(`^latest(?:~(\d+))?$`)

// schema|planner|activity; activity defaults to the sole node, else errors listing them
func (s *Store) ResolveKindFlag(ctx context.Context, key SnapshotKey, kindFlag, nodeFlag string) (SnapshotKind, error) {
	switch strings.ToLower(kindFlag) {
	case "", "schema":
		return SchemaKind(), nil
	case "planner":
		return PlannerKind(), nil
	case "activity":
		return s.resolveActivityKind(ctx, key, nodeFlag)
	default:
		return SnapshotKind{}, fmt.Errorf("unknown kind %q (use schema|planner|activity)", kindFlag)
	}
}

func (s *Store) resolveActivityKind(ctx context.Context, key SnapshotKey, nodeFlag string) (SnapshotKind, error) {
	if nodeFlag != "" {
		return ActivityKind(nodeFlag), nil
	}
	kinds, err := s.ListKinds(ctx, key)
	if err != nil {
		return SnapshotKind{}, err
	}
	var nodes []string
	for _, k := range kinds {
		if k.Tag == KindActivity {
			nodes = append(nodes, k.NodeLabel)
		}
	}
	switch len(nodes) {
	case 0:
		return SnapshotKind{}, fmt.Errorf("no activity snapshots in history")
	case 1:
		return ActivityKind(nodes[0]), nil
	default:
		return SnapshotKind{}, fmt.Errorf("multiple activity nodes (%s); pass a node to pick one", strings.Join(nodes, ", "))
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
