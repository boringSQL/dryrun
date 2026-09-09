package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/config"
	"github.com/boringsql/dryrun/internal/history"
)

func setupCmd() *cobra.Command {
	var (
		agentsFlag string
		hindsight  bool
		remoteName string
	)

	cmd := &cobra.Command{
		Use:   "setup",
		Short: "Wire this repo's AI agents (Claude Code, Cursor, …) to the dryrun MCP server",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cwd, err := os.Getwd()
			if err != nil {
				return err
			}

			// resolve before writing anything: --hindsight must not half-apply silently
			var hosted *hindsightTarget
			if hindsight {
				_, cfg, err := loadProjectConfig()
				if err != nil {
					return err
				}
				key, profile, err := setupKey(cfg, cwd)
				if err != nil {
					return err
				}
				target, err := resolveHindsight(cfg, key, remoteName, profile)
				if err != nil {
					return err
				}
				target.report(os.Stderr)
				hosted = &target
			}
			return configureAgents(cwd, cmd.Flags().Changed("agents"), agentsFlag, hosted)
		},
	}
	cmd.Flags().StringVar(&agentsFlag, "agents", "",
		"agents to configure: comma list (claude,cursor,codex,zed), 'all', or '' to skip; omit the flag for interactive detection")
	cmd.Flags().BoolVar(&hindsight, "hindsight", false,
		"also register the hosted Hindsight endpoint for this database, alongside the local server")
	cmd.Flags().StringVar(&remoteName, "remote", "",
		"which [[remote]] the hosted endpoint lives on (with --hindsight); default remote if omitted")
	return cmd
}

func configureAgents(cwd string, flagChanged bool, raw string, hosted *hindsightTarget) error {
	home, _ := os.UserHomeDir()
	reg := agentRegistry()
	raw = strings.TrimSpace(raw)

	var selected []agentDef
	switch {
	case flagChanged && (raw == "" || raw == "none"):
		noteUnwritten(hosted)
		return nil
	case !flagChanged:
		detected := detectAgents(reg, cwd, home)
		if len(detected) == 0 {
			fmt.Fprintln(os.Stderr, "No agents detected in this repo. Pass --agents=claude,cursor,… to configure explicitly.")
			noteUnwritten(hosted)
			return nil
		}
		if !isTTY() {
			fmt.Fprintf(os.Stderr,
				"Detected agents: %s. Pass --agents=all (or a comma list) to write MCP config (skipped: not a TTY).\n",
				agentLabels(detected))
			noteUnwritten(hosted)
			return nil
		}
		selected = promptSelect(detected)
	case raw == "all" || raw == "auto":
		selected = detectAgents(reg, cwd, home)
		if len(selected) == 0 {
			fmt.Fprintln(os.Stderr, "No agents detected in this repo; nothing to configure.")
			noteUnwritten(hosted)
			return nil
		}
	default:
		var unknown []string
		selected, unknown = resolveNamed(reg, raw)
		for _, u := range unknown {
			fmt.Fprintf(os.Stderr, "Unknown agent %q (known: claude, cursor, codex, zed)\n", u)
		}
	}

	if len(selected) == 0 {
		noteUnwritten(hosted)
		return nil
	}
	return writeAgentConfigs(cwd, selected, hosted)
}

// The endpoint was already reported, so an opt-out must not imply it was written.
func noteUnwritten(hosted *hindsightTarget) {
	if hosted != nil {
		fmt.Fprintln(os.Stderr, "No agent configured, so the endpoint above was not written anywhere.")
	}
}

// Key and profile name resolved ONCE, together: resolveSnapshotKey swallows an
// ambiguous profile (addressing a different database), and a second resolution
// could disagree with the reported URL.
func setupKey(cfg *config.ProjectConfig, cwd string) (history.SnapshotKey, string, error) {
	if cfg == nil || len(cfg.Profiles) == 0 {
		return resolveSnapshotKey(), "", nil
	}
	rp, err := cfg.ResolveProfile(nil, nilIfEmpty(flagProfile), cwd)
	if err != nil {
		if len(cfg.Profiles) > 1 {
			names := make([]string, 0, len(cfg.Profiles))
			for name := range cfg.Profiles {
				names = append(names, name)
			}
			sort.Strings(names)
			return history.SnapshotKey{}, "", fmt.Errorf("several profiles defined (%s) and none selected; pass --profile to say which database this endpoint is for",
				strings.Join(names, ", "))
		}
		return history.SnapshotKey{}, "", err
	}
	return rp.SnapshotKey(), rp.Name, nil
}
