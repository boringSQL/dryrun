package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func setupCmd() *cobra.Command {
	var agentsFlag string

	cmd := &cobra.Command{
		Use:   "setup",
		Short: "Wire this repo's AI agents (Claude Code, Cursor, …) to the dryrun MCP server",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cwd, err := os.Getwd()
			if err != nil {
				return err
			}
			return configureAgents(cwd, cmd.Flags().Changed("agents"), agentsFlag)
		},
	}
	cmd.Flags().StringVar(&agentsFlag, "agents", "",
		"agents to configure: comma list (claude,cursor,codex,zed), 'all', or '' to skip; omit the flag for interactive detection")
	return cmd
}

func configureAgents(cwd string, flagChanged bool, raw string) error {
	home, _ := os.UserHomeDir()
	reg := agentRegistry()
	raw = strings.TrimSpace(raw)

	var selected []agentDef
	switch {
	case flagChanged && (raw == "" || raw == "none"):
		return nil
	case !flagChanged:
		detected := detectAgents(reg, cwd, home)
		if len(detected) == 0 {
			fmt.Fprintln(os.Stderr, "No agents detected in this repo. Pass --agents=claude,cursor,… to configure explicitly.")
			return nil
		}
		if !isTTY() {
			fmt.Fprintf(os.Stderr,
				"Detected agents: %s. Pass --agents=all (or a comma list) to write MCP config (skipped: not a TTY).\n",
				agentLabels(detected))
			return nil
		}
		selected = promptSelect(detected)
	case raw == "all" || raw == "auto":
		selected = detectAgents(reg, cwd, home)
		if len(selected) == 0 {
			fmt.Fprintln(os.Stderr, "No agents detected in this repo; nothing to configure.")
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
		return nil
	}
	return writeAgentConfigs(cwd, selected)
}
