package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// removed subcommands and their replacements, keyed by full command path
var removedCommands = map[string]string{
	"dryrun snapshot take":        "dryrun snapshot capture --label primary --streams schema,planner,activity,query",
	"dryrun snapshot activity":    "dryrun snapshot capture --from <url> --label <name> --streams activity",
	"dryrun snapshot query-stats": "dryrun snapshot capture --from <url> --label <name> --streams query",
}

// cobra rejects unknown subcommands only at the root; below it they print help and exit 0
func rejectUnknownSubcommands(c *cobra.Command) {
	for _, sub := range c.Commands() {
		rejectUnknownSubcommands(sub)
	}
	if !c.HasSubCommands() || c.Runnable() || !c.HasParent() {
		return
	}
	// a removed command's flags must not fail parsing before the hint below
	c.FParseErrWhitelist.UnknownFlags = true
	c.SuggestionsMinimumDistance = 2
	c.RunE = func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}
		path := cmd.CommandPath() + " " + args[0]
		if repl, ok := removedCommands[path]; ok {
			cmd.SilenceUsage = true
			return fmt.Errorf("`%s` was removed in v0.19; use:\n  %s", path, repl)
		}
		if s := cmd.SuggestionsFor(args[0]); len(s) > 0 {
			return fmt.Errorf("unknown command %q for %q\n\nDid you mean this?\n\t%s", args[0], cmd.CommandPath(), strings.Join(s, "\n\t"))
		}
		return fmt.Errorf("unknown command %q for %q", args[0], cmd.CommandPath())
	}
}
