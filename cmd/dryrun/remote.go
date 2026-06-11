package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/config"
)

func remoteCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "remote", Short: "Manage [[remote]] entries in dryrun.toml"}
	cmd.AddCommand(remoteAddCmd(), remoteListCmd(), remoteRmCmd())
	return cmd
}

func remoteListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List configured remotes",
		RunE: func(cmd *cobra.Command, args []string) error {
			path, cfg, err := loadProjectConfig()
			if err != nil {
				return err
			}
			fmt.Printf("Config: %s\n", path)
			if len(cfg.Remotes) == 0 {
				fmt.Println("No remotes configured.")
				return nil
			}
			for _, r := range cfg.Remotes {
				def := ""
				if r.Default {
					def = " (default)"
				}
				fmt.Printf("  %s  %s  %s%s\n", r.Name, r.Type, r.Ref, def)
			}
			return nil
		},
	}
}

func remoteAddCmd() *cobra.Command {
	var (
		typ, ref, tokenEnv string
		isDefault          bool
	)
	cmd := &cobra.Command{
		Use:   "add <name>",
		Short: "Add a remote to dryrun.toml",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if ref == "" {
				return fmt.Errorf("--ref is required")
			}
			path, cfg, err := loadProjectConfig()
			if err != nil {
				return err
			}
			for _, r := range cfg.Remotes {
				if r.Name == name {
					return fmt.Errorf("remote %q already exists", name)
				}
			}
			block := remoteBlock(config.RemoteConfig{
				Name: name, Type: typ, Ref: ref, TokenEnv: tokenEnv, Default: isDefault,
			})
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			out := strings.TrimRight(string(data), "\n") + "\n" + block
			if err := os.WriteFile(path, []byte(out), 0o644); err != nil {
				return err
			}
			fmt.Printf("Added remote %q -> %s\n", name, ref)
			return nil
		},
	}
	cmd.Flags().StringVar(&typ, "type", "oci", "remote type")
	cmd.Flags().StringVar(&ref, "ref", "", "registry base ref (e.g. ghcr.io/org/dryrun)")
	cmd.Flags().StringVar(&tokenEnv, "token-env", "", "env var holding a bearer token")
	cmd.Flags().BoolVar(&isDefault, "default", false, "mark as the default remote")
	return cmd
}

func remoteRmCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rm <name>",
		Short: "Remove a remote from dryrun.toml",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			path, _, err := loadProjectConfig()
			if err != nil {
				return err
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			out, removed := removeRemoteBlock(string(data), name)
			if !removed {
				return fmt.Errorf("remote %q not found", name)
			}
			if err := os.WriteFile(path, []byte(out), 0o644); err != nil {
				return err
			}
			fmt.Printf("Removed remote %q\n", name)
			return nil
		},
	}
}

func remoteBlock(r config.RemoteConfig) string {
	var b strings.Builder
	b.WriteString("\n[[remote]]\n")
	fmt.Fprintf(&b, "name = %q\n", r.Name)
	fmt.Fprintf(&b, "type = %q\n", r.Type)
	fmt.Fprintf(&b, "ref = %q\n", r.Ref)
	if r.TokenEnv != "" {
		fmt.Fprintf(&b, "token_env = %q\n", r.TokenEnv)
	}
	if r.Default {
		b.WriteString("default = true\n")
	}
	return b.String()
}

// drops the [[remote]] block whose name matches, plus any blank lines before it.
// a block runs from its [[remote]] header to the next table header or EOF.
func removeRemoteBlock(content, name string) (string, bool) {
	lines := strings.Split(content, "\n")
	var out []string
	removed := false
	for i := 0; i < len(lines); {
		if strings.TrimSpace(lines[i]) == "[[remote]]" {
			j := i + 1
			for j < len(lines) && !strings.HasPrefix(strings.TrimSpace(lines[j]), "[") {
				j++
			}
			if blockHasName(lines[i:j], name) {
				for len(out) > 0 && strings.TrimSpace(out[len(out)-1]) == "" {
					out = out[:len(out)-1]
				}
				removed = true
				i = j
				continue
			}
			out = append(out, lines[i:j]...)
			i = j
			continue
		}
		out = append(out, lines[i])
		i++
	}
	return strings.Join(out, "\n"), removed
}

func blockHasName(block []string, name string) bool {
	for _, l := range block {
		k, v, ok := strings.Cut(l, "=")
		if ok && strings.TrimSpace(k) == "name" && strings.Trim(strings.TrimSpace(v), `"'`) == name {
			return true
		}
	}
	return false
}
