package cli

import (
	"errors"

	"github.com/spf13/cobra"
)

func (a *application) completionCommand(root *cobra.Command) *cobra.Command {
	return &cobra.Command{
		Use:                   "completion [bash|zsh|fish|powershell]",
		Short:                 "Generate a shell completion script",
		Args:                  usageArgs(cobra.ExactArgs(1)),
		DisableFlagsInUseLine: true,
		ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
		RunE: func(_ *cobra.Command, args []string) error {
			switch args[0] {
			case "bash":
				return root.GenBashCompletion(a.stdout)
			case "zsh":
				return root.GenZshCompletion(a.stdout)
			case "fish":
				return root.GenFishCompletion(a.stdout, true)
			case "powershell":
				return root.GenPowerShellCompletion(a.stdout)
			default:
				return withExit(ExitUsage, errors.New("unsupported shell "+args[0]))
			}
		},
	}
}
