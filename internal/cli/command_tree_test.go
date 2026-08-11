package cli

import (
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// The command tree is assembled by hand across root.go, api.go and config.go, so
// a subcommand can be dropped from its parent without any other test noticing:
// the constructor still exists and still compiles. This pins the whole reachable
// tree, which is the part users actually address.
func TestCommandTreeExposesEveryAddressableCommand(t *testing.T) {
	t.Parallel()

	want := map[string][]string{
		"tamsin":                     {"api", "completion", "config", "doctor", "ingest", "profiles"},
		"tamsin api":                 {"flow", "object", "request", "segment", "service", "storage", "storage-backends"},
		"tamsin api flow":            {"get", "put"},
		"tamsin api storage":         {"allocate"},
		"tamsin api segment":         {"delete", "list", "register"},
		"tamsin api object":          {"get", "instance"},
		"tamsin api object instance": {"delete", "register"},
		"tamsin config":              {"show", "validate"},
	}

	app := &application{v: viper.New()}
	got := map[string][]string{}
	collectCommandTree(app.rootCommand(), got)

	for path, wantChildren := range want {
		gotChildren, ok := got[path]
		if !ok {
			t.Errorf("command %q is not reachable from the root command", path)
			continue
		}
		if strings.Join(gotChildren, " ") != strings.Join(wantChildren, " ") {
			t.Errorf("%q subcommands want %v, got %v", path, wantChildren, gotChildren)
		}
	}
	for path := range got {
		if _, ok := want[path]; !ok {
			t.Errorf("command %q has subcommands but is not pinned by this test", path)
		}
	}
}

// collectCommandTree records the sorted subcommand names of every command that
// has any, keyed by its full invocation path. Cobra's generated help and
// completion commands are excluded: they are framework-owned, not ours.
func collectCommandTree(command *cobra.Command, into map[string][]string) {
	var names []string
	for _, child := range command.Commands() {
		if child.Name() == "help" {
			continue
		}
		names = append(names, child.Name())
		collectCommandTree(child, into)
	}
	if len(names) == 0 {
		return
	}
	sort.Strings(names)
	into[command.CommandPath()] = names
}
