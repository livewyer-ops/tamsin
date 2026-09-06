package cli

import (
	"bytes"
	"testing"
)

func TestHumanColorPolicyIsExplicitAndAccessible(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		mode    string
		noColor bool
		term    string
		want    bool
	}{
		{name: "forced", mode: "always", noColor: true, term: "dumb", want: true},
		{name: "disabled", mode: "never"},
		{name: "no-color", mode: "auto", noColor: true},
		{name: "dumb", mode: "auto", term: "dumb"},
		{name: "redirected", mode: "auto", term: "xterm-256color"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv("TERM", testCase.term)
			if testCase.noColor {
				t.Setenv("NO_COLOR", "1")
			}
			settings := newSettings()
			t.Setenv("TAMSIN_COLOR", testCase.mode)
			app := &application{v: settings, stdout: &bytes.Buffer{}}
			if got := app.humanColorEnabled(); got != testCase.want {
				t.Fatalf("color enabled = %t, want %t", got, testCase.want)
			}
		})
	}
}
