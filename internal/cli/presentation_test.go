package cli

import (
	"bytes"
	"testing"

	"github.com/spf13/viper"
)

func TestOutputWidthUsesBoundedColumnsFallback(t *testing.T) {
	var output bytes.Buffer
	for _, testCase := range []struct {
		columns string
		want    int
	}{
		{columns: "42", want: 42},
		{columns: "19", want: fallbackOutputWidth},
		{columns: "1001", want: fallbackOutputWidth},
		{columns: "not-a-number", want: fallbackOutputWidth},
	} {
		t.Run(testCase.columns, func(t *testing.T) {
			t.Setenv("COLUMNS", testCase.columns)
			if got := outputWidth(&output); got != testCase.want {
				t.Fatalf("output width = %d, want %d", got, testCase.want)
			}
		})
	}
}

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
			settings := viper.New()
			settings.Set("color", testCase.mode)
			app := &application{v: settings, stdout: &bytes.Buffer{}}
			if got := app.humanColorEnabled(); got != testCase.want {
				t.Fatalf("color enabled = %t, want %t", got, testCase.want)
			}
		})
	}
}
