package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/ingest"
)

func TestStrictYAMLConfiguration(t *testing.T) {
	t.Parallel()
	app := &application{v: newSettings()}
	valid := []byte(`endpoint: https://tams.example.test
ingest:
  profile: essence-segments
  concurrency: 3
  segment_duration: 12s
media:
  ffmpeg_args:
    - -copyts
`)
	values, err := decodeConfigFile(valid)
	if err != nil {
		t.Fatal(err)
	}
	app.v.file = values
	if app.v.GetString("endpoint") != "https://tams.example.test" ||
		app.v.GetInt("ingest.concurrency") != 3 ||
		app.v.GetDuration("ingest.segment_duration") != 12*time.Second ||
		len(app.v.GetStringSlice("media.ffmpeg_args")) != 1 {
		t.Fatalf("decoded values = %#v", values)
	}

	for _, invalid := range []string{
		"ingest:\n  concurrncy: 2\n",
		"ingest:\n  concurrency: two\n",
		"ingest:\n  profile: preserve\n  profile: demux\n",
		"ingest:\n  segment_duration: 10\n",
		"config: other.yaml\n",
		"quiet: true\n---\nquiet: false\n",
		"ingest:\n  profile: preserve\ningest.profile: demux\n",
		"QUIET: true\nquiet: false\n",
		"quiet: null\n",
		"media:\n  ffmpeg_args: [1]\n",
		"ingest: &cycle\n  ingest: *cycle\n",
		"ingest:\n  <<: {profile: preserve}\n",
	} {
		if _, err := decodeConfigFile([]byte(invalid)); err == nil {
			t.Errorf("invalid configuration succeeded: %q", invalid)
		}
	}
}

func TestYAMLConfigurationAliasesAndCanonicalKeys(t *testing.T) {
	t.Parallel()
	values, err := decodeConfigFile([]byte("Input: &inputs [one, two]\nauth.scopes: *inputs\nINGEST:\n  segment_duration: 0\nquiet: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	settings := newSettings()
	settings.file = values
	if got := settings.GetStringSlice("auth.scopes"); len(got) != 2 || got[1] != "two" {
		t.Fatalf("aliased strings = %#v", got)
	}
	if settings.GetDuration("ingest.segment_duration") != 0 || !settings.GetBool("quiet") {
		t.Fatalf("decoded values = %#v", values)
	}
	for _, empty := range []string{"", "# comment\n", "{}"} {
		values, err := decodeConfigFile([]byte(empty))
		if err != nil || len(values) != 0 {
			t.Fatalf("empty configuration %q: %v, %v", empty, values, err)
		}
	}
}

func TestConfigurationPrecedence(t *testing.T) {
	app := &application{v: newSettings()}
	command := app.rootCommand()
	app.v.file = map[string]any{"endpoint": "https://file.example", "http.retries": 2}
	t.Setenv("TAMSIN_ENDPOINT", "https://env.example")
	if got := app.v.GetString("endpoint"); got != "https://env.example" {
		t.Fatalf("environment endpoint = %q", got)
	}
	if err := command.PersistentFlags().Set("endpoint", "https://flag.example"); err != nil {
		t.Fatal(err)
	}
	if got := app.v.GetString("endpoint"); got != "https://flag.example" {
		t.Fatalf("flag endpoint = %q", got)
	}
	if got := app.v.GetInt("http.retries"); got != 2 {
		t.Fatalf("file retries = %d", got)
	}
}

func TestConfigurationEnvironmentIsStrict(t *testing.T) {
	app := &application{v: newSettings()}
	t.Setenv("TAMSIN_HTTP_RETRIES", "4")
	t.Setenv("TAMSIN_AUTH_SCOPES", `["read media","write"]`)
	if err := app.validateConfigEnvironment(); err != nil {
		t.Fatal(err)
	}
	if app.v.GetInt("http.retries") != 4 {
		t.Fatalf("retries = %d", app.v.GetInt("http.retries"))
	}
	if got := app.v.GetStringSlice("auth.scopes"); len(got) != 2 || got[0] != "read media" {
		t.Fatalf("scopes = %#v", got)
	}

	t.Setenv("TAMSIN_HTTP_RETRIES", "many")
	if err := app.validateConfigEnvironment(); err == nil || !strings.Contains(err.Error(), "integer") {
		t.Fatalf("invalid integer error = %v", err)
	}
	t.Setenv("TAMSIN_HTTP_RETRIES", "")
	t.Setenv("TAMSIN_UNKNOWN_SETTING", "")
	if err := app.validateConfigEnvironment(); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unknown variable error = %v", err)
	}
}

func TestLoadConfigUsesExplicitFileAndChecksPermissions(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("Unix permission check")
	}
	directory := t.TempDir()
	filename := filepath.Join(directory, "config.yaml")
	if err := os.WriteFile(filename, []byte("auth:\n  token: secret\ningest:\n  profile: preserve\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	app := &application{v: newSettings()}
	command := app.rootCommand()
	if err := command.PersistentFlags().Set("config", filename); err != nil {
		t.Fatal(err)
	}
	if err := app.loadConfig(); err != nil {
		t.Fatal(err)
	}
	if app.configFile != filename || app.v.GetString("auth.token") != "secret" || app.configFileWarning == "" {
		t.Fatalf("loaded file=%q token=%q warning=%q", app.configFile, app.v.GetString("auth.token"), app.configFileWarning)
	}
}

func TestConfigPermissionWarningExaminesTheFileRatherThanOverrides(t *testing.T) {
	t.Parallel()
	app := &application{v: newSettings()}
	app.v.file = map[string]any{"auth.token": ""}
	command := app.rootCommand()
	if err := command.PersistentFlags().Set("token", "flag-secret"); err != nil {
		t.Fatal(err)
	}
	if app.configFileContainsSecrets() {
		t.Fatal("empty file secret was inferred from a higher-precedence value")
	}
	app.v.file["auth.token"] = "file-secret"
	if err := command.PersistentFlags().Set("token", ""); err != nil {
		t.Fatal(err)
	}
	if !app.configFileContainsSecrets() {
		t.Fatal("file secret was hidden by a higher-precedence empty value")
	}
}

func TestProfileConfigurationKeepsFFmpegPassThrough(t *testing.T) {
	app := &application{v: newSettings()}
	app.v.file = map[string]any{
		"ingest.profile":          ingest.ProfileEssenceSegments,
		"media.ffmpeg_args":       []string{"-copyts"},
		"ingest.segment_format":   "source",
		"ingest.essence_storage":  "independent",
		"ingest.segment_duration": 10 * time.Second,
	}
	profile, err := app.resolvedConfigProfile(nil)
	if err != nil {
		t.Fatal(err)
	}
	if profile.Name != ingest.ProfileCustom || profile.Version != ingest.CustomProfileVersion {
		t.Fatalf("resolved profile = %s@%s", profile.Name, profile.Version)
	}
}
