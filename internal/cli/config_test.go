package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestValidateConfigFileRejectsUnknownKeysAndWrongTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		config string
		want   string
	}{
		{
			name: "unknown-nested-key",
			config: `ingest:
  concurreny: 4
`,
			want: `unknown configuration key "ingest.concurreny"`,
		},
		{
			name: "suggestion",
			config: `ingest:
  concurreny: 4
`,
			want: `did you mean "ingest.concurrency"`,
		},
		{
			name: "wrong-verification-type",
			config: `ingest:
  verify: true
`,
			want: `configuration key "ingest.verify"`,
		},
		{
			name: "wrong-integer",
			config: `http:
  retries: many
`,
			want: "must be an integer",
		},
		{
			name: "duration-without-units",
			config: `http:
  timeout: 30
`,
			want: "must use units",
		},
		{
			name: "wrong-list",
			config: `source:
  http_headers: 'Authorization: secret'
`,
			want: "must be a list of strings",
		},
		{
			name: "wrong-list-element",
			config: `input:
  - fixture.mp4
  - 3
`,
			want: `configuration key "input" element 1`,
		},
		{
			name: "non-mapping-parent",
			config: `ingest: true
`,
			want: `configuration key "ingest"`,
		},
		{
			name: "duplicate-dotted-key",
			config: `ingest:
  concurrency: 2
ingest.concurrency: 3
`,
			want: "duplicates line",
		},
		{
			name: "file-selects-another-file",
			config: `config: other.yaml
`,
			want: "cannot be set inside a file",
		},
		{
			name: "multiple-documents",
			config: `format: json
---
format: human
`,
			want: "exactly one YAML document",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			app := &application{v: viper.New()}
			err := app.validateConfigFile([]byte(testCase.config))
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("error = %v, want it to contain %q", err, testCase.want)
			}
		})
	}
}

func TestValidateConfigFileAcceptsDocumentedTypes(t *testing.T) {
	t.Parallel()
	config := `format: json
http:
  timeout: 30s
  transfer_timeout: 0
  retries: 3
  insecure_skip_verify: false
auth:
  scopes: [tams.write, tams.read]
ingest:
  concurrency: 4
  verify: auto
input:
  - fixture.mp4
`
	app := &application{v: viper.New()}
	if err := app.validateConfigFile([]byte(config)); err != nil {
		t.Fatal(err)
	}
}

func TestValidateConfigFileAcceptsEmptyFile(t *testing.T) {
	t.Parallel()
	app := &application{v: viper.New()}
	for _, body := range []string{"", "# intentionally empty\n"} {
		if err := app.validateConfigFile([]byte(body)); err != nil {
			t.Fatalf("empty configuration %q: %v", body, err)
		}
	}
}

func TestCLIConfigRejectsOversizedFileBeforeParsing(t *testing.T) {
	config := filepath.Join(t.TempDir(), "oversized.yaml")
	if err := os.WriteFile(config, []byte(strings.Repeat("#", maxConfigFileBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{"--config", config, "config", "validate"},
		strings.NewReader(""), &stdout, &stderr)
	if code != ExitUsage || !strings.Contains(stderr.String(), "configuration exceeds 2 MiB") {
		t.Fatalf("exit = %d; stdout = %s; stderr = %s", code, stdout.String(), stderr.String())
	}
}

func TestReadConfigFileRejectsNonRegularTarget(t *testing.T) {
	t.Parallel()
	_, err := readConfigFile(t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("error = %v, want regular-file rejection", err)
	}
}

func TestCLIDefaultConfigBrokenSymlinkIsNotSilentlyIgnored(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("default configuration discovery uses a different environment variable on Windows")
	}
	configHome := t.TempDir()
	directory := filepath.Join(configHome, "tamsin")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(directory, "config.yaml")
	if err := os.Symlink(filepath.Join(directory, "missing.yaml"), config); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", configHome)
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{"config", "validate"}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitUsage || !strings.Contains(stderr.String(), "read configuration") {
		t.Fatalf("exit = %d; stdout = %s; stderr = %s", code, stdout.String(), stderr.String())
	}
}

func TestConfigDefinitionsAreUniqueSortedAndBound(t *testing.T) {
	t.Parallel()
	definitions := configDefinitions()
	seenKeys := make(map[string]struct{}, len(definitions))
	seenEnvironment := make(map[string]string, len(definitions))
	previous := ""
	for _, definition := range definitions {
		if definition.key <= previous {
			t.Fatalf("configuration definitions are not strictly sorted: %q follows %q", definition.key, previous)
		}
		previous = definition.key
		if _, exists := seenKeys[definition.key]; exists {
			t.Fatalf("duplicate configuration key %q", definition.key)
		}
		seenKeys[definition.key] = struct{}{}
		environment := configEnvironmentName(definition.key)
		if other, exists := seenEnvironment[environment]; exists {
			t.Fatalf("configuration keys %q and %q both map to %s", other, definition.key, environment)
		}
		seenEnvironment[environment] = definition.key
	}

	app := &application{v: viper.New(), stdin: strings.NewReader("")}
	root := app.rootCommand()
	for _, definition := range definitions {
		if definition.flag != "" && root.PersistentFlags().Lookup(definition.flag) == nil {
			t.Fatalf("configuration key %q names missing persistent flag --%s", definition.key, definition.flag)
		}
	}
}

func TestValidateEnvironmentValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value string
		kind  configKind
		valid bool
	}{
		{name: "bool", value: "true", kind: configBool, valid: true},
		{name: "bad-bool", value: "yes", kind: configBool},
		{name: "int", value: "4", kind: configInt, valid: true},
		{name: "bad-int", value: "four", kind: configInt},
		{name: "duration", value: "30s", kind: configDuration, valid: true},
		{name: "zero-duration", value: "0", kind: configDuration, valid: true},
		{name: "bad-duration", value: "30", kind: configDuration},
		{name: "string", value: "anything", kind: configString, valid: true},
		{name: "list-transport", value: "one,two", kind: configStrings, valid: true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			err := validateEnvironmentValue("TAMSIN_TEST", testCase.value, testCase.kind)
			if (err == nil) != testCase.valid {
				t.Fatalf("error = %v, valid = %t", err, testCase.valid)
			}
		})
	}
}

func TestClosestConfigKeyBoundsUnhelpfulInputs(t *testing.T) {
	t.Parallel()
	candidates := []string{"ingest.concurrency", "ingest.max_inputs"}
	if got := closestConfigKey("ingest.concurreny", candidates); got != "ingest.concurrency" {
		t.Fatalf("ordinary typo suggestion = %q", got)
	}
	if got := closestConfigKey(strings.Repeat("x", maxConfigFileBytes), candidates); got != "" {
		t.Fatalf("oversized unknown key suggestion = %q, want none", got)
	}
}

func TestCLIConfigStringEnvironmentArraysAreLosslessAndRedacted(t *testing.T) {
	const headerSecret = "review-header-secret"
	const ffmpegSecret = "review-ffmpeg-secret"
	t.Setenv("TAMSIN_INPUT", `["asset one.mp4","asset,two.mp4"]`)
	t.Setenv("TAMSIN_AUTH_SCOPES", `["tams.read","tams.write"]`)
	t.Setenv("TAMSIN_SOURCE_HTTP_HEADERS", `["Authorization: Bearer `+headerSecret+`","X-Label: value with spaces"]`)
	t.Setenv("TAMSIN_MEDIA_FFMPEG_ARGS", `["-headers","Authorization: Bearer `+ffmpegSecret+`"]`)

	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--format", "json", "config", "show", "--effective",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitOK {
		t.Fatalf("exit = %d; stderr = %s", code, stderr.String())
	}
	if strings.Contains(stdout.String()+stderr.String(), headerSecret) ||
		strings.Contains(stdout.String()+stderr.String(), ffmpegSecret) {
		t.Fatalf("effective configuration leaked a list secret: stdout=%s stderr=%s", stdout.String(), stderr.String())
	}

	var result effectiveConfigResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(result.Effective["input"].Value); got != "[asset one.mp4 asset,two.mp4]" {
		t.Fatalf("input = %s", got)
	}
	if got := fmt.Sprint(result.Effective["auth.scopes"].Value); got != "[tams.read tams.write]" {
		t.Fatalf("auth scopes = %s", got)
	}
	if got := fmt.Sprint(result.Effective["media.ffmpeg_args"].Value); got != "[<redacted> <redacted>]" {
		t.Fatalf("FFmpeg argument redaction = %s", got)
	}
}

func TestCLIConfigStringEnvironmentRejectsMalformedJSONArray(t *testing.T) {
	t.Setenv("TAMSIN_INPUT", `["one", 2]`)
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{"config", "validate"}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitUsage || !strings.Contains(stderr.String(), "must be a JSON array of strings") {
		t.Fatalf("exit = %d; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
}

func TestDecodeEnvironmentStringsPreservesLegacyAndLosslessForms(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name  string
		value string
		want  string
	}{
		{name: "legacy whitespace", value: "one two", want: "[one two]"},
		{name: "lossless JSON", value: `["one two","three,four"]`, want: "[one two three,four]"},
		{name: "empty override", value: `[]`, want: "[]"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			values, err := decodeEnvironmentStrings("TAMSIN_TEST", testCase.value)
			if err != nil || fmt.Sprint(values) != testCase.want {
				t.Fatalf("values/error = %v/%v, want %s", values, err, testCase.want)
			}
		})
	}
}

func TestCLIConfigShowEffectiveRedactsSecretsAndShowsProvenance(t *testing.T) {
	directory := t.TempDir()
	config := writeTestConfig(t, directory, `
endpoint: https://tams.example.test/v8.1?access_token=file-url-token
format: human
auth:
  password: file-password
  token: file-token
  client_secret: file-client-secret
  code: file-oauth-code
  pkce_verifier: file-pkce-verifier
  url_token: file-explicit-url-token
  token_url: https://identity.example.test/token?client_assertion=file-assertion
ingest:
  concurrency: 4
source:
  http_headers:
    - 'Authorization: file-header-token'
input:
  - https://media.example.test/asset.mp4?signature=file-signature
`)
	t.Setenv("TAMSIN_AUTH_PASSWORD", "environment-password")

	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--config", config,
		"--format", "json",
		"--log-level", "warn",
		"--client-secret", "flag-client-secret",
		"config", "show", "--effective",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitOK {
		t.Fatalf("exit = %d; stderr = %s", code, stderr.String())
	}
	for _, secret := range []string{
		"file-password", "file-url-token", "file-token", "file-assertion",
		"file-client-secret", "file-oauth-code", "file-pkce-verifier",
		"file-explicit-url-token", "flag-client-secret", "file-header-token",
		"file-signature", "environment-password",
	} {
		if strings.Contains(stdout.String(), secret) || strings.Contains(stderr.String(), secret) {
			t.Fatalf("configuration output leaked %q: stdout=%s stderr=%s", secret, stdout.String(), stderr.String())
		}
	}

	var result effectiveConfigResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	assertEffectiveSource(t, result, "auth.password", "<redacted>", "env", "TAMSIN_AUTH_PASSWORD")
	assertEffectiveSource(t, result, "auth.token", "<redacted>", "file", config)
	assertEffectiveSource(t, result, "auth.client_secret", "<redacted>", "flag", "--client-secret")
	assertEffectiveSource(t, result, "format", "json", "flag", "--format")
	assertEffectiveSource(t, result, "log.level", "warn", "flag", "--log-level")
	assertEffectiveSource(t, result, "ingest.concurrency", float64(4), "file", config)
	assertEffectiveSource(t, result, "ingest.transfers", float64(4), "default", "")
	if result.Effective["ingest.transfers"].ResolvedFrom != "ingest.concurrency" {
		t.Fatalf("transfer derivation = %#v", result.Effective["ingest.transfers"])
	}
	assertEffectiveSource(t, result, "progress", "auto", "default", "")
	assertEffectiveSource(t, result, "config", config, "flag", "--config")

	endpoint, ok := result.Effective["endpoint"].Value.(string)
	if !ok || strings.Contains(endpoint, "file-password") || !strings.Contains(endpoint, "REDACTED") {
		t.Fatalf("redacted endpoint = %#v", result.Effective["endpoint"].Value)
	}
}

func TestCLIConfigShowEffectiveResolvesMediaProfiles(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		config         string
		profile        string
		duration       string
		format         string
		storage        string
		profileDerived bool
	}{
		{
			name: "preserve",
			config: `ingest:
  profile: preserve
  max_inputs: 24
  staging_byte_budget: 12GiB
`,
			profile:        "preserve",
			duration:       "0s",
			format:         "source",
			storage:        "muxed",
			profileDerived: true,
		},
		{
			name: "explicit-override-is-custom",
			config: `ingest:
  profile: preserve
  segment_duration: 3s
`,
			profile:  "custom",
			duration: "3s",
			format:   "source",
			storage:  "muxed",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			directory := t.TempDir()
			config := writeTestConfig(t, directory, testCase.config)
			var stdout, stderr bytes.Buffer
			code := Execute(context.Background(), []string{
				"--config", config, "--format", "json", "config", "show", "--effective",
			}, strings.NewReader(""), &stdout, &stderr)
			if code != ExitOK {
				t.Fatalf("exit = %d; stderr = %s", code, stderr.String())
			}
			var result effectiveConfigResult
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if got := result.Effective["ingest.profile"].Value; got != testCase.profile {
				t.Fatalf("profile = %#v, want %q", got, testCase.profile)
			}
			for key, want := range map[string]string{
				"ingest.segment_duration": testCase.duration,
				"ingest.segment_format":   testCase.format,
				"ingest.essence_storage":  testCase.storage,
			} {
				setting := result.Effective[key]
				if setting.Value != want {
					t.Errorf("%s = %#v, want %q", key, setting.Value, want)
				}
				if testCase.profileDerived && setting.ResolvedFrom != "ingest.profile" {
					t.Errorf("%s derivation = %#v, want ingest.profile", key, setting)
				}
			}
			if testCase.profile == "custom" && result.Effective["ingest.profile"].ResolvedFrom != "ingest.profile + explicit media overrides" {
				t.Fatalf("custom profile provenance = %#v", result.Effective["ingest.profile"])
			}
			if testCase.name == "preserve" {
				assertEffectiveSource(t, result, "ingest.max_inputs", float64(24), "file", config)
				assertEffectiveSource(t, result, "ingest.staging_byte_budget", "12GiB", "file", config)
			}
		})
	}
}

func TestCLIConfigValidateChecksAuthWithoutNetwork(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		code int
		want string
	}{
		{
			name: "incomplete-basic",
			args: []string{"--auth", "basic", "--username", "operator", "config", "validate"},
			code: ExitUsage,
			want: "basic authentication requires username and password",
		},
		{
			name: "incomplete-bearer",
			args: []string{"--auth", "bearer", "config", "validate"},
			code: ExitUsage,
			want: "bearer authentication requires a token",
		},
		{
			name: "incomplete-oauth-client",
			args: []string{"--auth", "oauth-client", "--client-id", "client", "config", "validate"},
			code: ExitUsage,
			want: "OAuth client credentials require",
		},
		{
			name: "URL-token-from-endpoint",
			args: []string{"--auth", "url-token", "--endpoint", "https://tams.example.test/v8.1?access_token=secret", "config", "validate"},
			code: ExitOK,
			want: `"status": "valid"`,
		},
		{
			name: "invalid-endpoint",
			args: []string{"--endpoint", "tams.example.test/v8.1", "config", "validate"},
			code: ExitUsage,
			want: "valid absolute HTTP(S) URL",
		},
		{
			name: "invalid-token-URL",
			args: []string{"--token-url", "://broken", "config", "validate"},
			code: ExitUsage,
			want: "OAuth token URL must be a valid",
		},
		{
			name: "authenticated-remote-HTTP",
			args: []string{"--endpoint", "http://service.example.test/v8.1", "--auth", "bearer", "--token", "secret", "config", "validate"},
			code: ExitUsage,
			want: "must use HTTPS",
		},
		{
			name: "explicit-loopback-development-exception",
			args: []string{"--allow-insecure-auth-loopback", "--endpoint", "http://127.0.0.1:8000/v8.1", "--auth", "bearer", "--token", "secret", "config", "validate"},
			code: ExitOK,
			want: `"status": "valid"`,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			code := Execute(context.Background(), testCase.args, strings.NewReader(""), &stdout, &stderr)
			if code != testCase.code {
				t.Fatalf("exit = %d, want %d; stdout=%s stderr=%s", code, testCase.code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stdout.String()+stderr.String(), testCase.want) {
				t.Fatalf("output does not contain %q; stdout=%s stderr=%s", testCase.want, stdout.String(), stderr.String())
			}
		})
	}
}

func TestCLIConfigValidateRejectsUnusableTreatment(t *testing.T) {
	config := writeTestConfig(t, t.TempDir(), `
ingest:
  profile: preserve
media:
  ffmpeg_args:
    - -c:v
`)
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{"--config", config, "config", "validate"}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitUsage || !strings.Contains(stderr.String(), "--ffmpeg-arg cannot take effect without segmentation") {
		t.Fatalf("exit = %d; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
}

func assertEffectiveSource(t *testing.T, result effectiveConfigResult, key string, value any, source, detail string) {
	t.Helper()
	setting, exists := result.Effective[key]
	if !exists {
		t.Fatalf("effective configuration has no %q", key)
	}
	if fmt.Sprint(setting.Value) != fmt.Sprint(value) || setting.Source != source || setting.SourceDetail != detail {
		t.Fatalf("%s = %#v, want value=%#v source=%q detail=%q", key, setting, value, source, detail)
	}
}

func writeTestConfig(t *testing.T, directory, body string) string {
	t.Helper()
	filename := filepath.Join(directory, "config.yaml")
	if err := os.WriteFile(filename, []byte(strings.TrimSpace(body)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return filename
}
