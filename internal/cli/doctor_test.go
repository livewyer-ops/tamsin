package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/ingest"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

const (
	doctorDefaultStorage   = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	doctorRequestedStorage = "6ba7b811-9dad-11d1-80b4-00c04fd430c8"
)

func fakeDoctorTool(t *testing.T, name, version string) string {
	t.Helper()
	filename := filepath.Join(t.TempDir(), name)
	body := "#!/bin/sh\nprintf '%s\\n' '" + version + "'\n"
	if err := os.WriteFile(filename, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	return filename
}

func doctorArgs(t *testing.T, format string) []string {
	t.Helper()
	return []string{
		"--format", format,
		"--ffprobe", fakeDoctorTool(t, "ffprobe", "ffprobe version doctor-test"),
		"--ffmpeg", fakeDoctorTool(t, "ffmpeg", "ffmpeg version doctor-test"),
		"--retries", "0",
		"doctor", "--temp-dir", t.TempDir(),
	}
}

func executeDoctor(t *testing.T, arguments []string) (int, doctorResult, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), arguments, strings.NewReader(""), &stdout, &stderr)
	var result doctorResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode doctor output %q: %v", stdout.String(), err)
	}
	return code, result, stdout.String(), stderr.String()
}

func checkNamed(t *testing.T, result doctorResult, name string) doctorCheck {
	t.Helper()
	for _, check := range result.Checks {
		if check.Name == name {
			return check
		}
	}
	t.Fatalf("doctor report has no %q check: %#v", name, result.Checks)
	return doctorCheck{}
}

func assertDoctorCheckOrder(t *testing.T, result doctorResult) {
	t.Helper()
	want := []string{
		"configuration", "profile", "staging", "ffprobe", "ffmpeg", "authentication",
		"service", "api_compatibility", "service_lifetimes", "storage_backends", "storage_selection",
	}
	if len(result.Checks) != len(want) {
		t.Fatalf("check inventory = %d, want %d: %#v", len(result.Checks), len(want), result.Checks)
	}
	for index, name := range want {
		if result.Checks[index].Name != name {
			t.Fatalf("check %d = %q, want %q: %#v", index, result.Checks[index].Name, name, result.Checks)
		}
	}
}

func validDoctorService(apiVersion string) map[string]any {
	return map[string]any{
		"api_version": apiVersion, "min_object_timeout": "300:0", "min_presigned_url_timeout": "30:0",
	}
}

func doctorServer(t *testing.T, service map[string]any, backends []tams.StorageBackend, tls bool) (*httptest.Server, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	requests := &atomic.Int32{}
	mutations := &atomic.Int32{}
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.Method != http.MethodGet {
			mutations.Add(1)
			http.Error(writer, "mutation forbidden", http.StatusMethodNotAllowed)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/service":
			_ = json.NewEncoder(writer).Encode(service)
		case "/service/storage-backends":
			_ = json.NewEncoder(writer).Encode(backends)
		default:
			mutations.Add(1)
			http.NotFound(writer, request)
		}
	})
	if tls {
		return httptest.NewTLSServer(handler), requests, mutations
	}
	return httptest.NewServer(handler), requests, mutations
}

func onlineDoctorArgs(t *testing.T, endpoint string) []string {
	t.Helper()
	arguments := doctorArgs(t, "json")
	arguments = append([]string{"--endpoint", endpoint, "--auth", "none"}, arguments...)
	return append(arguments, "--online")
}

func TestDoctorReportJSONAndHumanAreCheckOriented(t *testing.T) {
	t.Parallel()
	jsonArgs := append(doctorArgs(t, "json"), "--profile", "preserve")
	code, result, stdout, stderr := executeDoctor(t, jsonArgs)
	if code != ExitOK {
		t.Fatalf("exit = %d; stdout = %s; stderr = %s", code, stdout, stderr)
	}
	if result.SchemaVersion != DoctorReportSchemaVersion || result.Status != doctorPass ||
		result.Tamsin == "" || result.ToolVersion == "" || result.ToolCommit == "" ||
		result.Go == "" || result.OS == "" || result.Arch == "" || result.Profile.Name != ingest.ProfilePreserve {
		t.Fatalf("incomplete doctor provenance: %#v", result)
	}
	assertDoctorCheckOrder(t, result)
	if check := checkNamed(t, result, "ffmpeg"); check.Status != doctorSkip {
		t.Fatalf("preserve ffmpeg check = %#v, want skipped", check)
	}
	if check := checkNamed(t, result, "authentication"); check.Status != doctorSkip {
		t.Fatalf("offline authentication check = %#v, want skipped", check)
	}

	textArgs := append(doctorArgs(t, "human"), "--profile", "preserve")
	var textOut, textErr bytes.Buffer
	code = Execute(context.Background(), textArgs, strings.NewReader(""), &textOut, &textErr)
	if code != ExitOK {
		t.Fatalf("text exit = %d; stdout = %s; stderr = %s", code, textOut.String(), textErr.String())
	}
	for _, want := range []string{"doctor\tstatus=pass", "PASS\tconfiguration", "PASS\tprofile", "SKIPPED\tffmpeg", "SKIPPED\tauthentication"} {
		if !strings.Contains(textOut.String(), want) {
			t.Fatalf("text report %q does not contain %q", textOut.String(), want)
		}
	}
	if strings.HasPrefix(strings.TrimSpace(textOut.String()), "{") {
		t.Fatalf("text report is JSON: %s", textOut.String())
	}
}

func TestDoctorChecksFFmpegOnlyForMediaWritingTreatment(t *testing.T) {
	t.Parallel()
	missing := filepath.Join(t.TempDir(), "missing-ffmpeg")
	for _, testCase := range []struct {
		name       string
		profile    string
		wantCode   int
		wantStatus string
	}{
		{name: "preserve whole-file", profile: "preserve", wantCode: ExitOK, wantStatus: doctorSkip},
		{name: "demux may separate a multiplex", profile: "demux", wantCode: ExitMedia, wantStatus: doctorFail},
		{name: "essence segments write media", profile: "essence-segments", wantCode: ExitMedia, wantStatus: doctorFail},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			arguments := []string{
				"--format", "json", "--ffprobe", fakeDoctorTool(t, "ffprobe", "ffprobe version test"),
				"--ffmpeg", missing, "doctor", "--temp-dir", t.TempDir(), "--profile", testCase.profile,
			}
			code, result, stdout, stderr := executeDoctor(t, arguments)
			if code != testCase.wantCode {
				t.Fatalf("exit = %d, want %d; stdout = %s; stderr = %s", code, testCase.wantCode, stdout, stderr)
			}
			if check := checkNamed(t, result, "ffmpeg"); check.Status != testCase.wantStatus {
				t.Fatalf("ffmpeg check = %#v, want %s", check, testCase.wantStatus)
			}
		})
	}
}

func TestDoctorMediaToolVersionCheckHasLocalDeadline(t *testing.T) {
	t.Parallel()
	_, err := doctorToolVersion(context.Background(), func(ctx context.Context) (string, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			return "", errors.New("version check has no deadline")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 || remaining > doctorToolVersionTimeout {
			return "", fmt.Errorf("version deadline remaining = %s", remaining)
		}
		return "tool version test", nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDoctorDoesNotReportMediaToolControlledText(t *testing.T) {
	t.Parallel()

	t.Run("successful banners are omitted", func(t *testing.T) {
		const probeSecret = "probe-banner-credential"
		const ffmpegSecret = "ffmpeg-banner-credential"
		arguments := []string{
			"--format", "json",
			"--ffprobe", fakeDoctorTool(t, "ffprobe", "ffprobe version "+probeSecret),
			"--ffmpeg", fakeDoctorTool(t, "ffmpeg", "ffmpeg version "+ffmpegSecret),
			"doctor", "--temp-dir", t.TempDir(), "--profile", "essence-segments",
		}
		code, result, stdout, stderr := executeDoctor(t, arguments)
		if code != ExitOK || result.Status != doctorPass {
			t.Fatalf("exit/report = %d/%#v; stdout = %s; stderr = %s", code, result, stdout, stderr)
		}
		for _, secret := range []string{probeSecret, ffmpegSecret} {
			if strings.Contains(stdout, secret) || strings.Contains(stderr, secret) {
				t.Fatalf("doctor leaked successful tool output %q; stdout = %s; stderr = %s", secret, stdout, stderr)
			}
		}
	})

	for _, testCase := range []struct {
		name    string
		profile string
		flag    string
		check   string
	}{
		{name: "ffprobe failure", profile: "preserve", flag: "--ffprobe", check: "ffprobe"},
		{name: "ffmpeg failure", profile: "essence-segments", flag: "--ffmpeg", check: "ffmpeg"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			const pathSecret = "tool-path-credential"
			const stderrSecret = "tool-stderr-credential"
			toxicTool := filepath.Join(t.TempDir(), testCase.check+"-"+pathSecret)
			if err := os.WriteFile(toxicTool, []byte("#!/bin/sh\nprintf '%s\\n' '"+stderrSecret+"' >&2\nexit 42\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			arguments := []string{
				"--format", "json",
				"--ffprobe", fakeDoctorTool(t, "ffprobe", "ffprobe version safe"),
				"--ffmpeg", fakeDoctorTool(t, "ffmpeg", "ffmpeg version safe"),
				testCase.flag, toxicTool,
				"doctor", "--temp-dir", t.TempDir(), "--profile", testCase.profile,
			}
			code, result, stdout, stderr := executeDoctor(t, arguments)
			if code != ExitMedia || checkNamed(t, result, testCase.check).Status != doctorFail {
				t.Fatalf("exit/check = %d/%#v; stdout = %s; stderr = %s",
					code, checkNamed(t, result, testCase.check), stdout, stderr)
			}
			for _, secret := range []string{pathSecret, stderrSecret} {
				if strings.Contains(stdout, secret) || strings.Contains(stderr, secret) {
					t.Fatalf("doctor leaked tool-controlled text %q; stdout = %s; stderr = %s", secret, stdout, stderr)
				}
			}
		})
	}
}

func TestDoctorReportsStagingWritabilityAndSpace(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission-bit fixture")
	}
	t.Run("unwritable", func(t *testing.T) {
		directory := t.TempDir()
		if err := os.Chmod(directory, 0o500); err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(directory, 0o700) //nolint:errcheck -- best-effort test cleanup
		arguments := doctorArgs(t, "json")
		arguments = append(arguments, "--profile", "preserve", "--temp-dir", directory)
		code, result, stdout, stderr := executeDoctor(t, arguments)
		if code != ExitGeneral || checkNamed(t, result, "staging").Status != doctorFail {
			t.Fatalf("exit/check = %d/%#v; stdout = %s; stderr = %s", code, checkNamed(t, result, "staging"), stdout, stderr)
		}
		if !strings.Contains(checkNamed(t, result, "staging").Error, "not writable") {
			t.Fatalf("staging error = %q", checkNamed(t, result, "staging").Error)
		}
	})

	t.Run("configured budget exceeds free space", func(t *testing.T) {
		directory := t.TempDir()
		inspection, err := ingest.InspectStaging(directory, 0)
		if err != nil {
			t.Fatal(err)
		}
		budget := strconv.FormatInt(inspection.FilesystemFreeBytes+1, 10)
		arguments := doctorArgs(t, "json")
		arguments = append(arguments, "--profile", "preserve", "--temp-dir", directory, "--staging-byte-budget", budget)
		code, result, stdout, stderr := executeDoctor(t, arguments)
		check := checkNamed(t, result, "staging")
		if code != ExitGeneral || check.Status != doctorFail || !strings.Contains(check.Error, "exceeds") {
			t.Fatalf("exit/check = %d/%#v; stdout = %s; stderr = %s", code, check, stdout, stderr)
		}
	})
}

func TestDoctorOnlineChecksAPIVersionWithoutMutation(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		version      string
		wantCode     int
		wantStatus   string
		relationship string
	}{
		{version: "8.0", wantCode: ExitRemote, wantStatus: doctorFail, relationship: "incompatible"},
		{version: "8.7", wantCode: ExitOK, wantStatus: doctorPass, relationship: "newer"},
		{version: "9.0", wantCode: ExitRemote, wantStatus: doctorFail, relationship: "incompatible"},
	} {
		t.Run(testCase.version, func(t *testing.T) {
			server, requests, mutations := doctorServer(t, validDoctorService(testCase.version), []tams.StorageBackend{{
				ID: doctorDefaultStorage, DefaultStorage: true,
			}}, false)
			defer server.Close()
			code, result, stdout, stderr := executeDoctor(t, onlineDoctorArgs(t, server.URL))
			check := checkNamed(t, result, "api_compatibility")
			if code != testCase.wantCode || check.Status != testCase.wantStatus || check.Detail["relationship"] != testCase.relationship {
				t.Fatalf("exit/check = %d/%#v; stdout = %s; stderr = %s", code, check, stdout, stderr)
			}
			if requests.Load() != 2 || mutations.Load() != 0 {
				t.Fatalf("online doctor made requests/mutations = %d/%d, want two read-only requests", requests.Load(), mutations.Load())
			}
		})
	}
}

func TestDoctorOnlineRejectsMalformedServiceLifetimesWithoutEchoingThem(t *testing.T) {
	t.Parallel()
	service := validDoctorService("8.1")
	service["min_object_timeout"] = "server-response-secret"
	server, _, mutations := doctorServer(t, service, []tams.StorageBackend{{ID: doctorDefaultStorage, DefaultStorage: true}}, false)
	defer server.Close()
	code, result, stdout, stderr := executeDoctor(t, onlineDoctorArgs(t, server.URL))
	check := checkNamed(t, result, "service_lifetimes")
	if code != ExitRemote || check.Status != doctorFail || !strings.Contains(check.Error, "/min_object_timeout") {
		t.Fatalf("exit/check = %d/%#v; stdout = %s; stderr = %s", code, check, stdout, stderr)
	}
	if strings.Contains(stdout, "server-response-secret") || strings.Contains(stderr, "server-response-secret") {
		t.Fatalf("doctor leaked malformed response value; stdout = %s; stderr = %s", stdout, stderr)
	}
	if mutations.Load() != 0 {
		t.Fatalf("malformed service response caused %d mutation(s)", mutations.Load())
	}
}

func TestDoctorInvalidConfigSkipsDependentChecksAndMakesNoRequests(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	config := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(config, []byte("unknown_doctor_key: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	arguments := doctorArgs(t, "json")
	arguments = append([]string{
		"--config", config, "--endpoint", server.URL, "--auth", "none",
	}, arguments...)
	arguments = append(arguments, "--online")
	code, result, stdout, stderr := executeDoctor(t, arguments)
	if code != ExitUsage || result.Status != doctorFail {
		t.Fatalf("exit/report = %d/%#v; stdout = %s; stderr = %s", code, result, stdout, stderr)
	}
	assertDoctorCheckOrder(t, result)
	if checkNamed(t, result, "configuration").Status != doctorFail {
		t.Fatalf("configuration check = %#v", checkNamed(t, result, "configuration"))
	}
	for _, check := range result.Checks[1:] {
		if check.Status != doctorSkip {
			t.Fatalf("config-dependent check ran after invalid config: %#v", check)
		}
	}
	if requests.Load() != 0 {
		t.Fatalf("invalid configuration caused %d TAMS request(s)", requests.Load())
	}
}

func TestDoctorFlagOverridesInvalidLowerPrecedenceTreatment(t *testing.T) {
	config := writeTestConfig(t, t.TempDir(), `
ingest:
  profile: not-a-profile
`)
	arguments := append(doctorArgs(t, "json"), "--config", config, "--profile", "preserve")
	code, result, _, stderr := executeDoctor(t, arguments)
	if code != ExitOK || result.Status != doctorPass {
		t.Fatalf("exit = %d; result=%#v stderr=%s", code, result, stderr)
	}
	if checkNamed(t, result, "configuration").Status != doctorPass || checkNamed(t, result, "profile").Status != doctorPass {
		t.Fatalf("flag did not repair lower-precedence profile: %#v", result.Checks)
	}
}

func TestDoctorReportsTheEffectiveErrorAfterAFlagRepairsLowerPrecedenceConfig(t *testing.T) {
	config := writeTestConfig(t, t.TempDir(), `
ingest:
  profile: not-a-profile
  storage_id: not-a-uuid
`)
	arguments := append(doctorArgs(t, "json"), "--config", config, "--profile", "preserve")
	code, result, stdout, stderr := executeDoctor(t, arguments)
	check := checkNamed(t, result, "configuration")
	if code != ExitUsage || check.Status != doctorFail ||
		!strings.Contains(check.Error, "storage ID must be a UUID") ||
		strings.Contains(check.Error, "not-a-profile") {
		t.Fatalf("exit/configuration = %d/%#v; stdout = %s; stderr = %s", code, check, stdout, stderr)
	}
}

func TestDoctorInvalidStorageFlagSkipsOnlineChecksAndMakesNoRequests(t *testing.T) {
	t.Parallel()
	server, requests, _ := doctorServer(t, validDoctorService("8.1"), []tams.StorageBackend{{
		ID: doctorDefaultStorage, DefaultStorage: true,
	}}, false)
	defer server.Close()

	arguments := append(onlineDoctorArgs(t, server.URL), "--storage-id", "not-a-uuid")
	code, result, stdout, stderr := executeDoctor(t, arguments)
	if code != ExitUsage || checkNamed(t, result, "configuration").Status != doctorFail {
		t.Fatalf("exit/configuration = %d/%#v; stdout = %s; stderr = %s",
			code, checkNamed(t, result, "configuration"), stdout, stderr)
	}
	for _, name := range []string{"authentication", "service", "api_compatibility", "service_lifetimes", "storage_backends", "storage_selection"} {
		if check := checkNamed(t, result, name); check.Status != doctorSkip {
			t.Fatalf("%s ran after invalid local configuration: %#v", name, check)
		}
	}
	if requests.Load() != 0 {
		t.Fatalf("invalid storage ID caused %d TAMS request(s)", requests.Load())
	}
}

func TestDoctorOnlineOAuthCodeModeIsNonInteractive(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	const querySecret = "doctor-authorization-query-secret"
	arguments := doctorArgs(t, "json")
	arguments = append([]string{
		"--endpoint", server.URL, "--allow-insecure-auth-loopback",
		"--auth", "oauth-code",
		"--token-url", "https://identity.example.test/token",
		"--authorization-url", "https://identity.example.test/authorize?configured=" + querySecret,
		"--client-id", "doctor-client",
	}, arguments...)
	arguments = append(arguments, "--online")
	code, result, stdout, stderr := executeDoctor(t, arguments)
	if code != ExitAuth || checkNamed(t, result, "authentication").Status != doctorFail {
		t.Fatalf("exit/authentication = %d/%#v; stdout = %s; stderr = %s",
			code, checkNamed(t, result, "authentication"), stdout, stderr)
	}
	if requests.Load() != 0 {
		t.Fatalf("non-interactive doctor caused %d network request(s)", requests.Load())
	}
	if strings.Contains(stdout, querySecret) || strings.Contains(stderr, querySecret) ||
		strings.Contains(stderr, "Open this URL") {
		t.Fatalf("doctor exposed an interactive authorization prompt; stdout = %s; stderr = %s", stdout, stderr)
	}
}

func TestDoctorOnlineReportsUnsafeOAuthEndpointBeforeMissingCode(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	const querySecret = "unsafe-authorization-query-secret"
	arguments := doctorArgs(t, "json")
	arguments = append([]string{
		"--endpoint", server.URL, "--allow-insecure-auth-loopback",
		"--auth", "oauth-code",
		"--token-url", "http://identity.example.test/token",
		"--authorization-url", "http://identity.example.test/authorize?configured=" + querySecret,
		"--client-id", "doctor-client",
	}, arguments...)
	arguments = append(arguments, "--online")
	code, result, stdout, stderr := executeDoctor(t, arguments)
	check := checkNamed(t, result, "authentication")
	if code != ExitAuth || check.Status != doctorFail || !strings.Contains(check.Error, "HTTPS") ||
		strings.Contains(check.Error, "pre-obtained") {
		t.Fatalf("exit/authentication = %d/%#v; stdout = %s; stderr = %s", code, check, stdout, stderr)
	}
	if requests.Load() != 0 {
		t.Fatalf("unsafe OAuth configuration caused %d network request(s)", requests.Load())
	}
	if strings.Contains(stdout, querySecret) || strings.Contains(stderr, querySecret) {
		t.Fatalf("unsafe OAuth error exposed a URL query; stdout = %s; stderr = %s", stdout, stderr)
	}
}

func TestDoctorStorageResponseCannotReflectCredential(t *testing.T) {
	t.Parallel()
	const token = "7f344efb-2084-41ac-9038-6b7cb5ca978a"
	for _, testCase := range []struct {
		name       string
		backends   []tams.StorageBackend
		wantCode   int
		wantStatus string
	}{
		{
			name:     "selected default",
			backends: []tams.StorageBackend{{ID: token, DefaultStorage: true}},
			wantCode: ExitOK, wantStatus: doctorPass,
		},
		{
			name: "ambiguous defaults",
			backends: []tams.StorageBackend{
				{ID: token, DefaultStorage: true},
				{ID: doctorDefaultStorage, DefaultStorage: true},
			},
			wantCode: ExitRemote, wantStatus: doctorFail,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			server, _, _ := doctorServer(t, validDoctorService("8.1"), testCase.backends, false)
			defer server.Close()
			arguments := doctorArgs(t, "json")
			arguments = append([]string{
				"--endpoint", server.URL, "--auth", "bearer", "--token", token,
				"--allow-insecure-auth-loopback",
			}, arguments...)
			arguments = append(arguments, "--online")
			code, result, stdout, stderr := executeDoctor(t, arguments)
			if code != testCase.wantCode || checkNamed(t, result, "storage_selection").Status != testCase.wantStatus {
				t.Fatalf("exit/selection = %d/%#v; stdout = %s; stderr = %s",
					code, checkNamed(t, result, "storage_selection"), stdout, stderr)
			}
			if strings.Contains(stdout, token) || strings.Contains(stderr, token) {
				t.Fatalf("peer-controlled backend ID reflected a credential; stdout = %s; stderr = %s", stdout, stderr)
			}
		})
	}
}

func TestDoctorOnlineResolvesDefaultAndRequestedStorage(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name       string
		backends   []tams.StorageBackend
		requested  string
		wantCode   int
		wantStatus string
		wantSource string
	}{
		{
			name: "default", backends: []tams.StorageBackend{{ID: doctorDefaultStorage, DefaultStorage: true}},
			wantCode: ExitOK, wantStatus: doctorPass, wantSource: "default",
		},
		{
			name: "requested", backends: []tams.StorageBackend{{ID: doctorDefaultStorage, DefaultStorage: true}, {ID: doctorRequestedStorage}},
			requested: doctorRequestedStorage, wantCode: ExitOK, wantStatus: doctorPass, wantSource: "requested",
		},
		{
			name: "no default", backends: []tams.StorageBackend{{ID: doctorRequestedStorage}},
			wantCode: ExitRemote, wantStatus: doctorFail,
		},
		{
			name: "none advertised", backends: []tams.StorageBackend{},
			wantCode: ExitRemote, wantStatus: doctorFail,
		},
		{
			name: "requested missing", backends: []tams.StorageBackend{{ID: doctorDefaultStorage, DefaultStorage: true}},
			requested: doctorRequestedStorage, wantCode: ExitRemote, wantStatus: doctorFail,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			server, _, mutations := doctorServer(t, validDoctorService("8.1"), testCase.backends, false)
			defer server.Close()
			arguments := onlineDoctorArgs(t, server.URL)
			if testCase.requested != "" {
				arguments = append(arguments, "--storage-id", testCase.requested)
			}
			code, result, stdout, stderr := executeDoctor(t, arguments)
			check := checkNamed(t, result, "storage_selection")
			if code != testCase.wantCode || check.Status != testCase.wantStatus {
				t.Fatalf("exit/check = %d/%#v; stdout = %s; stderr = %s", code, check, stdout, stderr)
			}
			if testCase.wantSource != "" && check.Detail["source"] != testCase.wantSource {
				t.Fatalf("selection source = %#v, want %q", check.Detail, testCase.wantSource)
			}
			if mutations.Load() != 0 {
				t.Fatalf("storage preflight caused %d mutation(s)", mutations.Load())
			}
		})
	}
}

func TestDoctorOnlineReportsRedactedEndpointAndResolvedAuthMode(t *testing.T) {
	t.Parallel()
	const token = "doctor-url-token-secret"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("access_token") != token {
			http.Error(writer, "missing token", http.StatusUnauthorized)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/service":
			_ = json.NewEncoder(writer).Encode(validDoctorService("8.1"))
		case "/service/storage-backends":
			_ = json.NewEncoder(writer).Encode([]tams.StorageBackend{{ID: doctorDefaultStorage, DefaultStorage: true}})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	arguments := doctorArgs(t, "json")
	arguments = append([]string{
		"--endpoint", server.URL + "?access_token=" + token,
		"--auth", "url-token", "--allow-insecure-auth-loopback",
	}, arguments...)
	arguments = append(arguments, "--online")
	code, result, stdout, stderr := executeDoctor(t, arguments)
	if code != ExitOK || result.Auth != "url-token" || !strings.Contains(result.Endpoint, "REDACTED") {
		t.Fatalf("exit/auth/endpoint = %d/%q/%q; stdout = %s; stderr = %s",
			code, result.Auth, result.Endpoint, stdout, stderr)
	}
	if strings.Contains(stdout, token) || strings.Contains(stderr, token) {
		t.Fatalf("doctor leaked endpoint token; stdout = %s; stderr = %s", stdout, stderr)
	}
}

func TestDoctorOnlineAuthenticationFailureDoesNotLeakSecrets(t *testing.T) {
	t.Parallel()
	const responseSecret = "server-response-secret"
	basicCredential := base64.StdEncoding.EncodeToString([]byte("basic-user:basic-secret"))
	basicAuthorization := "Basic " + basicCredential
	tests := []struct {
		name              string
		endpointQuery     string
		authArguments     []string
		wantAuthorization string
		wantQueryToken    string
		secrets           []string
	}{
		{
			name:              "bearer",
			authArguments:     []string{"--auth", "bearer", "--token", "bearer-doctor-secret"},
			wantAuthorization: "Bearer bearer-doctor-secret",
			secrets:           []string{"bearer-doctor-secret"},
		},
		{
			name:           "URL token",
			endpointQuery:  "?access_token=url-doctor-secret",
			authArguments:  []string{"--auth", "url-token"},
			wantQueryToken: "url-doctor-secret",
			secrets:        []string{"url-doctor-secret"},
		},
		{
			name:              "Basic",
			authArguments:     []string{"--auth", "basic", "--username", "basic-user", "--password", "basic-secret"},
			wantAuthorization: basicAuthorization,
			secrets:           []string{"basic-user", "basic-secret", basicCredential},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if got := request.Header.Get("Authorization"); got != testCase.wantAuthorization {
					t.Errorf("Authorization = %q, want %q", got, testCase.wantAuthorization)
				}
				if got := request.URL.Query().Get("access_token"); got != testCase.wantQueryToken {
					t.Errorf("access_token = %q, want %q", got, testCase.wantQueryToken)
				}
				http.Error(writer, strings.Join([]string{
					responseSecret,
					request.Header.Get("Authorization"),
					request.URL.RawQuery,
				}, " "), http.StatusUnauthorized)
			}))
			defer server.Close()

			arguments := doctorArgs(t, "json")
			globalArguments := []string{"--endpoint", server.URL + testCase.endpointQuery, "--allow-insecure-auth-loopback"}
			globalArguments = append(globalArguments, testCase.authArguments...)
			arguments = append(globalArguments, arguments...)
			arguments = append(arguments, "--online")
			code, result, stdout, stderr := executeDoctor(t, arguments)
			if code != ExitAuth || checkNamed(t, result, "authentication").Status != doctorFail {
				t.Fatalf("exit/auth = %d/%#v; stdout = %s; stderr = %s", code, checkNamed(t, result, "authentication"), stdout, stderr)
			}
			secrets := append([]string{responseSecret}, testCase.secrets...)
			for _, secret := range secrets {
				if strings.Contains(stdout, secret) || strings.Contains(stderr, secret) {
					t.Fatalf("doctor leaked %q; stdout = %s; stderr = %s", secret, stdout, stderr)
				}
			}
		})
	}
}

func TestDoctorOnlineAcceptedResponseRemainsAuthenticationEvidence(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/service":
			_ = json.NewEncoder(writer).Encode(validDoctorService("8.1"))
		case "/service/storage-backends":
			writer.WriteHeader(http.StatusForbidden)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	code, result, stdout, stderr := executeDoctor(t, onlineDoctorArgs(t, server.URL))
	if code != ExitAuth || checkNamed(t, result, "authentication").Status != doctorPass ||
		checkNamed(t, result, "service").Status != doctorPass ||
		checkNamed(t, result, "storage_backends").Status != doctorFail {
		t.Fatalf("exit/report = %d/%#v; stdout = %s; stderr = %s", code, result, stdout, stderr)
	}
}

func TestDoctorOnlineAppliesCredentialLoopbackAndTLSPolicy(t *testing.T) {
	t.Parallel()
	t.Run("credentialed HTTP loopback needs explicit opt-in", func(t *testing.T) {
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			requests.Add(1)
			writer.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()
		arguments := doctorArgs(t, "json")
		arguments = append([]string{"--endpoint", server.URL, "--auth", "bearer", "--token", "loopback-secret"}, arguments...)
		arguments = append(arguments, "--online")
		code, result, stdout, stderr := executeDoctor(t, arguments)
		if code != ExitAuth || checkNamed(t, result, "authentication").Status != doctorFail || requests.Load() != 0 {
			t.Fatalf("exit/auth/requests = %d/%#v/%d; stdout = %s; stderr = %s",
				code, checkNamed(t, result, "authentication"), requests.Load(), stdout, stderr)
		}
		if strings.Contains(stdout, "loopback-secret") || strings.Contains(stderr, "loopback-secret") {
			t.Fatal("loopback policy failure leaked bearer token")
		}
	})

	t.Run("HTTPS certificate policy", func(t *testing.T) {
		server, _, _ := doctorServer(t, validDoctorService("8.1"), []tams.StorageBackend{{ID: doctorDefaultStorage, DefaultStorage: true}}, true)
		defer server.Close()
		code, result, stdout, stderr := executeDoctor(t, onlineDoctorArgs(t, server.URL))
		if code != ExitRemote || checkNamed(t, result, "service").Status != doctorFail ||
			checkNamed(t, result, "authentication").Status != doctorSkip {
			t.Fatalf("untrusted TLS exit/service = %d/%#v; stdout = %s; stderr = %s", code, checkNamed(t, result, "service"), stdout, stderr)
		}

		arguments := onlineDoctorArgs(t, server.URL)
		arguments = append([]string{"--insecure-skip-verify"}, arguments...)
		code, result, stdout, stderr = executeDoctor(t, arguments)
		if code != ExitOK || result.Status != doctorPass {
			t.Fatalf("explicit insecure TLS exit/report = %d/%#v; stdout = %s; stderr = %s", code, result, stdout, stderr)
		}
	})
}

func TestDoctorOnlineDoesNotRequireInputOrRenderMedia(t *testing.T) {
	t.Parallel()
	server, requests, mutations := doctorServer(t, validDoctorService("8.1"), []tams.StorageBackend{{ID: doctorDefaultStorage, DefaultStorage: true}}, false)
	defer server.Close()
	arguments := onlineDoctorArgs(t, server.URL)
	code, result, stdout, stderr := executeDoctor(t, arguments)
	if code != ExitOK || result.Status != doctorPass {
		t.Fatalf("exit/report = %d/%#v; stdout = %s; stderr = %s", code, result, stdout, stderr)
	}
	if requests.Load() != 2 || mutations.Load() != 0 {
		t.Fatalf("doctor requests/mutations = %d/%d", requests.Load(), mutations.Load())
	}
	for _, check := range result.Checks {
		if strings.Contains(check.Name, "input") || strings.Contains(check.Name, "render") {
			t.Fatalf("doctor unexpectedly required media work: %#v", check)
		}
	}
}

func TestDoctorOnlineHTTPErrorBodyIsNeverReported(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(writer, "peer-response-secret")
	}))
	defer server.Close()
	code, result, stdout, stderr := executeDoctor(t, onlineDoctorArgs(t, server.URL))
	if code != ExitRemote {
		t.Fatalf("exit = %d; stdout = %s; stderr = %s", code, stdout, stderr)
	}
	if checkNamed(t, result, "authentication").Status != doctorSkip {
		t.Fatalf("authentication was claimed without a successful response: %#v", checkNamed(t, result, "authentication"))
	}
	if strings.Contains(stdout, "peer-response-secret") || strings.Contains(stderr, "peer-response-secret") {
		t.Fatalf("doctor leaked HTTP response body; stdout = %s; stderr = %s", stdout, stderr)
	}
}

func TestDoctorExitPrecedenceIsStable(t *testing.T) {
	t.Parallel()
	arguments := []string{
		"--format", "json", "--ffprobe", filepath.Join(t.TempDir(), "missing-ffprobe"),
		"--ffmpeg", filepath.Join(t.TempDir(), "missing-ffmpeg"),
		"doctor", "--temp-dir", t.TempDir(), "--profile", "not-a-profile",
	}
	code, result, stdout, stderr := executeDoctor(t, arguments)
	if code != ExitUsage {
		t.Fatalf("exit = %d, want usage to precede media; stdout = %s; stderr = %s", code, stdout, stderr)
	}
	if checkNamed(t, result, "profile").Status != doctorFail || checkNamed(t, result, "ffprobe").Status != doctorFail {
		t.Fatalf("failures were not both reported: %#v", result.Checks)
	}

	for _, testCase := range []struct {
		name     string
		failures []doctorFailure
		want     int
	}{
		{name: "usage over every category", failures: []doctorFailure{{ExitGeneral}, {ExitRemote}, {ExitAuth}, {ExitMedia}, {ExitUsage}}, want: ExitUsage},
		{name: "media over auth remote and general", failures: []doctorFailure{{ExitGeneral}, {ExitRemote}, {ExitAuth}, {ExitMedia}}, want: ExitMedia},
		{name: "auth over remote and general", failures: []doctorFailure{{ExitGeneral}, {ExitRemote}, {ExitAuth}}, want: ExitAuth},
		{name: "remote over general", failures: []doctorFailure{{ExitGeneral}, {ExitRemote}}, want: ExitRemote},
		{name: "general fallback", failures: []doctorFailure{{ExitGeneral}}, want: ExitGeneral},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			run := doctorRun{failures: testCase.failures}
			if got := run.exitCode(); got != testCase.want {
				t.Fatalf("exit precedence = %d, want %d", got, testCase.want)
			}
		})
	}
}

func Example_doctorTextReport() {
	fmt.Println("doctor\tstatus=pass\ttamsin=\"v1.0.0\"\tgo=\"go1.26\"\tos=linux\tarch=amd64\tprofile=preserve@1")
	// Output:
	// doctor	status=pass	tamsin="v1.0.0"	go="go1.26"	os=linux	arch=amd64	profile=preserve@1
}
