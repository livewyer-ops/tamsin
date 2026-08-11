package contracts_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/cli"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func doctorContractTool(t *testing.T, name string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("doctor contract fixture uses a POSIX test executable")
	}
	filename := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(filename, []byte("#!/bin/sh\nprintf '%s version contract-test\\n' '"+name+"'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return filename
}

func runDoctorContract(t *testing.T, extra ...string) (int, map[string]any) {
	t.Helper()
	config := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(config, []byte("format: json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	arguments := []string{
		"--config", config,
		"--format", "json",
		"--ffprobe", doctorContractTool(t, "ffprobe"),
		"--ffmpeg", doctorContractTool(t, "ffmpeg"),
		"--retries", "0",
		"doctor", "--temp-dir", t.TempDir(),
	}
	arguments = append(arguments, extra...)
	var stdout, stderr bytes.Buffer
	code := cli.Execute(context.Background(), arguments, strings.NewReader(""), &stdout, &stderr)
	var report map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode doctor report %q (stderr %q): %v", stdout.String(), stderr.String(), err)
	}
	return code, report
}

func validateDoctorRuntimeReport(t *testing.T, schema *jsonschema.Schema, report map[string]any) {
	t.Helper()
	if err := validate(t, schema, report); err != nil {
		t.Fatalf("runtime doctor report does not satisfy its published schema: %v\n%#v", err, report)
	}
	want := []string{
		"configuration", "profile", "staging", "ffprobe", "ffmpeg", "authentication",
		"service", "api_compatibility", "service_lifetimes", "storage_backends", "storage_selection",
	}
	checks, ok := report["checks"].([]any)
	if !ok || len(checks) != len(want) {
		t.Fatalf("doctor check inventory = %#v, want %d ordered checks", report["checks"], len(want))
	}
	for index, name := range want {
		check, ok := checks[index].(map[string]any)
		if !ok || check["name"] != name {
			t.Fatalf("doctor check %d = %#v, want %q", index, checks[index], name)
		}
	}
}

func TestPublishedDoctorReportSchemaAcceptsRuntimeReports(t *testing.T) {
	t.Parallel()
	schema := compileTamsinSchema(t, "doctor-report-v1.json")

	t.Run("offline", func(t *testing.T) {
		code, report := runDoctorContract(t)
		if code != cli.ExitOK || report["online"] != false || report["status"] != "pass" {
			t.Fatalf("offline exit/report = %d/%#v", code, report)
		}
		validateDoctorRuntimeReport(t, schema, report)
	})

	t.Run("online", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			switch request.URL.Path {
			case "/service":
				_, _ = fmt.Fprint(writer, `{"api_version":"8.1","min_object_timeout":"300:0","min_presigned_url_timeout":"30:0"}`)
			case "/service/storage-backends":
				_, _ = fmt.Fprint(writer, `[{"id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8","default_storage":true}]`)
			default:
				http.NotFound(writer, request)
			}
		}))
		defer server.Close()
		code, report := runDoctorContract(t, "--profile", "preserve", "--online", "--endpoint", server.URL, "--auth", "none")
		if code != cli.ExitOK || report["online"] != true || report["status"] != "pass" {
			t.Fatalf("online exit/report = %d/%#v", code, report)
		}
		validateDoctorRuntimeReport(t, schema, report)
	})

	t.Run("failure", func(t *testing.T) {
		code, report := runDoctorContract(t, "--profile", "unknown-profile")
		if code != cli.ExitUsage || report["status"] != "fail" {
			t.Fatalf("failure exit/report = %d/%#v", code, report)
		}
		validateDoctorRuntimeReport(t, schema, report)

		report["unexpected"] = true
		if err := validate(t, schema, report); err == nil {
			t.Fatal("doctor schema accepted an unknown top-level property")
		}
	})
}
