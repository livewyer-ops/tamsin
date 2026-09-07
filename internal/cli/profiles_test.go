package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/ingest"
)

func TestProfilesCommandPublishesOrderedHumanAndMachineCatalogues(t *testing.T) {
	t.Parallel()
	want := []string{"preserve@1", "demux@1", "muxed-segments@1", "essence-segments@1", "mpegts-segments@1"}

	var humanOut, humanErr bytes.Buffer
	code := Execute(context.Background(), []string{"--config", "does-not-exist.yaml", "profiles"}, strings.NewReader(""), &humanOut, &humanErr)
	if code != ExitOK || humanErr.Len() != 0 {
		t.Fatalf("human profiles exit = %d, stderr = %s", code, humanErr.String())
	}
	last := -1
	for _, selection := range want {
		index := strings.Index(humanOut.String(), selection)
		if index <= last {
			t.Fatalf("human catalogue does not contain %q in registry order:\n%s", selection, humanOut.String())
		}
		last = index
	}

	var jsonOut, jsonErr bytes.Buffer
	code = Execute(context.Background(), []string{"--format", "json", "profiles"}, strings.NewReader(""), &jsonOut, &jsonErr)
	if code != ExitOK || jsonErr.Len() != 0 {
		t.Fatalf("JSON profiles exit = %d, stderr = %s", code, jsonErr.String())
	}
	wire := assertJSONKeys(t, jsonOut.Bytes(), "schema_version", "profile_policy_version", "profiles")
	if string(wire["schema_version"]) != `"1.0"` || string(wire["profile_policy_version"]) != `"1"` {
		t.Fatalf("profiles report versions = %s", jsonOut.String())
	}
	var profiles []json.RawMessage
	if err := json.Unmarshal(wire["profiles"], &profiles); err != nil {
		t.Fatal(err)
	}
	for _, profile := range profiles {
		assertJSONKeys(t, profile, "name", "version", "selection", "essence_storage", "segment_duration", "segment_format", "ffmpeg", "source_bytes_preserved", "object_pattern", "intended_use", "resource_note")
	}
	var report profilesReport
	if err := json.Unmarshal(jsonOut.Bytes(), &report); err != nil {
		t.Fatalf("decode profiles report %q: %v", jsonOut.String(), err)
	}
	if report.SchemaVersion != ProfilesReportSchemaVersion || report.ProfilePolicyVersion != ingest.ProfilePolicyVersion || len(report.Profiles) != len(want) {
		t.Fatalf("profiles report = %#v", report)
	}
	for index, profile := range report.Profiles {
		if profile.Selection != want[index] {
			t.Fatalf("profiles[%d] = %#v, want %s", index, profile, want[index])
		}
	}
}

func TestProfilesCommandRejectsArguments(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{"profiles", "preserve"}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitUsage || !strings.Contains(stderr.String(), "unknown command") && !strings.Contains(stderr.String(), "accepts 0 arg") {
		t.Fatalf("profiles argument exit/stdout/stderr = %d/%q/%q", code, stdout.String(), stderr.String())
	}
}
