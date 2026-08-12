package contracts_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/cli"
)

func TestPublishedProfilesReportSchemaAcceptsRuntimeCatalogue(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := cli.Execute(context.Background(), []string{"--format", "json", "profiles"}, strings.NewReader(""), &stdout, &stderr)
	if code != cli.ExitOK {
		t.Fatalf("profiles exit = %d, stderr = %s", code, stderr.String())
	}
	var report map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode profiles report %q: %v", stdout.String(), err)
	}
	schema := compileTamsinSchema(t, "profiles-report-v1.json")
	if err := validate(t, schema, report); err != nil {
		t.Fatalf("runtime profiles report does not satisfy its published schema: %v\n%#v", err, report)
	}
	report["unexpected"] = true
	if err := validate(t, schema, report); err == nil {
		t.Fatal("profiles report schema accepted an unknown top-level property")
	}
}
