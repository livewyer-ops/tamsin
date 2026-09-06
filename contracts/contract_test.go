package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
)

type compatibilityContract struct {
	SchemaVersion string `json:"schema_version"`
	Extends       string `json:"extends"`
	TAMS          struct {
		Version string `json:"version"`
		Commit  string `json:"commit"`
	} `json:"tams"`
	TAMOSS struct {
		Commit              string `json:"commit"`
		TAMSSubmoduleCommit string `json:"tams_submodule_commit"`
		Profile             string `json:"profile"`
		Release             string `json:"release"`
	} `json:"tamoss"`
	Authentication   []string `json:"authentication"`
	IngestOperations []struct {
		Method string `json:"method"`
		Path   string `json:"path"`
	} `json:"ingest_operations"`
	Requirements []string `json:"requirements"`
	OpenFindings []struct {
		ID            string `json:"id"`
		BlocksRelease bool   `json:"blocks_release"`
	} `json:"open_findings"`
}

func readCompatibilityContract(t *testing.T, name string) compatibilityContract {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var contract compatibilityContract
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	return contract
}

func TestPinnedTAMSCompatibilityContracts(t *testing.T) {
	t.Parallel()
	commit := regexp.MustCompile(`^[0-9a-f]{40}$`)
	for _, testCase := range []struct {
		name, version, parent string
	}{
		{"tams-v8.1.json", "8.1", ""},
		{"tams-v8.2.json", "8.2", "tams-v8.1.json"},
	} {
		contract := readCompatibilityContract(t, testCase.name)
		if contract.SchemaVersion != "1.0" || contract.TAMS.Version != testCase.version || contract.Extends != testCase.parent {
			t.Errorf("%s identity is incomplete: %#v", testCase.name, contract)
		}
		if !commit.MatchString(contract.TAMS.Commit) || !commit.MatchString(contract.TAMOSS.Commit) ||
			contract.TAMS.Commit != contract.TAMOSS.TAMSSubmoduleCommit || contract.TAMOSS.Profile == "" || contract.TAMOSS.Release == "" {
			t.Errorf("%s pins are incomplete: %#v", testCase.name, contract)
		}
		if len(contract.Requirements) == 0 {
			t.Errorf("%s records no ingest requirements", testCase.name)
		}
		for _, finding := range contract.OpenFindings {
			if finding.ID == "" {
				t.Errorf("%s has an unnamed open finding", testCase.name)
			}
		}
	}
}

func TestPinnedTAMSOperationsCoverTheIngestClient(t *testing.T) {
	t.Parallel()
	base := readCompatibilityContract(t, "tams-v8.1.json")
	target := readCompatibilityContract(t, "tams-v8.2.json")
	var got []string
	seen := make(map[string]bool)
	for _, operation := range append(base.IngestOperations, target.IngestOperations...) {
		key := operation.Method + " " + operation.Path
		if operation.Method == "" || operation.Path == "" || seen[key] {
			t.Fatalf("invalid or duplicate operation %q", key)
		}
		seen[key] = true
		got = append(got, key)
	}
	slices.Sort(got)
	want := []string{
		"DELETE /flows/{flowId}/segments",
		"GET /flow-delete-requests/{requestId}",
		"GET /flows/{flowId}",
		"GET /flows/{flowId}/segments",
		"GET /service",
		"GET /service/profiles/{profileId}",
		"GET /service/storage-backends",
		"POST /flows/{flowId}/segments",
		"POST /flows/{flowId}/storage",
		"PUT /flows/{flowId}",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("ingest operations = %v, want %v", got, want)
	}
	wantAuth := []string{"basic", "bearer", "oauth-client", "oauth-code", "url-token"}
	slices.Sort(base.Authentication)
	if !slices.Equal(base.Authentication, wantAuth) {
		t.Fatalf("authentication modes = %v", base.Authentication)
	}
}

func TestE2EHarnessReadsTAMSSPinsFromTheContract(t *testing.T) {
	t.Parallel()
	script, err := os.ReadFile(filepath.Join("..", "scripts", "e2e-kind.sh"))
	if err != nil {
		t.Fatal(err)
	}
	for _, selector := range []string{".tamoss.commit", ".tams.commit", ".tamoss.release", ".tamoss.profile"} {
		if !regexp.MustCompile(regexp.QuoteMeta(selector)).Match(script) {
			t.Errorf("E2E harness does not read %s", selector)
		}
	}
}
