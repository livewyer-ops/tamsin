package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// rule is one specification requirement bearing on the ingest write path,
// carrying the document it comes from and the test that asserts it. This is
// what makes a pin bump a diff against a checklist rather than a re-read.
type rule struct {
	ID          string `json:"id"`
	Source      string `json:"source"`
	Requirement string `json:"requirement"`
	AssertedBy  string `json:"asserted_by"`
}

type matrix struct {
	Rules        []rule `json:"rules"`
	OpenFindings []struct {
		ID            string `json:"id"`
		Source        string `json:"source"`
		Requirement   string `json:"requirement"`
		Exposure      string `json:"exposure"`
		Status        string `json:"status"`
		BlocksRelease *bool  `json:"blocks_release"`
	} `json:"open_findings"`
	NotApplicable []struct {
		Source string `json:"source"`
		Reason string `json:"reason"`
	} `json:"not_applicable"`
	SpecReview struct {
		ReviewedAtCommit string `json:"reviewed_at_commit"`
		ADRsTotal        int    `json:"adrs_total"`
		ADRsRead         int    `json:"adrs_read"`
		AppNotesTotal    int    `json:"appnotes_total"`
		AppNotesRead     int    `json:"appnotes_read"`
	} `json:"spec_review"`
	TAMS struct {
		Version        string `json:"version"`
		Commit         string `json:"commit"`
		OpenAPIGitBlob string `json:"openapi_git_blob"`
	} `json:"tams"`
	TAMOSS struct {
		Commit              string `json:"commit"`
		TAMSSubmoduleCommit string `json:"tams_submodule_commit"`
		Profile             string `json:"profile"`
		Release             string `json:"release"`
	} `json:"tamoss"`
	Authentication []struct {
		ID            string `json:"id"`
		CLI           string `json:"cli"`
		ImplementedBy string `json:"implemented_by"`
		TestedBy      string `json:"tested_by"`
	} `json:"authentication"`
	IngestOperations []struct {
		OperationID   string `json:"operation_id"`
		Method        string `json:"method"`
		Path          string `json:"path"`
		CLI           string `json:"cli"`
		ImplementedBy string `json:"implemented_by"`
		TestedBy      string `json:"tested_by"`
	} `json:"ingest_operations"`
}

func TestPinnedTAMSIngestConformanceMatrixIsComplete(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("tams-v8.1.json")
	if err != nil {
		t.Fatal(err)
	}
	var contract matrix
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatal(err)
	}
	if contract.TAMS.Version != "8.1" || contract.TAMS.Commit != "98d307b09b5ebf79278aa7d3aad53295154e2c17" || contract.TAMS.OpenAPIGitBlob == "" {
		t.Fatalf("unexpected TAMS pin: %#v", contract.TAMS)
	}
	if contract.TAMOSS.Commit == "" || contract.TAMOSS.TAMSSubmoduleCommit != contract.TAMS.Commit {
		t.Fatalf("TAMOSS and TAMS pins are not aligned: %#v", contract.TAMOSS)
	}
	if contract.TAMOSS.Profile == "" || contract.TAMOSS.Release == "" {
		t.Fatalf("TAMOSS pin lacks a profile or release: %#v", contract.TAMOSS)
	}

	actualAuth := make([]string, 0, len(contract.Authentication))
	for _, entry := range contract.Authentication {
		if entry.CLI == "" || entry.ImplementedBy == "" || entry.TestedBy == "" {
			t.Fatalf("authentication %s lacks CLI, implementation, or test coverage", entry.ID)
		}
		actualAuth = append(actualAuth, entry.ID)
	}
	sort.Strings(actualAuth)
	expectedAuth := []string{"basic_auth", "bearer_token_auth", "oauth2_authorization_code", "oauth2_client_credentials", "url_token_auth"}
	if !reflect.DeepEqual(actualAuth, expectedAuth) {
		t.Fatalf("authentication coverage = %v, want %v", actualAuth, expectedAuth)
	}

	actualOperations := make([]string, 0, len(contract.IngestOperations))
	for _, entry := range contract.IngestOperations {
		if entry.Method == "" || entry.Path == "" || entry.CLI == "" || entry.ImplementedBy == "" || entry.TestedBy == "" {
			t.Fatalf("operation %s lacks method, path, CLI, implementation, or test coverage", entry.OperationID)
		}
		actualOperations = append(actualOperations, entry.OperationID)
	}
	sort.Strings(actualOperations)
	expectedOperations := []string{
		"DELETE_flows-flowId-segments", "DELETE_objects-instances", "GET_flow-delete-requests-request-id", "GET_flows-flowId", "GET_flows-flowId-segments", "GET_objects", "GET_service", "GET_storage-backends",
		"POST_flows-flowId-segments", "POST_flows-flowId-storage", "POST_objects-instances", "PUT_flows-flowId",
	}
	sort.Strings(expectedOperations)
	if !reflect.DeepEqual(actualOperations, expectedOperations) {
		t.Fatalf("operation coverage = %v, want %v", actualOperations, expectedOperations)
	}

	// Every recorded rule must name where it comes from and what proves it,
	// otherwise the inventory decays into prose nobody can act on.
	if len(contract.Rules) == 0 {
		t.Fatal("no specification rules recorded")
	}
	seen := make(map[string]bool, len(contract.Rules))
	for _, entry := range contract.Rules {
		if entry.ID == "" || entry.Source == "" || entry.Requirement == "" || entry.AssertedBy == "" {
			t.Fatalf("rule %q lacks an id, source, requirement, or asserting test", entry.ID)
		}
		if seen[entry.ID] {
			t.Fatalf("duplicate rule id %q", entry.ID)
		}
		seen[entry.ID] = true
	}

	// A requirement that bears on ingest but is not met is recorded rather than
	// omitted. Without this the inventory would read as complete conformance
	// while quietly saying nothing about what is known to be missing, which is
	// the one failure mode that makes an audit worse than no audit.
	for _, entry := range contract.OpenFindings {
		if entry.ID == "" || entry.Source == "" || entry.Requirement == "" || entry.Exposure == "" || entry.Status == "" {
			t.Fatalf("open finding %q lacks an id, source, requirement, exposure, or status", entry.ID)
		}
		// Whether a gap can be shipped around is a decision, and scripts/
		// check-release-gate.sh enforces it. Leaving the field out would let a
		// finding quietly default to harmless.
		if entry.BlocksRelease == nil {
			t.Fatalf("open finding %q does not say whether it blocks a release", entry.ID)
		}
		if seen[entry.ID] {
			t.Fatalf("open finding %q reuses a rule id", entry.ID)
		}
		seen[entry.ID] = true
	}

	// A document dismissed as irrelevant has to say why, or "not applicable"
	// becomes indistinguishable from "not read".
	for _, entry := range contract.NotApplicable {
		if entry.Source == "" || entry.Reason == "" {
			t.Fatalf("not-applicable entry %+v lacks a source or a reason", entry)
		}
	}

	// The review is only meaningful against the commit it was carried out at.
	if contract.SpecReview.ReviewedAtCommit != contract.TAMS.Commit {
		t.Fatalf("specification review was carried out at %s but the pin is %s",
			contract.SpecReview.ReviewedAtCommit, contract.TAMS.Commit)
	}
	if contract.SpecReview.ADRsRead > contract.SpecReview.ADRsTotal ||
		contract.SpecReview.AppNotesRead > contract.SpecReview.AppNotesTotal {
		t.Fatalf("specification review counts are inconsistent: %+v", contract.SpecReview)
	}
	// The review is complete at this pin. If the pin moves, the new documents
	// have to be read before this passes again, which is the point.
	if contract.SpecReview.ADRsRead != contract.SpecReview.ADRsTotal ||
		contract.SpecReview.AppNotesRead != contract.SpecReview.AppNotesTotal {
		t.Fatalf("specification review is incomplete: %d/%d ADRs and %d/%d AppNotes read",
			contract.SpecReview.ADRsRead, contract.SpecReview.ADRsTotal,
			contract.SpecReview.AppNotesRead, contract.SpecReview.AppNotesTotal)
	}

	// Every reviewed specification document is either the source of a rule or
	// finding, or is explicitly dismissed with a reason. Deriving the counts
	// from that inventory prevents a pin bump from being made to look complete
	// by changing two matching numbers while leaving new documents unclassified.
	adrs, appNotes := reviewedDocuments(contract)
	if len(adrs) != contract.SpecReview.ADRsRead || len(appNotes) != contract.SpecReview.AppNotesRead {
		t.Fatalf("review inventory names %d ADRs and %d AppNotes, but the review records %d and %d",
			len(adrs), len(appNotes), contract.SpecReview.ADRsRead, contract.SpecReview.AppNotesRead)
	}
}

func reviewedDocuments(contract matrix) (map[string]bool, map[string]bool) {
	adrs := make(map[string]bool)
	appNotes := make(map[string]bool)
	add := func(source string) {
		for _, value := range strings.Split(source, ",") {
			value = strings.TrimSpace(value)
			switch {
			case strings.HasPrefix(value, "adr/"):
				adrs[value] = true
			case strings.HasPrefix(value, "appnote/"):
				appNotes[value] = true
			}
		}
	}
	for _, entry := range contract.Rules {
		add(entry.Source)
	}
	for _, entry := range contract.OpenFindings {
		add(entry.Source)
	}
	for _, entry := range contract.NotApplicable {
		add(entry.Source)
	}
	return adrs, appNotes
}

// The Kind harness must consume the same pin as the offline conformance
// matrix. Copying these values into shell made a contract update capable of
// testing an older service while every Go check stayed green.
func TestE2EHarnessReadsTAMSSPinsFromTheContract(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("tams-v8.1.json")
	if err != nil {
		t.Fatal(err)
	}
	var contract matrix
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatal(err)
	}
	script, err := os.ReadFile(filepath.Join("..", "scripts", "e2e-kind.sh"))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(script)
	for _, selector := range []string{".tams.commit", ".tamoss.commit", ".tamoss.release", ".tamoss.profile"} {
		if !strings.Contains(contents, "contract_value '"+selector+"'") {
			t.Errorf("E2E harness does not load %s from the conformance matrix", selector)
		}
	}
	for label, assignment := range map[string]string{
		"TAMS commit":    `TAMS_COMMIT="` + contract.TAMS.Commit + `"`,
		"TAMOSS commit":  `TAMOSS_COMMIT="` + contract.TAMOSS.Commit + `"`,
		"TAMOSS release": `TAMOSS_RELEASE="` + contract.TAMOSS.Release + `"`,
		"TAMOSS profile": `PROFILE="` + contract.TAMOSS.Profile + `"`,
	} {
		if strings.Contains(contents, assignment) {
			t.Errorf("E2E harness duplicates the %s instead of reading it from the contract", label)
		}
	}
}
