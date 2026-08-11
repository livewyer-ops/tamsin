package contracts_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/livewyer-ops/tamsin/ingestevent"
	"github.com/livewyer-ops/tamsin/internal/ingest"
)

// Failure codes are the machine-readable half of the published contract:
// consumers switch on them, and the schemas only constrain their shape
// (^[a-z][a-z0-9_.-]*$), not their values. Naming them as constants is right for
// the production code, but it means an assertion written against the constant
// moves with any rename and pins nothing. This table is the pin -- the literals
// here are the wire values, and changing one is a breaking change to every
// consumer of the result journal and the process event stream.
func TestPublishedFailureCodesAreStableWireValues(t *testing.T) {
	t.Parallel()

	published := map[string]struct {
		value string
		wire  string
	}{
		// Run-level outcomes, reported once for the whole invocation.
		"FailureCodeConfigInvalid": {ingest.FailureCodeConfigInvalid, "config.invalid"},
		"FailureCodeAuthFailed":    {ingest.FailureCodeAuthFailed, "authentication.failed"},
		"FailureCodeInputFailed":   {ingest.FailureCodeInputFailed, "ingest.input_failures"},
		"FailureCodeSourceFailed":  {ingest.FailureCodeSourceFailed, "source.failed"},
		"FailureCodeMediaFailed":   {ingest.FailureCodeMediaFailed, "media.failed"},
		"FailureCodeTAMSFailed":    {ingest.FailureCodeTAMSFailed, "tams.failed"},
		"FailureCodeInterrupted":   {ingest.FailureCodeInterrupted, "run.interrupted"},
		"FailureCodeRunFailed":     {ingest.FailureCodeRunFailed, "run.failed"},
		"FailureCodeJournalWrite":  {ingest.FailureCodeJournalWrite, "journal.write_failed"},

		// Input-level outcomes.
		"FailureCodeInputFailedGeneric":     {ingest.FailureCodeInputFailedGeneric, "ingest.input_failed"},
		"FailureCodeVerificationNotReached": {ingest.FailureCodeVerificationNotReached, "verification.not_reached"},
		"FailureCodeVerificationStranded":   {ingest.FailureCodeVerificationStranded, "verification.stranded"},
		"FailureCodeVerificationRetracted":  {ingest.FailureCodeVerificationRetracted, "verification.retracted"},

		// Terminal states that require an operator to intervene.
		"FailureCodeFlowIndeterminate":   {ingest.FailureCodeFlowIndeterminate, "flow.indeterminate"},
		"FailureCodeObjectStranded":      {ingest.FailureCodeObjectStranded, "object.stranded"},
		"FailureCodeObjectIndeterminate": {ingest.FailureCodeObjectIndeterminate, "object.indeterminate"},

		// Stage failures.
		"FailureCodeHTTPRequestFailed":      {ingest.FailureCodeHTTPRequestFailed, "tams.request_failed"},
		"FailureCodeOutputFailed":           {ingest.FailureCodeOutputFailed, "output.failed"},
		"FailureCodePreflightFailed":        {ingest.FailureCodePreflightFailed, "tams.preflight_failed"},
		"FailureCodeStorageUnavailable":     {ingest.FailureCodeStorageUnavailable, "tams.storage_unavailable"},
		"FailureCodeFlowPlanFailed":         {ingest.FailureCodeFlowPlanFailed, "flow.plan_failed"},
		"FailureCodeFlowWriteFailed":        {ingest.FailureCodeFlowWriteFailed, "flow.write_failed"},
		"FailureCodeTAMSRegistrationFailed": {ingest.FailureCodeTAMSRegistrationFailed, "tams.registration_failed"},
		"FailureCodeStagingCapacity":        {ingest.FailureCodeStagingCapacity, "staging.capacity"},
		"FailureCodeSourceTransferFailed":   {ingest.FailureCodeSourceTransferFailed, "source.transfer_failed"},
		"FailureCodeSourceChanged":          {ingest.FailureCodeSourceChanged, "source.changed"},
		"FailureCodeMediaAnalysisFailed":    {ingest.FailureCodeMediaAnalysisFailed, "media.analysis_failed"},
		"FailureCodeMediaUnsupported":       {ingest.FailureCodeMediaUnsupported, "media.unsupported"},
		"FailureCodeMediaOptionsInvalid":    {ingest.FailureCodeMediaOptionsInvalid, "media.options_invalid"},
		"FailureCodeMediaOptionsIgnored":    {ingest.FailureCodeMediaOptionsIgnored, "media.options_ignored"},
		"FailureCodeMediaToolUnavailable":   {ingest.FailureCodeMediaToolUnavailable, "media.tool_unavailable"},
		"FailureCodeMediaPrepareFailed":     {ingest.FailureCodeMediaPrepareFailed, "media.prepare_failed"},
	}

	seenWire := make(map[string]string, len(published))
	for name, contract := range published {
		if contract.value != contract.wire {
			t.Errorf("published failure code %s changed: %q is no longer %q", name, contract.value, contract.wire)
		}
		if previous, duplicate := seenWire[contract.value]; duplicate {
			t.Errorf("published failure codes %s and %s both use %q", previous, name, contract.value)
		}
		seenWire[contract.value] = name
	}

	for _, problem := range failureCodeCoverageProblems(declaredFailureCodes(t), published) {
		t.Error(problem)
	}
}

func failureCodeCoverageProblems[T any](declared map[string]struct{}, pinned map[string]T) []string {
	var problems []string
	for name := range declared {
		if _, exists := pinned[name]; !exists {
			problems = append(problems, "published failure code "+name+" is declared but not pinned")
		}
	}
	for name := range pinned {
		if _, exists := declared[name]; !exists {
			problems = append(problems, "pinned failure code "+name+" is no longer declared")
		}
	}
	return problems
}

func TestFailureCodeCoverageRejectsUnpinnedDeclarations(t *testing.T) {
	t.Parallel()
	declared := map[string]struct{}{"FailureCodeExisting": {}, "FailureCodeAdded": {}}
	pinned := map[string]string{"FailureCodeExisting": "existing"}
	problems := failureCodeCoverageProblems(declared, pinned)
	if len(problems) != 1 || !strings.Contains(problems[0], "FailureCodeAdded") {
		t.Fatalf("unpinned declaration problems = %v", problems)
	}
}

func declaredFailureCodes(t *testing.T) map[string]struct{} {
	t.Helper()
	filename := filepath.Join("..", "internal", "ingest", "failure.go")
	file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
	if err != nil {
		t.Fatalf("parse failure vocabulary: %v", err)
	}
	declared := make(map[string]struct{})
	for _, declaration := range file.Decls {
		constants, ok := declaration.(*ast.GenDecl)
		if !ok || constants.Tok != token.CONST {
			continue
		}
		for _, specification := range constants.Specs {
			values, ok := specification.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, name := range values.Names {
				if strings.HasPrefix(name.Name, "FailureCode") {
					declared[name.Name] = struct{}{}
				}
			}
		}
	}
	return declared
}

// Two packages name the same wire codes for different reasons: ingest publishes
// them as run and input failures, and ingestevent names the diagnostic codes its
// stream carries. Nothing in the type system connects the two, so a rename on
// one side would silently emit a code consumers no longer recognise.
func TestDiagnosticCodesMatchTheirPublishedFailureCodes(t *testing.T) {
	t.Parallel()

	for _, pair := range []struct {
		name       string
		diagnostic string
		failure    string
	}{
		{"config invalid", ingestevent.DiagnosticCodeConfigInvalid, ingest.FailureCodeConfigInvalid},
		{"object stranded", ingestevent.DiagnosticCodeObjectStranded, ingest.FailureCodeObjectStranded},
		{"run interrupted", ingestevent.InputErrorCodeRunInterrupted, ingest.FailureCodeInterrupted},
		{"input failed", ingestevent.InputErrorCodeIngestFailed, ingest.FailureCodeInputFailedGeneric},
	} {
		if pair.diagnostic != pair.failure {
			t.Errorf("%s: diagnostic code %q and failure code %q have drifted apart",
				pair.name, pair.diagnostic, pair.failure)
		}
	}
}
