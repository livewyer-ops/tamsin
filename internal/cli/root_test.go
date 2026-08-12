package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/ingestevent"
	"github.com/livewyer-ops/tamsin/internal/ingest"
	"github.com/livewyer-ops/tamsin/internal/progress"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func TestLoggerForProgressDoesNotChangeConfiguredLevel(t *testing.T) {
	t.Parallel()
	for _, mode := range []progress.Mode{"discard", progress.ModePlain, progress.ModeTTY} {
		t.Run(string(mode), func(t *testing.T) {
			var stderr bytes.Buffer
			app := &application{v: viper.New(), stdin: strings.NewReader(""), stdout: io.Discard, stderr: &stderr}
			_ = app.rootCommand()
			var reporter progress.Reporter = progress.Discard{}
			if mode != "discard" {
				reporter = progress.New(&stderr, progress.Options{Mode: mode, Getenv: func(string) string { return "" }})
			}
			defer reporter.Close()
			logger, err := app.loggerFor(reporter)
			if err != nil {
				t.Fatal(err)
			}
			logger.Info("info-marker")
			logger.Warn("warn-marker")
			if !strings.Contains(stderr.String(), "info-marker") || !strings.Contains(stderr.String(), "warn-marker") {
				t.Fatalf("mode %q changed the default info level: %q", mode, stderr.String())
			}
		})
	}
}

func TestCLIInputResolutionIsAtomicBeforeJournal(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	valid := filepath.Join(directory, "valid.mp4")
	if err := os.WriteFile(valid, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(directory, "results.jsonl")
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--profile", "preserve", "--dry-run", "--journal", journal, "--format", "json", "--log-format", "json", "--progress", "none",
		"--ffprobe", fakeMediaTool(t, directory),
		"-i", valid, "-i", filepath.Join(directory, "missing.mp4"),
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitSource {
		t.Fatalf("exit = %d, want %d; stdout=%s stderr=%s", code, ExitSource, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(journal); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal exists after atomic resolution failure: %v", err)
	}
	stream := decodeCLIIngestEventStream(t, stdout.Bytes())
	if stream.state.Started == nil || stream.state.Started.RequestedInputs == nil || *stream.state.Started.RequestedInputs != 2 {
		t.Fatalf("requested inputs were not retained: %#v", stream.state.Started)
	}
	if stream.state.Manifest == nil || stream.state.Manifest.TotalInputs != 0 || len(stream.state.Inputs) != 0 {
		t.Fatalf("atomic resolution failure declared a partial manifest: %#v", stream.state)
	}
	if stream.state.Finished == nil || stream.state.Finished.Outcome != ingestevent.RunFailed ||
		stream.state.Finished.ExitCode != ExitSource || stream.state.Finished.Total != 0 {
		t.Fatalf("unexpected source-failure terminal record: %#v", stream.state.Finished)
	}
	if len(stream.state.Diagnostics) != 1 || stream.state.Diagnostics[0].Code != ingest.FailureCodeSourceFailed ||
		stream.state.Diagnostics[0].Severity != ingestevent.SeverityError {
		t.Fatalf("source failure is not self-contained in stdout: %#v", stream.state.Diagnostics)
	}
	if strings.Contains(stderr.String(), "tamsin:") {
		t.Fatalf("JSON failure fell back to an unstructured command error: %s", stderr.String())
	}
}

func TestCLIDryRunFromConfig(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "fixture.mp4")
	if err := os.WriteFile(input, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	ffprobe := fakeMediaTool(t, directory)
	config := filepath.Join(directory, "config.yaml")
	// segment_duration 0 keeps this focused on configuration resolution rather
	// than pulling ffmpeg into the run.
	configBody := "format: json\ninput:\n  - " + input + "\ningest:\n  profile: preserve\n  dry_run: fast\n  segment_duration: 0\nmedia:\n  ffprobe: " + ffprobe + "\n"
	if err := os.WriteFile(config, []byte(configBody), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{"--config", config, "--log-level", "error"}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitOK {
		t.Fatalf("exit = %d, stderr = %s", code, stderr.String())
	}
	stream := decodeCLIIngestEventStream(t, stdout.Bytes())
	inputState := stream.state.Inputs[0]
	if stream.state.Hello == nil || stream.state.Hello.ResultSchemaVersion != ingest.ResultSchemaVersion ||
		stream.state.Hello.ToolVersion == "" || stream.state.Hello.ToolCommit == "" || stream.state.RunID == "" ||
		stream.state.Started == nil || stream.state.Started.Profile != "preserve" || stream.state.Started.ProfileVersion != "1" ||
		stream.state.Manifest == nil || stream.state.Manifest.TotalInputs != 1 || inputState == nil || inputState.Finished == nil ||
		inputState.Finished.Status != ingestevent.InputPlanned || inputState.Finished.RootFlowID == "" {
		t.Fatalf("unexpected reduced dry-run stream: hello=%#v started=%#v manifest=%#v input=%#v run=%#v",
			stream.state.Hello, stream.state.Started, stream.state.Manifest, inputState, stream.state.Finished)
	}
	if inputState.Finished.Profile != "preserve" || inputState.Finished.ProfileVersion != "1" {
		t.Fatalf("preserve profile did not survive configuration resolution: %#v", inputState.Finished)
	}
	if stream.state.Finished == nil || stream.state.Finished.Outcome != ingestevent.RunSucceeded ||
		stream.state.Finished.Succeeded != 1 || stream.state.Finished.BytesStaged != 7 {
		t.Fatalf("unexpected run summary: %#v", stream.state.Finished)
	}
}

func TestCLINamedProfileIsReportedInMachineOutput(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "fixture.mp4")
	if err := os.WriteFile(input, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--ffprobe", fakeMediaTool(t, directory), "--format", "json", "--log-level", "error",
		"--dry-run", "--profile", "preserve@1", input,
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitOK {
		t.Fatalf("exit = %d, stderr = %s", code, stderr.String())
	}
	stream := decodeCLIIngestEventStream(t, stdout.Bytes())
	inputState := stream.state.Inputs[0]
	if stream.state.Started == nil || stream.state.Started.Profile != "preserve" || stream.state.Started.ProfileVersion != "1" ||
		inputState == nil || inputState.Finished == nil || inputState.Finished.Profile != "preserve" ||
		inputState.Finished.ProfileVersion != "1" {
		t.Fatalf("profile was not retained across lifecycle events: started=%#v input=%#v", stream.state.Started, inputState)
	}
	plan := inputState.PlannedFlows[inputState.Finished.RootFlowID]
	if inputState.Started == nil || plan.Kind != ingestevent.FlowKindEssence || plan.Role != "video" ||
		!plan.Root || plan.ParentFlowID != "" || plan.Format != "urn:x-nmos:format:video" ||
		len(inputState.ObjectResults) != 1 {
		t.Fatalf("mono-essence lifecycle was not planned explicitly: started=%#v plan=%#v", inputState.Started, plan)
	}
	if object := inputState.ObjectResults[0].Result; object.Disposition != ingestevent.ObjectDispositionPlanned ||
		object.Verification != ingestevent.ObjectVerificationNotReached {
		t.Fatalf("dry-run Object terminal state = %#v, want planned and not_reached", object)
	}
}

func TestCLIProfileResolvesFromEnvironment(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "fixture.mp4")
	if err := os.WriteFile(input, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TAMSIN_INGEST_PROFILE", "preserve@1")
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--ffprobe", fakeMediaTool(t, directory), "--format", "json", "--log-level", "error",
		"--dry-run", input,
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitOK {
		t.Fatalf("exit = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"profile":"preserve","profile_version":"1"`) {
		t.Fatalf("environment profile was not resolved in output: %s", stdout.String())
	}
}

func TestCLIJournalWritesIndexedResultsAndFinalSummary(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	first := filepath.Join(directory, "first.mp4")
	second := filepath.Join(directory, "second.mp4")
	for _, filename := range []string{first, second} {
		if err := os.WriteFile(filename, []byte("fixture"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ffprobe := fakeMediaTool(t, directory)
	journalPath := filepath.Join(directory, "result.jsonl")
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--ffprobe", ffprobe, "--format", "json", "--log-level", "error", "--dry-run", "-d", "0",
		"--profile", "preserve", "--journal", journalPath, "-i", first, "-i", second,
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitOK {
		t.Fatalf("exit = %d; stdout = %s; stderr = %s", code, stdout.String(), stderr.String())
	}
	data, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 6 {
		t.Fatalf("journal has %d lines, want start, two Objects, two inputs and a summary: %s", len(lines), data)
	}
	seen := map[float64]bool{}
	objects := 0
	var runID string
	for index, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode journal line %d: %v", index, err)
		}
		if index == 0 {
			runID, _ = record["run_id"].(string)
		}
		if record["run_id"] != runID || record["schema_version"] != ingest.ResultSchemaVersion {
			t.Fatalf("journal metadata drifted on line %d: %#v", index, record)
		}
		if record["record_type"] == "input" {
			seen[record["index"].(float64)] = true
		}
		if record["record_type"] == "object" {
			objects++
		}
	}
	if !strings.Contains(lines[0], `"record_type":"start"`) || objects != 2 || !seen[0] || !seen[1] ||
		!strings.Contains(lines[len(lines)-1], `"record_type":"summary"`) ||
		!strings.Contains(lines[len(lines)-1], `"outcome":"completed"`) {
		t.Fatalf("journal is not index-complete: %s", data)
	}
	stream := decodeCLIIngestEventStream(t, stdout.Bytes())
	if stream.state.RunID == "" || stream.state.RunID != runID {
		t.Fatalf("stdout run_id %q does not correlate with journal run_id %q", stream.state.RunID, runID)
	}
	if stream.state.Finished == nil || stream.state.Finished.Total != 2 || stream.state.Finished.Succeeded != 2 {
		t.Fatalf("journaled run terminal summary = %#v", stream.state.Finished)
	}
}

func TestCLIFlagOverridesConfig(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "fixture.mp4")
	if err := os.WriteFile(input, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	ffprobe := fakeMediaTool(t, directory)
	config := filepath.Join(directory, "config.yaml")
	body := "format: json\ningest:\n  profile: preserve\n  dry_run: fast\nmedia:\n  ffprobe: " + ffprobe + "\n"
	if err := os.WriteFile(config, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{"--config", config, "--format", "human", "--log-level", "error", "-d", "0", input}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitOK {
		t.Fatalf("exit = %d, stderr = %s", code, stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "PLANNED - NO CHANGES MADE") {
		t.Fatalf("flag did not override configured JSON format: %q", stdout.String())
	}
}

func TestCLIFlagOverridesInvalidLowerPrecedenceTreatment(t *testing.T) {
	t.Parallel()
	config := writeTestConfig(t, t.TempDir(), `
ingest:
  profile: not-a-profile
`)
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--config", config, "--profile", "preserve", "--dry-run", filepath.Join(t.TempDir(), "missing.mp4"),
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitSource || strings.Contains(stderr.String(), "not-a-profile") {
		t.Fatalf("exit = %d, want source failure after valid override; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
}

// TestCLISegmentDurationShorthand keeps -d wired to the same setting as
// --segment-duration, so the documented shorthand cannot drift away from the
// long form.
func TestCLISegmentDurationShorthand(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "fixture.mp4")
	if err := os.WriteFile(input, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	ffprobe := fakeMediaTool(t, directory)

	for _, flag := range []string{"-d", "--segment-duration"} {
		t.Run(flag, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := Execute(context.Background(), []string{
				"--ffprobe", ffprobe, "--format", "json", "--log-level", "error",
				"--profile", "preserve", "--dry-run", flag, "0s", input,
			}, strings.NewReader(""), &stdout, &stderr)
			if code != ExitOK {
				t.Fatalf("%s: exit = %d, stderr = %s", flag, code, stderr.String())
			}
			if !strings.Contains(stdout.String(), `"status":"planned"`) {
				t.Fatalf("%s: unexpected output %q", flag, stdout.String())
			}
		})
	}
}

// TestCLIDefaultSegmentDurationIsTenSeconds pins the default. Segmenting by
// default makes ffmpeg a runtime requirement for every ingest, so a silent
// change here alters what a deployment needs installed.
func TestCLIDefaultSegmentDurationIsTenSeconds(t *testing.T) {
	t.Parallel()
	if defaultSegmentDuration != 10*time.Second {
		t.Fatalf("default segment duration = %s, want 10s", defaultSegmentDuration)
	}
}

func TestCLIRequiresProfileAndResolvesExplicitEssenceSegmentsProfile(t *testing.T) {
	t.Parallel()
	app := &application{v: viper.New()}
	app.configureDefaults()
	command := &cobra.Command{}
	raw := addIngestFlags(command)
	if _, _, err := app.ingestOptions(command, []string{"input.mp4"}, raw); err == nil || !strings.Contains(err.Error(), "profile is required") {
		t.Fatalf("ingest without a profile error = %v", err)
	}
	if err := command.Flags().Set("profile", ingest.ProfileEssenceSegments); err != nil {
		t.Fatal(err)
	}
	options, _, err := app.ingestOptions(command, []string{"input.mp4"}, raw)
	if err != nil {
		t.Fatal(err)
	}
	if options.profile != "essence-segments" || options.profileVersion != "1" ||
		options.segmentDuration != 10*time.Second || options.segmentFormat != "source" ||
		options.essenceStorage != "independent" {
		t.Fatalf("default resolved options = %#v", options)
	}
}

// TestCLIObjectInstanceLabelRules encodes ADR0042, which made label mandatory
// when registering an uncontrolled Object Instance. A controlled instance lives
// in a known Storage Backend and has nothing to label, so the two are rejected
// in opposite directions.
func TestCLIObjectInstanceLabelRules(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "uncontrolled-without-label",
			args: []string{"--url", "https://objects.example.test/o"},
			want: "--label is required with --url",
		},
		{
			name: "controlled-with-label",
			args: []string{"--storage-id", "6ba7b810-9dad-11d1-80b4-00c04fd430c8", "--label", "archive"},
			want: "--label may only be used with --url",
		},
		{
			name: "neither-target",
			args: nil,
			want: "provide exactly one of --storage-id or --url",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			arguments := append([]string{
				"--endpoint", "https://tams.example.test", "--auth", "none",
				"api", "object", "instance", "register", "object-1",
			}, testCase.args...)
			code := Execute(context.Background(), arguments, strings.NewReader(""), &stdout, &stderr)
			if code != ExitUsage {
				t.Fatalf("exit = %d, want %d; stderr = %s", code, ExitUsage, stderr.String())
			}
			if !strings.Contains(stderr.String(), testCase.want) {
				t.Fatalf("stderr = %q, want it to mention %q", stderr.String(), testCase.want)
			}
		})
	}
}

func TestCLIAPIService(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/service" {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"api_version":"8.1"}`)
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{"--endpoint", server.URL, "--auth", "none", "--format", "json", "api", "service"}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitOK {
		t.Fatalf("exit = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"api_version":"8.1"`) {
		t.Fatalf("unexpected output: %q", stdout.String())
	}
}

func TestCLIAPIFlowProfileOperations(t *testing.T) {
	t.Parallel()
	const profileID = "33333333-3333-4333-8333-333333333333"
	var created map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/service/profiles":
			if request.URL.Query().Get("format") != "urn:x-nmos:format:video" || request.URL.Query().Get("codec") != "video/h264" || request.URL.Query().Get("label") != "house" {
				http.Error(writer, "missing filters", http.StatusBadRequest)
				return
			}
			_, _ = io.WriteString(writer, `[{"id":"`+profileID+`","label":"house","flow_metadata":{"format":"urn:x-nmos:format:video"}}]`)
		case request.Method == http.MethodGet && request.URL.Path == "/service/profiles/"+profileID:
			_, _ = io.WriteString(writer, `{"id":"`+profileID+`","label":"house","flow_metadata":{"format":"urn:x-nmos:format:video"}}`)
		case request.Method == http.MethodPost && request.URL.Path == "/service/profiles/"+profileID:
			decoder := json.NewDecoder(request.Body)
			decoder.UseNumber()
			if err := decoder.Decode(&created); err != nil {
				t.Errorf("decode profile: %v", err)
			}
			writer.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(writer).Encode(created)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	run := func(arguments []string, input string) string {
		t.Helper()
		base := []string{"--endpoint", server.URL, "--auth", "none", "--format", "json", "api", "flow-profile"}
		var stdout, stderr bytes.Buffer
		if code := Execute(context.Background(), append(base, arguments...), strings.NewReader(input), &stdout, &stderr); code != ExitOK {
			t.Fatalf("%v exit = %d; stdout=%s stderr=%s", arguments, code, stdout.String(), stderr.String())
		}
		return stdout.String()
	}
	if output := run([]string{"list", "--format", "urn:x-nmos:format:video", "--codec", "video/h264", "--label", "house"}, ""); !strings.Contains(output, profileID) {
		t.Fatalf("list output = %s", output)
	}
	if output := run([]string{"get", profileID}, ""); !strings.Contains(output, `"label":"house"`) {
		t.Fatalf("get output = %s", output)
	}
	run([]string{"create", profileID}, `{"id":"ignored","label":"new","flow_metadata":{"format":"urn:x-nmos:format:video","essence_parameters":{"frame_width":9007199254740993}}}`)
	parameters := created["flow_metadata"].(map[string]any)["essence_parameters"].(map[string]any)
	if created["id"] != profileID || created["label"] != "new" || parameters["frame_width"] != json.Number("9007199254740993") {
		t.Fatalf("created profile = %#v", created)
	}
}

func TestCLINumericFlowProfileMatchesByJSONSemanticsBeforeMutation(t *testing.T) {
	const profileID = "60d9df18-6d9d-4b86-84bf-d1dcf14b3a28"
	var mismatch atomic.Bool
	var mutations, flowReads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.Method != http.MethodGet {
			mutations.Add(1)
			http.Error(writer, "mutation forbidden", http.StatusMethodNotAllowed)
			return
		}
		switch request.URL.Path {
		case "/service":
			_, _ = io.WriteString(writer, `{"api_version":"8.2","min_object_timeout":"300:0","min_presigned_url_timeout":"30:0"}`)
		case "/service/storage-backends":
			_, _ = io.WriteString(writer, `[{"id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8","default_storage":true,"store_type":"memory"}]`)
		case "/service/profiles/" + profileID:
			numerator := "25"
			if mismatch.Load() {
				numerator = "24"
			}
			_, _ = io.WriteString(writer, `{"id":"`+profileID+`","label":"numeric video","flow_metadata":{`+
				`"format":"urn:x-nmos:format:video","codec":"video/h264","container":"video/mp4",`+
				`"essence_parameters":{"frame_width":64,"frame_height":64,"frame_rate":{"numerator":`+numerator+`,"denominator":1}}}}`)
		default:
			if strings.HasPrefix(request.URL.Path, "/flows/") {
				flowReads.Add(1)
			}
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	directory := t.TempDir()
	input := filepath.Join(directory, "fixture.mp4")
	// Supply an ISO BMFF signature containing an MP4 compatible brand so the
	// content detector and the fake probe both describe the fixture as MP4.
	if err := os.WriteFile(input, []byte("\x00\x00\x00\x18ftypisom\x00\x00\x02\x00isommp41"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := []string{
		"--endpoint", server.URL, "--auth", "none", "--retries", "0",
		"--format", "json", "--progress", "none", "--log-level", "error",
		"--profile", "preserve", "--ffprobe", fakeMediaTool(t, directory),
		"--tams-flow-profile", profileID, "--input", input,
	}

	var stdout, stderr bytes.Buffer
	matching := append([]string{"--dry-run=exact"}, base...)
	if code := Execute(context.Background(), matching, strings.NewReader(""), &stdout, &stderr); code != ExitOK {
		t.Fatalf("matching Profile exit = %d; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	state := decodeCLIIngestEventStream(t, stdout.Bytes()).state
	inputState := state.Inputs[0]
	foundProfile := false
	for _, flow := range inputState.PlannedFlows {
		foundProfile = foundProfile || flow.TAMSFlowProfileID == profileID
	}
	if !foundProfile || inputState.Finished == nil || inputState.Finished.Status != ingestevent.InputPlanned {
		t.Fatalf("matching Profile was not retained: %#v", inputState)
	}

	mismatch.Store(true)
	stdout.Reset()
	stderr.Reset()
	if code := Execute(context.Background(), base, strings.NewReader(""), &stdout, &stderr); code != ExitPartial {
		t.Fatalf("mismatching Profile exit = %d, want %d; stdout=%s stderr=%s", code, ExitPartial, stdout.String(), stderr.String())
	}
	state = decodeCLIIngestEventStream(t, stdout.Bytes()).state
	inputState = state.Inputs[0]
	if inputState.Finished == nil || inputState.Finished.ErrorCode != ingest.FailureCodeFlowPlanFailed ||
		inputState.Finished.Status != ingestevent.InputFailed {
		t.Fatalf("mismatching Profile result = %#v", inputState.Finished)
	}
	if mutations.Load() != 0 || flowReads.Load() != 0 {
		t.Fatalf("Profile mismatch crossed planning boundary: mutations=%d flow_reads=%d", mutations.Load(), flowReads.Load())
	}
}

func TestCLIAPIStorageAllocationRequiresOneMode(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name      string
		args      []string
		wantIDs   bool
		wantLimit bool
	}{
		{name: "object IDs", args: []string{"--object-id", "object-1"}, wantIDs: true},
		{name: "server limit", args: []string{"--limit", "2"}, wantLimit: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var requestBody map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodPost || request.URL.Path != "/flows/flow/storage" {
					http.NotFound(writer, request)
					return
				}
				if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
					t.Errorf("decode request: %v", err)
				}
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(writer, `{"media_objects":[]}`)
			}))
			defer server.Close()

			arguments := []string{"--endpoint", server.URL, "--auth", "none", "--format", "json", "api", "storage", "allocate", "flow"}
			arguments = append(arguments, testCase.args...)
			var stdout, stderr bytes.Buffer
			if code := Execute(context.Background(), arguments, strings.NewReader(""), &stdout, &stderr); code != ExitOK {
				t.Fatalf("exit = %d; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			_, hasIDs := requestBody["object_ids"]
			_, hasLimit := requestBody["limit"]
			if hasIDs != testCase.wantIDs || hasLimit != testCase.wantLimit {
				t.Fatalf("request body = %#v", requestBody)
			}
		})
	}
}

func TestCLIAPIStorageAllocationSupportsTAMS82Options(t *testing.T) {
	t.Parallel()
	var requestBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/flows/flow/storage" {
			http.NotFound(writer, request)
			return
		}
		if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(writer, `{"media_objects":[]}`)
	}))
	defer server.Close()

	arguments := []string{
		"--endpoint", server.URL, "--auth", "none", "--format", "json",
		"api", "storage", "allocate", "flow", "--object-id", "object-1",
		"--content-type", "video/mp4", "--presigned=false",
	}
	var stdout, stderr bytes.Buffer
	if code := Execute(context.Background(), arguments, strings.NewReader(""), &stdout, &stderr); code != ExitOK {
		t.Fatalf("exit = %d; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if requestBody["content_type"] != "video/mp4" || requestBody["presigned"] != false {
		t.Fatalf("request body = %#v", requestBody)
	}
}

func TestCLIAPIStorageAllocationRejectsInvalidModesBeforeRequest(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name string
		args []string
		want string
	}{
		{name: "neither", want: "provide exactly one"},
		{name: "both", args: []string{"--object-id", "object-1", "--limit", "2"}, want: "provide exactly one"},
		{name: "zero limit", args: []string{"--limit", "0"}, want: "--limit must be positive"},
		{name: "negative limit", args: []string{"--limit", "-1"}, want: "--limit must be positive"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
			defer server.Close()
			arguments := []string{"--endpoint", server.URL, "--auth", "none", "api", "storage", "allocate", "flow"}
			arguments = append(arguments, testCase.args...)
			var stdout, stderr bytes.Buffer
			code := Execute(context.Background(), arguments, strings.NewReader(""), &stdout, &stderr)
			if code != ExitUsage || !strings.Contains(stderr.String(), testCase.want) || requests.Load() != 0 {
				t.Fatalf("exit = %d; requests=%d stdout=%s stderr=%s", code, requests.Load(), stdout.String(), stderr.String())
			}
		})
	}
}

func TestCLIAPISegmentListDownloadURLsAreOptIn(t *testing.T) {
	t.Parallel()
	const downloadURL = "https://storage.example.test/object?X-Amz-Signature=usable-secret"
	for _, testCase := range []struct {
		name         string
		extraArgs    []string
		response     string
		wantQuery    string
		wantDownload bool
	}{
		{
			name:      "lean default",
			response:  `[{"object_id":"object","timerange":"[0:0_1:0)","get_urls":[{"url":"` + downloadURL + `"}]}]`,
			wantQuery: "accept_get_urls=&limit=1000&presigned=false",
		},
		{
			name:         "download URL opt-in",
			extraArgs:    []string{"--include-download-urls"},
			response:     `[{"object_id":"object","timerange":"[0:0_1:0)","get_urls":[{"url":"` + downloadURL + `"}]}]`,
			wantQuery:    "limit=1000&presigned=true&verbose_storage=true",
			wantDownload: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var rawQuery string
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodGet || request.URL.Path != "/flows/flow/segments" {
					http.NotFound(writer, request)
					return
				}
				rawQuery = request.URL.RawQuery
				writer.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(writer, testCase.response)
			}))
			defer server.Close()

			arguments := []string{
				"--endpoint", server.URL, "--auth", "none", "--format", "json", "--log-level", "debug",
				"api", "segment", "list", "flow",
			}
			arguments = append(arguments, testCase.extraArgs...)
			var stdout, stderr bytes.Buffer
			code := Execute(context.Background(), arguments, strings.NewReader(""), &stdout, &stderr)
			if code != ExitOK {
				t.Fatalf("exit = %d; stdout = %s; stderr = %s", code, stdout.String(), stderr.String())
			}
			if rawQuery != testCase.wantQuery {
				t.Fatalf("query = %q, want exactly %q", rawQuery, testCase.wantQuery)
			}
			if strings.Contains(stdout.String(), downloadURL) != testCase.wantDownload {
				t.Fatalf("stdout download URL presence = %t, want %t: %s",
					strings.Contains(stdout.String(), downloadURL), testCase.wantDownload, stdout.String())
			}
			if strings.Contains(stderr.String(), downloadURL) {
				t.Fatalf("diagnostic log exposed a usable storage URL: %s", stderr.String())
			}
		})
	}
}

func TestCLIRawAPINoContentWritesSuccessJSON(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodDelete || request.URL.Path != "/resource" {
			http.NotFound(writer, request)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--endpoint", server.URL, "--auth", "none", "--format", "json", "api", "request", "DELETE", "/resource",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitOK {
		t.Fatalf("exit = %d; stdout = %s; stderr = %s", code, stdout.String(), stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != `{"status":"success"}` {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestCLITypedAPIRequestsRejectJSONNullBeforeRequest(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name string
		args []string
	}{
		{name: "Flow", args: []string{"api", "flow", "put", "flow-id"}},
		{name: "Segment", args: []string{"api", "segment", "register", "flow-id"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				requests.Add(1)
			}))
			defer server.Close()
			arguments := []string{"--endpoint", server.URL, "--auth", "none"}
			arguments = append(arguments, testCase.args...)
			var stdout, stderr bytes.Buffer
			code := Execute(context.Background(), arguments, strings.NewReader("null\n"), &stdout, &stderr)
			if code != ExitUsage || !strings.Contains(stderr.String(), "must be an object, not null") {
				t.Fatalf("exit = %d; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			if got := requests.Load(); got != 0 {
				t.Fatalf("requests = %d, want none for invalid typed JSON", got)
			}
		})
	}
}

func TestCLIUsageExitCode(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{"--concurrency", "0", "--dry-run", "missing"}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitUsage {
		t.Fatalf("exit = %d, want %d; stderr = %s", code, ExitUsage, stderr.String())
	}
}

func TestCLIArgumentFailuresUseUsageExitCode(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name string
		args []string
	}{
		{name: "extra API leaf argument", args: []string{"api", "service", "extra"}},
		{name: "missing API leaf argument", args: []string{"api", "flow", "get"}},
		{name: "extra config argument", args: []string{"config", "validate", "extra"}},
		{name: "extra doctor argument", args: []string{"doctor", "extra"}},
		{name: "missing completion argument", args: []string{"completion"}},
		{name: "unknown API group argument", args: []string{"api", "extra"}},
		{name: "unknown nested group argument", args: []string{"api", "flow", "extra"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			code := Execute(context.Background(), testCase.args, strings.NewReader(""), &stdout, &stderr)
			if code != ExitUsage {
				t.Fatalf("exit = %d, want %d; stdout=%s stderr=%s", code, ExitUsage, stdout.String(), stderr.String())
			}
		})
	}
}

func TestCLIJSONStartupFailureIsACompleteEventStream(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--format", "json", "--concurrency", "0", "--dry-run", "input.mp4",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitUsage {
		t.Fatalf("exit = %d, want %d; stdout = %s; stderr = %s", code, ExitUsage, stdout.String(), stderr.String())
	}
	stream := decodeCLIIngestEventStream(t, stdout.Bytes())
	if stream.state.Started == nil || stream.state.Manifest == nil || stream.state.Manifest.TotalInputs != 0 ||
		stream.state.Finished == nil || stream.state.Finished.Outcome != ingestevent.RunFailed ||
		stream.state.Finished.ExitCode != ExitUsage {
		t.Fatalf("unexpected startup failure stream: %#v", stream.state)
	}
	if len(stream.state.Diagnostics) != 1 || stream.state.Diagnostics[0].Code != ingest.FailureCodeConfigInvalid ||
		!stream.state.Diagnostics[0].ActionRequired {
		t.Fatalf("startup diagnostic is not independently actionable: %#v", stream.state.Diagnostics)
	}
	if strings.Contains(stderr.String(), "tamsin:") {
		t.Fatalf("complete JSON startup failure also emitted an unstructured fallback: %s", stderr.String())
	}
}

func TestCLIFlowMetadataOwnershipOverrideIsUsageError(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	input := filepath.Join(directory, "fixture.mp4")
	metadata := filepath.Join(directory, "metadata.json")
	if err := os.WriteFile(input, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metadata, []byte(`{"id":"3f79b0f7-43d2-47ac-b75e-785f4ca25b96"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--profile", "preserve", "--dry-run", "--flow-metadata", metadata, "-d", "0", "-i", input,
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitUsage {
		t.Fatalf("exit = %d, want %d; stdout = %s; stderr = %s", code, ExitUsage, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "/id") || !strings.Contains(stderr.String(), "--flow-id") {
		t.Fatalf("stderr = %q, want JSON path /id and --flow-id action", stderr.String())
	}
}

func TestCLIRejectsUnsafeCapacityOptions(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name string
		args []string
		want string
	}{
		{name: "zero input limit", args: []string{"--max-inputs", "0"}, want: "max-inputs must be positive"},
		{name: "zero staging budget", args: []string{"--staging-byte-budget", "0"}, want: "must be positive"},
		{name: "unknown staging unit", args: []string{"--staging-byte-budget", "10parsecs"}, want: "invalid staging byte budget unit"},
		{name: "negative transfers", args: []string{"--transfers", "-1"}, want: "transfers must be between 0 and 256"},
		{name: "negative probe concurrency", args: []string{"--probe-concurrency", "-1"}, want: "probe-concurrency must be between 0 and 256"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			arguments := append(testCase.args, "--profile", "preserve", "--dry-run", "input.mp4")
			var stdout, stderr bytes.Buffer
			code := Execute(context.Background(), arguments, strings.NewReader(""), &stdout, &stderr)
			if code != ExitUsage {
				t.Fatalf("exit = %d, want %d; stderr = %s", code, ExitUsage, stderr.String())
			}
			if !strings.Contains(stderr.String(), testCase.want) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), testCase.want)
			}
		})
	}
}

func TestCLIMaxInputsStopsDirectoryExpansion(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	for _, name := range []string{"a.mxf", "b.mxf"} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte("media"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--profile", "preserve", "--dry-run", "--max-inputs", "1", directory,
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitSource {
		t.Fatalf("exit = %d, want %d; stderr = %s", code, ExitSource, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--max-inputs=1") {
		t.Fatalf("stderr = %q, want the effective expansion limit", stderr.String())
	}
}
func TestCLIDoctorUsesMediaExitCode(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--ffprobe", filepath.Join(t.TempDir(), "missing-ffprobe"),
		"--ffmpeg", filepath.Join(t.TempDir(), "missing-ffmpeg"),
		"--format", "json",
		"doctor",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitMedia {
		t.Fatalf("exit = %d, want %d; stdout = %s; stderr = %s", code, ExitMedia, stdout.String(), stderr.String())
	}
}
func TestCLIEmptySourceUsesSourceExitCode(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{"--profile", "preserve", "--dry-run", "-i", t.TempDir()}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitSource {
		t.Fatalf("exit = %d, want %d; stderr = %s", code, ExitSource, stderr.String())
	}
}

func TestCLIPreflightFailureUsesRemoteExitCode(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	filename := filepath.Join(t.TempDir(), "fixture.bin")
	if err := os.WriteFile(filename, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--profile", "preserve", "--endpoint", server.URL, "--auth", "none", "--retries", "0", "--log-level", "error", "-i", filename,
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitRemote {
		t.Fatalf("exit = %d, want %d; stdout = %s; stderr = %s", code, ExitRemote, stdout.String(), stderr.String())
	}
}

func TestCLIPerRequestPreflightDeadlineIsNotRunInterruption(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	defer server.Close()
	input := filepath.Join(t.TempDir(), "input.ts")
	if err := os.WriteFile(input, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"ingest", "--format", "json", "--progress", "none", "--log-level", "error",
		"--profile", "preserve", "--auth", "none", "--endpoint", server.URL, "--timeout", "10ms", "--retries", "0", "--input", input,
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitRemote {
		t.Fatalf("child deadline exit = %d, want %d; stdout=%s stderr=%s", code, ExitRemote, stdout.String(), stderr.String())
	}
	stream := decodeCLIIngestEventStream(t, stdout.Bytes())
	inputState := stream.state.Inputs[0]
	if stream.state.Cancellation != nil || stream.state.Finished == nil || stream.state.Finished.Outcome != ingestevent.RunFailed ||
		stream.state.Finished.ExitCode != ExitRemote || inputState == nil || inputState.Finished == nil ||
		inputState.Finished.ErrorCode != ingest.FailureCodePreflightFailed {
		t.Fatalf("child deadline was mislabeled as interruption: %#v", stream.state)
	}
}

func TestExplicitExitCodeOwnsWrappedDeadline(t *testing.T) {
	t.Parallel()
	if got := exitCode(withExit(ExitRemote, context.DeadlineExceeded)); got != ExitRemote {
		t.Fatalf("explicit remote exit wrapped around a child deadline = %d, want %d", got, ExitRemote)
	}
}

func TestCLIIngestFailureNeverReproducesUntrustedTAMSBody(t *testing.T) {
	const toxicBody = "peer-response-top-secret"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, toxicBody, http.StatusInternalServerError)
	}))
	defer server.Close()

	for _, format := range []string{"human", "json"} {
		t.Run(format, func(t *testing.T) {
			directory := t.TempDir()
			input := filepath.Join(directory, "fixture.bin")
			if err := os.WriteFile(input, []byte("fixture"), 0o600); err != nil {
				t.Fatal(err)
			}
			journalPath := filepath.Join(directory, "results.jsonl")
			var stdout, stderr bytes.Buffer
			code := Execute(context.Background(), []string{
				"--endpoint", server.URL, "--auth", "none", "--retries", "0",
				"--format", format, "--progress", "none", "--log-level", "error",
				"--profile", "preserve", "--journal", journalPath, "--input", input,
			}, strings.NewReader(""), &stdout, &stderr)
			if code != ExitRemote {
				t.Fatalf("exit = %d, want %d; stdout=%s stderr=%s", code, ExitRemote, stdout.String(), stderr.String())
			}

			journal, err := os.ReadFile(journalPath)
			if err != nil {
				t.Fatal(err)
			}
			for name, output := range map[string]string{
				"stdout": stdout.String(), "stderr": stderr.String(), "journal": string(journal),
			} {
				if strings.Contains(output, toxicBody) {
					t.Errorf("untrusted TAMS response body leaked to %s: %s", name, output)
				}
			}

			result := journalResultRecord(t, journal)
			failure, ok := result["failure"].(map[string]any)
			if !ok || failure["code"] != ingest.FailureCodePreflightFailed ||
				failure["message"] != ingest.FailureMessagePreflightFailed {
				t.Fatalf("journal failure is not a stable, safe value: %#v", result["failure"])
			}
			if _, leakedRawError := result["error"]; leakedRawError {
				t.Fatalf("journal retained the raw implementation error: %#v", result)
			}

			switch format {
			case "human":
				if !strings.Contains(stdout.String(), "INGEST FAILED") ||
					!strings.Contains(stdout.String(), ingest.FailureMessagePreflightFailed) {
					t.Fatalf("human receipt omitted the safe terminal failure: %s", stdout.String())
				}
				if strings.Contains(stderr.String(), "tamsin:") {
					t.Fatalf("human receipt was followed by a duplicate raw command footer: %s", stderr.String())
				}
			case "json":
				stream := decodeCLIIngestEventStream(t, stdout.Bytes())
				inputState := stream.state.Inputs[0]
				if inputState == nil || inputState.Finished == nil ||
					inputState.Finished.ErrorCode != ingest.FailureCodePreflightFailed ||
					inputState.Finished.Message != ingest.FailureMessagePreflightFailed {
					t.Fatalf("structured terminal failure is not stable and safe: %#v", inputState)
				}
				if strings.Contains(stderr.String(), "tamsin:") {
					t.Fatalf("complete NDJSON stream was followed by an unstructured footer: %s", stderr.String())
				}
			}
		})
	}
}

func journalResultRecord(t *testing.T, journal []byte) map[string]any {
	t.Helper()
	for lineNumber, line := range bytes.Split(bytes.TrimSpace(journal), []byte{'\n'}) {
		var record struct {
			RecordType string         `json:"record_type"`
			Result     map[string]any `json:"result"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decode journal line %d: %v", lineNumber+1, err)
		}
		if record.RecordType == "input" {
			return record.Result
		}
	}
	t.Fatalf("journal contains no terminal result: %s", journal)
	return nil
}

func TestCLIHTTPInputRetriesTransientFailure(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			http.Error(writer, "temporary", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(writer, "fixture")
	}))
	defer server.Close()
	directory := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--profile", "preserve", "--dry-run", "--retries", "1", "--ffprobe", fakeMediaTool(t, directory),
		"--format", "json", "--log-format", "json", "--log-level", "debug",
		"-d", "0", "-i", server.URL + "/fixture.mp4?signature=top-secret",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitOK {
		t.Fatalf("exit = %d; stdout = %s; stderr = %s", code, stdout.String(), stderr.String())
	}
	if attempts.Load() != 2 {
		t.Fatalf("HTTP attempts = %d, want 2", attempts.Load())
	}
	stream := decodeCLIIngestEventStream(t, stdout.Bytes())
	retryEvents := stream.eventsOfType(ingestevent.TypeRetryScheduled)
	if len(retryEvents) != 1 {
		t.Fatalf("retry events = %d, want 1: %s", len(retryEvents), stdout.String())
	}
	retry, ok := retryEvents[0].(ingestevent.RetryScheduled)
	if !ok {
		t.Fatalf("retry payload type = %T", retryEvents[0])
	}
	if retry.Operation != "source_request" || retry.Attempt != 2 || retry.MaxAttempts != 2 ||
		retry.StatusClass != "server_error" || stream.state.RetryCount != 1 ||
		stream.state.Finished == nil || stream.state.Finished.Retries != 1 {
		t.Fatalf("source retry is not represented in the replayable stream: retry=%#v state=%#v", retry, stream.state)
	}
	if strings.Contains(stderr.String(), "signature") || strings.Contains(stderr.String(), "top-secret") ||
		strings.Contains(stdout.String(), "signature") || strings.Contains(stdout.String(), "top-secret") {
		t.Fatalf("authenticated input detail leaked: stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}
func TestCLIExternalObjectInstanceRequiresLabel(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--endpoint", "https://tams.example.test", "api", "object", "instance", "register", "object",
		"--url", "https://objects.example.test/object",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitUsage {
		t.Fatalf("exit = %d, want %d; stdout = %s; stderr = %s", code, ExitUsage, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "--label is required") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestCLIRequiresExplicitOptInForAuthenticatedLoopbackHTTP(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.Header.Get("Authorization") != "Bearer loopback-secret" {
			t.Errorf("Authorization = %q", request.Header.Get("Authorization"))
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{}`)
	}))
	defer server.Close()

	arguments := []string{
		"--endpoint", server.URL, "--auth", "bearer", "--token", "loopback-secret",
		"--format", "json", "api", "service",
	}
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), arguments, strings.NewReader(""), &stdout, &stderr)
	if code != ExitAuth {
		t.Fatalf("without opt-in exit = %d, want %d; stdout = %s; stderr = %s", code, ExitAuth, stdout.String(), stderr.String())
	}
	if requests.Load() != 0 {
		t.Fatalf("server received %d request(s) before plaintext rejection", requests.Load())
	}
	if strings.Contains(stderr.String(), "loopback-secret") || !strings.Contains(stderr.String(), "HTTPS") {
		t.Fatalf("unsafe or unhelpful error: %s", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	arguments = append([]string{"--allow-insecure-auth-loopback"}, arguments...)
	code = Execute(context.Background(), arguments, strings.NewReader(""), &stdout, &stderr)
	if code != ExitOK {
		t.Fatalf("with opt-in exit = %d; stdout = %s; stderr = %s", code, stdout.String(), stderr.String())
	}
	if requests.Load() != 1 {
		t.Fatalf("server received %d request(s), want 1", requests.Load())
	}
}

func TestCLIPlaintextURLTokenErrorIsRedacted(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--endpoint", "http://service.example.test/tams?access_token=endpoint-secret",
		"--auth", "url-token", "api", "service",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitAuth {
		t.Fatalf("exit = %d, want %d; stdout = %s; stderr = %s", code, ExitAuth, stdout.String(), stderr.String())
	}
	if strings.Contains(stderr.String(), "endpoint-secret") || !strings.Contains(stderr.String(), "HTTPS") {
		t.Fatalf("unsafe or unhelpful error: %s", stderr.String())
	}
}

func TestCLIBasicErrorRedactsEncodedAuthorization(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "rejected "+request.Header.Get("Authorization"), http.StatusUnauthorized)
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"--allow-insecure-auth-loopback", "--endpoint", server.URL,
		"--auth", "basic", "--username", "basic-user", "--password", "basic-secret",
		"api", "service",
	}, strings.NewReader(""), &stdout, &stderr)
	if code != ExitRemote {
		t.Fatalf("exit = %d, want %d; stdout = %s; stderr = %s", code, ExitRemote, stdout.String(), stderr.String())
	}
	encoded := base64.StdEncoding.EncodeToString([]byte("basic-user:basic-secret"))
	if strings.Contains(stderr.String(), "basic-secret") || strings.Contains(stderr.String(), encoded) ||
		strings.Contains(stderr.String(), "rejected Basic") {
		t.Fatalf("basic credential or untrusted peer body leaked: %s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "401 Unauthorized") {
		t.Fatalf("safe HTTP status was omitted: %s", stderr.String())
	}
}

func fakeMediaTool(t *testing.T, directory string) string {
	t.Helper()
	filename := filepath.Join(directory, "fake-ffmpeg")
	script := `#!/bin/sh
if [ "$1" = "-version" ]; then
  echo "ffprobe version test"
  exit 0
fi
for arg in "$@"; do
  if [ "$arg" = "-show_frames" ]; then
    printf '%s\n' \
      'stream_index=0|best_effort_timestamp=0' \
      'stream_index=0|best_effort_timestamp=40' \
      'stream_index=0|best_effort_timestamp=80'
    exit 0
  fi
done
cat <<'EOF'
{"streams":[{"index":0,"codec_name":"h264","codec_type":"video","width":64,"height":64,"avg_frame_rate":"25/1","start_time":"0.0","duration":"1.0","disposition":{"attached_pic":0}}],"format":{"format_name":"mov,mp4","start_time":"0.0","duration":"1.0","size":"7","bit_rate":"56000"}}
EOF
`
	if err := os.WriteFile(filename, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return filename
}
