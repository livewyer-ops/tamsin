package observability

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRetryEventHasOnlySafeFixedClasses(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	run := New("97bc2aa6-f947-4426-897f-8fcfc052cc60", logger)
	toxic := errors.New("GET https://alice:password@objects.example.test/file?X-Amz-Signature=secret Authorization: Bearer token: provider says nope")

	run.Retry(OperationObjectUpload, 2, 4, 0, toxic, 250*time.Millisecond)
	run.Retry(OperationTAMSMetadata, 3, 4, 503, nil, time.Second)

	text := output.String()
	for _, forbidden := range []string{
		"alice", "password", "objects.example.test", "X-Amz-Signature", "secret",
		"Authorization", "Bearer", "provider says nope", "Service Unavailable",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("retry diagnostic leaked %q: %s", forbidden, text)
		}
	}
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if len(lines) != 2 {
		t.Fatalf("retry records = %d, want 2: %s", len(lines), text)
	}
	first := decodeRecord(t, lines[0])
	if first["msg"] != "retry scheduled" || first["run_id"] != run.RunID() ||
		first["operation"] != "object_upload" || first["attempt"] != float64(2) ||
		first["max_attempts"] != float64(4) || first["status_class"] != "none" ||
		first["error_class"] != "transport" || first["backoff"] != float64(250_000_000) {
		t.Fatalf("first retry record = %#v", first)
	}
	second := decodeRecord(t, lines[1])
	if second["operation"] != "tams_metadata" || second["status_class"] != "server_error" ||
		second["error_class"] != "none" {
		t.Fatalf("second retry record = %#v", second)
	}
}

func TestRetryObserverReceivesTheCommittedSafeEvent(t *testing.T) {
	t.Parallel()
	run := New("fd186c0e-eeeb-4d26-848f-f4effdb8ba51", nil)
	var received []RetryEvent
	run.SetRetryObserver(func(event RetryEvent) {
		received = append(received, event)
	})
	run.Retry(OperationObjectVerification, 3, 5, 429,
		errors.New("https://user:secret@example.test/?token=hidden"), 750*time.Millisecond)

	if len(received) != 1 {
		t.Fatalf("observer events = %d, want 1", len(received))
	}
	event := received[0]
	if event.Operation != OperationObjectVerification || event.Attempt != 3 || event.MaxAttempts != 5 ||
		event.StatusClass != "throttled" || event.ErrorClass != "transport" ||
		event.Backoff != 750*time.Millisecond {
		t.Fatalf("observer event = %+v", event)
	}
	if metrics := run.Snapshot(); metrics.Retries != 1 {
		t.Fatalf("observer ran before retry metric was committed: %+v", metrics)
	}
}

func TestFailureEventIsCorrelatedAndSecretSafe(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	run := New("96322033-c15c-4945-82f9-12bb6cfe281e", slog.New(slog.NewJSONHandler(&output, nil)))
	run.Failure(errors.New("GET https://alice:password@example.test/media?signature=secret: provider rejected token"))

	text := output.String()
	for _, forbidden := range []string{"alice", "password", "example.test", "signature", "secret", "provider", "token"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("terminal failure diagnostic leaked %q: %s", forbidden, text)
		}
	}
	record := decodeRecord(t, strings.TrimSpace(text))
	if record["msg"] != "ingest run failed" || record["run_id"] != run.RunID() || record["error_class"] != "transport" {
		t.Fatalf("failure record = %#v", record)
	}
}

func TestMetricsAreConcurrentAndInvocationScoped(t *testing.T) {
	t.Parallel()
	run := New("d25ab986-94ae-4ca1-89af-cd4bf4faee4b", nil)
	const workers = 100
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			run.Staged(10)
			run.Uploaded(20)
			run.Verification(30, OutcomeVerified)
			run.Staged(1)
			run.Uploaded(2)
			run.Verification(3, OutcomeVerified)
			run.Retry(OperationSourceTransfer, 2, 4, 0, errors.New("private provider detail"), 0)
		}()
	}
	group.Wait()

	metrics := run.Snapshot()
	if metrics.BytesStaged != 1100 || metrics.BytesUploaded != 2200 || metrics.BytesVerified != 3300 ||
		metrics.Retries != workers || metrics.Verified != workers*2 || metrics.Retracted != 0 || metrics.Stranded != 0 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

func TestSnapshotContainsElapsedTimeAndOutcomes(t *testing.T) {
	t.Parallel()
	run := New("47f553fb-c362-447a-95bc-07f44302cf8e", nil)
	run.now = func() time.Time { return run.started.Add(2 * time.Second) }
	run.Staged(11)
	run.Uploaded(7)
	run.Verification(7, OutcomeVerified)
	run.Verification(3, OutcomeRetracted)
	run.Verification(5, OutcomeStranded)
	metrics := run.Snapshot()
	if metrics.Elapsed != 2*time.Second || metrics.BytesStaged != 11 || metrics.BytesUploaded != 7 ||
		metrics.BytesVerified != 7 || metrics.Verified != 1 || metrics.Retracted != 1 || metrics.Stranded != 1 {
		t.Fatalf("snapshot = %#v", metrics)
	}
}

func decodeRecord(t *testing.T, line string) map[string]any {
	t.Helper()
	var result map[string]any
	if err := json.Unmarshal([]byte(line), &result); err != nil {
		t.Fatalf("decode diagnostic %q: %v", line, err)
	}
	return result
}
