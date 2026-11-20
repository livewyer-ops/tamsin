package tams

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestDeleteSegmentsWaitsForTerminalAbsence(t *testing.T) {
	t.Parallel()
	const (
		flowID    = "00000000-0000-4000-8000-000000000001"
		objectID  = "00000000-0000-4000-8000-000000000002"
		timerange = "[0:0_1:0)"
	)
	var listings atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.Method {
		case http.MethodDelete:
			if request.URL.Query().Get("object_id") != objectID || request.URL.Query().Get("timerange") != timerange {
				http.Error(writer, "deletion was not exact", http.StatusBadRequest)
				return
			}
			writer.WriteHeader(http.StatusNoContent)
		case http.MethodGet:
			// A 204 is only an acknowledgement. Keep returning the target twice
			// to prove the client waits for observable absence.
			if listings.Add(1) < 3 {
				_ = json.NewEncoder(writer).Encode([]Segment{{ObjectID: objectID, Timerange: timerange}})
				return
			}
			_, _ = io.WriteString(writer, `[]`)
		default:
			http.Error(writer, request.Method, http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	client.deletePollInterval = time.Millisecond
	if err := client.DeleteSegments(context.Background(), flowID, SegmentDeleteOptions{
		Timerange: timerange, ObjectID: objectID,
	}); err != nil {
		t.Fatal(err)
	}
	if got := listings.Load(); got != 3 {
		t.Fatalf("Segment listings = %d, want 3 before terminal absence", got)
	}
}

func TestDeleteSegmentsReconcilesCommittedRequestAfterLostResponse(t *testing.T) {
	t.Parallel()
	const (
		flowID    = "00000000-0000-4000-8000-000000000001"
		objectID  = "00000000-0000-4000-8000-000000000002"
		timerange = "[0:0_1:0)"
	)
	var deletes atomic.Int32
	var listings atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodDelete:
			if deletes.Add(1) == 1 {
				writer.WriteHeader(http.StatusNoContent)
				return
			}
			http.NotFound(writer, request)
		case http.MethodGet:
			listings.Add(1)
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `[]`)
		default:
			http.Error(writer, request.Method, http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	base := server.Client().Transport
	var loseFirstDelete atomic.Bool
	transport := roundTripError(func(request *http.Request) (*http.Response, error) {
		response, err := base.RoundTrip(request)
		if err != nil || request.Method != http.MethodDelete || !loseFirstDelete.CompareAndSwap(false, true) {
			return response, err
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return nil, errors.New("response lost after server committed deletion")
	})
	client, err := New(Config{Endpoint: server.URL, Transport: transport, Retries: 1, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	client.deletePollInterval = time.Millisecond
	if err := client.DeleteSegments(context.Background(), flowID, SegmentDeleteOptions{
		Timerange: timerange, ObjectID: objectID,
	}); err != nil {
		t.Fatal(err)
	}
	if deletes.Load() != 2 || listings.Load() != 1 {
		t.Fatalf("deletes = %d, absence listings = %d; want 2 and 1", deletes.Load(), listings.Load())
	}
}

func TestDeleteSegmentsJoinsAmbiguousFailureWithFailedConfirmation(t *testing.T) {
	t.Parallel()
	const (
		flowID    = "00000000-0000-4000-8000-000000000001"
		objectID  = "00000000-0000-4000-8000-000000000002"
		timerange = "[0:0_1:0)"
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodDelete {
			http.Error(writer, "temporary failure", http.StatusServiceUnavailable)
			return
		}
		http.Error(writer, "confirmation unavailable", http.StatusInternalServerError)
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	client.deletePollInterval = time.Millisecond
	err = client.DeleteSegments(context.Background(), flowID, SegmentDeleteOptions{
		Timerange: timerange, ObjectID: objectID,
	})
	if !strings.Contains(err.Error(), "503 Service Unavailable") ||
		!strings.Contains(err.Error(), "500 Internal Server Error") ||
		!strings.Contains(err.Error(), "confirm segment deletion") {
		t.Fatalf("DeleteSegments() error = %v, want original and confirmation failures", err)
	}
	if strings.Contains(err.Error(), "still listed") {
		t.Fatalf("DeleteSegments() error = %v, which takes a failed listing for a Segment that is still there", err)
	}
}

func TestDeleteSegmentsDoesNotReconcileDefinitiveClientError(t *testing.T) {
	t.Parallel()
	var listings atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodDelete {
			http.Error(writer, "invalid deletion", http.StatusBadRequest)
			return
		}
		listings.Add(1)
		_, _ = io.WriteString(writer, `[]`)
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	err = client.DeleteSegments(context.Background(), "flow", SegmentDeleteOptions{
		Timerange: "[0:0_1:0)", ObjectID: "object",
	})
	if err == nil || !strings.Contains(err.Error(), "400 Bad Request") {
		t.Fatalf("DeleteSegments() error = %v, want definitive client error", err)
	}
	if listings.Load() != 0 {
		t.Fatalf("definitive client error triggered %d absence listing(s)", listings.Load())
	}
}

func TestDeleteSegmentsMonitorsAcceptedRequest(t *testing.T) {
	t.Parallel()
	const (
		flowID    = "00000000-0000-4000-8000-000000000001"
		objectID  = "00000000-0000-4000-8000-000000000002"
		timerange = "[0:0_1:0)"
	)
	var polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodDelete:
			writer.Header().Set("Location", "/v8.1/flow-delete-requests/request")
			writer.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(writer, `{"id":"request","flow_id":"`+flowID+`","timerange_to_delete":"`+timerange+`","delete_flow":false,"status":"created"}`)
		case request.Method == http.MethodGet && request.URL.Path == "/v8.1/flow-delete-requests/request":
			status := "started"
			if polls.Add(1) > 1 {
				status = "done"
			}
			_, _ = io.WriteString(writer, `{"id":"request","flow_id":"`+flowID+`","timerange_to_delete":"`+timerange+`","delete_flow":false,"status":"`+status+`"}`)
		case request.Method == http.MethodGet && request.URL.Path == "/v8.1/flows/"+flowID+"/segments":
			_, _ = io.WriteString(writer, `[]`)
		default:
			http.Error(writer, request.Method+" "+request.RequestURI, http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL + "/v8.1", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	client.deletePollInterval = time.Millisecond
	if err := client.DeleteSegments(context.Background(), flowID, SegmentDeleteOptions{
		Timerange: timerange, ObjectID: objectID,
	}); err != nil {
		t.Fatal(err)
	}
	if got := polls.Load(); got != 2 {
		t.Fatalf("deletion request polls = %d, want 2", got)
	}
}

// TestDeleteSegmentsConfirmsByAbsenceWhenTheRequestCannotBeFollowed covers a
// 202 whose deletion request TAMSin will not follow: no Location, or one that
// points off the configured API. Credentials go nowhere new, and the Segment's
// absence decides the outcome. A request that ends in error is likewise
// accepted once the Segment has gone.
func TestDeleteSegmentsConfirmsByAbsenceWhenTheRequestCannotBeFollowed(t *testing.T) {
	t.Parallel()
	const (
		flowID    = "00000000-0000-4000-8000-000000000001"
		objectID  = "00000000-0000-4000-8000-000000000002"
		timerange = "[0:0_1:0)"
	)
	for _, testCase := range []struct {
		name     string
		location string
		body     string
		warning  string
	}{
		{name: "missing location", body: `{}`, warning: "without a Location header"},
		{name: "malformed location", location: "https://%zz", body: `{}`, warning: "cannot be followed"},
		{name: "cross-origin location", location: "https://attacker.example/request", body: `{}`, warning: "cannot be followed"},
		{name: "outside API path", location: "/outside/request", body: `{}`, warning: "cannot be followed"},
		{name: "error status with the segment gone", location: "/v8.1/flow-delete-requests/request", body: `{"id":"request","flow_id":"` + flowID + `","timerange_to_delete":"[0:0_1:0)","delete_flow":false,"status":"error","error":{"summary":"storage failed"}}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var followed atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch {
				case request.Method == http.MethodDelete:
					if testCase.location != "" {
						writer.Header().Set("Location", testCase.location)
					}
					writer.WriteHeader(http.StatusAccepted)
					_, _ = io.WriteString(writer, testCase.body)
				case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/segments"):
					_, _ = io.WriteString(writer, `[]`)
				default:
					followed.Add(1)
					http.Error(writer, "unexpected request", http.StatusBadRequest)
				}
			}))
			defer server.Close()
			var logged bytes.Buffer
			client, err := New(Config{Endpoint: server.URL + "/v8.1", Timeout: time.Second,
				Logger: slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))})
			if err != nil {
				t.Fatal(err)
			}
			client.deletePollInterval = time.Millisecond
			if err := client.DeleteSegments(context.Background(), flowID, SegmentDeleteOptions{
				Timerange: timerange, ObjectID: objectID,
			}); err != nil {
				t.Fatalf("DeleteSegments() = %v, want success through absence", err)
			}
			if followed.Load() != 0 {
				t.Fatal("an unusable Location was followed")
			}
			if testCase.warning != "" && !strings.Contains(logged.String(), testCase.warning) {
				t.Fatalf("warning %q lacks %q", logged.String(), testCase.warning)
			}
		})
	}
}

func TestDeleteSegmentsRejectsUntrustworthyAcceptedRequests(t *testing.T) {
	t.Parallel()
	const (
		flowID    = "00000000-0000-4000-8000-000000000001"
		objectID  = "00000000-0000-4000-8000-000000000002"
		timerange = "[0:0_1:0)"
	)
	for _, testCase := range []struct {
		name     string
		location string
		body     string
		want     string
	}{
		{name: "missing request ID", location: "/v8.1/flow-delete-requests/request", body: `{"flow_id":"` + flowID + `","timerange_to_delete":"[0:0_1:0)","delete_flow":false,"status":"done"}`, want: "has no ID"},
		{name: "wrong flow", location: "/v8.1/flow-delete-requests/request", body: `{"id":"request","flow_id":"wrong","timerange_to_delete":"[0:0_1:0)","delete_flow":false,"status":"done"}`, want: "targets an unexpected flow"},
		{name: "wrong timerange", location: "/v8.1/flow-delete-requests/request", body: `{"id":"request","flow_id":"` + flowID + `","timerange_to_delete":"[1:0_2:0)","delete_flow":false,"status":"done"}`, want: "targets an unexpected timerange"},
		{name: "deletes flow", location: "/v8.1/flow-delete-requests/request", body: `{"id":"request","flow_id":"` + flowID + `","timerange_to_delete":"[0:0_1:0)","delete_flow":true,"status":"done"}`, want: "unexpectedly deletes"},
		{name: "error status while the segment remains", location: "/v8.1/flow-delete-requests/request", body: `{"id":"request","flow_id":"` + flowID + `","timerange_to_delete":"[0:0_1:0)","delete_flow":false,"status":"error","error":{"summary":"storage failed"}}`, want: "request failed"},
		{name: "unknown status", location: "/v8.1/flow-delete-requests/request", body: `{"id":"request","flow_id":"` + flowID + `","timerange_to_delete":"[0:0_1:0)","delete_flow":false,"status":"mystery"}`, want: "unknown status"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/segments") {
					// The Segment is still there, so absence cannot excuse the request.
					_ = json.NewEncoder(writer).Encode([]Segment{{ObjectID: objectID, Timerange: timerange}})
					return
				}
				if request.Method != http.MethodDelete {
					t.Errorf("unexpected request followed deletion Location: %s", request.URL)
					http.Error(writer, "unexpected request", http.StatusBadRequest)
					return
				}
				if testCase.location != "" {
					writer.Header().Set("Location", testCase.location)
				}
				writer.WriteHeader(http.StatusAccepted)
				_, _ = io.WriteString(writer, testCase.body)
			}))
			defer server.Close()

			client, err := New(Config{Endpoint: server.URL + "/v8.1", Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			client.deletePollInterval = time.Millisecond
			err = client.DeleteSegments(context.Background(), flowID, SegmentDeleteOptions{
				Timerange: timerange, ObjectID: objectID,
			})
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("DeleteSegments() error = %v, want %q", err, testCase.want)
			}
		})
	}
}

func TestDeletionRequestSuppressesUntrustedDetails(t *testing.T) {
	t.Parallel()
	const secret = "peer-secret"
	requests := []DeletionRequest{
		{ID: "request", FlowID: secret, TimerangeToDelete: "[0:0_1:0)", Status: "done"},
		{ID: "request", FlowID: "flow", TimerangeToDelete: secret, Status: "done"},
		{ID: "request", FlowID: "flow", TimerangeToDelete: "[0:0_1:0)", Status: secret},
		{ID: "request", FlowID: "flow", TimerangeToDelete: "[0:0_1:0)", Status: "error", Error: json.RawMessage(`{"detail":"peer-secret"}`)},
	}
	for _, request := range requests {
		_, err := validateDeletionRequest(request, "flow", "[0:0_1:0)")
		if err == nil {
			t.Fatalf("validateDeletionRequest(%#v) unexpectedly succeeded", request)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("deletion error leaked peer detail: %v", err)
		}
	}
}

func TestDeleteSegmentsUsesAbsenceWhenMonitorExpired(t *testing.T) {
	t.Parallel()
	const (
		flowID    = "00000000-0000-4000-8000-000000000001"
		objectID  = "00000000-0000-4000-8000-000000000002"
		timerange = "[0:0_1:0)"
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodDelete:
			writer.Header().Set("Location", "/v8.1/flow-delete-requests/expired")
			writer.WriteHeader(http.StatusAccepted)
		case request.URL.Path == "/v8.1/flow-delete-requests/expired":
			http.NotFound(writer, request)
		case request.Method == http.MethodGet:
			_, _ = io.WriteString(writer, `[]`)
		default:
			http.Error(writer, request.Method, http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL + "/v8.1", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	client.deletePollInterval = time.Millisecond
	if err := client.DeleteSegments(context.Background(), flowID, SegmentDeleteOptions{
		Timerange: timerange, ObjectID: objectID,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDeletionLocationAcceptsSameOriginAbsoluteURL(t *testing.T) {
	t.Parallel()
	client, err := New(Config{Endpoint: "https://tams.example.test/v8.1"})
	if err != nil {
		t.Fatal(err)
	}
	requestURL, err := client.resolve("flows/flow/segments")
	if err != nil {
		t.Fatal(err)
	}
	for _, location := range []string{
		"https://tams.example.test/v8.1/flow-delete-requests/request",
		"/v8.1/flow-delete-requests/request",
	} {
		got, err := client.apiReference(requestURL, location)
		if err != nil {
			t.Fatalf("apiReference(%q) = %v", location, err)
		}
		if got != "flow-delete-requests/request" {
			t.Fatalf("apiReference(%q) = %q", location, got)
		}
	}
}

func TestAPIReferenceRejectsRecursivelyEncodedLocationPath(t *testing.T) {
	t.Parallel()
	client, err := New(Config{Endpoint: "https://tams.example.test/v8.1"})
	if err != nil {
		t.Fatal(err)
	}
	requestURL, err := client.resolve("flows/flow/segments")
	if err != nil {
		t.Fatal(err)
	}
	for _, location := range []string{
		"/v8.1/%252e%252e/outside",
		"/v8.1/flow-delete-requests%252f..%252f..%252foutside",
		"/v8.1/flow-delete-requests%255c..%255c..%255coutside",
	} {
		if _, err := client.apiReference(requestURL, location); err == nil ||
			!strings.Contains(err.Error(), "ambiguous encoded traversal or separator") {
			t.Fatalf("apiReference(%q) error = %v, want recursive path rejection", location, err)
		}
	}
}

func TestDeleteSegmentsTimesOutWhileSegmentRemains(t *testing.T) {
	t.Parallel()
	const (
		flowID    = "00000000-0000-4000-8000-000000000001"
		objectID  = "00000000-0000-4000-8000-000000000002"
		timerange = "[0:0_1:0)"
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodDelete {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		_ = json.NewEncoder(writer).Encode([]Segment{{ObjectID: objectID, Timerange: timerange}})
	}))
	defer server.Close()

	// The deletion deadline, not the per-request metadata timeout, bounds
	// the wait for absence.
	client, err := New(Config{Endpoint: server.URL, Timeout: time.Second, DeletionTimeout: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	client.deletePollInterval = time.Millisecond
	started := time.Now()
	err = client.DeleteSegments(context.Background(), flowID, SegmentDeleteOptions{
		Timerange: timerange, ObjectID: objectID,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DeleteSegments() error = %v, want deadline exceeded", err)
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("the deletion deadline was not applied")
	}
	if client.DeletionTimeout() != 30*time.Millisecond {
		t.Fatalf("DeletionTimeout() = %v", client.DeletionTimeout())
	}
}

// TestUnfinishedDeletionSaysWhatWasLastSeen covers a deletion that its
// deadline ends. The error says what the store last showed, the polls grow
// further apart, the DELETE is sent no more often than the retries allow and
// a deletion request the service is still working on is not asked for again.
func TestUnfinishedDeletionSaysWhatWasLastSeen(t *testing.T) {
	t.Parallel()
	const (
		flowID    = "00000000-0000-4000-8000-000000000001"
		objectID  = "00000000-0000-4000-8000-000000000002"
		timerange = "[0:0_1:0)"
		pending   = `{"id":"request","flow_id":"` + flowID + `","timerange_to_delete":"` + timerange + `","delete_flow":false,"status":"started"}`
	)
	for _, tc := range []struct {
		name     string
		status   int
		retries  int
		deadline time.Duration
		deletes  int32
		seen     string
	}{
		{name: "acknowledged and still listed", status: http.StatusNoContent, deadline: 200 * time.Millisecond, deletes: 1, seen: "segment still listed after"},
		{name: "failing and still listed", status: http.StatusServiceUnavailable, retries: 1, deadline: 600 * time.Millisecond, deletes: 2, seen: "segment still listed after"},
		{name: "accepted and still pending", status: http.StatusAccepted, deadline: 200 * time.Millisecond, deletes: 1, seen: "wait for segment deletion request"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// The store answers at once on a clock that moves only while the
			// client waits, so the deadline always passes between requests.
			// Over a socket it can pass inside a listing, which is a
			// different error with a test of its own.
			synctest.Test(t, func(t *testing.T) {
				var deletes, polls atomic.Int32
				respond := func(status int, body string) *http.Response {
					return &http.Response{
						StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}},
						Body: io.NopCloser(strings.NewReader(body)),
					}
				}
				store := roundTripError(func(request *http.Request) (*http.Response, error) {
					switch {
					case request.Method == http.MethodDelete:
						deletes.Add(1)
						if tc.status != http.StatusAccepted {
							return respond(tc.status, ""), nil
						}
						response := respond(tc.status, pending)
						response.Header.Set("Location", "/flow-delete-requests/request")
						return response, nil
					case request.URL.Path == "/flow-delete-requests/request":
						polls.Add(1)
						return respond(http.StatusOK, pending), nil
					default:
						polls.Add(1)
						return respond(http.StatusOK, `[{"object_id":"`+objectID+`","timerange":"`+timerange+`"}]`), nil
					}
				})
				client, err := New(Config{
					Endpoint: "https://example.test", Transport: store,
					Timeout: time.Second, Retries: tc.retries, DeletionTimeout: tc.deadline,
				})
				if err != nil {
					t.Fatal(err)
				}
				client.deletePollInterval = time.Millisecond
				err = client.DeleteSegments(context.Background(), flowID, SegmentDeleteOptions{Timerange: timerange, ObjectID: objectID})
				if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), tc.seen) {
					t.Fatalf("DeleteSegments() error = %v, want the deadline and %q", err, tc.seen)
				}
				if deletes.Load() != tc.deletes {
					t.Fatalf("DELETE was sent %d times, want %d", deletes.Load(), tc.deletes)
				}
				// A poll every millisecond would be hundreds. Four come before
				// the interval reaches its sixteen, and one every sixteen after.
				if most := int32(5 + tc.deadline/(16*time.Millisecond)); polls.Load() < 3 || polls.Load() > most {
					t.Fatalf("the store was polled %d times, want between 3 and %d", polls.Load(), most)
				}
			})
		})
	}
}

// TestDeletionCutShortInsideAListingSaysTheListingFailed covers a deadline
// that passes while a listing is still being read, after an earlier one
// showed the Segment. The error names the listing as what failed and does
// not say the Segment was still there, which that listing never showed.
func TestDeletionCutShortInsideAListingSaysTheListingFailed(t *testing.T) {
	t.Parallel()
	const (
		flowID    = "00000000-0000-4000-8000-000000000001"
		objectID  = "00000000-0000-4000-8000-000000000002"
		timerange = "[0:0_1:0)"
	)
	// The store answers at once on a clock that moves only while the client
	// waits, and its second listing never answers, so the deadline always
	// passes inside that listing. Over a socket a slow start can spend the
	// deadline inside the first one.
	synctest.Test(t, func(t *testing.T) {
		var listings atomic.Int32
		store := roundTripError(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodDelete {
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: http.NoBody}, nil
			}
			if listings.Add(1) > 1 {
				<-request.Context().Done()
				return nil, request.Context().Err()
			}
			return &http.Response{
				StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}},
				Body: io.NopCloser(strings.NewReader(`[{"object_id":"` + objectID + `","timerange":"` + timerange + `"}]`)),
			}, nil
		})
		// The request timeout is the longer, so the deletion deadline is what
		// ends the second listing.
		client, err := New(Config{
			Endpoint: "https://example.test", Transport: store,
			Timeout: 10 * time.Second, DeletionTimeout: 150 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		client.deletePollInterval = time.Millisecond
		err = client.DeleteSegments(context.Background(), flowID, SegmentDeleteOptions{Timerange: timerange, ObjectID: objectID})
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "list segments to confirm deletion") {
			t.Fatalf("DeleteSegments() error = %v, want the deadline inside a named listing", err)
		}
		if strings.Contains(err.Error(), "still listed") {
			t.Fatalf("DeleteSegments() error = %v, which reports a Segment the last listing did not show", err)
		}
		if listings.Load() != 2 {
			t.Fatalf("the Segment was listed %d times, want 2", listings.Load())
		}
	})
}

// TestDeleteSegmentsNeedsAValidListingToConfirmAbsence keeps a listing that
// names nothing usable from standing in for the empty array that proves a
// Segment is gone.
func TestDeleteSegmentsNeedsAValidListingToConfirmAbsence(t *testing.T) {
	t.Parallel()
	const (
		flowID    = "00000000-0000-4000-8000-000000000001"
		objectID  = "00000000-0000-4000-8000-000000000002"
		timerange = "[0:0_1:0)"
	)
	for name, listing := range map[string]string{
		"empty body":                           "",
		"null":                                 "null",
		"entry without fields":                 "[{}]",
		"target with a malformed timerange":    `[{"object_id":"` + objectID + `","timerange":"0:0-1:0"}]`,
		"target without a timerange":           `[{"object_id":"` + objectID + `"}]`,
		"unnamed entry at the exact timerange": `[{"timerange":"` + timerange + `"}]`,
		"target with an unbounded timerange":   `[{"object_id":"` + objectID + `","timerange":"_"}]`,
		"target with an empty timerange":       `[{"object_id":"` + objectID + `","timerange":"()"}]`,
		"target with an exclusive start":       `[{"object_id":"` + objectID + `","timerange":"(1:0_2:0)"}]`,
		"target with a misspelt timestamp":     `[{"object_id":"` + objectID + `","timerange":"[02:0_3:0)"}]`,
		"target under keys in another case":    `[{"OBJECT_ID":"` + objectID + `","TIMERANGE":"[2:0_3:0)"}]`,
	} {
		for _, deleteStatus := range []int{http.StatusNoContent, http.StatusServiceUnavailable} {
			t.Run(name+"/"+http.StatusText(deleteStatus), func(t *testing.T) {
				t.Parallel()
				var listings atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					if request.Method == http.MethodDelete {
						writer.WriteHeader(deleteStatus)
						return
					}
					listings.Add(1)
					writer.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(writer, listing)
				}))
				defer server.Close()

				client, err := New(Config{Endpoint: server.URL, Timeout: time.Second})
				if err != nil {
					t.Fatal(err)
				}
				client.deletePollInterval = time.Millisecond
				err = client.DeleteSegments(context.Background(), flowID, SegmentDeleteOptions{
					Timerange: timerange, ObjectID: objectID,
				})
				if err == nil || !strings.Contains(err.Error(), "confirm segment deletion") {
					t.Fatalf("DeleteSegments() = %v, want an unconfirmed deletion", err)
				}
				if strings.Contains(err.Error(), "still listed") {
					t.Fatalf("DeleteSegments() = %v, which takes an unusable listing for a Segment that is still there", err)
				}
				if listings.Load() == 0 {
					t.Fatal("the confirmation listing was never requested")
				}
			})
		}
	}
}
