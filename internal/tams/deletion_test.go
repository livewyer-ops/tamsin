package tams

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
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
		{name: "missing location", body: `{}`, want: "without a Location"},
		{name: "malformed location", location: "https://%zz", body: `{}`, want: "not a valid URL"},
		{name: "cross-origin location", location: "https://attacker.example/request", body: `{}`, want: "not the configured endpoint"},
		{name: "outside API path", location: "/outside/request", body: `{}`, want: "outside the configured API path"},
		{name: "missing request ID", location: "/v8.1/flow-delete-requests/request", body: `{"flow_id":"` + flowID + `","timerange_to_delete":"[0:0_1:0)","delete_flow":false,"status":"done"}`, want: "has no ID"},
		{name: "wrong flow", location: "/v8.1/flow-delete-requests/request", body: `{"id":"request","flow_id":"wrong","timerange_to_delete":"[0:0_1:0)","delete_flow":false,"status":"done"}`, want: "targets flow"},
		{name: "wrong timerange", location: "/v8.1/flow-delete-requests/request", body: `{"id":"request","flow_id":"` + flowID + `","timerange_to_delete":"[1:0_2:0)","delete_flow":false,"status":"done"}`, want: "targets timerange"},
		{name: "deletes flow", location: "/v8.1/flow-delete-requests/request", body: `{"id":"request","flow_id":"` + flowID + `","timerange_to_delete":"[0:0_1:0)","delete_flow":true,"status":"done"}`, want: "unexpectedly deletes"},
		{name: "error status", location: "/v8.1/flow-delete-requests/request", body: `{"id":"request","flow_id":"` + flowID + `","timerange_to_delete":"[0:0_1:0)","delete_flow":false,"status":"error","error":{"summary":"storage failed"}}`, want: "storage failed"},
		{name: "unknown status", location: "/v8.1/flow-delete-requests/request", body: `{"id":"request","flow_id":"` + flowID + `","timerange_to_delete":"[0:0_1:0)","delete_flow":false,"status":"mystery"}`, want: "unknown status"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
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
	client := &Client{suppressErrorBody: true}
	requests := []DeletionRequest{
		{ID: "request", FlowID: secret, TimerangeToDelete: "[0:0_1:0)", Status: "done"},
		{ID: "request", FlowID: "flow", TimerangeToDelete: secret, Status: "done"},
		{ID: "request", FlowID: "flow", TimerangeToDelete: "[0:0_1:0)", Status: secret},
		{ID: "request", FlowID: "flow", TimerangeToDelete: "[0:0_1:0)", Status: "error", Error: json.RawMessage(`{"detail":"peer-secret"}`)},
	}
	for _, request := range requests {
		_, err := client.validateDeletionRequest(request, "flow", "[0:0_1:0)")
		if err == nil {
			t.Fatalf("validateDeletionRequest(%#v) unexpectedly succeeded", request)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("suppressed deletion error leaked peer detail: %v", err)
		}
	}
}

func TestDeletionRequestRedactsAndBoundsUnsuppressedDetail(t *testing.T) {
	t.Parallel()
	const secret = "peer-secret"
	client := &Client{redactValues: []string{secret}}
	detail := `{"detail":"` + secret + strings.Repeat("x", maxErrorBody) + `"}`
	_, err := client.validateDeletionRequest(DeletionRequest{
		ID: "request", FlowID: "flow", TimerangeToDelete: "[0:0_1:0)",
		Status: "error", Error: json.RawMessage(detail),
	}, "flow", "[0:0_1:0)")
	if err == nil {
		t.Fatal("validateDeletionRequest() unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), secret) || !strings.Contains(err.Error(), "REDACTED") ||
		!strings.Contains(err.Error(), "[detail truncated]") || len(err.Error()) > maxErrorBody+256 {
		t.Fatalf("unsafe or unbounded deletion detail: length=%d, error=%v", len(err.Error()), err)
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

	client, err := New(Config{Endpoint: server.URL, Timeout: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	client.deletePollInterval = time.Millisecond
	err = client.DeleteSegments(context.Background(), flowID, SegmentDeleteOptions{
		Timerange: timerange, ObjectID: objectID,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DeleteSegments() error = %v, want deadline exceeded", err)
	}
}
