package tams

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/netio"
	"github.com/livewyer-ops/tamsin/internal/observability"
)

func TestUploadStorageSHA256RecognizesOnlyStrongEvidence(t *testing.T) {
	t.Parallel()
	digest := bytes.Repeat([]byte{0x7f}, sha256.Size)
	encoded := base64.StdEncoding.EncodeToString(digest)
	want := hex.EncodeToString(digest)

	tests := []struct {
		name     string
		response http.Header
		request  http.Header
		want     string
		wantErr  bool
	}{
		{name: "S3 response", response: http.Header{"X-Amz-Checksum-Sha256": []string{encoded}}, want: want},
		{name: "content digest response", response: http.Header{"Content-Digest": []string{"sha-256=:" + encoded + ":"}}, want: want},
		{name: "legacy digest response", response: http.Header{"Digest": []string{"sha-512=ignored, sha-256=" + encoded}}, want: want},
		{name: "signed allocation request", request: http.Header{"X-Amz-Checksum-Sha256": []string{encoded}}, want: want},
		{name: "etag is not evidence", response: http.Header{"ETag": []string{"\"not-a-checksum\""}}},
		{name: "malformed evidence", response: http.Header{"Content-Digest": []string{"sha-256=:bad:"}}, wantErr: true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			got, err := uploadStorageSHA256(testCase.response, testCase.request)
			if (err != nil) != testCase.wantErr || got != testCase.want {
				t.Fatalf("uploadStorageSHA256() = %q, %v; want %q, error=%t", got, err, testCase.want, testCase.wantErr)
			}
		})
	}
}

func TestRegisterSegmentsReturnsStructuredPartialResult(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/flows/flow/segments" {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(BulkSegmentFailure{FailedSegments: []FailedSegment{
			{ObjectID: "object-b", Timerange: "[1:0_2:0)"},
			// An omitted timerange is authoritative for every request carrying
			// that Object ID.
			{ObjectID: "object-c"},
		}})
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	requests := []SegmentRequest{
		{ObjectID: "object-a", Timerange: "[0:0_1:0)"},
		{ObjectID: "object-b", Timerange: "[1:0_2:0)"},
		{ObjectID: "object-c", Timerange: "[2:0_3:0)"},
	}
	err = client.RegisterSegments(context.Background(), "flow", requests)
	var partial *PartialSegmentRegistrationError
	if !errors.As(err, &partial) {
		t.Fatalf("RegisterSegments() error = %T %v, want PartialSegmentRegistrationError", err, err)
	}
	if partial.Total != 3 || len(partial.FailedSegments) != 2 {
		t.Fatalf("partial result = %#v", partial)
	}
	if len(partial.RegisteredSegments) != 1 || partial.RegisteredSegments[0] != requests[0] {
		t.Fatalf("registered complement = %#v, want %#v", partial.RegisteredSegments, requests[:1])
	}
	if !strings.Contains(err.Error(), "object-b") || !strings.Contains(err.Error(), "object-c") {
		t.Fatalf("partial error does not retain failed Object IDs: %v", err)
	}
}

func TestClientIngestOperations(t *testing.T) {
	t.Parallel()
	var baseURL string
	var lock sync.Mutex
	uploaded := map[string][]byte{}
	segments := map[string]Segment{}
	var flowPuts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/v8.1/service":
			_, _ = io.WriteString(writer, `{"api_version":"8.1"}`)
		case request.Method == http.MethodGet && request.URL.Path == "/v8.1/service/storage-backends":
			_, _ = io.WriteString(writer, `[{"id":"storage","default_storage":true}]`)
		case request.Method == http.MethodPut && request.URL.Path == "/v8.1/flows/flow":
			if flowPuts.Add(1) == 1 {
				writer.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(writer, `{"id":"flow","source_id":"source"}`)
			} else {
				writer.WriteHeader(http.StatusNoContent)
			}
		case request.Method == http.MethodGet && request.URL.Path == "/v8.1/flows/flow":
			_, _ = io.WriteString(writer, `{"id":"flow","source_id":"source"}`)
		case request.Method == http.MethodPost && request.URL.Path == "/v8.1/flows/flow/storage":
			var body StorageRequest
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			writer.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(writer).Encode(StorageResponse{MediaObjects: []AllocatedObject{{
				ObjectID: body.ObjectIDs[0], PutURL: PresignedURL{URL: baseURL + "/upload/" + body.ObjectIDs[0], Headers: map[string]string{"Content-Type": "video/mp4"}},
			}}})
		case request.Method == http.MethodPut && strings.HasPrefix(request.URL.Path, "/upload/"):
			data, _ := io.ReadAll(request.Body)
			lock.Lock()
			uploaded[strings.TrimPrefix(request.URL.Path, "/upload/")] = data
			lock.Unlock()
			writer.WriteHeader(http.StatusOK)
		case request.Method == http.MethodPost && request.URL.Path == "/v8.1/flows/flow/segments":
			var body SegmentRequest
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			lock.Lock()
			segments[body.ObjectID] = Segment{ObjectID: body.ObjectID, Timerange: body.Timerange, GetURLs: []PresignedURL{{URL: baseURL + "/download/" + body.ObjectID}}}
			lock.Unlock()
			writer.WriteHeader(http.StatusCreated)
		case request.Method == http.MethodGet && request.URL.Path == "/v8.1/flows/flow/segments":
			lock.Lock()
			segment, exists := segments[request.URL.Query().Get("object_id")]
			lock.Unlock()
			if exists {
				_ = json.NewEncoder(writer).Encode([]Segment{segment})
			} else {
				_, _ = io.WriteString(writer, `[]`)
			}
		case request.Method == http.MethodDelete && request.URL.Path == "/v8.1/flows/flow/segments":
			timerange := request.URL.Query().Get("timerange")
			objectID := request.URL.Query().Get("object_id")
			if timerange == "" || objectID == "" {
				http.Error(writer, "timerange and object_id are required", http.StatusBadRequest)
				return
			}
			lock.Lock()
			for candidateID, segment := range segments {
				if segment.Timerange == timerange && candidateID == objectID {
					delete(segments, candidateID)
				}
			}
			lock.Unlock()
			// A slow delete answers 202 instead; both are accepted.
			writer.WriteHeader(http.StatusNoContent)
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/download/"):
			lock.Lock()
			data := uploaded[strings.TrimPrefix(request.URL.Path, "/download/")]
			lock.Unlock()
			_, _ = writer.Write(data)
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/v8.1/objects/"):
			if strings.Contains(request.RequestURI, "a%2Fb") {
				_, _ = io.WriteString(writer, `{"object_id":"a/b"}`)
			} else {
				_, _ = io.WriteString(writer, `{"object_id":"object"}`)
			}
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/instances"):
			var body ObjectInstanceRequest
			decodeErr := json.NewDecoder(request.Body).Decode(&body)
			controlled := body.StorageID == "storage" && body.URL == "" && body.Label == ""
			uncontrolled := body.StorageID == "" && body.URL == "https://objects.example.test/object" && body.Label == "archive"
			if decodeErr != nil || (!controlled && !uncontrolled) {
				http.Error(writer, "invalid Object instance request", http.StatusBadRequest)
				return
			}
			writer.WriteHeader(http.StatusCreated)
		case request.Method == http.MethodDelete && strings.HasSuffix(request.URL.Path, "/instances"):
			if request.URL.Query().Get("storage_id") != "storage" || request.URL.Query().Has("label") {
				http.Error(writer, "invalid Object instance query", http.StatusBadRequest)
				return
			}
			writer.WriteHeader(http.StatusNoContent)
		default:
			http.Error(writer, request.Method+" "+request.RequestURI, http.StatusNotFound)
		}
	}))
	baseURL = server.URL
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL + "/v8.1", Timeout: 5 * time.Second, Retries: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := client.Service(ctx); err != nil {
		t.Fatal(err)
	}
	backends, err := client.StorageBackends(ctx)
	if err != nil || len(backends) != 1 || !backends[0].DefaultStorage {
		t.Fatalf("StorageBackends() = %#v, %v", backends, err)
	}
	if _, err := client.PutFlow(ctx, "flow", Flow{"id": "flow", "source_id": "source"}); err != nil {
		t.Fatal(err)
	}
	if updated, err := client.PutFlow(ctx, "flow", Flow{"id": "flow", "source_id": "source"}); err != nil || updated != nil {
		t.Fatalf("updated PutFlow() = %#v, %v", updated, err)
	}
	if _, err := client.Flow(ctx, "flow"); err != nil {
		t.Fatal(err)
	}
	allocation, err := client.AllocateStorage(ctx, "flow", StorageRequest{ObjectIDs: []string{"object"}})
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(t.TempDir(), "object.bin")
	if err := os.WriteFile(filename, []byte("media-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	receipt, err := client.UploadFile(ctx, allocation.MediaObjects[0].PutURL, filename)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Bytes != 11 || receipt.SHA256 != "bd7aa67d0cee967e6fca8ef4917e3c70445a9cfe0f3d91ddd2eeff1bfe4b2069" {
		t.Fatalf("UploadFile() receipt = %#v", receipt)
	}
	if err := client.RegisterSegment(ctx, "flow", SegmentRequest{ObjectID: "object", Timerange: "[0:0_1:0)"}); err != nil {
		t.Fatal(err)
	}
	listed, err := client.Segments(ctx, "flow", "object")
	if err != nil || len(listed) != 1 {
		t.Fatalf("Segments() = %#v, %v", listed, err)
	}
	size, digest, err := client.DownloadDigest(ctx, listed[0].GetURLs[0])
	if err != nil || size != 11 || digest != "bd7aa67d0cee967e6fca8ef4917e3c70445a9cfe0f3d91ddd2eeff1bfe4b2069" {
		t.Fatalf("DownloadDigest() = %d, %q, %v", size, digest, err)
	}
	if _, err := client.Object(ctx, "a/b"); err != nil {
		t.Fatal(err)
	}
	// Retracting an unverified Segment removes it, and TAMS drops any Media
	// Object the removal leaves unreferenced.
	if err := client.DeleteSegments(ctx, "flow", SegmentDeleteOptions{
		Timerange: "[0:0_1:0)", ObjectID: "object",
	}); err != nil {
		t.Fatalf("DeleteSegments() = %v", err)
	}
	if remaining, err := client.Segments(ctx, "flow", "object"); err != nil || len(remaining) != 0 {
		t.Fatalf("Segments() after delete = %#v, %v", remaining, err)
	}
	// Re-register so the Object-instance assertions below still have a Segment.
	if err := client.RegisterSegment(ctx, "flow", SegmentRequest{ObjectID: "object", Timerange: "[0:0_1:0)"}); err != nil {
		t.Fatal(err)
	}
	if err := client.RegisterObjectInstance(ctx, "object", ObjectInstanceRequest{StorageID: "storage"}); err != nil {
		t.Fatal(err)
	}
	if err := client.RegisterObjectInstance(ctx, "object", ObjectInstanceRequest{
		URL: "https://objects.example.test/object", Label: "archive",
	}); err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteObjectInstance(ctx, "object", "storage", ""); err != nil {
		t.Fatal(err)
	}
}

func TestClientRejectsEndpointUserinfo(t *testing.T) {
	t.Parallel()
	if _, err := New(Config{Endpoint: "https://user:secret@example.test"}); err == nil || !strings.Contains(err.Error(), "userinfo") {
		t.Fatalf("New() error = %v, want userinfo rejection", err)
	}
}

func TestClientRetriesSafeOperations(t *testing.T) {
	t.Parallel()
	run := observability.New("3adb82ef-fc30-4972-ac9c-8f2f05f922ae", nil)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			http.Error(writer, "retry", http.StatusServiceUnavailable)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{}`)
	}))
	defer server.Close()
	client, err := New(Config{Endpoint: server.URL, Retries: 1, Timeout: time.Second, Observability: run})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Service(context.Background()); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
	if got := run.Snapshot().Retries; got != 1 {
		t.Fatalf("observed retries = %d, want 1", got)
	}
}
func TestClientRetriesObjectDownload(t *testing.T) {
	t.Parallel()
	run := observability.New("4167f56c-4134-478f-83eb-1f192f908aea", nil)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			http.Error(writer, "retry", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(writer, "verified")
	}))
	defer server.Close()
	client, err := New(Config{
		Endpoint: "https://tams.example.test", Retries: 1, Timeout: time.Second, Observability: run,
	})
	if err != nil {
		t.Fatal(err)
	}
	size, digest, err := client.DownloadDigest(context.Background(), PresignedURL{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 || size != 8 || len(digest) != 64 {
		t.Fatalf("attempts = %d, size = %d, digest = %q", attempts.Load(), size, digest)
	}
	if got := run.Snapshot().Retries; got != 1 {
		t.Fatalf("observed retries = %d, want 1", got)
	}
}

func TestRetryDiagnosticDoesNotExposePresignedRequest(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	transport := roundTripError(func(*http.Request) (*http.Response, error) {
		status, body := http.StatusOK, "verified"
		reason := "200 OK"
		if attempts.Add(1) == 1 {
			status, body = http.StatusServiceUnavailable, "provider body top-secret"
			reason = "503 Provider top-secret reason"
		}
		return &http.Response{
			StatusCode: status, Status: reason, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)),
		}, nil
	})
	var diagnostics bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&diagnostics, &slog.HandlerOptions{Level: slog.LevelDebug}))
	run := observability.New("b8572ea7-4b41-48fe-81cc-30e562607a2f", logger)
	client, err := New(Config{
		Endpoint: "https://tams.example.test", ExternalTransport: transport, Retries: 1, Observability: run,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = client.DownloadDigest(context.Background(), PresignedURL{
		URL: "https://alice:password@objects.example.test/object?X-Amz-Signature=top-secret",
		Headers: map[string]string{
			"Authorization": "Bearer top-secret",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	text := diagnostics.String()
	if !strings.Contains(text, `"operation":"object_verification"`) ||
		!strings.Contains(text, `"status_class":"server_error"`) {
		t.Fatalf("missing structured retry classification: %s", text)
	}
	for _, forbidden := range []string{
		"alice", "password", "objects.example.test", "X-Amz-Signature", "Authorization",
		"Bearer", "top-secret", "Provider",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("retry diagnostic leaked %q: %s", forbidden, text)
		}
	}
}

// TestPresignedTransfersDoNotRetryAfterTheirStartDeadline keeps retries from
// undoing lifetime-aware scheduling. The first attempt starts while the URL is
// valid, then the controlled clock advances by its complete lifetime while the
// transport fails. A second attempt would present an expired signature.
func TestPresignedTransfersDoNotRetryAfterTheirStartDeadline(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"upload", "download"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			run := observability.New("163eae35-ce88-490f-ae03-40ab7926ab51", nil)
			var attempts atomic.Int32
			now := time.Unix(0, 0)
			transport := roundTripError(func(*http.Request) (*http.Response, error) {
				attempts.Add(1)
				now = now.Add(30 * time.Second)
				return &http.Response{
					StatusCode: http.StatusServiceUnavailable,
					Status:     "503 Service Unavailable",
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("retry")),
				}, nil
			})
			client, err := New(Config{
				Endpoint: "https://tams.example.test", Transport: transport,
				ExternalTransport: transport, Retries: 1, Observability: run,
			})
			if err != nil {
				t.Fatal(err)
			}
			client.now = func() time.Time { return now }
			presigned := PresignedURL{
				URL: "https://storage.example.test/object", StartBefore: now.Add(30 * time.Second),
			}

			switch operation {
			case "upload":
				filename := filepath.Join(t.TempDir(), "object")
				if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
					t.Fatal(err)
				}
				if _, err = client.UploadFile(context.Background(), presigned, filename); err == nil {
					t.Fatal("expired upload URL was retried")
				}
			case "download":
				_, _, err = client.DownloadDigest(context.Background(), presigned)
				if err == nil {
					t.Fatal("expired download URL was retried")
				}
			}
			if !strings.Contains(err.Error(), "expired before another request attempt") {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := attempts.Load(); got != 1 {
				t.Fatalf("attempts = %d, want 1 fresh attempt", got)
			}
			if got := run.Snapshot().Retries; got != 0 {
				t.Fatalf("expired URL reported %d scheduled retries, want 0", got)
			}
		})
	}
}

func TestPresignedTransfersStillRetryWithinTheirStartDeadline(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"upload", "download"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			var attempts atomic.Int32
			transport := roundTripError(func(*http.Request) (*http.Response, error) {
				status, body := http.StatusOK, "verified"
				if attempts.Add(1) == 1 {
					status, body = http.StatusServiceUnavailable, "retry"
				}
				return &http.Response{
					StatusCode: status, Status: http.StatusText(status), Header: make(http.Header),
					Body: io.NopCloser(strings.NewReader(body)),
				}, nil
			})
			client, err := New(Config{
				Endpoint: "https://tams.example.test", Transport: transport,
				ExternalTransport: transport, Retries: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			presigned := PresignedURL{
				URL: "https://storage.example.test/object", StartBefore: time.Now().Add(time.Minute),
			}
			if operation == "upload" {
				filename := filepath.Join(t.TempDir(), "object")
				if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
					t.Fatal(err)
				}
				_, err = client.UploadFile(context.Background(), presigned, filename)
			} else {
				_, _, err = client.DownloadDigest(context.Background(), presigned)
			}
			if err != nil {
				t.Fatalf("fresh presigned %s did not retry: %v", operation, err)
			}
			if got := attempts.Load(); got != 2 {
				t.Fatalf("attempts = %d, want 2", got)
			}
		})
	}
}

// TestPresignedTransferMayFinishAfterItsStartDeadline distinguishes a request
// start limit from an absolute transfer deadline. Large healthy media may keep
// streaming after the signed URL's start window and must not be cut off.
func TestPresignedTransferMayFinishAfterItsStartDeadline(t *testing.T) {
	t.Parallel()
	now := time.Unix(0, 0)
	transport := roundTripError(func(*http.Request) (*http.Response, error) {
		now = now.Add(time.Minute)
		return &http.Response{
			StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader("verified")),
		}, nil
	})
	client, err := New(Config{
		Endpoint: "https://tams.example.test", ExternalTransport: transport,
	})
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return now }
	_, _, err = client.DownloadDigest(context.Background(), PresignedURL{
		URL: "https://storage.example.test/object", StartBefore: now.Add(30 * time.Second),
	})
	if err != nil {
		t.Fatalf("transfer that began while fresh was capped by URL lifetime: %v", err)
	}
}

func TestPresignedURLAcceptsBothHeaderShapes(t *testing.T) {
	t.Parallel()
	for _, payload := range []string{
		`{"url":"https://example.test","content-type":"video/mp2t"}`,
		`{"url":"https://example.test","headers":{"Content-Type":"video/mp4","x-test":"value"}}`,
	} {
		var parsed PresignedURL
		if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
			t.Fatal(err)
		}
		if parsed.Headers["Content-Type"] == "" {
			t.Fatalf("Content-Type missing for %s", payload)
		}
		if value, exists := parsed.Headers["X-Test"]; strings.Contains(payload, "x-test") && (!exists || value != "value") {
			t.Fatalf("headers were not canonicalized for %s: %#v", payload, parsed.Headers)
		}
	}
	var preferred PresignedURL
	if err := json.Unmarshal([]byte(`{"url":"https://example.test","content-type":"legacy/type","headers":{"content-type":"headers/type"}}`), &preferred); err != nil {
		t.Fatal(err)
	}
	if got := preferred.Headers["Content-Type"]; got != "headers/type" {
		t.Fatalf("headers Content-Type = %q, want headers object to override legacy member", got)
	}
	for _, payload := range []string{
		`{"url":"https://example.test","headers":{"X-Test":"one","x-test":"two"}}`,
		`{"url":"https://example.test","headers":{"bad header":"value"}}`,
	} {
		var rejected PresignedURL
		if err := json.Unmarshal([]byte(payload), &rejected); err == nil {
			t.Fatalf("ambiguous or invalid presigned headers accepted: %s", payload)
		}
	}
	parsed := PresignedURL{StartBefore: time.Now()}
	if err := json.Unmarshal([]byte(`{"url":"https://example.test"}`), &parsed); err != nil {
		t.Fatal(err)
	}
	if !parsed.StartBefore.IsZero() {
		t.Fatal("a local scheduling deadline survived decoding a different wire URL")
	}
	encoded, err := json.Marshal(PresignedURL{
		URL: "https://example.test", StartBefore: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "StartBefore") || strings.Contains(string(encoded), "start_before") {
		t.Fatalf("local scheduling deadline leaked into TAMS JSON: %s", encoded)
	}
}
func TestClientNetworkErrorsDoNotLeakTransportCredentials(t *testing.T) {
	t.Parallel()
	transport := roundTripError(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial https://example.test?access_token=top-secret")
	})
	client, err := New(Config{Endpoint: "https://example.test", Transport: transport, Retries: 0})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Service(context.Background())
	if err == nil {
		t.Fatal("Service() unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), "top-secret") {
		t.Fatalf("network error leaked credentials: %v", err)
	}
}
func TestClientResponseErrorsRedactConfiguredCredentials(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "credential top-secret was rejected", http.StatusUnauthorized)
	}))
	defer server.Close()
	client, err := New(Config{Endpoint: server.URL, RedactValues: []string{"top-secret"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Service(context.Background())
	if err == nil {
		t.Fatal("Service() unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), "top-secret") || !strings.Contains(err.Error(), "REDACTED") {
		t.Fatalf("response error was not redacted: %v", err)
	}
}
func TestClientCanSuppressOAuthResponseBodies(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "dynamic-access-token", http.StatusUnauthorized)
	}))
	defer server.Close()
	client, err := New(Config{Endpoint: server.URL, SuppressErrorBody: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Service(context.Background())
	if err == nil {
		t.Fatal("Service() unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), "dynamic-access-token") || !strings.Contains(err.Error(), "401") {
		t.Fatalf("response body suppression failed: %v", err)
	}
}

func TestClientErrorIgnoresUntrustedReasonPhrase(t *testing.T) {
	t.Parallel()
	transport := roundTripError(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusUnauthorized,
			Status:     "401 dynamic-access-token was rejected",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})
	client, err := New(Config{Endpoint: "https://example.test", Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Service(context.Background())
	if err == nil {
		t.Fatal("Service() unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), "dynamic-access-token") || !strings.Contains(err.Error(), "401 Unauthorized") {
		t.Fatalf("unsafe TAMS status error: %v", err)
	}
}

func TestCrossOriginStorageDoesNotReceiveTAMSCredentials(t *testing.T) {
	t.Parallel()
	uploadServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if authorization := request.Header.Get("Authorization"); authorization != "" {
			t.Fatalf("cross-origin upload received TAMS Authorization header %q", authorization)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer uploadServer.Close()
	apiServer := httptest.NewServer(http.NotFoundHandler())
	defer apiServer.Close()

	authenticated := roundTripError(func(request *http.Request) (*http.Response, error) {
		clone := request.Clone(request.Context())
		clone.Header = request.Header.Clone()
		clone.Header.Set("Authorization", "Bearer TAMS-secret")
		return http.DefaultTransport.RoundTrip(clone)
	})
	client, err := New(Config{Endpoint: apiServer.URL, Transport: authenticated})
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(t.TempDir(), "object.bin")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := client.UploadFile(context.Background(), PresignedURL{URL: uploadServer.URL + "/object"}, filename); err != nil {
		t.Fatal(err)
	}
}
func TestClientsDoNotFollowCredentialedRedirects(t *testing.T) {
	t.Parallel()
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		redirected.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/service" && request.Header.Get("Authorization") != "Bearer TAMS-secret" {
			t.Errorf("initial TAMS request Authorization = %q", request.Header.Get("Authorization"))
		}
		http.Redirect(writer, request, target.URL+"/captured", http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	authenticated := roundTripError(func(request *http.Request) (*http.Response, error) {
		clone := request.Clone(request.Context())
		clone.Header = request.Header.Clone()
		clone.Header.Set("Authorization", "Bearer TAMS-secret")
		return http.DefaultTransport.RoundTrip(clone)
	})
	client, err := New(Config{Endpoint: redirector.URL, Transport: authenticated})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Service(context.Background()); err == nil {
		t.Fatal("TAMS redirect unexpectedly succeeded")
	}
	filename := filepath.Join(t.TempDir(), "object.bin")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := client.UploadFile(context.Background(), PresignedURL{
		URL: redirector.URL + "/upload", Headers: map[string]string{"X-Storage-Secret": "secret"},
	}, filename); err == nil {
		t.Fatal("presigned redirect unexpectedly succeeded")
	}
	if redirected.Load() != 0 {
		t.Fatalf("credentialed redirects followed %d times", redirected.Load())
	}
}

type roundTripError func(*http.Request) (*http.Response, error)

func (function roundTripError) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

// TestTransfersOutliveTheMetadataTimeout covers the difference between a
// deadline that detects a stall and one that caps throughput.
//
// http.Client.Timeout includes reading or writing the body, so applying the
// metadata timeout to media meant a perfectly healthy transfer of a large
// Object failed once it outlived the clock. Bytes were arriving the whole time.
func TestTransfersOutliveTheMetadataTimeout(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		flusher, ok := writer.(http.Flusher)
		if !ok {
			t.Error("test server cannot stream")
			return
		}
		// Healthy but unhurried: ten chunks spread well past the metadata budget.
		for range 10 {
			_, _ = writer.Write([]byte("0123456789"))
			flusher.Flush()
			time.Sleep(30 * time.Millisecond)
		}
	}))
	defer server.Close()

	client, err := New(Config{
		Endpoint: server.URL, Timeout: 150 * time.Millisecond,
		TransferIdleTimeout: 100 * time.Millisecond, Retries: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	size, _, err := client.DownloadDigest(context.Background(), PresignedURL{URL: server.URL})
	if err != nil {
		t.Fatalf("a healthy transfer outliving the metadata timeout must still complete: %v", err)
	}
	if size != 100 {
		t.Fatalf("downloaded %d bytes, want 100", size)
	}
}

// TestTransferTimeoutIsHonouredWhenSet keeps the opt-in cap working, so an
// operator who does want a hard bound still gets one.
func TestTransferTimeoutIsHonouredWhenSet(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		flusher, _ := writer.(http.Flusher)
		for range 10 {
			_, _ = writer.Write([]byte("0123456789"))
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(30 * time.Millisecond)
		}
	}))
	defer server.Close()

	client, err := New(Config{
		Endpoint: server.URL, Timeout: time.Minute,
		TransferTimeout: 100 * time.Millisecond, Retries: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.DownloadDigest(context.Background(), PresignedURL{URL: server.URL}); err == nil {
		t.Fatal("an explicit transfer timeout must still bound a transfer")
	}
}

func TestVerificationDownloadRetriesAnIdleBodyAndNamesTheAttempts(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		attempts.Add(1)
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("prefix"))
		writer.(http.Flusher).Flush()
		<-request.Context().Done()
	}))
	defer server.Close()

	client, err := New(Config{
		Endpoint: server.URL, TransferIdleTimeout: 120 * time.Millisecond, Retries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = client.DownloadDigest(context.Background(), PresignedURL{URL: server.URL + "/object"})
	var idle *netio.IdleTimeoutError
	if !errors.As(err, &idle) {
		t.Fatalf("DownloadDigest error = %v, want idle timeout", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
	if !strings.Contains(err.Error(), "after 2 attempt(s)") {
		t.Fatalf("error = %q, want exact attempt count", err)
	}
}

func TestUploadRetriesWhenThePeerStopsReading(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	transport := roundTripError(func(request *http.Request) (*http.Response, error) {
		attempts.Add(1)
		buffer := make([]byte, 1)
		if _, err := request.Body.Read(buffer); err != nil {
			return nil, err
		}
		// A transport blocked writing the remainder is released by cancelling
		// the request context when no further bytes have moved.
		<-request.Context().Done()
		return nil, request.Context().Err()
	})
	client, err := New(Config{
		Endpoint: "https://tams.example.test", ExternalTransport: transport,
		TransferIdleTimeout: 120 * time.Millisecond, Retries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(t.TempDir(), "object.bin")
	if err := os.WriteFile(filename, []byte("a media object larger than one byte"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = client.UploadFile(context.Background(), PresignedURL{URL: "https://objects.example.test/upload"}, filename)
	var idle *netio.IdleTimeoutError
	if !errors.As(err, &idle) {
		t.Fatalf("UploadFile error = %v, want idle timeout", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
	if !strings.Contains(err.Error(), "upload failed after 2 attempt(s)") {
		t.Fatalf("error = %q, want upload phase and exact attempt count", err)
	}
}

// TestMetadataStillHasADeadline confirms the split did not simply remove the
// metadata bound: a service that accepts a connection and never answers must
// not hang the run.
func TestMetadataStillHasADeadline(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		<-release
	}))
	// Order matters: Close waits for outstanding handlers, so the handler has to
	// be released first or the two deadlock.
	defer server.Close()
	defer close(release)

	client, err := New(Config{Endpoint: server.URL, Timeout: 100 * time.Millisecond, Retries: 0})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := client.Service(context.Background()); err == nil {
		t.Fatal("a metadata request against an unresponsive service must time out")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("metadata request took %s to give up", elapsed)
	}
}

// TestBackoffIsJitteredWithinBounds covers the retry schedule under load.
//
// Transfers that fail together retry together: a store shedding load answers
// every in-flight request at once, so an unjittered backoff returns the whole
// set at the same instant and reproduces the burst that caused the shedding.
// The spread has to be real, and it also has to stay bounded -- half the
// computed delay is kept as a floor so a retry never becomes more aggressive
// than the schedule allows.
func TestBackoffIsJitteredWithinBounds(t *testing.T) {
	t.Parallel()
	for attempt := range 6 {
		base := 200 * time.Millisecond * time.Duration(1<<min(attempt, 4))
		seen := make(map[time.Duration]struct{})
		for range 200 {
			delay := backoffDelay(attempt, "")
			if delay < base/2 || delay >= base {
				t.Fatalf("attempt %d: delay %v outside [%v, %v)", attempt, delay, base/2, base)
			}
			seen[delay] = struct{}{}
		}
		if len(seen) < 2 {
			t.Fatalf("attempt %d: every delay was identical; retries stay synchronised", attempt)
		}
	}
}

// TestBackoffNeverPrecedesRetryAfter keeps the jitter from overriding the
// server. Retry-After is an instruction, not an estimate: returning before it
// elapses ignores what the store asked for, so jitter may only ever be added.
func TestBackoffNeverPrecedesRetryAfter(t *testing.T) {
	t.Parallel()
	for _, header := range []string{"1", "5", "30"} {
		seconds, err := strconv.Atoi(header)
		if err != nil {
			t.Fatal(err)
		}
		floor := time.Duration(seconds) * time.Second
		for range 200 {
			delay := backoffDelay(0, header)
			if delay < floor {
				t.Fatalf("Retry-After %ss produced a %v delay, which returns early", header, delay)
			}
			if delay >= floor+backoffJitter {
				t.Fatalf("Retry-After %ss produced a %v delay, beyond the jitter window", header, delay)
			}
		}
	}
}

// TestUploadReusesConnections covers the cost of not reading a response nobody
// wants. Go returns a connection to the pool only once its response body has
// been consumed to EOF, so closing an unread body forces the next Media Object
// to open a fresh connection -- and on TLS that is a full handshake per Object.
func TestUploadReusesConnections(t *testing.T) {
	t.Parallel()
	var (
		lock        sync.Mutex
		connections = make(map[string]struct{})
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		lock.Lock()
		connections[request.RemoteAddr] = struct{}{}
		lock.Unlock()
		_, _ = io.Copy(io.Discard, request.Body)
		// A store that answers an upload with a body is the case that matters;
		// an empty response leaves nothing to drain and would pass either way.
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(`{"status":"stored"}`))
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL, Timeout: 5 * time.Second, Retries: 1})
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(t.TempDir(), "object")
	if err := os.WriteFile(filename, []byte("media bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	const uploads = 10
	for range uploads {
		if _, err := client.UploadFile(context.Background(), PresignedURL{URL: server.URL + "/upload"}, filename); err != nil {
			t.Fatal(err)
		}
	}

	// The assertion is deliberately "fewer than one connection per upload"
	// rather than a proportion. A connection returns to the pool
	// asynchronously, so a tight loop on a busy machine dials more of them, and
	// a tighter bound here failed on a loaded host while the code was correct.
	//
	// Nothing is given up by loosening it. Without the drain the count is not
	// merely higher, it is exactly one per upload every time, because a body
	// left unread can never be pooled -- so this separates the two states
	// completely while staying indifferent to scheduling.
	lock.Lock()
	defer lock.Unlock()
	if len(connections) >= uploads {
		t.Fatalf("%d uploads opened %d connections, so none was reused; an undrained response body keeps them out of the pool",
			uploads, len(connections))
	}
}

// TestParseServiceLimits covers the lifetimes a service advertises. They are
// scheduling instructions: a Media Object is collected if it is not registered
// in time, and a presigned URL stops working when it expires, so misreading one
// is worse than not reading it.
func TestParseServiceLimits(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name     string
		document map[string]any
		object   time.Duration
		url      time.Duration
		wantErr  string
	}{
		{
			// The specification's stated minimums.
			name:     "the guaranteed minimums",
			document: map[string]any{"min_object_timeout": "300:0", "min_presigned_url_timeout": "30:0"},
			object:   5 * time.Minute, url: 30 * time.Second,
		},
		{
			name: "sub-second precision",
			document: map[string]any{
				"min_object_timeout": "300:500000000", "min_presigned_url_timeout": "30:500000000",
			},
			object: 300500 * time.Millisecond, url: 30500 * time.Millisecond,
		},
		{
			name:     "an absent limit",
			document: map[string]any{"name": "store"},
			wantErr:  "/min_object_timeout is required",
		},
		{
			// The URL field is conditional in the schema. In its absence the
			// client schedules against the pinned minimum rather than treating it
			// as unlimited.
			name:     "an absent presigned URL limit uses the pinned minimum",
			document: map[string]any{"min_object_timeout": "300:0"},
			object:   5 * time.Minute, url: 30 * time.Second,
		},
		{
			name: "values that are not timestamps",
			document: map[string]any{
				"min_object_timeout":        "300",
				"min_presigned_url_timeout": "30:not-a-number",
			},
			wantErr: "/min_object_timeout",
		},
		{
			name: "values that are not strings",
			document: map[string]any{
				"min_object_timeout":        300,
				"min_presigned_url_timeout": nil,
			},
			wantErr: "/min_object_timeout",
		},
		{
			name: "a present malformed presigned URL limit",
			document: map[string]any{
				"min_object_timeout": "300:0", "min_presigned_url_timeout": "30:not-a-number",
			},
			wantErr: "/min_presigned_url_timeout",
		},
		{
			name: "a present presigned URL limit below its minimum",
			document: map[string]any{
				"min_object_timeout": "300:0", "min_presigned_url_timeout": "29:999999999",
			},
			wantErr: "requires at least 30:0",
		},
		{
			// Nanoseconds outside their range make the whole value untrustworthy.
			name:     "nanoseconds beyond a second",
			document: map[string]any{"min_object_timeout": "300:1000000000", "min_presigned_url_timeout": "30:0"},
			wantErr:  "/min_object_timeout",
		},
		{
			name:     "a negative duration",
			document: map[string]any{"min_object_timeout": "-300:0", "min_presigned_url_timeout": "30:0"},
			wantErr:  "/min_object_timeout",
		},
		{
			name: "below specification minima",
			document: map[string]any{
				"min_object_timeout": "299:999999999", "min_presigned_url_timeout": "29:999999999",
			},
			wantErr: "requires at least 300:0",
		},
		{
			name: "presigned lifetime exceeds Object lifetime",
			document: map[string]any{
				"min_object_timeout": "300:0", "min_presigned_url_timeout": "301:0",
			},
			wantErr: "exceeds /min_object_timeout",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			limits, err := ParseServiceLimits(testCase.document)
			if testCase.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), testCase.wantErr) {
					t.Fatalf("ParseServiceLimits() error = %v, want containing %q", err, testCase.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseServiceLimits(): %v", err)
			}
			if limits.ObjectRegistration != testCase.object {
				t.Errorf("ObjectRegistration = %v, want %v", limits.ObjectRegistration, testCase.object)
			}
			if limits.PresignedURL != testCase.url {
				t.Errorf("PresignedURL = %v, want %v", limits.PresignedURL, testCase.url)
			}
		})
	}
}

// TestRetryAfterIsCapped covers a store asking a client to go away for longer
// than the client should wait.
//
// Retry-After is an instruction and it is honoured, but a command bounded by
// its own retry budget should not become unbounded because a store asked for an
// hour. Past a point the delay is worth less than the chance to fail and let the
// caller decide what to do.
func TestRetryAfterIsCapped(t *testing.T) {
	t.Parallel()
	for _, header := range []string{"3600", "86400"} {
		delay := backoffDelay(0, header)
		if delay > maxRetryAfter+backoffJitter {
			t.Fatalf("Retry-After %ss produced a %v delay, beyond the %v cap", header, delay, maxRetryAfter)
		}
		if delay < maxRetryAfter {
			t.Fatalf("Retry-After %ss produced %v, which is shorter than the cap it should have hit", header, delay)
		}
	}
	// A reasonable value is still honoured exactly, so the cap is a ceiling
	// rather than a replacement.
	if delay := backoffDelay(0, "5"); delay < 5*time.Second || delay >= 5*time.Second+backoffJitter {
		t.Fatalf("Retry-After 5s produced %v, want it honoured as asked", delay)
	}
}

func TestTransferRequestErrorPreservesParentCause(t *testing.T) {
	t.Parallel()
	parent, cancel := context.WithCancelCause(context.Background())
	want := errors.New("another transfer failed")
	watch := netio.NewIdleWatch(parent, time.Minute)
	cancel(want)
	<-watch.Context().Done()

	err := transferRequestError(parent, watch, http.MethodPut,
		"https://storage.example.test/object?signature=secret", context.Canceled)
	watch.Stop()
	if !errors.Is(err, want) {
		t.Fatalf("transferRequestError = %v, want parent cause %v", err, want)
	}
}
