package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/tams"
)

// The store returns other bytes than were uploaded, so the Segment is
// retracted. It accepts the deletion without naming a request to follow and
// goes on listing the Segment until the fourth confirming listing, which the
// client makes 1.75 seconds after the first. A deadline of one second ends
// the wait before that and leaves the Segment stranded; a longer one sees the
// retraction through.
func TestCLIDeletionTimeoutBoundsARetraction(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		timeout  string
		stranded uint64
		code     int
	}{
		{timeout: "1s", stranded: 1, code: ExitPartial},
		{timeout: "30s", stranded: 0, code: ExitPartial},
	} {
		t.Run(testCase.timeout, func(t *testing.T) {
			t.Parallel()
			const backendID = "2834cde7-b5db-47e3-9fdd-e793376565fe"
			var mu sync.Mutex
			flows := make(map[string]tams.Flow)
			var segments []tams.SegmentRequest
			var uploaded []byte
			var deletions, confirmations int
			var baseURL string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.URL.Path == "/api/service":
					_, _ = io.WriteString(w, `{"api_version":"8.2","min_object_timeout":"300:0"}`)
				case r.URL.Path == "/api/service/storage-backends":
					_, _ = io.WriteString(w, `[{"id":"`+backendID+`","default_storage":true,"store_type":"http_object_store"}]`)
				case r.URL.Path == "/media/object":
					if r.Method == http.MethodPut {
						uploaded, _ = io.ReadAll(r.Body)
						return
					}
					_, _ = w.Write(bytes.ToUpper(uploaded))
				case strings.HasSuffix(r.URL.Path, "/storage"):
					var request tams.StorageRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.ObjectIDs) != 1 {
						t.Errorf("storage request = %#v, %v", request, err)
						http.Error(w, "invalid allocation", http.StatusBadRequest)
						return
					}
					w.WriteHeader(http.StatusCreated)
					_ = json.NewEncoder(w).Encode(tams.StorageResponse{MediaObjects: []tams.AllocatedObject{{
						ObjectID: request.ObjectIDs[0], PutURL: tams.PresignedURL{URL: baseURL + "/media/object"},
					}}})
				case strings.HasSuffix(r.URL.Path, "/segments"):
					switch r.Method {
					case http.MethodPost:
						var segment tams.SegmentRequest
						if err := json.NewDecoder(r.Body).Decode(&segment); err != nil {
							t.Error(err)
						}
						segments = append(segments, segment)
						w.WriteHeader(http.StatusCreated)
					case http.MethodDelete:
						deletions++
						w.WriteHeader(http.StatusAccepted)
					default:
						if deletions > 0 {
							if confirmations++; confirmations >= 4 {
								segments = nil
							}
						}
						listed := make([]tams.Segment, 0, len(segments))
						for _, segment := range segments {
							item := tams.Segment{ObjectID: segment.ObjectID, Timerange: segment.Timerange, TSOffset: segment.TSOffset, ObjectTimerange: segment.ObjectTimerange}
							if !r.URL.Query().Has("accept_get_urls") {
								item.GetURLs = []tams.PresignedURL{{URL: baseURL + "/media/object"}}
							}
							listed = append(listed, item)
						}
						_ = json.NewEncoder(w).Encode(listed)
					}
				case strings.HasPrefix(r.URL.Path, "/api/sources/"):
					if r.Method == http.MethodPut {
						w.WriteHeader(http.StatusNoContent)
						return
					}
					_, _ = io.WriteString(w, `{"id":"`+strings.TrimPrefix(r.URL.Path, "/api/sources/")+`","tags":{}}`)
				case strings.HasPrefix(r.URL.Path, "/api/flows/"):
					if r.Method == http.MethodPut {
						var flow tams.Flow
						if err := json.NewDecoder(r.Body).Decode(&flow); err != nil {
							t.Error(err)
						}
						flows[r.URL.Path] = flow
						w.WriteHeader(http.StatusCreated)
					} else if flow, ok := flows[r.URL.Path]; ok {
						_ = json.NewEncoder(w).Encode(flow)
					} else {
						http.NotFound(w, r)
					}
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)
			baseURL = server.URL
			directory := t.TempDir()
			input := filepath.Join(directory, "fixture.mp4")
			if err := os.WriteFile(input, []byte("\x00\x00\x00\x18ftypisom\x00\x00\x02\x00isommp41"), 0o600); err != nil {
				t.Fatal(err)
			}
			args := []string{"ingest", "--input", input, "--profile", "preserve", "--verify=readback",
				"--endpoint", baseURL + "/api", "--allow-insecure-auth-loopback", "--ffprobe", fakeMediaTool(t, directory),
				"--format", "json", "--progress", "none", "--deletion-timeout", testCase.timeout}
			var stdout, stderr bytes.Buffer
			code := Execute(t.Context(), args, strings.NewReader(""), &stdout, &stderr)
			finished := decodeCLIIngestEventStream(t, stdout.Bytes()).state.Finished
			if code != testCase.code || finished == nil || finished.ObjectsStranded != testCase.stranded || finished.ObjectsRetracted != 1-testCase.stranded {
				t.Fatalf("exit = %d, run.finished = %+v; want exit %d and %d stranded\nstderr = %s", code, finished, testCase.code, testCase.stranded, &stderr)
			}
			// The client says how it will confirm a deletion it cannot follow.
			if !strings.Contains(stderr.String(), "confirming by segment absence") {
				t.Fatalf("the client's warning did not reach the run's log: %s", &stderr)
			}
		})
	}
}
