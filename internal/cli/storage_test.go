package cli

import (
	"bytes"
	"context"
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

func TestCLIIngestSupportsBothStorageURLModes(t *testing.T) {
	for _, presigned := range []bool{false, true} {
		name := "non-presigned"
		if presigned {
			name = "presigned"
		}
		t.Run(name, func(t *testing.T) {
			const backendID = "2834cde7-b5db-47e3-9fdd-e793376565fe"
			var mu sync.Mutex
			flows := make(map[string]tams.Flow)
			var segments []tams.SegmentRequest
			var uploaded []byte
			var uploads, downloads, deletions int
			var baseURL string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.Header.Get("Authorization") != "Bearer fixture-token" {
					t.Errorf("missing same-origin credentials for %s", r.URL.Path)
					http.Error(w, "unauthorised", http.StatusUnauthorized)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.URL.Path == "/api/service":
					service := map[string]any{"api_version": "8.2", "min_object_timeout": "300:0"}
					if presigned {
						service["min_presigned_url_timeout"] = "30:0"
					}
					_ = json.NewEncoder(w).Encode(service)
				case r.URL.Path == "/api/service/storage-backends":
					if r.URL.Query().Get("page") == "second" {
						_, _ = io.WriteString(w, `[{"id":"`+backendID+`","default_storage":true,"store_type":"http_object_store"}]`)
					} else {
						w.Header().Set("Link", `<?page=second>; rel=next`)
						_, _ = io.WriteString(w, `[{"id":"9cb30d91-e456-41f3-9398-bf726c749e96","tags":{"auth_classes":["production"]}}]`)
					}
				case r.URL.Path == "/media/object":
					if r.Method == http.MethodPut {
						uploads++
						uploaded, _ = io.ReadAll(r.Body)
					} else {
						downloads++
						_, _ = w.Write(uploaded)
					}
				case strings.HasSuffix(r.URL.Path, "/storage"):
					var request tams.StorageRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.ObjectIDs) != 1 || request.StorageID != backendID {
						t.Errorf("storage request = %#v, %v", request, err)
						http.Error(w, "invalid allocation", http.StatusBadRequest)
						return
					}
					w.WriteHeader(http.StatusCreated)
					_ = json.NewEncoder(w).Encode(tams.StorageResponse{MediaObjects: []tams.AllocatedObject{{
						ObjectID: request.ObjectIDs[0], Presigned: &presigned,
						PutURL: tams.PresignedURL{URL: baseURL + "/media/object"},
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
						segments = nil
						w.WriteHeader(http.StatusNoContent)
					default:
						listed := make([]tams.Segment, 0, len(segments))
						for _, segment := range segments {
							item := tams.Segment{ObjectID: segment.ObjectID, Timerange: segment.Timerange}
							if !r.URL.Query().Has("accept_get_urls") && (presigned || r.URL.Query().Get("presigned") != "true") {
								item.GetURLs = []tams.PresignedURL{{URL: baseURL + "/media/object", Presigned: presigned}}
								if presigned {
									// Direct storage access can require credentials TAMSin does not have.
									item.GetURLs = append([]tams.PresignedURL{{URL: baseURL + "/media/unavailable"}}, item.GetURLs...)
								}
							}
							listed = append(listed, item)
						}
						_ = json.NewEncoder(w).Encode(listed)
					}
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
			defer server.Close()
			baseURL = server.URL
			directory := t.TempDir()
			input := filepath.Join(directory, "fixture.mp4")
			if err := os.WriteFile(input, []byte("\x00\x00\x00\x18ftypisom\x00\x00\x02\x00isommp41"), 0o600); err != nil {
				t.Fatal(err)
			}
			args := []string{"ingest", "--input", input, "--profile", "preserve", "--verify=readback",
				"--endpoint", baseURL + "/api", "--auth", "bearer", "--token", "fixture-token",
				"--allow-insecure-auth-loopback", "--ffprobe", fakeMediaTool(t, directory), "--progress", "none"}
			var stdout, stderr bytes.Buffer
			if code := Execute(context.Background(), args, strings.NewReader(""), &stdout, &stderr); code != ExitOK {
				t.Fatalf("exit=%d stdout=%s stderr=%s", code, &stdout, &stderr)
			}
			mu.Lock()
			defer mu.Unlock()
			if uploads != 1 || downloads != 1 || deletions != 0 || len(segments) != 1 {
				t.Fatalf("uploads=%d downloads=%d deletions=%d segments=%d", uploads, downloads, deletions, len(segments))
			}
		})
	}
}
