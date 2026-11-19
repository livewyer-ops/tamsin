package tams

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestAPICredentialsOnlyReachUnsignedSameOriginMedia pins the same-origin
// rule: the authenticated transport is used for an unsigned URL on the API
// origin and for nothing else. A presigned URL on the API origin must not have
// a bearer header or access_token added to the request it was signed for.
func TestAPICredentialsOnlyReachUnsignedSameOriginMedia(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		url       string
		presigned bool
		wantAPI   bool
	}{
		{name: "unsigned same-origin", url: "https://tams.example.test/media/object", wantAPI: true},
		{name: "presigned same-origin", url: "https://tams.example.test/media/object?X-Amz-Signature=sig", presigned: true},
		{name: "unsigned cross-origin", url: "https://objects.example.test/object"},
		{name: "presigned cross-origin", url: "https://objects.example.test/object?X-Amz-Signature=sig", presigned: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var apiCalls, externalCalls atomic.Int32
			respond := func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header),
					Body: io.NopCloser(strings.NewReader("verified")),
				}, nil
			}
			client, err := New(Config{
				Endpoint: "https://tams.example.test", TransferIdleTimeout: time.Minute,
				Transport: roundTripError(func(request *http.Request) (*http.Response, error) {
					apiCalls.Add(1)
					return respond(request)
				}),
				ExternalTransport: roundTripError(func(request *http.Request) (*http.Response, error) {
					externalCalls.Add(1)
					return respond(request)
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			location := PresignedURL{URL: test.url, Presigned: test.presigned}
			if _, _, err := client.DownloadDigest(context.Background(), location, 8); err != nil {
				t.Fatal(err)
			}
			filename := filepath.Join(t.TempDir(), "object")
			if err := os.WriteFile(filename, []byte("verified"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := client.UploadFile(context.Background(), location, filename); err != nil {
				t.Fatal(err)
			}
			wantAPI, wantExternal := int32(0), int32(2)
			if test.wantAPI {
				wantAPI, wantExternal = 2, 0
			}
			if apiCalls.Load() != wantAPI || externalCalls.Load() != wantExternal {
				t.Fatalf("authenticated transport used %d times and external %d; want %d and %d",
					apiCalls.Load(), externalCalls.Load(), wantAPI, wantExternal)
			}
		})
	}
}

// TestUploadFollowsHeaderInstructionsWithoutInventingAType covers two halves
// of the HTTP request instruction contract: every returned header reaches the
// storage request, and no Content-Type is made up when none was returned. The
// caller supplies the Flow's container in that case.
func TestUploadFollowsHeaderInstructionsWithoutInventingAType(t *testing.T) {
	t.Parallel()
	var seen atomic.Pointer[http.Header]
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		header := request.Header.Clone()
		seen.Store(&header)
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, err := New(Config{Endpoint: "https://tams.example.test", TransferIdleTimeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(t.TempDir(), "object.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := client.UploadFile(context.Background(), PresignedURL{URL: server.URL,
		Headers: map[string]string{"X-Instruction": "kept"}}, filename); err != nil {
		t.Fatal(err)
	}
	header := seen.Load()
	if header.Get("X-Instruction") != "kept" {
		t.Fatalf("header instruction was dropped: %v", *header)
	}
	if _, present := (*header)["Content-Type"]; present {
		t.Fatalf("a Content-Type was invented for an uninstructed upload: %q", header.Get("Content-Type"))
	}
	if _, err := client.UploadFile(context.Background(), PresignedURL{URL: server.URL,
		Headers: map[string]string{"Content-Type": "video/mp4"}}, filename); err != nil {
		t.Fatal(err)
	}
	if got := seen.Load().Get("Content-Type"); got != "video/mp4" {
		t.Fatalf("Content-Type instruction = %q, want video/mp4", got)
	}
}

func TestUploadRefusesABodyInstruction(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, err := New(Config{Endpoint: "https://tams.example.test", TransferIdleTimeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(t.TempDir(), "object.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	body := "instruction text"
	_, err = client.UploadFile(context.Background(), PresignedURL{URL: server.URL, Body: &body}, filename)
	if err == nil || !strings.Contains(err.Error(), "request body") {
		t.Fatalf("body instruction accepted: %v", err)
	}
	if requests.Load() != 0 {
		t.Fatal("a request was sent despite the unsupported instruction")
	}
}

func TestCanonicalUUID(t *testing.T) {
	t.Parallel()
	want := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	for _, value := range []string{want, strings.ToUpper(want), strings.ReplaceAll(want, "-", ""), "urn:uuid:" + want, "{" + want + "}", " " + want + " "} {
		if got, err := CanonicalUUID(value); err != nil || got != want {
			t.Errorf("CanonicalUUID(%q) = %q, %v", value, got, err)
		}
	}
	for _, value := range []string{
		"",
		"not-a-uuid",
		"00000000-0000-0000-0000-000000000000",
		"6ba7b810-9dad-71d1-80b4-00c04fd430c8",
		"6ba7b810-9dad-11d1-00b4-00c04fd430c8",
	} {
		if _, err := CanonicalUUID(value); err == nil {
			t.Errorf("CanonicalUUID(%q) unexpectedly succeeded", value)
		}
	}
}
