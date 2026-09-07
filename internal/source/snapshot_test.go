package source

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestHTTPSnapshotAndPrivateBridge(t *testing.T) {
	t.Parallel()
	var changed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer input-secret" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("upstream credentials or identity encoding missing")
		}
		etag := `"revision-one"`
		if changed.Load() {
			etag = `"revision-two"`
		}
		w.Header().Set("ETag", etag)
		http.ServeContent(w, r, "media", time.Time{}, strings.NewReader("0123456789"))
	}))
	defer server.Close()
	resolver := New(Config{HTTPHeaders: http.Header{"Authorization": {"Bearer input-secret"}}})
	items, err := resolver.Resolve(t.Context(), []string{server.URL + "/media?signature=secret"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := items[0].Snapshot(t.Context())
	if err != nil || snapshot.Size != 10 || snapshot.Revision != `"revision-one"` {
		t.Fatalf("snapshot = %#v, %v", snapshot, err)
	}
	bridge, err := NewBridge(t.Context(), snapshot, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()
	if strings.Contains(bridge.URL, "secret") {
		t.Fatal("bridge exposed upstream credentials")
	}
	for _, method := range []string{http.MethodHead, http.MethodGet, http.MethodPost} {
		req, _ := http.NewRequestWithContext(t.Context(), method, bridge.URL, nil)
		req.Header.Set("Range", "bytes=3-6")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || (method == http.MethodPost && resp.StatusCode != 405) || (method == http.MethodGet && string(body) != "3456") || (method == http.MethodHead && len(body) != 0) {
			t.Fatalf("%s: status=%d body=%q err=%v", method, resp.StatusCode, body, err)
		}
	}
	resp, err := http.Get(bridge.URL + "/wrong")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatal("unregistered path was served")
	}
	changed.Store(true)
	if _, err := snapshot.OpenAt(t.Context(), 4); !errors.Is(err, ErrSnapshotChanged) {
		t.Fatalf("changed input: %v", err)
	}
	if err := bridge.Redact(fmt.Errorf("probe %s failed", bridge.URL)); strings.Contains(err.Error(), bridge.URL) {
		t.Fatalf("capability leaked: %v", err)
	}
	bridge.Close()
	if _, err := http.Get(bridge.URL); err == nil {
		t.Fatal("listener survived close")
	}
}

func TestHTTPSnapshotRejectsUnsafeInitialResponses(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, etag, span string
		status           int
		fallback         bool
	}{
		{"no ranges", `"one"`, "", 200, true},
		{"weak ETag", `W/"one"`, "bytes 0-0/10", 206, true},
		{"missing ETag", "", "bytes 0-0/10", 206, true},
		{"wrong span", `"one"`, "bytes 1-1/10", 206, false},
		{"auth failure", "", "", 403, false},
		{"range probe rejected", "", "", 405, true},
		{"range probe not understood", "", "", 400, true},
		{"origin failure", "", "", 503, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", tc.etag)
				w.Header().Set("Content-Range", tc.span)
				w.Header().Set("Content-Length", "1")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, "0")
			}))
			defer server.Close()
			target, _ := url.Parse(server.URL)
			_, err := New(Config{}).httpSnapshot(t.Context(), target)
			var unavailable *StreamUnavailableError
			if err == nil || errors.As(err, &unavailable) != tc.fallback {
				t.Fatalf("snapshot error=%v, fallback=%v", err, tc.fallback)
			}
		})
	}
}

func TestHTTPSnapshotRedirectStripsAllInputCredentials(t *testing.T) {
	t.Parallel()
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("X-Private-Input") != "" {
			t.Error("credentials crossed origin")
		}
		w.Header().Set("ETag", `"one"`)
		http.ServeContent(w, r, "media", time.Time{}, strings.NewReader("data"))
	}))
	defer destination.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	target, _ := url.Parse(origin.URL)
	snapshot, err := New(Config{HTTPHeaders: http.Header{"Authorization": {"Bearer secret"}, "X-Private-Input": {"secret"}}}).httpSnapshot(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Resource != destination.URL {
		t.Fatalf("effective resource = %q", snapshot.Resource)
	}
	body, err := snapshot.OpenAt(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	if data, err := io.ReadAll(body); err != nil || string(data) != "ata" {
		t.Fatalf("range = %q, %v", data, err)
	}
}

type snapshotS3 struct {
	fakeS3
	version string
	changed bool
}

func (s *snapshotS3) HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(10), ETag: aws.String(`"one"`), VersionId: aws.String(s.version)}, nil
}

func (s *snapshotS3) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if s.version != "" && aws.ToString(input.VersionId) != s.version || s.version == "" && aws.ToString(input.IfMatch) != `"one"` {
		return nil, errors.New("request did not pin S3 revision")
	}
	var start, end int
	if _, err := fmt.Sscanf(aws.ToString(input.Range), "bytes=%d-%d", &start, &end); err != nil {
		return nil, err
	}
	etag := `"one"`
	if s.changed {
		etag = `"two"`
	}
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(strings.NewReader("0123456789"[start : end+1])),
		ContentLength: aws.Int64(int64(end - start + 1)), ContentRange: aws.String(fmt.Sprintf("bytes %d-%d/10", start, end)),
		ETag: aws.String(etag), VersionId: aws.String(s.version),
	}, nil
}

func TestS3SnapshotPinsVersionOrETag(t *testing.T) {
	t.Parallel()
	for _, version := range []string{"", "version-one"} {
		client := &snapshotS3{version: version}
		snapshot, err := New(Config{}).s3Snapshot(t.Context(), client, "bucket", "exact/key")
		if err != nil {
			t.Fatal(err)
		}
		body, err := snapshot.OpenAt(t.Context(), 5)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(body)
		_ = body.Close()
		if err != nil || string(data) != "56789" {
			t.Fatalf("range = %q, %v", data, err)
		}
		client.changed = true
		if _, err := snapshot.OpenAt(t.Context(), 2); !errors.Is(err, ErrSnapshotChanged) {
			t.Fatalf("changed revision: %v", err)
		}
	}
}

func TestBridgeRetriesTruncatedRangesWithoutChangingRevision(t *testing.T) {
	t.Parallel()
	for _, change := range []bool{false, true} {
		t.Run(fmt.Sprint(change), func(t *testing.T) {
			var truncated atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"one"`)
				if r.Header.Get("Range") == "bytes=0-" && truncated.CompareAndSwap(false, true) {
					w.Header().Set("Content-Range", "bytes 0-9/10")
					w.Header().Set("Content-Length", "10")
					w.WriteHeader(206)
					_, _ = io.WriteString(w, "01234")
					return
				}
				if change && truncated.Load() {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
				http.ServeContent(w, r, "media", time.Time{}, strings.NewReader("0123456789"))
			}))
			defer server.Close()
			target, _ := url.Parse(server.URL)
			snapshot, err := New(Config{}).httpSnapshot(t.Context(), target)
			if err != nil {
				t.Fatal(err)
			}
			bridge, err := NewBridge(t.Context(), snapshot, 1, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer bridge.Close()
			response, err := http.Get(bridge.URL)
			if err == nil {
				var data []byte
				data, err = io.ReadAll(response.Body)
				_ = response.Body.Close()
				if !change && string(data) != "0123456789" {
					t.Fatalf("resumed body=%q", data)
				}
			}
			if change {
				if err == nil || !errors.Is(bridge.Upstream(), ErrSnapshotChanged) {
					t.Fatalf("changed resumed input: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHTTPSnapshotRejectsChangedOrMalformedPinnedRange(t *testing.T) {
	t.Parallel()
	for _, response := range []struct {
		name, span, etag string
		status           int
	}{
		{"ignored range", "", `"one"`, 200},
		{"changed size", "bytes 0-10/11", `"one"`, 206},
		{"wrong offset", "bytes 1-9/10", `"one"`, 206},
		{"missing validator", "bytes 0-9/10", "", 206},
	} {
		t.Run(response.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Range") == "bytes=0-0" {
					w.Header().Set("ETag", `"one"`)
					http.ServeContent(w, r, "media", time.Time{}, strings.NewReader("0123456789"))
					return
				}
				w.Header().Set("ETag", response.etag)
				w.Header().Set("Content-Range", response.span)
				w.WriteHeader(response.status)
				_, _ = io.WriteString(w, "0123456789")
			}))
			defer server.Close()
			target, _ := url.Parse(server.URL)
			snapshot, err := New(Config{}).httpSnapshot(t.Context(), target)
			if err != nil {
				t.Fatal(err)
			}
			body, err := snapshot.OpenAt(t.Context(), 0)
			if body != nil {
				_ = body.Close()
			}
			var unavailable *StreamUnavailableError
			if err == nil || errors.As(err, &unavailable) {
				t.Fatalf("invalid pinned response was accepted or allowed fallback: %v", err)
			}
		})
	}
}

func TestBridgeRejectsInvalidRangeTerminator(t *testing.T) {
	t.Parallel()
	for _, ending := range []string{"1\r\nx\r\n0\r\n\r\n", ""} {
		t.Run(fmt.Sprintf("ending=%q", ending), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Range") == "bytes=0-0" {
					w.Header().Set("ETag", `"one"`)
					http.ServeContent(w, r, "media", time.Time{}, strings.NewReader("0123456789"))
					return
				}
				connection, buffer, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer connection.Close()
				_, _ = buffer.WriteString("HTTP/1.1 206 Partial Content\r\nETag: \"one\"\r\nContent-Range: bytes 0-9/10\r\nTransfer-Encoding: chunked\r\n\r\na\r\n0123456789\r\n" + ending)
				_ = buffer.Flush()
			}))
			defer server.Close()
			target, _ := url.Parse(server.URL)
			snapshot, err := New(Config{}).httpSnapshot(t.Context(), target)
			if err != nil {
				t.Fatal(err)
			}
			bridge, err := NewBridge(t.Context(), snapshot, 1, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer bridge.Close()
			response, err := http.Get(bridge.URL)
			if err == nil {
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
			}
			if bridge.Upstream() == nil {
				t.Fatal("range serving hid an invalid body terminator")
			}
		})
	}
}

func TestBridgeCloseCancelsActiveUpstreamRead(t *testing.T) {
	t.Parallel()
	reading, cancelled := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			w.Header().Set("ETag", `"one"`)
			http.ServeContent(w, r, "media", time.Time{}, strings.NewReader("0123456789"))
			return
		}
		close(reading)
		<-r.Context().Done()
		close(cancelled)
	}))
	defer server.Close()
	target, _ := url.Parse(server.URL)
	snapshot, err := New(Config{}).httpSnapshot(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	bridge, err := NewBridge(t.Context(), snapshot, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if response, err := http.Get(bridge.URL); err == nil {
			_ = response.Body.Close()
		}
	}()
	select {
	case <-reading:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream read did not start")
	}
	bridge.Close()
	for _, signal := range []chan struct{}{cancelled, done} {
		select {
		case <-signal:
		case <-time.After(5 * time.Second):
			t.Fatal("close left an active request")
		}
	}
}

func TestHTTPSnapshotDoesNotTimeOutConsumerPause(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"one"`)
		http.ServeContent(w, r, "media", time.Time{}, strings.NewReader("0123456789"))
	}))
	defer server.Close()
	target, _ := url.Parse(server.URL)
	snapshot, err := New(Config{TransferIdleTimeout: 100 * time.Millisecond}).httpSnapshot(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	body, err := snapshot.OpenAt(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	var first [1]byte
	if _, err := io.ReadFull(body, first[:]); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if data, err := io.ReadAll(body); err != nil || string(data) != "123456789" {
		t.Fatalf("paused input = %q, %v", data, err)
	}
}

// Resolve a signed redirect once, then reuse that exact resource for ranges.
func TestHTTPSnapshotPinsSignedRedirect(t *testing.T) {
	t.Parallel()
	var issued atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/media" {
			http.Redirect(w, r, fmt.Sprintf("/signed?token=%d", issued.Add(1)), http.StatusFound)
			return
		}
		if r.URL.RawQuery != "token=1" {
			t.Errorf("read used a different signed resource: %q", r.URL.RawQuery)
		}
		w.Header().Set("ETag", `"one"`)
		http.ServeContent(w, r, "media", time.Time{}, strings.NewReader("0123456789"))
	}))
	defer server.Close()
	items, err := New(Config{}).Resolve(t.Context(), []string{server.URL + "/media"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := items[0].Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	body, err := snapshot.OpenAt(t.Context(), 4)
	if err != nil {
		t.Fatalf("pinned signed location rejected: %v", err)
	}
	data, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil || string(data) != "456789" || issued.Load() != 1 {
		t.Fatalf("data=%q err=%v redirects=%d", data, err, issued.Load())
	}
}

func TestHTTPSnapshotDoesNotSwitchResourcesWithMatchingETags(t *testing.T) {
	t.Parallel()
	for _, redirectPinned := range []bool{false, true} {
		t.Run(fmt.Sprintf("redirect_pinned=%t", redirectPinned), func(t *testing.T) {
			var changed atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/input" {
					target := "/a"
					if changed.Load() {
						target = "/b"
					}
					http.Redirect(w, r, target, http.StatusTemporaryRedirect)
					return
				}
				if r.URL.Path == "/a" && changed.Load() && redirectPinned {
					http.Redirect(w, r, "/b", http.StatusTemporaryRedirect)
					return
				}
				data := "AAAAAAAAAA"
				if r.URL.Path == "/b" {
					data = "BBBBBBBBBB"
				}
				// Validators need not differ between distinct resources.
				w.Header().Set("ETag", `"revision-1"`)
				http.ServeContent(w, r, "media", time.Time{}, strings.NewReader(data))
			}))
			defer server.Close()
			target, _ := url.Parse(server.URL + "/input")
			snapshot, err := New(Config{}).httpSnapshot(t.Context(), target)
			if err != nil {
				t.Fatal(err)
			}
			changed.Store(true)
			body, err := snapshot.OpenAt(t.Context(), 5)
			if body != nil {
				defer body.Close()
			}
			if redirectPinned {
				if !errors.Is(err, ErrSnapshotChanged) {
					t.Fatalf("changed resource accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if data, err := io.ReadAll(body); err != nil || string(data) != "AAAAA" {
				t.Fatalf("pinned range = %q, %v", data, err)
			}
		})
	}
}

// One open-ended tool read can outlive many idle-closing proxies. Each
// reconnect that makes sustained progress restores the retry budget.
func TestBridgeRestoresRetryBudgetAfterSustainedProgress(t *testing.T) {
	t.Parallel()
	const size = 4 << 20
	const truncateAfter = reconnectResetBytes + reconnectResetBytes/2
	content := bytes.Repeat([]byte("0123456789abcdef"), size/16)
	var truncations atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"one"`)
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "bytes=0-0" {
			http.ServeContent(w, r, "media", time.Time{}, bytes.NewReader(content))
			return
		}
		offset, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(rangeHeader, "bytes="), "-"), 10, 64)
		if err != nil {
			t.Errorf("unexpected range %q", rangeHeader)
			w.WriteHeader(400)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, size-1, size))
		w.Header().Set("Content-Length", strconv.FormatInt(size-offset, 10))
		w.WriteHeader(http.StatusPartialContent)
		remaining := content[offset:]
		if len(remaining) > truncateAfter {
			remaining = remaining[:truncateAfter]
			truncations.Add(1)
		}
		_, _ = w.Write(remaining)
	}))
	defer server.Close()
	target, _ := url.Parse(server.URL)
	snapshot, err := New(Config{}).httpSnapshot(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	bridge, err := NewBridge(t.Context(), snapshot, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()
	response, err := http.Get(bridge.URL)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || !bytes.Equal(data, content) {
		t.Fatalf("read %d bytes, err=%v, truncations=%d", len(data), err, truncations.Load())
	}
	if truncations.Load() < 2 {
		t.Fatalf("the source truncated only %d times; the budget was never exercised", truncations.Load())
	}
}
