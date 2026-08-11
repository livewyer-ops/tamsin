package tams

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestSegmentsFollowsPaging covers the hazard created by listing a Flow's
// Segments in one call rather than filtering by Object: the service caps the
// page size, and a truncated listing reads as "these Segments do not exist",
// which would make a resume re-upload media that is already stored.
func TestSegmentsFollowsPaging(t *testing.T) {
	t.Parallel()
	const total = 250
	var served int
	var baseURL string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/flows/flow/segments" {
			http.NotFound(writer, request)
			return
		}
		const pageSize = 100
		start := 0
		if cursor := request.URL.Query().Get("page"); cursor != "" {
			start, _ = strconv.Atoi(cursor)
		}
		end := min(start+pageSize, total)
		page := make([]Segment, 0, end-start)
		for index := start; index < end; index++ {
			page = append(page, Segment{ObjectID: strconv.Itoa(index), Timerange: "[0:0_1:0)"})
		}
		served += len(page)
		if end < total {
			// An absolute cursor on the same origin, which is what a service
			// behind a gateway typically emits.
			writer.Header().Set("Link", "<"+baseURL+"/flows/flow/segments?page="+strconv.Itoa(end)+">; rel=\"next\"")
		}
		writer.Header().Set("X-Paging-Limit", strconv.Itoa(pageSize))
		_ = json.NewEncoder(writer).Encode(page)
	}))
	baseURL = server.URL
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL, Timeout: 5 * time.Second, Retries: 1})
	if err != nil {
		t.Fatal(err)
	}
	segments, err := client.Segments(context.Background(), "flow", "")
	if err != nil {
		t.Fatalf("Segments() = %v", err)
	}
	if len(segments) != total {
		t.Fatalf("listed %d segments, want %d: a truncated listing would silently re-upload stored media", len(segments), total)
	}
	if served != total {
		t.Fatalf("server served %d segments, want %d", served, total)
	}
}

// TestSegmentsResolvesPagingAgainstThePreviousPage distinguishes RFC reference
// resolution from resolving every cursor against the configured API root. In
// particular, a query-only cursor must keep the current /flows/.../segments
// path, and a path-relative cursor starts in that page's directory.
func TestSegmentsResolvesPagingAgainstThePreviousPage(t *testing.T) {
	const leakedURL = "https://storage.example.test/object?signature=must-not-escape"
	for _, testCase := range []struct {
		name       string
		cursor     func(string) string
		secondPage string
	}{
		{
			name:       "query-only",
			cursor:     func(string) string { return "?cursor=two" },
			secondPage: "/api/v8.1/flows/flow/segments?cursor=two",
		},
		{
			name:       "path-relative",
			cursor:     func(string) string { return "next-page?cursor=two" },
			secondPage: "/api/v8.1/flows/flow/next-page?cursor=two",
		},
		{
			name: "absolute-same-origin",
			cursor: func(baseURL string) string {
				return baseURL + "/api/v8.1/flows/flow/segments?cursor=two"
			},
			secondPage: "/api/v8.1/flows/flow/segments?cursor=two",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var baseURL string
			var requests []string
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requests = append(requests, request.URL.RequestURI())
				if len(requests) == 1 {
					writer.Header().Set("Link", "<"+testCase.cursor(baseURL)+">; rel=\"next\"")
				}
				_ = json.NewEncoder(writer).Encode([]Segment{{
					ObjectID: strconv.Itoa(len(requests)), Timerange: "[0:0_1:0)",
					GetURLs: []PresignedURL{{URL: leakedURL}},
				}})
			}))
			baseURL = server.URL
			defer server.Close()

			client, err := New(Config{Endpoint: server.URL + "/api/v8.1", Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			segments, err := client.ListSegments(context.Background(), "flow", SegmentListOptions{})
			if err != nil {
				t.Fatalf("ListSegments() = %v", err)
			}
			if len(segments) != 2 {
				t.Fatalf("ListSegments() returned %d pages' Segments, want 2", len(segments))
			}
			for index, segment := range segments {
				if len(segment.GetURLs) != 0 {
					t.Fatalf("page %d retained a download URL from a lean listing: %#v", index+1, segment.GetURLs)
				}
			}
			if len(requests) != 2 {
				t.Fatalf("requests = %v, want two pages", requests)
			}
			const firstPage = "/api/v8.1/flows/flow/segments?accept_get_urls=&limit=1000&presigned=false"
			if requests[0] != firstPage || requests[1] != testCase.secondPage {
				t.Fatalf("requests = %v, want [%s %s]", requests, firstPage, testCase.secondPage)
			}
		})
	}
}

func TestSegmentsParsesAllRFCLinkFieldValues(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests = append(requests, request.URL.RequestURI())
		if len(requests) == 1 {
			// Multiple field-values must remain visible, while commas and semicolons
			// inside the URI-reference or quoted parameter are not list delimiters.
			// An extension relation URI and ptoken parameter value are both legal
			// without quotes; rel can itself be a space-separated relation list.
			writer.Header().Add("Link", `<ignored?cursor=old>; rel=https://relations.example.test/archive; type=text/html`)
			writer.Header().Add("Link", `<next;page?cursor=a,b>; title="quoted, value; still one parameter"; rel=alternate next; type=text/html`)
		}
		_ = json.NewEncoder(writer).Encode([]Segment{{ObjectID: strconv.Itoa(len(requests)), Timerange: "[0:0_1:0)"}})
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL + "/api/v8.1", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	segments, err := client.ListSegments(context.Background(), "flow", SegmentListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 2 {
		t.Fatalf("segments = %d, want both pages", len(segments))
	}
	want := []string{
		"/api/v8.1/flows/flow/segments?accept_get_urls=&limit=1000&presigned=false",
		"/api/v8.1/flows/flow/next;page?cursor=a,b",
	}
	if len(requests) != len(want) || requests[0] != want[0] || requests[1] != want[1] {
		t.Fatalf("requests = %v, want %v", requests, want)
	}
}

func TestNextPageURLAcceptsRFCParameterAndRelationSyntax(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		value string
	}{
		{name: "unquoted relation list", value: `<next>; rel=alternate next`},
		{name: "extension relation URI", value: `<next>; rel=https://relations.example.test/custom next`},
		{name: "ptoken media type", value: `<next>; type=text/html; rel=next`},
		{name: "case insensitive registered relation", value: `<next>; rel=NeXt`},
		{name: "multiple relation spaces", value: `<next>; rel=alternate   next`},
		{name: "broad ptoken punctuation", value: `<next>; x=https://example.test/{item}?a=b@[c]; rel=next`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			target, found, err := nextPageURL([]string{testCase.value})
			if err != nil {
				t.Fatalf("nextPageURL() error = %v", err)
			}
			if !found || target != "next" {
				t.Fatalf("nextPageURL() = %q, %t, want next, true", target, found)
			}
		})
	}
}

func TestSegmentsRejectsMalformedOrAmbiguousNextLinks(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		values []string
	}{
		{name: "next target lacks brackets", values: []string{`next-page; rel=next`}},
		{name: "next target is unterminated", values: []string{`<next-page; rel=next`}},
		{name: "next relation is unterminated", values: []string{`<next-page>; rel="next`}},
		{name: "two next links in one field", values: []string{`<one>; rel=next, <two>; rel=next`}},
		{name: "two next header fields", values: []string{`<one>; rel=next`, `<two>; rel=next`}},
		{name: "duplicate relation parameters", values: []string{`<one>; rel=prev; rel=next`}},
		{name: "invalid member after next relation", values: []string{`<one>; rel=next invalid_relation`}},
		{name: "tab separated relation list", values: []string{"<one>; rel=next\talternate"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				for _, value := range testCase.values {
					writer.Header().Add("Link", value)
				}
				_, _ = io.WriteString(writer, `[]`)
			}))
			defer server.Close()

			client, err := New(Config{Endpoint: server.URL, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.ListSegments(context.Background(), "flow", SegmentListOptions{}); err == nil ||
				!strings.Contains(err.Error(), "paging Link header") {
				t.Fatalf("ListSegments() error = %v, want malformed/ambiguous Link rejection", err)
			}
			if requests.Load() != 1 {
				t.Fatalf("requests = %d, want rejection before a follow-up request", requests.Load())
			}
		})
	}
}

// TestSegmentsRejectsOffOriginPaging proves validation happens before the
// credentialed metadata transport sees the cursor target.
func TestSegmentsRejectsOffOriginPaging(t *testing.T) {
	t.Parallel()
	var attackerRequests atomic.Int32
	attacker := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		attackerRequests.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer attacker.Close()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer TAMS-secret" {
			t.Errorf("initial API request Authorization = %q", request.Header.Get("Authorization"))
		}
		writer.Header().Set("Link", "<"+attacker.URL+"/steal>; rel=\"next\"")
		_, _ = io.WriteString(writer, `[]`)
	}))
	defer server.Close()

	authenticated := roundTripError(func(request *http.Request) (*http.Response, error) {
		clone := request.Clone(request.Context())
		clone.Header = request.Header.Clone()
		clone.Header.Set("Authorization", "Bearer TAMS-secret")
		return http.DefaultTransport.RoundTrip(clone)
	})
	client, err := New(Config{Endpoint: server.URL, Transport: authenticated, Timeout: 5 * time.Second, Retries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Segments(context.Background(), "flow", ""); err == nil ||
		!strings.Contains(err.Error(), "not the configured endpoint") {
		t.Fatalf("Segments() error = %v, want an off-origin cursor to be rejected", err)
	}
	if attackerRequests.Load() != 0 {
		t.Fatalf("off-origin cursor received %d credentialed request(s)", attackerRequests.Load())
	}
}

func TestSegmentsRejectsPagingOutsideAPIBasePath(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		cursor string
	}{
		{name: "absolute path", cursor: "/outside?page=two"},
		{name: "dot segments", cursor: "../../../outside?page=two"},
		{name: "encoded dot segments", cursor: "%2e%2e/%2e%2e/%2e%2e/%2e%2e/outside?page=two"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				writer.Header().Set("Link", "<"+testCase.cursor+">; rel=\"next\"")
				_, _ = io.WriteString(writer, `[]`)
			}))
			defer server.Close()

			client, err := New(Config{Endpoint: server.URL + "/api/v8.1", Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.ListSegments(context.Background(), "flow", SegmentListOptions{}); err == nil ||
				!strings.Contains(err.Error(), "outside the configured API path") {
				t.Fatalf("ListSegments() error = %v, want API base-path escape rejection", err)
			}
			if requests.Load() != 1 {
				t.Fatalf("requests = %d, want only the initial API page", requests.Load())
			}
		})
	}
}

func TestSegmentsRejectsRecursivelyEncodedPagingTraversal(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		cursor string
	}{
		{name: "double encoded dot segments", cursor: "%252e%252e/%252e%252e/%252e%252e/%252e%252e/outside"},
		{name: "encoded slash", cursor: "next%2f..%2f..%2foutside"},
		{name: "double encoded slash", cursor: "next%252f..%252f..%252foutside"},
		{name: "encoded backslash", cursor: "next%5c..%5c..%5coutside"},
		{name: "double encoded backslash", cursor: "next%255c..%255c..%255coutside"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				writer.Header().Set("Link", "<"+testCase.cursor+">; rel=next")
				_, _ = io.WriteString(writer, `[]`)
			}))
			defer server.Close()

			client, err := New(Config{Endpoint: server.URL + "/api/v8.1", Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.ListSegments(context.Background(), "flow", SegmentListOptions{}); err == nil ||
				!strings.Contains(err.Error(), "ambiguous encoded traversal or separator") {
				t.Fatalf("ListSegments() error = %v, want recursively encoded path rejection", err)
			}
			if requests.Load() != 1 {
				t.Fatalf("requests = %d, want rejection before a credentialed follow-up", requests.Load())
			}
		})
	}
}

func TestSegmentsRejectsMalformedPagingCursor(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Link", `<%zz>; rel="next"`)
		_, _ = io.WriteString(writer, `[]`)
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListSegments(context.Background(), "flow", SegmentListOptions{}); err == nil ||
		!strings.Contains(err.Error(), "paging cursor") || !strings.Contains(err.Error(), "not a valid URL") {
		t.Fatalf("ListSegments() error = %v, want malformed paging cursor rejection", err)
	}
}

func TestSegmentsRejectsPagingCursorCycle(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requestNumber := requests.Add(1)
		writer.Header().Set("Link", `<?cursor=two>; rel="next"`)
		_ = json.NewEncoder(writer).Encode([]Segment{{ObjectID: strconv.Itoa(int(requestNumber)), Timerange: "[0:0_1:0)"}})
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListSegments(context.Background(), "flow", SegmentListOptions{}); err == nil ||
		!strings.Contains(err.Error(), "cursor cycle") {
		t.Fatalf("ListSegments() error = %v, want cursor cycle rejection", err)
	}
	if requests.Load() != 2 {
		t.Fatalf("requests = %d, want cycle rejection before a third request", requests.Load())
	}
}

func TestSegmentsEnforcesAtomicCollectionLimits(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name          string
		body          string
		link          bool
		pageLimit     int
		segmentLimit  int
		byteLimit     int
		wantRequests  int32
		wantErrorText string
	}{
		{
			name: "page limit", body: `[]`, link: true,
			pageLimit: 2, segmentLimit: maxListedSegments, byteLimit: maxSegmentResponseBytes,
			wantRequests: 2, wantErrorText: "exceeded 2 pages",
		},
		{
			name: "segment limit", body: `[{"object_id":"one","timerange":"[0:0_1:0)"},{"object_id":"two","timerange":"[1:0_2:0)"}]`,
			pageLimit: maxSegmentPages, segmentLimit: 1, byteLimit: maxSegmentResponseBytes,
			wantRequests: 1, wantErrorText: "exceeded 1 Segments",
		},
		{
			name: "response byte limit", body: `[{"object_id":"one","timerange":"[0:0_1:0)"}]`,
			pageLimit: maxSegmentPages, segmentLimit: maxListedSegments, byteLimit: 1,
			wantRequests: 1, wantErrorText: "exceeded 1 response bytes",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requestNumber := requests.Add(1)
				if testCase.link {
					writer.Header().Set("Link", fmt.Sprintf("<?page=%d>; rel=next", requestNumber+1))
				}
				_, _ = io.WriteString(writer, testCase.body)
			}))
			defer server.Close()

			client, err := New(Config{Endpoint: server.URL, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			client.segmentPageLimit = testCase.pageLimit
			client.segmentCountLimit = testCase.segmentLimit
			client.segmentByteLimit = testCase.byteLimit
			segments, err := client.ListSegments(context.Background(), "flow", SegmentListOptions{})
			if err == nil || !strings.Contains(err.Error(), testCase.wantErrorText) {
				t.Fatalf("ListSegments() = %#v, %v; want %q", segments, err, testCase.wantErrorText)
			}
			if segments != nil {
				t.Fatalf("limited ListSegments() returned partial results: %#v", segments)
			}
			if requests.Load() != testCase.wantRequests {
				t.Fatalf("requests = %d, want %d", requests.Load(), testCase.wantRequests)
			}
		})
	}
}
