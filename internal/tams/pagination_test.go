package tams

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
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
	segments, err := client.ListSegments(context.Background(), "flow", SegmentListOptions{IncludeDownloadURLs: true})
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

func TestStorageBackendsFollowsPagingAndIgnoresUnusedTags(t *testing.T) {
	t.Parallel()
	const first = "9cb30d91-e456-41f3-9398-bf726c749e96"
	const second = "2834cde7-b5db-47e3-9fdd-e793376565fe"
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/service/storage-backends" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if r.URL.Query().Get("page") == "second" {
			_, _ = io.WriteString(w, `[{"id":"`+second+`","default_storage":true,"tags":{"auth_classes":["production","review"]}}]`)
			return
		}
		w.Header().Set("Link", `<?page=second>; rel="next"`)
		_, _ = io.WriteString(w, `[{"id":"`+first+`","tags":{"location":"local"}}]`)
	}))
	defer server.Close()
	client, err := New(Config{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	backends, err := client.StorageBackends(context.Background())
	if err != nil || len(backends) != 2 || backends[1].ID != second || !backends[1].DefaultStorage || requests.Load() != 2 {
		t.Fatalf("StorageBackends() = %#v, %v; requests=%d", backends, err, requests.Load())
	}
}

func TestStorageBackendsRejectsUnsafePagingWithoutPartialResults(t *testing.T) {
	t.Parallel()
	for _, link := range []string{`<https://other.example/backends>; rel=next`, `<?page=repeat>; rel=next`} {
		t.Run(link, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Link", link)
				_, _ = io.WriteString(w, `[{"id":"9cb30d91-e456-41f3-9398-bf726c749e96"}]`)
			}))
			defer server.Close()
			client, err := New(Config{Endpoint: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			if backends, err := client.StorageBackends(context.Background()); err == nil || backends != nil {
				t.Fatalf("unsafe listing returned %#v, %v", backends, err)
			}
		})
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
	if _, err := client.ListSegments(context.Background(), "flow", SegmentListOptions{IncludeDownloadURLs: true}); err == nil ||
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
			wantRequests: 1, wantErrorText: "exceeded 1 entries",
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

// pagedListing serves each body as one page of a listing, linking every page
// but the last to its successor.
func pagedListing(t *testing.T, pages []string) *Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		index, _ := strconv.Atoi(request.URL.Query().Get("page"))
		if index+1 < len(pages) {
			writer.Header().Set("Link", "<"+request.URL.Path+"?page="+strconv.Itoa(index+1)+">; rel=\"next\"")
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, pages[index])
	}))
	t.Cleanup(server.Close)
	client, err := New(Config{Endpoint: server.URL, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// TestSegmentListingRejectsWhatCannotBeMatched covers the listings that used
// to read as "no such Segment": that answer is the proof a retraction
// completed and the reason a resume uploads, so only a well-formed array may
// give it.
func TestSegmentListingRejectsWhatCannotBeMatched(t *testing.T) {
	t.Parallel()
	const valid = `[{"object_id":"object","timerange":"[0:0_1:0)"}]`
	for name, pages := range map[string][]string{
		"empty body":                          {""},
		"null":                                {"null"},
		"entry without fields":                {"[{}]"},
		"entry without an object_id":          {`[{"timerange":"[0:0_1:0)"}]`},
		"entry without a timerange":           {`[{"object_id":"object"}]`},
		"entry with a malformed timerange":    {`[{"object_id":"object","timerange":"0:0-1:0"}]`},
		"empty body after a valid page":       {valid, ""},
		"null after a valid page":             {valid, "null"},
		"malformed entry after a valid page":  {valid, `[{"object_id":"object","timerange":"0:0-1:0"}]`},
		"entry without fields after an empty": {"[]", "[{}]"},
		"never":                               {`[{"object_id":"object","timerange":"()"}]`},
		"eternity":                            {`[{"object_id":"object","timerange":"_"}]`},
		"timerange without a start":           {`[{"object_id":"object","timerange":"_1:0)"}]`},
		"timerange without an end":            {`[{"object_id":"object","timerange":"[1:0_"}]`},
		"timerange with an exclusive start":   {`[{"object_id":"object","timerange":"(1:0_2:0)"}]`},
		"timerange with a plus sign":          {`[{"object_id":"object","timerange":"[+2:0_3:0)"}]`},
		"timerange with a leading zero":       {`[{"object_id":"object","timerange":"[02:0_3:0)"}]`},
		"timerange with signed nanoseconds":   {`[{"object_id":"object","timerange":"[1:+0_2:0)"}]`},
		"keys in another case":                {`[{"OBJECT_ID":"object","TIMERANGE":"[0:0_1:0)"}]`},
		"object_id key in another case":       {`[{"Object_ID":"object","timerange":"[0:0_1:0)"}]`},
		"timerange key in another case":       {`[{"object_id":"object","Timerange":"[0:0_1:0)"}]`},
		"entry that is not an object":         {`["object"]`},
		"null entry":                          {`[null]`},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			segments, err := pagedListing(t, pages).ListSegments(context.Background(), "flow", SegmentListOptions{})
			if err == nil {
				t.Fatalf("ListSegments() = %v, want an error", segments)
			}
			if segments != nil {
				t.Fatalf("ListSegments() returned %v beside %v", segments, err)
			}
		})
	}
}

// TestSegmentListingReadsExactKeys covers the lenient key matching of
// encoding/json: a key that differs only in case is an extension, so it must
// neither supply nor replace a field a Segment is matched on.
func TestSegmentListingReadsExactKeys(t *testing.T) {
	t.Parallel()
	count := 2
	want := Segment{
		ObjectID: "object", Timerange: "[0:0_1:0)", ObjectTimerange: "[5:0_6:0)", TSOffset: "-5:0",
		LastDuration: "0:40000000", KeyFrameCount: &count,
		GetURLs: []PresignedURL{{URL: "https://media.example/object", Presigned: true}},
		// A number past 2^53 shows whether provider metadata keeps its digits.
		InitObject: &ObjectCore{"object_id": "init", "sequence": json.Number("9007199254740993")},
	}
	fields := reflect.ValueOf(want)
	for index := range fields.NumField() {
		if fields.Field(index).IsZero() {
			t.Fatalf("the fixture leaves %s unset", fields.Type().Field(index).Name)
		}
	}
	exact, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	// Every exact key is followed by the same key in another case carrying a
	// different value, and by an unrelated extension.
	shadowed := strings.TrimSuffix(string(exact), "}") + `,"OBJECT_ID":"other","TimeRange":"[2:0_3:0)",` +
		`"Object_Timerange":"[7:0_8:0)","TS_OFFSET":"0:0","Last_Duration":"0:1","KEY_FRAME_COUNT":9,` +
		`"GET_URLS":[],"Init_Object":{"object_id":"other"},"vendor":{"note":true}}`
	for name, body := range map[string]string{"exact keys": string(exact), "shadowed keys": shadowed} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			segments, err := pagedListing(t, []string{"[" + body + "]"}).ListSegments(
				context.Background(), "flow", SegmentListOptions{IncludeDownloadURLs: true})
			if err != nil {
				t.Fatal(err)
			}
			if len(segments) != 1 {
				t.Fatalf("ListSegments() = %+v, want one Segment", segments)
			}
			if got, err := json.Marshal(segments[0]); err != nil || string(got) != string(exact) {
				t.Fatalf("ListSegments() read %s, %v; want %s", got, err, exact)
			}
		})
	}
}

func TestListingAcceptsAnEmptyArray(t *testing.T) {
	t.Parallel()
	const valid = `[{"object_id":"object","timerange":"[0:0_1:0)"}]`
	for name, test := range map[string]struct {
		pages []string
		want  int
	}{
		"empty":                      {[]string{"[]"}, 0},
		"empty page before entries":  {[]string{"[]", valid}, 1},
		"empty page after entries":   {[]string{valid, "[]"}, 1},
		"whitespace around an array": {[]string{" [] \n"}, 0},
		"instant":                    {[]string{`[{"object_id":"object","timerange":"[5:0]"}]`}, 1},
		"instant in two timestamps":  {[]string{`[{"object_id":"object","timerange":"[5:0_5:0]"}]`}, 1},
		"inclusive end":              {[]string{`[{"object_id":"object","timerange":"[0:0_1:0]"}]`}, 1},
		"omitted markers":            {[]string{`[{"object_id":"object","timerange":"0:0_1:0"}]`}, 1},
		"negative start":             {[]string{`[{"object_id":"object","timerange":"[-1:500000000_0:0)"}]`}, 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			segments, err := pagedListing(t, test.pages).ListSegments(context.Background(), "flow", SegmentListOptions{})
			if err != nil || len(segments) != test.want {
				t.Fatalf("ListSegments() = %v, %v, want %d entries", segments, err, test.want)
			}
		})
	}
	backends, err := pagedListing(t, []string{"[]"}).StorageBackends(context.Background())
	if err != nil || len(backends) != 0 {
		t.Fatalf("StorageBackends() = %v, %v", backends, err)
	}
}

func TestStorageBackendListingRequiresAnArray(t *testing.T) {
	t.Parallel()
	for name, pages := range map[string][]string{
		"empty body":             {""},
		"null":                   {"null"},
		"null after a full page": {`[{"id":"backend"}]`, "null"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			backends, err := pagedListing(t, pages).StorageBackends(context.Background())
			if err == nil || backends != nil {
				t.Fatalf("StorageBackends() = %v, %v, want an error and no partial result", backends, err)
			}
		})
	}
}
