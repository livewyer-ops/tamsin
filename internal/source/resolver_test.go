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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/livewyer-ops/tamsin/internal/netio"
)

func TestResolveDirectoryAndManifestDeterministically(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	first := filepath.Join(directory, "a.bin")
	secondDirectory := filepath.Join(directory, "nested")
	second := filepath.Join(secondDirectory, "b.bin")
	if err := os.Mkdir(secondDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(first, []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("bb"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(directory, "sources.txt")
	if err := os.WriteFile(manifest, []byte("# comment\nnested/b.bin\na.bin\na.bin\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	items, err := New(Config{}).Resolve(context.Background(), []string{manifest})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("resolved %d items, want 2", len(items))
	}
	if items[0].Name != "b.bin" || items[1].Name != "a.bin" {
		t.Fatalf("manifest order was not preserved: %q, %q", items[0].Name, items[1].Name)
	}

	directoryItems, err := New(Config{}).Resolve(context.Background(), []string{secondDirectory, first})
	if err != nil {
		t.Fatal(err)
	}
	if len(directoryItems) != 2 || directoryItems[0].Name != "b.bin" || directoryItems[1].Name != "a.bin" {
		t.Fatalf("unexpected directory resolution: %#v", directoryItems)
	}
}

func TestResolveEnforcesMaxInputsAcrossEveryExpansionPath(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	paths := make([]string, 3)
	for index := range paths {
		paths[index] = filepath.Join(directory, fmt.Sprintf("asset-%d.bin", index))
		if err := os.WriteFile(paths[index], []byte{byte(index)}, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifest := filepath.Join(t.TempDir(), "inputs.txt")
	if err := os.WriteFile(manifest, []byte(strings.Join(paths, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, testCase := range []struct {
		name   string
		inputs []string
		client S3API
	}{
		{name: "direct inputs", inputs: paths},
		{name: "directory", inputs: []string{directory}},
		{name: "manifest", inputs: []string{manifest}},
		{
			name: "S3 prefix", inputs: []string{"s3://media/day/"},
			client: &fakeS3{objects: map[string][]byte{
				"day/a.ts": []byte("a"), "day/b.ts": []byte("b"), "day/c.ts": []byte("c"),
			}},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(Config{MaxInputs: 2, S3Client: testCase.client}).Resolve(context.Background(), testCase.inputs)
			if err == nil {
				t.Fatal("three unique inputs unexpectedly passed a limit of two")
			}
			if !strings.Contains(err.Error(), "--max-inputs=2") || !strings.Contains(err.Error(), "narrow") {
				t.Fatalf("error = %q, want the limit and an actionable remedy", err)
			}
		})
	}
}

func TestResolveMaxInputsCountsUniqueItems(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "asset.bin")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	items, err := New(Config{MaxInputs: 1}).Resolve(context.Background(), []string{filename, filename})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("resolved %d items, want one unique item", len(items))
	}
	client := &fakeS3{objects: map[string][]byte{"day/asset.ts": []byte("media")}}
	s3Items, err := New(Config{MaxInputs: 1, S3Client: client}).Resolve(context.Background(), []string{
		"s3://media/day/asset.ts", "s3://media/day/",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(s3Items) != 1 {
		t.Fatalf("resolved %d S3 items, want one unique item", len(s3Items))
	}
}
func TestResolveEmptyDirectoryFails(t *testing.T) {
	t.Parallel()
	if _, err := New(Config{}).Resolve(context.Background(), []string{t.TempDir()}); err == nil {
		t.Fatal("empty directory unexpectedly resolved")
	}
}
func TestResolveDeduplicatesDirectFileSymlinks(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	target := filepath.Join(directory, "media.bin")
	alias := filepath.Join(directory, "alias.bin")
	if err := os.WriteFile(target, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	items, err := New(Config{}).Resolve(context.Background(), []string{target, alias})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].LocalPath != target {
		t.Fatalf("symlink aliases were not canonicalized: %#v", items)
	}
}

func TestResolveFileURIDecodesPathExactlyOnce(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	for _, testCase := range []struct {
		name     string
		filename string
	}{
		{name: "space", filename: "space name.ts"},
		{name: "literal encoded space", filename: "literal%20name.ts"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			path := filepath.Join(directory, testCase.filename)
			if err := os.WriteFile(path, []byte("media"), 0o600); err != nil {
				t.Fatal(err)
			}
			input := (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String()
			items, err := New(Config{}).Resolve(context.Background(), []string{input})
			if err != nil {
				t.Fatal(err)
			}
			if len(items) != 1 || items[0].LocalPath != path {
				t.Fatalf("resolved %#v, want exact path %q", items, path)
			}
		})
	}
}

func TestResolverStateIsScopedToEachResolveCall(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "asset.ts")
	if err := os.WriteFile(path, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	resolver := New(Config{})
	for call := range 2 {
		items, err := resolver.Resolve(context.Background(), []string{path})
		if err != nil {
			t.Fatalf("Resolve() call %d: %v", call+1, err)
		}
		if len(items) != 1 || items[0].LocalPath != path {
			t.Fatalf("Resolve() call %d returned %#v", call+1, items)
		}
	}
}

func TestResolveManifestCycle(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	first := filepath.Join(directory, "first.txt")
	second := filepath.Join(directory, "second.txt")
	if err := os.WriteFile(first, []byte("second.txt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("first.txt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{}).Resolve(context.Background(), []string{first}); err == nil {
		t.Fatal("manifest cycle unexpectedly succeeded")
	}
}
func TestResolveManifestCycleThroughSymlink(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	manifest := filepath.Join(directory, "manifest.txt")
	alias := filepath.Join(directory, "alias.txt")
	if err := os.WriteFile(manifest, []byte("alias.txt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(manifest, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := New(Config{}).Resolve(context.Background(), []string{manifest}); err == nil {
		t.Fatal("symlinked manifest cycle unexpectedly succeeded")
	}
}
func TestResolveManifestRejectsInvalidUTF8(t *testing.T) {
	t.Parallel()
	manifest := filepath.Join(t.TempDir(), "sources.txt")
	if err := os.WriteFile(manifest, []byte{0xff, '\n'}, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{}).Resolve(context.Background(), []string{manifest}); err == nil || !strings.Contains(err.Error(), "UTF-8") {
		t.Fatalf("Resolve() error = %v, want UTF-8 validation error", err)
	}
}

func TestResolveHTTPAndStdin(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-Input") != "allowed" {
			t.Fatalf("missing input header")
		}
		_, _ = io.WriteString(writer, "remote")
	}))
	defer server.Close()

	resolver := New(Config{
		Stdin: bytes.NewBufferString("stdin"), StdinName: "/drop/customer-secret/event.ts",
		HTTPHeaders: http.Header{"X-Input": []string{"allowed"}},
	})
	items, err := resolver.Resolve(context.Background(), []string{server.URL + "/asset.mp4", "-"})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Name != "asset.mp4" || items[1].Name != "event.ts" {
		t.Fatalf("unexpected items: %#v", items)
	}
	if items[1].URI != "stdin:" || strings.Contains(items[1].URI, "customer-secret") {
		t.Fatalf("stdin provenance persisted its name hint: %#v", items[1])
	}
	for index, expected := range []string{"remote", "stdin"} {
		reader, err := items[index].Open(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil || string(data) != expected {
			t.Fatalf("item %d data = %q, error = %v", index, data, err)
		}
	}
}
func TestHTTPSourceStripsConfiguredHeadersOnCrossOriginRedirect(t *testing.T) {
	t.Parallel()
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-Input-Secret") != "" {
			http.Error(writer, "redirect leaked input credentials", http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(writer, "redirected")
	}))
	defer target.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-Input-Secret") != "secret" {
			http.Error(writer, "initial input credential missing", http.StatusBadRequest)
			return
		}
		http.Redirect(writer, request, target.URL+"/asset.mp4", http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	items, err := New(Config{HTTPHeaders: http.Header{"X-Input-Secret": []string{"secret"}}}).
		Resolve(context.Background(), []string{redirector.URL + "/asset.mp4"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := items[0].Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(data) != "redirected" {
		t.Fatalf("redirected data = %q, error = %v", data, err)
	}
}

func TestHTTPSourcePreservesConfiguredHeadersOnSameOriginRedirect(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/redirect":
			http.Redirect(writer, request, "/asset.mp4", http.StatusTemporaryRedirect)
		case "/asset.mp4":
			if request.Header.Get("X-Input-Secret") != "secret" {
				http.Error(writer, "same-origin credential missing", http.StatusBadRequest)
				return
			}
			_, _ = io.WriteString(writer, "redirected")
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	items, err := New(Config{HTTPHeaders: http.Header{"X-Input-Secret": []string{"secret"}}}).
		Resolve(context.Background(), []string{server.URL + "/redirect"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := items[0].Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(data) != "redirected" {
		t.Fatalf("redirected data = %q, error = %v", data, err)
	}
}

func TestResolveHTTPRejectsUserinfo(t *testing.T) {
	t.Parallel()
	_, err := New(Config{}).Resolve(context.Background(), []string{"https://user:secret@example.test/asset.mp4"})
	if err == nil || !strings.Contains(err.Error(), "must not include userinfo") {
		t.Fatalf("Resolve() error = %v, want HTTP userinfo rejection", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("Resolve() error disclosed URL credentials: %v", err)
	}
}

func TestResolveS3Prefix(t *testing.T) {
	t.Parallel()
	client := &fakeS3{objects: map[string][]byte{"prefix/b.mp4": []byte("b"), "prefix/a.mp4": []byte("aa"), "other": []byte("ignored")}}
	items, err := New(Config{S3Client: client}).Resolve(context.Background(), []string{"s3://media/prefix/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].URI != "s3://media/prefix/a.mp4" || items[1].URI != "s3://media/prefix/b.mp4" {
		t.Fatalf("unexpected S3 items: %#v", items)
	}
	reader, err := items[0].Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(reader)
	_ = reader.Close()
	if string(data) != "aa" {
		t.Fatalf("S3 object data = %q", data)
	}
}
func TestHTTPSourceErrorsDoNotLeakCredentials(t *testing.T) {
	t.Parallel()
	client := &http.Client{Transport: sourceRoundTripError(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial https://example.test/media?signature=top-secret")
	})}
	items, err := New(Config{HTTPClient: client}).Resolve(context.Background(), []string{"https://example.test/media?signature=input-secret"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := items[0].Open(context.Background())
	if reader != nil {
		_ = reader.Close()
	}
	if err == nil {
		t.Fatal("HTTP input unexpectedly opened")
	}
	if strings.Contains(err.Error(), "top-secret") || strings.Contains(err.Error(), "input-secret") {
		t.Fatalf("HTTP input error leaked credentials: %v", err)
	}
}

type sourceRoundTripError func(*http.Request) (*http.Response, error)

func (function sourceRoundTripError) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type fakeS3 struct {
	objects map[string][]byte
	// listCalls counts prefix listings, which is how a test tells an exact key
	// apart from one that quietly became a prefix.
	listCalls int
}

type pagedS3 struct {
	fakeS3
	pages []*s3.ListObjectsV2Output
	calls int
}

func (s *pagedS3) ListObjectsV2(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	index := min(s.calls, len(s.pages)-1)
	s.calls++
	return s.pages[index], nil
}

func TestResolveS3RejectsNonAdvancingPagination(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name  string
		pages []*s3.ListObjectsV2Output
	}{
		{
			name: "missing token on truncated page",
			pages: []*s3.ListObjectsV2Output{{
				IsTruncated: aws.Bool(true),
				Contents:    []types.Object{{Key: aws.String("prefix/one.ts"), Size: aws.Int64(1)}},
			}},
		},
		{
			name: "repeated token",
			pages: []*s3.ListObjectsV2Output{
				{IsTruncated: aws.Bool(true), NextContinuationToken: aws.String("same"), Contents: []types.Object{{Key: aws.String("prefix/one.ts"), Size: aws.Int64(1)}}},
				{IsTruncated: aws.Bool(true), NextContinuationToken: aws.String("same"), Contents: []types.Object{{Key: aws.String("prefix/two.ts"), Size: aws.Int64(1)}}},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			client := &pagedS3{fakeS3: fakeS3{objects: map[string][]byte{}}, pages: testCase.pages}
			_, err := New(Config{S3Client: client}).Resolve(context.Background(), []string{"s3://bucket/prefix/"})
			if err == nil || !strings.Contains(err.Error(), "continuation token") {
				t.Fatalf("Resolve() error = %v, want pagination-token rejection", err)
			}
			if client.calls > 2 {
				t.Fatalf("ListObjectsV2 calls = %d, want bounded failure", client.calls)
			}
		})
	}
}

type stalledS3 struct {
	fakeS3
	getCalls int
}

func (s *stalledS3) GetObject(ctx context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	s.getCalls++
	return &s3.GetObjectOutput{Body: &contextBody{ctx: ctx}}, nil
}

type contextBody struct{ ctx context.Context }

func (b *contextBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (*contextBody) Close() error { return nil }

type stalledS3Metadata struct{ fakeS3 }

func (s *stalledS3Metadata) HeadObject(ctx context.Context, _ *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type toxicMetadataS3 struct{ fakeS3 }

func (s *toxicMetadataS3) HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return nil, errors.New("peer-response-top-secret")
}

func (s *toxicMetadataS3) ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return nil, errors.New("peer-response-top-secret")
}

func TestS3MetadataErrorsExposeOnlyStableProcessMessages(t *testing.T) {
	t.Parallel()
	for _, input := range []string{"s3://bucket/exact.ts", "s3://bucket/prefix/"} {
		resolver := New(Config{S3Client: &toxicMetadataS3{fakeS3{objects: map[string][]byte{}}}})
		_, err := resolver.Resolve(context.Background(), []string{input})
		if err == nil || !strings.Contains(err.Error(), "peer-response-top-secret") {
			t.Fatalf("detailed in-process error was not retained for %s: %v", input, err)
		}
		var public interface{ PublicMessage() string }
		if !errors.As(err, &public) {
			t.Fatalf("S3 metadata error has no safe process projection: %T", err)
		}
		if message := public.PublicMessage(); strings.Contains(message, "top-secret") || !strings.Contains(message, "S3") {
			t.Fatalf("unsafe or unhelpful process message for %s: %q", input, message)
		}
	}
}

// HeadObject answers 404 for a key that is not there, as S3 does. Answering
// success for everything would hide the distinction this fake exists to test.
func (f *fakeS3) HeadObject(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	data, present := f.objects[aws.ToString(input.Key)]
	if !present {
		return nil, &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusNotFound}},
			Err:      errors.New("NotFound"),
		}
	}
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(data)))}, nil
}

func (f *fakeS3) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(f.objects[aws.ToString(input.Key)]))}, nil
}

func (f *fakeS3) ListObjectsV2(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.listCalls++
	prefix := aws.ToString(input.Prefix)
	output := &s3.ListObjectsV2Output{}
	for key, data := range f.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			output.Contents = append(output.Contents, types.Object{Key: aws.String(key), Size: aws.Int64(int64(len(data)))})
		}
	}
	return output, nil
}

type scriptedS3 struct {
	fakeS3
	getObject func(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
}

func (client *scriptedS3) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if client.getObject != nil {
		return client.getObject(input)
	}
	return client.fakeS3.GetObject(context.Background(), input)
}

func fullS3Object(contents, etag, versionID string) *s3.GetObjectOutput {
	output := &s3.GetObjectOutput{
		Body:          io.NopCloser(strings.NewReader(contents)),
		ContentLength: aws.Int64(int64(len(contents))),
	}
	if etag != "" {
		output.ETag = aws.String(etag)
	}
	if versionID != "" {
		output.VersionId = aws.String(versionID)
	}
	return output
}

func s3ItemAfterFirstRead(t *testing.T, client S3API, contents string) Item {
	t.Helper()
	items, err := New(Config{S3Client: client}).Resolve(context.Background(), []string{"s3://media/asset.mxf"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := items[0].Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(reader)
	_ = reader.Close()
	if readErr != nil || string(body) != contents {
		t.Fatalf("initial S3 body = %q, error = %v", body, readErr)
	}
	return items[0]
}

// TestHTTPSourceResumesWithARangeRequest covers the protocol side of resuming a
// broken transfer: the range asked for, and the validator that makes appending
// to what is already staged safe.
func TestHTTPSourceResumesWithARangeRequest(t *testing.T) {
	t.Parallel()
	const body = "time-addressable media store"
	const etag = `"v1"`
	var (
		lock       sync.Mutex
		sawRange   string
		sawIfRange string
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("ETag", etag)
		rangeHeader := request.Header.Get("Range")
		if rangeHeader == "" {
			_, _ = io.WriteString(writer, body)
			return
		}
		lock.Lock()
		sawRange, sawIfRange = rangeHeader, request.Header.Get("If-Range")
		lock.Unlock()
		var offset int
		if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-", &offset); err != nil || offset > len(body) {
			writer.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		writer.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, len(body)-1, len(body)))
		writer.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(writer, body[offset:])
	}))
	defer server.Close()

	resolver := New(Config{})
	items, err := resolver.Resolve(context.Background(), []string{server.URL + "/asset.mp4"})
	if err != nil {
		t.Fatal(err)
	}
	item := items[0]
	if item.Reopen == nil {
		t.Fatal("an HTTP source must be resumable")
	}

	// The first read is what teaches the item which resource it is reading.
	first, err := item.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(first)
	_ = first.Close()

	const offset = 10
	rest, resumed, err := item.Reopen(context.Background(), offset)
	if err != nil {
		t.Fatal(err)
	}
	defer rest.Close()
	if !resumed {
		t.Fatal("a 206 answer means the stream continues where it left off")
	}
	remainder, err := io.ReadAll(rest)
	if err != nil {
		t.Fatal(err)
	}
	if string(remainder) != body[offset:] {
		t.Fatalf("resumed body = %q, want %q", remainder, body[offset:])
	}

	lock.Lock()
	defer lock.Unlock()
	if sawRange != fmt.Sprintf("bytes=%d-", offset) {
		t.Fatalf("Range header = %q", sawRange)
	}
	if sawIfRange != etag {
		t.Fatalf("If-Range = %q, want %q: without it the server may splice a changed resource onto what is staged",
			sawIfRange, etag)
	}
}

// TestHTTPSourceReportsAResourceItCannotResume covers a server that answers a
// ranged request with the whole body, which is what it must do when the
// resource has changed. The caller has to be told, because appending that body
// to what is already staged would produce a file matching neither version.
func TestHTTPSourceReportsAResourceItCannotResume(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		// Ignores Range entirely, as a server may.
		_, _ = io.WriteString(writer, "replacement contents")
	}))
	defer server.Close()

	resolver := New(Config{})
	items, err := resolver.Resolve(context.Background(), []string{server.URL + "/asset.mp4"})
	if err != nil {
		t.Fatal(err)
	}
	body, resumed, err := items[0].Reopen(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	if resumed {
		t.Fatal("a 200 answer to a ranged request must not be reported as a resume")
	}
	contents, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "replacement contents" {
		t.Fatalf("body = %q", contents)
	}
}

// rangeServer serves body, optionally lying about which bytes it is sending.
// A server that answers a range request with the wrong span is the case that
// silently corrupts a staged file, because the bytes are appended as though
// they were contiguous and the resulting digest authenticates the composite.
type rangeServer struct {
	body         string
	etag         string
	omitRange    bool  // answer 206 without saying which bytes
	startLie     int64 // report a different start than the one requested
	endLie       int64 // report a different end than the final byte
	totalLie     int64 // report a different total
	unknownTotal bool
	encoding     string
}

func (s rangeServer) handler() http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if s.etag != "" {
			writer.Header().Set("ETag", s.etag)
		}
		rangeHeader := request.Header.Get("Range")
		if rangeHeader == "" {
			_, _ = io.WriteString(writer, s.body)
			return
		}
		// Applied only to the ranged answer, so the first read succeeds and the
		// test exercises the resume rather than the initial transfer.
		if s.encoding != "" {
			writer.Header().Set("Content-Encoding", s.encoding)
		}
		var offset int64
		_, _ = fmt.Sscanf(rangeHeader, "bytes=%d-", &offset)
		if !s.omitRange {
			start, end, total := offset, int64(len(s.body)-1), int64(len(s.body))
			if s.startLie != 0 {
				start = s.startLie
			}
			if s.endLie != 0 {
				end = s.endLie
			}
			if s.totalLie != 0 {
				total = s.totalLie
			}
			totalValue := fmt.Sprint(total)
			if s.unknownTotal {
				totalValue = "*"
			}
			writer.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%s", start, end, totalValue))
		}
		writer.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(writer, s.body[offset:])
	}
}

// httpItemAfterFirstRead returns a resolved item that has already read the
// resource once, which is what teaches it the representation to hold the server
// to on a resume.
func httpItemAfterFirstRead(t *testing.T, handler http.HandlerFunc) (Item, func()) {
	t.Helper()
	server := httptest.NewServer(handler)
	resolver := New(Config{})
	items, err := resolver.Resolve(context.Background(), []string{server.URL + "/asset.mp4"})
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	body, err := items[0].Open(context.Background())
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	_, _ = io.ReadAll(body)
	_ = body.Close()
	return items[0], server.Close
}

// TestHTTPResumeRefusesAnUntrustworthyRange covers every way a server can answer a
// range request that must not be appended to what is already staged. None of
// these can be caught later: the staged digest is computed over whatever was
// written, so a spliced file verifies perfectly against itself.
func TestHTTPResumeRefusesAnUntrustworthyRange(t *testing.T) {
	t.Parallel()
	const body = "time-addressable media store"
	for _, testCase := range []struct {
		name   string
		server rangeServer
		want   string
	}{
		{
			name:   "a range starting somewhere else",
			server: rangeServer{body: body, etag: `"v1"`, startLie: 3},
			want:   "resumed at byte 3 after asking for 10",
		},
		{
			name:   "a continuation that does not say which bytes",
			server: rangeServer{body: body, etag: `"v1"`, omitRange: true},
			want:   "did not state which bytes",
		},
		{
			name:   "a resource that changed length",
			server: rangeServer{body: body, etag: `"v1"`, totalLie: 999},
			want:   "resource is now 999 bytes",
		},
		{
			name:   "a range that stops before the representation ends",
			server: rangeServer{body: body, etag: `"v1"`, endLie: 20},
			want:   "not the final byte",
		},
		{
			name:   "a range whose end equals its total",
			server: rangeServer{body: body, etag: `"v1"`, endLie: int64(len(body))},
			want:   "malformed Content-Range",
		},
		{
			name:   "a range with no representation total",
			server: rangeServer{body: body, etag: `"v1"`, unknownTotal: true},
			want:   "malformed Content-Range",
		},
		{
			// Byte offsets address the bytes on the wire. A compressed body is
			// a different coordinate system, so a range into it is meaningless.
			name:   "a body the server compressed",
			server: rangeServer{body: body, etag: `"v1"`, encoding: "gzip"},
			want:   "which byte ranges cannot address",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			item, closeServer := httpItemAfterFirstRead(t, testCase.server.handler())
			defer closeServer()

			reader, resumed, err := item.Reopen(context.Background(), 10)
			if reader != nil {
				_ = reader.Close()
			}
			if err == nil {
				t.Fatalf("resume was accepted (resumed=%v); it must be refused", resumed)
			}
			if !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("error = %q, want it to mention %q", err, testCase.want)
			}
		})
	}
}

func TestHTTPResumeRequiresAStrongETag(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name string
		etag string
	}{
		{name: "Last-Modified only"},
		{name: "weak ETag", etag: `W/"v1"`},
		{name: "malformed ETag", etag: "v1"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			const contents = "one complete representation"
			var lock sync.Mutex
			var sawRange bool
			item, closeServer := httpItemAfterFirstRead(t, func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Last-Modified", "Sat, 08 Aug 2026 12:00:00 GMT")
				if testCase.etag != "" {
					writer.Header().Set("ETag", testCase.etag)
				}
				lock.Lock()
				sawRange = sawRange || request.Header.Get("Range") != ""
				lock.Unlock()
				_, _ = io.WriteString(writer, contents)
			})
			defer closeServer()

			reader, resumed, err := item.Reopen(context.Background(), 5)
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(reader)
			_ = reader.Close()
			if readErr != nil {
				t.Fatal(readErr)
			}
			if resumed || string(body) != contents {
				t.Fatalf("resumed = %v, body = %q; want a complete restart", resumed, body)
			}
			lock.Lock()
			defer lock.Unlock()
			if sawRange {
				t.Fatal("sent a Range request without a valid strong ETag")
			}
		})
	}
}

func TestHTTPResumeBodyMustMatchItsDeclaredRange(t *testing.T) {
	t.Parallel()
	const (
		contents = "time-addressable media store"
		offset   = int64(10)
	)
	for _, testCase := range []struct {
		name string
		body string
	}{
		{name: "clean short body", body: contents[offset:26]},
		{name: "body beyond the range", body: contents[offset:] + "x"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			client := &http.Client{Transport: sourceRoundTripError(func(request *http.Request) (*http.Response, error) {
				if request.Header.Get("Range") == "" {
					return &http.Response{
						StatusCode:    http.StatusOK,
						Header:        http.Header{"Etag": []string{`"v1"`}},
						Body:          io.NopCloser(strings.NewReader(contents)),
						ContentLength: int64(len(contents)),
					}, nil
				}
				return &http.Response{
					StatusCode: http.StatusPartialContent,
					Header: http.Header{
						"Content-Range": []string{fmt.Sprintf("bytes %d-%d/%d", offset, len(contents)-1, len(contents))},
					},
					Body:          io.NopCloser(strings.NewReader(testCase.body)),
					ContentLength: -1,
				}, nil
			})}
			items, err := New(Config{HTTPClient: client}).Resolve(context.Background(), []string{"https://media.example/asset.mxf"})
			if err != nil {
				t.Fatal(err)
			}
			initial, err := items[0].Open(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.ReadAll(initial)
			_ = initial.Close()

			reader, resumed, err := items[0].Reopen(context.Background(), offset)
			if err != nil {
				t.Fatal(err)
			}
			_, err = io.ReadAll(reader)
			_ = reader.Close()
			if !resumed || err == nil {
				t.Fatalf("resumed = %v, read error = %v; an inexact body must fail while reading", resumed, err)
			}
		})
	}
}

func TestHTTPRangeNotSatisfiable(t *testing.T) {
	t.Parallel()
	const contents = "one complete representation"
	for _, testCase := range []struct {
		name        string
		offset      int64
		rangeTotal  string
		wantResumed bool
		wantBody    string
		wantFull    int
	}{
		{
			name:        "staged prefix is already complete",
			offset:      int64(len(contents)),
			rangeTotal:  fmt.Sprint(len(contents)),
			wantResumed: true,
			wantFull:    1,
		},
		{
			name:       "offset is beyond the representation",
			offset:     int64(len(contents) + 1),
			rangeTotal: fmt.Sprint(len(contents)),
			wantBody:   contents,
			wantFull:   2,
		},
		{
			name:       "server reports a different total",
			offset:     int64(len(contents)),
			rangeTotal: fmt.Sprint(len(contents) + 1),
			wantBody:   contents,
			wantFull:   2,
		},
		{
			name:     "server omits the total",
			offset:   int64(len(contents)),
			wantBody: contents,
			wantFull: 2,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var lock sync.Mutex
			fullRequests := 0
			item, closeServer := httpItemAfterFirstRead(t, func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("ETag", `"v1"`)
				if request.Header.Get("Range") != "" {
					if testCase.rangeTotal != "" {
						writer.Header().Set("Content-Range", "bytes */"+testCase.rangeTotal)
					}
					writer.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
					return
				}
				lock.Lock()
				fullRequests++
				lock.Unlock()
				_, _ = io.WriteString(writer, contents)
			})
			defer closeServer()

			reader, resumed, err := item.Reopen(context.Background(), testCase.offset)
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(reader)
			_ = reader.Close()
			if readErr != nil {
				t.Fatal(readErr)
			}
			if resumed != testCase.wantResumed || string(body) != testCase.wantBody {
				t.Fatalf("resumed = %v, body = %q; want resumed = %v, body = %q",
					resumed, body, testCase.wantResumed, testCase.wantBody)
			}
			lock.Lock()
			defer lock.Unlock()
			if fullRequests != testCase.wantFull {
				t.Fatalf("full requests = %d, want %d", fullRequests, testCase.wantFull)
			}
		})
	}
}

// TestHTTPResumeWithoutAValidatorStartsAgain covers a server that offers nothing
// to hold it to. Asking for a range anyway would be taking the continuation on
// trust, so the transfer restarts instead.
func TestHTTPResumeWithoutAValidatorStartsAgain(t *testing.T) {
	t.Parallel()
	const body = "no validator here"
	var sawRange bool
	item, closeServer := httpItemAfterFirstRead(t, func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Range") != "" {
			sawRange = true
		}
		_, _ = io.WriteString(writer, body)
	})
	defer closeServer()

	reader, resumed, err := item.Reopen(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if resumed {
		t.Fatal("reported a resume from a server that published no validator")
	}
	if sawRange {
		t.Fatal("asked for a range with nothing to hold the server to")
	}
	contents, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != body {
		t.Fatalf("body = %q, want the whole resource", contents)
	}
}

// TestHTTPRestartReplacesTheValidator covers the second interruption after a
// resource changed. Carrying the original validator forward would make every
// later resume ask about a representation that no longer exists, so the restart
// has to take the new one.
func TestHTTPRestartReplacesTheValidator(t *testing.T) {
	t.Parallel()
	var (
		lock       sync.Mutex
		etag       = `"v1"`
		ifRanges   []string
		bodyLength = 40
	)
	item, closeServer := httpItemAfterFirstRead(t, func(writer http.ResponseWriter, request *http.Request) {
		lock.Lock()
		current := etag
		if value := request.Header.Get("If-Range"); value != "" {
			ifRanges = append(ifRanges, value)
		}
		lock.Unlock()
		writer.Header().Set("ETag", current)
		// Always answers whole, as a server does when the resource changed.
		_, _ = io.WriteString(writer, strings.Repeat("x", bodyLength))
	})
	defer closeServer()

	// The resource is replaced between attempts.
	lock.Lock()
	etag = `"v2"`
	lock.Unlock()

	first, _, err := item.Reopen(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(first)
	_ = first.Close()

	second, _, err := item.Reopen(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	_ = second.Close()

	lock.Lock()
	defer lock.Unlock()
	if len(ifRanges) < 2 {
		t.Fatalf("expected two conditional resumes, saw %v", ifRanges)
	}
	if last := ifRanges[len(ifRanges)-1]; last != `"v2"` {
		t.Fatalf("second resume asked about %s; the validator was not replaced after the restart", last)
	}
}

// TestHTTPTransfersAskForIdentityEncoding pins the request side of the same
// problem. Go negotiates gzip and transparently decodes it unless told not to,
// which would leave offsets counted in decoded bytes and requested in encoded
// ones.
func TestHTTPTransfersAskForIdentityEncoding(t *testing.T) {
	t.Parallel()
	var encodings []string
	var lock sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		lock.Lock()
		encodings = append(encodings, request.Header.Get("Accept-Encoding"))
		lock.Unlock()
		_, _ = io.WriteString(writer, "media")
	}))
	defer server.Close()

	resolver := New(Config{})
	items, err := resolver.Resolve(context.Background(), []string{server.URL + "/asset.mp4"})
	if err != nil {
		t.Fatal(err)
	}
	body, err := items[0].Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(body)
	_ = body.Close()

	lock.Lock()
	defer lock.Unlock()
	for _, encoding := range encodings {
		if encoding != "identity" {
			t.Fatalf("Accept-Encoding = %q, want identity so byte offsets address the bytes on the wire", encoding)
		}
	}
}

func TestS3SourceResumesOnePinnedCompleteRepresentation(t *testing.T) {
	t.Parallel()
	const (
		contents  = "time-addressable media store"
		etag      = `"object-v1"`
		versionID = "version-1"
		offset    = int64(10)
	)
	type getCall struct {
		rangeHeader string
		ifMatch     string
		versionID   string
	}
	var lock sync.Mutex
	var calls []getCall
	client := &scriptedS3{fakeS3: fakeS3{objects: map[string][]byte{"asset.mxf": []byte(contents)}}}
	client.getObject = func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		lock.Lock()
		calls = append(calls, getCall{
			rangeHeader: aws.ToString(input.Range),
			ifMatch:     aws.ToString(input.IfMatch),
			versionID:   aws.ToString(input.VersionId),
		})
		lock.Unlock()
		if input.Range == nil {
			return fullS3Object(contents, etag, versionID), nil
		}
		return &s3.GetObjectOutput{
			Body:          io.NopCloser(strings.NewReader(contents[offset:])),
			ContentLength: aws.Int64(int64(len(contents)) - offset),
			ContentRange:  aws.String(fmt.Sprintf("bytes %d-%d/%d", offset, len(contents)-1, len(contents))),
			ETag:          aws.String(etag),
			VersionId:     aws.String(versionID),
		}, nil
	}

	item := s3ItemAfterFirstRead(t, client, contents)
	reader, resumed, err := item.Reopen(context.Background(), offset)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(reader)
	_ = reader.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !resumed || string(body) != contents[offset:] {
		t.Fatalf("resumed = %v, body = %q", resumed, body)
	}

	lock.Lock()
	defer lock.Unlock()
	if len(calls) != 2 {
		t.Fatalf("GetObject calls = %d, want 2", len(calls))
	}
	resume := calls[1]
	if resume.rangeHeader != "bytes=10-" || resume.ifMatch != etag || resume.versionID != versionID {
		t.Fatalf("resume request = %#v", resume)
	}
}

func TestS3ResumeRejectsAnIncompleteOrAmbiguousRange(t *testing.T) {
	t.Parallel()
	const (
		contents = "time-addressable media store"
		etag     = `"object-v1"`
		offset   = int64(10)
	)
	for _, testCase := range []struct {
		name          string
		contentRange  *string
		contentLength *int64
		body          string
		responseETag  string
	}{
		{name: "missing Content-Range", body: contents[offset:], contentLength: aws.Int64(int64(len(contents)) - offset)},
		{name: "wrong start", contentRange: aws.String("bytes 9-27/28"), body: contents[9:], contentLength: aws.Int64(19)},
		{name: "changed total", contentRange: aws.String("bytes 10-28/29"), body: contents[offset:] + "x", contentLength: aws.Int64(19)},
		{name: "non-final end", contentRange: aws.String("bytes 10-20/28"), body: contents[offset:21], contentLength: aws.Int64(11)},
		{name: "end equals total", contentRange: aws.String("bytes 10-28/28"), body: contents[offset:] + "x", contentLength: aws.Int64(19)},
		{name: "unknown total", contentRange: aws.String("bytes 10-27/*"), body: contents[offset:], contentLength: aws.Int64(18)},
		{name: "declared body span mismatch", contentRange: aws.String("bytes 10-27/28"), body: contents[offset:], contentLength: aws.Int64(17)},
		{name: "clean short body", contentRange: aws.String("bytes 10-27/28"), body: contents[offset:26]},
		{name: "body beyond declared range", contentRange: aws.String("bytes 10-27/28"), body: contents[offset:] + "x"},
		{name: "changed ETag", contentRange: aws.String("bytes 10-27/28"), body: contents[offset:], contentLength: aws.Int64(18), responseETag: `"object-v2"`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			client := &scriptedS3{fakeS3: fakeS3{objects: map[string][]byte{"asset.mxf": []byte(contents)}}}
			client.getObject = func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
				if input.Range == nil {
					return fullS3Object(contents, etag, ""), nil
				}
				output := &s3.GetObjectOutput{
					Body:          io.NopCloser(strings.NewReader(testCase.body)),
					ContentLength: testCase.contentLength,
					ContentRange:  testCase.contentRange,
				}
				if testCase.responseETag != "" {
					output.ETag = aws.String(testCase.responseETag)
				}
				return output, nil
			}

			item := s3ItemAfterFirstRead(t, client, contents)
			reader, _, err := item.Reopen(context.Background(), offset)
			if err == nil {
				_, err = io.ReadAll(reader)
			}
			if reader != nil {
				_ = reader.Close()
			}
			if err == nil {
				t.Fatal("ambiguous S3 range was accepted")
			}
		})
	}
}

func TestS3ResumeWithoutAValidatorStartsAgain(t *testing.T) {
	t.Parallel()
	const contents = "one complete object"
	var lock sync.Mutex
	var ranges []string
	client := &scriptedS3{fakeS3: fakeS3{objects: map[string][]byte{"asset.mxf": []byte(contents)}}}
	client.getObject = func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		lock.Lock()
		ranges = append(ranges, aws.ToString(input.Range))
		lock.Unlock()
		return fullS3Object(contents, "", ""), nil
	}
	item := s3ItemAfterFirstRead(t, client, contents)
	reader, resumed, err := item.Reopen(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(reader)
	_ = reader.Close()
	if readErr != nil || resumed || string(body) != contents {
		t.Fatalf("resumed = %v, body = %q, error = %v", resumed, body, readErr)
	}
	lock.Lock()
	defer lock.Unlock()
	if len(ranges) != 2 || ranges[0] != "" || ranges[1] != "" {
		t.Fatalf("Range headers = %q; an unpinned object must restart", ranges)
	}
}

func TestS3PreconditionFailureStartsAgain(t *testing.T) {
	t.Parallel()
	const (
		oldContents = "old representation"
		newContents = "replacement representation"
	)
	var lock sync.Mutex
	fullRequests := 0
	client := &scriptedS3{fakeS3: fakeS3{objects: map[string][]byte{"asset.mxf": []byte(oldContents)}}}
	client.getObject = func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		if input.Range != nil {
			return nil, &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusPreconditionFailed, Body: http.NoBody}},
				Err:      errors.New("PreconditionFailed"),
			}
		}
		lock.Lock()
		fullRequests++
		requestNumber := fullRequests
		lock.Unlock()
		if requestNumber == 1 {
			return fullS3Object(oldContents, `"v1"`, ""), nil
		}
		return fullS3Object(newContents, `"v2"`, ""), nil
	}
	item := s3ItemAfterFirstRead(t, client, oldContents)
	reader, resumed, err := item.Reopen(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(reader)
	_ = reader.Close()
	if readErr != nil || resumed || string(body) != newContents {
		t.Fatalf("resumed = %v, body = %q, error = %v", resumed, body, readErr)
	}
}

func TestS3RangeNotSatisfiable(t *testing.T) {
	t.Parallel()
	const contents = "one complete object"
	for _, testCase := range []struct {
		name        string
		offset      int64
		rangeTotal  string
		wantResumed bool
		wantBody    string
		wantFull    int
	}{
		{
			name:        "staged prefix is already complete",
			offset:      int64(len(contents)),
			rangeTotal:  fmt.Sprint(len(contents)),
			wantResumed: true,
			wantFull:    1,
		},
		{
			name:       "offset is beyond the object",
			offset:     int64(len(contents) + 1),
			rangeTotal: fmt.Sprint(len(contents)),
			wantBody:   contents,
			wantFull:   2,
		},
		{
			name:       "reported total changed",
			offset:     int64(len(contents)),
			rangeTotal: fmt.Sprint(len(contents) + 1),
			wantBody:   contents,
			wantFull:   2,
		},
		{
			name:     "missing Content-Range",
			offset:   int64(len(contents)),
			wantBody: contents,
			wantFull: 2,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var lock sync.Mutex
			fullRequests := 0
			client := &scriptedS3{fakeS3: fakeS3{objects: map[string][]byte{"asset.mxf": []byte(contents)}}}
			client.getObject = func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
				if input.Range == nil {
					lock.Lock()
					fullRequests++
					lock.Unlock()
					return fullS3Object(contents, `"v1"`, ""), nil
				}
				header := make(http.Header)
				if testCase.rangeTotal != "" {
					header.Set("Content-Range", "bytes */"+testCase.rangeTotal)
				}
				return nil, &smithyhttp.ResponseError{
					Response: &smithyhttp.Response{Response: &http.Response{
						StatusCode: http.StatusRequestedRangeNotSatisfiable,
						Header:     header,
						Body:       http.NoBody,
					}},
					Err: errors.New("InvalidRange"),
				}
			}

			item := s3ItemAfterFirstRead(t, client, contents)
			reader, resumed, err := item.Reopen(context.Background(), testCase.offset)
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(reader)
			_ = reader.Close()
			if readErr != nil {
				t.Fatal(readErr)
			}
			if resumed != testCase.wantResumed || string(body) != testCase.wantBody {
				t.Fatalf("resumed = %v, body = %q; want resumed = %v, body = %q",
					resumed, body, testCase.wantResumed, testCase.wantBody)
			}
			lock.Lock()
			defer lock.Unlock()
			if fullRequests != testCase.wantFull {
				t.Fatalf("full requests = %d, want %d", fullRequests, testCase.wantFull)
			}
		})
	}
}

// TestS3KeyWithoutASlashIsExact covers a mistyped object key.
//
// A key that does not end in a slash names one object. Falling back to a prefix
// listing when it is absent turns a typo into a silent import of whatever else
// shares the prefix: ask for programme.mxf, get programme.mxf.bak and
// programme.mxf.tmp, with nothing to say the file you named was never there.
// Ingesting the wrong media without an error is the worst available outcome.
func TestS3KeyWithoutASlashIsExact(t *testing.T) {
	t.Parallel()
	newResolver := func(client *fakeS3) *Resolver {
		resolver := New(Config{})
		resolver.s3Client = client
		return resolver
	}

	t.Run("a key that exists imports exactly it", func(t *testing.T) {
		t.Parallel()
		client := &fakeS3{objects: map[string][]byte{
			"programme.mxf":     []byte("wanted"),
			"programme.mxf.bak": []byte("not wanted"),
		}}
		items, err := newResolver(client).Resolve(context.Background(), []string{"s3://media/programme.mxf"})
		if err != nil {
			t.Fatal(err)
		}
		if len(items) != 1 || !strings.HasSuffix(items[0].URI, "/programme.mxf") {
			t.Fatalf("expected only the named object, got %d: %v", len(items), items)
		}
		if client.listCalls != 0 {
			t.Fatalf("an existing exact key should need no prefix listing, saw %d", client.listCalls)
		}
	})

	t.Run("a key that does not exist fails", func(t *testing.T) {
		t.Parallel()
		client := &fakeS3{objects: map[string][]byte{
			"programme.mxf.bak": []byte("not wanted"),
			"programme.mxf.tmp": []byte("also not wanted"),
		}}
		_, err := newResolver(client).Resolve(context.Background(), []string{"s3://media/programme.mxf"})
		if err == nil {
			t.Fatal("a missing object key must fail rather than importing its neighbours")
		}
		if !strings.Contains(err.Error(), "does not exist") {
			t.Fatalf("error = %q, want it to say the object is not there", err)
		}
		if client.listCalls != 0 {
			t.Fatalf("a missing exact key must not fall back to a prefix listing, saw %d", client.listCalls)
		}
	})

	t.Run("a trailing slash still imports the prefix", func(t *testing.T) {
		t.Parallel()
		client := &fakeS3{objects: map[string][]byte{
			"day-001/a.ts": []byte("one"),
			"day-001/b.ts": []byte("two"),
			"day-002/c.ts": []byte("elsewhere"),
		}}
		items, err := newResolver(client).Resolve(context.Background(), []string{"s3://media/day-001/"})
		if err != nil {
			t.Fatal(err)
		}
		if len(items) != 2 {
			t.Fatalf("expected the two objects beneath the prefix, got %d: %v", len(items), items)
		}
	})
}

func TestHTTPSourceBodyIdleTimeout(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("prefix"))
		writer.(http.Flusher).Flush()
		<-request.Context().Done()
	}))
	defer server.Close()

	resolver := New(Config{TransferIdleTimeout: 80 * time.Millisecond})
	items, err := resolver.Resolve(context.Background(), []string{server.URL + "/programme.ts"})
	if err != nil {
		t.Fatal(err)
	}
	body, err := items[0].Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(body)
	_ = body.Close()
	var idle *netio.IdleTimeoutError
	if !errors.As(err, &idle) {
		t.Fatalf("read error = %v, want idle timeout", err)
	}
}

func TestHTTPSourceRequestPreservesParentCause(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	defer server.Close()

	resolver := New(Config{TransferIdleTimeout: time.Minute})
	items, err := resolver.Resolve(context.Background(), []string{server.URL + "/programme.ts"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	want := errors.New("another input failed")
	done := make(chan error, 1)
	go func() {
		_, openErr := items[0].Open(ctx)
		done <- openErr
	}()
	cancel(want)
	if err := <-done; !errors.Is(err, want) {
		t.Fatalf("Open error = %v, want parent cause %v", err, want)
	}
}

func TestS3SourceBodyIdleTimeout(t *testing.T) {
	t.Parallel()
	client := &stalledS3{fakeS3: fakeS3{objects: map[string][]byte{"programme.mxf": []byte("media")}}}
	resolver := New(Config{S3Client: client, TransferIdleTimeout: 80 * time.Millisecond})
	items, err := resolver.Resolve(context.Background(), []string{"s3://archive/programme.mxf"})
	if err != nil {
		t.Fatal(err)
	}
	body, err := items[0].Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(body)
	_ = body.Close()
	var idle *netio.IdleTimeoutError
	if !errors.As(err, &idle) {
		t.Fatalf("read error = %v, want idle timeout", err)
	}
	if client.getCalls != 1 {
		t.Fatalf("GetObject calls = %d, want 1", client.getCalls)
	}
}

func TestS3MetadataUsesTheMetadataDeadline(t *testing.T) {
	t.Parallel()
	client := &stalledS3Metadata{fakeS3: fakeS3{objects: map[string][]byte{"programme.mxf": []byte("media")}}}
	resolver := New(Config{S3Client: client, MetadataTimeout: 80 * time.Millisecond})
	started := time.Now()
	_, err := resolver.Resolve(context.Background(), []string{"s3://archive/programme.mxf"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Resolve error = %v, want metadata deadline", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("stalled S3 metadata took %s to fail", elapsed)
	}
}
