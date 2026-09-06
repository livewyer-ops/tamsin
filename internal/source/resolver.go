package source

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/livewyer-ops/tamsin/internal/auth"
	"github.com/livewyer-ops/tamsin/internal/netio"
	"github.com/livewyer-ops/tamsin/internal/observability"
)

const (
	maxManifestLine  = 4 << 20
	maxManifestDepth = 32
	// DefaultMaxInputs bounds expansion before a directory tree, manifest, or
	// S3 prefix can turn one argument into an unexpectedly large ingest. The
	// limit applies to unique resolved inputs, not to duplicate aliases.
	DefaultMaxInputs = 10_000
)

type OpenFunc func(context.Context) (io.ReadCloser, error)

// ReopenFunc resumes reading an item at offset.
//
// It reports whether the stream it returns actually begins there. A source that
// cannot honour the request answers from the beginning instead -- because the
// resource changed underneath us, or because the server does not do ranges --
// and the caller has to discard what it already staged rather than append to
// it. That is returned as a value rather than an error because it is a normal
// outcome the caller must act on, not a failure.
type ReopenFunc func(ctx context.Context, offset int64) (body io.ReadCloser, resumed bool, err error)

type Item struct {
	URI       string
	Name      string
	Size      int64
	LocalPath string
	Open      OpenFunc
	// Reopen resumes an interrupted read, and is nil for sources that cannot be
	// resumed. Standard input is the obvious one: the bytes are gone once read.
	Reopen ReopenFunc
}

type S3Config struct {
	Region       string
	Endpoint     string
	UsePathStyle bool
	HTTPClient   *http.Client
}

type Config struct {
	HTTPClient  *http.Client
	HTTPHeaders http.Header
	Stdin       io.Reader
	StdinName   string
	S3          S3Config
	S3Client    S3API
	// TransferIdleTimeout bounds time without byte-level progress in an HTTP
	// or S3 media body. A non-positive value selects netio.DefaultIdleTimeout.
	TransferIdleTimeout time.Duration
	// MetadataTimeout bounds S3 configuration, HeadObject, and ListObjectsV2
	// calls. A non-positive value selects 30 seconds.
	MetadataTimeout time.Duration
	// MaxInputs is the largest number of unique items one Resolve call may
	// produce. A non-positive value selects DefaultMaxInputs.
	MaxInputs int
	// Observability correlates source retries with the ingest which requested
	// them. It is optional for direct package callers.
	Observability *observability.Run
}

type S3API interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type Resolver struct {
	resolveMu           sync.Mutex
	httpClient          *http.Client
	httpHeaders         http.Header
	stdin               io.Reader
	stdinName           string
	s3Config            S3Config
	s3Client            S3API
	transferIdleTimeout time.Duration
	metadataTimeout     time.Duration
	maxInputs           int
	seen                map[string]struct{}
	manifests           map[string]struct{}
	stdinUsed           bool
	observability       *observability.Run
}

// publicError keeps detailed provider diagnostics available to in-process
// callers and error unwrapping while giving process front ends a bounded,
// disclosure-safe message. Provider API messages can reflect request material
// and must never be copied blindly into terminal output.
type publicError struct {
	message string
	err     error
}

func (e *publicError) Error() string         { return e.err.Error() }
func (e *publicError) Unwrap() error         { return e.err }
func (e *publicError) PublicMessage() string { return e.message }

func withPublicError(message string, err error) error {
	if err == nil {
		return nil
	}
	return &publicError{message: message, err: err}
}

func New(config Config) *Resolver {
	client := config.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	headers := config.HTTPHeaders.Clone()
	client = HTTPClientWithSafeRedirects(client, headers)
	stdin := config.Stdin
	if stdin == nil {
		stdin = os.Stdin
	}
	name := config.StdinName
	if name == "" {
		name = "stdin.bin"
	}
	idleTimeout := config.TransferIdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = netio.DefaultIdleTimeout
	}
	metadataTimeout := config.MetadataTimeout
	if metadataTimeout <= 0 {
		metadataTimeout = 30 * time.Second
	}
	maxInputs := config.MaxInputs
	if maxInputs <= 0 {
		maxInputs = DefaultMaxInputs
	}
	return &Resolver{
		httpClient:          client,
		httpHeaders:         headers,
		stdin:               stdin,
		stdinName:           name,
		s3Config:            config.S3,
		s3Client:            config.S3Client,
		transferIdleTimeout: idleTimeout,
		metadataTimeout:     metadataTimeout,
		maxInputs:           maxInputs,
		observability:       config.Observability,
		seen:                make(map[string]struct{}),
		manifests:           make(map[string]struct{}),
	}
}

// Resolve expands every input into a stable, de-duplicated list. Directories,
// manifests, and S3 prefixes are sorted by canonical URI before ingest starts.
func (r *Resolver) Resolve(ctx context.Context, inputs []string) ([]Item, error) {
	// Expansion state belongs to one call, not to the configured resolver. A
	// resolver is intentionally reusable by command and library callers; keeping
	// seen inputs or manifest/stdin state after a completed (or failed) call made
	// a later call silently omit otherwise valid inputs.
	r.resolveMu.Lock()
	defer r.resolveMu.Unlock()
	r.seen = make(map[string]struct{})
	r.manifests = make(map[string]struct{})
	r.stdinUsed = false

	if len(inputs) == 0 {
		return nil, errors.New("at least one input is required")
	}
	var items []Item
	for _, input := range inputs {
		resolved, err := r.resolveOne(ctx, strings.TrimSpace(input), "")
		if err != nil {
			return nil, err
		}
		items = append(items, resolved...)
	}
	if len(items) == 0 {
		return nil, errors.New("inputs resolved to no files or objects")
	}
	return items, nil
}

func (r *Resolver) resolveOne(ctx context.Context, input, relativeTo string) ([]Item, error) {
	if input == "" {
		return nil, errors.New("input cannot be empty")
	}
	if input == "-" {
		if r.stdinUsed {
			return nil, errors.New("stdin may only appear once")
		}
		r.stdinUsed = true
		item := Item{
			// The name is a probe/label hint, not a locator. Keeping it in the
			// provenance URI persisted arbitrary directory or token-like material
			// supplied through --stdin-name. Standard input has one honest stable
			// locator regardless of that hint.
			URI:  "stdin:",
			Name: filepath.Base(r.stdinName),
			Size: -1,
			Open: func(context.Context) (io.ReadCloser, error) {
				return io.NopCloser(r.stdin), nil
			},
		}
		return r.add(item)
	}

	parsed, err := url.Parse(input)
	if err != nil {
		return nil, errors.New("input URI is not valid")
	}
	switch strings.ToLower(parsed.Scheme) {
	case "s3":
		if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return nil, errors.New("S3 input must not include userinfo, a query, or a fragment")
		}
		return r.resolveS3(ctx, parsed)
	case "http", "https":
		if parsed.User != nil {
			return nil, errors.New("HTTP input must not include userinfo; use --input-header or configured authentication instead")
		}
		parsed.Fragment = ""
		return r.resolveHTTP(parsed)
	case "file":
		if parsed.RawQuery != "" || parsed.Fragment != "" {
			return nil, errors.New("file input must not include a query or fragment")
		}
		if parsed.Host != "" && !strings.EqualFold(parsed.Host, "localhost") {
			return nil, errors.New("file input host must be empty or localhost")
		}
		// url.Parse has already decoded Path. Decode EscapedPath instead so an
		// encoded percent sign is not interpreted a second time (for example,
		// literal%2520name must resolve to literal%20name, not literal name).
		path, err := url.PathUnescape(parsed.EscapedPath())
		if err != nil {
			return nil, fmt.Errorf("decode file URL %q: %w", input, err)
		}
		return r.resolveLocal(ctx, path)
	case "":
		if relativeTo != "" && !filepath.IsAbs(input) {
			input = filepath.Join(relativeTo, input)
		}
		return r.resolveLocal(ctx, input)
	default:
		return nil, fmt.Errorf("unsupported input scheme %q", parsed.Scheme)
	}
}

func (r *Resolver) resolveLocal(ctx context.Context, input string) ([]Item, error) {
	absolute, err := filepath.Abs(input)
	if err != nil {
		return nil, fmt.Errorf("resolve input path %q: %w", input, err)
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return nil, fmt.Errorf("stat input %q: %w", input, err)
	}
	if info.IsDir() {
		canonical, err := filepath.EvalSymlinks(absolute)
		if err != nil {
			return nil, fmt.Errorf("resolve directory %q: %w", input, err)
		}
		absolute = canonical
		var paths []string
		pending := make(map[string]struct{})
		err = filepath.WalkDir(absolute, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.Type().IsRegular() {
				uri := fileURI(path)
				if _, exists := r.seen[uri]; exists {
					return nil
				}
				if _, exists := pending[uri]; exists {
					return nil
				}
				if len(r.seen)+len(pending) >= r.maxInputs {
					return r.tooManyInputsError()
				}
				pending[uri] = struct{}{}
				paths = append(paths, path)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walk input directory %q: %w", input, err)
		}
		sort.Strings(paths)
		items := make([]Item, 0, len(paths))
		for _, path := range paths {
			fileInfo, statErr := os.Stat(path)
			if statErr != nil {
				return nil, fmt.Errorf("stat input %q: %w", path, statErr)
			}
			added, addErr := r.add(localItem(path, fileInfo))
			if addErr != nil {
				return nil, addErr
			}
			items = append(items, added...)
		}
		return items, nil
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("input %q is not a regular file", input)
	}
	if strings.EqualFold(filepath.Ext(absolute), ".txt") {
		return r.resolveManifest(ctx, absolute)
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, fmt.Errorf("resolve input path %q: %w", input, err)
	}
	if canonical != absolute {
		info, err = os.Stat(canonical)
		if err != nil {
			return nil, fmt.Errorf("stat input %q: %w", input, err)
		}
	}
	return r.add(localItem(canonical, info))
}

func localItem(path string, info os.FileInfo) Item {
	return Item{
		URI:       fileURI(path),
		Name:      filepath.Base(path),
		Size:      info.Size(),
		LocalPath: path,
		Open: func(context.Context) (io.ReadCloser, error) {
			file, err := os.Open(path)
			if err != nil {
				return nil, fmt.Errorf("open input %q: %w", path, err)
			}
			return file, nil
		},
	}
}

func (r *Resolver) resolveManifest(ctx context.Context, filename string) ([]Item, error) {
	if len(r.manifests) >= maxManifestDepth {
		return nil, fmt.Errorf("manifest nesting exceeds %d levels", maxManifestDepth)
	}
	canonical, err := filepath.EvalSymlinks(filename)
	if err != nil {
		return nil, fmt.Errorf("resolve manifest %q: %w", filename, err)
	}
	if _, exists := r.manifests[canonical]; exists {
		return nil, fmt.Errorf("manifest cycle detected at %q", filename)
	}
	r.manifests[canonical] = struct{}{}
	defer delete(r.manifests, canonical)

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open manifest %q: %w", filename, err)
	}
	defer file.Close()

	var items []Item
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), maxManifestLine)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		if !utf8.Valid(scanner.Bytes()) {
			return nil, fmt.Errorf("manifest %s:%d is not valid UTF-8", filename, lineNumber)
		}
		line := strings.TrimSpace(strings.TrimPrefix(scanner.Text(), "\ufeff"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		resolved, resolveErr := r.resolveOne(ctx, line, filepath.Dir(filename))
		if resolveErr != nil {
			return nil, fmt.Errorf("manifest %s:%d: %w", filename, lineNumber, resolveErr)
		}
		items = append(items, resolved...)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read manifest %q: %w", filename, err)
	}
	return items, nil
}

func (r *Resolver) resolveHTTP(parsed *url.URL) ([]Item, error) {
	name := filepath.Base(parsed.Path)
	if name == "." || name == "/" || name == "" {
		name = "download.bin"
	}
	// What the first response said the resource was. A resume may only append
	// to bytes that provably came from the same representation, so this is
	// tracked deliberately rather than inferred from a status code.
	var (
		representationLock sync.Mutex
		validator          string
		totalSize          int64 = -1
	)
	// recordRepresentation replaces what is known about the resource. It runs
	// for every full body, because a 200 answering a ranged request means the
	// server is serving something else now, and carrying the old validator
	// forward would make the next resume ask about a resource that is gone.
	recordRepresentation := func(header http.Header, contentLength int64) {
		candidate := header.Get("ETag")
		// If-Range also permits a sufficiently precise Last-Modified date, but
		// it is not a byte-for-byte identity claim. Only a syntactically valid
		// strong ETag is enough evidence to append bytes to an existing file.
		if !validStrongETag(candidate) {
			candidate = ""
		}
		representationLock.Lock()
		defer representationLock.Unlock()
		validator, totalSize = candidate, contentLength
	}
	var get ReopenFunc
	get = func(ctx context.Context, offset int64) (io.ReadCloser, bool, error) {
		representationLock.Lock()
		knownValidator, knownTotal := validator, totalSize
		representationLock.Unlock()
		// Without something to hold the server to, a range request would be
		// taken on trust. Start again instead: a slower transfer is better than
		// one that splices a stale prefix onto a body it never belonged to.
		ranged := offset > 0 && knownValidator != ""

		watch := netio.NewIdleWatch(ctx, r.transferIdleTimeout)
		request, err := http.NewRequestWithContext(watch.Context(), http.MethodGet, parsed.String(), nil)
		if err != nil {
			watch.Stop()
			return nil, false, errors.New("create HTTP input request")
		}
		request.Header = r.httpHeaders.Clone()
		if request.Header == nil {
			// Clone answers nil for nil, and these requests have headers of
			// their own to set.
			request.Header = make(http.Header)
		}
		// Byte offsets only mean anything against the bytes actually sent. Go
		// asks for gzip and transparently decodes it unless told otherwise, so
		// a resumed range would be counted in decoded bytes and requested in
		// encoded ones -- two different coordinate systems, and a corrupt file.
		request.Header.Set("Accept-Encoding", "identity")
		if ranged {
			request.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
			request.Header.Set("If-Range", knownValidator)
		}

		response, err := r.httpClient.Do(request)
		if err != nil {
			cause := watch.Error(err)
			watch.Stop()
			var idle *netio.IdleTimeoutError
			if errors.As(cause, &idle) || context.Cause(ctx) != nil || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
				return nil, false, fmt.Errorf("download input %s: %w", auth.RedactURL(parsed.String()), cause)
			}
			// A transport error can contain the complete request, including input
			// credentials. The operation and redacted URL are enough here.
			return nil, false, fmt.Errorf("download input %s: request failed", auth.RedactURL(parsed.String()))
		}
		watch.Progress()
		responseBody := watch.Body(response.Body)
		fail := func(format string, args ...any) (io.ReadCloser, bool, error) {
			_ = responseBody.Close()
			return nil, false, fmt.Errorf(format, args...)
		}
		if encoding := response.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
			return fail("download input %s: server applied %s encoding, which byte ranges cannot address",
				auth.RedactURL(parsed.String()), encoding)
		}

		switch {
		case ranged && response.StatusCode == http.StatusPartialContent:
			// The server says it is continuing. Whether it is continuing from
			// where we asked is a different question, and the only one that
			// protects the file: a range starting anywhere else would be
			// appended as though it were contiguous.
			contentRange, err := validateCompleteRange(
				response.Header.Get("Content-Range"), offset, knownTotal, response.ContentLength)
			if err != nil {
				return fail("download input %s: %w", auth.RedactURL(parsed.String()), err)
			}
			if responseETag := response.Header.Get("ETag"); responseETag != "" && responseETag != knownValidator {
				return fail("download input %s: resumed representation has ETag %q, expected %q",
					auth.RedactURL(parsed.String()), responseETag, knownValidator)
			}
			if knownTotal < 0 {
				representationLock.Lock()
				if validator == knownValidator && totalSize < 0 {
					totalSize = contentRange.total
				}
				representationLock.Unlock()
			}
			return exactRangeBody(responseBody, contentRange.end-contentRange.start+1), true, nil

		case ranged && response.StatusCode == http.StatusRequestedRangeNotSatisfiable:
			// A connection can fail after its final byte but before clean EOF. If
			// the same validated representation says the staged offset is exactly
			// its size, staging is already complete. Every other 416 restarts: no
			// bytes from an uncertain prefix may be retained.
			rangeTotal, rangeErr := parseUnsatisfiedContentRange(response.Header.Get("Content-Range"))
			_ = responseBody.Close()
			if rangeErr == nil && knownTotal >= 0 && offset == knownTotal && rangeTotal == knownTotal {
				return http.NoBody, true, nil
			}
			body, _, restartErr := get(ctx, 0)
			return body, false, restartErr

		case response.StatusCode == http.StatusRequestedRangeNotSatisfiable:
			return fail("download input %s: server refused the full representation", auth.RedactURL(parsed.String()))

		case response.StatusCode >= 200 && response.StatusCode < 300:
			if response.StatusCode == http.StatusPartialContent {
				// Partial content nobody asked for cannot be placed.
				return fail("download input %s: server sent a partial response to an unranged request",
					auth.RedactURL(parsed.String()))
			}
			// A plain 200 means the whole body is coming, either because the
			// resource changed or because the server does not do ranges. What
			// was staged is not a prefix of it, so the caller starts again.
			recordRepresentation(response.Header, response.ContentLength)
			return responseBody, offset == 0, nil

		default:
			return fail("download input %s: %d %s", auth.RedactURL(parsed.String()), response.StatusCode,
				http.StatusText(response.StatusCode))
		}
	}
	item := Item{
		URI:  parsed.String(),
		Name: name,
		Size: -1,
		Open: func(ctx context.Context) (io.ReadCloser, error) {
			body, _, err := get(ctx, 0)
			return body, err
		},
		Reopen: get,
	}
	return r.add(item)
}

func (r *Resolver) resolveS3(ctx context.Context, parsed *url.URL) ([]Item, error) {
	if parsed.Host == "" {
		return nil, errors.New("S3 input must include a bucket")
	}
	key, err := url.PathUnescape(strings.TrimPrefix(parsed.EscapedPath(), "/"))
	if err != nil {
		return nil, fmt.Errorf("decode S3 key: %w", err)
	}
	client, err := r.getS3Client(ctx)
	if err != nil {
		return nil, withPublicError("AWS configuration could not be loaded.", err)
	}
	bucket := parsed.Host

	// A key that does not end in a slash names one object, which is what the
	// documented forms say: a prefix import ends in "/", and a complete key
	// imports a single Object.
	//
	// Falling back to a prefix listing when that object is absent turns a typo
	// into a silent import of whatever else happens to share the prefix -- ask
	// for programme.mxf, get programme.mxf.bak and programme.mxf.tmp, with no
	// error to say the file you named was never there.
	if key != "" && !strings.HasSuffix(key, "/") {
		metadataCtx, cancel := r.metadataContext(ctx)
		head, headErr := client.HeadObject(metadataCtx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		cancel()
		if headErr == nil {
			return r.add(r.s3Item(client, bucket, key, aws.ToInt64(head.ContentLength)))
		}
		if isNotFound(headErr) {
			return nil, fmt.Errorf(
				"S3 input s3://%s/%s does not exist; end the key with \"/\" to import every object beneath it",
				bucket, key)
		}
		return nil, withPublicError("The S3 input could not be inspected.",
			fmt.Errorf("inspect s3://%s/%s: %w", bucket, key, headErr))
	}

	var items []Item
	pending := make(map[string]struct{})
	matched := false
	var continuation *string
	seenContinuations := make(map[string]struct{})
	for {
		metadataCtx, cancel := r.metadataContext(ctx)
		page, listErr := client.ListObjectsV2(metadataCtx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(key),
			ContinuationToken: continuation,
		})
		cancel()
		if listErr != nil {
			return nil, withPublicError("The S3 input prefix could not be listed.",
				fmt.Errorf("list s3://%s/%s: %w", bucket, key, listErr))
		}
		for _, object := range page.Contents {
			objectKey := aws.ToString(object.Key)
			if objectKey == "" || strings.HasSuffix(objectKey, "/") {
				continue
			}
			matched = true
			item := r.s3Item(client, bucket, objectKey, aws.ToInt64(object.Size))
			if _, exists := r.seen[item.URI]; exists {
				continue
			}
			if _, exists := pending[item.URI]; exists {
				continue
			}
			if len(r.seen)+len(pending) >= r.maxInputs {
				return nil, r.tooManyInputsError()
			}
			pending[item.URI] = struct{}{}
			items = append(items, item)
		}
		if !aws.ToBool(page.IsTruncated) {
			break
		}
		next := aws.ToString(page.NextContinuationToken)
		if next == "" || next == aws.ToString(continuation) {
			return nil, withPublicError("The S3 input prefix returned an invalid pagination cursor.",
				fmt.Errorf("list s3://%s/%s: truncated page did not advance its continuation token", bucket, key))
		}
		if _, repeated := seenContinuations[next]; repeated {
			return nil, withPublicError("The S3 input prefix returned an invalid pagination cursor.",
				fmt.Errorf("list s3://%s/%s: continuation token cycle detected", bucket, key))
		}
		seenContinuations[next] = struct{}{}
		continuation = page.NextContinuationToken
	}
	if !matched {
		return nil, fmt.Errorf("S3 input s3://%s/%s matched no objects", bucket, key)
	}
	sort.Slice(items, func(left, right int) bool { return items[left].URI < items[right].URI })
	result := make([]Item, 0, len(items))
	for _, item := range items {
		added, addErr := r.add(item)
		if addErr != nil {
			return nil, addErr
		}
		result = append(result, added...)
	}
	return result, nil
}

func (r *Resolver) getS3Client(ctx context.Context) (S3API, error) {
	if r.s3Client != nil {
		return r.s3Client, nil
	}
	options := make([]func(*awsconfig.LoadOptions) error, 0, 2)
	if r.s3Config.Region != "" {
		options = append(options, awsconfig.WithRegion(r.s3Config.Region))
	}
	if r.s3Config.HTTPClient != nil {
		options = append(options, awsconfig.WithHTTPClient(r.s3Config.HTTPClient))
	}
	metadataCtx, cancel := r.metadataContext(ctx)
	defer cancel()
	config, err := awsconfig.LoadDefaultConfig(metadataCtx, options...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	client := s3.NewFromConfig(config, func(options *s3.Options) {
		options.UsePathStyle = r.s3Config.UsePathStyle
		if r.s3Config.Endpoint != "" {
			options.BaseEndpoint = aws.String(r.s3Config.Endpoint)
		}
		// NewFromConfig resolves the selected standard/adaptive/custom policy,
		// shared-config attempt limit, and its private quota before applying this
		// service option. Wrap that exact instance rather than replacing it: AWS
		// retry environment and config semantics remain authoritative.
		options.Retryer = observedRetryer{Retryer: options.Retryer, run: r.observability}
	})
	r.s3Client = client
	return client, nil
}

// observedRetryer delegates every policy decision and quota operation to the
// AWS-selected standard, adaptive, or custom retryer. RetryDelay is the point
// at which the SDK has decided an additional attempt is permitted and has
// computed its wait, so observing here neither changes the policy nor reports
// retries that were refused.
type observedRetryer struct {
	aws.Retryer
	run *observability.Run
}

// GetAttemptToken preserves the context-aware RetryerV2 path used by adaptive
// mode. Merely embedding aws.Retryer would make the SDK fall back to the legacy
// GetInitialToken adapter and silently disable adaptive send-rate limiting.
func (r observedRetryer) GetAttemptToken(ctx context.Context) (func(error) error, error) {
	if retryer, ok := r.Retryer.(aws.RetryerV2); ok {
		return retryer.GetAttemptToken(ctx)
	}
	return r.GetInitialToken(), nil
}

func (r observedRetryer) RetryDelay(attempt int, err error) (time.Duration, error) {
	delay, delayErr := r.Retryer.RetryDelay(attempt, err)
	if delayErr != nil {
		return 0, delayErr
	}
	statusCode := 0
	var responseError *smithyhttp.ResponseError
	if errors.As(err, &responseError) {
		statusCode = responseError.HTTPStatusCode()
	}
	cause := err
	if statusCode != 0 {
		cause = nil
	}
	nextAttempt := attempt + 1
	observedDelay := delay
	if os.Getenv("AWS_NEW_RETRIES_2026") == "true" {
		// The opt-in AWS policy numbers the first retry delay from zero and may
		// clamp it to X-Amz-Retry-After after RetryDelay returns. Mirror only
		// those reporting semantics; return the delegate's value untouched so
		// the SDK remains the sole owner of the actual policy.
		nextAttempt = attempt + 2
		observedDelay = aws2026RetryAfter(delay, err)
	}
	r.run.Retry(observability.OperationS3Request, nextAttempt, r.MaxAttempts(), statusCode, cause, observedDelay)
	return delay, nil
}

func aws2026RetryAfter(backoff time.Duration, err error) time.Duration {
	var responseError *smithyhttp.ResponseError
	if !errors.As(err, &responseError) || responseError.Response == nil || responseError.Response.Response == nil {
		return backoff
	}
	header := responseError.Response.Header.Get("X-Amz-Retry-After")
	milliseconds, parseErr := strconv.ParseInt(header, 10, 64)
	if parseErr != nil || milliseconds < 0 {
		return backoff
	}
	retryAfter := time.Duration(milliseconds) * time.Millisecond
	return min(max(retryAfter, backoff), backoff+5*time.Second)
}

func (r *Resolver) s3Item(client S3API, bucket, key string, size int64) Item {
	uri := (&url.URL{Scheme: "s3", Host: bucket, Path: "/" + key}).String()
	// What the object was when first read. A resumed range may only append to
	// bytes from that same version, so it is pinned rather than assumed.
	var (
		representationLock sync.Mutex
		etag               string
		versionID          string
		totalSize          int64 = -1
	)
	var get ReopenFunc
	get = func(ctx context.Context, offset int64) (io.ReadCloser, bool, error) {
		representationLock.Lock()
		knownETag, knownVersion, knownTotal := etag, versionID, totalSize
		representationLock.Unlock()
		// With nothing to pin the object to, a range would be taken on trust.
		ranged := offset > 0 && (knownETag != "" || knownVersion != "")

		request := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
		if ranged {
			request.Range = aws.String("bytes=" + strconv.FormatInt(offset, 10) + "-")
			// A version identifier names the exact object; an ETag only says
			// the current one still matches. Both are sent where available,
			// because a versioned bucket can then answer from the version that
			// was actually staged rather than refusing.
			if knownVersion != "" {
				request.VersionId = aws.String(knownVersion)
			}
			if knownETag != "" {
				request.IfMatch = aws.String(knownETag)
			}
		}

		watch := netio.NewIdleWatch(ctx, r.transferIdleTimeout)
		response, err := client.GetObject(watch.Context(), request)
		if err != nil {
			// The object changed, so the staged prefix belongs to something
			// else. Fetch it whole and tell the caller it is starting again.
			if ranged && isPreconditionFailed(err) {
				watch.Stop()
				body, _, restartErr := get(ctx, 0)
				return body, false, restartErr
			}
			if ranged {
				if responseError, rangeRejected := s3ResponseError(err, http.StatusRequestedRangeNotSatisfiable); rangeRejected {
					var header http.Header
					var errorBody io.ReadCloser
					if responseError.Response != nil && responseError.Response.Response != nil {
						header = responseError.Response.Response.Header
						errorBody = responseError.Response.Response.Body
					}
					watch.Stop()
					if errorBody != nil {
						_ = errorBody.Close()
					}
					rangeTotal, rangeErr := parseUnsatisfiedContentRange(header.Get("Content-Range"))
					if rangeErr == nil && knownTotal >= 0 && offset == knownTotal && rangeTotal == knownTotal {
						return http.NoBody, true, nil
					}
					body, _, restartErr := get(ctx, 0)
					return body, false, restartErr
				}
			}
			cause := watch.Error(err)
			watch.Stop()
			return nil, false, fmt.Errorf("open %s: %w", uri, cause)
		}
		watch.Progress()
		body := watch.Body(response.Body)

		if !ranged {
			representationLock.Lock()
			etag, versionID, totalSize = "", "", -1
			if response.ETag != nil {
				etag = *response.ETag
			}
			if response.VersionId != nil {
				versionID = *response.VersionId
			}
			if response.ContentLength != nil {
				totalSize = *response.ContentLength
			}
			representationLock.Unlock()
			return body, offset == 0, nil
		}

		// A range was asked for, so the answer has to say which bytes it is --
		// and they have to be the ones requested. Trusting the presence of a
		// Content-Range would let a non-conforming endpoint hand back a
		// different span that is then appended as though it were contiguous.
		if response.ContentRange == nil {
			_ = body.Close()
			return nil, false, fmt.Errorf("open %s: server did not state which bytes it sent", uri)
		}
		contentLength := int64(-1)
		if response.ContentLength != nil {
			contentLength = *response.ContentLength
		}
		contentRange, parseErr := validateCompleteRange(*response.ContentRange, offset, knownTotal, contentLength)
		if parseErr != nil {
			_ = body.Close()
			return nil, false, fmt.Errorf("open %s: %w", uri, parseErr)
		}
		if response.ETag != nil && knownETag != "" && *response.ETag != knownETag {
			_ = body.Close()
			return nil, false, fmt.Errorf("open %s: resumed object has ETag %q, expected %q", uri, *response.ETag, knownETag)
		}
		if knownTotal < 0 {
			representationLock.Lock()
			if etag == knownETag && versionID == knownVersion && totalSize < 0 {
				totalSize = contentRange.total
			}
			representationLock.Unlock()
		}
		return exactRangeBody(body, contentRange.end-contentRange.start+1), true, nil
	}
	return Item{
		URI:  uri,
		Name: filepath.Base(key),
		Size: size,
		Open: func(ctx context.Context) (io.ReadCloser, error) {
			body, _, err := get(ctx, 0)
			return body, err
		},
		Reopen: get,
	}
}

func (r *Resolver) metadataContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, r.metadataTimeout)
}

func (r *Resolver) add(item Item) ([]Item, error) {
	if _, exists := r.seen[item.URI]; exists {
		return nil, nil
	}
	if len(r.seen) >= r.maxInputs {
		return nil, r.tooManyInputsError()
	}
	r.seen[item.URI] = struct{}{}
	return []Item{item}, nil
}

func (r *Resolver) tooManyInputsError() error {
	return fmt.Errorf(
		"resolved input count exceeds --max-inputs=%d; narrow the directory, manifest, or S3 prefix, or raise the limit deliberately",
		r.maxInputs)
}

func fileURI(path string) string {
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String()
}

func isPreconditionFailed(err error) bool {
	_, matched := s3ResponseError(err, http.StatusPreconditionFailed)
	return matched
}

func s3ResponseError(err error, status int) (*smithyhttp.ResponseError, bool) {
	var responseError *smithyhttp.ResponseError
	if !errors.As(err, &responseError) || responseError.HTTPStatusCode() != status {
		return nil, false
	}
	return responseError, true
}

func isNotFound(err error) bool {
	var responseError *smithyhttp.ResponseError
	if errors.As(err, &responseError) && responseError.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "NotFound", "NoSuchKey", "NoSuchBucket":
			return true
		}
	}
	return false
}

// HTTPClientWithSafeRedirects returns a shallow copy of base whose redirect
// policy never forwards caller-configured input credentials to another
// origin. It must be applied to the client which actually follows redirects;
// wrapping only an outer RoundTripper is insufficient when that transport has
// its own HTTP client.
func HTTPClientWithSafeRedirects(base *http.Client, headers http.Header) *http.Client {
	client := *base
	originalPolicy := client.CheckRedirect
	client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) > 0 && !sameOrigin(via[0].URL, request.URL) {
			request.Header.Del("Authorization")
			request.Header.Del("Proxy-Authorization")
			for name := range headers {
				request.Header.Del(name)
			}
		}
		if originalPolicy != nil {
			return originalPolicy(request, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	return &client
}

func sameOrigin(left, right *url.URL) bool {
	return strings.EqualFold(left.Scheme, right.Scheme) && strings.EqualFold(left.Host, right.Host)
}
