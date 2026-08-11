package tams

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/livewyer-ops/tamsin/internal/auth"
	"github.com/livewyer-ops/tamsin/internal/netio"
	"github.com/livewyer-ops/tamsin/internal/observability"
)

const (
	// maxSegmentPages bounds a paged listing so a service returning a cursor
	// that never advances fails loudly instead of looping.
	maxSegmentPages         = 1_000
	maxListedSegments       = 100_000
	maxSegmentResponseBytes = 64 << 20
	maxErrorBody            = 64 << 10
	maxJSONBody             = 32 << 20
)

type Config struct {
	Endpoint          string
	Transport         http.RoundTripper
	ExternalTransport http.RoundTripper
	// Timeout bounds a metadata request end to end. These carry small JSON
	// bodies, so a wall-clock deadline is the right shape for them.
	Timeout time.Duration
	// TransferTimeout optionally bounds a Media Object upload or verification
	// end to end. It defaults to none so it does not cap a large healthy body.
	TransferTimeout time.Duration
	// TransferIdleTimeout bounds only time without byte-level progress. It has
	// a positive default because dial, TLS, and response-header deadlines do not
	// cover a body that stalls after headers or a peer that stops reading a PUT.
	TransferIdleTimeout time.Duration
	Retries             int
	UserAgent           string
	RedactValues        []string
	SuppressErrorBody   bool
	Observability       *observability.Run
}

type Client struct {
	timeout             time.Duration
	transferTimeout     time.Duration
	transferIdleTimeout time.Duration
	base                *url.URL
	http                *http.Client
	external            *http.Client
	retries             int
	userAgent           string
	baseOrigin          string
	redactValues        []string
	suppressErrorBody   bool
	observability       *observability.Run
	deletePollInterval  time.Duration
	segmentPageLimit    int
	segmentCountLimit   int
	segmentByteLimit    int
	// now is injectable inside package tests so expiry between retry attempts is
	// deterministic without sleeping through the specification's URL lifetime.
	now func() time.Time
}

func New(config Config) (*Client, error) {
	if config.Endpoint == "" {
		return nil, errors.New("TAMS endpoint is required")
	}
	base, err := url.Parse(strings.TrimRight(config.Endpoint, "/"))
	if err != nil {
		return nil, errors.New("TAMS endpoint is not a valid URL")
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, fmt.Errorf("TAMS endpoint must use http or https, got %q", base.Scheme)
	}
	if base.Host == "" {
		return nil, errors.New("TAMS endpoint must include a host")
	}
	if base.User != nil {
		return nil, errors.New("TAMS endpoint must not contain userinfo; configure basic authentication explicitly")
	}
	if base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("TAMS endpoint must not contain a query or fragment other than the extracted access_token")
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	if config.Retries < 0 || config.Retries > 20 {
		return nil, errors.New("retry count must be between 0 and 20")
	}
	if config.TransferIdleTimeout <= 0 {
		config.TransferIdleTimeout = netio.DefaultIdleTimeout
	}
	transport := config.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	externalTransport := config.ExternalTransport
	if externalTransport == nil {
		externalTransport = http.DefaultTransport
	}
	if config.UserAgent == "" {
		config.UserAgent = "tamsin/dev"
	}
	redactValues := make([]string, 0, len(config.RedactValues))
	for _, value := range config.RedactValues {
		if value != "" {
			redactValues = append(redactValues, value)
		}
	}

	// The clients carry no absolute timeout. Metadata requests get a deadline
	// from their context, and media transfers are deliberately uncapped unless
	// an operator asks otherwise.
	return &Client{
		timeout:             config.Timeout,
		transferTimeout:     config.TransferTimeout,
		transferIdleTimeout: config.TransferIdleTimeout,
		base:                base,
		http: &http.Client{
			Transport: transport, CheckRedirect: rejectRedirect,
		},
		external: &http.Client{
			Transport: externalTransport, CheckRedirect: rejectRedirect,
		},
		retries:            config.Retries,
		userAgent:          config.UserAgent,
		baseOrigin:         origin(base),
		redactValues:       redactValues,
		suppressErrorBody:  config.SuppressErrorBody,
		observability:      config.Observability,
		deletePollInterval: 250 * time.Millisecond,
		segmentPageLimit:   maxSegmentPages,
		segmentCountLimit:  maxListedSegments,
		segmentByteLimit:   maxSegmentResponseBytes,
		now:                time.Now,
	}, nil
}

func (c *Client) Service(ctx context.Context) (map[string]any, error) {
	var result map[string]any
	if err := c.doJSON(ctx, http.MethodGet, "service", nil, &result, http.StatusOK); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) StorageBackends(ctx context.Context) ([]StorageBackend, error) {
	var result []StorageBackend
	if err := c.doJSON(ctx, http.MethodGet, "service/storage-backends", nil, &result, http.StatusOK); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) Flow(ctx context.Context, flowID string) (Flow, error) {
	var result Flow
	if err := c.doJSON(ctx, http.MethodGet, "flows/"+escapeSegment(flowID), nil, &result, http.StatusOK); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) PutFlow(ctx context.Context, flowID string, flow Flow) (Flow, error) {
	var result Flow
	if err := c.doJSON(ctx, http.MethodPut, "flows/"+escapeSegment(flowID), flow, &result, http.StatusCreated, http.StatusNoContent, http.StatusOK); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) AllocateStorage(ctx context.Context, flowID string, request StorageRequest) (StorageResponse, error) {
	var result StorageResponse
	if err := c.doJSON(ctx, http.MethodPost, "flows/"+escapeSegment(flowID)+"/storage", request, &result, http.StatusCreated); err != nil {
		return StorageResponse{}, err
	}
	return result, nil
}

func (c *Client) RegisterSegment(ctx context.Context, flowID string, request SegmentRequest) error {
	return c.doJSON(ctx, http.MethodPost, "flows/"+escapeSegment(flowID)+"/segments", request, nil, http.StatusCreated)
}

func (c *Client) RegisterSegments(ctx context.Context, flowID string, requests []SegmentRequest) error {
	if len(requests) == 0 {
		return nil
	}
	if len(requests) == 1 {
		return c.RegisterSegment(ctx, flowID, requests[0])
	}
	var failure BulkSegmentFailure
	path := "flows/" + escapeSegment(flowID) + "/segments"
	if err := c.doJSON(ctx, http.MethodPost, path, requests, &failure, http.StatusCreated, http.StatusOK); err != nil {
		return err
	}
	if len(failure.FailedSegments) > 0 {
		registered := make([]SegmentRequest, 0, max(len(requests)-len(failure.FailedSegments), 0))
		for _, request := range requests {
			if !failedRegistration(failure.FailedSegments, request) {
				registered = append(registered, request)
			}
		}
		return &PartialSegmentRegistrationError{
			FailedSegments:     append([]FailedSegment(nil), failure.FailedSegments...),
			RegisteredSegments: registered,
			Total:              len(requests),
		}
	}
	return nil
}

func failedRegistration(failures []FailedSegment, request SegmentRequest) bool {
	for _, failed := range failures {
		if failed.ObjectID == request.ObjectID && (failed.Timerange == "" || failed.Timerange == request.Timerange) {
			return true
		}
	}
	return false
}

func (c *Client) Object(ctx context.Context, objectID string) (ObjectInfo, error) {
	var result ObjectInfo
	if err := c.doJSON(ctx, http.MethodGet, "objects/"+escapeSegment(objectID), nil, &result, http.StatusOK); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) RegisterObjectInstance(ctx context.Context, objectID string, request ObjectInstanceRequest) error {
	return c.doJSON(ctx, http.MethodPost, "objects/"+escapeSegment(objectID)+"/instances", request, nil, http.StatusCreated)
}

func (c *Client) DeleteObjectInstance(ctx context.Context, objectID, storageID, label string) error {
	query := make(url.Values)
	if storageID != "" {
		query.Set("storage_id", storageID)
	}
	if label != "" {
		query.Set("label", label)
	}
	if len(query) == 0 {
		return errors.New("storage ID or label is required to delete an object instance")
	}
	return c.doJSON(ctx, http.MethodDelete, "objects/"+escapeSegment(objectID)+"/instances?"+query.Encode(), nil, nil, http.StatusNoContent)
}

// RawJSON exposes the pinned API operations without forcing callers through a
// lossy generic map. It is used by the low-level `tamsin api` commands.
func (c *Client) RawJSON(ctx context.Context, method, requestPath string, body []byte) ([]byte, error) {
	var decoded json.RawMessage
	if len(body) > 0 && !json.Valid(body) {
		return nil, errors.New("request body is not valid JSON")
	}
	var payload any
	if len(body) > 0 {
		payload = json.RawMessage(body)
	}
	if err := c.doJSON(ctx, strings.ToUpper(method), strings.TrimLeft(requestPath, "/"), payload, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

// transferContext applies the optional transfer deadline. Without one the
// caller's context governs, so a large healthy transfer is limited by the run
// rather than by a per-request clock.
func (c *Client) transferContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.transferTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.transferTimeout)
}

func (c *Client) UploadFile(ctx context.Context, destination PresignedURL, filename string) (UploadReceipt, error) {
	ctx, cancel := c.transferContext(ctx)
	defer cancel()
	parsed, err := url.Parse(destination.URL)
	if err != nil {
		return UploadReceipt{}, errors.New("TAMS upload URL is not valid")
	}
	client := c.external
	if origin(parsed) == c.baseOrigin {
		client = c.http
	}

	for attempt := range c.retries + 1 {
		file, openErr := os.Open(filename)
		if openErr != nil {
			return UploadReceipt{}, fmt.Errorf("open upload file: %w", openErr)
		}
		info, statErr := file.Stat()
		if statErr != nil {
			_ = file.Close()
			return UploadReceipt{}, fmt.Errorf("stat upload file: %w", statErr)
		}
		hash := sha256.New()
		watch := netio.NewIdleWatch(ctx, c.transferIdleTimeout)
		request, requestErr := http.NewRequestWithContext(
			watch.Context(), http.MethodPut, destination.URL, watch.Reader(io.TeeReader(file, hash)))
		if requestErr != nil {
			watch.Stop()
			_ = file.Close()
			return UploadReceipt{}, fmt.Errorf("create upload request for %s", auth.RedactURL(destination.URL))
		}
		request.ContentLength = info.Size()
		request.Header.Set("User-Agent", c.userAgent)
		for name, value := range destination.Headers {
			request.Header.Set(name, value)
		}
		if request.Header.Get("Content-Type") == "" {
			request.Header.Set("Content-Type", "application/octet-stream")
		}
		if err := c.ensureURLAttemptCanStart(destination, "upload"); err != nil {
			watch.Stop()
			_ = file.Close()
			return UploadReceipt{}, err
		}

		response, requestErr := client.Do(request)
		_ = file.Close()
		if requestErr == nil {
			watch.Progress()
			// The body is read before closing even though nothing wants it: a
			// connection is only returned to the pool once its response has been
			// consumed to EOF, and an ingest uploading many Media Objects would
			// otherwise pay a fresh handshake for each one.
			body := watch.Body(response.Body)
			_, _ = io.Copy(io.Discard, io.LimitReader(body, maxErrorBody))
			_ = body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				storageSHA256, checksumErr := uploadStorageSHA256(response.Header, request.Header)
				if checksumErr != nil {
					return UploadReceipt{}, fmt.Errorf("read upload checksum evidence: %w", checksumErr)
				}
				return UploadReceipt{
					Bytes: info.Size(), SHA256: hex.EncodeToString(hash.Sum(nil)), StorageSHA256: storageSHA256,
				}, nil
			}
		}
		retryAfter := ""
		if requestErr == nil {
			retryAfter = response.Header.Get("Retry-After")
			if !retryableStatus(response.StatusCode) || attempt == c.retries {
				return UploadReceipt{}, &HTTPError{Method: http.MethodPut, URL: auth.RedactURL(destination.URL), StatusCode: response.StatusCode, Status: safeHTTPStatus(response.StatusCode)}
			}
		} else {
			cause := transferRequestError(ctx, watch, http.MethodPut, destination.URL, requestErr)
			watch.Stop()
			if attempt == c.retries || ctx.Err() != nil {
				return UploadReceipt{}, fmt.Errorf("upload failed after %d attempt(s): %w", attempt+1, cause)
			}
		}
		statusCode := 0
		var retryCause error
		if requestErr == nil {
			statusCode = response.StatusCode
		} else {
			retryCause = requestErr
		}
		if err := c.sleepPresignedBackoff(ctx, attempt, c.retries+1, retryAfter, destination,
			"upload", observability.OperationObjectUpload, statusCode, retryCause); err != nil {
			return UploadReceipt{}, err
		}
	}
	return UploadReceipt{}, errors.New("upload attempts exhausted")
}

// uploadStorageSHA256 extracts only checksums whose semantics are SHA-256 over
// the uploaded representation. A provider response is preferred; a checksum
// header included in the signed allocation request is also useful evidence
// because a successful checksum-aware PUT means the provider accepted it.
// ETag is deliberately excluded: it is not reliably a content digest.
func uploadStorageSHA256(response, request http.Header) (string, error) {
	for _, headers := range []http.Header{response, request} {
		for _, candidate := range []struct {
			name       string
			structured bool
		}{
			{name: "X-Amz-Checksum-Sha256"},
			{name: "Content-Digest", structured: true},
			{name: "Digest", structured: true},
		} {
			for _, value := range headers.Values(candidate.name) {
				encoded, found := sha256HeaderValue(value, candidate.structured)
				if !found {
					continue
				}
				digest, err := decodeSHA256Base64(encoded)
				if err != nil {
					return "", fmt.Errorf("invalid %s header: %w", candidate.name, err)
				}
				return digest, nil
			}
		}
	}
	return "", nil
}

func sha256HeaderValue(value string, structured bool) (string, bool) {
	if !structured {
		value = strings.TrimSpace(value)
		return value, value != ""
	}
	for _, member := range strings.Split(value, ",") {
		name, encoded, found := strings.Cut(strings.TrimSpace(member), "=")
		if !found || !strings.EqualFold(strings.TrimSpace(name), "sha-256") {
			continue
		}
		encoded = strings.TrimSpace(strings.SplitN(encoded, ";", 2)[0])
		encoded = strings.Trim(encoded, "\"")
		if strings.HasPrefix(encoded, ":") && strings.HasSuffix(encoded, ":") && len(encoded) >= 2 {
			encoded = encoded[1 : len(encoded)-1]
		}
		return encoded, true
	}
	return "", false
}

func decodeSHA256Base64(encoded string) (string, error) {
	digest, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		digest, err = base64.RawStdEncoding.DecodeString(encoded)
	}
	if err != nil {
		return "", errors.New("checksum is not valid base64")
	}
	if len(digest) != sha256.Size {
		return "", fmt.Errorf("checksum decodes to %d bytes, want %d", len(digest), sha256.Size)
	}
	return hex.EncodeToString(digest), nil
}

// DownloadDigest streams an Object instance and returns its byte length and
// SHA-256 without retaining media in memory.
func (c *Client) DownloadDigest(ctx context.Context, source PresignedURL) (int64, string, error) {
	ctx, cancel := c.transferContext(ctx)
	defer cancel()
	parsed, err := url.Parse(source.URL)
	if err != nil {
		return 0, "", errors.New("TAMS download URL is not valid")
	}
	client := c.external
	if origin(parsed) == c.baseOrigin {
		client = c.http
	}
	for attempt := range c.retries + 1 {
		watch := netio.NewIdleWatch(ctx, c.transferIdleTimeout)
		request, requestErr := http.NewRequestWithContext(watch.Context(), http.MethodGet, source.URL, nil)
		if requestErr != nil {
			watch.Stop()
			return 0, "", fmt.Errorf("create download request for %s", auth.RedactURL(source.URL))
		}
		request.Header.Set("User-Agent", c.userAgent)
		for name, value := range source.Headers {
			request.Header.Set(name, value)
		}
		if err := c.ensureURLAttemptCanStart(source, "download"); err != nil {
			watch.Stop()
			return 0, "", err
		}
		response, requestErr := client.Do(request)
		if requestErr != nil {
			cause := transferRequestError(ctx, watch, http.MethodGet, source.URL, requestErr)
			watch.Stop()
			if attempt == c.retries || ctx.Err() != nil {
				return 0, "", fmt.Errorf("download failed after %d attempt(s): %w", attempt+1, cause)
			}
			if err := c.sleepPresignedBackoff(ctx, attempt, c.retries+1, "", source,
				"download", observability.OperationObjectVerification, 0, requestErr); err != nil {
				return 0, "", err
			}
			continue
		}
		watch.Progress()
		body := watch.Body(response.Body)
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			retryAfter := response.Header.Get("Retry-After")
			_, _ = io.Copy(io.Discard, io.LimitReader(body, maxErrorBody))
			_ = body.Close()
			if !retryableStatus(response.StatusCode) || attempt == c.retries {
				return 0, "", &HTTPError{Method: http.MethodGet, URL: auth.RedactURL(source.URL), StatusCode: response.StatusCode, Status: safeHTTPStatus(response.StatusCode)}
			}
			if err := c.sleepPresignedBackoff(ctx, attempt, c.retries+1, retryAfter, source,
				"download", observability.OperationObjectVerification, response.StatusCode, nil); err != nil {
				return 0, "", err
			}
			continue
		}
		hash := sha256.New()
		size, readErr := io.Copy(hash, body)
		_ = body.Close()
		if readErr == nil {
			return size, hex.EncodeToString(hash.Sum(nil)), nil
		}
		if attempt == c.retries || ctx.Err() != nil {
			return 0, "", fmt.Errorf("download failed after %d attempt(s), reading %s: %w",
				attempt+1, auth.RedactURL(source.URL), readErr)
		}
		if err := c.sleepPresignedBackoff(ctx, attempt, c.retries+1, "", source,
			"download", observability.OperationObjectVerification, 0, readErr); err != nil {
			return 0, "", err
		}
	}
	return 0, "", errors.New("download attempts exhausted")
}

// ensureURLAttemptCanStart prevents a retry from presenting a signature after
// its advertised validity elapsed. It is checked immediately before Do rather
// than by placing a deadline on the request context: a PUT or GET that starts
// while the URL is valid may legitimately keep streaming after that instant.
func (c *Client) ensureURLAttemptCanStart(presigned PresignedURL, operation string) error {
	if presigned.StartBefore.IsZero() || c.now().Before(presigned.StartBefore) {
		return nil
	}
	return fmt.Errorf("presigned %s URL expired before another request attempt could start", operation)
}

// sleepPresignedBackoff declines a retry immediately when its required wait
// would consume the rest of the URL's start window. Waiting only to discover
// the same fact at the top of the loop wastes that window and delays recovery.
func (c *Client) sleepPresignedBackoff(ctx context.Context, attempt, maxAttempts int, retryAfter string,
	presigned PresignedURL, operation string, observedOperation observability.Operation,
	statusCode int, cause error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	delay := backoffDelay(attempt, retryAfter)
	if !presigned.StartBefore.IsZero() && !c.now().Add(delay).Before(presigned.StartBefore) {
		return fmt.Errorf("presigned %s URL expired before another request attempt could start", operation)
	}
	c.observability.Retry(observedOperation, attempt+2, maxAttempts, statusCode, cause, delay)
	return sleepDuration(ctx, delay)
}

func (c *Client) doJSON(ctx context.Context, method, requestPath string, input, output any, expected ...int) error {
	_, err := c.doJSONResponse(ctx, method, requestPath, input, output, expected...)
	return err
}

// doJSONHeaders is doJSON that also reports the response headers, which paging
// needs: the cursor for the next page arrives in a Link header rather than the
// body.
func (c *Client) doJSONHeaders(ctx context.Context, method, requestPath string, input, output any, expected ...int) (http.Header, int, error) {
	response, err := c.doJSONResponse(ctx, method, requestPath, input, output, expected...)
	return response.Header, response.BodyBytes, err
}

type jsonResponse struct {
	StatusCode        int
	Header            http.Header
	BodyBytes         int
	AmbiguousMutation bool
}

// doJSONResponse is the common JSON request path. Most callers need only the
// decoded body, while asynchronous operations also need the exact status and
// Location response header.
func (c *Client) doJSONResponse(ctx context.Context, method, requestPath string, input, output any, expected ...int) (jsonResponse, error) {
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	var body []byte
	var err error
	if input != nil {
		body, err = json.Marshal(input)
		if err != nil {
			return jsonResponse{}, fmt.Errorf("encode request: %w", err)
		}
	}
	requestURL, err := c.resolve(requestPath)
	if err != nil {
		return jsonResponse{}, err
	}
	if len(expected) == 0 {
		expected = []int{http.StatusOK, http.StatusCreated, http.StatusNoContent}
	}

	attempts := 1
	if retryableMethod(method) {
		attempts += c.retries
	}
	for attempt := range attempts {
		request, requestErr := http.NewRequestWithContext(ctx, method, requestURL.String(), bytes.NewReader(body))
		if requestErr != nil {
			return jsonResponse{}, fmt.Errorf("create %s request for %s", method, auth.RedactURL(requestURL.String()))
		}
		request.Header.Set("Accept", "application/json")
		request.Header.Set("User-Agent", c.userAgent)
		if input != nil {
			request.Header.Set("Content-Type", "application/json")
		}
		response, requestErr := c.http.Do(request)
		if requestErr != nil {
			result := jsonResponse{AmbiguousMutation: method == http.MethodDelete}
			if attempt+1 == attempts {
				return result, requestError(ctx, method, requestURL.String(), requestErr)
			}
			if err := ctx.Err(); err != nil {
				return result, err
			}
			delay := backoffDelay(attempt, "")
			c.observability.Retry(observability.OperationTAMSMetadata, attempt+2, attempts, 0, requestErr, delay)
			if err := sleepDuration(ctx, delay); err != nil {
				return result, err
			}
			continue
		}

		success := containsStatus(expected, response.StatusCode)
		limit := int64(maxErrorBody)
		if success {
			limit = maxJSONBody
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, limit+1))
		_ = response.Body.Close()
		result := jsonResponse{
			StatusCode: response.StatusCode,
			Header:     response.Header,
			BodyBytes:  len(responseBody),
		}
		if readErr != nil {
			result.AmbiguousMutation = ambiguousDeleteResponse(method, response.StatusCode, success)
			return result, fmt.Errorf("read %s response: %w", method, readErr)
		}
		if len(responseBody) > int(limit) {
			if success {
				result.AmbiguousMutation = method == http.MethodDelete
				return result, fmt.Errorf("%s response exceeds %d bytes", method, limit)
			}
			responseBody = append(responseBody[:maxErrorBody], []byte("\n[response truncated]")...)
		}
		if success {
			if output == nil || len(bytes.TrimSpace(responseBody)) == 0 {
				return result, nil
			}
			if err := json.Unmarshal(responseBody, output); err != nil {
				result.AmbiguousMutation = method == http.MethodDelete
				return result, fmt.Errorf("decode %s response: %w", method, err)
			}
			return result, nil
		}

		if retryableStatus(response.StatusCode) && attempt+1 < attempts {
			if err := ctx.Err(); err != nil {
				result.AmbiguousMutation = method == http.MethodDelete
				return result, err
			}
			delay := backoffDelay(attempt, response.Header.Get("Retry-After"))
			c.observability.Retry(observability.OperationTAMSMetadata, attempt+2, attempts,
				response.StatusCode, nil, delay)
			if err := sleepDuration(ctx, delay); err != nil {
				result.AmbiguousMutation = method == http.MethodDelete
				return result, err
			}
			continue
		}
		errorBody := c.redact(string(responseBody))
		if c.suppressErrorBody {
			errorBody = ""
		}
		result.AmbiguousMutation = method == http.MethodDelete && retryableStatus(response.StatusCode)
		return result, &HTTPError{Method: method, URL: auth.RedactURL(requestURL.String()), StatusCode: response.StatusCode, Status: safeHTTPStatus(response.StatusCode), Body: errorBody}
	}
	return jsonResponse{}, fmt.Errorf("%s request attempts exhausted", method)
}

func ambiguousDeleteResponse(method string, status int, expected bool) bool {
	if method != http.MethodDelete {
		return false
	}
	return expected || status == http.StatusNotFound || retryableStatus(status)
}

func (c *Client) resolve(requestPath string) (*url.URL, error) {
	reference, err := url.Parse(requestPath)
	if err != nil {
		return nil, errors.New("TAMS request path is not valid")
	}
	resolved := *c.base
	baseEscaped := strings.TrimRight(c.base.EscapedPath(), "/")
	referenceEscaped := strings.TrimLeft(reference.EscapedPath(), "/")
	rawPath := baseEscaped + "/" + referenceEscaped
	decodedPath, err := url.PathUnescape(rawPath)
	if err != nil {
		return nil, fmt.Errorf("decode request path: %w", err)
	}
	resolved.Path = decodedPath
	if rawPath != decodedPath {
		resolved.RawPath = rawPath
	} else {
		resolved.RawPath = ""
	}
	resolved.RawQuery = reference.RawQuery
	resolved.Fragment = ""
	return &resolved, nil
}

func escapeSegment(value string) string {
	return url.PathEscape(value)
}

func origin(value *url.URL) string {
	scheme := strings.ToLower(value.Scheme)
	host := strings.ToLower(value.Hostname())
	port := value.Port()
	if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
		port = ""
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return scheme + "://" + host
}

func retryableMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete:
		return true
	default:
		return false
	}
}

func retryableStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func safeHTTPStatus(code int) string {
	if text := http.StatusText(code); text != "" {
		return fmt.Sprintf("%d %s", code, text)
	}
	return strconv.Itoa(code)
}

func containsStatus(expected []int, actual int) bool {
	for _, status := range expected {
		if status == actual {
			return true
		}
	}
	return false
}

// maxRetryAfter caps how long a server can send a client away for.
//
// Retry-After is an instruction, and it is honoured -- but a command that was
// bounded by its own retry budget should not become unbounded because a store
// asked for an hour. Past this point the delay is worth less than the chance to
// fail and let the caller decide.
const maxRetryAfter = 2 * time.Minute

// backoffJitter is the width of the random spread added to a delay the server
// asked for. It is small, because it only has to break the synchronisation
// between clients, not meaningfully change when they return.
const backoffJitter = 500 * time.Millisecond

// backoffDelay is how long to wait before retrying attempt.
//
// The exponential part is jittered because the transfers that fail together are
// the ones most likely to retry together. A store shedding load under pressure
// answers every in-flight request at once, and an unjittered backoff sends the
// whole set back at the same instant, reproducing the burst that caused the
// shedding. Half the computed delay is kept as a floor so a retry cannot become
// aggressive, and the remainder is spread randomly.
//
// A Retry-After is an instruction rather than an estimate, so it is never
// shortened. Jitter is added on top of it, which desynchronises clients that
// were all told the same thing without any of them returning early.
func backoffDelay(attempt int, retryAfter string) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return min(time.Duration(seconds)*time.Second, maxRetryAfter) + jitter(backoffJitter)
	}
	if parsed, err := http.ParseTime(retryAfter); err == nil {
		if until := time.Until(parsed); until > 0 {
			return min(until, maxRetryAfter) + jitter(backoffJitter)
		}
	}
	delay := 200 * time.Millisecond * time.Duration(1<<min(attempt, 4))
	return delay/2 + jitter(delay/2)
}

// jitter returns a random duration in [0, span).
func jitter(span time.Duration) time.Duration {
	if span <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(span)))
}

func sleepDuration(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func requestError(ctx context.Context, method, rawURL string, err error) error {
	if contextError := ctx.Err(); contextError != nil {
		return contextError
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return fmt.Errorf("%s %s: request failed", method, auth.RedactURL(rawURL))
}

func transferRequestError(ctx context.Context, watch *netio.IdleWatch, method, rawURL string, err error) error {
	cause := watch.Error(err)
	var idle *netio.IdleTimeoutError
	if errors.As(cause, &idle) {
		return idle
	}
	// A caller may attach an operationally meaningful cancellation cause (for
	// example, a failed sibling in an errgroup). Do not flatten that to the
	// generic context.Canceled classification below.
	if parentCause := context.Cause(ctx); parentCause != nil {
		return parentCause
	}
	// requestError deliberately suppresses raw transport text because it can
	// contain signed URLs or credentials. Parent cancellation and the optional
	// absolute transfer deadline are still preserved through ctx.
	return requestError(ctx, method, rawURL, err)
}
func (c *Client) redact(value string) string {
	for _, secret := range c.redactValues {
		value = strings.ReplaceAll(value, secret, "REDACTED")
	}
	return value
}

func (c *Client) untrustedDetail(value string) string {
	value = c.redact(value)
	if len(value) <= maxErrorBody {
		return value
	}
	return strings.ToValidUTF8(value[:maxErrorBody], "�") + "\n[detail truncated]"
}

func rejectRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}
