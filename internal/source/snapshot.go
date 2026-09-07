package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/livewyer-ops/tamsin/internal/netio"
)

// StreamUnavailableError permits staging before any Flow or Object is written.
// Authentication, changed revisions and malformed range responses are errors,
// not reasons to fall back onto a different representation.
type StreamUnavailableError struct{ Reason string }

func (e *StreamUnavailableError) Error() string { return e.Reason }

var ErrSnapshotChanged = errors.New("remote input changed during ingest")

// Snapshot identifies one finite representation. Resource may contain a signed
// query and must only be used as fingerprint input, never in diagnostics or tags.
type Snapshot struct {
	Resource string
	Revision string
	Size     int64
	OpenAt   func(context.Context, int64) (io.ReadCloser, error)
}

func snapshotURL(value *url.URL) string {
	copyURL := *value
	copyURL.User = nil
	copyURL.Fragment = ""
	copyURL.RawFragment = ""
	return copyURL.String()
}

func (r *Resolver) httpRange(ctx context.Context, target *url.URL, headers http.Header, byteRange, etag string) (*http.Response, error) {
	watch := netio.NewIdleWatch(ctx, r.transferIdleTimeout)
	request, err := http.NewRequestWithContext(watch.Context(), http.MethodGet, target.String(), nil)
	if err != nil {
		watch.Stop()
		return nil, errors.New("create remote input request")
	}
	request.Header = headers.Clone()
	if request.Header == nil {
		request.Header = make(http.Header)
	}
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("Range", byteRange)
	request.Header.Del("If-Range")
	if etag != "" {
		request.Header.Set("If-Match", etag)
	}
	response, err := r.httpClient.Do(request)
	if err != nil {
		cause := watch.Error(err)
		watch.Stop()
		var idle *netio.IdleTimeoutError
		if errors.As(cause, &idle) || context.Cause(ctx) != nil || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
			return nil, withPublicError("Remote input request failed.", cause)
		}
		// A transport error can contain the complete request, including a
		// signed query. The operation alone is enough here.
		return nil, withPublicError("Remote input request failed.", errors.New("remote input request failed"))
	}
	response.Body = watch.DemandBody(response.Body)
	return response, nil
}

func (r *Resolver) httpSnapshot(ctx context.Context, target *url.URL) (*Snapshot, error) {
	response, err := r.httpRange(ctx, target, r.httpHeaders, "bytes=0-0", "")
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusOK || response.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		return nil, &StreamUnavailableError{Reason: "HTTP input does not provide finite byte ranges"}
	}
	if response.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("inspect remote input: HTTP %d", response.StatusCode)
	}
	etag := response.Header.Get("ETag")
	if !validStrongETag(etag) {
		return nil, &StreamUnavailableError{Reason: "HTTP input does not provide a strong ETag"}
	}
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return nil, &StreamUnavailableError{Reason: "HTTP input does not provide identity-encoded byte ranges"}
	}
	span, err := parseContentRange(response.Header.Get("Content-Range"))
	if strings.HasSuffix(response.Header.Get("Content-Range"), "/*") {
		return nil, &StreamUnavailableError{Reason: "HTTP input does not provide its complete length"}
	}
	if err != nil || span.start != 0 || span.end != 0 || (response.ContentLength >= 0 && response.ContentLength != 1) {
		return nil, errors.New("remote input returned an invalid initial byte range or length")
	}
	if _, err := io.Copy(io.Discard, exactRangeBody(response.Body, 1)); err != nil {
		return nil, errors.New("remote input returned an incomplete initial byte range")
	}
	// Reuse the discovered resource, including its signing query. Preserve
	// headers after redirect policy has stripped cross-origin credentials.
	target = response.Request.URL
	headers := response.Request.Header.Clone()
	resource := snapshotURL(target)
	snapshot := &Snapshot{Resource: resource, Revision: etag, Size: span.total}
	snapshot.OpenAt = func(ctx context.Context, offset int64) (io.ReadCloser, error) {
		if offset < 0 || offset > snapshot.Size {
			return nil, errors.New("remote input offset is outside its representation")
		}
		if offset == snapshot.Size {
			return http.NoBody, nil
		}
		response, err := r.httpRange(ctx, target, headers, "bytes="+strconv.FormatInt(offset, 10)+"-", etag)
		if err != nil {
			return nil, err
		}
		fail := func(err error) (io.ReadCloser, error) {
			_ = response.Body.Close()
			return nil, err
		}
		// An ETag is scoped to one resource; matching validators on different
		// redirect targets do not prove that their bytes are equivalent.
		if snapshotURL(response.Request.URL) != resource || response.StatusCode == http.StatusPreconditionFailed || response.Header.Get("ETag") != etag {
			return fail(ErrSnapshotChanged)
		}
		if response.StatusCode != http.StatusPartialContent {
			return fail(fmt.Errorf("read remote input range: HTTP %d", response.StatusCode))
		}
		if encoding := response.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
			return fail(errors.New("remote input applied encoding to a pinned byte range"))
		}
		if _, err := validateCompleteRange(response.Header.Get("Content-Range"), offset, snapshot.Size, response.ContentLength); err != nil {
			return fail(errors.New("remote input returned an invalid pinned byte range or length"))
		}
		return exactRangeBody(response.Body, snapshot.Size-offset), nil
	}
	return snapshot, nil
}

func (r *Resolver) s3Snapshot(ctx context.Context, client S3API, bucket, key string) (*Snapshot, error) {
	metadataCtx, cancel := r.metadataContext(ctx)
	head, err := client.HeadObject(metadataCtx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	cancel()
	if err != nil {
		return nil, withPublicError("The S3 input could not be inspected.", err)
	}
	etag, version := aws.ToString(head.ETag), aws.ToString(head.VersionId)
	if version == "null" {
		version = ""
	}
	if head.ContentLength == nil || *head.ContentLength <= 0 || (version == "" && !validStrongETag(etag)) {
		return nil, &StreamUnavailableError{Reason: "S3 input does not provide a finite length and stable revision"}
	}
	endpoint := r.s3Config.Endpoint
	if endpoint == "" {
		endpoint = "https://s3.amazonaws.com"
	}
	if parsed, err := url.Parse(endpoint); err == nil {
		endpoint = snapshotURL(parsed)
	}
	// Length framing prevents endpoint, bucket and key boundaries colliding.
	resource := fmt.Sprintf("s3:%d:%s%d:%s%d:%s", len(endpoint), endpoint, len(bucket), bucket, len(key), key)
	revision := "etag:" + etag
	if version != "" {
		revision = "version:" + version
	}
	snapshot := &Snapshot{Resource: resource, Revision: revision, Size: *head.ContentLength}
	get := func(ctx context.Context, offset, end int64, discover bool) (io.ReadCloser, error) {
		request := &s3.GetObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
			Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset, end)),
		}
		if version != "" {
			request.VersionId = aws.String(version)
		} else {
			request.IfMatch = aws.String(etag)
		}
		watch := netio.NewIdleWatch(ctx, r.transferIdleTimeout)
		response, err := client.GetObject(watch.Context(), request)
		if err != nil {
			cause := watch.Error(err)
			watch.Stop()
			if isPreconditionFailed(err) || isNotFound(err) {
				return nil, ErrSnapshotChanged
			}
			return nil, withPublicError("The S3 input range could not be read.", cause)
		}
		body := watch.DemandBody(response.Body)
		fail := func(err error) (io.ReadCloser, error) {
			_ = body.Close()
			return nil, err
		}
		if (version != "" && aws.ToString(response.VersionId) != version) || (etag != "" && aws.ToString(response.ETag) != etag) {
			return fail(ErrSnapshotChanged)
		}
		if discover && aws.ToString(response.ContentRange) == "" {
			return fail(&StreamUnavailableError{Reason: "S3 input does not provide byte ranges"})
		}
		span, err := parseContentRange(aws.ToString(response.ContentRange))
		if err != nil || span.start != offset || span.end != end || span.total != snapshot.Size || response.ContentLength == nil || *response.ContentLength != end-offset+1 {
			return fail(errors.New("S3 input returned an invalid pinned byte range or length"))
		}
		return exactRangeBody(body, end-offset+1), nil
	}
	snapshot.OpenAt = func(ctx context.Context, offset int64) (io.ReadCloser, error) {
		if offset < 0 || offset > snapshot.Size {
			return nil, errors.New("S3 input offset is outside its representation")
		}
		if offset == snapshot.Size {
			return http.NoBody, nil
		}
		return get(ctx, offset, snapshot.Size-1, false)
	}
	// Prove range support before allowing the input into the streaming pipeline.
	body, err := get(ctx, 0, 0, true)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	if _, err := io.Copy(io.Discard, body); err != nil {
		return nil, errors.New("S3 input returned an incomplete initial byte range")
	}
	return snapshot, nil
}
