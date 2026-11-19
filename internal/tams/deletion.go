package tams

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	pathpkg "path"
	"strings"
	"time"

	"github.com/livewyer-ops/tamsin/internal/auth"
	"github.com/livewyer-ops/tamsin/internal/tamstime"
)

// DeleteSegments removes the selected Flow Segments and does not return until
// the exact Object/timerange tuple is absent. TAMS says even a 204 response
// means the Segments "have been or will be deleted", so the response status is
// an acknowledgement rather than a terminal state.
func (c *Client) DeleteSegments(ctx context.Context, flowID string, options SegmentDeleteOptions) error {
	if strings.TrimSpace(options.Timerange) == "" {
		return errors.New("segment deletion requires a timerange")
	}
	if strings.TrimSpace(options.ObjectID) == "" {
		return errors.New("terminal segment deletion requires an object ID")
	}
	// Deletion is asynchronous on the service side and confirmed by polling,
	// so it has a deadline of its own; each request inside still gets the
	// metadata timeout.
	if c.deletionTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.deletionTimeout)
		defer cancel()
	}
	query := make(url.Values)
	query.Set("timerange", options.Timerange)
	query.Set("object_id", options.ObjectID)
	path := "flows/" + escapeSegment(flowID) + "/segments?" + query.Encode()
	var initial DeletionRequest
	response, err := c.doJSONResponse(ctx, http.MethodDelete, path, nil, &initial, http.StatusNoContent, http.StatusAccepted)
	if err != nil {
		deleteErr := fmt.Errorf("delete segments from flow %s: %w", flowID, err)
		if response.StatusCode != http.StatusNotFound && !response.AmbiguousMutation {
			return deleteErr
		}
		if confirmErr := c.waitForSegmentAbsence(ctx, flowID, options); confirmErr != nil {
			return errors.Join(deleteErr, fmt.Errorf("confirm segment deletion from flow %s: %w", flowID, confirmErr))
		}
		return nil
	}
	if response.StatusCode == http.StatusAccepted {
		if err := c.waitForDeletionRequest(ctx, path, response.Header.Get("Location"), flowID, options, initial); err != nil {
			if !errors.Is(err, errDeletionRequestFailed) {
				return err
			}
			// The service reports its request as failed, yet the Segment may
			// already be gone; its absence is the fact that matters.
			present, presentErr := c.segmentPresent(ctx, flowID, options)
			if presentErr != nil {
				return errors.Join(err, fmt.Errorf("confirm segment deletion from flow %s: %w", flowID, presentErr))
			}
			if present {
				return err
			}
			return nil
		}
	}
	if err := c.waitForSegmentAbsence(ctx, flowID, options); err != nil {
		return fmt.Errorf("confirm segment deletion from flow %s: %w", flowID, err)
	}
	return nil
}

// DeletionTimeout is the deadline one Segment deletion is given. A nil
// client, which a dry run passes around, has none.
func (c *Client) DeletionTimeout() time.Duration {
	if c == nil {
		return 0
	}
	return c.deletionTimeout
}

var errDeletionRequestFailed = errors.New("segment deletion request failed")

// waitForDeletionRequest follows a 202's deletion request to its end. A
// request that cannot be followed, because the service sent no Location or
// one outside the configured API, is not an error in itself: the Segment's
// absence, which DeleteSegments confirms afterwards, is the authoritative
// result. Credentials are only ever sent to a reference apiReference accepts.
func (c *Client) waitForDeletionRequest(ctx context.Context, deletePath, location, flowID string, options SegmentDeleteOptions, initial DeletionRequest) error {
	if strings.TrimSpace(location) == "" {
		c.logger.Warn("TAMS accepted segment deletion without a Location header; confirming by segment absence", "flow_id", flowID)
		return nil
	}
	deleteURL, err := c.resolve(deletePath)
	if err != nil {
		return err
	}
	reference, err := c.apiReference(deleteURL, location)
	if err != nil {
		c.logger.Warn("segment deletion request cannot be followed; confirming by segment absence",
			"flow_id", flowID, "reason", err.Error())
		return nil
	}

	request := initial
	for {
		if request.Status != "" {
			complete, err := validateDeletionRequest(request, flowID, options.Timerange)
			if err != nil {
				return err
			}
			if complete {
				return nil
			}
		}

		if err := waitForPoll(ctx, c.deletePollInterval); err != nil {
			return fmt.Errorf("wait for segment deletion request: %w", err)
		}
		request = DeletionRequest{}
		if err := c.doJSON(ctx, http.MethodGet, reference, nil, &request, http.StatusOK); err != nil {
			var httpErr *HTTPError
			if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
				// Completed requests may expire before a client observes their final
				// state. Exact Segment absence below remains the authoritative result.
				return nil
			}
			return fmt.Errorf("read segment deletion request: %w", err)
		}
	}
}

func validateDeletionRequest(request DeletionRequest, flowID, timerange string) (bool, error) {
	if strings.TrimSpace(request.ID) == "" {
		return false, errors.New("segment deletion request has no ID")
	}
	if request.FlowID != flowID {
		return false, errors.New("segment deletion request targets an unexpected flow")
	}
	if !tamstime.EqualTimeRanges(request.TimerangeToDelete, timerange) {
		return false, errors.New("segment deletion request targets an unexpected timerange")
	}
	if request.DeleteFlow {
		return false, errors.New("segment deletion request unexpectedly deletes the Flow")
	}
	switch request.Status {
	case "created", "started":
		return false, nil
	case "done":
		return true, nil
	case "error":
		return false, errDeletionRequestFailed
	default:
		return false, errors.New("segment deletion request has an unknown status")
	}
}

func (c *Client) segmentPresent(ctx context.Context, flowID string, options SegmentDeleteOptions) (bool, error) {
	segments, err := c.ListSegments(ctx, flowID, SegmentListOptions{
		ObjectID: options.ObjectID, Timerange: options.Timerange,
	})
	if err != nil {
		return false, err
	}
	for _, segment := range segments {
		if tamstime.EqualTimeRanges(segment.Timerange, options.Timerange) && segment.ObjectID == options.ObjectID {
			return true, nil
		}
	}
	return false, nil
}

func (c *Client) waitForSegmentAbsence(ctx context.Context, flowID string, options SegmentDeleteOptions) error {
	for {
		present, err := c.segmentPresent(ctx, flowID, options)
		if err != nil {
			return err
		}
		if !present {
			return nil
		}
		if err := waitForPoll(ctx, c.deletePollInterval); err != nil {
			return err
		}
	}
}

func waitForPoll(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

// apiReference resolves a response reference according to HTTP Location rules,
// then constrains it to the configured TAMS API origin and path before turning
// it back into a request path understood by doJSON.
func (c *Client) apiReference(requestURL *url.URL, value string) (string, error) {
	reference, err := url.Parse(value)
	if err != nil {
		return "", errors.New("reference is not a valid URL")
	}
	if reference.User != nil {
		return "", errors.New("reference must not contain userinfo")
	}
	if reference.Fragment != "" {
		return "", errors.New("reference must not contain a fragment")
	}
	resolved := requestURL.ResolveReference(reference)
	if resolved.User != nil {
		return "", errors.New("reference must not contain userinfo")
	}
	if auth.Origin(resolved) != c.baseOrigin {
		return "", fmt.Errorf("reference points at %s, not the configured endpoint", auth.Origin(resolved))
	}
	basePath := strings.TrimRight(c.base.EscapedPath(), "/")
	if resolved.EscapedPath() != basePath && !strings.HasPrefix(resolved.EscapedPath(), basePath+"/") {
		return "", errors.New("reference is outside the configured API path")
	}
	// ResolveReference removes literal dot segments. Check the decoded path as
	// well so percent-encoded dot or slash segments cannot survive validation
	// and be normalised by a proxy into a path outside the API base.
	decodedBase := pathpkg.Clean("/" + strings.TrimLeft(c.base.Path, "/"))
	decodedPath := pathpkg.Clean("/" + strings.TrimLeft(resolved.Path, "/"))
	if decodedBase != "/" && decodedPath != decodedBase && !strings.HasPrefix(decodedPath, decodedBase+"/") {
		return "", errors.New("reference is outside the configured API path")
	}
	if err := rejectAmbiguousPathEncoding(resolved.EscapedPath()); err != nil {
		return "", err
	}
	path := strings.TrimPrefix(resolved.EscapedPath(), basePath)
	if resolved.RawQuery != "" {
		path += "?" + resolved.RawQuery
	}
	return strings.TrimLeft(path, "/"), nil
}

// rejectAmbiguousPathEncoding applies URL decoding until the path is stable,
// with a fixed depth bound so an adversarially layered cursor cannot turn
// validation into quadratic work. A proxy or backend that decodes more often
// than net/url must not be able to discover a separator, backslash, or dot
// segment that validation did not see before credentials were attached to the
// follow-up request.
func rejectAmbiguousPathEncoding(escapedPath string) error {
	const maxDecodeDepth = 16
	current := escapedPath
	for range maxDecodeDepth {
		if strings.Contains(current, `\`) || hasDotPathSegment(current) {
			return errors.New("reference path contains ambiguous encoded traversal or separator")
		}
		decoded, err := url.PathUnescape(current)
		if err != nil {
			return errors.New("reference path contains malformed recursive escaping")
		}
		if decoded == current {
			return nil
		}
		if strings.Count(decoded, "/") != strings.Count(current, "/") || strings.Contains(decoded, `\`) || hasDotPathSegment(decoded) {
			return errors.New("reference path contains ambiguous encoded traversal or separator")
		}
		current = decoded
	}
	return errors.New("reference path has excessive recursive escaping")
}

func hasDotPathSegment(value string) bool {
	for _, segment := range strings.Split(value, "/") {
		if segment == "." || segment == ".." {
			return true
		}
	}
	return false
}
