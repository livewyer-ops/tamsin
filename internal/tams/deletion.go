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
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
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
			return err
		}
	}
	if err := c.waitForSegmentAbsence(ctx, flowID, options); err != nil {
		return fmt.Errorf("confirm segment deletion from flow %s: %w", flowID, err)
	}
	return nil
}

func (c *Client) waitForDeletionRequest(ctx context.Context, deletePath, location, flowID string, options SegmentDeleteOptions, initial DeletionRequest) error {
	if strings.TrimSpace(location) == "" {
		return errors.New("TAMS accepted segment deletion without a Location header")
	}
	deleteURL, err := c.resolve(deletePath)
	if err != nil {
		return err
	}
	reference, err := c.apiReference(deleteURL, location)
	if err != nil {
		return fmt.Errorf("invalid segment deletion Location: %w", err)
	}

	request := initial
	for {
		if request.Status != "" {
			complete, err := c.validateDeletionRequest(request, flowID, options.Timerange)
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

func (c *Client) validateDeletionRequest(request DeletionRequest, flowID, timerange string) (bool, error) {
	if strings.TrimSpace(request.ID) == "" {
		return false, errors.New("segment deletion request has no ID")
	}
	if request.FlowID != flowID {
		if c.suppressErrorBody {
			return false, errors.New("segment deletion request targets an unexpected flow")
		}
		return false, fmt.Errorf("segment deletion request targets flow %q, want %q", c.untrustedDetail(request.FlowID), flowID)
	}
	if request.TimerangeToDelete != timerange {
		if c.suppressErrorBody {
			return false, errors.New("segment deletion request targets an unexpected timerange")
		}
		return false, fmt.Errorf("segment deletion request targets timerange %q, want %q", c.untrustedDetail(request.TimerangeToDelete), timerange)
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
		if c.suppressErrorBody {
			return false, errors.New("segment deletion request failed")
		}
		detail := strings.TrimSpace(string(request.Error))
		if detail == "" {
			detail = "no error detail"
		}
		return false, fmt.Errorf("segment deletion request failed: %s", c.untrustedDetail(detail))
	default:
		if c.suppressErrorBody {
			return false, errors.New("segment deletion request has an unknown status")
		}
		return false, fmt.Errorf("segment deletion request has unknown status %q", c.untrustedDetail(request.Status))
	}
}

func (c *Client) waitForSegmentAbsence(ctx context.Context, flowID string, options SegmentDeleteOptions) error {
	for {
		segments, err := c.ListSegments(ctx, flowID, SegmentListOptions{
			ObjectID: options.ObjectID, Timerange: options.Timerange,
		})
		if err != nil {
			return err
		}
		found := false
		for _, segment := range segments {
			if segment.Timerange == options.Timerange && (options.ObjectID == "" || segment.ObjectID == options.ObjectID) {
				found = true
				break
			}
		}
		if !found {
			return nil
		}
		if err := waitForPoll(ctx, c.deletePollInterval); err != nil {
			return err
		}
	}
}

func waitForPoll(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Millisecond
	}
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
	if origin(resolved) != c.baseOrigin {
		return "", fmt.Errorf("reference points at %s, not the configured endpoint", origin(resolved))
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
