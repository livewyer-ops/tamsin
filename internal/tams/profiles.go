package tams

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/livewyer-ops/tamsin/internal/auth"
)

// ProfileListOptions are the server-side filters defined by TAMS 8.2.
type ProfileListOptions struct {
	Format string
	Codec  string
	Label  string
}

// Profiles lists every matching immutable Flow Profile, following the store's
// opaque paging cursor with the same origin and resource bounds as Segments.
func (c *Client) Profiles(ctx context.Context, options ProfileListOptions) ([]Profile, error) {
	query := make(url.Values)
	query.Set("limit", "1000")
	if options.Format != "" {
		query.Set("format", options.Format)
	}
	if options.Codec != "" {
		query.Set("codec", options.Codec)
	}
	if options.Label != "" {
		query.Set("label", options.Label)
	}
	requestPath := "service/profiles?" + query.Encode()
	requestURL, err := c.resolve(requestPath)
	if err != nil {
		return nil, err
	}

	var profiles []Profile
	seen := make(map[string]struct{})
	responseBytes := 0
	for range c.segmentPageLimit {
		pageKey := requestURL.String()
		if _, exists := seen[pageKey]; exists {
			return nil, fmt.Errorf("TAMS paging cursor cycle repeats %s", auth.RedactURL(pageKey))
		}
		seen[pageKey] = struct{}{}

		var page []Profile
		headers, pageBytes, err := c.doJSONHeaders(ctx, http.MethodGet, requestPath, nil, &page, http.StatusOK)
		if err != nil {
			return nil, err
		}
		if pageBytes > c.segmentByteLimit-responseBytes {
			return nil, fmt.Errorf("listing Profiles exceeded %d response bytes", c.segmentByteLimit)
		}
		responseBytes += pageBytes
		if len(page) > c.segmentCountLimit-len(profiles) {
			return nil, fmt.Errorf("listing Profiles exceeded %d Profiles", c.segmentCountLimit)
		}
		profiles = append(profiles, page...)

		next, hasNext, err := c.pageReference(requestURL, headers.Values("Link"))
		if err != nil {
			return nil, err
		}
		if !hasNext {
			return profiles, nil
		}
		requestPath = next
		requestURL, err = c.resolve(requestPath)
		if err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("listing Profiles exceeded %d pages", c.segmentPageLimit)
}
