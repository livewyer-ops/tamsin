package tams

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/livewyer-ops/tamsin/internal/auth"
)

// SegmentListOptions selects what a Segment listing should contain.
type SegmentListOptions struct {
	// ObjectID filters to a single Media Object when set.
	ObjectID string
	// Timerange narrows the listing to Segments overlapping it. A caller that
	// only needs the Segments it has just written should say so: without it the
	// service returns, and signs URLs for, everything in the Flow.
	Timerange string
	// IncludeDownloadURLs asks the service for presigned get_urls. Generating
	// them is work proportional to the number of Segments returned, so a
	// listing that only needs to know which Objects already exist should leave
	// it off: deciding what to resume needs an Object identifier and a
	// timerange, nothing more.
	IncludeDownloadURLs bool
}

// Segments lists a Flow's Segments with download URLs. It is the convenience
// form for callers that will immediately read the media; callers that only
// need Segment identity should use ListSegments with the lean default options.
func (c *Client) Segments(ctx context.Context, flowID, objectID string) ([]Segment, error) {
	return c.ListSegments(ctx, flowID, SegmentListOptions{ObjectID: objectID, IncludeDownloadURLs: true})
}

// ListSegments lists a Flow's Segments, following the paging cursor to
// completion.
//
// Paging matters here: the service caps the page size and reports its own limit
// in X-Paging-Limit, so a Flow with more Segments than one page would otherwise
// come back silently truncated. A truncated listing reads as "these Segments do
// not exist", which would make a resume re-upload media that is already stored.
func (c *Client) ListSegments(ctx context.Context, flowID string, options SegmentListOptions) ([]Segment, error) {
	query := make(url.Values)
	query.Set("limit", "1000")
	if options.ObjectID != "" {
		query.Set("object_id", options.ObjectID)
	}
	if options.Timerange != "" {
		query.Set("timerange", options.Timerange)
	}
	if options.IncludeDownloadURLs {
		query.Set("presigned", "true")
		query.Set("verbose_storage", "true")
	} else {
		// An empty accept_get_urls asks for no get_urls at all. The spec calls
		// this out as the way to avoid making a service generate presigned URLs
		// a caller will discard. presigned=false narrows the same thing from the
		// other side — it is a filter for URLs that are not presigned, not an
		// instruction not to sign — so a service that implements one parameter
		// but not the other still skips the work.
		query.Set("accept_get_urls", "")
		query.Set("presigned", "false")
	}
	requestPath := "flows/" + escapeSegment(flowID) + "/segments?" + query.Encode()
	requestURL, err := c.resolve(requestPath)
	if err != nil {
		return nil, err
	}

	var segments []Segment
	seen := make(map[string]struct{})
	// A novel cursor stream can still be infinite, so cycle detection is backed
	// by a hard page bound.
	responseBytes := 0
	for range c.segmentPageLimit {
		pageKey := requestURL.String()
		if _, exists := seen[pageKey]; exists {
			return nil, fmt.Errorf("TAMS paging cursor cycle repeats %s", auth.RedactURL(pageKey))
		}
		seen[pageKey] = struct{}{}

		var page []Segment
		headers, pageBytes, err := c.doJSONHeaders(ctx, http.MethodGet, requestPath, nil, &page, http.StatusOK)
		if err != nil {
			return nil, err
		}
		if pageBytes > c.segmentByteLimit-responseBytes {
			return nil, fmt.Errorf("listing segments for flow %s exceeded %d response bytes", flowID, c.segmentByteLimit)
		}
		responseBytes += pageBytes
		if len(page) > c.segmentCountLimit-len(segments) {
			return nil, fmt.Errorf("listing segments for flow %s exceeded %d Segments", flowID, c.segmentCountLimit)
		}
		if !options.IncludeDownloadURLs {
			// The request asks a conforming service to omit get_urls, but keep the
			// caller's disclosure choice authoritative if a service ignores that
			// filter. This is repeated for every page because a Link cursor may
			// replace the original query entirely.
			for index := range page {
				page[index].GetURLs = nil
			}
		}
		segments = append(segments, page...)

		next, hasNext, err := c.pageReference(requestURL, headers.Values("Link"))
		if err != nil {
			return nil, err
		}
		if !hasNext {
			return segments, nil
		}
		requestPath = next
		requestURL, err = c.resolve(requestPath)
		if err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("listing segments for flow %s exceeded %d pages", flowID, c.segmentPageLimit)
}

// pageReference resolves a Link cursor against the exact page that supplied
// it, as RFC URL resolution requires. The resolved URL must remain inside the
// configured API origin and base path before it is handed to the credentialed
// metadata client.
func (c *Client) pageReference(requestURL *url.URL, headerValues []string) (string, bool, error) {
	next, found, err := nextPageURL(headerValues)
	if err != nil {
		return "", false, fmt.Errorf("invalid TAMS paging Link header: %w", err)
	}
	if !found {
		return "", false, nil
	}
	reference, err := c.apiReference(requestURL, next)
	if err != nil {
		return "", false, fmt.Errorf("invalid TAMS paging cursor: %w", err)
	}
	return reference, true, nil
}

// nextPageURL extracts one unambiguous rel=next target from every Link field
// value. RFC 8288 uses RFC 7230 list syntax, but commas and semicolons inside a
// URI-reference or quoted parameter are data rather than delimiters.
func nextPageURL(headerValues []string) (string, bool, error) {
	var next string
	found := false
	for fieldIndex, fieldValue := range headerValues {
		links, err := splitLinkValues(fieldValue)
		if err != nil {
			return "", false, fmt.Errorf("field value %d is malformed: %w", fieldIndex+1, err)
		}
		for linkIndex, encoded := range links {
			if strings.TrimSpace(encoded) == "" {
				continue
			}
			link, err := parseLinkValue(encoded)
			if err != nil {
				return "", false, fmt.Errorf("field value %d link %d is malformed: %w", fieldIndex+1, linkIndex+1, err)
			}
			if !link.next {
				continue
			}
			if link.anchor {
				return "", false, errors.New("rel=next uses an unsupported anchor context")
			}
			if found {
				return "", false, errors.New("multiple rel=next targets are ambiguous")
			}
			next = link.target
			found = true
		}
	}
	return next, found, nil
}

type parsedLinkValue struct {
	target string
	next   bool
	anchor bool
}

func splitLinkValues(fieldValue string) ([]string, error) {
	var values []string
	start := 0
	atLinkStart := true
	inTarget := false
	inQuote := false
	escaped := false
	for index := 0; index < len(fieldValue); index++ {
		character := fieldValue[index]
		if character == '\r' || character == '\n' {
			return nil, errors.New("line break is not allowed")
		}
		if inTarget {
			if character == '>' {
				inTarget = false
			}
			continue
		}
		if inQuote {
			switch {
			case escaped:
				escaped = false
			case character == '\\':
				escaped = true
			case character == '"':
				inQuote = false
			}
			continue
		}
		switch character {
		case '<':
			if atLinkStart {
				inTarget = true
			}
			atLinkStart = false
		case '"':
			inQuote = true
			atLinkStart = false
		case ',':
			values = append(values, fieldValue[start:index])
			start = index + 1
			atLinkStart = true
		case ' ', '\t':
			// Whitespace before the target does not end the beginning of a
			// link-value. Elsewhere it is just data for parseLinkValue to
			// validate.
		default:
			atLinkStart = false
		}
	}
	if inTarget {
		return nil, errors.New("unterminated URI-reference")
	}
	if inQuote || escaped {
		return nil, errors.New("unterminated quoted parameter")
	}
	values = append(values, fieldValue[start:])
	return values, nil
}

func parseLinkValue(encoded string) (parsedLinkValue, error) {
	value := strings.TrimSpace(encoded)
	if value == "" || value[0] != '<' {
		return parsedLinkValue{}, errors.New("link target must be enclosed in angle brackets")
	}
	targetEnd := strings.IndexByte(value, '>')
	if targetEnd < 0 {
		return parsedLinkValue{}, errors.New("unterminated URI-reference")
	}
	link := parsedLinkValue{target: value[1:targetEnd]}
	remainder := value[targetEnd+1:]
	seenRelation := false
	for {
		remainder = trimLinkWhitespace(remainder)
		if remainder == "" {
			return link, nil
		}
		if remainder[0] != ';' {
			return parsedLinkValue{}, errors.New("unexpected data after URI-reference")
		}
		remainder = trimLinkWhitespace(remainder[1:])
		nameLength := linkTokenLength(remainder)
		if nameLength == 0 {
			return parsedLinkValue{}, errors.New("link parameter has no valid name")
		}
		name := remainder[:nameLength]
		remainder = trimLinkWhitespace(remainder[nameLength:])
		parameterValue := ""
		hasValue := false
		if remainder != "" && remainder[0] == '=' {
			hasValue = true
			remainder = trimLinkWhitespace(remainder[1:])
			if remainder == "" {
				return parsedLinkValue{}, fmt.Errorf("link parameter %q has no value", name)
			}
			var err error
			if remainder[0] == '"' {
				parameterValue, remainder, err = consumeLinkQuotedString(remainder)
				if err != nil {
					return parsedLinkValue{}, fmt.Errorf("link parameter %q: %w", name, err)
				}
			} else if strings.EqualFold(name, "rel") {
				parameterValue, remainder, err = consumeUnquotedLinkRelationTypes(remainder)
				if err != nil {
					return parsedLinkValue{}, fmt.Errorf("link parameter %q: %w", name, err)
				}
			} else {
				valueLength := linkPTokenLength(remainder)
				if valueLength == 0 {
					return parsedLinkValue{}, fmt.Errorf("link parameter %q has an invalid value", name)
				}
				parameterValue = remainder[:valueLength]
				remainder = remainder[valueLength:]
			}
		}

		switch {
		case strings.EqualFold(name, "rel"):
			if seenRelation {
				return parsedLinkValue{}, errors.New("duplicate rel parameters are ambiguous")
			}
			seenRelation = true
			if !hasValue {
				return parsedLinkValue{}, errors.New("rel parameter has no relation type")
			}
			relations, err := parseLinkRelationTypes(parameterValue)
			if err != nil {
				return parsedLinkValue{}, fmt.Errorf("rel parameter: %w", err)
			}
			for _, relation := range relations {
				if strings.EqualFold(relation, "next") {
					link.next = true
				}
			}
		case strings.EqualFold(name, "anchor"):
			if !hasValue {
				return parsedLinkValue{}, errors.New("anchor parameter has no value")
			}
			link.anchor = true
		}
	}
}

func trimLinkWhitespace(value string) string {
	return strings.TrimLeft(value, " \t")
}

func linkTokenLength(value string) int {
	for index := 0; index < len(value); index++ {
		if !isLinkTokenCharacter(value[index]) {
			return index
		}
	}
	return len(value)
}

// linkPTokenLength implements the Web Linking ptokenchar grammar. Unlike an
// HTTP token, a ptoken can carry media types and URI-shaped values without
// quoting (for example text/html).
func linkPTokenLength(value string) int {
	for index := 0; index < len(value); index++ {
		if !isLinkPTokenCharacter(value[index]) {
			return index
		}
	}
	return len(value)
}

func isLinkPTokenCharacter(character byte) bool {
	if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9' {
		return true
	}
	return strings.ContainsRune("!#$%&'()*+-./:<=>?@[]^_`{|}~", rune(character))
}

func isLinkTokenCharacter(character byte) bool {
	if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9' {
		return true
	}
	return strings.ContainsRune("!#$%&'*+-.^_`|~", rune(character))
}

// consumeUnquotedLinkRelationTypes consumes the special relation-types value,
// whose space-separated members are not described by the generic parameter
// token grammar. Horizontal whitespace before a following parameter remains
// BWS; only ASCII spaces may separate relation types.
func consumeUnquotedLinkRelationTypes(value string) (string, string, error) {
	position := 0
	for {
		valueLength := linkPTokenLength(value[position:])
		if valueLength == 0 {
			return "", "", errors.New("invalid relation type")
		}
		position += valueLength
		if position == len(value) || value[position] == ';' {
			return value[:position], value[position:], nil
		}
		if value[position] != ' ' && value[position] != '\t' {
			return "", "", errors.New("invalid character after relation type")
		}

		whitespaceStart := position
		hasTab := false
		for position < len(value) && (value[position] == ' ' || value[position] == '\t') {
			hasTab = hasTab || value[position] == '\t'
			position++
		}
		if position == len(value) || value[position] == ';' {
			return value[:whitespaceStart], value[whitespaceStart:], nil
		}
		if hasTab {
			return "", "", errors.New("relation types must be separated by spaces")
		}
	}
}

func parseLinkRelationTypes(value string) ([]string, error) {
	if value == "" {
		return nil, errors.New("has no relation type")
	}
	if value[0] == ' ' || value[len(value)-1] == ' ' || strings.ContainsRune(value, '\t') {
		return nil, errors.New("has invalid whitespace")
	}
	relations := strings.Fields(value)
	for _, relation := range relations {
		if relation == "" || !isLinkRelationType(relation) {
			return nil, fmt.Errorf("%q is not a registered or extension relation type", relation)
		}
	}
	return relations, nil
}

func isLinkRelationType(value string) bool {
	if isRegisteredLinkRelationType(value) {
		return true
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character == '%' {
			if index+2 >= len(value) || !isHexDigit(value[index+1]) || !isHexDigit(value[index+2]) {
				return false
			}
			index += 2
			continue
		}
		if !isURICharacter(character) {
			return false
		}
	}
	relationURI, err := url.Parse(value)
	return err == nil && relationURI.IsAbs()
}

func isRegisteredLinkRelationType(value string) bool {
	if value == "" || !isASCIILetter(value[0]) {
		return false
	}
	for index := 1; index < len(value); index++ {
		character := value[index]
		if !isASCIILetter(character) && (character < '0' || character > '9') &&
			character != '.' && character != '-' {
			return false
		}
	}
	return true
}

func isASCIILetter(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z'
}

func isHexDigit(character byte) bool {
	return character >= '0' && character <= '9' || character >= 'a' && character <= 'f' ||
		character >= 'A' && character <= 'F'
}

func isURICharacter(character byte) bool {
	if isASCIILetter(character) || character >= '0' && character <= '9' {
		return true
	}
	return strings.ContainsRune("-._~:/?#[]@!$&'()*+,;=", rune(character))
}

func consumeLinkQuotedString(value string) (string, string, error) {
	var decoded strings.Builder
	for index := 1; index < len(value); index++ {
		character := value[index]
		switch {
		case character == '"':
			return decoded.String(), value[index+1:], nil
		case character == '\\':
			index++
			if index >= len(value) || value[index] == '\r' || value[index] == '\n' {
				return "", "", errors.New("invalid quoted-pair")
			}
			decoded.WriteByte(value[index])
		case character == '\t' || character >= 0x20 && character != 0x7f:
			decoded.WriteByte(character)
		default:
			return "", "", errors.New("invalid quoted character")
		}
	}
	return "", "", errors.New("unterminated quoted string")
}

// RegisterSegments registers several Flow Segments in one request. The endpoint
// accepts either a single Segment or an array, so batching turns one round trip
// per Media Object into one per Flow, which is the dominant cost on a
// high-latency link.
//
// A 201 means every Segment was created. A 200 is a partial success carrying
// the ones that were not, so it is reported as a failure naming them.
