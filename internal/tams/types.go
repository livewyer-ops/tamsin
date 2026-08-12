package tams

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Flow is intentionally open because TAMS Flow metadata is type-dependent and
// the CLI also accepts operator-supplied extension fields.
type Flow map[string]any

// Profile remains open for the same reason as Flow: flow_metadata is selected
// by media format and services may preserve extension metadata.
type Profile map[string]any

type StorageBackend struct {
	ID               string            `json:"id"`
	Label            string            `json:"label,omitempty"`
	DefaultStorage   bool              `json:"default_storage,omitempty"`
	Type             string            `json:"type,omitempty"` // TAMS 8.1/TAMOSS compatibility.
	StoreType        string            `json:"store_type,omitempty"`
	Provider         string            `json:"provider,omitempty"`
	Region           string            `json:"region,omitempty"`
	AvailabilityZone string            `json:"availability_zone,omitempty"`
	StoreProduct     string            `json:"store_product,omitempty"`
	Tags             map[string]string `json:"tags,omitempty"`
}

type StorageRequest struct {
	Limit       int      `json:"limit,omitempty"`
	ObjectIDs   []string `json:"object_ids,omitempty"`
	StorageID   string   `json:"storage_id,omitempty"`
	ContentType string   `json:"content_type,omitempty"`
	Presigned   *bool    `json:"presigned,omitempty"`
}

type StorageResponse struct {
	MediaObjects []AllocatedObject `json:"media_objects"`
}

type AllocatedObject struct {
	ObjectID  string       `json:"object_id"`
	PutURL    PresignedURL `json:"put_url"`
	Presigned *bool        `json:"presigned,omitempty"`
}

// UploadReceipt describes the bytes accepted by one successful PUT attempt.
// SHA256 is calculated over the request body as it is transmitted, so callers
// can stop a changed local file from being registered under a digest-derived
// Object identifier. StorageSHA256 is reserved for independently validated
// provider evidence and remains empty when the response offers none.
type UploadReceipt struct {
	Bytes         int64
	SHA256        string
	StorageSHA256 string
}

// PresignedURL accepts both the upstream v8.1 "content-type" member and the
// newer TAMOSS headers object without losing provider-required headers.
type PresignedURL struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
	// StartBefore is the latest time another HTTP attempt may begin. It is
	// populated by the ingest scheduler from min_presigned_url_timeout after a
	// URL-producing response arrives and is deliberately not part of TAMS JSON.
	// A request already in flight may continue beyond it; only a new attempt
	// would present an expired signature.
	StartBefore time.Time `json:"-"`
}

func (p *PresignedURL) UnmarshalJSON(data []byte) error {
	var raw struct {
		URL         string            `json:"url"`
		Headers     map[string]string `json:"headers"`
		ContentType string            `json:"content-type"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	p.URL = raw.URL
	p.StartBefore = time.Time{}
	var err error
	p.Headers, err = normalizePresignedHeaders(raw.Headers)
	if err != nil {
		return err
	}
	if _, exists := p.Headers["Content-Type"]; raw.ContentType != "" && !exists {
		p.Headers["Content-Type"] = raw.ContentType
	}
	return nil
}

func normalizePresignedHeaders(headers map[string]string) (map[string]string, error) {
	normalized := make(map[string]string, len(headers))
	for key, value := range headers {
		if !validHTTPHeaderName(key) {
			return nil, errors.New("presigned URL contains an invalid header name")
		}
		canonical := http.CanonicalHeaderKey(key)
		if _, duplicate := normalized[canonical]; duplicate {
			return nil, errors.New("presigned URL contains duplicate header names")
		}
		normalized[canonical] = value
	}
	return normalized, nil
}

func validHTTPHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for index := range len(name) {
		character := name[index]
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(character)) {
			continue
		}
		return false
	}
	return true
}

type SegmentRequest struct {
	ObjectID        string `json:"object_id"`
	InitObjectID    string `json:"init_object_id,omitempty"`
	Timerange       string `json:"timerange"`
	ObjectTimerange string `json:"object_timerange,omitempty"`
	TSOffset        string `json:"ts_offset,omitempty"`
	LastDuration    string `json:"last_duration,omitempty"`
	KeyFrameCount   *int   `json:"key_frame_count,omitempty"`
}

// FailedSegment is one entry of a partial bulk registration. TAMS answers a
// bulk POST with 201 when every Segment was created, or 200 carrying the ones
// that were not, so a 200 is a failure report rather than a success.
type FailedSegment struct {
	ObjectID  string          `json:"object_id"`
	Timerange string          `json:"timerange,omitempty"`
	Error     json.RawMessage `json:"error,omitempty"`
}

type BulkSegmentFailure struct {
	FailedSegments []FailedSegment `json:"failed_segments"`
}

// PartialSegmentRegistrationError is the authoritative result of a bulk
// registration that TAMS accepted only in part. Unlike a transport error, this
// is not ambiguous: FailedSegments were rejected and RegisteredSegments are
// the complement that the service committed.
//
// Keeping the structured result matters to callers that must resolve every
// Segment in the batch. Flattening it into an error string throws away the one
// piece of information that lets them clean up without a racy read-after-write.
type PartialSegmentRegistrationError struct {
	FailedSegments     []FailedSegment
	RegisteredSegments []SegmentRequest
	Total              int
}

func (e *PartialSegmentRegistrationError) Error() string {
	objects := make([]string, 0, len(e.FailedSegments))
	for _, failed := range e.FailedSegments {
		objects = append(objects, failed.ObjectID)
	}
	return fmt.Sprintf("register %d of %d segments: %s",
		len(e.FailedSegments), e.Total, strings.Join(objects, ", "))
}

type Segment struct {
	ObjectID        string         `json:"object_id"`
	Timerange       string         `json:"timerange"`
	ObjectTimerange string         `json:"object_timerange,omitempty"`
	TSOffset        string         `json:"ts_offset,omitempty"`
	GetURLs         []PresignedURL `json:"get_urls,omitempty"`
	InitObject      *ObjectCore    `json:"init_object,omitempty"`
}

// ObjectCore is the object metadata nested under an 8.2 Segment for an
// initialisation object. Extra storage metadata is retained without forcing
// the client to know every provider-specific field.
type ObjectCore map[string]any

// SegmentDeleteOptions identifies the exact Segment or set of Segments to
// remove. Integrity cleanup supplies both fields so an overlapping Segment
// written by another producer cannot be removed accidentally.
type SegmentDeleteOptions struct {
	Timerange string
	ObjectID  string
}

// DeletionRequest is the TAMS resource returned while Segment removal
// continues asynchronously.
type DeletionRequest struct {
	ID                 string          `json:"id"`
	FlowID             string          `json:"flow_id"`
	TimerangeToDelete  string          `json:"timerange_to_delete"`
	TimerangeRemaining string          `json:"timerange_remaining,omitempty"`
	DeleteFlow         bool            `json:"delete_flow"`
	Status             string          `json:"status"`
	Error              json.RawMessage `json:"error,omitempty"`
}

type ObjectInfo map[string]any

type ObjectInstanceRequest struct {
	StorageID string `json:"storage_id,omitempty"`
	URL       string `json:"url,omitempty"`
	Label     string `json:"label,omitempty"`
}

type HTTPError struct {
	Method     string
	URL        string
	StatusCode int
	Status     string
	Body       string
}

func (e *HTTPError) Error() string {
	body := strings.TrimSpace(e.Body)
	if body == "" {
		return fmt.Sprintf("%s %s: %s", e.Method, e.URL, e.Status)
	}
	return fmt.Sprintf("%s %s: %s: %s", e.Method, e.URL, e.Status, body)
}
