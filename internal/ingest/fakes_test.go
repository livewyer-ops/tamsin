package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

type presentationCountingProber struct {
	presentations atomic.Int64
}

func (*presentationCountingProber) Probe(context.Context, string) (media.Probe, error) {
	return media.Probe{Streams: []media.Stream{{
		CodecName: "h264", CodecType: "video", Width: 64, Height: 64, AverageFrameRate: "25/1",
	}}}, nil
}

func (*presentationCountingProber) Version(context.Context) (string, error) {
	return "ffprobe version 5.1", nil
}

func (p *presentationCountingProber) ProbePresentation(context.Context, string, *media.Probe) error {
	p.presentations.Add(1)
	return nil
}

type fakeProber struct {
	failSuffix string
}

func (p fakeProber) Probe(_ context.Context, filename string) (media.Probe, error) {
	if p.failSuffix != "" && strings.HasSuffix(filename, p.failSuffix) {
		return media.Probe{}, errors.New("invalid media fixture")
	}
	probe := media.Probe{Format: media.Format{
		Name: "mov,mp4", Duration: "1.0", StartTime: "0.0",
		Tags: map[string]string{"major_brand": "isom"},
	}}
	probe.Streams = []media.Stream{{CodecName: "h264", CodecType: "video", Width: 64, Height: 64, AverageFrameRate: "25/1"}}
	return probe, nil
}

// muxedProber reports a container holding two elementary streams, which is what
// drives the multi-essence Flow path.
type muxedProber struct{}

func (muxedProber) Probe(context.Context, string) (media.Probe, error) {
	return media.Probe{
		Format: media.Format{Name: "mpegts", Duration: "1.0", StartTime: "0.0"},
		Streams: []media.Stream{
			{CodecName: "h264", CodecType: "video", Width: 64, Height: 64, AverageFrameRate: "25/1"},
			{CodecName: "aac", CodecType: "audio", SampleRate: "48000", Channels: 2},
		},
	}, nil
}

func (muxedProber) Version(context.Context) (string, error) {
	return "ffprobe version 5.1 test", nil
}

type multiAudioProber struct{}

func (multiAudioProber) Probe(context.Context, string) (media.Probe, error) {
	return media.Probe{
		Format: media.Format{Name: "mpegts", Duration: "1.0", StartTime: "0.0"},
		Streams: []media.Stream{
			{Index: 0, CodecName: "h264", CodecType: "video", Width: 64, Height: 64, AverageFrameRate: "25/1"},
			{Index: 1, CodecName: "aac", CodecType: "audio", SampleRate: "48000", Channels: 2},
			{Index: 2, CodecName: "aac", CodecType: "audio", SampleRate: "48000", Channels: 2},
		},
	}, nil
}

func (multiAudioProber) Version(context.Context) (string, error) {
	return "ffprobe version 5.1 test", nil
}

type fourEssenceProber struct{}

func (fourEssenceProber) Probe(context.Context, string) (media.Probe, error) {
	return media.Probe{
		Format: media.Format{Name: "mpegts", Duration: "2.0", StartTime: "0.0"},
		Streams: []media.Stream{
			{Index: 0, CodecName: "h264", CodecType: "video", Width: 64, Height: 64, AverageFrameRate: "25/1"},
			{Index: 1, CodecName: "aac", CodecType: "audio", SampleRate: "48000", Channels: 2},
			{Index: 2, CodecName: "aac", CodecType: "audio", SampleRate: "48000", Channels: 2},
			{Index: 3, CodecName: "aac", CodecType: "audio", SampleRate: "48000", Channels: 2},
		},
	}, nil
}

func (fourEssenceProber) Version(context.Context) (string, error) {
	return "ffprobe version 5.1 test", nil
}

func (fakeProber) Version(context.Context) (string, error) {
	return "ffprobe version 5.1 test", nil
}

type oldVersionProber struct{ fakeProber }

func (oldVersionProber) Version(context.Context) (string, error) {
	return "ffprobe version 4.4 unsupported", nil
}

type unknownContainerProber struct{}

func (unknownContainerProber) Probe(context.Context, string) (media.Probe, error) {
	return media.Probe{
		Format: media.Format{Name: "proprietary-container", Duration: "1.0", StartTime: "0.0"},
		Streams: []media.Stream{{
			CodecName: "h264", CodecType: "video", Width: 64, Height: 64, AverageFrameRate: "25/1",
		}},
	}, nil
}

func (unknownContainerProber) Version(context.Context) (string, error) {
	return "ffprobe version 5.1 test", nil
}

type fakeSegmenter struct{}

func (fakeSegmenter) Segment(_ context.Context, request media.SegmentRequest, sink media.SegmentSink) error {
	for _, streamIndex := range request.StreamIndices {
		prefix := "segment-"
		if streamIndex != media.AllStreams {
			prefix = "essence-" + strconv.Itoa(streamIndex) + "-"
		}
		paths := []string{
			filepath.Join(request.Directory, prefix+"00000000.mp4"),
			filepath.Join(request.Directory, prefix+"00000001.mp4"),
		}
		for index, path := range paths {
			if err := os.WriteFile(path, []byte{byte(index + 1)}, 0o600); err != nil {
				return err
			}
			record := media.SegmentRecord{StreamIndex: streamIndex, Path: path}
			if request.Duration > 0 {
				record.Timed = true
				record.Start = int64(index) * int64(request.Duration)
				record.End = record.Start + int64(request.Duration)
			}
			if err := sink(record); err != nil {
				return err
			}
		}
	}
	return nil
}

func (fakeSegmenter) Version(context.Context) (string, error) {
	return "ffmpeg version 5.1 test", nil
}

type recordingSegmenter struct {
	lock     sync.Mutex
	requests []media.SegmentRequest
}

func (s *recordingSegmenter) Segment(ctx context.Context, request media.SegmentRequest, sink media.SegmentSink) error {
	s.lock.Lock()
	s.requests = append(s.requests, request)
	s.lock.Unlock()
	return fakeSegmenter{}.Segment(ctx, request, sink)
}

func (*recordingSegmenter) Version(context.Context) (string, error) {
	return "ffmpeg version 5.1 recording test", nil
}

type failSecondEssenceSegmenter struct {
	lock  sync.Mutex
	calls int
}

func (s *failSecondEssenceSegmenter) Segment(ctx context.Context, request media.SegmentRequest, sink media.SegmentSink) error {
	s.lock.Lock()
	s.calls++
	call := s.calls
	s.lock.Unlock()
	if call == 2 {
		return errors.New("audio demultiplex failed")
	}
	return fakeSegmenter{}.Segment(ctx, request, sink)
}

func (*failSecondEssenceSegmenter) Version(context.Context) (string, error) {
	return "ffmpeg version 5.1 failing test", nil
}

type fakeClient struct {
	lock                 sync.Mutex
	flows                map[string]tams.Flow
	segments             map[string]map[string]tams.Segment
	objects              map[string][]byte
	backends             []tams.StorageBackend
	uploads              int
	allocations          int
	maxAllocationObjects int
	corruptOnUpload      bool
	storageChecksum      bool
	storageChecksumValue string
	flowOrder            map[string]int
	deletedTimeranges    []string
	deletedSegments      []tams.SegmentDeleteOptions
	deleteDeadlines      []time.Time
	listings             []tams.SegmentListOptions
	verified             []string
	// callLog records the order of store interactions, which is what proves an
	// Object is registered before the next batch is allocated.
	callLog         []string
	serviceDocument map[string]any
	profiles        map[string]tams.Profile
	profileReads    int
	serviceReads    int
	backendReads    int
	// flowReadErr fails the read that precedes a Flow write.
	flowReadErr error
	// backendsErr fails the startup storage backends request.
	backendsErr error
	// putFlowErrAt fails the numbered Flow PUT, allowing graph-transaction tests
	// to observe a prefix written before the collector.
	putFlowErrAt       int
	putFlowCalls       int
	flowStatusWrites   []string
	allocationStatuses []string
	// registerSegmentsErr fails a bulk registration; registerSegmentsCommit is
	// how many of the batch reach the store first. A commit of -1 means the
	// whole batch lands and only the response is lost.
	registerSegmentsErr    error
	registerSegmentsCommit int
	onRegisterSegments     func()
	listSegmentsErr        error
	listSegmentsOverride   []tams.Segment
	hasListingOverride     bool
	listSegmentsCalls      int
	deleteSegmentsErr      error
	deleteSegmentsErrors   map[string]error
	blockDeleteUntilDone   bool
	// onDownload fires as verification reads an Object back, which is the only
	// point where a test can interrupt a run that has already registered.
	onDownload func()
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		flows: make(map[string]tams.Flow), segments: make(map[string]map[string]tams.Segment),
		objects: make(map[string][]byte), backends: []tams.StorageBackend{{ID: "storage", DefaultStorage: true}},
		flowOrder: make(map[string]int),
		profiles:  make(map[string]tams.Profile),
	}
}

func (c *fakeClient) Profile(_ context.Context, id string) (tams.Profile, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.profileReads++
	profile := c.profiles[id]
	if profile == nil {
		return nil, &tams.HTTPError{Method: http.MethodGet, URL: "service/profiles/" + id,
			StatusCode: http.StatusNotFound, Status: "404 Not Found"}
	}
	return profile, nil
}

func (c *fakeClient) Service(context.Context) (map[string]any, error) {
	c.lock.Lock()
	c.serviceReads++
	c.lock.Unlock()
	if c.serviceDocument != nil {
		return c.serviceDocument, nil
	}
	return map[string]any{
		"api_version": "8.1", "min_object_timeout": "300:0", "min_presigned_url_timeout": "30:0",
	}, nil
}

func (c *fakeClient) StorageBackends(context.Context) ([]tams.StorageBackend, error) {
	c.lock.Lock()
	c.backendReads++
	c.lock.Unlock()
	if c.backendsErr != nil {
		return nil, c.backendsErr
	}
	return append([]tams.StorageBackend(nil), c.backends...), nil
}

// Flow answers 404 for a Flow that is not there, as TAMS does. Returning an
// empty Flow with no error would hide the difference between creating one and
// replacing one, which is the distinction the metadata rules turn on.
func (c *fakeClient) Flow(_ context.Context, id string) (tams.Flow, error) {
	c.record("flowRead:" + id)
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.flowReadErr != nil {
		return nil, c.flowReadErr
	}
	flow, present := c.flows[id]
	if !present {
		return nil, &tams.HTTPError{
			Method: http.MethodGet, URL: "flows/" + id,
			StatusCode: http.StatusNotFound, Status: "404 Not Found",
		}
	}
	return flow, nil
}

func (c *fakeClient) PutFlow(_ context.Context, id string, flow tams.Flow) (tams.Flow, error) {
	c.record("flowWrite:" + id)
	c.lock.Lock()
	defer c.lock.Unlock()
	c.putFlowCalls++
	if status, _ := flow["status"].(string); status != "" {
		c.flowStatusWrites = append(c.flowStatusWrites, status)
	}
	if c.putFlowErrAt > 0 && c.putFlowCalls == c.putFlowErrAt {
		return nil, errors.New("injected Flow PUT failure")
	}
	if _, seen := c.flowOrder[id]; !seen {
		// Registration order matters: a Collection Item may only reference a Flow
		// already registered in the service.
		c.flowOrder[id] = len(c.flowOrder)
	}
	stored := make(tams.Flow, len(flow)+len(profileTechnicalFields))
	for key, value := range flow {
		stored[key] = value
	}
	if profileID, _ := flow["profile_id"].(string); profileID != "" {
		if profile := c.profiles[profileID]; profile != nil {
			if metadata, ok := profile["flow_metadata"].(map[string]any); ok {
				for key, value := range metadata {
					stored[key] = value
				}
			}
		}
	}
	c.flows[id] = stored
	return stored, nil
}

func (c *fakeClient) record(call string) {
	c.lock.Lock()
	c.callLog = append(c.callLog, call)
	c.lock.Unlock()
}

func (c *fakeClient) AllocateStorage(_ context.Context, flowID string, request tams.StorageRequest) (tams.StorageResponse, error) {
	c.record(fmt.Sprintf("allocate:%d", len(request.ObjectIDs)))
	c.lock.Lock()
	c.allocations++
	status, _ := c.flows[flowID]["status"].(string)
	c.allocationStatuses = append(c.allocationStatuses, status)
	c.maxAllocationObjects = max(c.maxAllocationObjects, len(request.ObjectIDs))
	c.lock.Unlock()
	response := tams.StorageResponse{MediaObjects: make([]tams.AllocatedObject, len(request.ObjectIDs))}
	for index := range request.ObjectIDs {
		response.MediaObjects[index] = tams.AllocatedObject{ObjectID: request.ObjectIDs[index], PutURL: tams.PresignedURL{URL: "mem://" + request.ObjectIDs[index]}}
	}
	return response, nil
}

func (c *fakeClient) RegisterSegment(_ context.Context, flowID string, request tams.SegmentRequest) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.segments[flowID] == nil {
		c.segments[flowID] = make(map[string]tams.Segment)
	}
	c.segments[flowID][request.ObjectID] = tams.Segment{
		ObjectID: request.ObjectID, Timerange: request.Timerange,
		GetURLs: []tams.PresignedURL{{URL: "mem://" + request.ObjectID}},
	}
	return nil
}

// DeleteSegments drops every Segment whose timerange matches, mirroring the
// TAMS rule that Media Objects left unreferenced are removed with them.
func (c *fakeClient) RegisterSegments(ctx context.Context, flowID string, requests []tams.SegmentRequest) error {
	c.record(fmt.Sprintf("register:%d", len(requests)))
	c.lock.Lock()
	commit := c.registerSegmentsCommit
	failure := c.registerSegmentsErr
	c.lock.Unlock()
	// A partial bulk registration: TAMS answers with a success status and a
	// list of the Segments that failed, which the client turns into an error
	// even though some of the batch is now in the store.
	if failure != nil && commit >= 0 && commit < len(requests) {
		for _, request := range requests[:commit] {
			if err := c.RegisterSegment(ctx, flowID, request); err != nil {
				return err
			}
		}
		if c.onRegisterSegments != nil {
			c.onRegisterSegments()
		}
		return failure
	}
	for _, request := range requests {
		if err := c.RegisterSegment(ctx, flowID, request); err != nil {
			return err
		}
	}
	if c.onRegisterSegments != nil {
		c.onRegisterSegments()
	}
	return failure
}

func (c *fakeClient) DeleteSegments(ctx context.Context, flowID string, options tams.SegmentDeleteOptions) error {
	c.lock.Lock()
	c.deletedTimeranges = append(c.deletedTimeranges, options.Timerange)
	c.deletedSegments = append(c.deletedSegments, options)
	deadline, _ := ctx.Deadline()
	c.deleteDeadlines = append(c.deleteDeadlines, deadline)
	block := c.blockDeleteUntilDone
	perSegmentErr := c.deleteSegmentsErrors[options.Timerange]
	if c.deleteSegmentsErr != nil {
		perSegmentErr = c.deleteSegmentsErr
	}
	c.lock.Unlock()
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	if perSegmentErr != nil {
		return perSegmentErr
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	for objectID, segment := range c.segments[flowID] {
		if segment.Timerange == options.Timerange && (options.ObjectID == "" || objectID == options.ObjectID) {
			delete(c.segments[flowID], objectID)
			delete(c.objects, objectID)
		}
	}
	return nil
}

// ListSegments records whether download URLs were asked for, so a test can
// assert that a listing which only decides what to resume does not make the
// service sign URLs it will discard.
func (c *fakeClient) ListSegments(ctx context.Context, flowID string, options tams.SegmentListOptions) ([]tams.Segment, error) {
	c.lock.Lock()
	c.listings = append(c.listings, options)
	c.listSegmentsCalls++
	listingErr := c.listSegmentsErr
	override := append([]tams.Segment(nil), c.listSegmentsOverride...)
	hasOverride := c.hasListingOverride
	c.lock.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if listingErr != nil {
		return nil, listingErr
	}
	if hasOverride {
		return override, nil
	}
	segments, err := c.Segments(ctx, flowID, options.ObjectID)
	if err != nil || options.IncludeDownloadURLs {
		return segments, err
	}
	// Mirror the service: without presigned URLs requested, none come back.
	lean := make([]tams.Segment, len(segments))
	for index, segment := range segments {
		segment.GetURLs = nil
		lean[index] = segment
	}
	return lean, nil
}

func (c *fakeClient) Segments(_ context.Context, flowID, objectID string) ([]tams.Segment, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	// An empty objectID lists every Segment of the Flow, matching the API where
	// object_id is an optional filter.
	if objectID == "" {
		listed := make([]tams.Segment, 0, len(c.segments[flowID]))
		for _, segment := range c.segments[flowID] {
			listed = append(listed, segment)
		}
		return listed, nil
	}
	segment, exists := c.segments[flowID][objectID]
	if !exists {
		return []tams.Segment{}, nil
	}
	return []tams.Segment{segment}, nil
}

func (c *fakeClient) UploadFile(_ context.Context, destination tams.PresignedURL, filename string) (tams.UploadReceipt, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return tams.UploadReceipt{}, err
	}
	digest := sha256.Sum256(data)
	receipt := tams.UploadReceipt{Bytes: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
	if c.storageChecksum {
		receipt.StorageSHA256 = receipt.SHA256
	}
	if c.storageChecksumValue != "" {
		receipt.StorageSHA256 = c.storageChecksumValue
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.corruptOnUpload && len(data) > 0 {
		// Stand in for bytes damaged in transit or at rest: the upload reports
		// success but what lands is not what was sent. The length is preserved
		// so this exercises the checksum rather than the cheaper size check.
		data = append([]byte(nil), data...)
		data[len(data)-1] ^= 0xff
	}
	c.objects[strings.TrimPrefix(destination.URL, "mem://")] = data
	c.uploads++
	return receipt, nil
}

type changedUploadClient struct{ *fakeClient }

func (c *changedUploadClient) UploadFile(ctx context.Context, destination tams.PresignedURL, filename string) (tams.UploadReceipt, error) {
	receipt, err := c.fakeClient.UploadFile(ctx, destination, filename)
	if err == nil {
		receipt.SHA256 = strings.Repeat("0", 64)
	}
	return receipt, err
}

func (c *fakeClient) DownloadDigest(_ context.Context, source tams.PresignedURL, _ int64) (int64, string, error) {
	c.record("verify")
	c.lock.Lock()
	hook := c.onDownload
	c.verified = append(c.verified, source.URL)
	c.lock.Unlock()
	if hook != nil {
		hook()
	}
	c.lock.Lock()
	data := append([]byte(nil), c.objects[strings.TrimPrefix(source.URL, "mem://")]...)
	c.lock.Unlock()
	digest := sha256.Sum256(data)
	return int64(len(data)), hex.EncodeToString(digest[:]), nil
}

func localSource(filename string) source.Item {
	info, err := os.Stat(filename)
	if err != nil {
		panic(err)
	}
	return source.Item{
		URI: "file://" + filepath.ToSlash(filename), Name: filepath.Base(filename), Size: info.Size(), LocalPath: filename,
		Open: func(context.Context) (io.ReadCloser, error) { return os.Open(filename) },
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// concurrentStartupClient reports whether Service and StorageBackends overlap.
// Each blocks until the other has been entered, so a pipeline that asks for
// them in sequence never gets past the first and the test fails on timeout
// rather than by inspecting how the calls were made.
type concurrentStartupClient struct {
	*fakeClient
	serviceEntered  chan struct{}
	backendsEntered chan struct{}
	timeout         time.Duration
}

func (c *concurrentStartupClient) Service(ctx context.Context) (map[string]any, error) {
	close(c.serviceEntered)
	select {
	case <-c.backendsEntered:
		return c.fakeClient.Service(ctx)
	case <-time.After(c.timeout):
		return nil, errors.New("storage backends were not requested while service information was in flight")
	}
}

func (c *concurrentStartupClient) StorageBackends(ctx context.Context) ([]tams.StorageBackend, error) {
	close(c.backendsEntered)
	select {
	case <-c.serviceEntered:
		return c.fakeClient.StorageBackends(ctx)
	case <-time.After(c.timeout):
		return nil, errors.New("service information was not requested while storage backends were in flight")
	}
}

// sizedSegmenter writes Segments of a chosen size, so a test can assert a bit
// rate rather than a placeholder. The counting segmenter writes one byte per
// Segment, which rounds every rate to zero.
type sizedSegmenter struct {
	objects int
	bytes   int
}

func (s sizedSegmenter) Segment(_ context.Context, request media.SegmentRequest, sink media.SegmentSink) error {
	for _, streamIndex := range request.StreamIndices {
		prefix := "segment-"
		if streamIndex != media.AllStreams {
			prefix = fmt.Sprintf("essence-%d-", streamIndex)
		}
		for index := range s.objects {
			path := filepath.Join(request.Directory, fmt.Sprintf("%s%08d.mp4", prefix, index))
			if err := os.WriteFile(path, bytes.Repeat([]byte{byte(index + 1)}, s.bytes), 0o600); err != nil {
				return err
			}
			record := media.SegmentRecord{StreamIndex: streamIndex, Path: path}
			if request.Duration > 0 {
				record.Timed = true
				record.Start = int64(index) * int64(request.Duration)
				record.End = record.Start + int64(request.Duration)
			}
			if err := sink(record); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sizedSegmenter) Version(context.Context) (string, error) {
	return "ffmpeg version 5.1 sized", nil
}

// essenceBitRateProber reports an essence bit rate that does not match what the
// Segments will imply, so a test can tell which of the two was written.
type essenceBitRateProber struct{}

func (essenceBitRateProber) Probe(context.Context, string) (media.Probe, error) {
	return media.Probe{
		// 12000 kbit/s of essence, against Segments that will work out at 8000.
		Format: media.Format{
			Name: "mov,mp4", Duration: "1.0", StartTime: "0.0", BitRate: "12000000",
			Tags: map[string]string{"major_brand": "isom"},
		},
		Streams: []media.Stream{{CodecName: "h264", CodecType: "video", Width: 64, Height: 64, AverageFrameRate: "25/1"}},
	}, nil
}

func (essenceBitRateProber) Version(context.Context) (string, error) {
	return "ffprobe version 5.1 fake", nil
}

type versionedCountingSegmenter struct {
	countingSegmenter
	version string
	err     error
}

func (s versionedCountingSegmenter) Version(context.Context) (string, error) {
	return s.version, s.err
}

// controlledLifetimeClock advances only when the test client completes a media
// transfer. It makes a long transfer deterministic without making the suite
// sleep for the specification's thirty-second minimum URL lifetime.
type controlledLifetimeClock struct {
	lock    sync.Mutex
	elapsed time.Duration
}

func (c *controlledLifetimeClock) now() time.Duration {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.elapsed
}

func (c *controlledLifetimeClock) advance(duration time.Duration) {
	c.lock.Lock()
	c.elapsed += duration
	c.lock.Unlock()
}

// expiringURLClient stamps every generated URL with the controlled issue time.
// Each transfer consumes one whole lifetime, so a URL generated for queued work
// is expired by definition when the preceding transfer completes.
type expiringURLClient struct {
	*fakeClient
	clock    *controlledLifetimeClock
	lifetime time.Duration

	lock         sync.Mutex
	uploadAges   []time.Duration
	downloadAges []time.Duration
}

func newExpiringURLClient(lifetime time.Duration) *expiringURLClient {
	client := &expiringURLClient{
		fakeClient: newFakeClient(), clock: &controlledLifetimeClock{}, lifetime: lifetime,
	}
	client.serviceDocument = map[string]any{
		"api_version":               "8.1",
		"min_object_timeout":        "300:0",
		"min_presigned_url_timeout": "30:0",
	}
	return client
}

const issuedAtMarker = "?tamsin-test-issued-at="

func (c *expiringURLClient) stampedURL(raw string) string {
	return raw + issuedAtMarker + strconv.FormatInt(int64(c.clock.now()), 10)
}

func (c *expiringURLClient) urlAge(raw string) (string, time.Duration, error) {
	base, issued, found := strings.Cut(raw, issuedAtMarker)
	if !found {
		return "", 0, fmt.Errorf("test URL %q has no issue time", raw)
	}
	issuedNanoseconds, err := strconv.ParseInt(issued, 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("parse test URL issue time: %w", err)
	}
	return base, c.clock.now() - time.Duration(issuedNanoseconds), nil
}

func (c *expiringURLClient) AllocateStorage(ctx context.Context, flowID string, request tams.StorageRequest) (tams.StorageResponse, error) {
	response, err := c.fakeClient.AllocateStorage(ctx, flowID, request)
	if err != nil {
		return response, err
	}
	for index := range response.MediaObjects {
		response.MediaObjects[index].PutURL.URL = c.stampedURL(response.MediaObjects[index].PutURL.URL)
	}
	return response, nil
}

func (c *expiringURLClient) UploadFile(ctx context.Context, destination tams.PresignedURL, filename string) (tams.UploadReceipt, error) {
	if destination.StartBefore.IsZero() {
		return tams.UploadReceipt{}, errors.New("upload URL has no scheduler start deadline")
	}
	base, age, err := c.urlAge(destination.URL)
	if err != nil {
		return tams.UploadReceipt{}, err
	}
	c.lock.Lock()
	c.uploadAges = append(c.uploadAges, age)
	c.lock.Unlock()
	if age >= c.lifetime {
		return tams.UploadReceipt{}, fmt.Errorf("upload URL expired %s ago", age-c.lifetime)
	}
	destination.URL = base
	receipt, err := c.fakeClient.UploadFile(ctx, destination, filename)
	if err != nil {
		return tams.UploadReceipt{}, err
	}
	c.clock.advance(c.lifetime)
	return receipt, nil
}

func (c *expiringURLClient) ListSegments(ctx context.Context, flowID string, options tams.SegmentListOptions) ([]tams.Segment, error) {
	segments, err := c.fakeClient.ListSegments(ctx, flowID, options)
	if err != nil || !options.IncludeDownloadURLs {
		return segments, err
	}
	for segmentIndex := range segments {
		for urlIndex := range segments[segmentIndex].GetURLs {
			segments[segmentIndex].GetURLs[urlIndex].Presigned = true
			segments[segmentIndex].GetURLs[urlIndex].URL = c.stampedURL(
				segments[segmentIndex].GetURLs[urlIndex].URL)
		}
	}
	return segments, nil
}

func (c *expiringURLClient) DownloadDigest(ctx context.Context, source tams.PresignedURL, expectedBytes int64) (int64, string, error) {
	if source.StartBefore.IsZero() {
		return 0, "", errors.New("download URL has no scheduler start deadline")
	}
	base, age, err := c.urlAge(source.URL)
	if err != nil {
		return 0, "", err
	}
	c.lock.Lock()
	c.downloadAges = append(c.downloadAges, age)
	c.lock.Unlock()
	if age >= c.lifetime {
		return 0, "", fmt.Errorf("download URL expired %s ago", age-c.lifetime)
	}
	source.URL = base
	size, digest, err := c.fakeClient.DownloadDigest(ctx, source, expectedBytes)
	if err != nil {
		return 0, "", err
	}
	c.clock.advance(c.lifetime)
	return size, digest, nil
}

func assertFreshURLAges(t *testing.T, kind string, ages []time.Duration, count int, lifetime time.Duration) {
	t.Helper()
	if len(ages) != count {
		t.Fatalf("%s transfers = %d, want %d", kind, len(ages), count)
	}
	for index, age := range ages {
		if age >= lifetime {
			t.Fatalf("%s %d began with URL age %s against lifetime %s", kind, index, age, lifetime)
		}
	}
}
