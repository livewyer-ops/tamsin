package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

// Ingest is latency-bound rather than bandwidth-bound: the cost is dominated by
// how many sequential round trips each Media Object costs, not by how fast the
// bytes move. Measuring against a real store makes that invisible, because the
// number moves with whatever link the machine happens to have.
//
// countingClient therefore wraps a fake store, counts every call, and can
// inject a fixed per-call delay standing in for network latency. That gives two
// things a live benchmark cannot: an exact round-trip count, and a throughput
// figure that is reproducible on any machine.
type countingClient struct {
	inner   *fakeClient
	latency time.Duration

	calls sync.Map // method name -> *atomic.Int64
	total atomic.Int64
	// peak records the greatest number of calls in flight at once, which is how
	// a change to concurrency shows up rather than being inferred from timing.
	inFlight atomic.Int64
	peak     atomic.Int64
	// Transfers are tracked separately: the transfer budget bounds Media Object
	// uploads and verification downloads, not the metadata calls around them.
	transfersInFlight atomic.Int64
	peakTransfers     atomic.Int64
}

// isTransfer reports whether a call moves Media Object bytes, which is what the
// transfer budget governs.
func isTransfer(method string) bool {
	return method == "UploadFile" || method == "DownloadDigest"
}

func newCountingClient(latency time.Duration) *countingClient {
	return &countingClient{inner: newFakeClient(), latency: latency}
}

func (c *countingClient) record(method string) func() {
	counter, _ := c.calls.LoadOrStore(method, &atomic.Int64{})
	counter.(*atomic.Int64).Add(1)
	c.total.Add(1)

	current := c.inFlight.Add(1)
	recordPeak(&c.peak, current)
	transfer := isTransfer(method)
	if transfer {
		recordPeak(&c.peakTransfers, c.transfersInFlight.Add(1))
	}
	if c.latency > 0 {
		time.Sleep(c.latency)
	}
	return func() {
		c.inFlight.Add(-1)
		if transfer {
			c.transfersInFlight.Add(-1)
		}
	}
}

func recordPeak(peak *atomic.Int64, current int64) {
	for {
		observed := peak.Load()
		if current <= observed || peak.CompareAndSwap(observed, current) {
			return
		}
	}
}

func (c *countingClient) count(method string) int64 {
	if counter, ok := c.calls.Load(method); ok {
		return counter.(*atomic.Int64).Load()
	}
	return 0
}

func (c *countingClient) Service(ctx context.Context) (map[string]any, error) {
	defer c.record("Service")()
	return c.inner.Service(ctx)
}

func (c *countingClient) StorageBackends(ctx context.Context) ([]tams.StorageBackend, error) {
	defer c.record("StorageBackends")()
	return c.inner.StorageBackends(ctx)
}

func (c *countingClient) Profile(ctx context.Context, id string) (tams.Profile, error) {
	defer c.record("Profile")()
	return c.inner.Profile(ctx, id)
}

func (c *countingClient) Flow(ctx context.Context, id string) (tams.Flow, error) {
	defer c.record("Flow")()
	return c.inner.Flow(ctx, id)
}

func (c *countingClient) PutFlow(ctx context.Context, id string, flow tams.Flow) (tams.Flow, error) {
	defer c.record("PutFlow")()
	return c.inner.PutFlow(ctx, id, flow)
}

func (c *countingClient) AllocateStorage(ctx context.Context, flowID string, request tams.StorageRequest) (tams.StorageResponse, error) {
	defer c.record("AllocateStorage")()
	return c.inner.AllocateStorage(ctx, flowID, request)
}

func (c *countingClient) RegisterSegment(ctx context.Context, flowID string, request tams.SegmentRequest) error {
	defer c.record("RegisterSegment")()
	return c.inner.RegisterSegment(ctx, flowID, request)
}

func (c *countingClient) RegisterSegments(ctx context.Context, flowID string, requests []tams.SegmentRequest) error {
	defer c.record("RegisterSegments")()
	return c.inner.RegisterSegments(ctx, flowID, requests)
}

func (c *countingClient) DeleteSegments(ctx context.Context, flowID string, options tams.SegmentDeleteOptions) error {
	defer c.record("DeleteSegments")()
	return c.inner.DeleteSegments(ctx, flowID, options)
}

func (c *countingClient) ListSegments(ctx context.Context, flowID string, options tams.SegmentListOptions) ([]tams.Segment, error) {
	defer c.record("Segments")()
	return c.inner.ListSegments(ctx, flowID, options)
}

func (c *countingClient) UploadFile(ctx context.Context, destination tams.PresignedURL, filename string) (tams.UploadReceipt, error) {
	defer c.record("UploadFile")()
	return c.inner.UploadFile(ctx, destination, filename)
}

func (c *countingClient) DownloadDigest(ctx context.Context, source tams.PresignedURL, expectedBytes int64) (int64, string, error) {
	defer c.record("DownloadDigest")()
	return c.inner.DownloadDigest(ctx, source, expectedBytes)
}

// countingSegmenter produces a requested number of Media Objects so the harness
// can vary object count independently of media size.
type countingSegmenter struct{ objects int }

func (s countingSegmenter) Segment(_ context.Context, request media.SegmentRequest, sink media.SegmentSink) error {
	for _, streamIndex := range request.StreamIndices {
		prefix := "segment-"
		if streamIndex != media.AllStreams {
			prefix = fmt.Sprintf("essence-%d-", streamIndex)
		}
		for index := range s.objects {
			path := filepath.Join(request.Directory, fmt.Sprintf("%s%08d.mp4", prefix, index))
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

func (countingSegmenter) Version(context.Context) (string, error) {
	return "ffmpeg version 5.1 bench", nil
}

type boundedRollingSegmenter struct {
	inFlight atomic.Int64
	peak     atomic.Int64
}

func (s *boundedRollingSegmenter) Segment(ctx context.Context, request media.SegmentRequest, sink media.SegmentSink) error {
	recordPeak(&s.peak, s.inFlight.Add(1))
	defer s.inFlight.Add(-1)
	return (countingSegmenter{objects: 1}).Segment(ctx, request, sink)
}

func (*boundedRollingSegmenter) Version(context.Context) (string, error) {
	return "ffmpeg version 5.1 bounded-rolling-test", nil
}

func benchFixture(tb testing.TB) source.Item {
	tb.Helper()
	filename := filepath.Join(tb.TempDir(), "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		tb.Fatal(err)
	}
	return localSource(filename)
}

// TestRoundTripsPerObject pins the number of sequential store interactions each
// Media Object costs. This is the metric worth defending: it is what makes a
// high-latency ingest slow, and unlike throughput it does not vary with the
// machine running the test.
//
// Update the expectation deliberately when reducing round trips, so a
// regression has to be argued for rather than slipping through.
func TestRoundTripsPerObject(t *testing.T) {
	t.Parallel()
	const objects = 8

	client := newCountingClient(0)
	pipeline, err := New(Config{
		Concurrency: 1, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}, client, fakeProber{}, countingSegmenter{objects: objects}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{benchFixture(t)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Succeeded != 1 || batch.Results[0].rootFlow().ObjectSummary.Total != objects {
		t.Fatalf("unexpected batch: %#v", batch)
	}

	perObject := map[string]int64{
		"Segments":         client.count("Segments"),
		"AllocateStorage":  client.count("AllocateStorage"),
		"UploadFile":       client.count("UploadFile"),
		"RegisterSegment":  client.count("RegisterSegment"),
		"RegisterSegments": client.count("RegisterSegments"),
		"Object":           client.count("Object"),
		"DownloadDigest":   client.count("DownloadDigest"),
	}
	var total int64
	for _, count := range perObject {
		total += count
	}

	// Per Media Object with one transfer worker: allocate its fresh PUT URL,
	// upload, register before another allocation starts, fetch an exact fresh
	// GET URL after the verification worker is ready, then download. The one
	// additional lean listing that decides resume identity is per Flow and falls
	// out of integer per-Object accounting here. The metadata cost is deliberate:
	// issuing URLs for queued Objects would violate their advertised lifetime.
	const want = 5
	if got := total / objects; got != want {
		t.Fatalf("round trips per object = %d, want %d\nbreakdown for %d objects: %v",
			got, want, objects, perObject)
	}
}

// TestTransfersRunConcurrently proves the transfer budget is actually used.
// Before this existed, peak in-flight calls stayed at 1 no matter what
// concurrency was set to, because a single input was served by a single worker
// and its Media Objects were uploaded one at a time.
func TestTransfersRunConcurrently(t *testing.T) {
	t.Parallel()
	const objects = 16

	for _, transfers := range []int{1, 4} {
		t.Run(fmt.Sprintf("transfers=%d", transfers), func(t *testing.T) {
			t.Parallel()
			// A per-call delay makes overlap observable: without it a transfer can
			// finish before the next one starts and peak stays at 1 legitimately.
			client := newCountingClient(2 * time.Millisecond)
			pipeline, err := New(Config{
				Concurrency: 1, Transfers: transfers, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
				EssenceStorage: media.EssenceStorageMuxed,
			}, client, fakeProber{}, countingSegmenter{objects: objects}, discardLogger(), nil)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := pipeline.Run(context.Background(), []source.Item{benchFixture(t)})
			if err != nil {
				t.Fatal(err)
			}
			if batch.Succeeded != 1 {
				t.Fatalf("ingest should succeed: %#v", batch)
			}
			peak := client.peakTransfers.Load()
			if peak > int64(transfers) {
				t.Fatalf("peak in-flight transfers = %d, which exceeds the budget of %d", peak, transfers)
			}
			if transfers > 1 && peak < 2 {
				t.Fatalf("peak in-flight transfers = %d with a budget of %d: transfers are still serial", peak, transfers)
			}
		})
	}
}

// TestTransferBudgetIsGlobal keeps the bound from multiplying by input count.
// A per-input budget would let eight inputs run eight transfers each.
func TestTransferBudgetIsGlobal(t *testing.T) {
	t.Parallel()
	client := newCountingClient(2 * time.Millisecond)
	pipeline, err := New(Config{
		Concurrency: 4, Transfers: 2, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
		EssenceStorage: media.EssenceStorageMuxed,
	}, client, fakeProber{}, countingSegmenter{objects: 8}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	items := []source.Item{benchFixture(t), benchFixture(t), benchFixture(t), benchFixture(t)}
	if _, err := pipeline.Run(context.Background(), items); err != nil {
		t.Fatal(err)
	}
	if peak := client.peakTransfers.Load(); peak > 2 {
		t.Fatalf("peak in-flight transfers = %d across 4 concurrent inputs, want at most the global budget of 2", peak)
	}
}

// BenchmarkIngestByLatency reports throughput at round-trip times spanning a
// same-datacentre store through to a cross-region one. The point is not the
// absolute figures but how steeply cost scales with latency, which is what
// tells you whether round trips or bytes are the problem.
func BenchmarkIngestByLatency(b *testing.B) {
	for _, latency := range []time.Duration{0, time.Millisecond, 10 * time.Millisecond} {
		for _, concurrency := range []int{1, 8} {
			name := fmt.Sprintf("rtt=%s/transfers=%d", latency, concurrency)
			b.Run(name, func(b *testing.B) {
				item := benchFixture(b)
				b.ReportAllocs()
				for b.Loop() {
					client := newCountingClient(latency)
					pipeline, err := New(Config{
						Concurrency: 1, Transfers: concurrency, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
						EssenceStorage: media.EssenceStorageMuxed,
					}, client, fakeProber{}, countingSegmenter{objects: 16}, discardLogger(), nil)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := pipeline.Run(context.Background(), []source.Item{item}); err != nil {
						b.Fatal(err)
					}
					b.ReportMetric(float64(client.total.Load()), "calls/op")
					b.ReportMetric(float64(client.peakTransfers.Load()), "peak-transfers")
				}
			})
		}
	}
}

// BenchmarkIngestByObjectCount shows how cost scales with the number of Media
// Objects, which is what a shorter segment duration produces.
func BenchmarkIngestByObjectCount(b *testing.B) {
	for _, objects := range []int{1, 16, 64} {
		b.Run(fmt.Sprintf("objects=%d", objects), func(b *testing.B) {
			item := benchFixture(b)
			b.ReportAllocs()
			for b.Loop() {
				client := newCountingClient(0)
				pipeline, err := New(Config{
					Concurrency: 8, VerificationMode: VerificationReadback, SegmentDuration: time.Second,
					EssenceStorage: media.EssenceStorageMuxed,
				}, client, fakeProber{}, countingSegmenter{objects: objects}, discardLogger(), nil)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := pipeline.Run(context.Background(), []source.Item{item}); err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(client.total.Load())/float64(objects), "calls/object")
			}
		})
	}
}

// countingProber records how many media measurements run at once. Measurement
// spawns ffprobe and reads a file, so oversubscription costs processes, file
// descriptors and disk bandwidth rather than just goroutines.
type countingProber struct {
	inFlight atomic.Int64
	peak     atomic.Int64
	calls    atomic.Int64
	delay    time.Duration
}

func (p *countingProber) Probe(_ context.Context, _ string) (media.Probe, error) {
	p.calls.Add(1)
	recordPeak(&p.peak, p.inFlight.Add(1))
	defer p.inFlight.Add(-1)
	if p.delay > 0 {
		time.Sleep(p.delay)
	}
	probe := media.Probe{Format: media.Format{
		Name: "mov,mp4", Duration: "1.0", StartTime: "0.0",
		Tags: map[string]string{"major_brand": "isom"},
	}}
	probe.Streams = []media.Stream{{CodecName: "h264", CodecType: "video", Width: 64, Height: 64, AverageFrameRate: "25/1"}}
	return probe, nil
}

func (p *countingProber) Version(context.Context) (string, error) {
	return "ffprobe version 5.1 test", nil
}

// TestMediaProcessBudgetIsGlobalAcrossInputs covers a bound that multiplied
// instead of holding. Media measurement was limited per Flow, while Run
// prepares Concurrency Flows at once, so the effective ceiling was the product
// of the two: on a sixteen-thread machine four concurrent inputs could run
// sixty-four ffprobe processes.
func TestMediaProcessBudgetIsGlobalAcrossInputs(t *testing.T) {
	t.Parallel()
	const mediaProcessBudget = 2
	prober := &countingProber{delay: 2 * time.Millisecond}
	client := newFakeClient()
	pipeline, err := New(Config{
		Concurrency: 4, Transfers: 4, ProbeConcurrency: 8, VerificationMode: VerificationNone,
		SegmentDuration: time.Second, EssenceStorage: media.EssenceStorageMuxed,
	}, client, prober, countingSegmenter{objects: 8}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	items := []source.Item{benchFixture(t), benchFixture(t), benchFixture(t), benchFixture(t)}
	if _, err := pipeline.Run(context.Background(), items); err != nil {
		t.Fatal(err)
	}
	if peak := prober.peak.Load(); peak > mediaProcessBudget {
		t.Fatalf("peak concurrent media processes = %d across 4 inputs, want at most the global budget of %d",
			peak, mediaProcessBudget)
	}
}

// TestRollingMediaProcessBudgetCannotDeadlockAcrossInputs covers the lock
// cycle where two FFmpeg segmenters occupied the complete two-process budget,
// then each sink waited for FFprobe to measure its first Segment. Rolling
// renders must serialize at this budget so their nested probe always has a
// process slot available.
func TestRollingMediaProcessBudgetCannotDeadlockAcrossInputs(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	items := make([]source.Item, 2)
	for index := range items {
		filename := filepath.Join(directory, fmt.Sprintf("fixture-%d.mp4", index))
		if err := os.WriteFile(filename, []byte(fmt.Sprintf("media-%d", index)), 0o600); err != nil {
			t.Fatal(err)
		}
		items[index] = localSource(filename)
	}
	segmenter := &boundedRollingSegmenter{}
	pipeline, err := New(Config{
		Concurrency: 2, Transfers: 2, ProbeConcurrency: 2, VerificationMode: VerificationNone,
		SegmentDuration: time.Second, EssenceStorage: media.EssenceStorageMuxed,
	}, newFakeClient(), &countingProber{}, segmenter, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	batch, err := pipeline.Run(ctx, items)
	if err != nil {
		t.Fatalf("two rolling inputs did not complete within the bounded process budget: %v", err)
	}
	if batch.Succeeded != len(items) {
		t.Fatalf("succeeded = %d, want %d: %#v", batch.Succeeded, len(items), batch)
	}
	if peak := segmenter.peak.Load(); peak != 1 {
		t.Fatalf("peak concurrent rolling FFmpeg processes = %d, want 1 so nested FFprobe retains a slot", peak)
	}
}

func TestSegmentManifestUsesOneAnchorProbePerOutput(t *testing.T) {
	t.Parallel()
	prober := &countingProber{}
	pipeline, err := New(Config{
		Concurrency: 1, Transfers: 1, ProbeConcurrency: 8, VerificationMode: VerificationNone,
		SegmentDuration: time.Second, EssenceStorage: media.EssenceStorageMuxed,
	}, newFakeClient(), prober, countingSegmenter{objects: 8}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipeline.Run(context.Background(), []source.Item{benchFixture(t)}); err != nil {
		t.Fatal(err)
	}
	if calls := prober.calls.Load(); calls != 2 {
		t.Fatalf("ffprobe calls = %d, want one source probe and one generated-output anchor", calls)
	}
}

func TestCustomMediaProcessUsesTheCompleteLocalBudget(t *testing.T) {
	t.Parallel()
	pipeline, err := New(Config{Concurrency: 1, DryRunMode: DryRunExact}, nil, &countingProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	releaseCustom, err := pipeline.acquireMediaProcess(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if pipeline.mediaProcesses.TryAcquire(1) {
		pipeline.mediaProcesses.Release(1)
		t.Fatal("ordinary media process overlapped an exclusive custom process")
	}
	releaseCustom()
	if !pipeline.mediaProcesses.TryAcquire(1) {
		t.Fatal("ordinary media process did not start after custom process released the budget")
	}
	pipeline.mediaProcesses.Release(1)
}
