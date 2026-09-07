package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
)

func fixedFreeSpace(bytes int64) freeSpaceFunc {
	return func(string) (int64, error) { return bytes, nil }
}

func remoteSizedItem(size int64) source.Item {
	contents := []byte(nil)
	if size > 0 {
		contents = make([]byte, size)
	}
	return source.Item{
		URI: "https://media.example.test/asset.mxf", Name: "asset.mxf", Size: size,
		Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(contents)), nil
		},
	}
}

func TestStagingPreflightReportsRequiredAndAvailableCapacity(t *testing.T) {
	manager, err := newStagingManager(t.TempDir(), 100, fixedFreeSpace(50))
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = manager.reserve(context.Background(), remoteSizedItem(60), Config{
		SegmentDuration: 0, EssenceStorage: media.EssenceStorageMuxed,
	})
	var capacity *stagingCapacityError
	if !errors.As(err, &capacity) {
		t.Fatalf("error = %v, want stagingCapacityError", err)
	}
	if capacity.Required != 60 || capacity.Available != 50 {
		t.Fatalf("required/available = %d/%d, want 60/50", capacity.Required, capacity.Available)
	}
	for _, text := range []string{"60 B", "50 B", "--staging-byte-budget", "--concurrency"} {
		if !strings.Contains(err.Error(), text) {
			t.Fatalf("error %q does not contain actionable detail %q", err, text)
		}
	}
}

func TestAutomaticStagingBudgetLeavesFilesystemHeadroom(t *testing.T) {
	manager, err := newStagingManager(t.TempDir(), 0, fixedFreeSpace(1_000))
	if err != nil {
		t.Fatal(err)
	}
	if manager.limit != 800 {
		t.Fatalf("automatic budget = %d, want 80%% of 1000 bytes", manager.limit)
	}
}

func TestInspectStagingReportsBudgetAndRejectsUnavailableCapacity(t *testing.T) {
	directory := t.TempDir()
	inspection, err := inspectStaging(directory, 0, fixedFreeSpace(1_000))
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Directory != directory || inspection.FilesystemFreeBytes != 1_000 ||
		inspection.ConfiguredBudgetBytes != 0 || inspection.EffectiveBudgetBytes != 800 {
		t.Fatalf("inspection = %#v, want auto 800-byte budget from 1000 free", inspection)
	}

	inspection, err = inspectStaging(directory, 1_001, fixedFreeSpace(1_000))
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("inspection/error = %#v/%v, want unavailable configured budget", inspection, err)
	}
	if inspection.FilesystemFreeBytes != 1_000 || inspection.ConfiguredBudgetBytes != 1_001 {
		t.Fatalf("failed inspection omitted required/free detail: %#v", inspection)
	}

	if _, err := inspectStaging(directory, 0, fixedFreeSpace(0)); err == nil || !strings.Contains(err.Error(), "no available space") {
		t.Fatalf("zero-space inspection error = %v", err)
	}
}

func TestStagingEstimateCoversSourceAndGeneratedOutput(t *testing.T) {
	t.Parallel()
	const size = int64(100 << 20)
	outputAllowance := size * segmentAllowancePercent / 100
	for _, testCase := range []struct {
		name    string
		item    source.Item
		config  Config
		want    int64
		unknown bool
	}{
		{
			name: "local whole file", item: source.Item{LocalPath: "/asset", Size: size},
			config: Config{EssenceStorage: media.EssenceStorageMuxed}, want: 0,
		},
		{
			name: "local segmented output", item: source.Item{LocalPath: "/asset", Size: size},
			config: Config{EssenceStorage: media.EssenceStorageMuxed, SegmentDuration: time.Second}, want: outputAllowance,
		},
		{
			name: "remote whole file", item: source.Item{Size: size},
			config: Config{EssenceStorage: media.EssenceStorageMuxed}, want: size,
		},
		{
			name: "remote source plus segments", item: source.Item{Size: size},
			config: Config{EssenceStorage: media.EssenceStorageMuxed, SegmentDuration: time.Second}, want: size + outputAllowance,
		},
		{
			name: "streamed output only", item: source.Item{Size: size},
			config: Config{InputMode: InputStream, SegmentDuration: time.Second}, want: outputAllowance,
		},
		{
			name: "largest finite stream reserves bounded output", item: source.Item{Size: 1<<63 - 1},
			config: Config{InputMode: InputStream, SegmentDuration: time.Second}, want: rollingOutputWindowBytes,
		},
		{
			name: "streamed fast dry run creates no output", item: source.Item{Size: size},
			config: Config{InputMode: InputStream, DryRunMode: DryRunFast, SegmentDuration: time.Second}, want: 0,
		},
		{
			name: "unknown remote length", item: source.Item{Size: -1},
			config: Config{EssenceStorage: media.EssenceStorageMuxed}, unknown: true,
		},
		{
			name: "custom FFmpeg output", item: source.Item{LocalPath: "/asset", Size: size},
			config: Config{EssenceStorage: media.EssenceStorageMuxed, SegmentDuration: time.Second, FFmpegArgs: []string{"-c:v", "rawvideo"}}, want: outputAllowance,
		},
		{
			name: "fast dry run creates no local output", item: source.Item{LocalPath: "/asset", Size: size},
			config: Config{DryRunMode: DryRunFast, EssenceStorage: media.EssenceStorageMuxed, SegmentDuration: time.Second}, want: 0,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			got, unknown, err := estimatedStagingRequirement(testCase.item, testCase.config)
			if err != nil {
				t.Fatal(err)
			}
			if got != testCase.want || unknown != testCase.unknown {
				t.Fatalf("estimate = %d, unknown %v; want %d, %v", got, unknown, testCase.want, testCase.unknown)
			}
		})
	}
}

func TestRealSegmentedStagingEstimateUsesRollingOutputCap(t *testing.T) {
	t.Parallel()
	const sourceBytes = int64(10 << 30)
	for _, testCase := range []struct {
		name string
		item source.Item
		want int64
	}{
		{
			name: "local output window",
			item: source.Item{LocalPath: "/asset", Size: sourceBytes},
			want: rollingOutputWindowBytes,
		},
		{
			name: "remote source plus output window",
			item: source.Item{Size: sourceBytes},
			want: sourceBytes + rollingOutputWindowBytes,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			required, unknown, err := estimatedStagingRequirement(testCase.item, Config{
				DryRunMode: DryRunOff, SegmentDuration: time.Second,
				EssenceStorage: media.EssenceStorageMuxed, FFmpegArgs: []string{"-c:v", "libx264"},
			})
			if err != nil || unknown || required != testCase.want {
				t.Fatalf("rolling estimate = %d unknown=%v error=%v, want %d/false/nil",
					required, unknown, err, testCase.want)
			}
		})
	}
}

func TestStagingReservationsAreGlobalAndReleased(t *testing.T) {
	manager, err := newStagingManager(t.TempDir(), 100, fixedFreeSpace(100))
	if err != nil {
		t.Fatal(err)
	}
	config := Config{SegmentDuration: 0, EssenceStorage: media.EssenceStorageMuxed}
	first, _, _, err := manager.reserve(context.Background(), remoteSizedItem(60), config)
	if err != nil {
		t.Fatal(err)
	}
	type answer struct {
		lease *stagingLease
		err   error
	}
	secondResult := make(chan answer, 1)
	go func() {
		lease, _, _, reserveErr := manager.reserve(context.Background(), remoteSizedItem(60), config)
		secondResult <- answer{lease: lease, err: reserveErr}
	}()
	select {
	case result := <-secondResult:
		if result.lease != nil {
			result.lease.release()
		}
		t.Fatalf("second reservation did not wait: %v", result.err)
	case <-time.After(50 * time.Millisecond):
	}
	first.release()
	select {
	case result := <-secondResult:
		if result.err != nil {
			t.Fatal(result.err)
		}
		result.lease.release()
	case <-time.After(time.Second):
		t.Fatal("second reservation did not proceed after capacity was released")
	}
}

func TestUnknownLengthInputReservesTheWholeBudget(t *testing.T) {
	manager, err := newStagingManager(t.TempDir(), 100, fixedFreeSpace(100))
	if err != nil {
		t.Fatal(err)
	}
	item := remoteSizedItem(-1)
	lease, required, _, err := manager.reserve(context.Background(), item, Config{
		SegmentDuration: 0, EssenceStorage: media.EssenceStorageMuxed,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()
	if required != 100 || lease.reserved != 100 {
		t.Fatalf("unknown input reserved %d/%d, want the whole 100-byte budget", required, lease.reserved)
	}
}

func TestStagingStopsBeforeWritingPastItsLease(t *testing.T) {
	directory := t.TempDir()
	manager, err := newStagingManager(directory, 8, fixedFreeSpace(8))
	if err != nil {
		t.Fatal(err)
	}
	item := remoteSizedItem(8)
	item.Open = func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("nine-byte"))), nil
	}
	lease, _, _, err := manager.reserve(context.Background(), item, Config{
		SegmentDuration: 0, EssenceStorage: media.EssenceStorageMuxed,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()
	_, err = stage(context.Background(), item, directory, 0, lease, nil)
	var capacity *stagingCapacityError
	if !errors.As(err, &capacity) {
		t.Fatalf("stage error = %v, want capacity failure", err)
	}
	if capacity.Required != 9 || capacity.Available != 0 {
		t.Fatalf("required/available = %d/%d, want 9/0", capacity.Required, capacity.Available)
	}
}

type sparseCapacitySegmenter struct{ bytes int64 }

func (s sparseCapacitySegmenter) Segment(_ context.Context, request media.SegmentRequest, sink media.SegmentSink) error {
	path := filepath.Join(request.Directory, "segment.ts")
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := file.Truncate(s.bytes); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return sink(media.SegmentRecord{StreamIndex: request.StreamIndices[0], Path: path})
}

func (sparseCapacitySegmenter) Version(context.Context) (string, error) {
	return "ffmpeg version 5.1 capacity-test", nil
}

func TestSegmentOutputsCannotGrowPastTheGlobalBudget(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "input.ts")
	if err := os.WriteFile(input, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	item := source.Item{URI: "file://" + input, Name: "input.ts", Size: 1, LocalPath: input}
	config := Config{
		Concurrency: 1, Transfers: 1, ProbeConcurrency: 1,
		SegmentDuration: time.Second, EssenceStorage: media.EssenceStorageMuxed,
	}
	required, _, err := estimatedStagingRequirement(item, config)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := newStagingManager(directory, required+8, fixedFreeSpace(required+8))
	if err != nil {
		t.Fatal(err)
	}
	lease, _, _, err := manager.reserve(context.Background(), item, config)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()
	pipeline, err := New(config, newFakeClient(), fakeProber{}, sparseCapacitySegmenter{bytes: required + 16}, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	staged := stagedFile{path: input, size: 1, lease: lease, cleanup: func() {}}
	_, cleanup, err := pipeline.prepareObjects(context.Background(), "flow", staged,
		media.FlowInfo{Duration: int64(time.Second), SegmentContainer: media.SegmentContainer{Muxer: "mpegts", Extension: ".ts"}})
	cleanup()
	var capacity *stagingCapacityError
	if !errors.As(err, &capacity) {
		t.Fatalf("prepare error = %v, want capacity failure", err)
	}
}

func TestLocalWholeFileNeedsNoStagingCapacity(t *testing.T) {
	manager, err := newStagingManager(t.TempDir(), 0, fixedFreeSpace(0))
	if err != nil {
		t.Fatal(err)
	}
	lease, required, available, err := manager.reserve(context.Background(), source.Item{
		URI: "file:///asset.mxf", LocalPath: "/asset.mxf", Size: 1 << 40,
	}, Config{SegmentDuration: 0, EssenceStorage: media.EssenceStorageMuxed})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()
	if required != 0 || available != 0 {
		t.Fatalf("required/available = %d/%d, want 0/0", required, available)
	}
}

func TestDeferredLocalEssenceReservationStillUsesGlobalBudget(t *testing.T) {
	manager, err := newStagingManager(t.TempDir(), 10, fixedFreeSpace(10))
	if err != nil {
		t.Fatal(err)
	}
	lease, _, _, err := manager.reserve(context.Background(), source.Item{
		URI: "file:///asset.mxf", LocalPath: "/asset.mxf", Size: 8,
	}, Config{SegmentDuration: 0, EssenceStorage: media.EssenceStorageIndependent})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()
	if err := lease.reserveAdditional(context.Background(), 11); err == nil {
		t.Fatal("post-probe essence output bypassed the global staging budget")
	}
	if err := lease.reserveAdditional(context.Background(), 8); err != nil {
		t.Fatalf("valid post-probe reservation failed: %v", err)
	}
	if lease.reserved != 8 || manager.availableCapacity() != 2 {
		t.Fatalf("lease/global reservation = %d/%d, want 8 bytes held and 2 available", lease.reserved, manager.availableCapacity())
	}
}

type rollingBoundSegmenter struct {
	objects   int
	peakFiles int
	window    *media.SegmentStagingWindow
}

func (s *rollingBoundSegmenter) Segment(_ context.Context, request media.SegmentRequest, sink media.SegmentSink) error {
	s.window = request.StagingWindow
	for _, streamIndex := range request.StreamIndices {
		for index := range s.objects {
			path := filepath.Join(request.Directory, fmt.Sprintf("segment-%d-%08d.ts", streamIndex, index))
			if err := os.WriteFile(path, []byte{byte(index)}, 0o600); err != nil {
				return err
			}
			entries, err := os.ReadDir(request.Directory)
			if err != nil {
				return err
			}
			s.peakFiles = max(s.peakFiles, len(entries))
			if err := sink(media.SegmentRecord{
				StreamIndex: streamIndex, Path: path, Timed: true,
				Start: int64(index) * int64(request.Duration), End: int64(index+1) * int64(request.Duration),
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestRollingMultiOutputUsesOneGlobalPendingWindow(t *testing.T) {
	const objectsPerFlow = 20
	directory := t.TempDir()
	input := filepath.Join(directory, "input.ts")
	if err := os.WriteFile(input, []byte("four essences"), 0o600); err != nil {
		t.Fatal(err)
	}
	segmenter := &rollingBoundSegmenter{objects: objectsPerFlow}
	pipeline, err := New(Config{
		Concurrency: 1, Transfers: 8, ProbeConcurrency: 1,
		SegmentDuration: time.Second, EssenceStorage: media.EssenceStorageIndependent,
		TempDirectory: directory,
	}, newFakeClient(), fourEssenceProber{}, segmenter, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(input)})
	if err != nil || batch.Succeeded != 1 {
		t.Fatalf("multi-output rolling ingest = %#v, %v", batch, err)
	}
	total := 0
	for _, flow := range batch.Results[0].Flows {
		total += flow.ObjectSummary.Total
	}
	if total != 4*objectsPerFlow {
		t.Fatalf("multi-output summary total = %d, want %d", total, 4*objectsPerFlow)
	}
	if segmenter.peakFiles > rollingCommitObjects {
		t.Fatalf("multi-output staging peaked at %d files across Flows, global bound is %d",
			segmenter.peakFiles, rollingCommitObjects)
	}
}

func (*rollingBoundSegmenter) Version(context.Context) (string, error) {
	return "ffmpeg version 5.1 rolling-bound-test", nil
}

func TestRollingSegmentsKeepDiskAndTerminalMemoryBounded(t *testing.T) {
	const objects = 1_000
	directory := t.TempDir()
	input := filepath.Join(directory, "input.ts")
	if err := os.WriteFile(input, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	segmenter := &rollingBoundSegmenter{objects: objects}
	pipeline, err := New(Config{
		Concurrency: 1, Transfers: 8, ProbeConcurrency: 1,
		SegmentDuration: time.Second, EssenceStorage: media.EssenceStorageMuxed,
		TempDirectory: directory,
	}, newFakeClient(), fakeProber{}, segmenter, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := pipeline.Run(context.Background(), []source.Item{localSource(input)})
	if err != nil || batch.Succeeded != 1 {
		t.Fatalf("rolling ingest = %#v, %v", batch, err)
	}
	root := batch.Results[0].rootFlow()
	if root == nil || root.ObjectSummary.Total != objects || root.ObjectSummary.Ingested != objects {
		t.Fatalf("rolling summary = %#v, want %d ingested Objects", root, objects)
	}
	if len(root.Objects) != 0 {
		t.Fatalf("default rolling result retained %d per-Object records", len(root.Objects))
	}
	if segmenter.peakFiles > rollingCommitObjects {
		t.Fatalf("rolling staging peaked at %d completed files, bound is %d", segmenter.peakFiles, rollingCommitObjects)
	}
	if segmenter.window == nil || segmenter.window.HighBytes > rollingOutputWindowBytes ||
		segmenter.window.LowBytes >= segmenter.window.HighBytes {
		t.Fatalf("renderer staging window = %#v", segmenter.window)
	}
}
