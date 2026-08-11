package media

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestFFmpegSegmentProducesObjects(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	// The suffix deliberately lies. The explicit source profile must decide
	// both muxer and output name; consulting this filename would write MP4.
	input := filepath.Join(directory, "input.mp4")
	if err := os.WriteFile(input, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	tool := filepath.Join(directory, "ffmpeg")
	script := `#!/bin/sh
input=""
previous=""
last=""
segment_format=""
segment_list=""
for argument in "$@"; do
  if [ "$previous" = "-i" ]; then input="$argument"; fi
  if [ "$previous" = "-segment_format" ]; then segment_format="$argument"; fi
  if [ "$previous" = "-segment_list" ]; then segment_list="$argument"; fi
  previous="$argument"
  last="$argument"
done
[ "$segment_format" = "mpegts" ] || exit 9
[ "$segment_list" = "pipe:3" ] || exit 10
output="$(printf "$last" 0)"
cp "$input" "$output"
printf '"%s",0.000000,1.000000\n' "$output" >&3
`
	if err := os.WriteFile(tool, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	outputDirectory := filepath.Join(directory, "segments")
	var segments []SegmentRecord
	err := (FFmpeg{Executable: tool}).Segment(context.Background(), SegmentRequest{
		Input: input, Duration: time.Second, Format: SegmentFormatSource,
		SourceContainer: SegmentContainer{Muxer: "mpegts", Extension: ".ts"},
		StreamIndices:   []int{AllStreams}, Directory: outputDirectory,
	}, func(record SegmentRecord) error {
		segments = append(segments, record)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 {
		t.Fatalf("segments = %v", segments)
	}
	if filepath.Ext(segments[0].Path) != ".ts" {
		t.Fatalf("segment path = %q, want probed .ts suffix rather than input .mp4", segments[0].Path)
	}
	if !segments[0].Timed || segments[0].Start != 0 || segments[0].End != int64(time.Second) {
		t.Fatalf("segment timing = %#v", segments[0])
	}
	data, err := os.ReadFile(segments[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "media" {
		t.Fatalf("segment data = %q", data)
	}
}

func TestFFmpegSegmentsSeveralEssencesInOneProcess(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	input := filepath.Join(directory, "input.ts")
	if err := os.WriteFile(input, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	tool := filepath.Join(directory, "ffmpeg")
	script := `#!/bin/sh
previous=""
manifest=""
stream=""
for argument in "$@"; do
  if [ "$previous" = "-map" ]; then stream="$argument"; fi
  if [ "$previous" = "-segment_list" ]; then manifest="$argument"; fi
  case "$argument" in
    *%08d.ts)
      output="$(printf "$argument" 0)"
      printf '%s' "$stream" >"$output"
      case "$manifest" in
        pipe:3) printf '"%s",0.000000,1.000000\n' "$output" >&3 ;;
        pipe:4) printf '"%s",0.000000,1.000000\n' "$output" >&4 ;;
        pipe:5) printf '"%s",0.000000,1.000000\n' "$output" >&5 ;;
        pipe:6) printf '"%s",0.000000,1.000000\n' "$output" >&6 ;;
        *) exit 12 ;;
      esac
      ;;
  esac
  previous="$argument"
done
`
	if err := os.WriteFile(tool, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	var records []SegmentRecord
	err := (FFmpeg{Executable: tool}).Segment(context.Background(), SegmentRequest{
		Input: input, Duration: time.Second, Format: SegmentFormatMPEGTS,
		StreamIndices: []int{0, 1, 2, 3}, Directory: filepath.Join(directory, "segments"),
	}, func(record SegmentRecord) error {
		records = append(records, record)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 {
		t.Fatalf("records = %#v, want one per essence", records)
	}
	seen := make(map[int]bool)
	for _, record := range records {
		seen[record.StreamIndex] = true
		data, err := os.ReadFile(record.Path)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "0:"+strconv.Itoa(record.StreamIndex) {
			t.Fatalf("stream %d output = %q", record.StreamIndex, data)
		}
	}
	for streamIndex := range 4 {
		if !seen[streamIndex] {
			t.Fatalf("stream %d was not emitted", streamIndex)
		}
	}
}

func TestInstalledFFmpegPublishesLiveSegmentManifest(t *testing.T) {
	executable, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg is not installed")
	}
	directory := t.TempDir()
	input := filepath.Join(directory, "input.ts")
	command := exec.Command(executable,
		"-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i", "testsrc=size=64x64:rate=25",
		"-t", "1", "-c:v", "libx264", input)
	if output, err := command.CombinedOutput(); err != nil {
		t.Skipf("installed ffmpeg cannot create the fixture: %v: %s", err, output)
	}
	var records []SegmentRecord
	sawPressureFlush := false
	err = (FFmpeg{Executable: executable}).Segment(context.Background(), SegmentRequest{
		Input: input, Duration: time.Second, Format: SegmentFormatMPEGTS,
		StreamIndices: []int{AllStreams}, Directory: filepath.Join(directory, "segments"),
		StagingWindow: &SegmentStagingWindow{HighBytes: 1, LowBytes: 0},
	}, func(record SegmentRecord) error {
		records = append(records, record)
		sawPressureFlush = sawPressureFlush || record.FlushStaging
		return os.Remove(record.Path)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) == 0 {
		t.Fatal("installed ffmpeg emitted no live manifest records")
	}
	if !sawPressureFlush {
		t.Fatal("closed output above the high watermark did not request a staging flush")
	}
}

func TestFFmpegVersionRetainsBuildEvidence(t *testing.T) {
	directory := t.TempDir()
	tool := filepath.Join(directory, "ffmpeg")
	script := "#!/bin/sh\nprintf '%s\\n' 'ffmpeg version 7.0' 'configuration: --enable-libexample' 'libavformat 61.0'\n"
	if err := os.WriteFile(tool, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	report, err := (FFmpeg{Executable: tool}).Version(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, evidence := range []string{"ffmpeg version 7.0", "configuration: --enable-libexample", "libavformat 61.0"} {
		if !strings.Contains(report, evidence) {
			t.Fatalf("version report %q does not retain %q", report, evidence)
		}
	}
}

func TestFFmpegSegmentRejectsUnsupportedSourceContainer(t *testing.T) {
	t.Parallel()
	err := (FFmpeg{Executable: "must-not-run"}).Segment(context.Background(), SegmentRequest{
		Input: "input.bin", Duration: time.Second, Format: SegmentFormatSource,
		SourceContainer: SegmentContainer{}, StreamIndices: []int{AllStreams}, Directory: t.TempDir(),
	}, func(SegmentRecord) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "outside Tamsin's segmentation profile") {
		t.Fatalf("Segment() error = %v, want unsupported source-container guidance", err)
	}
}
