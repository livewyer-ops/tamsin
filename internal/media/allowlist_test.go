package media

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestInputOptionsPinProtocolAndFormats(t *testing.T) {
	t.Parallel()
	local := inputOptions("/tmp/tamsin-input-1/input.bin")
	bridge := inputOptions("http://127.0.0.1:43111/abc")
	if local[0] != "-protocol_whitelist" || local[1] != "file" || bridge[1] != "http,tcp" {
		t.Fatalf("protocol whitelists: local=%v bridge=%v", local, bridge)
	}
	if local[2] != "-format_whitelist" || local[3] != inputFormatAllowlist {
		t.Fatalf("format whitelist missing: %v", local)
	}
	for _, denied := range []string{"hls", "dash", "concat", "image2", "imf", "lavfi", "vobsub", "webm_dash_manifest", "rtsp", "sdp", "data"} {
		for _, name := range strings.Split(inputFormatAllowlist, ",") {
			if name == denied {
				t.Fatalf("resource-opening demuxer %q is on the allowlist", denied)
			}
		}
	}
	for _, required := range []string{"mov", "mp4", "matroska", "webm", "mpegts", "mxf", "wav", "flac", "mp3", "png_pipe", "jpeg_pipe"} {
		if !strings.Contains(","+inputFormatAllowlist+",", ","+required+",") {
			t.Fatalf("supported container %q is missing from the allowlist", required)
		}
	}
}

// A manifest names further resources. With the allowlists in place the tools
// must refuse it before any nested URL is fetched, whether the manifest is a
// staged file or arrives through the loopback bridge.
func TestProbeRefusesManifestsThatOpenOtherResources(t *testing.T) {
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("requires ffprobe")
	}
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.URL.Path)
		mu.Unlock()
		if r.URL.Path == "/manifest" {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = fmt.Fprint(w, manifest(server(r)))
			return
		}
		http.Error(w, "nested resource must never be requested", http.StatusForbidden)
	}))
	defer server.Close()
	staged := filepath.Join(t.TempDir(), "input.bin")
	if err := os.WriteFile(staged, []byte(manifest(server.URL)), 0o600); err != nil {
		t.Fatal(err)
	}
	prober := FFprobe{}
	for _, input := range []string{staged, server.URL + "/manifest"} {
		if _, err := prober.Probe(context.Background(), input); err == nil {
			t.Fatalf("manifest %q was accepted", input)
		}
		if err := prober.ProbePresentation(context.Background(), input, &Probe{Streams: []Stream{{CodecType: "video"}}}); err == nil {
			t.Fatalf("manifest %q was accepted by the presentation probe", input)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for _, path := range requests {
		if path != "/manifest" {
			t.Fatalf("a media tool fetched a nested resource: %v", requests)
		}
	}
}

func server(r *http.Request) string { return "http://" + r.Host }

func manifest(base string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" type="static" mediaPresentationDuration="PT10S" minBufferTime="PT1S" profiles="urn:mpeg:dash:profile:isoff-on-demand:2011">
  <BaseURL>` + base + `/nested/</BaseURL>
  <Period>
    <AdaptationSet mimeType="video/mp4">
      <Representation id="v" bandwidth="1000" codecs="avc1.42c01e">
        <BaseURL>segment.mp4</BaseURL>
        <SegmentBase indexRange="0-100"/>
      </Representation>
    </AdaptationSet>
  </Period>
</MPD>
`
}
