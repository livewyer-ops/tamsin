package ingest

import (
	"context"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/media"
)

// The first four fingerprints predate the container restriction; the ancillary
// cases predate the subsequent model cleanup. Both must preserve resume identity.
func TestSupportedContainerIdentitySurvivesPolicyRestriction(t *testing.T) {
	mp4, _ := (fakeProber{}).Probe(context.Background(), "")
	ts, _ := (muxedProber{}).Probe(context.Background(), "")
	audio := mp4
	audio.Streams = []media.Stream{{CodecType: "audio", CodecName: "aac", SampleRate: "48000", Channels: 2}}
	mp4Ancillary := mp4
	mp4Ancillary.Streams = []media.Stream{
		{Index: 0, ID: "0x1", CodecType: "video", CodecName: "h264", Width: 64, Height: 64, AverageFrameRate: "25/1"},
		{Index: 1, ID: "0x2", CodecType: "data", CodecTagString: "tmcd"},
		{Index: 2, ID: "0x3", CodecType: "audio", CodecName: "aac", SampleRate: "48000", Channels: 2},
	}
	tsAncillary := ts
	tsAncillary.Streams = []media.Stream{
		{Index: 0, ID: "0x100", CodecType: "video", CodecName: "h264", Width: 64, Height: 64, AverageFrameRate: "25/1"},
		{Index: 1, ID: "0x101", CodecType: "data", CodecName: "bin_data"},
		{Index: 2, ID: "0x102", CodecType: "data", CodecName: "scte_35"},
		{Index: 3, ID: "0x103", CodecType: "audio", CodecName: "aac", SampleRate: "48000", Channels: 2},
	}
	for _, test := range []struct {
		name    string
		probe   media.Probe
		storage media.EssenceStorage
		want    string
	}{
		{"mp4 video", mp4, media.EssenceStorageMuxed, "sha256:6ae966aed0a6ec012129c47d94fa9972c17335e7ae75a7fe82ba2a2bde872b13"},
		{"mp4 audio", audio, media.EssenceStorageMuxed, "sha256:c8957ccb2127628116ab056005679834ec89c26d4695e8d2dd263f6c5e09e0c6"},
		{"ts multiplex", ts, media.EssenceStorageMuxed, "sha256:a8a061563da121469c472ac3f5bdb0f7ef2da61bd8baec006f7b2ee3e32a95e7"},
		{"ts independent", ts, media.EssenceStorageIndependent, "sha256:4f6f95fa1531350e86cf636e167d43def2da3f874fa9ae2a8d4053b7b9a1ab4e"},
		{"mp4 timecode preserved", mp4Ancillary, media.EssenceStorageMuxed, "sha256:610055489a61be17597bfbe4e65c2a7ca60c45b471181e7c6e98f677606b8a4a"},
		{"ts ancillary preserved", tsAncillary, media.EssenceStorageMuxed, "sha256:33c0d451212a148b6ba658376a32b111f1321fa108d1faab40db722438dbb404"},
	} {
		t.Run(test.name, func(t *testing.T) {
			flow, info, err := media.BuildFlow(test.probe, media.Identity{}, "application/octet-stream", test.storage)
			if err != nil {
				t.Fatal(err)
			}
			got, err := mediaInterpretationFingerprint(flow, info)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Errorf("fingerprint = %q, want %q", got, test.want)
			}
		})
	}
}
