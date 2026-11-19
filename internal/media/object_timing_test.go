package media

import (
	"strings"
	"testing"
)

func TestObjectPacketBounds(t *testing.T) {
	for _, tc := range []struct {
		name, packets           string
		wantStart, wantDuration int64
		invalid                 bool
	}{
		{"reordered video", `[{"stream_index":0,"pts":80,"duration":40},{"stream_index":0,"pts":0,"duration":40},{"stream_index":0,"pts":40,"duration":40}]`, 0, 120000000, false},
		{"negative PTS", `[{"stream_index":0,"pts":-80,"duration":40}]`, -80000000, 40000000, false},
		{"audio padding", `[{"stream_index":0,"pts":0,"duration":40,"side_data_list":[{"skip_samples":480,"discard_padding":480}]}]`, 10000000, 20000000, false},
		{"missing duration", `[{"stream_index":0,"pts":0}]`, 0, 0, true},
		{"missing PTS", `[{"stream_index":0,"duration":40}]`, 0, 0, true},
		{"empty", `[]`, 0, 0, true},
		{"invalid padding", `[{"stream_index":0,"pts":0,"duration":40,"side_data_list":[{"skip_samples":4800}]}]`, 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := Probe{Format: Format{StartTime: "999", Duration: "999"},
				Streams: []Stream{{Index: 0, CodecType: "audio", TimeBase: "1/1000", SampleRate: "48000"}}}
			err := measureObjectPackets(strings.NewReader(`{"packets":`+tc.packets+`}`), &probe)
			if tc.invalid {
				if err == nil {
					t.Fatal("invalid timing accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			start, duration, err := ProbeTiming(probe)
			if err != nil || start != tc.wantStart || duration != tc.wantDuration {
				t.Fatalf("bounds = %d, %d, %v", start, duration, err)
			}
		})
	}
}

func TestSparseTracksDoNotSupplyObjectBounds(t *testing.T) {
	for _, kind := range []string{"subtitle", "data", "audio", "video"} {
		t.Run(kind, func(t *testing.T) {
			probe := Probe{Format: Format{Name: "mov,mp4", Tags: map[string]string{"major_brand": "isom"}}, Streams: []Stream{
				{Index: 0, CodecType: "video", CodecName: "h264", TimeBase: "1/1000", Width: 64, Height: 64, AverageFrameRate: "25/1"},
				{Index: 1, CodecType: kind, CodecName: "mov_text", StartTime: "-99", Duration: "999", TimeBase: "1/1000"},
			}}
			err := measureObjectPackets(strings.NewReader(`{"packets":[{"stream_index":0,"pts":2000,"duration":40}]}`), &probe)
			if kind == "audio" || kind == "video" {
				if err == nil {
					t.Fatal("missing A/V timing accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			start, duration, err := ProbeTiming(probe)
			if err != nil || start != 2000000000 || duration != 40000000 || !probe.Streams[1].Empty {
				t.Fatalf("bounds: %d %d %v; sparse=%+v", start, duration, err, probe.Streams[1])
			}
			_, info, err := BuildFlow(probe, testIdentity(), "video/mp4", EssenceStorageMuxed)
			if err != nil || len(info.Collected) != 2 {
				t.Fatalf("sparse descriptor lost: %+v, %v", info, err)
			}
		})
	}
}

func TestMovTextClearingSampleHasNoPresentationDuration(t *testing.T) {
	for _, size := range []string{"2", "8"} {
		probe := Probe{Streams: []Stream{{Index: 0, CodecType: "subtitle", CodecName: "mov_text", TimeBase: "1/1000"}}}
		err := measureObjectPackets(strings.NewReader(`{"packets":[{"stream_index":0,"pts":2000,"size":"`+size+`"}]}`), &probe)
		if size == "2" {
			if err != nil || !probe.Streams[0].Empty {
				t.Fatalf("empty clearing sample: %v, %+v", err, probe.Streams[0])
			}
		} else if err == nil {
			t.Fatal("subtitle payload without duration accepted")
		}
	}
}
