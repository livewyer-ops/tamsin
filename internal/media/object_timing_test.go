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
