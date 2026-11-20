package media

import (
	"bytes"
	"testing"
)

func TestReadParameterSets(t *testing.T) {
	t.Parallel()
	var (
		delimiter = []byte{0, 0, 0, 1, 0x09, 0xf0}
		sequence  = []byte{0, 0, 0, 1, 0x67, 0x64, 0x00, 0x0a, 0xac, 0xd9}
		picture   = []byte{0, 0, 0, 1, 0x68, 0xef, 0x8f, 0xcb}
		// The same identifier with other content, as a second encode writes it.
		replaced = []byte{0, 0, 1, 0x68, 0xcf, 0x8f, 0xcb}
		// Identifier 1 is the Exp-Golomb code 010.
		second = []byte{0, 0, 1, 0x68, 0x5f, 0x8f, 0xcb}
		// The same set with reference priority 2 in place of 3.
		demoted = []byte{0, 0, 1, 0x48, 0xef, 0x8f, 0xcb}
		// Picture set 0 of sequence set 1.
		foreign = []byte{0, 0, 1, 0x68, 0xaf, 0x8f, 0xcb}
		// An I slice of picture set 0, and a P slice of picture set 1.
		slice = []byte{0, 0, 1, 0x65, 0x88, 0x84}
		other = []byte{0, 0, 1, 0x41, 0xd0, 0x84}
		// Partition A of a slice of picture set 1.
		partition = []byte{0, 0, 1, 0x42, 0xd0, 0x84}
		// Sequence set 32 is the Exp-Golomb code 00000100001.
		unnumbered = []byte{0, 0, 1, 0x67, 0x64, 0x00, 0x0a, 0x04, 0x3f}
	)
	join := func(units ...[]byte) []byte { return bytes.Join(units, nil) }
	// A slice longer than the reader keeps of one unit.
	long := join(slice, bytes.Repeat([]byte{0xff}, 5*unitHead/2))
	for name, tc := range map[string]struct {
		stream  []byte
		missing bool
		changed bool
		invalid bool
	}{
		"sets before every picture":             {stream: join(delimiter, sequence, picture, slice, delimiter, slice)},
		"a picture before its sets":             {stream: join(delimiter, slice, delimiter, sequence, picture, slice), missing: true},
		"no picture set":                        {stream: join(delimiter, sequence, slice), missing: true},
		"no sequence set":                       {stream: join(delimiter, picture, slice), missing: true},
		"a picture set of another sequence set": {stream: join(delimiter, sequence, foreign, slice), missing: true},
		"another picture set than the slice's":  {stream: join(delimiter, sequence, picture, other, delimiter, sequence, second, other), missing: true},
		"a second picture set before its slice": {stream: join(delimiter, sequence, picture, slice, delimiter, second, other)},
		"a set replaced":                        {stream: join(delimiter, sequence, picture, slice, delimiter, replaced, slice), changed: true},
		"a set repeated":                        {stream: join(delimiter, sequence, picture, slice, delimiter, sequence, picture, slice)},
		"a set repeated with another priority":  {stream: join(delimiter, sequence, picture, slice, delimiter, demoted, slice)},
		"a partitioned slice without its set":   {stream: join(delimiter, sequence, picture, slice, delimiter, partition), missing: true},
		"a set numbered past the last":          {stream: join(delimiter, unnumbered, picture, slice), invalid: true},
		"sets after a long slice":               {stream: join(sequence, picture, long, replaced, long, second, other), changed: true},
		"no pictures":                           {stream: join(delimiter, delimiter)},
		"a set cut short":                       {stream: join(delimiter, []byte{0, 0, 1, 0x67, 0x64}), invalid: true},
		"a slice cut short":                     {stream: join(delimiter, sequence, picture, []byte{0, 0, 1, 0x65}), invalid: true},
		"a set too long to keep":                {stream: join(sequence, bytes.Repeat([]byte{0xff}, 2*unitHead), picture, slice), invalid: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sets, err := readParameterSets(bytes.NewReader(tc.stream))
			if tc.invalid {
				if err == nil {
					t.Fatalf("read %+v, want an error", sets)
				}
				return
			}
			if err != nil || sets.Missing != tc.missing || sets.Changed != tc.changed {
				t.Fatalf("read %+v (%v), want missing %t and changed %t", sets, err, tc.missing, tc.changed)
			}
		})
	}
}
