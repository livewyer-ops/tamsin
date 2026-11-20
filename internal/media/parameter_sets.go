package media

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ParameterSets is what the pictures of one H.264 stream of an MPEG-TS Object
// find of the sequence and picture parameter sets they refer to.
type ParameterSets struct {
	// Missing reports a picture that refers to a set the Object has not
	// carried by then, so that picture cannot be decoded from the Object
	// alone.
	Missing bool
	// Changed reports a set the Object carries with two contents: the stream
	// replaces it part of the way through.
	Changed bool
}

// ProbeParameterSets reads the parameter sets of one H.264 stream of an
// MPEG-TS Object, named by its place among the Object's video streams, and
// the set each of its slices refers to. The stream is copied out and read as
// it arrives, so nothing is decoded.
func (f FFmpeg) ProbeParameterSets(ctx context.Context, filename string, video int) (ParameterSets, error) {
	executable := f.Executable
	if executable == "" {
		executable = "ffmpeg"
	}
	arguments := append([]string{"-hide_banner", "-loglevel", "error", "-nostdin"}, inputOptions(filename)...)
	arguments = append(arguments, "-i", filename, "-map", "0:v:"+strconv.Itoa(video), "-c", "copy", "-copyinkf", "-f", "h264", "-")
	var sets ParameterSets
	step, stderr, err := streamTool(ctx, executable, arguments, func(stdout io.Reader) (scanErr error) {
		sets, scanErr = readParameterSets(stdout)
		return scanErr
	})
	if step == toolStepWait && err != nil {
		err = fmt.Errorf("%w: %s", err, strings.TrimSpace(string(stderr)))
	}
	if err != nil {
		return ParameterSets{}, fmt.Errorf("read H.264 parameter sets of %q: %w", filename, err)
	}
	return sets, nil
}

// readParameterSets reads an Annex B stream in decoding order. A slice names
// a picture parameter set, which names a sequence parameter set, and both
// must have arrived before it. The slices read are those of the base layer,
// whole or partitioned, and of auxiliary pictures. Slices of the scalable,
// multiview and 3D extensions refer to subset sequence parameter sets and
// are not read.
func readParameterSets(reader io.Reader) (ParameterSets, error) {
	var sets ParameterSets
	contents := make(map[string]string)
	sequences := make(map[int]bool)
	pictures := make(map[int]int)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 2*unitHead)
	scanner.Split(splitUnits())
	for scanner.Scan() {
		unit := scanner.Bytes()
		if len(unit) == 0 {
			continue
		}
		kind, bit := "pps ", 8
		switch unit[0] & 0x1f {
		case 1, 2, 5, 19:
			// A slice header begins with its first macroblock and its type.
			// Partition A of a partitioned slice carries the header.
			_, first := readGolomb(unit, &bit)
			_, second := readGolomb(unit, &bit)
			picture, ok := readGolomb(unit, &bit)
			if !first || !second || !ok {
				return ParameterSets{}, errors.New("unreadable H.264 slice")
			}
			if sequence, carried := pictures[picture]; !carried || !sequences[sequence] {
				sets.Missing = true
			}
			continue
		case 7:
			// A sequence set's identifier follows its profile, constraint and level bytes.
			kind, bit = "sps ", 32
		case 8:
		default:
			continue
		}
		id, ok := readGolomb(unit, &bit)
		sequence := id
		if kind == "pps " && ok {
			sequence, ok = readGolomb(unit, &bit)
		}
		// H.264 numbers sequence sets up to 31 and picture sets up to 255.
		if !ok || len(unit) >= unitHead || sequence > 31 || id > 255 {
			return ParameterSets{}, errors.New("unreadable H.264 parameter set")
		}
		if kind == "sps " {
			sequences[id] = true
		} else {
			pictures[id] = sequence
		}
		// The first byte holds the unit's type and reference priority, which
		// may differ between copies of one set.
		key, content := kind+strconv.Itoa(id), string(unit[1:])
		if known, seen := contents[key]; seen && known != content {
			sets.Changed = true
		}
		contents[key] = content
	}
	return sets, scanner.Err()
}

// unitHead bounds how much of one unit is read: far more than a parameter set
// holds, and a slice is wanted only for the start of its header.
const unitHead = 1 << 16

// splitUnits returns a bufio.SplitFunc that yields the units of an Annex B
// stream without their start codes, each cut to its first unitHead bytes.
func splitUnits() bufio.SplitFunc {
	skipping := false
	return func(data []byte, atEOF bool) (int, []byte, error) {
		end := bytes.Index(data, []byte{0, 0, 1})
		advance, whole := end+3, true
		switch {
		case end >= 0:
		case atEOF:
			end, advance = len(data), len(data)
		case len(data) < unitHead+2:
			return 0, nil, nil
		default:
			// The last two bytes may begin the next start code.
			end, advance, whole = len(data)-2, len(data)-2, false
		}
		continued := skipping
		skipping = !whole
		if continued || end == 0 {
			return advance, nil, nil
		}
		return advance, bytes.TrimRight(data[:min(end, unitHead)], "\x00"), nil
	}
}

// readGolomb reads the unsigned Exp-Golomb value at a bit of a unit and moves
// the bit past it. The values read here come before any byte sequence that
// emulation prevention would alter.
func readGolomb(unit []byte, bit *int) (int, bool) {
	next := func() (int, bool) {
		if *bit >= len(unit)*8 {
			return 0, false
		}
		value := int(unit[*bit/8] >> (7 - *bit%8) & 1)
		*bit++
		return value, true
	}
	zeros := 0
	for {
		value, ok := next()
		if !ok || zeros > 20 {
			return 0, false
		}
		if value == 1 {
			break
		}
		zeros++
	}
	value := 1
	for range zeros {
		low, ok := next()
		if !ok {
			return 0, false
		}
		value = value<<1 | low
	}
	return value - 1, true
}
