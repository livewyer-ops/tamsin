package media

import (
	"math/big"
	"time"
)

// SegmentMeasurement is one Flow Segment's contribution to a Flow's bit rate.
type SegmentMeasurement struct {
	Bytes int64
	// Duration is in nanoseconds, matching the rest of the timeline code.
	Duration int64
}

// bitsPerByte and the nanosecond scale together convert bytes over nanoseconds
// into the 1000 bits/second the Flow properties are expressed in.
const bitsPerByte = 8

// SegmentBitRates returns the avg_bit_rate and max_bit_rate for a Flow, in
// 1000 bits/second, and whether they could be derived at all.
//
// These are deliberately not the essence bit rate FFprobe reports. AppNote 0013
// defines both properties as *Segment* bit rates, so they include container
// overhead and the packaging the Segments were actually written with. An
// essence figure understates what a reader has to pull off the wire, which
// matters because max_bit_rate is what sizes a receiver's buffer.
//
// avg_bit_rate is the whole Flow's bits over its whole duration. max_bit_rate
// is the peak over any contiguous run of Segments lasting between 0.5 and 1.5
// times the target duration, which is the HLS definition the AppNote adopts,
// extended so that a single Segment longer than 1.5 times the target still
// counts -- otherwise segmentation with large GOPs would have no peak at all.
func SegmentBitRates(segments []SegmentMeasurement, target time.Duration) (avgBitRate, maxBitRate int64, ok bool) {
	accumulator := NewSegmentBitRateAccumulator(target)
	for _, segment := range segments {
		accumulator.Add(segment)
	}
	return accumulator.Result()
}

// SegmentBitRateAccumulator derives the same AppNote 0013 rates while keeping
// only the current peak-rate time window. A long Flow therefore does not need
// one retained measurement per Segment merely to calculate final metadata.
type SegmentBitRateAccumulator struct {
	target        time.Duration
	totalBytes    big.Int
	totalDuration big.Int
	window        []SegmentMeasurement
	windowTime    int64
	maxBitRate    int64
}

func NewSegmentBitRateAccumulator(target time.Duration) *SegmentBitRateAccumulator {
	return &SegmentBitRateAccumulator{target: target}
}

func (a *SegmentBitRateAccumulator) Add(segment SegmentMeasurement) {
	if a == nil || segment.Duration <= 0 || segment.Bytes < 0 {
		return
	}
	a.totalBytes.Add(&a.totalBytes, big.NewInt(segment.Bytes))
	a.totalDuration.Add(&a.totalDuration, big.NewInt(segment.Duration))

	lower, upper := int64(a.target)/2, int64(a.target)*3/2
	if upper <= 0 {
		a.consider(segment.Bytes, segment.Duration)
		return
	}
	a.window = append(a.window, segment)
	a.windowTime += segment.Duration

	var bytes, duration int64
	for index := len(a.window) - 1; index >= 0; index-- {
		bytes += a.window[index].Bytes
		duration += a.window[index].Duration
		if duration > upper {
			if index == len(a.window)-1 {
				a.consider(bytes, duration)
			}
			break
		}
		if duration >= lower {
			a.consider(bytes, duration)
		}
	}
	// A prefix already outside the largest admissible window cannot become
	// eligible again when future Segments extend it, so release it now.
	for len(a.window) > 1 && a.windowTime > upper {
		a.windowTime -= a.window[0].Duration
		a.window = a.window[1:]
	}
}

func (a *SegmentBitRateAccumulator) consider(bytes, duration int64) {
	if rate := kilobitsPerSecond(bytes, duration); rate > a.maxBitRate {
		a.maxBitRate = rate
	}
}

func (a *SegmentBitRateAccumulator) Result() (avgBitRate, maxBitRate int64, ok bool) {
	if a == nil || a.totalDuration.Sign() <= 0 {
		return 0, 0, false
	}
	avgBitRate = kilobitsPerSecondBig(&a.totalBytes, &a.totalDuration)
	maxBitRate = a.maxBitRate
	if maxBitRate == 0 {
		// Nothing filled a window, which happens when the media is shorter than
		// half a target Segment. The whole Flow is then the only run there is.
		maxBitRate = avgBitRate
	}
	return avgBitRate, maxBitRate, true
}

// kilobitsPerSecond converts bytes over nanoseconds into 1000 bits/second,
// truncating as the AppNote's int() does. Arbitrary precision is used because
// an hour of uncompressed video overflows the intermediate product in int64.
func kilobitsPerSecond(bytes, nanoseconds int64) int64 {
	if nanoseconds <= 0 {
		return 0
	}
	return kilobitsPerSecondBig(big.NewInt(bytes), big.NewInt(nanoseconds))
}

func kilobitsPerSecondBig(bytes, nanoseconds *big.Int) int64 {
	if bytes == nil || nanoseconds == nil || nanoseconds.Sign() <= 0 {
		return 0
	}
	numerator := new(big.Int).Mul(new(big.Int).Set(bytes), big.NewInt(bitsPerByte*int64(time.Second/time.Nanosecond)))
	denominator := new(big.Int).Mul(new(big.Int).Set(nanoseconds), big.NewInt(1000))
	result := new(big.Int).Quo(numerator, denominator)
	if !result.IsInt64() {
		return 0
	}
	return result.Int64()
}
