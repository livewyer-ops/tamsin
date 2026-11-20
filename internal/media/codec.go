package media

import (
	"regexp"
	"strconv"
	"strings"
)

func codecMIME(codec string) string {
	known := map[string]string{
		"aac": "audio/aac", "ac3": "audio/ac3", "eac3": "audio/eac3", "flac": "audio/flac", "mp2": "audio/mpeg", "mp3": "audio/mpeg", "opus": "audio/opus", "vorbis": "audio/vorbis",
		"h264": "video/h264", "hevc": "video/h265", "av1": "video/AV1", "ffv1": "video/FFV1", "mpeg2video": "video/mpeg", "mpeg4": "video/mp4v-es", "prores": "video/quicktime", "vp8": "video/VP8", "vp9": "video/VP9",
		"mpeg1video": "video/mpeg", "dvvideo": "video/DV", "dnxhd": "video/x-dnxhd",
		"mjpeg": "image/jpeg", "jpeg2000": "image/jp2", "png": "image/png", "gif": "image/gif", "webp": "image/webp",
		"dts": "audio/vnd.dts", "truehd": "audio/vnd.dolby.mlp", "s302m": "audio/x-smpte302m",
		"pcm_alaw": "audio/PCMA", "pcm_mulaw": "audio/PCMU",
		"subrip": "application/x-subrip", "ass": "text/x-ssa", "webvtt": "text/vtt", "ttml": "application/ttml+xml",
		"mov_text": "text/x-quicktime-text", "dvb_subtitle": "application/x-dvb-subtitle",
		"dvb_teletext": "application/x-dvb-teletext", "scte_35": "application/x-scte35",
		"eia_608": "application/x-cea-608",
	}
	if value := known[strings.ToLower(codec)]; value != "" {
		return value
	}
	if strings.HasPrefix(codec, "pcm_") {
		switch {
		case strings.Contains(codec, "f32") || strings.Contains(codec, "f64"):
			return "audio/x-raw-float"
		case strings.Contains(codec, "f16") || strings.Contains(codec, "f24"):
			// The schema allows only 32- and 64-bit floating point.
			return ""
		}
		return "audio/x-raw-int"
	}
	return ""
}

// essenceCodec is the coding media type for a stream as the Flow describes
// it. JPEG 2000 is image/jp2 for a still picture and video/jp2 for moving
// pictures, the value the BBC examples use for J2K video Flows.
func essenceCodec(stream Stream, stillImage bool) string {
	codec := codecMIME(stream.CodecName)
	if stream.CodecType == "video" && !stillImage && strings.EqualFold(stream.CodecName, "jpeg2000") {
		return "video/jp2"
	}
	return codec
}

// describableStreams separates the streams Tamsin can describe as Flows from
// data and attachment tracks that have no coding media type to declare. Video,
// audio and subtitle streams are always kept: an unknown codec there is a
// reason to stop, not to drop the essence.
func describableStreams(streams []Stream) (kept, undescribed []Stream) {
	for _, stream := range streams {
		if UndescribableDataStream(stream) {
			undescribed = append(undescribed, stream)
			continue
		}
		kept = append(kept, stream)
	}
	return kept, undescribed
}

// streamCodecLabel names a stream's coding for diagnostics. FFprobe leaves
// codec_name out for tracks FFmpeg has no decoder for, such as timecode,
// and reports only the container's four-character tag.
func streamCodecLabel(stream Stream) string {
	switch {
	case stream.CodecName != "":
		return stream.CodecName
	case stream.CodecTagString != "":
		return stream.CodecTagString
	}
	return "unknown"
}

// UndescribableDataStream reports a data or attachment track with no coding
// media type Tamsin can name, such as timecode. Such a track cannot be an
// elemental Flow, because the schema requires a codec, so it stays in the
// preserved multiplex undescribed.
func UndescribableDataStream(stream Stream) bool {
	switch stream.CodecType {
	case "data", "attachment":
		return codecMIME(stream.CodecName) == ""
	}
	return false
}

func pcmUncompressedType(codec string) string {
	codec = strings.ToLower(strings.TrimSpace(codec))
	if strings.HasSuffix(codec, "_planar") || codec == "pcm_lxf" {
		return "planar"
	}
	return "interleaved"
}

func bitDepth(stream Stream) int {
	for _, value := range []string{stream.BitsPerRawSample, strconv.Itoa(stream.BitsPerSample)} {
		depth, err := strconv.Atoi(value)
		if err == nil && depth > 0 {
			return depth
		}
	}
	return pixelFormatDepth(stream.PixelFormat)
}

var (
	planarDepthPattern = regexp.MustCompile(`p(9|1[0-6])$`)
	grayDepthPattern   = regexp.MustCompile(`^(?:gray|ya)(9|1[0-6]|32)f?$`)
	semiPlanarPattern  = regexp.MustCompile(`^p[024](10|12|16)$`)
	packedRGB16Pattern = regexp.MustCompile(`^(?:rgb|bgr)48$|^(?:rgba|bgra)64$`)
	packedRGB10Pattern = regexp.MustCompile(`^x2(?:rgb|bgr)10$`)
)

// pixelFormatDepth reads the sample depth an FFmpeg pixel format name states
// explicitly: the planar suffix (yuv420p10le, gbrp12be), gray and gray-alpha
// depths, semi-planar P010/P016, and the 16-bit packed RGB forms. Formats
// that state no depth, such as yuv420p or nv12, give zero rather than a
// guess: digits in a chroma-subsampling group are not a depth.
func pixelFormatDepth(name string) int {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.TrimSuffix(strings.TrimSuffix(name, "le"), "be")
	if name == "" {
		return 0
	}
	depth := func(digits string) int {
		value, _ := strconv.Atoi(digits)
		return value
	}
	switch {
	case packedRGB16Pattern.MatchString(name):
		return 16
	case packedRGB10Pattern.MatchString(name):
		return 10
	}
	if match := semiPlanarPattern.FindStringSubmatch(name); match != nil {
		return depth(match[1])
	}
	if match := grayDepthPattern.FindStringSubmatch(name); match != nil {
		return depth(match[1])
	}
	if match := planarDepthPattern.FindStringSubmatch(name); match != nil {
		return depth(match[1])
	}
	return 0
}

func interlaceMode(value string) string {
	switch strings.ToLower(value) {
	case "progressive":
		return "progressive"
	case "tt":
		return "interlaced_tff"
	case "bb":
		return "interlaced_bff"
	default:
		// FFmpeg documents tb/bt in terms of coded and displayed order,
		// while its demuxers and muxers do not interpret those values
		// consistently. Neither value distinguishes interlace from PsF.
		// Omitting an optional field is safer than choosing the wrong scan.
		return ""
	}
}

// videoColorspace names the colour system the essence was authored in.
//
// The primaries answer that question. The matrix coefficients FFprobe reports
// as color_space usually agree with them and are a different property: a file
// can carry BT.709 matrix coefficients with BT.2020 primaries, and reading the
// matrix would then describe it as the wrong system entirely. Primaries are
// therefore preferred, with the matrix used only when a file states no
// primaries -- which older material often does not.
//
// BT.2100 is BT.2020 primaries with an HDR transfer, so the transfer is what
// separates the two. Reporting BT2020 alongside HLG or PQ would name a system
// that does not use those transfer functions.
func videoColorspace(primaries, matrix, transfer string) string {
	system := colorSystem(primaries)
	if system == "" {
		system = colorSystem(matrix)
	}
	if system == "BT2020" && (transfer == "HLG" || transfer == "PQ") {
		return "BT2100"
	}
	return system
}

// colorSystem maps either a primaries or a matrix coefficient name onto the
// colour systems TAMS names. The two vocabularies overlap, which is why one
// table serves both.
func colorSystem(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "bt709":
		return "BT709"
	case "bt2020", "bt2020nc", "bt2020c":
		return "BT2020"
	case "smpte170m", "bt470bg", "bt470m", "smpte240m":
		return "BT601"
	default:
		return ""
	}
}

func transferCharacteristic(value string) string {
	switch strings.ToLower(value) {
	case "arib-std-b67":
		return "HLG"
	case "smpte2084":
		return "PQ"
	case "bt709", "smpte170m", "gamma22", "gamma28":
		return "SDR"
	default:
		return ""
	}
}
