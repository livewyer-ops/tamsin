package media

import (
	"fmt"
	"mime"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/livewyer-ops/tamsin/internal/tams"
)

type Identity struct {
	FlowID               string
	SourceID             string
	Label                string
	URI                  string
	SHA256               string
	Size                 int64
	IngestProfile        string
	IngestProfileVersion string
	FFmpegVersion        string
	MediaToolchain       string
}

type FlowInfo struct {
	Format      string
	Codec       string
	Container   string
	Start       int64
	Duration    int64
	ContentType string
	// SegmentContainer is the explicit FFmpeg muxer/suffix pair for preserving
	// the source container during segmentation. It is empty when Tamsin can
	// store the input whole but cannot safely promise a source-format remux.
	SegmentContainer SegmentContainer
	// ContainerSupported distinguishes an intentional supported-profile mapping
	// from the honest application/octet-stream fallback. TAMS still requires an
	// object-owning Flow to declare a container; callers use this bit to tell the
	// operator that Tamsin declined to guess a more specific value.
	ContainerSupported bool
	// Collected holds the mono-essence Flows a multi-essence Flow gathers, in
	// container track order. Empty for single-essence inputs.
	Collected []CollectedFlow
	// UnsupportedCodecs records elementary streams for which Tamsin has no
	// defensible coding media type. The generated elemental Flow omits codec;
	// the ingest layer warns and an explicit metadata override may supply the
	// required value before final schema preflight.
	UnsupportedCodecs []UnsupportedCodec
}

type UnsupportedCodec struct {
	Name        string
	StreamType  string
	StreamIndex int
}

func BuildFlow(probe Probe, identity Identity, detectedContentType string, storage EssenceStorage) (tams.Flow, FlowInfo, error) {
	streams := contentStreams(probe.Streams)
	start, duration, err := probeTiming(probe, streams)
	if err != nil {
		return nil, FlowInfo{}, err
	}

	flow := tams.Flow{
		"id":          identity.FlowID,
		"source_id":   identity.SourceID,
		"label":       identity.Label,
		"description": "Ingested by Tamsin",
		"tags":        provenanceTags(identity),
	}

	info := FlowInfo{
		Start: start, Duration: duration,
		SegmentContainer: SourceSegmentContainer(probe.Format, detectedContentType),
	}
	for _, stream := range streams {
		if codecMIME(stream.CodecName) == "" {
			info.UnsupportedCodecs = append(info.UnsupportedCodecs, UnsupportedCodec{
				Name: stream.CodecName, StreamType: stream.CodecType, StreamIndex: stream.Index,
			})
		}
	}
	if len(streams) == 0 {
		contentType := normalizeMIME(detectedContentType)
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		flow["format"] = "urn:x-nmos:format:data"
		flow["codec"] = contentType
		flow["container"] = contentType
		flow["essence_parameters"] = map[string]any{}
		info.Format = "urn:x-nmos:format:data"
		info.Codec = contentType
		info.Container = contentType
		info.ContentType = contentType
		info.ContainerSupported = contentType != "application/octet-stream"
		return flow, info, nil
	}

	// A muxed input is described by a multi-essence Flow that owns the Media
	// Objects, collecting one mono-essence Flow per elementary stream. Per
	// AppNote 0006 the collected Flows carry no `container`, because they do not
	// reference Media Objects directly; their media is reached through the
	// multi-essence Flow's Segments, located by `container_mapping`.
	if len(streams) > 1 {
		container := describeContainer(probe.Format, containerEssence(streams), detectedContentType)
		flow["format"] = "urn:x-nmos:format:multi"
		flow["container"] = container.mediaType
		info.Format = "urn:x-nmos:format:multi"
		info.Container = container.mediaType
		info.ContentType = container.mediaType
		info.ContainerSupported = container.supported

		collected, err := collectEssenceFlows(streams, probe, identity, detectedContentType, start, duration, storage)
		if err != nil {
			return nil, FlowInfo{}, err
		}
		info.Collected = collected
		return flow, info, nil
	}

	stream := streams[0]
	codec := codecMIME(stream.CodecName)
	container := describeContainer(probe.Format, stream.CodecType, detectedContentType)
	info.Codec = codec
	info.Container = container.mediaType
	info.ContentType = container.mediaType
	info.ContainerSupported = container.supported

	stillImage, err := applyEssence(flow, stream, probe, identity, duration, true)
	if err != nil {
		return nil, FlowInfo{}, err
	}
	info.Format, _ = flow["format"].(string)
	if stillImage {
		info.Duration = 0
	}

	flow["container"] = container.mediaType
	return flow, info, nil
}

// provenanceTags records where the media came from. AppNote 0003 reserves
// unprefixed tag names for interoperable use and requires implementation-
// specific tags to carry an underscore and the service name.
//
// Every Flow Tamsin creates carries these, including the per-essence Flows of a
// demultiplexed input, so provenance survives however the essences were stored.
// TagPrefix marks a tag as Tamsin's own. AppNote 0003 reserves unprefixed names
// for tags that mean the same thing across implementations, so anything an
// implementation invents carries an underscore and its name. It is also how a
// later run tells its own tags from another system's.
const (
	TagPrefix            = "_tamsin_"
	ProvenanceSourcesTag = TagPrefix + "sources"
)

func provenanceTags(identity Identity) map[string]any {
	tags := map[string]any{
		ProvenanceSourcesTag: []string{identity.URI},
		TagPrefix + "sha256": identity.SHA256,
		TagPrefix + "bytes":  strconv.FormatInt(identity.Size, 10),
	}
	if identity.IngestProfile != "" {
		tags[TagPrefix+"ingest_profile"] = identity.IngestProfile
	}
	if identity.IngestProfileVersion != "" {
		tags[TagPrefix+"ingest_profile_version"] = identity.IngestProfileVersion
	}
	// FFmpeg appears only when it actually wrote the stored representation.
	// Whole-file muxed ingest therefore keeps the same identity and provenance
	// regardless of which unused FFmpeg happens to be installed on the host.
	if identity.FFmpegVersion != "" {
		tags[TagPrefix+"ffmpeg_version"] = identity.FFmpegVersion
	}
	if identity.MediaToolchain != "" {
		tags[TagPrefix+"media_toolchain"] = identity.MediaToolchain
	}
	return tags
}

// applyEssence writes the format-specific properties for one elementary stream
// onto flow. allowStillImage is set only for single-stream inputs, where a
// zero-rate video track describes a picture rather than a moving image; inside
// a mux every track is described as its own essence. It reports whether the
// stream was described as a still image.
func applyEssence(flow tams.Flow, stream Stream, probe Probe, identity Identity, duration int64, allowStillImage bool) (bool, error) {
	codec := codecMIME(stream.CodecName)
	switch stream.CodecType {
	case "video":
		if allowStillImage && isStillImage(probe, stream, duration) {
			if stream.Width <= 0 || stream.Height <= 0 {
				return false, errorsForDimensions(identity.Label)
			}
			flow["format"] = "urn:x-tam:format:image"
			flow["essence_parameters"] = map[string]any{
				"frame_width":  stream.Width,
				"frame_height": stream.Height,
			}
			if codec != "" {
				flow["codec"] = codec
			}
			return true, nil
		} else {
			if stream.Width <= 0 || stream.Height <= 0 {
				return false, errorsForDimensions(identity.Label)
			}
			parameters := map[string]any{
				"frame_width":  stream.Width,
				"frame_height": stream.Height,
			}
			switch stream.Cadence {
			case CadenceVariable:
				parameters["vfr"] = true
			case CadenceUnknown:
				// TAMS 8.1 has no representation for an unknown video rate:
				// vfr=true is a positive claim, while omitting vfr requires a
				// frame_rate. Leave both absent so final schema preflight rejects
				// the Flow unless an operator supplies complete evidence.
			default:
				if numerator, denominator, ok := streamFrameRate(stream); ok {
					parameters["frame_rate"] = map[string]any{"numerator": numerator, "denominator": denominator}
				} else if stream.Cadence == CadenceUnexamined {
					// Preserve the behavior of direct/programmatic Probe values. The
					// production pipeline always performs the timestamp scan first.
					parameters["vfr"] = true
				}
			}
			if mode := interlaceMode(probe.Format, stream.FieldOrder); mode != "" {
				parameters["interlace_mode"] = mode
			}
			transfer := transferCharacteristic(stream.ColorTransfer)
			if colorspace := videoColorspace(stream.ColorPrimaries, stream.ColorSpace, transfer); colorspace != "" {
				parameters["colorspace"] = colorspace
			}
			if transfer != "" {
				parameters["transfer_characteristic"] = transfer
			}
			if numerator, denominator, ok := ParseAspectRatio(stream.SampleAspectRatio); ok {
				parameters["pixel_aspect_ratio"] = map[string]any{
					"numerator": numerator, "denominator": denominator,
				}
			}
			if numerator, denominator, ok := ParseAspectRatio(stream.DisplayAspectRatio); ok {
				parameters["aspect_ratio"] = map[string]any{
					"numerator": numerator, "denominator": denominator,
				}
			}
			if depth := bitDepth(stream); depth > 0 {
				parameters["bit_depth"] = depth
			}
			flow["format"] = "urn:x-nmos:format:video"
			if codec != "" {
				flow["codec"] = codec
			}
			flow["essence_parameters"] = parameters
		}
	case "audio":
		sampleRate, parseErr := strconv.Atoi(stream.SampleRate)
		if parseErr != nil || sampleRate <= 0 || stream.Channels <= 0 {
			return false, fmt.Errorf("audio input %q is missing a valid sample rate or channel count", identity.Label)
		}
		parameters := map[string]any{"sample_rate": sampleRate, "channels": stream.Channels}
		if depth := bitDepth(stream); depth > 0 {
			parameters["bit_depth"] = depth
		}
		if strings.HasPrefix(stream.CodecName, "pcm_") {
			parameters["unc_parameters"] = map[string]any{"unc_type": pcmUncompressedType(stream.CodecName)}
		}
		flow["format"] = "urn:x-nmos:format:audio"
		if codec != "" {
			flow["codec"] = codec
		}
		flow["essence_parameters"] = parameters
	case "data", "subtitle", "attachment":
		flow["format"] = "urn:x-nmos:format:data"
		if codec != "" {
			flow["codec"] = codec
		}
		flow["essence_parameters"] = map[string]any{}
	default:
		return false, fmt.Errorf("unsupported FFprobe stream type %q", stream.CodecType)
	}

	return false, nil
}

// CollectedFlow is a mono-essence Flow gathered by a multi-essence Flow. Role
// is the human-readable label recorded in the parent's `flow_collection`; the
// caller assigns identifiers, which is where deterministic ID policy lives.
type CollectedFlow struct {
	Role string
	Flow tams.Flow
	// ContainerMapping belongs to the item in the parent Flow's collection.
	// It describes how that parent container maps to this essence; putting it
	// only on the child loses which particular collection/container it applies
	// to when a Flow is collected more than once.
	ContainerMapping map[string]any
	// ContainerSupported describes the independently written essence Object. It
	// is unused for a mapped essence inside a shared mux.
	ContainerSupported bool
	// StreamIndex is where this stream sits in the container, which is not its
	// position in this slice: anything that is not essence, such as an attached
	// picture, is filtered out before the collection is built. Demultiplexing
	// by slice position would extract the wrong track.
	StreamIndex int
	// Offset is how long after the container's own start this stream begins, in
	// nanoseconds. Essences do not necessarily start together, and preserving
	// the difference between them is what keeps them in sync once each is
	// stored as a Flow of its own.
	Offset int64
}

// collectEssenceFlows describes each elementary stream of a muxed input as its
// own Flow. `track_index` counts every track in the container and
// `format_track_index` counts within one format, so a reader that cannot parse
// container-specific metadata can still locate the essence.
// storage selects which AppNote 0006 arrangement applies. The rules invert
// between them: a collected Flow describes essence inside a shared multiplex,
// so its parent Collection Item carries container_mapping and the child has no
// container of its own; an independently stored Flow owns its Media Objects,
// so it declares a container and has no multiplex to map into.
func collectEssenceFlows(streams []Stream, probe Probe, identity Identity, detectedContentType string, start, duration int64, storage EssenceStorage) ([]CollectedFlow, error) {
	formatCounts := make(map[string]int, len(streams))
	for _, stream := range streams {
		formatCounts[essenceRole(stream.CodecType)]++
	}

	seen := make(map[string]int, len(streams))
	collected := make([]CollectedFlow, 0, len(streams))
	for _, stream := range streams {
		role := essenceRole(stream.CodecType)
		formatTrackIndex := seen[role]
		seen[role]++

		flow := tams.Flow{
			"label":       identity.Label + " (" + role + ")",
			"description": "Ingested by Tamsin",
			"tags":        provenanceTags(identity),
		}
		container := describeContainer(probe.Format, stream.CodecType, detectedContentType)
		var mapping map[string]any
		if storage == EssenceStorageIndependent {
			// The essence is demultiplexed into its own Media Objects, so the Flow
			// declares the container those Objects are written in.
			flow["container"] = container.mediaType
		} else {
			mapping = map[string]any{
				// Counts every track in the container, so it is the stream's
				// own index rather than its position among the essences: a
				// reader locating the track by this number has the whole
				// container in front of it, attached pictures included.
				"track_index":        stream.Index,
				"format_track_index": formatTrackIndex,
			}
		}
		// A still image is only meaningful for a whole single-stream input, so
		// streams inside a mux are always described as their own essence type.
		if _, err := applyEssence(flow, stream, probe, identity, duration, false); err != nil {
			return nil, err
		}

		// Roles must distinguish members of the collection, so only number them
		// where a format appears more than once.
		itemRole := role
		if formatCounts[role] > 1 {
			itemRole = role + " " + strconv.Itoa(formatTrackIndex)
		}
		offset, err := streamOffset(stream, start)
		if err != nil {
			return nil, err
		}
		collected = append(collected, CollectedFlow{
			Role: itemRole, Flow: flow, ContainerMapping: mapping,
			ContainerSupported: container.supported, StreamIndex: stream.Index, Offset: offset,
		})
	}
	return collected, nil
}

// streamOffset reports how long after containerStart a stream begins.
//
// A stream that does not say when it starts is taken to start with the
// container, which is the only assumption available and the one that leaves the
// essences where they already were. A negative result is treated as zero: a
// stream cannot begin before the container it is in, and trusting a
// contradictory value would move media backwards on the timeline.
func streamOffset(stream Stream, containerStart int64) (int64, error) {
	if stream.StartTime == "" || stream.StartTime == "N/A" {
		return 0, nil
	}
	streamStart, err := ParseSeconds(stream.StartTime)
	if err != nil {
		return 0, fmt.Errorf("parse stream start time: %w", err)
	}
	offset, err := TimestampOffset(streamStart, containerStart)
	if err != nil {
		return 0, fmt.Errorf("calculate stream start offset: %w", err)
	}
	return max(offset, 0), nil
}

func essenceRole(codecType string) string {
	switch codecType {
	case "video", "audio":
		return codecType
	default:
		return "data"
	}
}

func DetectContentType(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", fmt.Errorf("open input for content detection: %w", err)
	}
	defer file.Close()
	buffer := make([]byte, 512)
	read, err := file.Read(buffer)
	if err != nil && read == 0 {
		return "", fmt.Errorf("read input for content detection: %w", err)
	}
	return normalizeMIME(http.DetectContentType(buffer[:read])), nil
}

func contentStreams(streams []Stream) []Stream {
	result := make([]Stream, 0, len(streams))
	for _, stream := range streams {
		if stream.Disposition.AttachedPicture == 0 {
			result = append(result, stream)
		}
	}
	return result
}

// ProbeTiming returns the timeline represented by a probe, excluding attached
// pictures. When the container does not report a duration, the result extends
// through the latest stream end rather than merely choosing the longest stream;
// that distinction preserves delayed audio, subtitle, and data tracks.
func ProbeTiming(probe Probe) (start, duration int64, err error) {
	return probeTiming(probe, contentStreams(probe.Streams))
}

func probeTiming(probe Probe, streams []Stream) (start, duration int64, err error) {
	start, err = probeStart(probe, streams)
	if err != nil {
		return 0, 0, err
	}
	duration, err = probeDuration(probe, streams, start)
	if err != nil {
		return 0, 0, err
	}
	return start, duration, nil
}

func probeStart(probe Probe, streams []Stream) (int64, error) {
	if probe.Format.StartTime != "" && probe.Format.StartTime != "N/A" {
		return ParseSeconds(probe.Format.StartTime)
	}
	var earliest int64
	set := false
	for _, stream := range streams {
		if stream.StartTime == "" || stream.StartTime == "N/A" {
			continue
		}
		start, err := ParseSeconds(stream.StartTime)
		if err != nil {
			return 0, fmt.Errorf("parse stream start time: %w", err)
		}
		if !set || start < earliest {
			earliest = start
			set = true
		}
	}
	return earliest, nil
}

func probeDuration(probe Probe, streams []Stream, start int64) (int64, error) {
	if probe.Format.Duration != "" && probe.Format.Duration != "N/A" {
		duration, err := ParseSeconds(probe.Format.Duration)
		if err != nil {
			return 0, err
		}
		if duration < 0 {
			return 0, fmt.Errorf("container duration cannot be negative")
		}
		return duration, nil
	}
	var latestEnd int64
	for _, stream := range streams {
		duration, err := ParseSeconds(stream.Duration)
		if err != nil {
			return 0, fmt.Errorf("parse stream duration: %w", err)
		}
		if duration < 0 {
			return 0, fmt.Errorf("stream %d duration cannot be negative", stream.Index)
		}
		offset, err := streamOffset(stream, start)
		if err != nil {
			return 0, err
		}
		end, err := TimestampShift(offset, duration)
		if err != nil {
			return 0, fmt.Errorf("calculate stream %d end: %w", stream.Index, err)
		}
		if end > latestEnd {
			latestEnd = end
		}
	}
	return latestEnd, nil
}

func codecMIME(codec string) string {
	known := map[string]string{
		"aac": "audio/aac", "ac3": "audio/ac3", "eac3": "audio/eac3", "flac": "audio/flac", "mp2": "audio/mpeg", "mp3": "audio/mpeg", "opus": "audio/opus", "vorbis": "audio/vorbis",
		"h264": "video/h264", "hevc": "video/h265", "av1": "video/AV1", "ffv1": "video/FFV1", "mpeg2video": "video/mpeg", "mpeg4": "video/mp4v-es", "prores": "video/quicktime", "vp8": "video/VP8", "vp9": "video/VP9",
		"mjpeg": "image/jpeg", "jpeg2000": "image/jp2", "png": "image/png", "gif": "image/gif", "webp": "image/webp",
		"subrip": "application/x-subrip", "ass": "text/x-ssa", "webvtt": "text/vtt", "ttml": "application/ttml+xml",
	}
	if value := known[strings.ToLower(codec)]; value != "" {
		return value
	}
	if strings.HasPrefix(codec, "pcm_") {
		if strings.Contains(codec, "f32") || strings.Contains(codec, "f64") {
			return "audio/x-raw-float"
		}
		return "audio/x-raw-int"
	}
	return ""
}

func pcmUncompressedType(codec string) string {
	codec = strings.ToLower(strings.TrimSpace(codec))
	if strings.HasSuffix(codec, "_planar") || codec == "pcm_lxf" {
		return "planar"
	}
	return "interleaved"
}

const fallbackContainerMIME = "application/octet-stream"

// containerDescription is deliberately a supported-profile result, not a
// general-purpose file classifier. FFprobe identifies the format family and
// the file supplies any ISO BMFF brand; Tamsin owns only the final MIME policy
// for formats it can describe defensibly.
type containerDescription struct {
	mediaType string
	supported bool
}

func supportedContainer(mediaType string) containerDescription {
	return containerDescription{mediaType: mediaType, supported: true}
}

func unsupportedContainer() containerDescription {
	return containerDescription{mediaType: fallbackContainerMIME}
}

func describeContainer(format Format, streamType, detected string) containerDescription {
	names := ffprobeFormatNames(format.Name)

	// FFprobe reports WebM as "matroska,webm". Test the constrained subtype
	// before its parent family or every WebM Object becomes Matroska.
	if names["webm"] {
		return essenceContainer(streamType, "video/webm", "audio/webm")
	}
	if names["matroska"] {
		// RFC 9559 registered these names and says new implementations should
		// no longer emit the historical x-matroska forms.
		return essenceContainer(streamType, "video/matroska", "audio/matroska")
	}
	if names["mov"] || names["mp4"] || names["m4a"] || names["3gp"] || names["3g2"] || names["mj2"] {
		return isoBMFFContainer(format.Tags, streamType, detected)
	}

	switch {
	case names["mpegts"]:
		// MPEG-TS keeps this file-level type even when it carries audio only.
		return supportedContainer("video/mp2t")
	case names["mxf"]:
		return supportedContainer("application/mxf")
	case names["mpeg"] || names["mpegvideo"]:
		return supportedContainer("video/mpeg")
	case names["avi"]:
		return supportedContainer("video/x-msvideo")
	case names["asf"]:
		return supportedContainer("video/x-ms-asf")
	case names["wav"]:
		return supportedContainer("audio/wav")
	case names["aiff"]:
		return supportedContainer("audio/aiff")
	case names["flac"]:
		return supportedContainer("audio/flac")
	case names["mp3"]:
		return supportedContainer("audio/mpeg")
	case names["aac"]:
		return supportedContainer("audio/aac")
	case names["ac3"]:
		return supportedContainer("audio/ac3")
	case names["eac3"]:
		return supportedContainer("audio/eac3")
	case names["ogg"]:
		return essenceContainer(streamType, "video/ogg", "audio/ogg")
	case names["jpeg_pipe"] || names["mjpeg"]:
		return supportedContainer("image/jpeg")
	case names["j2k_pipe"] || names["jpeg2000"]:
		return supportedContainer("image/jp2")
	case names["png_pipe"]:
		return supportedContainer("image/png")
	case names["gif"]:
		return supportedContainer("image/gif")
	case names["webp_pipe"]:
		return supportedContainer("image/webp")
	case names["image2"] || names["image2pipe"]:
		if mediaType := normalizeMIME(detected); strings.HasPrefix(mediaType, "image/") {
			return supportedContainer(mediaType)
		}
	}
	return unsupportedContainer()
}

func ffprobeFormatNames(value string) map[string]bool {
	names := make(map[string]bool)
	for _, name := range strings.Split(strings.ToLower(value), ",") {
		names[strings.TrimSpace(name)] = true
	}
	return names
}

// SourceSegmentContainer is intentionally narrower than describeContainer. A
// Flow can honestly label a whole stored Object even when FFmpeg cannot safely
// remux that family into independently named Segments. An empty answer makes
// that unsupported treatment fail before TAMS is mutated.
func SourceSegmentContainer(format Format, detected string) SegmentContainer {
	names := ffprobeFormatNames(format.Name)
	if names["webm"] {
		return SegmentContainer{Muxer: "webm", Extension: ".webm"}
	}
	if names["matroska"] {
		return SegmentContainer{Muxer: "matroska", Extension: ".mkv"}
	}
	if names["mov"] || names["mp4"] || names["m4a"] || names["3gp"] || names["3g2"] || names["mj2"] {
		switch isoBMFFContainer(format.Tags, "video", detected).mediaType {
		case "video/quicktime":
			return SegmentContainer{Muxer: "mov", Extension: ".mov"}
		case "video/3gpp", "audio/3gpp":
			return SegmentContainer{Muxer: "3gp", Extension: ".3gp"}
		case "video/3gpp2", "audio/3gpp2":
			return SegmentContainer{Muxer: "3g2", Extension: ".3g2"}
		case "video/mp4", "audio/mp4", "application/mp4":
			return SegmentContainer{Muxer: "mp4", Extension: ".mp4"}
		}
		return SegmentContainer{}
	}

	switch {
	case names["mpegts"]:
		return SegmentContainer{Muxer: "mpegts", Extension: ".ts"}
	case names["mxf"]:
		return SegmentContainer{Muxer: "mxf", Extension: ".mxf"}
	case names["mpeg"]:
		return SegmentContainer{Muxer: "mpeg", Extension: ".mpg"}
	case names["avi"]:
		return SegmentContainer{Muxer: "avi", Extension: ".avi"}
	case names["asf"]:
		return SegmentContainer{Muxer: "asf", Extension: ".asf"}
	case names["wav"]:
		return SegmentContainer{Muxer: "wav", Extension: ".wav"}
	case names["aiff"]:
		return SegmentContainer{Muxer: "aiff", Extension: ".aiff"}
	case names["flac"]:
		return SegmentContainer{Muxer: "flac", Extension: ".flac"}
	case names["mp3"]:
		return SegmentContainer{Muxer: "mp3", Extension: ".mp3"}
	case names["aac"]:
		return SegmentContainer{Muxer: "adts", Extension: ".aac"}
	case names["ac3"]:
		return SegmentContainer{Muxer: "ac3", Extension: ".ac3"}
	case names["eac3"]:
		return SegmentContainer{Muxer: "eac3", Extension: ".eac3"}
	case names["ogg"]:
		return SegmentContainer{Muxer: "ogg", Extension: ".ogg"}
	case names["jpeg_pipe"] || names["mjpeg"]:
		return SegmentContainer{Muxer: "image2", Extension: ".jpg"}
	case names["j2k_pipe"] || names["jpeg2000"]:
		return SegmentContainer{Muxer: "image2", Extension: ".jp2"}
	case names["png_pipe"]:
		return SegmentContainer{Muxer: "image2", Extension: ".png"}
	case names["gif"]:
		return SegmentContainer{Muxer: "image2", Extension: ".gif"}
	case names["webp_pipe"]:
		return SegmentContainer{Muxer: "image2", Extension: ".webp"}
	case names["image2"] || names["image2pipe"]:
		switch normalizeMIME(detected) {
		case "image/jpeg":
			return SegmentContainer{Muxer: "image2", Extension: ".jpg"}
		case "image/png":
			return SegmentContainer{Muxer: "image2", Extension: ".png"}
		case "image/gif":
			return SegmentContainer{Muxer: "image2", Extension: ".gif"}
		case "image/webp":
			return SegmentContainer{Muxer: "image2", Extension: ".webp"}
		}
	}
	return SegmentContainer{}
}

func essenceContainer(streamType, videoType, audioType string) containerDescription {
	switch streamType {
	case "video":
		return supportedContainer(videoType)
	case "audio":
		return supportedContainer(audioType)
	default:
		return unsupportedContainer()
	}
}

func isoBMFFContainer(tags map[string]string, streamType, detected string) containerDescription {
	brand := strings.ToLower(strings.TrimSpace(tags["major_brand"]))
	switch {
	case brand == "qt":
		// video/quicktime is the registered file-level type, including for a
		// QuickTime container whose only presentation is audio.
		return supportedContainer("video/quicktime")
	case strings.HasPrefix(brand, "3gp"):
		return essenceContainer(streamType, "video/3gpp", "audio/3gpp")
	case strings.HasPrefix(brand, "3g2"):
		return essenceContainer(streamType, "video/3gpp2", "audio/3gpp2")
	case brand == "mjp2" || brand == "mj2s":
		return supportedContainer("video/mj2")
	case isMP4Brand(brand) || hasMP4CompatibleBrand(tags["compatible_brands"]):
		return mp4Container(streamType)
	}

	// Some probers omit format tags. A content signature that identifies one
	// member still supplies a deterministic fallback; an unknown, contradictory
	// brand does not get silently promoted to MP4.
	if brand == "" {
		switch normalizeMIME(detected) {
		case "video/quicktime":
			return supportedContainer("video/quicktime")
		case "video/3gpp", "audio/3gpp":
			return essenceContainer(streamType, "video/3gpp", "audio/3gpp")
		case "video/3gpp2", "audio/3gpp2":
			return essenceContainer(streamType, "video/3gpp2", "audio/3gpp2")
		case "video/mj2":
			return supportedContainer("video/mj2")
		case "video/mp4", "audio/mp4", "application/mp4":
			return mp4Container(streamType)
		}
	}
	return unsupportedContainer()
}

func mp4Container(streamType string) containerDescription {
	switch streamType {
	case "video":
		return supportedContainer("video/mp4")
	case "audio":
		return supportedContainer("audio/mp4")
	default:
		// RFC 4337 reserves application/mp4 for a file with neither an audio
		// nor a visual presentation. A normal multiplex therefore reaches the
		// video or audio branch according to its actual streams.
		return supportedContainer("application/mp4")
	}
}

func isMP4Brand(brand string) bool {
	switch brand {
	case "isom", "mp41", "mp42", "m4a", "m4b", "m4p", "m4v":
		return true
	default:
		return false
	}
}

// FFprobe exposes ISO compatible_brands as concatenated four-byte codes. A new
// major brand that declares compatibility with the stable MP4 brands does not
// need another Tamsin table entry; unrelated ISO formats such as AVIF/HEIF do
// not acquire MP4 compatibility merely because the same demuxer opened them.
func hasMP4CompatibleBrand(value string) bool {
	for len(value) >= 4 {
		brand := strings.ToLower(strings.TrimSpace(value[:4]))
		if brand == "isom" || brand == "mp41" || brand == "mp42" {
			return true
		}
		value = value[4:]
	}
	return false
}

func containerEssence(streams []Stream) string {
	hasAudio := false
	for _, stream := range streams {
		switch stream.CodecType {
		case "video":
			return "video"
		case "audio":
			hasAudio = true
		}
	}
	if hasAudio {
		return "audio"
	}
	return "data"
}

func normalizeMIME(value string) string {
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil {
		return ""
	}
	return mediaType
}

func isStillImage(probe Probe, stream Stream, duration int64) bool {
	if duration > 0 {
		return false
	}
	format := strings.ToLower(probe.Format.Name)
	return strings.Contains(format, "image") || strings.Contains(format, "pipe") || stream.CodecName == "png" || stream.CodecName == "mjpeg" || stream.CodecName == "jpeg2000" || stream.CodecName == "webp" || stream.CodecName == "gif"
}

func bitDepth(stream Stream) int {
	for _, value := range []string{stream.BitsPerRawSample, strconv.Itoa(stream.BitsPerSample)} {
		depth, err := strconv.Atoi(value)
		if err == nil && depth > 0 {
			return depth
		}
	}
	for _, candidate := range []string{"16", "14", "12", "10", "8"} {
		if strings.Contains(stream.PixelFormat, candidate) {
			depth, _ := strconv.Atoi(candidate)
			return depth
		}
	}
	return 0
}

func interlaceMode(format Format, value string) string {
	switch strings.ToLower(value) {
	case "progressive":
		// FFmpeg deliberately collapses both FullFrame and SegmentedFrame
		// (PsF) MXF descriptors to AV_FIELD_PROGRESSIVE. The value therefore
		// cannot support either TAMS claim for MXF without another source of
		// production metadata.
		if ffprobeFormatNames(format.Name)["mxf"] {
			return ""
		}
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

func errorsForDimensions(label string) error {
	return fmt.Errorf("video input %q is missing valid frame dimensions", label)
}
