package media

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"strings"
)

func DetectContentType(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", fmt.Errorf("open input for content detection: %w", err)
	}
	defer file.Close()
	return DetectReaderContentType(file)
}

func DetectReaderContentType(reader io.Reader) (string, error) {
	buffer := make([]byte, 512)
	read, err := io.ReadFull(reader, buffer)
	if err != nil && read == 0 {
		return "", fmt.Errorf("read input for content detection: %w", err)
	}
	return normalizeMIME(http.DetectContentType(buffer[:read])), nil
}

var ErrUnsupportedContainer = errors.New("unsupported input container")

// sourceContainer is the packaging that an input's format name, brand and
// sniffed content type establish. The zero value is a container Tamsin
// refuses.
type sourceContainer int

const (
	containerUnsupported sourceContainer = iota
	containerMPEGTS
	containerMP4
)

// resolveContainer applies the same content-based policy to every input.
func resolveContainer(format Format, detected string) sourceContainer {
	names := ffprobeFormatNames(format.Name)
	switch {
	case names["mpegts"]:
		return containerMPEGTS
	case isISOBMFF(names) && isMP4(format.Tags, detected):
		return containerMP4
	}
	return containerUnsupported
}

// mime is the container's media type on a Flow of the given stream type, or
// empty for a refused container.
func (c sourceContainer) mime(streamType string) string {
	switch c {
	case containerMPEGTS:
		return "video/mp2t"
	case containerMP4:
		return mp4Container(streamType)
	}
	return ""
}

// segmentContainer is the muxer and extension that write this container.
func (c sourceContainer) segmentContainer() SegmentContainer {
	switch c {
	case containerMP4:
		return SegmentContainer{Muxer: "mp4", Extension: ".mp4"}
	case containerMPEGTS:
		return SegmentContainer{Muxer: "mpegts", Extension: ".ts"}
	}
	return SegmentContainer{}
}

func (c sourceContainer) validate(format Format) error {
	if c == containerUnsupported {
		return fmt.Errorf("%w %q (brand %q): only MP4 and MPEG-TS are supported", ErrUnsupportedContainer, format.Name, format.Tags["major_brand"])
	}
	return nil
}

// ValidateContainer refuses an input whose container is not supported.
func ValidateContainer(format Format, detected string) error {
	return resolveContainer(format, detected).validate(format)
}

// SourceSegmentContainer resolves a supported container from media evidence.
func SourceSegmentContainer(format Format, detected string) SegmentContainer {
	return resolveContainer(format, detected).segmentContainer()
}

func isISOBMFF(names map[string]bool) bool {
	return names["mov"] || names["mp4"] || names["m4a"] || names["3gp"] || names["3g2"] || names["mj2"]
}

func ffprobeFormatNames(value string) map[string]bool {
	names := make(map[string]bool)
	for _, name := range strings.Split(strings.ToLower(value), ",") {
		names[strings.TrimSpace(name)] = true
	}
	return names
}

// isMP4 reports whether an ISO base media file is one Tamsin stores as MP4.
func isMP4(tags map[string]string, detected string) bool {
	brand := strings.ToLower(strings.TrimSpace(tags["major_brand"]))
	switch {
	case brand == "qt", strings.HasPrefix(brand, "3gp"), strings.HasPrefix(brand, "3g2"),
		brand == "mjp2", brand == "mj2s", brand == "avif", brand == "avis", brand == "mif1", brand == "msf1",
		strings.HasPrefix(brand, "hei"):
		return false
	case isMP4Brand(brand) || hasMP4CompatibleBrand(tags["compatible_brands"]):
		return true
	}
	if brand == "" {
		switch normalizeMIME(detected) {
		case "video/mp4", "audio/mp4", "application/mp4":
			return true
		}
	}
	return false
}

func mp4Container(streamType string) string {
	switch streamType {
	case "video":
		return "video/mp4"
	case "audio":
		return "audio/mp4"
	default:
		// RFC 4337 reserves application/mp4 for a file with neither an audio
		// nor a visual presentation. A normal multiplex therefore reaches the
		// video or audio branch according to its actual streams.
		return "application/mp4"
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
