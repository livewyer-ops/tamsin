package media

import "strings"

// MP4 shares FFmpeg's mov demuxer with other ISO BMFF formats. The probe's
// brands distinguish supported MP4 from QuickTime, 3GP and other formats.
// Neither demuxer may open playlists, packages or nested resources.
const inputFormatAllowlist = "mov,mpegts"

// inputOptions pins one input to a single transport and to self-contained
// demuxers. A private bridge input is reached over loopback HTTP; every other
// input is a local file. Both lists deny the file, data, crypto, subfile and
// network protocols a nested resource would otherwise inherit.
func inputOptions(input string) []string {
	protocols := "file"
	if strings.HasPrefix(input, "http://") {
		protocols = "http,tcp"
	}
	return []string{"-protocol_whitelist", protocols, "-format_whitelist", inputFormatAllowlist}
}
