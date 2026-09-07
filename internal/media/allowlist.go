package media

import "strings"

// inputFormatAllowlist names the demuxers an FFmpeg-family tool may select for
// an input. Every entry reads only the bytes it was given. Playlist,
// concatenation, pattern, package and script demuxers (hls, dash, concat,
// image2, imf, vobsub, lavfi and their relatives) open further resources named
// inside the input, which would let a crafted file reach local files or
// arbitrary network hosts through the media tools. Names absent from a given
// FFmpeg build are ignored by the whitelist match.
var inputFormatAllowlist = strings.Join([]string{
	// Containers TAMSin describes and can remux.
	"mov", "mp4", "m4a", "3gp", "3g2", "mj2", "matroska", "webm", "mpegts", "mxf",
	"mpeg", "mpegvideo", "avi", "asf", "wav", "aiff", "flac", "mp3", "aac", "ac3",
	"eac3", "ogg", "jpeg_pipe", "mjpeg", "j2k_pipe", "png_pipe", "gif", "webp_pipe", "image2pipe",
	// Self-contained containers and elementary streams accepted for preserve@1.
	"dv", "gxf", "flv", "live_flv", "nut", "yuv4mpegpipe", "h264", "hevc", "vvc", "av1", "obu", "m4v",
	"dnxhd", "dirac", "vc1", "cavsvideo", "ivf", "rm", "swf", "wtv", "nsv", "lxf", "mxg", "apng",
	"mlp", "truehd", "dts", "dtshd", "ac4", "loas", "amr", "au", "caf", "w64", "wv", "tta", "ape", "tak",
	"mpc", "mpc8", "oma", "sox", "voc", "spdif", "iff", "nistsphere", "ircam", "sbc", "aptx", "aptx_hd",
	"codec2", "dsf", "wsd",
	// Timed text.
	"srt", "ass", "webvtt", "scc", "stl", "jacosub", "microdvd", "mpl2", "mpsub", "pjs", "realtext",
	"sami", "subviewer", "subviewer1", "vplayer", "lrc", "tedcaptions", "sup",
	// Single still images.
	"bmp_pipe", "tiff_pipe", "exr_pipe", "dpx_pipe", "jpegls_pipe", "jpegxl_pipe", "pam_pipe", "pbm_pipe",
	"pgm_pipe", "pgmyuv_pipe", "ppm_pipe", "pfm_pipe", "phm_pipe", "psd_pipe", "sgi_pipe", "sunrast_pipe",
	"xbm_pipe", "xpm_pipe", "xwd_pipe", "qoi_pipe", "pcx_pipe", "pictor_pipe", "hdr_pipe", "ico",
}, ",")

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
