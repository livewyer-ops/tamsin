package source

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// byteContentRange is the satisfied form of Content-Range. Resumed transfers
// deliberately require a numeric total: without one, there is no way to prove
// that an open-ended range delivered the rest of one complete representation.
type byteContentRange struct {
	start int64
	end   int64
	total int64
}

func parseContentRange(value string) (byteContentRange, error) {
	unit, spec, found := strings.Cut(strings.TrimSpace(value), " ")
	if !found || !strings.EqualFold(unit, "bytes") {
		return byteContentRange{}, fmt.Errorf("server did not state which bytes it sent (Content-Range %q)", value)
	}
	rangeSpec, totalSpec, found := strings.Cut(strings.TrimSpace(spec), "/")
	if !found {
		return byteContentRange{}, fmt.Errorf("malformed Content-Range %q", value)
	}
	startSpec, endSpec, found := strings.Cut(strings.TrimSpace(rangeSpec), "-")
	if !found {
		return byteContentRange{}, fmt.Errorf("malformed Content-Range %q", value)
	}
	start, startErr := parseContentRangeDecimal(startSpec)
	end, endErr := parseContentRangeDecimal(endSpec)
	total, totalErr := parseContentRangeDecimal(totalSpec)
	if startErr != nil || endErr != nil || totalErr != nil || start < 0 || end < start || total <= 0 || end >= total {
		return byteContentRange{}, fmt.Errorf("malformed Content-Range %q", value)
	}
	return byteContentRange{start: start, end: end, total: total}, nil
}

func parseUnsatisfiedContentRange(value string) (int64, error) {
	unit, spec, found := strings.Cut(strings.TrimSpace(value), " ")
	if !found || !strings.EqualFold(unit, "bytes") {
		return 0, fmt.Errorf("malformed unsatisfied Content-Range %q", value)
	}
	rangeSpec, totalSpec, found := strings.Cut(strings.TrimSpace(spec), "/")
	if !found || strings.TrimSpace(rangeSpec) != "*" {
		return 0, fmt.Errorf("malformed unsatisfied Content-Range %q", value)
	}
	total, err := parseContentRangeDecimal(totalSpec)
	if err != nil || total < 0 {
		return 0, fmt.Errorf("malformed unsatisfied Content-Range %q", value)
	}
	return total, nil
}

func parseContentRangeDecimal(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, strconv.ErrSyntax
	}
	for _, digit := range []byte(value) {
		if digit < '0' || digit > '9' {
			return 0, strconv.ErrSyntax
		}
	}
	return strconv.ParseInt(value, 10, 64)
}

// validateCompleteRange checks an answer to Range: bytes=offset-. Its span must
// start at offset and end at the final byte of a stable representation.
func validateCompleteRange(value string, offset, knownTotal, contentLength int64) (byteContentRange, error) {
	contentRange, err := parseContentRange(value)
	if err != nil {
		return byteContentRange{}, err
	}
	if contentRange.start != offset {
		return byteContentRange{}, fmt.Errorf("resumed at byte %d after asking for %d", contentRange.start, offset)
	}
	if knownTotal >= 0 && contentRange.total != knownTotal {
		return byteContentRange{}, fmt.Errorf("resource is now %d bytes, was %d", contentRange.total, knownTotal)
	}
	if contentRange.end != contentRange.total-1 {
		return byteContentRange{}, fmt.Errorf("resumed range ended at byte %d, not the final byte %d",
			contentRange.end, contentRange.total-1)
	}
	span := contentRange.end - contentRange.start + 1
	if contentLength >= 0 && contentLength != span {
		return byteContentRange{}, fmt.Errorf("resumed response declares a %d-byte body for a %d-byte range", contentLength, span)
	}
	return contentRange, nil
}

// validStrongETag accepts the strong entity-tag grammar used by If-Range. A
// date or weak validator can make a useful cache validator, but cannot prove
// byte-for-byte identity strongly enough to append a resumed body.
func validStrongETag(value string) bool {
	if len(value) < 2 || value[0] != '"' || value[len(value)-1] != '"' {
		return false
	}
	for _, character := range []byte(value[1 : len(value)-1]) {
		if character <= 0x20 || character == '"' || character == 0x7f {
			return false
		}
	}
	return true
}

// exactRangeReadCloser turns a clean, short response (or a response with extra
// bytes) into a read error, so staging retries instead of authenticating an
// incomplete or overlong composite.
type exactRangeReadCloser struct {
	body      io.ReadCloser
	remaining int64
}

func exactRangeBody(body io.ReadCloser, length int64) io.ReadCloser {
	return &exactRangeReadCloser{body: body, remaining: length}
}

func (reader *exactRangeReadCloser) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if reader.remaining > 0 {
		if int64(len(buffer)) > reader.remaining {
			buffer = buffer[:reader.remaining]
		}
		read, err := reader.body.Read(buffer)
		reader.remaining -= int64(read)
		if err == io.EOF && reader.remaining > 0 {
			return read, io.ErrUnexpectedEOF
		}
		return read, err
	}

	var probe [1]byte
	read, err := reader.body.Read(probe[:])
	if read > 0 {
		return 0, errors.New("resumed response body exceeds its declared Content-Range")
	}
	return 0, err
}

func (reader *exactRangeReadCloser) Close() error {
	return reader.body.Close()
}
