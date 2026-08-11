package source

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestValidateCompleteRange(t *testing.T) {
	t.Parallel()

	valid, err := validateCompleteRange("bytes 10-27/28", 10, 28, 18)
	if err != nil {
		t.Fatal(err)
	}
	if valid != (byteContentRange{start: 10, end: 27, total: 28}) {
		t.Fatalf("range = %#v", valid)
	}

	for _, testCase := range []struct {
		name          string
		value         string
		offset        int64
		knownTotal    int64
		contentLength int64
	}{
		{name: "missing", value: "", offset: 10, knownTotal: 28, contentLength: 18},
		{name: "signed start", value: "bytes +10-27/28", offset: 10, knownTotal: 28, contentLength: 18},
		{name: "signed zero start", value: "bytes -0-27/28", offset: 0, knownTotal: 28, contentLength: 28},
		{name: "unknown total", value: "bytes 10-27/*", offset: 10, knownTotal: 28, contentLength: 18},
		{name: "wrong start", value: "bytes 9-27/28", offset: 10, knownTotal: 28, contentLength: 19},
		{name: "changed total", value: "bytes 10-28/29", offset: 10, knownTotal: 28, contentLength: 19},
		{name: "not the final span", value: "bytes 10-20/28", offset: 10, knownTotal: 28, contentLength: 11},
		{name: "end equals total", value: "bytes 10-28/28", offset: 10, knownTotal: 28, contentLength: 19},
		{name: "end beyond total", value: "bytes 10-29/28", offset: 10, knownTotal: 28, contentLength: 20},
		{name: "declared body too short", value: "bytes 10-27/28", offset: 10, knownTotal: 28, contentLength: 17},
		{name: "declared body too long", value: "bytes 10-27/28", offset: 10, knownTotal: 28, contentLength: 19},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if _, err := validateCompleteRange(testCase.value, testCase.offset, testCase.knownTotal, testCase.contentLength); err == nil {
				t.Fatalf("validateCompleteRange(%q) unexpectedly succeeded", testCase.value)
			}
		})
	}
}

func TestParseUnsatisfiedContentRange(t *testing.T) {
	t.Parallel()
	total, err := parseUnsatisfiedContentRange("bytes */28")
	if err != nil || total != 28 {
		t.Fatalf("total = %d, error = %v", total, err)
	}
	for _, value := range []string{"", "bytes 28-28/28", "bytes */*", "items */28", "bytes */-1", "bytes */+28"} {
		if _, err := parseUnsatisfiedContentRange(value); err == nil {
			t.Fatalf("parseUnsatisfiedContentRange(%q) unexpectedly succeeded", value)
		}
	}
}

func TestValidStrongETag(t *testing.T) {
	t.Parallel()
	for _, value := range []string{`"v1"`, `"opaque!#$%&'()*+,-./:;<=>?@[]^_{}~"`} {
		if !validStrongETag(value) {
			t.Errorf("validStrongETag(%q) = false", value)
		}
	}
	for _, value := range []string{"", "v1", `W/"v1"`, `"has space"`, `"broken"quote"`} {
		if validStrongETag(value) {
			t.Errorf("validStrongETag(%q) = true", value)
		}
	}
}

func TestExactRangeBody(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name    string
		body    string
		length  int64
		want    string
		wantErr error
	}{
		{name: "exact", body: "media", length: 5, want: "media"},
		{name: "short", body: "med", length: 5, want: "med", wantErr: io.ErrUnexpectedEOF},
		{name: "long", body: "media-extra", length: 5, want: "media", wantErr: errors.New("too long")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			body := exactRangeBody(io.NopCloser(strings.NewReader(testCase.body)), testCase.length)
			got, err := io.ReadAll(body)
			_ = body.Close()
			if string(got) != testCase.want {
				t.Fatalf("body = %q, want %q", got, testCase.want)
			}
			switch {
			case testCase.wantErr == nil && err != nil:
				t.Fatalf("ReadAll() error = %v", err)
			case errors.Is(testCase.wantErr, io.ErrUnexpectedEOF) && !errors.Is(err, io.ErrUnexpectedEOF):
				t.Fatalf("ReadAll() error = %v, want unexpected EOF", err)
			case testCase.wantErr != nil && err == nil:
				t.Fatal("ReadAll() unexpectedly succeeded")
			}
		})
	}
}
