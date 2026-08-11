package tams

import (
	"testing"
)

func TestParseAPIVersionRequiresExactSchemaForm(t *testing.T) {
	t.Parallel()
	for _, version := range []string{"0.0", "8.1", "8.10", "10.0"} {
		parsed, err := ParseAPIVersion(map[string]any{"api_version": version})
		if err != nil {
			t.Fatalf("ParseAPIVersion(%q): %v", version, err)
		}
		if parsed.String() != version {
			t.Fatalf("ParseAPIVersion(%q) = %q", version, parsed.String())
		}
	}

	for _, version := range []string{
		"+8.1", " 8.1", "8.1 ", "08.1", "8.01", "8", "8.1.0", "-8.1", "8.-1",
	} {
		if _, err := ParseAPIVersion(map[string]any{"api_version": version}); err == nil {
			t.Fatalf("ParseAPIVersion(%q) unexpectedly succeeded", version)
		}
	}
}
