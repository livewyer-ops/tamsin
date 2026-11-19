package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadCollectedFlowMetadata(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	metadata, err := readCollectedFlowMetadata(write("roles.json", `{"audio 1": {"codec": "audio/x-smpte302m"}, "data": {"tags": {"note": "x"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if metadata["audio 1"]["codec"] != "audio/x-smpte302m" || metadata["data"]["tags"] == nil {
		t.Fatalf("metadata = %#v", metadata)
	}
	if metadata, err = readCollectedFlowMetadata(""); err != nil || metadata != nil {
		t.Fatalf("no file should mean no overrides: %#v, %v", metadata, err)
	}
	for name, content := range map[string]string{
		"array.json":  `[{"codec": "x"}]`,
		"scalar.json": `{"audio": "audio/aac"}`,
		"null.json":   `{"audio": null}`,
		"two.json":    `{"audio": {}} {"video": {}}`,
	} {
		if _, err := readCollectedFlowMetadata(write(name, content)); err == nil {
			t.Errorf("%s accepted: %s", name, content)
		}
	}
	if _, err := readCollectedFlowMetadata(filepath.Join(directory, "missing.json")); err == nil || !strings.Contains(err.Error(), "open Flow metadata") {
		t.Fatalf("missing file error = %v", err)
	}
}
