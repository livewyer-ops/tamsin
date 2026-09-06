package contracts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestFormatCheckIncludesExternalPackageTests(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	for name, body := range map[string]string{
		"go.mod":          "module formatcheck\n\ngo 1.26.8\n",
		"example.go":      "package example\n",
		"example_test.go": "package example_test\n\nimport \"testing\"\n\nfunc TestExternal(t *testing.T){ }\n",
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	script, err := filepath.Abs(filepath.Join(repositoryRoot, "scripts/check-format.sh"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("bash", script)
	command.Dir = directory
	if output, err := command.CombinedOutput(); err == nil || !strings.Contains(string(output), "example_test.go") {
		t.Fatalf("external test formatting was not checked: %v\n%s", err, output)
	}
	if output, err := exec.Command("gofmt", "-w", filepath.Join(directory, "example_test.go")).CombinedOutput(); err != nil {
		t.Fatalf("format fixture: %v\n%s", err, output)
	}
	command = exec.Command("bash", script)
	command.Dir = directory
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("formatted fixture failed: %v\n%s", err, output)
	}
}

func TestFormatCheckValidatesEveryScript(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	if err := os.Mkdir(filepath.Join(directory, "scripts"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"check-format.sh": "#!/bin/sh\nexit 0\n",
		"first.sh":        "true\n",
		"last.sh":         "true\n",
		"first.py":        "pass\n",
		"last.py":         "pass\n",
	} {
		if err := os.WriteFile(filepath.Join(directory, "scripts", name), []byte(body), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	makefile, err := filepath.Abs(filepath.Join(repositoryRoot, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	for _, filename := range []string{"last.sh", "last.py"} {
		for _, valid := range []bool{false, true} {
			body := "if then\n"
			if valid {
				body = "true\n"
			}
			if err := os.WriteFile(filepath.Join(directory, "scripts", filename), []byte(body), 0o700); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("make", "-f", makefile, "format-check")
			command.Dir = directory
			output, err := command.CombinedOutput()
			if (err == nil) != valid {
				t.Fatalf("%s valid=%v: %v\n%s", filename, valid, err, output)
			}
		}
	}
}
