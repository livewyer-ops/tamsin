package docs

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const repoRoot = ".."

func markdownFiles(t *testing.T) []string {
	t.Helper()
	var files []string
	err := filepath.Walk(repoRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && (info.Name() == ".git" || info.Name() == ".cache" || info.Name() == ".tmp" || info.Name() == "dist" || info.Name() == "bin" || info.Name() == "testdata") {
			return filepath.SkipDir
		}
		if !info.IsDir() && strings.HasSuffix(path, ".md") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestDocumentationLinksResolve(t *testing.T) {
	t.Parallel()
	links := regexp.MustCompile(`\[[^\]]*\]\(([^)]+)\)`)
	for _, file := range markdownFiles(t) {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range links.FindAllStringSubmatch(string(body), -1) {
			target, _, _ := strings.Cut(match[1], "#")
			if target == "" || strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			if _, err := os.Stat(filepath.Join(filepath.Dir(file), target)); err != nil {
				t.Errorf("%s links to missing %q", file, match[1])
			}
		}
	}
}

func TestDocumentedTAMSinFlagsExist(t *testing.T) {
	t.Parallel()
	binary := filepath.Join(t.TempDir(), "tamsin")
	command := exec.Command("go", "build", "-o", binary, "./cmd/tamsin")
	command.Dir = repoRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build tamsin: %v\n%s", err, output)
	}
	var help strings.Builder
	for _, args := range [][]string{{"--help"}, {"ingest", "--help"}, {"doctor", "--help"}, {"profiles", "--help"}} {
		output, err := exec.Command(binary, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("help %v: %v\n%s", args, err, output)
		}
		help.Write(output)
	}
	foreign := map[string]bool{
		"--entrypoint": true, "--rm": true, "--network": true, "--no-verify-ssl": true,
		"--endpoint-url": true, "--source": true, "--exit-code": true,
		"--update": true, "--cover": true, "--coverpkg": true,
	}
	flags := regexp.MustCompile(`--[a-z][a-z0-9-]+`)
	for _, file := range markdownFiles(t) {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(body), "\n") {
			if !strings.Contains(line, "tamsin ") {
				continue
			}
			for _, flag := range flags.FindAllString(line, -1) {
				if !foreign[flag] && !strings.Contains(help.String(), flag) {
					t.Errorf("%s documents unknown %s in %q", file, flag, strings.TrimSpace(line))
				}
			}
		}
	}
}

func TestConformancePageUsesPinnedRevisions(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(filepath.Join(repoRoot, "contracts", "tams-v8.2.json"))
	if err != nil {
		t.Fatal(err)
	}
	var contract struct {
		TAMS struct {
			Commit string `json:"commit"`
		} `json:"tams"`
		TAMOSS struct {
			Commit string `json:"commit"`
		} `json:"tamoss"`
	}
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatal(err)
	}
	page, err := os.ReadFile(filepath.Join(repoRoot, "docs", "explanation", "conformance.md"))
	if err != nil {
		t.Fatal(err)
	}
	for label, revision := range map[string]string{"TAMS": contract.TAMS.Commit, "TAMOSS": contract.TAMOSS.Commit} {
		if revision == "" || !strings.Contains(string(page), revision) {
			t.Errorf("conformance page does not name pinned %s revision %q", label, revision)
		}
	}
}
