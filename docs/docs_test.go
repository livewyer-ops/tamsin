package docs

import (
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

func repositoryFile(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(repoRoot, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// headingSlugs mirrors GitHub's anchor generation closely enough for the
// headings used here: lowercase, punctuation other than underscores removed,
// spaces to hyphens.
func headingSlugs(body string) map[string]bool {
	slugs := make(map[string]bool)
	strip := regexp.MustCompile(`[^a-z0-9 _-]`)
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "#") {
			continue
		}
		heading := strings.ToLower(strings.TrimSpace(strings.TrimLeft(line, "#")))
		slugs[strings.ReplaceAll(strip.ReplaceAllString(heading, ""), " ", "-")] = true
	}
	return slugs
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
			target, fragment, _ := strings.Cut(match[1], "#")
			if strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			resolved := file
			if target != "" {
				resolved = filepath.Join(filepath.Dir(file), target)
				if _, err := os.Stat(resolved); err != nil {
					t.Errorf("%s links to missing %q", file, match[1])
					continue
				}
			}
			if fragment == "" || !strings.HasSuffix(resolved, ".md") {
				continue
			}
			page, err := os.ReadFile(resolved)
			if err != nil {
				t.Fatal(err)
			}
			if !headingSlugs(string(page))[fragment] {
				t.Errorf("%s links to missing anchor %q", file, match[1])
			}
		}
	}
}

// Every flag a document shows on a tamsin command line, or lists in the CLI
// and configuration tables, must exist in the installed help.
func TestDocumentedTAMSinFlagsExist(t *testing.T) {
	t.Parallel()
	binary := filepath.Join(t.TempDir(), "tamsin")
	command := exec.Command("go", "build", "-o", binary, "./cmd/tamsin")
	command.Dir = repoRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build tamsin: %v\n%s", err, output)
	}
	var help strings.Builder
	for _, args := range [][]string{{"--help"}, {"ingest", "--help"}, {"doctor", "--help"}, {"profiles", "--help"}, {"completion", "--help"}} {
		output, err := exec.Command(binary, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("help %v: %v\n%s", args, err, output)
		}
		help.Write(output)
	}
	foreign := map[string]bool{
		"--entrypoint": true, "--rm": true, "--network": true, "--no-verify-ssl": true,
		"--endpoint-url": true, "--source": true, "--exit-code": true, "--mount": true,
		"--update": true, "--cover": true, "--coverpkg": true,
	}
	flags := regexp.MustCompile(`--[a-z][a-z0-9-]+`)
	for _, file := range markdownFiles(t) {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		table := strings.HasSuffix(file, "docs/cli.md") || strings.HasSuffix(file, "docs/configuration.md")
		for _, line := range strings.Split(string(body), "\n") {
			if !strings.Contains(line, "tamsin ") && !(table && strings.HasPrefix(line, "| `")) {
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

// The failure-code list is a published interface; the events reference must
// name every code the binary can emit.
func TestEventsReferenceListsEveryFailureCode(t *testing.T) {
	t.Parallel()
	source := repositoryFile(t, filepath.Join("internal", "ingest", "failure.go"))
	events := repositoryFile(t, filepath.Join("docs", "events.md"))
	codes := regexp.MustCompile(`FailureCode[A-Za-z]+\s*=\s*"([a-z_.]+)"`).FindAllStringSubmatch(source, -1)
	if len(codes) < 30 {
		t.Fatalf("found only %d failure codes in failure.go", len(codes))
	}
	for _, match := range codes {
		if !strings.Contains(events, "`"+match[1]+"`") {
			t.Errorf("docs/events.md does not list failure code %q", match[1])
		}
	}
}

// The compatibility page names the exact TAMS revisions the schemas and the
// live test pin, so an upstream bump cannot leave the documentation behind.
func TestCompatibilityPageNamesPinnedTAMSRevisions(t *testing.T) {
	t.Parallel()
	page := repositoryFile(t, filepath.Join("docs", "compatibility.md"))
	revision := regexp.MustCompile(`[0-9a-f]{40}`)
	pins := map[string][]string{
		"internal/tamsschema/flow.go": revision.FindAllString(repositoryFile(t, filepath.Join("internal", "tamsschema", "flow.go")), -1),
	}
	for _, line := range strings.Split(repositoryFile(t, filepath.Join("scripts", "e2e-kind.sh")), "\n") {
		if strings.Contains(line, "TAMS_COMMIT=") {
			pins["scripts/e2e-kind.sh"] = append(pins["scripts/e2e-kind.sh"], revision.FindAllString(line, -1)...)
		}
	}
	for file, revisions := range pins {
		if len(revisions) == 0 {
			t.Fatalf("%s pins no TAMS revision", file)
		}
		for _, pinned := range revisions {
			if !strings.Contains(page, pinned) {
				t.Errorf("docs/compatibility.md does not name the TAMS revision %s pinned in %s", pinned, file)
			}
		}
	}
}
