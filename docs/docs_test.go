// Package docs tests the documentation against the binary it describes.
//
// Documentation drifts silently: a renamed flag or moved page breaks a reader's
// copy-and-paste long before anyone notices. These tests make that drift a
// build failure instead.
package docs

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
)

// repoRoot is the directory above this package.
const repoRoot = ".."

var (
	testBinaryDirectory string
	buildTamsinBinary   = sync.OnceValues(func() (string, error) {
		binary := filepath.Join(testBinaryDirectory, "tamsin")
		build := exec.Command("go", "build", "-o", binary, "./cmd/tamsin")
		build.Dir = repoRoot
		if output, err := build.CombinedOutput(); err != nil {
			return "", fmt.Errorf("build tamsin: %w\n%s", err, output)
		}
		return binary, nil
	})
)

func TestMain(m *testing.M) {
	directory, err := os.MkdirTemp("", "tamsin-docs-test-")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "create documentation test directory: %v\n", err)
		os.Exit(1)
	}
	testBinaryDirectory = directory
	code := m.Run()
	if err := os.RemoveAll(directory); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "remove documentation test directory: %v\n", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

func markdownFiles(t *testing.T) []string {
	t.Helper()
	var files []string
	err := filepath.Walk(repoRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Vendored schemas and build output are not documentation.
		if info.IsDir() && (info.Name() == ".git" || info.Name() == ".cache" || info.Name() == "dist" ||
			info.Name() == "bin" || info.Name() == "schemas" || info.Name() == "testdata") {
			return filepath.SkipDir
		}
		if !info.IsDir() && strings.HasSuffix(path, ".md") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repository: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no documentation found")
	}
	return files
}

var linkPattern = regexp.MustCompile(`\[[^\]]*\]\(([^)]+)\)`)

// TestDocumentationLinksResolve catches a moved or renamed page, which is the
// most common way restructured documentation breaks.
func TestDocumentationLinksResolve(t *testing.T) {
	t.Parallel()
	for _, file := range markdownFiles(t) {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, match := range linkPattern.FindAllStringSubmatch(string(body), -1) {
			target := match[1]
			// Only relative links point at things in this repository.
			if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") ||
				strings.HasPrefix(target, "#") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			target, _, _ = strings.Cut(target, "#")
			if target == "" {
				continue
			}
			resolved := filepath.Join(filepath.Dir(file), target)
			if _, err := os.Stat(resolved); err != nil {
				t.Errorf("%s links to %q, which does not exist", file, match[1])
			}
		}
	}
}

var productNamePattern = regexp.MustCompile(`\bT[Aa][Mm][Ss][Ii][Nn]\b`)
var nonBritishProsePattern = regexp.MustCompile(`(?i)\b(?:artifact|artifacts|behavior|behaviors|canceled|canceling|customize|customized|normalize|normalized|organize|organized|recognize|recognized|sanitize|sanitized|serialize|serialized|serializer)\b`)

// TestProductNameSpelling keeps the prose name distinct from literal lowercase
// command, module, and protocol identifiers.
func TestProductNameSpelling(t *testing.T) {
	t.Parallel()
	for _, file := range markdownFiles(t) {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, spelling := range productNamePattern.FindAllString(string(body), -1) {
			if spelling != "TAMSin" {
				t.Errorf("%s uses product spelling %q; use TAMSin in prose", file, spelling)
			}
		}
	}
}

// TestDocumentationUsesBritishEnglish catches common American spellings in
// prose. Literal upstream names such as OAuth "authorization", the `--color`
// flag, and the Apache License retain their defined spelling.
func TestDocumentationUsesBritishEnglish(t *testing.T) {
	t.Parallel()
	for _, file := range markdownFiles(t) {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, spelling := range nonBritishProsePattern.FindAllString(string(body), -1) {
			t.Errorf("%s uses American spelling %q in prose", file, spelling)
		}
	}
}

// TestDocumentationMapIsComplete ensures every reader-facing page has a route
// from the documentation landing page. Decision records are represented there
// by their directory because they form one indexed series.
func TestDocumentationMapIsComplete(t *testing.T) {
	t.Parallel()
	indexPath := filepath.Join(repoRoot, "docs", "README.md")
	index, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("read documentation map: %v", err)
	}

	for _, section := range []string{"tutorials", "how-to", "reference", "explanation"} {
		pages, err := filepath.Glob(filepath.Join(repoRoot, "docs", section, "*.md"))
		if err != nil {
			t.Fatalf("list %s pages: %v", section, err)
		}
		for _, page := range pages {
			target := filepath.ToSlash(filepath.Join(section, filepath.Base(page)))
			if !strings.Contains(string(index), target) {
				t.Errorf("docs/README.md does not link reader-facing page %s", target)
			}
		}
	}
}

// tamsinBinary builds the CLI once so the tests below assert against the real
// flag set rather than a copy of it.
func tamsinBinary(t *testing.T) string {
	t.Helper()
	binary, err := buildTamsinBinary()
	if err != nil {
		t.Fatal(err)
	}
	return binary
}

func helpText(t *testing.T, binary string, args ...string) string {
	t.Helper()
	command := exec.Command(binary, append(args, "--help")...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("tamsin %v --help: %v\n%s", args, err, output)
	}
	return string(output)
}

var documentedFlag = regexp.MustCompile(`--[a-z][a-z0-9-]+`)

// TestDocumentedFlagsExist fails when documentation names a flag the binary
// does not have, which is what a rename leaves behind.
func TestDocumentedFlagsExist(t *testing.T) {
	t.Parallel()
	binary := tamsinBinary(t)
	known := helpText(t, binary)
	for _, command := range discoverCommands(t, binary) {
		known += helpText(t, binary, command...)
	}

	// Flags belonging to other tools appear in documented examples; they are not
	// TAMSin's to define.
	foreign := map[string]bool{
		"--entrypoint": true, "--rm": true, "--network": true, "--no-verify-ssl": true,
		"--endpoint-url": true, "--source": true, "--config": true, "--exit-code": true,
		"--redact": true, "--no-git": true, "--update": true, "--cover": true,
		"--coverpkg": true, "--name": true, "--kubeconfig": true, "--force-with-lease": true,
	}

	for _, file := range markdownFiles(t) {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		// Only inspect lines that invoke tamsin.
		for _, line := range strings.Split(string(body), "\n") {
			if !strings.Contains(line, "tamsin ") {
				continue
			}
			for _, flag := range documentedFlag.FindAllString(line, -1) {
				if foreign[flag] || strings.Contains(known, flag) {
					continue
				}
				t.Errorf("%s documents %s, which tamsin does not accept:\n  %s",
					file, flag, strings.TrimSpace(line))
			}
		}
	}
}

// TestDocumentedEnvironmentKeysExist checks the TAMSIN_* names documentation
// tells readers to export. A key that no longer binds is silently ignored at
// runtime, so nothing else would catch this.
func TestDocumentedEnvironmentKeysExist(t *testing.T) {
	t.Parallel()
	// Derived from the configuration reference, which is the contract for these.
	reference, err := os.ReadFile(filepath.Join(repoRoot, "docs", "reference", "configuration.md"))
	if err != nil {
		t.Fatalf("read configuration reference: %v", err)
	}
	documented := regexp.MustCompile(`TAMSIN_[A-Z0-9_]+`)
	known := make(map[string]bool)
	for _, key := range documented.FindAllString(string(reference), -1) {
		known[key] = true
	}
	if len(known) == 0 {
		t.Fatal("configuration reference documents no environment keys")
	}

	for _, file := range markdownFiles(t) {
		if strings.HasSuffix(file, filepath.Join("reference", "configuration.md")) {
			continue
		}
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, key := range documented.FindAllString(string(body), -1) {
			if !known[key] {
				t.Errorf("%s tells the reader to set %s, which the configuration reference does not define", file, key)
			}
		}
	}
}

// helpEntry matches the first line of a flag's entry in pflag's help output.
// Continuation lines are indented further and are folded into the entry above.
var helpEntry = regexp.MustCompile(`^\s{2,7}(?:-[a-zA-Z], )?--([a-z][a-z0-9-]*)`)

// helpFlags parses help output into flag name to the whole of its entry.
func helpFlags(help string) map[string]string {
	flags := make(map[string]string)
	current := ""
	for _, line := range strings.Split(help, "\n") {
		if match := helpEntry.FindStringSubmatch(line); match != nil {
			current = match[1]
			flags[current] = strings.TrimSpace(line)
			continue
		}
		if current != "" && strings.HasPrefix(line, "        ") {
			flags[current] += " " + strings.TrimSpace(line)
			continue
		}
		current = ""
	}
	return flags
}

var statedDefault = regexp.MustCompile(`\(default [^)]*\)`)

// TestCLIReferenceDescribesEveryFlag covers the direction the other flag test
// does not.
//
// Checking that documented flags exist catches a rename, and says nothing about
// a flag that was added and never written down, or a default that moved
// underneath its description. Reference documentation is trusted precisely
// because nobody re-derives it from the binary, so it is worth failing a build
// over.
func TestCLIReferenceDescribesEveryFlag(t *testing.T) {
	t.Parallel()
	binary := tamsinBinary(t)
	reference, err := os.ReadFile(filepath.Join(repoRoot, "docs", "reference", "cli.md"))
	if err != nil {
		t.Fatal(err)
	}
	documented := string(reference)

	for _, section := range []struct {
		name        string
		args        []string
		helpSection string
		docSection  string
	}{
		{name: "ingest", args: []string{"ingest"}, helpSection: "Flags:", docSection: "## Ingest flags"},
		{name: "doctor", args: []string{"doctor"}, helpSection: "Flags:", docSection: "## Doctor flags"},
		{name: "global", args: []string{"api"}, helpSection: "Global Flags:", docSection: "## Global flags"},
	} {
		expected := helpFlagsInSection(helpText(t, binary, section.args...), section.helpSection)
		docBody := markdownSection(documented, section.docSection)
		for flag, entry := range expected {
			row := documentedRow(docBody, flag)
			if row == "" {
				t.Errorf("%s flag --%s is not in the CLI reference: %s", section.name, flag, entry)
				continue
			}
			// A default is the part most likely to drift, because changing one
			// touches no documentation and breaks no test.
			stated := statedDefault.FindString(entry)
			if stated == "" {
				continue
			}
			if !strings.Contains(row, stated) {
				t.Errorf("CLI reference gives --%s as %q, but the binary says %q", flag, row, stated)
			}
		}
		for flag := range documentedFlags(docBody) {
			if expected[flag] == "" {
				t.Errorf("%s reference still lists --%s, which is not a %s flag", section.name, flag, section.name)
			}
		}
	}

	rootOnly := markdownSection(documented, "## Root-only flag")
	version := helpFlags(helpText(t, binary))["version"]
	row := documentedRow(rootOnly, "version")
	if version == "" || row == "" {
		t.Errorf("root-only --version contract is incomplete: help=%q row=%q", version, row)
	}
}

// TestCLIReferenceDescribesEveryCommandAndScopedFlag walks Cobra's published
// command tree instead of keeping a second command list in the test. The
// reference therefore fails closed when a new leaf or leaf-only flag is added,
// including when every existing documentation example keeps working.
func TestCLIReferenceDescribesEveryCommandAndScopedFlag(t *testing.T) {
	t.Parallel()
	binary := tamsinBinary(t)
	reference, err := os.ReadFile(filepath.Join(repoRoot, "docs", "reference", "cli.md"))
	if err != nil {
		t.Fatal(err)
	}
	documented := string(reference)
	discovered := discoverCommands(t, binary)
	wantCommands := make(map[string]bool, len(discovered))
	wantScopedFlags := make(map[string]bool)
	for _, command := range discovered {
		path := strings.Join(command, " ")
		wantCommands[path] = true
		if !strings.Contains(documented, "| `tamsin "+path+"` |") {
			t.Errorf("CLI reference has no command row for tamsin %s", path)
		}

		if path == "ingest" || path == "doctor" {
			// Product commands have complete dedicated tables above rather than a
			// row per flag in the smaller command-specific table.
			continue
		}
		for flag, entry := range helpFlagsInSection(helpText(t, binary, command...), "Flags:") {
			if flag == "help" {
				continue
			}
			wantScopedFlags[path+"\x00"+flag] = true
			row := documentedScopedRow(documented, path, flag)
			if row == "" {
				t.Errorf("tamsin %s flag --%s is not in the command-specific reference: %s", path, flag, entry)
				continue
			}
			if stated := statedDefault.FindString(entry); stated != "" && !strings.Contains(row, stated) {
				t.Errorf("CLI reference gives tamsin %s --%s as %q, but the binary says %q", path, flag, row, stated)
			}
		}
	}

	// A removed command should not leave a plausible-looking reference row.
	commandRow := regexp.MustCompile("(?m)^\\| `tamsin ([^`]+)` \\|")
	for _, match := range commandRow.FindAllStringSubmatch(documented, -1) {
		if !wantCommands[match[1]] {
			t.Errorf("CLI reference still lists removed command tamsin %s", match[1])
		}
	}
	scopedRow := regexp.MustCompile("(?m)^\\| `([^`]+)` \\| `--([^`]+)` \\|")
	for _, match := range scopedRow.FindAllStringSubmatch(markdownSection(documented, "## Command-specific flags"), -1) {
		if !wantScopedFlags[match[1]+"\x00"+match[2]] {
			t.Errorf("CLI reference still lists removed or wrongly scoped flag tamsin %s --%s", match[1], match[2])
		}
	}
}

func discoverCommands(t *testing.T, binary string) [][]string {
	t.Helper()
	var commands [][]string
	queue := [][]string{nil}
	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]
		for _, child := range helpSubcommands(helpText(t, binary, parent...)) {
			path := append(append([]string(nil), parent...), child)
			commands = append(commands, path)
			queue = append(queue, path)
		}
	}
	return commands
}

func helpSubcommands(help string) []string {
	var commands []string
	inCommands := false
	for _, line := range strings.Split(help, "\n") {
		if line == "Available Commands:" {
			inCommands = true
			continue
		}
		if !inCommands {
			continue
		}
		if strings.TrimSpace(line) == "" {
			if len(commands) > 0 {
				break
			}
			continue
		}
		if !strings.HasPrefix(line, "  ") {
			break
		}
		fields := strings.Fields(line)
		if len(fields) > 0 {
			commands = append(commands, fields[0])
		}
	}
	return commands
}

func helpFlagsInSection(help, section string) map[string]string {
	lines := strings.Split(help, "\n")
	start := -1
	for index, line := range lines {
		if line == section {
			start = index + 1
			break
		}
	}
	if start < 0 {
		return nil
	}
	var body strings.Builder
	for _, line := range lines[start:] {
		if line != "" && !strings.HasPrefix(line, " ") {
			break
		}
		body.WriteString(line)
		body.WriteByte('\n')
	}
	return helpFlags(body.String())
}

func documentedScopedRow(reference, command, flag string) string {
	prefix := "| `" + command + "` | `--" + flag + "` |"
	for _, line := range strings.Split(reference, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), prefix) {
			return line
		}
	}
	return ""
}

func markdownSection(document, heading string) string {
	start := strings.Index(document, heading+"\n")
	if start < 0 {
		return ""
	}
	section := document[start+len(heading)+1:]
	if end := strings.Index(section, "\n## "); end >= 0 {
		section = section[:end]
	}
	return section
}

func documentedFlags(section string) map[string]bool {
	flags := make(map[string]bool)
	for _, line := range strings.Split(section, "\n") {
		match := documentedFlag.FindString(line)
		if strings.HasPrefix(strings.TrimSpace(line), "| `--") && match != "" {
			flags[strings.TrimPrefix(match, "--")] = true
		}
	}
	return flags
}

// The commit links are intentionally useful to a reader, but their values
// still come from the machine-readable pin. This catches a contract bump that
// otherwise leaves the conformance page linking to the service reviewed last
// time.
func TestConformancePageUsesThePinnedRevisions(t *testing.T) {
	t.Parallel()
	targetData, err := os.ReadFile(filepath.Join(repoRoot, "contracts", "tams-v8.2.json"))
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
	if err := json.Unmarshal(targetData, &contract); err != nil {
		t.Fatal(err)
	}
	page, err := os.ReadFile(filepath.Join(repoRoot, "docs", "explanation", "conformance.md"))
	if err != nil {
		t.Fatal(err)
	}
	for label, revision := range map[string]string{"TAMS": contract.TAMS.Commit, "TAMOSS": contract.TAMOSS.Commit} {
		if revision == "" || !strings.Contains(string(page), revision) {
			t.Errorf("conformance page does not name the pinned %s revision %q", label, revision)
		}
	}

	compatibilityData, err := os.ReadFile(filepath.Join(repoRoot, "contracts", "tams-v8.1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var compatibility struct {
		TAMS struct {
			Commit string `json:"commit"`
		} `json:"tams"`
	}
	if err := json.Unmarshal(compatibilityData, &compatibility); err != nil {
		t.Fatal(err)
	}
	allowedRevisions := map[string]bool{
		contract.TAMS.Commit:      true,
		compatibility.TAMS.Commit: true,
	}
	tamsRevisionLink := regexp.MustCompile(`github\.com/bbc/tams/(?:blob|commit)/([^/)]+)`)
	for _, file := range markdownFiles(t) {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range tamsRevisionLink.FindAllStringSubmatch(string(body), -1) {
			if !allowedRevisions[match[1]] {
				t.Errorf("%s links to unpinned TAMS revision %q", file, match[1])
			}
		}
	}
}

// This is the prose side of the executable ingest policy tests. It keeps the
// copy-and-paste dry run, its three-Flow interpretation, and the exact-byte
// preservation escape hatch together rather than letting one paragraph drift.
func TestFirstIngestTutorialStatesTheExecutableMediaContract(t *testing.T) {
	t.Parallel()
	page, err := os.ReadFile(filepath.Join(repoRoot, "docs", "tutorials", "first-ingest.md"))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(page)
	for _, required := range []string{
		"tamsin --profile essence-segments --dry-run=exact --format json -i first-ingest.ts",
		"**three** entries", `"role": "video"`, `"role": "audio"`, "root_flow_id",
		"tamsin --profile preserve", "--essence-storage muxed --segment-duration 0",
	} {
		if !strings.Contains(contents, required) {
			t.Errorf("first-ingest tutorial no longer states %q", required)
		}
	}
}

// documentedRow returns the reference's table row for a flag, if it has one.
func documentedRow(reference, flag string) string {
	for _, line := range strings.Split(reference, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "| `--"+flag+"`") {
			return line
		}
	}
	return ""
}
