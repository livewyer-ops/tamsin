package contracts

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

const repositoryRoot = ".."

func repositoryFile(t *testing.T, name string) string {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(repositoryRoot, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(contents)
}

type workflowStep struct {
	ID, Uses, Run string
}

type workflowJob struct {
	If, Uses    string
	Needs       yaml.Node
	Permissions map[string]string
	Strategy    struct{ Matrix map[string][]string }
	Steps       []workflowStep
}

type workflow struct {
	On   struct{ Push struct{ Branches []string } }
	Jobs map[string]workflowJob
}

func readWorkflow(t *testing.T, filename string) workflow {
	t.Helper()
	var value workflow
	if err := yaml.Unmarshal([]byte(repositoryFile(t, ".github/workflows/"+filename)), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func stepByID(t *testing.T, job workflowJob, id string) (int, workflowStep) {
	t.Helper()
	for index, step := range job.Steps {
		if step.ID == id {
			return index, step
		}
	}
	t.Fatalf("missing step %q", id)
	return -1, workflowStep{}
}

func requireText(t *testing.T, body string, values ...string) {
	t.Helper()
	for _, value := range values {
		if !strings.Contains(body, value) {
			t.Errorf("expected text %q", value)
		}
	}
}

func TestReleaseRunsVerificationAndE2EBeforePublishing(t *testing.T) {
	t.Parallel()
	workflow := readWorkflow(t, "release.yml")
	verify, e2e, publish := workflow.Jobs["verify"], workflow.Jobs["e2e"], workflow.Jobs["publish"]
	var dependencies []string
	if err := publish.Needs.Decode(&dependencies); err != nil || !slices.Equal(dependencies, []string{"verify", "e2e"}) ||
		e2e.Needs.Value != "verify" || e2e.Uses != "./.github/workflows/e2e.yml" {
		t.Fatal("publication must depend on verification and both live gates")
	}
	if !slices.ContainsFunc(verify.Steps, func(step workflowStep) bool { return step.Run == "make verify" }) {
		t.Fatal("release verification does not run make verify")
	}
	for _, job := range []workflowJob{verify, e2e} {
		for _, permission := range job.Permissions {
			if permission == "write" {
				t.Fatal("a pre-publication job has write permissions")
			}
		}
	}
	buildIndex, _ := stepByID(t, publish, "image")
	guardIndex, _ := stepByID(t, publish, "immutable-tag")
	smokeIndex, _ := stepByID(t, publish, "image-smoke")
	tagIndex, _ := stepByID(t, publish, "image-tags")
	if guardIndex >= buildIndex || buildIndex >= smokeIndex || smokeIndex >= tagIndex {
		t.Fatal("release publication must check tag absence, build a candidate and smoke it before assigning tags")
	}
}

func TestE2ECoversBothSupportedTAMSVersions(t *testing.T) {
	t.Parallel()
	workflow := readWorkflow(t, "e2e.yml")
	if !slices.Equal(workflow.Jobs["kind"].Strategy.Matrix["contract"], []string{"tams-v8.1.json", "tams-v8.2.json"}) {
		t.Fatal("live matrix must cover both supported contracts")
	}
	requireText(t, repositoryFile(t, "scripts/e2e-kind.sh"),
		"--tams-flow-profile", "service/profiles/$profile_id")
}

func TestRuntimePublicationFailsClosed(t *testing.T) {
	t.Parallel()
	workflow := readWorkflow(t, "ffmpeg-runtime.yml")
	job := workflow.Jobs["publish"]
	if !slices.Equal(workflow.On.Push.Branches, []string{"main"}) ||
		job.If != "github.repository == 'livewyer-ops/tamsin' && github.ref == 'refs/heads/main'" {
		t.Fatal("runtime publication must be restricted to main in the public repository")
	}
	_, _ = stepByID(t, job, "immutable-tag")
}

func TestVersionedImageTagsAreNotOverwritten(t *testing.T) {
	for _, filename := range []string{"ffmpeg-runtime.yml", "release.yml"} {
		t.Run(filename, func(t *testing.T) {
			_, check := stepByID(t, readWorkflow(t, filename).Jobs["publish"], "immutable-tag")
			checkImageTagAbsence(t, check.Run)
		})
	}
}

func checkImageTagAbsence(t *testing.T, script string) {
	t.Helper()
	for _, test := range []struct {
		name, output, status string
		allowed              bool
	}{
		{"absent", "ERROR: ghcr.io/example/app:1.0.0: not found", "1", true},
		{"exists", "Name: ghcr.io/example/app:1.0.0", "0", false},
		{"unauthorised", "ERROR: unexpected status: 401 Unauthorized", "1", false},
		{"unavailable", "ERROR: request timed out", "1", false},
		{"ambiguous absence", "ERROR: some other resource: not found", "1", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := runWorkflowShell(t, script, "printf '%s\\n' \"$LOOKUP_OUTPUT\" >&2\nexit \"$LOOKUP_STATUS\"\n",
				"RUNTIME_IMAGE=ghcr.io/example/app:1.0.0", "GITHUB_REPOSITORY=Example/App", "VERSION=1.0.0",
				"LOOKUP_OUTPUT="+test.output, "LOOKUP_STATUS="+test.status)
			if (err == nil) != test.allowed {
				t.Fatalf("tag check error=%v, allowed=%v", err, test.allowed)
			}
		})
	}
}

func TestReleaseImageSmokeAndTags(t *testing.T) {
	t.Parallel()
	job := readWorkflow(t, "release.yml").Jobs["publish"]
	_, smoke := stepByID(t, job, "image-smoke")
	for _, failure := range []string{"", "linux/amd64", "linux/arm64"} {
		_, err := runWorkflowShell(t, smoke.Run, "case \"$*\" in *\"$FAIL_PLATFORM\"*) [ -z \"$FAIL_PLATFORM\" ] || exit 1;; esac\ncase \"$*\" in *inspect*) echo 65532:65532;; esac\n",
			"IMAGE=example/app@sha256:test", "FAIL_PLATFORM="+failure)
		if (err == nil) != (failure == "") {
			t.Fatalf("smoke failure=%q, error=%v", failure, err)
		}
	}
	_, tags := stepByID(t, job, "image-tags")
	for _, version := range []string{"1.0.0-rc.4", "1.0.0"} {
		output, err := runWorkflowShell(t, tags.Run, "printf '%s\\n' \"$@\"\n", "IMAGE=example/app", "DIGEST=sha256:test", "VERSION="+version)
		if err != nil {
			t.Fatal(err)
		}
		requireText(t, output, "imagetools\ncreate", "example/app:"+version, "example/app@sha256:test")
		if strings.Contains(output, "example/app:latest") != (version == "1.0.0") {
			t.Fatalf("incorrect moving tags: %s", output)
		}
	}
}

func TestImageBuildExportsDigest(t *testing.T) {
	for _, build := range []struct{ workflow, step string }{
		{"ffmpeg-runtime.yml", "runtime"}, {"release.yml", "image"},
	} {
		t.Run(build.workflow, func(t *testing.T) {
			_, step := stepByID(t, readWorkflow(t, build.workflow).Jobs["publish"], build.step)
			for _, test := range []struct {
				name, metadata string
				valid          bool
			}{
				{"valid", `{"containerimage.digest":"sha256:test"}`, true},
				{"missing", `{}`, false},
				{"malformed", `{`, false},
			} {
				t.Run(test.name, func(t *testing.T) {
					outputPath := filepath.Join(t.TempDir(), "output")
					output, err := runWorkflowShell(t, "git() { printf '1970-01-01T00:00:00Z\\n'; }\n"+step.Run,
						"printf '%s\\n' \"$BUILD_METADATA\" > \"$METADATA_FILE\"\n",
						"BUILD_METADATA="+test.metadata, "METADATA_FILE=.tmp/"+build.step+"-metadata.json",
						"GITHUB_OUTPUT="+outputPath, "GITHUB_SHA=HEAD", "GITHUB_REPOSITORY=example/app",
						"GITHUB_RUN_ID=1", "GITHUB_RUN_ATTEMPT=1", "RUNTIME_IMAGE=example/runtime:test")
					if (err == nil) != test.valid {
						t.Fatalf("build error=%v, valid=%v\n%s", err, test.valid, output)
					}
					if test.valid {
						contents, err := os.ReadFile(outputPath)
						if err != nil || !strings.Contains(string(contents), "digest=sha256:test\n") {
							t.Fatalf("digest not exported: %s (error=%v)", contents, err)
						}
					}
				})
			}
		})
	}
}

func runWorkflowShell(t *testing.T, script, docker string, environment ...string) (string, error) {
	t.Helper()
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "docker"), []byte("#!/bin/sh\n"+docker), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("bash", "-euo", "pipefail", "-c", script)
	command.Dir = directory
	command.Env = append(os.Environ(), append(environment, "PATH="+directory+":"+os.Getenv("PATH"))...)
	output, err := command.CombinedOutput()
	return string(output), err
}

func TestWorkflowShellSyntax(t *testing.T) {
	t.Parallel()
	for _, filename := range []string{"ci.yml", "release.yml", "e2e.yml", "ffmpeg-runtime.yml"} {
		for name, job := range readWorkflow(t, filename).Jobs {
			for _, step := range job.Steps {
				command := exec.Command("bash", "-n")
				command.Stdin = strings.NewReader(step.Run)
				if output, err := command.CombinedOutput(); err != nil {
					t.Errorf("%s/%s: %v\n%s", filename, name, err, output)
				}
			}
		}
	}
}

func TestApplicationImagePinsTheFFmpegRuntimeIndex(t *testing.T) {
	t.Parallel()
	dockerfile := repositoryFile(t, "Dockerfile")
	reference := regexp.MustCompile(`ghcr\.io/livewyer-ops/tamsin-ffmpeg-runtime:[^@[:space:]]+@sha256:[0-9a-f]{64}`).FindString(dockerfile)
	if reference == "" {
		t.Fatal("Dockerfile does not pin the FFmpeg runtime by index digest")
	}
	if !strings.Contains(repositoryFile(t, "Makefile"), reference) {
		t.Fatal("Makefile and Dockerfile use different FFmpeg runtime references")
	}
}

func TestReleaseNotesComeFromDatedChangelogSection(t *testing.T) {
	t.Parallel()
	checker := filepath.Join(repositoryRoot, "scripts", "release-notes.py")
	changelog := filepath.Join(t.TempDir(), "CHANGELOG.md")
	body := "# Changelog\n\n## Unreleased\n\nFuture.\n\n" +
		"## [0.1.0] - 2026-08-10\n\nFirst release.\n\n" +
		"## [0.0.1] - 2026-08-01\n\nEarlier.\n"
	if err := os.WriteFile(changelog, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(checker, "v0.1.0-rc.1", changelog)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("extract notes: %v\n%s", err, output)
	}
	if notes := string(output); !strings.Contains(notes, "Release candidate `v0.1.0-rc.1`") ||
		!strings.Contains(notes, "First release.") || strings.Contains(notes, "Future.") {
		t.Fatalf("unexpected release notes:\n%s", notes)
	}
}
