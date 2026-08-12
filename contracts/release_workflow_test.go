package contracts

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
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

var workflowJob = regexp.MustCompile(`(?m)^  ([a-z][a-z0-9_-]*):\s*$`)

func workflowJobBlock(t *testing.T, workflow, name string) string {
	t.Helper()
	matches := workflowJob.FindAllStringSubmatchIndex(workflow, -1)
	for index, match := range matches {
		if workflow[match[2]:match[3]] != name {
			continue
		}
		end := len(workflow)
		if index+1 < len(matches) {
			end = matches[index+1][0]
		}
		return workflow[match[0]:end]
	}
	t.Fatalf("workflow has no %q job", name)
	return ""
}

func requireText(t *testing.T, body string, values ...string) {
	t.Helper()
	for _, value := range values {
		if !strings.Contains(body, value) {
			t.Errorf("expected text %q", value)
		}
	}
}

func rejectText(t *testing.T, body string, values ...string) {
	t.Helper()
	for _, value := range values {
		if strings.Contains(body, value) {
			t.Errorf("unexpected text %q", value)
		}
	}
}

// TestReleasePublishesOnlyAfterExactArtifactE2E guards job topology rather
// than merely checking that all the right words occur somewhere in the file.
// The tag-triggered jobs before E2E have read-only permissions and contain no
// registry/release mutation; publication receives the same index identity E2E
// accepted.
func TestReleasePublishesOnlyAfterExactArtifactE2E(t *testing.T) {
	t.Parallel()
	workflow := repositoryFile(t, ".github/workflows/release.yml")
	verify := workflowJobBlock(t, workflow, "verify")
	build := workflowJobBlock(t, workflow, "build")
	e2e := workflowJobBlock(t, workflow, "e2e")
	publish := workflowJobBlock(t, workflow, "publish")
	promote := workflowJobBlock(t, workflow, "promote")

	requireText(t, verify,
		"fetch-depth: 0", "git cat-file -t", "BEGIN SSH SIGNATURE",
		"ssh-keygen -Y verify", ".github/release-allowed-signers", "-I release",
		`git merge-base --is-ancestor "$GITHUB_SHA" origin/main`)
	requireText(t, repositoryFile(t, ".github/release-allowed-signers"),
		"release ssh-ed25519 ")

	requireText(t, build,
		"needs: verify", "make dist", `IMAGE_PLATFORMS="linux/amd64,linux/arm64"`,
		`OCI_LAYOUT=".tmp/release-bundle/tamsin-image"`,
		"resolve-image-reference.sh", "FFMPEG_RUNTIME_IMAGE=",
		"build-third-party-licenses.sh", "tamsin-third-party-licenses.tar.gz",
		"build-supply-chain-bundle.py", "tamsin-container-metadata.json", "tamsin-supply-chain.tar.gz",
		"verify-oci-layout.sh record", "smoke-release-image.sh",
		"image_index_digest:",
		"artifact_id:", "artifact_digest:", "actions/upload-artifact@",
		"include-hidden-files: true", "release-version.py describe")
	rejectText(t, build, "image_amd64_digest:", "image_arm64_digest:",
		"image_amd64_config_digest:", "image_arm64_config_digest:")

	requireText(t, e2e,
		"needs: build", "uses: ./.github/workflows/e2e.yml",
		"release_artifact_id: ${{ needs.build.outputs.artifact_id }}",
		"release_artifact_digest: ${{ needs.build.outputs.artifact_digest }}",
		"release_image_ref: ${{ needs.build.outputs.image_ref }}",
		"release_image_index_digest: ${{ needs.build.outputs.image_index_digest }}")
	rejectText(t, e2e, "release_image_amd64_digest:", "release_image_arm64_digest:",
		"release_image_amd64_config_digest:", "release_image_arm64_config_digest:")
	// Prereleases intentionally pass the same exact-artifact E2E. Stability is
	// allowed to decide release marking and latest promotion, not whether E2E runs.
	rejectText(t, e2e, "outputs.stable", "if:")

	requireText(t, publish,
		"needs: [build, e2e]", "actions: read", "attestations: write", "id-token: write",
		"actions/attest-build-provenance@977bb373ede98d70efdf65b84cb5f73e068dcc2a",
		"subject-path:", "actions/download-artifact@", "artifact-ids:",
		"verify-actions-artifact.sh", "verify-oci-layout.sh verify",
		"publish-oci-layout.sh", "${{ needs.build.outputs.image_ref }}",
		"${{ needs.build.outputs.image_index_digest }}",
		"tamsin-third-party-licenses.tar.gz",
		"tamsin-container-metadata.json", "tamsin-supply-chain.tar.gz",
		"oras-project/setup-oras@1d808f7d7f6995cc68b7bf507bfe5c5446e1dc9d",
		"version: 1.3.3",
		"release-notes.py", "gh release create", "--notes-file", "--prerelease")
	rejectText(t, publish, "--generate-notes")

	requireText(t, promote,
		"needs: [build, publish]", "if: needs.build.outputs.stable == 'true'",
		"group: release-promote-latest", "release-version.py newest", "promote-oci-index.sh",
		"oras-project/setup-oras@1d808f7d7f6995cc68b7bf507bfe5c5446e1dc9d",
		"${{ needs.build.outputs.image_ref }}",
		"${{ needs.build.outputs.image_index_digest }}")

	for name, beforeGate := range map[string]string{
		"verify": verify,
		"build":  build,
		"e2e":    e2e,
	} {
		t.Run(name+"CannotMutateExternally", func(t *testing.T) {
			rejectText(t, beforeGate,
				"contents: write", "packages: write", "docker push",
				"publish-oci-layout.sh", "promote-oci-index.sh", "gh release create")
		})
	}
	rejectText(t, publish, "make dist", "make image", "docker build", "docker push")
	rejectText(t, promote, "make dist", "make image", "docker build", "docker pull", "docker push")
	if count := strings.Count(workflow, "gh release create"); count != 1 {
		t.Errorf("release workflow contains %d GitHub release mutations, want one", count)
	}
	if count := strings.Count(workflow, "publish-oci-layout.sh"); count != 1 {
		t.Errorf("release workflow contains %d version-index publications, want one", count)
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
		t.Fatalf("write changelog: %v", err)
	}

	for _, test := range []struct {
		tag      string
		contains []string
	}{
		{tag: "v0.1.0", contains: []string{"First release."}},
		{tag: "v0.1.0-rc.1", contains: []string{"Release candidate `v0.1.0-rc.1`", "First release."}},
	} {
		t.Run(test.tag, func(t *testing.T) {
			command := exec.Command(checker, test.tag, changelog)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("extract notes: %v\n%s", err, output)
			}
			notes := string(output)
			requireText(t, notes, test.contains...)
			rejectText(t, notes, "Future.", "Earlier.")
		})
	}

	command := exec.Command(checker, "v0.2.0", changelog)
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("missing changelog release was accepted:\n%s", output)
	}
	command = exec.Command(checker, "v0.1.0+build.1", changelog)
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("release notes accepted unsupported build metadata:\n%s", output)
	}
}

func TestReleaseVersionTagValidation(t *testing.T) {
	t.Parallel()
	checker := filepath.Join(repositoryRoot, "scripts", "release-version.py")
	tests := []struct {
		name   string
		tag    string
		stable string
		image  string
		valid  bool
	}{
		{name: "initial", tag: "v0.0.0", stable: "true", image: "0.0.0", valid: true},
		{name: "stable metadata", tag: "v12.3.4+build.01"},
		{name: "prerelease metadata", tag: "v1.2.3-rc.1+sha.ABC"},
		{name: "leading core zero", tag: "v01.2.3"},
		{name: "empty prerelease", tag: "v1.2.3-"},
		{name: "dotted prerelease", tag: "v1.2.3-."},
		{name: "empty prerelease identifier", tag: "v1.2.3-rc..1"},
		{name: "numeric prerelease leading zero", tag: "v1.2.3-01"},
		{name: "empty build", tag: "v1.2.3+"},
		{name: "empty build identifier", tag: "v1.2.3+build..1"},
		{name: "underscore", tag: "v1.2.3+build_1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := exec.Command(checker, "describe", test.tag, "LiveWyer-Ops/Tamsin")
			output, err := command.CombinedOutput()
			if !test.valid {
				if err == nil {
					t.Fatalf("invalid tag %q was accepted:\n%s", test.tag, output)
				}
				return
			}
			if err != nil {
				t.Fatalf("valid tag %q was rejected: %v\n%s", test.tag, err, output)
			}
			body := string(output)
			requireText(t, body,
				"version="+test.tag[1:],
				"stable="+test.stable,
				"image_ref=ghcr.io/livewyer-ops/tamsin:"+test.image)
		})
	}
}

func TestNewestStableReleaseTag(t *testing.T) {
	t.Parallel()
	checker := filepath.Join(repositoryRoot, "scripts", "release-version.py")
	tests := []struct {
		name     string
		tags     string
		expected string
	}{
		{name: "none", tags: "v1.0.0-rc.1\nnot-a-version\n"},
		{name: "numeric core", tags: "v2.9.9\nv2.10.0\nv1.99.0\n", expected: "v2.10.0"},
		{name: "prerelease excluded", tags: "v3.0.0-rc.9\nv2.9.9\n", expected: "v2.9.9"},
		{name: "metadata excluded", tags: "v1.2.3+build.1\nv1.2.3\n", expected: "v1.2.3"},
		{name: "invalid ignored", tags: "v9.0.0-01\nv1.0.0\n", expected: "v1.0.0"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := exec.Command(checker, "newest")
			command.Stdin = strings.NewReader(test.tags)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("select newest stable tag: %v\n%s", err, output)
			}
			if actual := strings.TrimSpace(string(output)); actual != test.expected {
				t.Fatalf("newest stable tag is %q, expected %q", actual, test.expected)
			}
		})
	}
}

func TestReleaseBuildCreatesAndSmokesExactlyTwoPlatformsOnce(t *testing.T) {
	t.Parallel()
	workflow := repositoryFile(t, ".github/workflows/release.yml")
	build := workflowJobBlock(t, workflow, "build")
	requireText(t, build,
		"docker/setup-docker-action@77e84dbf09b47d1e29270283c22f16145aa85ca1",
		"version: type=archive,version=29.1.3,channel=stable",
		"docker/setup-qemu-action@96fe6ef7f33517b61c61be40b68a1882f3264fb8",
		"tonistiigi/binfmt:qemu-v10.0.4@sha256:8f58e6214f4cc9dc83ce8f5acad1ece508eb6b20e696a8c1e9f274481982c541",
		"docker/setup-buildx-action@bb05f3f5519dd87d3ba754cc423b652a5edd6d2c",
		"version: v0.35.0",
		"moby/buildkit:v0.26.2@sha256:de10faf919fc71ba4eb1dd7bd6449566d012b0c9436b1c61bfee21d621b009aa")
	ciWorkflow := repositoryFile(t, ".github/workflows/ci.yml")
	ciDist := workflowJobBlock(t, ciWorkflow, "dist")
	requireText(t, ciDist,
		"timeout-minutes: 60",
		"docker/setup-docker-action@77e84dbf09b47d1e29270283c22f16145aa85ca1",
		"docker/setup-qemu-action@96fe6ef7f33517b61c61be40b68a1882f3264fb8",
		"docker/setup-buildx-action@bb05f3f5519dd87d3ba754cc423b652a5edd6d2c",
		"moby/buildkit:v0.26.2@sha256:de10faf919fc71ba4eb1dd7bd6449566d012b0c9436b1c61bfee21d621b009aa",
		"make dist", "IMAGE_PLATFORMS=\"linux/amd64,linux/arm64\"",
		"resolve-image-reference.sh", "IMAGE_RESOLVE_ATTEMPTS=90", "IMAGE_RESOLVE_DELAY_SECONDS=10",
		"FFMPEG_RUNTIME_IMAGE=",
		"build-supply-chain-bundle.py", "tamsin-supply-chain.tar.gz",
		"verify-oci-layout.sh record", "smoke-release-image.sh",
		"git show -s --format=%ct", "SOURCE_DATE_EPOCH")

	makefile := repositoryFile(t, "Makefile")
	ociImage := makeTarget(t, makefile, "oci-image")
	requireText(t, ociImage,
		"docker buildx build", "--platform '$(IMAGE_PLATFORMS)'",
		"type=oci,dest=$(OCI_LAYOUT),tar=false,rewrite-timestamp=true",
		"--provenance=mode=min", "--sbom=true", "SOURCE_DATE_EPOCH")
	rejectText(t, ociImage, "--push", "docker push", "docker save")
	if count := strings.Count(makefile, "docker buildx build"); count != 1 {
		t.Errorf("Makefile invokes buildx build %d times, want one", count)
	}

	dist := makeTarget(t, makefile, "dist")
	requireText(t, dist, "$(MAKE) oci-image")
	if count := strings.Count(dist, "$(MAKE) oci-image"); count != 1 {
		t.Errorf("dist invokes the OCI index build %d times, want one", count)
	}

	smoke := repositoryFile(t, "scripts/smoke-release-image.sh")
	requireText(t, smoke,
		"for platform in linux/amd64 linux/arm64", `--platform "$platform"`,
		"--help", "--version", "doctor --format json")
	verifier := repositoryFile(t, "scripts/verify-oci-layout.sh")
	requireText(t, verifier,
		`required_platforms = ("linux/amd64", "linux/arm64")`,
		"expected exactly linux/amd64 and linux/arm64",
		`config.get("os") != os_name`, `config.get("architecture") != architecture`,
		"https://spdx.dev/Document", "https://slsa.dev/provenance/v0.2",
		"hash_chunk_bytes = 1 << 20", "one attestation per platform manifest")
	loader := repositoryFile(t, "scripts/load-release-image.sh")
	requireText(t, loader,
		`descriptor.get("digest") != expected_manifest`,
		"image_id not in (expected_manifest, expected_config)",
		"elif image_id != expected_config")
	publisher := repositoryFile(t, "scripts/publish-oci-layout.sh")
	requireText(t, publisher,
		"version image already contains exact index", "refusing to replace",
		"cannot establish whether", "--descriptor")

	dockerfile := repositoryFile(t, "Dockerfile")
	requireText(t, dockerfile,
		"ARG SOURCE_DATE_EPOCH=0", "ARG FFMPEG_RUNTIME_IMAGE=ghcr.io/livewyer-ops/tamsin-ffmpeg-runtime:5.1.9-bookworm-r1",
		"FROM ${FFMPEG_RUNTIME_IMAGE}", "org.opencontainers.image.base.name")
	rejectText(t, dockerfile, "apt-get", "snapshot.debian.org")
	runtimeDockerfile := repositoryFile(t, "Dockerfile.ffmpeg")
	requireText(t, runtimeDockerfile,
		"FROM debian:bookworm-slim@sha256:", "ARG DEBIAN_SNAPSHOT=20260803T000000Z",
		"ARG FFMPEG_VERSION=7:5.1.9-0+deb12u1",
		"snapshot.debian.org/archive/debian/", "Check-Valid-Until: no",
		"/var/log/*")
	runtimeWorkflow := repositoryFile(t, ".github/workflows/ffmpeg-runtime.yml")
	requireText(t, runtimeWorkflow,
		"contracts/ffmpeg-runtime.json", "${{ steps.contract.outputs.image }}",
		"--platform linux/amd64,linux/arm64", "--provenance=mode=max", "--sbom=true",
		"resolve-image-reference.sh", "push-to-registry: true", "cancel-in-progress: false")
	dockerignore := repositoryFile(t, ".dockerignore")
	requireText(t, dockerignore, "**", "!go.mod", "!cmd/**/*.go",
		"!contracts/schemas/**/*.json", "**/*_test.go")
}

func TestFFmpegRuntimeContractPinsEveryBuildConsumer(t *testing.T) {
	t.Parallel()
	data := repositoryFile(t, "contracts/ffmpeg-runtime.json")
	var runtime struct {
		SchemaVersion         string   `json:"schema_version"`
		Image                 string   `json:"image"`
		Revision              string   `json:"revision"`
		Platforms             []string `json:"platforms"`
		DebianBase            string   `json:"debian_base"`
		DebianSnapshot        string   `json:"debian_snapshot"`
		FFmpegPackage         string   `json:"ffmpeg_package"`
		CACertificatesPackage string   `json:"ca_certificates_package"`
	}
	if err := json.Unmarshal([]byte(data), &runtime); err != nil {
		t.Fatal(err)
	}
	if runtime.SchemaVersion != "1.0" || runtime.Revision == "" ||
		!reflect.DeepEqual(runtime.Platforms, []string{"linux/amd64", "linux/arm64"}) {
		t.Fatalf("invalid FFmpeg runtime contract: %#v", runtime)
	}
	for filename, values := range map[string][]string{
		"Dockerfile.ffmpeg": {
			strings.TrimPrefix(runtime.DebianBase, "debian:bookworm-slim@"),
			"ARG DEBIAN_SNAPSHOT=" + runtime.DebianSnapshot,
			"ARG FFMPEG_VERSION=" + runtime.FFmpegPackage,
			"ARG CA_CERTIFICATES_VERSION=" + runtime.CACertificatesPackage,
		},
		"Dockerfile":                           {runtime.Image},
		"Makefile":                             {runtime.Image},
		".github/workflows/ci.yml":             {"contracts/ffmpeg-runtime.json"},
		".github/workflows/release.yml":        {"contracts/ffmpeg-runtime.json"},
		".github/workflows/ffmpeg-runtime.yml": {"contracts/ffmpeg-runtime.json", runtime.Revision},
	} {
		requireText(t, repositoryFile(t, filename), values...)
	}
	requireText(t, repositoryFile(t, "scripts/build-supply-chain-bundle.py"),
		"DEFAULT_RUNTIME_CONTRACT", "ffmpeg-runtime.json", `runtime_contract["ffmpeg_package"]`)
}

func TestWorkflowActionsArePinnedByCommit(t *testing.T) {
	t.Parallel()
	remoteUse := regexp.MustCompile(`uses:\s+([^@\s]+)@([^\s#]+)`)
	commit := regexp.MustCompile(`^[0-9a-f]{40}$`)
	for _, name := range []string{
		".github/workflows/ci.yml",
		".github/workflows/e2e.yml",
		".github/workflows/ffmpeg-runtime.yml",
		".github/workflows/release.yml",
	} {
		workflow := repositoryFile(t, name)
		for _, match := range remoteUse.FindAllStringSubmatch(workflow, -1) {
			if !commit.MatchString(match[2]) {
				t.Errorf("%s uses %s at mutable ref %s", name, match[1], match[2])
			}
		}
	}
}

func TestCallableE2EIdentityIsAllOrNoneAndReleaseDoesNotRebuild(t *testing.T) {
	t.Parallel()
	workflow := repositoryFile(t, ".github/workflows/e2e.yml")
	for _, input := range []string{
		"release_artifact_id:",
		"release_artifact_digest:",
		"release_image_ref:",
		"release_image_index_digest:",
	} {
		if count := strings.Count(workflow, input); count != 1 {
			t.Errorf("callable E2E declares %q %d times, want once", input, count)
		}
	}
	requireText(t, workflow,
		`if [ "$supplied" -ne 0 ] && [ "$supplied" -ne 4 ]`,
		"verify-actions-artifact.sh", "artifact-ids: ${{ inputs.release_artifact_id }}",
		"load-release-image.sh", `printf 'image=%s@%s\n' "$IMAGE_REF" "$INDEX_DIGEST"`,
		`make e2e-existing IMAGE="$IMAGE" E2E_PLATFORM="linux/amd64"`)
	rejectText(t, workflow,
		"make dist", "oci-image", "docker buildx build", "docker push",
		"publish-oci-layout.sh", "promote-oci-index.sh", "gh release create")

	scheduledBuild := regexp.MustCompile(`(?s)- name: Build the scheduled CI image\n\s+if: inputs\.release_artifact_id == ''\n\s+run: \|.*?\n\s+make image`).FindString(workflow)
	if scheduledBuild == "" {
		t.Error("scheduled/manual image build is not explicitly restricted to calls without a release artifact")
	}

	makefile := repositoryFile(t, "Makefile")
	existing := makeTarget(t, makefile, "e2e-existing")
	requireText(t, existing,
		"docker image inspect --platform '$(E2E_PLATFORM)'",
		"DOCKER_DEFAULT_PLATFORM='$(E2E_PLATFORM)'", "./scripts/e2e-kind.sh")
	rejectText(t, existing, "$(MAKE) image", "docker build")

	ordinary := makeTarget(t, makefile, "e2e")
	requireText(t, ordinary, "$(MAKE) image", "$(MAKE) e2e-existing")
}

type ociFixtureIdentity struct {
	index       string
	amd64       string
	arm64       string
	amd64Config string
	arm64Config string
}

func writeOCIReleaseFixture(t *testing.T, includeArm64 bool) (string, ociFixtureIdentity) {
	t.Helper()
	bundle := t.TempDir()
	layout := filepath.Join(bundle, "tamsin-image")
	blobs := filepath.Join(layout, "blobs", "sha256")
	if err := os.MkdirAll(blobs, 0o700); err != nil {
		t.Fatal(err)
	}
	writeJSON := func(path string, value any) {
		t.Helper()
		contents, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeJSON(filepath.Join(layout, "oci-layout"), map[string]any{"imageLayoutVersion": "1.0.0"})

	writeBlob := func(contents []byte, mediaType string) map[string]any {
		t.Helper()
		sum := sha256.Sum256(contents)
		hexadecimal := hex.EncodeToString(sum[:])
		if err := os.WriteFile(filepath.Join(blobs, hexadecimal), contents, 0o600); err != nil {
			t.Fatal(err)
		}
		return map[string]any{
			"mediaType": mediaType,
			"digest":    "sha256:" + hexadecimal,
			"size":      len(contents),
		}
	}

	layer := writeBlob([]byte("fixture layer"), "application/vnd.oci.image.layer.v1.tar")
	platformManifest := func(architecture string) (map[string]any, string) {
		t.Helper()
		configContents, err := json.Marshal(map[string]any{
			"architecture": architecture,
			"os":           "linux",
			"config":       map[string]any{"User": "65532:65532"},
		})
		if err != nil {
			t.Fatal(err)
		}
		config := writeBlob(configContents, "application/vnd.oci.image.config.v1+json")
		manifestContents, err := json.Marshal(map[string]any{
			"schemaVersion": 2,
			"mediaType":     "application/vnd.oci.image.manifest.v1+json",
			"config":        config,
			"layers":        []any{layer},
		})
		if err != nil {
			t.Fatal(err)
		}
		manifest := writeBlob(manifestContents, "application/vnd.oci.image.manifest.v1+json")
		manifest["platform"] = map[string]any{"os": "linux", "architecture": architecture}
		return manifest, config["digest"].(string)
	}

	amd64, amd64Config := platformManifest("amd64")
	runnable := []map[string]any{amd64}
	arm64Digest := ""
	arm64Config := ""
	if includeArm64 {
		arm64, configDigest := platformManifest("arm64")
		runnable = append(runnable, arm64)
		arm64Digest = arm64["digest"].(string)
		arm64Config = configDigest
	}
	attestationManifest := func(subject map[string]any) map[string]any {
		t.Helper()
		subjectDigest := subject["digest"].(string)
		predicateTypes := []string{
			"https://spdx.dev/Document",
			"https://slsa.dev/provenance/v0.2",
		}
		layers := make([]any, 0, len(predicateTypes))
		for _, predicateType := range predicateTypes {
			statement, err := json.Marshal(map[string]any{
				"_type": "https://in-toto.io/Statement/v0.1",
				"subject": []any{map[string]any{
					"name": "fixture",
					"digest": map[string]any{
						"sha256": strings.TrimPrefix(subjectDigest, "sha256:"),
					},
				}},
				"predicateType": predicateType,
				"predicate":     map[string]any{},
			})
			if err != nil {
				t.Fatal(err)
			}
			layer := writeBlob(statement, "application/vnd.in-toto+json")
			layer["annotations"] = map[string]any{
				"in-toto.io/predicate-type": predicateType,
			}
			layers = append(layers, layer)
		}
		config := writeBlob([]byte("{}"), "application/vnd.oci.image.config.v1+json")
		contents, err := json.Marshal(map[string]any{
			"schemaVersion": 2,
			"mediaType":     "application/vnd.oci.image.manifest.v1+json",
			"config":        config,
			"layers":        layers,
		})
		if err != nil {
			t.Fatal(err)
		}
		manifest := writeBlob(contents, "application/vnd.oci.image.manifest.v1+json")
		manifest["platform"] = map[string]any{"os": "unknown", "architecture": "unknown"}
		manifest["annotations"] = map[string]any{
			"vnd.docker.reference.type":   "attestation-manifest",
			"vnd.docker.reference.digest": subjectDigest,
		}
		return manifest
	}
	manifests := make([]any, 0, len(runnable)*2)
	for _, manifest := range runnable {
		manifests = append(manifests, manifest)
	}
	for _, manifest := range runnable {
		manifests = append(manifests, attestationManifest(manifest))
	}
	indexContents, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     "application/vnd.oci.image.index.v1+json",
		"manifests":     manifests,
	})
	if err != nil {
		t.Fatal(err)
	}
	index := writeBlob(indexContents, "application/vnd.oci.image.index.v1+json")
	index["annotations"] = map[string]any{
		"io.containerd.image.name":          "ghcr.io/livewyer-ops/tamsin:fixture",
		"org.opencontainers.image.ref.name": "fixture",
	}
	writeJSON(filepath.Join(layout, "index.json"), map[string]any{
		"schemaVersion": 2,
		"manifests":     []any{index},
	})
	return bundle, ociFixtureIdentity{
		index:       index["digest"].(string),
		amd64:       amd64["digest"].(string),
		arm64:       arm64Digest,
		amd64Config: amd64Config,
		arm64Config: arm64Config,
	}
}

func TestOCILayoutVerifierRecordsAndRechecksExactIdentity(t *testing.T) {
	t.Parallel()
	bundle, identity := writeOCIReleaseFixture(t, true)
	checker := filepath.Join(repositoryRoot, "scripts", "verify-oci-layout.sh")
	imageRef := "ghcr.io/livewyer-ops/tamsin:fixture"
	record := exec.Command(checker, "record", bundle, imageRef)
	if output, err := record.CombinedOutput(); err != nil {
		t.Fatalf("record valid OCI identity: %v\n%s", err, output)
	}
	verify := exec.Command(
		checker,
		"verify",
		bundle,
		imageRef,
		identity.index,
	)
	if output, err := verify.CombinedOutput(); err != nil {
		t.Fatalf("verify recorded OCI identity: %v\n%s", err, output)
	}

	wrongArm64 := "sha256:" + strings.Repeat("0", 64) + "\n"
	if err := os.WriteFile(filepath.Join(bundle, "IMAGE_ARM64_DIGEST"), []byte(wrongArm64), 0o600); err != nil {
		t.Fatal(err)
	}
	verify = exec.Command(checker, "verify", bundle, imageRef, identity.index)
	if output, err := verify.CombinedOutput(); err == nil {
		t.Fatalf("verifier accepted substituted derived arm64 metadata:\n%s", output)
	}
	if err := os.WriteFile(filepath.Join(bundle, "IMAGE_ARM64_DIGEST"), []byte(identity.arm64+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	// A manifest descriptor and its image-config descriptor are different
	// digest classes. Docker's legacy store reports the config as Id, while the
	// pinned containerd store reports a manifest Descriptor (and currently that
	// manifest as Id). Accepting one in the other's metadata slot would let E2E
	// claim member selection using the wrong identity class.
	if err := os.WriteFile(filepath.Join(bundle, "IMAGE_AMD64_CONFIG_DIGEST"), []byte(identity.amd64+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	verify = exec.Command(checker, "verify", bundle, imageRef, identity.index)
	if output, err := verify.CombinedOutput(); err == nil {
		t.Fatalf("verifier accepted an amd64 manifest digest as its config digest:\n%s", output)
	}
}

func TestOCILayoutVerifierRejectsMissingArm64Member(t *testing.T) {
	t.Parallel()
	bundle, _ := writeOCIReleaseFixture(t, false)
	checker := filepath.Join(repositoryRoot, "scripts", "verify-oci-layout.sh")
	command := exec.Command(
		checker,
		"record",
		bundle,
		"ghcr.io/livewyer-ops/tamsin:fixture",
	)
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("verifier accepted an amd64-only release index:\n%s", output)
	}
}

func TestSupplyChainBundleContainsDeterministicImageStatements(t *testing.T) {
	t.Parallel()
	bundle, _ := writeOCIReleaseFixture(t, true)
	imageRef := "ghcr.io/livewyer-ops/tamsin:fixture"
	checker := filepath.Join(repositoryRoot, "scripts", "verify-oci-layout.sh")
	if output, err := exec.Command(checker, "record", bundle, imageRef).CombinedOutput(); err != nil {
		t.Fatalf("record valid OCI identity: %v\n%s", err, output)
	}
	directory := t.TempDir()
	metadataPath := filepath.Join(directory, "tamsin-container-metadata.json")
	archivePath := filepath.Join(directory, "tamsin-supply-chain.tar.gz")
	runtimeRef := "ghcr.io/livewyer-ops/tamsin-ffmpeg-runtime:5.1.9-bookworm-r1@sha256:" + strings.Repeat("a", 64)
	command := exec.Command(
		filepath.Join(repositoryRoot, "scripts", "build-supply-chain-bundle.py"),
		"--bundle", bundle,
		"--image-ref", imageRef,
		"--runtime-ref", runtimeRef,
		"--version", "1.0.0-rc.1",
		"--commit", strings.Repeat("b", 40),
		"--created", "2026-08-12T00:00:00Z",
		"--metadata-output", metadataPath,
		"--archive-output", archivePath,
	)
	command.Env = append(os.Environ(), "SOURCE_DATE_EPOCH=123")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build supply-chain bundle: %v\n%s", err, output)
	}
	metadataBytes, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		t.Fatal(err)
	}
	runtime, ok := metadata["ffmpeg_runtime"].(map[string]any)
	if !ok || runtime["immutable_reference"] != runtimeRef {
		t.Fatalf("runtime metadata = %#v", metadata["ffmpeg_runtime"])
	}

	archiveFile, err := os.Open(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer archiveFile.Close()
	compressed, err := gzip.NewReader(archiveFile)
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	entries := map[string]bool{}
	reader := tar.NewReader(compressed)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.ModTime.Unix() != 123 || header.Uid != 0 || header.Gid != 0 {
			t.Fatalf("non-deterministic archive header for %s: %#v", header.Name, header)
		}
		entries[header.Name] = true
	}
	for _, name := range []string{
		"tamsin-container-metadata.json",
		"attestations/linux-amd64-spdx-sbom.json",
		"attestations/linux-amd64-slsa-provenance.json",
		"attestations/linux-arm64-spdx-sbom.json",
		"attestations/linux-arm64-slsa-provenance.json",
	} {
		if !entries[name] {
			t.Errorf("supply-chain archive is missing %s", name)
		}
	}
}

func TestOCILayoutPublisherIsIdempotentAndNeverReplacesAVersion(t *testing.T) {
	bundle, identity := writeOCIReleaseFixture(t, true)
	imageRef := "ghcr.io/livewyer-ops/tamsin:fixture"
	checker := filepath.Join(repositoryRoot, "scripts", "verify-oci-layout.sh")
	if output, err := exec.Command(checker, "record", bundle, imageRef).CombinedOutput(); err != nil {
		t.Fatalf("record valid OCI identity: %v\n%s", err, output)
	}

	bin := t.TempDir()
	logPath := filepath.Join(bin, "oras.log")
	indexPath := filepath.Join(
		bundle,
		"tamsin-image",
		"blobs",
		"sha256",
		strings.TrimPrefix(identity.index, "sha256:"),
	)
	fakeORAS := `#!/usr/bin/env bash
set -Eeuo pipefail
command_name="${1:-} ${2:-}"
if [ "$command_name" = "manifest fetch" ]; then
  shift 2
  output=""
  descriptor=false
  local_layout=false
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --output) output="$2"; shift 2 ;;
      --descriptor) descriptor=true; shift ;;
      --oci-layout) local_layout=true; shift ;;
      --plain-http) shift ;;
      *) shift ;;
    esac
  done
  if [ "$local_layout" = true ]; then
    cp "$FAKE_ORAS_INDEX" "$output"
  elif [ "$descriptor" = true ]; then
    case "$FAKE_ORAS_MODE" in
      exact) printf '{"digest":"%s"}\n' "$FAKE_ORAS_DIGEST" ;;
      different) printf '{"digest":"sha256:%064d"}\n' 0 ;;
      missing) echo 'manifest unknown' >&2; exit 1 ;;
      error) echo 'unauthorized registry response' >&2; exit 1 ;;
    esac
  else
    cp "$FAKE_ORAS_INDEX" "$output"
  fi
elif [ "${1:-}" = "cp" ]; then
  echo cp >> "$FAKE_ORAS_LOG"
else
  echo "unexpected fake ORAS arguments: $*" >&2
  exit 2
fi
`
	if err := os.WriteFile(filepath.Join(bin, "oras"), []byte(fakeORAS), 0o700); err != nil {
		t.Fatal(err)
	}

	publisher := filepath.Join(repositoryRoot, "scripts", "publish-oci-layout.sh")
	run := func(mode string) ([]byte, error) {
		t.Helper()
		if err := os.Remove(logPath); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		command := exec.Command(publisher, bundle, imageRef, identity.index)
		command.Env = append(os.Environ(),
			"PATH="+bin+":"+os.Getenv("PATH"),
			"FAKE_ORAS_MODE="+mode,
			"FAKE_ORAS_INDEX="+indexPath,
			"FAKE_ORAS_DIGEST="+identity.index,
			"FAKE_ORAS_LOG="+logPath,
		)
		return command.CombinedOutput()
	}
	copyWasCalled := func() bool {
		t.Helper()
		_, err := os.Stat(logPath)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		return err == nil
	}

	if output, err := run("exact"); err != nil {
		t.Fatalf("reuse identical version image: %v\n%s", err, output)
	} else if copyWasCalled() {
		t.Fatal("publisher copied over an identical existing version image")
	}
	if output, err := run("different"); err == nil {
		t.Fatalf("publisher replaced a different version image:\n%s", output)
	} else if copyWasCalled() {
		t.Fatal("publisher invoked ORAS copy for a different existing version image")
	}
	if output, err := run("missing"); err != nil {
		t.Fatalf("publish an absent version image: %v\n%s", err, output)
	} else if !copyWasCalled() {
		t.Fatal("publisher did not copy an absent version image")
	}
	if output, err := run("error"); err == nil {
		t.Fatalf("publisher treated an indeterminate registry error as absence:\n%s", output)
	} else if copyWasCalled() {
		t.Fatal("publisher copied after an indeterminate registry error")
	}
}

func makeTarget(t *testing.T, makefile, name string) string {
	t.Helper()
	start := strings.Index(makefile, "\n"+name+":")
	if start < 0 {
		t.Fatalf("Makefile has no %q target", name)
	}
	start++
	rest := makefile[start:]
	lines := strings.Split(rest, "\n")
	end := 1
	for end < len(lines) {
		line := lines[end]
		if line != "" && line[0] != '\t' && line[0] != '#' {
			break
		}
		end++
	}
	return strings.Join(lines[:end], "\n")
}

// TestPythonHeredocCheckRejectsSwallowedShell proves the extra format check
// catches the regression bash -n cannot: deleting a Python heredoc terminator
// makes the next shell/YAML heredoc its body, while remaining valid shell.
func TestPythonHeredocCheckRejectsSwallowedShell(t *testing.T) {
	t.Parallel()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("python3 is required by the repository's release checks")
	}
	bad := filepath.Join(t.TempDir(), "bad.sh")
	contents := "python3 - <<'PY'\nprint('valid until the terminator is lost')\n" +
		"cat >fixture.yaml <<YAML\nmetadata:\n  name: broken\nYAML\nPY\n"
	if err := os.WriteFile(bad, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	checker := filepath.Join(repositoryRoot, "scripts", "check-python-heredocs.py")
	command := exec.Command(python, checker, bad)
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("heredoc checker accepted swallowed shell body:\n%s", output)
	}
}

func TestArtifactVerifierNormalizesTheUploadActionDigest(t *testing.T) {
	t.Parallel()
	const digest = "0fde654d4c6e659b45783a725dc92f1bfb0baa6c2de64b34e814dc206ff4aaaf"
	bin := t.TempDir()
	gh := filepath.Join(bin, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\nprintf '%s\\n' 'sha256:"+digest+"'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	checker := filepath.Join(repositoryRoot, "scripts", "verify-actions-artifact.sh")
	for _, expected := range []string{digest, "sha256:" + digest} {
		command := exec.Command(checker, "1234", expected)
		command.Env = append(os.Environ(),
			"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
			"GITHUB_REPOSITORY=livewyer-ops/tamsin",
			"GH_TOKEN=test-token",
		)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("digest %q was rejected: %v\n%s", expected, err, output)
		}
	}
}
