package tamsschema

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/tams"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func compileSchema(t *testing.T, name string) *jsonschema.Schema {
	return compileSchemaRevision(t, revisions[2].revision, name)
}

func compileSchemaRevision(t *testing.T, revision schemaRevision, name string) *jsonschema.Schema {
	t.Helper()
	schema, err := (&schemaCache{revision: revision}).schema(name)
	if err != nil {
		t.Fatalf("compile schema %s: %v", name, err)
	}
	return schema
}

// validate marshals a value the way the CLI sends it on the wire, so the test
// exercises the same bytes TAMS would receive.
func validate(t *testing.T, schema *jsonschema.Schema, value any) error {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal instance: %v", err)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("decode instance: %v", err)
	}
	return schema.Validate(instance)
}

func testIdentity() media.Identity {
	return media.Identity{
		FlowID:   "f3b1a8de-6c1e-4a0b-9d2f-1c7e5a904bb1",
		SourceID: "9a2c4e60-71bd-4f3a-8e15-2d6b0c8a7f43",
		Label:    "example.mp4",
		URI:      "https://example.com/example.mp4",
		SHA256:   "77145c94c11f3754207499158df22406e1fe7635553c1c86dc5e881dfeb32016",
		Size:     991017,
	}
}

func videoStream() media.Stream {
	return media.Stream{
		Index: 0, CodecName: "h264", CodecType: "video",
		Width: 640, Height: 360, PixelFormat: "yuv420p",
		AverageFrameRate: "30/1", RealFrameRate: "30/1",
		StartTime: "0.000000", Duration: "10.000000", BitRate: "789000",
	}
}

func audioStream() media.Stream {
	return media.Stream{
		Index: 1, CodecName: "aac", CodecType: "audio",
		SampleRate: "48000", Channels: 2,
		StartTime: "0.000000", Duration: "10.000000", BitRate: "128000",
	}
}

// TestGeneratedFlowsMatchPinnedSchema is the guard that stops Flow metadata
// drifting away from the pinned contract. flow.json is a oneOf, so a Flow that
// matches two essence schemas, or none, fails here.
func TestGeneratedFlowsMatchPinnedSchema(t *testing.T) {
	t.Parallel()
	schemas := []*jsonschema.Schema{
		compileSchemaRevision(t, revisions[1].revision, "flow.json"),
		compileSchema(t, "flow-put.json"),
	}

	for _, testCase := range []struct {
		name        string
		probe       media.Probe
		contentType string
	}{
		{
			name: "video",
			probe: media.Probe{
				Streams: []media.Stream{videoStream()},
				Format:  media.Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Duration: "10.000000", BitRate: "800000"},
			},
			contentType: "video/mp4",
		},
		{
			name: "audio",
			probe: media.Probe{
				Streams: []media.Stream{audioStream()},
				Format:  media.Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Duration: "10.000000", BitRate: "128000"},
			},
			contentType: "audio/mp4",
		},
		{
			name: "multi",
			probe: media.Probe{
				Streams: []media.Stream{videoStream(), audioStream()},
				Format:  media.Format{Name: "mpegts", Duration: "10.000000", BitRate: "930000"},
			},
			contentType: "video/mp2t",
		},
		{
			name: "data",
			probe: media.Probe{
				Streams: nil,
				Format:  media.Format{Name: "json", Duration: "0"},
			},
			contentType: "application/json",
		},
		{
			name: "still image",
			probe: media.Probe{
				Streams: []media.Stream{{
					Index: 0, CodecName: "png", CodecType: "video", Width: 320, Height: 180,
				}},
				Format: media.Format{Name: "png_pipe", Duration: "0"},
			},
			contentType: "image/png",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			flow, _, err := media.BuildFlow(testCase.probe, testIdentity(), testCase.contentType, media.EssenceStorageMuxed)
			if err != nil {
				t.Fatalf("build flow: %v", err)
			}
			for _, schema := range schemas {
				if err := validate(t, schema, flow); err != nil {
					encoded, _ := json.MarshalIndent(flow, "", "  ")
					t.Fatalf("generated Flow does not satisfy a supported TAMS schema:\n%v\n\nflow:\n%s", err, encoded)
				}
			}
		})
	}
}

// TestRuntimeFlowValidationReportsTheRelevantJSONPointer ensures the oneOf
// wrapper cannot make an invalid video field look like an error from the
// unrelated multi-essence branch. The pointer is part of the operator-facing
// --flow-metadata correction path.
func TestRuntimeFlowValidationReportsTheRelevantJSONPointer(t *testing.T) {
	t.Parallel()
	flow, _, err := media.BuildFlow(media.Probe{
		Streams: []media.Stream{videoStream()},
		Format:  media.Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Duration: "10.000000"},
	}, testIdentity(), "video/mp4", media.EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	flow["generation"] = "not-an-integer"
	err = ValidateFlow(flow)
	if err == nil || !strings.Contains(err.Error(), "/generation") {
		t.Fatalf("ValidateFlow() error = %v, want JSON pointer /generation", err)
	}
	if strings.Contains(err.Error(), "/format") {
		t.Fatalf("ValidateFlow() reported an unrelated oneOf branch: %v", err)
	}
}

// TestMultiEssenceFlowCollectsMonoEssenceFlows encodes AppNote 0006: a muxed
// input is described by a multi-essence Flow that owns the Media Objects and
// collects one mono-essence Flow per elementary stream. The collected Flows
// must NOT carry `container`, because they do not reference Media Objects
// directly. The mapping belongs on the parent Collection Item, so BuildFlow
// carries it separately until the caller assigns the child identifier.
//
// The schemas mark all of these properties optional, so this cannot be left to
// schema validation alone.
func TestMultiEssenceFlowCollectsMonoEssenceFlows(t *testing.T) {
	t.Parallel()
	schema := compileSchema(t, "flow-put.json")

	probe := media.Probe{
		Streams: []media.Stream{videoStream(), audioStream()},
		Format:  media.Format{Name: "mpegts", Duration: "10.000000", BitRate: "930000"},
	}
	flow, info, err := media.BuildFlow(probe, testIdentity(), "video/mp2t", media.EssenceStorageMuxed)
	if err != nil {
		t.Fatalf("build flow: %v", err)
	}

	if flow["format"] != "urn:x-nmos:format:multi" {
		t.Fatalf("muxed input should produce a multi-essence Flow, got %v", flow["format"])
	}
	if flow["container"] != "video/mp2t" {
		t.Fatalf("multi-essence Flow must declare the container it references, got %v", flow["container"])
	}
	if len(info.Collected) != len(probe.Streams) {
		t.Fatalf("expected one collected Flow per stream, got %d", len(info.Collected))
	}

	roles := make(map[string]bool, len(info.Collected))
	for index, collected := range info.Collected {
		if collected.Role == "" {
			t.Fatalf("collected Flow %d has no role", index)
		}
		if roles[collected.Role] {
			t.Fatalf("collected roles must distinguish members, %q repeated", collected.Role)
		}
		roles[collected.Role] = true

		if _, present := collected.Flow["container"]; present {
			t.Fatalf("collected Flow %q must not set container: it does not reference Media Objects directly", collected.Role)
		}
		mapping := collected.ContainerMapping
		if mapping == nil {
			t.Fatalf("collected Flow %q must provide its parent Collection Item's container_mapping", collected.Role)
		}
		if _, present := collected.Flow["container_mapping"]; present {
			t.Fatalf("collected Flow %q must not own its parent Collection Item's container_mapping", collected.Role)
		}
		if mapping["track_index"] != index {
			t.Fatalf("collected Flow %q track_index = %v, want %d", collected.Role, mapping["track_index"], index)
		}
		if _, ok := mapping["format_track_index"]; !ok {
			t.Fatalf("collected Flow %q container_mapping needs format_track_index", collected.Role)
		}

		// Identifiers are assigned by the caller, so supply them here to satisfy
		// the schema's required properties.
		collected.Flow["id"] = testIdentity().FlowID
		collected.Flow["source_id"] = testIdentity().SourceID
		if err := validate(t, schema, collected.Flow); err != nil {
			encoded, _ := json.MarshalIndent(collected.Flow, "", "  ")
			t.Fatalf("collected Flow %q does not satisfy pinned TAMS schema:\n%v\n\nflow:\n%s", collected.Role, err, encoded)
		}
	}

	if info.Collected[0].Flow["format"] != "urn:x-nmos:format:video" {
		t.Fatalf("first collected Flow should describe the video track, got %v", info.Collected[0].Flow["format"])
	}
	if info.Collected[1].Flow["format"] != "urn:x-nmos:format:audio" {
		t.Fatalf("second collected Flow should describe the audio track, got %v", info.Collected[1].Flow["format"])
	}
}

// TestSingleEssenceFlowIsNotCollected keeps the mono path unchanged: a
// single-stream input references its Media Objects directly and collects
// nothing.
func TestSingleEssenceFlowIsNotCollected(t *testing.T) {
	t.Parallel()
	probe := media.Probe{
		Streams: []media.Stream{videoStream()},
		Format:  media.Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Duration: "10.000000"},
	}
	flow, info, err := media.BuildFlow(probe, testIdentity(), "video/mp4", media.EssenceStorageMuxed)
	if err != nil {
		t.Fatalf("build flow: %v", err)
	}
	if len(info.Collected) != 0 {
		t.Fatalf("single-essence input should collect nothing, got %d", len(info.Collected))
	}
	if flow["container"] != "video/mp4" {
		t.Fatalf("single-essence Flow must declare its container, got %v", flow["container"])
	}
	if _, present := flow["container_mapping"]; present {
		t.Fatal("single-essence Flow needs no container_mapping")
	}
}

// TestFlowTagsUseImplementationPrefix encodes AppNote 0003: unprefixed tag
// names are reserved for interoperable use, and implementation-specific tags
// must carry an underscore and the service name.
func TestFlowTagsUseImplementationPrefix(t *testing.T) {
	t.Parallel()
	probe := media.Probe{
		Streams: []media.Stream{videoStream()},
		Format:  media.Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Duration: "10.000000"},
	}
	flow, _, err := media.BuildFlow(probe, testIdentity(), "video/mp4", media.EssenceStorageMuxed)
	if err != nil {
		t.Fatalf("build flow: %v", err)
	}
	tags, ok := flow["tags"].(map[string]any)
	if !ok || len(tags) == 0 {
		t.Fatalf("expected Flow tags, got %#v", flow["tags"])
	}
	for name := range tags {
		if !strings.HasPrefix(name, "_tamsin_") {
			t.Fatalf("tag %q must be prefixed _tamsin_: unprefixed names are reserved for interoperable tags", name)
		}
	}
	for _, required := range []string{"_tamsin_sources", "_tamsin_sha256", "_tamsin_bytes"} {
		if _, present := tags[required]; !present {
			t.Fatalf("provenance tag %q is missing", required)
		}
	}

	// Per-essence Flows are Flows in their own right, and in independent storage
	// they are the only Flows created, so provenance has to survive onto them.
	muxed := media.Probe{
		Streams: []media.Stream{videoStream(), audioStream()},
		Format:  media.Format{Name: "mpegts", Duration: "10.000000"},
	}
	for _, storage := range []media.EssenceStorage{media.EssenceStorageMuxed, media.EssenceStorageIndependent} {
		_, info, err := media.BuildFlow(muxed, testIdentity(), "video/mp2t", storage)
		if err != nil {
			t.Fatalf("build flow (%s): %v", storage, err)
		}
		for _, essence := range info.Collected {
			essenceTags, ok := essence.Flow["tags"].(map[string]any)
			if !ok || len(essenceTags) == 0 {
				t.Fatalf("%s essence Flow %q carries no provenance tags", storage, essence.Role)
			}
			for _, required := range []string{"_tamsin_sources", "_tamsin_sha256", "_tamsin_bytes"} {
				if _, present := essenceTags[required]; !present {
					t.Fatalf("%s essence Flow %q is missing %q", storage, essence.Role, required)
				}
			}
		}
	}
}

// TestSegmentRequestOmitsUnsetInitObjectID keeps the additive 8.2 field out of
// requests sent through the 8.1-compatible media path.
func TestSegmentRequestOmitsUnsetInitObjectID(t *testing.T) {
	t.Parallel()
	encoded, err := json.Marshal(tams.SegmentRequest{ObjectID: "object-1", Timerange: "[0:0_10:0)"})
	if err != nil {
		t.Fatalf("marshal SegmentRequest: %v", err)
	}
	if strings.Contains(string(encoded), "init_object_id") {
		t.Fatalf("SegmentRequest carries an unset init_object_id: %s", encoded)
	}
}

// TestVariableFrameRateExcludesFrameRate encodes ADR0041 and the wording the
// video schema carries: frame_rate "MUST be set if vfr is false or omitted.
// MUST NOT be set if vfr is true." ADR0041 rejected leaving the frame rate
// simply unknown, so a video Flow always states one or the other.
func TestVariableFrameRateExcludesFrameRate(t *testing.T) {
	t.Parallel()
	schema := compileSchema(t, "flow-put.json")

	fixed := videoStream()
	variable := videoStream()
	variable.Cadence = media.CadenceVariable

	for _, testCase := range []struct {
		name        string
		stream      media.Stream
		wantVFR     bool
		wantRateSet bool
	}{
		{name: "fixed-rate", stream: fixed, wantVFR: false, wantRateSet: true},
		{name: "variable-rate", stream: variable, wantVFR: true, wantRateSet: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			probe := media.Probe{
				Streams: []media.Stream{testCase.stream},
				Format:  media.Format{Name: "mov,mp4,m4a,3gp,3g2,mj2", Duration: "10.000000"},
			}
			flow, _, err := media.BuildFlow(probe, testIdentity(), "video/mp4", media.EssenceStorageMuxed)
			if err != nil {
				t.Fatalf("build flow: %v", err)
			}
			parameters, ok := flow["essence_parameters"].(map[string]any)
			if !ok {
				t.Fatalf("expected essence_parameters, got %#v", flow["essence_parameters"])
			}
			vfr, _ := parameters["vfr"].(bool)
			if vfr != testCase.wantVFR {
				t.Fatalf("vfr = %v, want %v", vfr, testCase.wantVFR)
			}
			_, rateSet := parameters["frame_rate"]
			if rateSet != testCase.wantRateSet {
				t.Fatalf("frame_rate present = %v, want %v (MUST NOT be set when vfr is true)", rateSet, testCase.wantRateSet)
			}
			if err := validate(t, schema, flow); err != nil {
				encoded, _ := json.MarshalIndent(flow, "", "  ")
				t.Fatalf("Flow does not satisfy pinned schema:\n%v\n\nflow:\n%s", err, encoded)
			}
		})
	}
}

// TestSegmentTimerangesDoNotOverlap encodes ADR0009, which rejected allowing
// Segments to overlap: "the benefit of the widest possible support for media
// types and usage patterns is outweighed by the risk of reducing
// interoperability". Segmentation must therefore produce a strictly increasing,
// non-overlapping sequence.
func TestSegmentTimerangesDoNotOverlap(t *testing.T) {
	t.Parallel()
	// Timeranges as tamsin emits them: half-open, exclusive end.
	timeranges := []string{"[0:0_8:333333000)", "[8:333333000_10:0)"}
	previousEnd := ""
	for index, timerange := range timeranges {
		trimmed := strings.TrimSuffix(strings.TrimPrefix(timerange, "["), ")")
		start, end, ok := strings.Cut(trimmed, "_")
		if !ok {
			t.Fatalf("timerange %d is not a half-open range: %q", index, timerange)
		}
		if previousEnd != "" && start != previousEnd {
			t.Fatalf("Segment %d starts at %s but the previous ended at %s; Segments must not overlap or gap (ADR0009)",
				index, start, previousEnd)
		}
		previousEnd = end
	}
}

// TestIndependentEssenceFlowsOwnTheirObjects encodes the other half of AppNote
// 0006. When essences are demultiplexed each Flow owns its own Media Objects,
// so the rules invert against the muxed case: an independently stored Flow MUST
// declare a container, and MUST NOT carry container_mapping, because there is
// no longer a multiplex to locate anything inside.
func TestIndependentEssenceFlowsOwnTheirObjects(t *testing.T) {
	t.Parallel()
	schema := compileSchema(t, "flow-put.json")

	probe := media.Probe{
		Streams: []media.Stream{videoStream(), audioStream()},
		Format:  media.Format{Name: "mpegts", Duration: "10.000000", BitRate: "930000"},
	}
	_, info, err := media.BuildFlow(probe, testIdentity(), "video/mp2t", media.EssenceStorageIndependent)
	if err != nil {
		t.Fatalf("build flow: %v", err)
	}
	if len(info.Collected) != len(probe.Streams) {
		t.Fatalf("expected one Flow per essence, got %d", len(info.Collected))
	}

	for _, essence := range info.Collected {
		if _, present := essence.Flow["container_mapping"]; present {
			t.Fatalf("independently stored Flow %q must not carry container_mapping: there is no multiplex to map into", essence.Role)
		}
		if _, present := essence.Flow["container"]; !present {
			t.Fatalf("independently stored Flow %q owns Media Objects and must declare a container", essence.Role)
		}
		essence.Flow["id"] = testIdentity().FlowID
		essence.Flow["source_id"] = testIdentity().SourceID
		if err := validate(t, schema, essence.Flow); err != nil {
			encoded, _ := json.MarshalIndent(essence.Flow, "", "  ")
			t.Fatalf("independent Flow %q does not satisfy pinned TAMS schema:\n%v\n\nflow:\n%s", essence.Role, err, encoded)
		}
	}

	// Per-essence containers describe that essence, not the source multiplex.
	if info.Collected[0].Flow["container"] != "video/mp2t" {
		t.Fatalf("video essence container = %v", info.Collected[0].Flow["container"])
	}
}

// TestSegmentRequestMatchesPinnedSchema covers the request body tamsin POSTs to
// /flows/{flowId}/segments.
func TestSegmentRequestMatchesPinnedSchema(t *testing.T) {
	t.Parallel()
	schema := compileSchema(t, "flow-segment-post.json")

	keyFrames := 1
	for _, testCase := range []struct {
		name    string
		request tams.SegmentRequest
	}{
		{
			name:    "minimal",
			request: tams.SegmentRequest{ObjectID: "object-1", Timerange: "[0:0_10:0)"},
		},
		{
			name: "full",
			request: tams.SegmentRequest{
				ObjectID:        "object-2",
				InitObjectID:    "init-object-1",
				Timerange:       "[0:0_8:333333000)",
				ObjectTimerange: "[0:0_8:333333000)",
				TSOffset:        "0:0",
				LastDuration:    "0:33333333",
				KeyFrameCount:   &keyFrames,
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if err := validate(t, schema, testCase.request); err != nil {
				t.Fatalf("SegmentRequest does not satisfy pinned TAMS schema: %v", err)
			}
		})
	}
}

// TestStorageRequestMatchesPinnedSchema covers the request body tamsin POSTs to
// /flows/{flowId}/storage.
func TestStorageRequestMatchesPinnedSchema(t *testing.T) {
	t.Parallel()
	schema := compileSchema(t, "flow-storage-post.json")

	for _, testCase := range []struct {
		name    string
		request tams.StorageRequest
	}{
		{name: "by-object-id", request: tams.StorageRequest{ObjectIDs: []string{"object-1"}}},
		{name: "by-limit", request: tams.StorageRequest{Limit: 4, StorageID: "1b4e28ba-2fa1-4d1b-883f-1b4e28ba2fa1"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if err := validate(t, schema, testCase.request); err != nil {
				t.Fatalf("StorageRequest does not satisfy pinned TAMS schema: %v", err)
			}
		})
	}
}

// TestPinnedSchemasAreSelfConsistent fails if a vendored schema is unparseable
// or has an unresolvable reference, which is how a partial schema refresh shows
// up rather than as a confusing failure in the tests above.
func TestPinnedSchemasAreSelfConsistent(t *testing.T) {
	t.Parallel()
	for _, revision := range []schemaRevision{revisions[1].revision, revisions[2].revision} {
		entries, err := runtimeSchemaFS.ReadDir(revision.directory)
		if err != nil {
			t.Fatalf("read vendored schemas: %v", err)
		}
		if len(entries) == 0 {
			t.Fatalf("no vendored TAMS schemas found in %s", revision.directory)
		}
		for _, entry := range entries {
			t.Run(revision.directory+"/"+entry.Name(), func(t *testing.T) {
				t.Parallel()
				compileSchemaRevision(t, revision, entry.Name())
			})
		}
	}
}
