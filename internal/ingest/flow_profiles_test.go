package ingest

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

const (
	testVideoProfileID = "60d9df18-6d9d-4b86-84bf-d1dcf14b3a28"
	testAudioProfileID = "8d5a25eb-35cb-423b-8e80-72258195ac2c"
)

func TestFlowProfileAssignmentsAreCanonicalAndUnambiguous(t *testing.T) {
	t.Parallel()
	assignments, normalized, err := parseFlowProfileAssignments([]string{
		"audio:0=" + testAudioProfileID,
		"VIDEO=" + testVideoProfileID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(assignments) != 2 || !assignments[0].indexed || assignments[0].index != 0 || assignments[1].selector != "video" {
		t.Fatalf("parsed assignments = %#v", assignments)
	}
	want := []string{"audio:0=" + testAudioProfileID, "video=" + testVideoProfileID}
	if !reflect.DeepEqual(normalized, want) {
		t.Fatalf("normalized assignments = %v, want %v", normalized, want)
	}

	for _, invalid := range [][]string{
		{"multi=" + testVideoProfileID},
		{"audio:01=" + testAudioProfileID},
		{"video=" + testVideoProfileID, "video=" + testAudioProfileID},
		{"not-a-uuid"},
	} {
		if _, _, err := parseFlowProfileAssignments(invalid); err == nil {
			t.Fatalf("parseFlowProfileAssignments(%v) unexpectedly succeeded", invalid)
		}
	}
}

func TestAssignFlowProfilesRequiresIndexedDuplicateFormats(t *testing.T) {
	t.Parallel()
	graph := flowGraph{flows: []graphFlow{
		{flow: tams.Flow{"format": "urn:x-nmos:format:audio"}},
		{flow: tams.Flow{"format": "urn:x-nmos:format:audio"}},
		{flow: tams.Flow{"format": "urn:x-nmos:format:multi"}},
	}}
	assignments, _, err := parseFlowProfileAssignments([]string{"audio=" + testAudioProfileID})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := &Pipeline{apiVersion: tams.APIVersion{Major: 8, Minor: 2}, profileAssignments: assignments}
	if _, err := pipeline.assignFlowProfiles(graph); err == nil || !strings.Contains(err.Error(), "matched 2") {
		t.Fatalf("unindexed duplicate selector error = %v", err)
	}

	assignments, _, err = parseFlowProfileAssignments([]string{"audio:1=" + testAudioProfileID})
	if err != nil {
		t.Fatal(err)
	}
	pipeline.profileAssignments = assignments
	assigned, err := pipeline.assignFlowProfiles(graph)
	if err != nil {
		t.Fatal(err)
	}
	if assigned.flows[0].profileID != "" || assigned.flows[1].profileID != testAudioProfileID || assigned.flows[2].profileID != "" {
		t.Fatalf("assigned graph = %#v", assigned.flows)
	}
}

func TestProfileBackedFlowPlansExpandedReadsAndCompactWrites(t *testing.T) {
	t.Parallel()
	identity := media.Identity{
		FlowID: "f3b1a8de-6c1e-4a0b-9d2f-1c7e5a904bb1", SourceID: "9a2c4e60-71bd-4f3a-8e15-2d6b0c8a7f43",
		Label: "fixture", URI: "file:///fixture.mp4", SHA256: strings.Repeat("a", 64), Size: 100,
	}
	flow, _, err := media.BuildFlow(media.Probe{
		Streams: []media.Stream{{
			Index: 0, CodecName: "h264", CodecType: "video", Width: 640, Height: 360,
			PixelFormat: "yuv420p", AverageFrameRate: "25/1", RealFrameRate: "25/1", Duration: "1.0",
		}},
		Format: media.Format{Name: "mov,mp4", Duration: "1.0"},
	}, identity, "video/mp4", media.EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	flow["id"] = identity.FlowID
	flow["source_id"] = identity.SourceID
	flow["avg_bit_rate"] = int64(900)
	flow["max_bit_rate"] = int64(1200)
	metadata := make(map[string]any)
	for _, field := range profileTechnicalFields {
		if value, present := flow[field]; present {
			metadata[field] = value
		}
	}
	metadata["avg_bit_rate"] = int64(800)

	client := newFakeClient()
	client.profiles[testVideoProfileID] = tams.Profile{
		"id": testVideoProfileID, "label": "HD", "flow_metadata": metadata,
	}
	pipeline, err := New(Config{Concurrency: 1, DryRun: true, TAMSFlowProfiles: []string{testVideoProfileID}},
		client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	plans, err := pipeline.planFlowGraph(context.Background(), flowGraph{flows: []graphFlow{{
		id: identity.FlowID, role: "single", flow: flow, ownsMedia: true,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 1 || plans[0].effective["profile_id"] != testVideoProfileID || plans[0].effective["avg_bit_rate"] != int64(800) {
		t.Fatalf("expanded plan = %#v", plans)
	}
	for _, field := range profileTechnicalFields {
		if _, present := plans[0].request[field]; present {
			t.Fatalf("compact Profile PUT contains technical field %q: %#v", field, plans[0].request)
		}
	}
	if plans[0].request["profile_id"] != testVideoProfileID || plans[0].request["max_bit_rate"] != int64(1200) {
		t.Fatalf("compact Profile PUT lost common metadata: %#v", plans[0].request)
	}
	if client.profileReads != 1 {
		t.Fatalf("Profile reads = %d, want one", client.profileReads)
	}
}

func TestFlowProfileDryRunOnlyReadsServiceAndProfilesPerRun(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	filename := filepath.Join(directory, "fixture.mp4")
	if err := os.WriteFile(filename, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	probe, err := (fakeProber{}).Probe(context.Background(), filename)
	if err != nil {
		t.Fatal(err)
	}
	flow, _, err := media.BuildFlow(probe, media.Identity{
		FlowID: "f3b1a8de-6c1e-4a0b-9d2f-1c7e5a904bb1", SourceID: "9a2c4e60-71bd-4f3a-8e15-2d6b0c8a7f43",
		Label: "fixture", URI: "file:///fixture.mp4", SHA256: strings.Repeat("a", 64), Size: 5,
	}, "video/mp4", media.EssenceStorageMuxed)
	if err != nil {
		t.Fatal(err)
	}
	metadata := make(map[string]any)
	for _, field := range profileTechnicalFields {
		if value, present := flow[field]; present {
			metadata[field] = value
		}
	}

	client := newFakeClient()
	client.serviceDocument = map[string]any{"api_version": "8.2"}
	client.profiles[testVideoProfileID] = tams.Profile{
		"id": testVideoProfileID, "label": "dry-run", "flow_metadata": metadata,
	}
	pipeline, err := New(Config{
		Concurrency: 1, Transfers: 2, DryRun: true, EssenceStorage: media.EssenceStorageMuxed,
		TAMSFlowProfiles: []string{testVideoProfileID},
	}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for run := range 2 {
		batch, runErr := pipeline.Run(context.Background(), []source.Item{localSource(filename)})
		if runErr != nil || batch.Succeeded != 1 {
			t.Fatalf("run %d = %#v, %v", run, batch, runErr)
		}
	}
	client.lock.Lock()
	defer client.lock.Unlock()
	if client.serviceReads != 2 || client.profileReads != 2 {
		t.Fatalf("service/profile reads = %d/%d, want one of each per run", client.serviceReads, client.profileReads)
	}
	if client.backendReads != 0 || client.putFlowCalls != 0 || client.allocations != 0 || len(client.callLog) != 0 {
		t.Fatalf("dry-run performed non-profile TAMS operations: backends=%d puts=%d allocations=%d calls=%v",
			client.backendReads, client.putFlowCalls, client.allocations, client.callLog)
	}
}
