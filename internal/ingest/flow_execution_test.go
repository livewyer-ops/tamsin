package ingest

import (
	"context"
	"strings"
	"testing"
)

func TestCollectPlannedFlowObjects(t *testing.T) {
	t.Parallel()
	first := []preparedObject{{id: "first"}, {id: "second"}}
	second := []preparedObject{{id: "third"}, {id: "fourth"}}
	targets := []flowRegistrationTarget{
		flowExecutionTarget("flow-a", 0, first, ""),
		flowExecutionTarget("flow-b", 1, second, ""),
	}
	got := collectPlannedFlowObjects(targets)
	if got == nil || len(got) != 4 {
		t.Fatalf("collectPlannedFlowObjects() = %#v", got)
	}
	if got[0].id != "first" || got[3].id != "fourth" {
		t.Fatalf("collectPlannedFlowObjects() = %#v", got)
	}
}

func TestFlowExecutionRejectsInvalidRegistrationTargetIndex(t *testing.T) {
	t.Parallel()
	client := newFakeClient()
	pipeline, err := New(Config{
		Concurrency: 1,
	}, client, fakeProber{}, nil, discardLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	graph := flowGraph{
		flows: []graphFlow{{id: "7f0dcbb5-bfb3-4f84-85ab-8ad5abf4af1c", flow: map[string]any{
			"id":        "7f0dcbb5-bfb3-4f84-85ab-8ad5abf4af1c",
			"source_id": "d2c3e7d3-3b0f-4a9d-9d6c-2b4f0ce0a8d1",
			"format":    "urn:x-nmos:format:multi",
		}}},
	}
	targets := []flowRegistrationTarget{flowExecutionTarget("flow", 1, nil, "")}
	result := Result{Flows: []FlowResult{{FlowID: "flow", Disposition: FlowPlanned, Objects: []ObjectResult{}}}}
	err = pipeline.executeFlowPlan(context.Background(), "file://input", graph, "", targets, result.Flows)
	if err == nil || !strings.Contains(err.Error(), "internal flow target index") {
		t.Fatalf("executeFlowPlan() = %v, want internal index validation error", err)
	}
	if status := stringField(client.flows[graph.flows[0].id], "status"); status != flowStatusAwaitingContent {
		t.Fatalf("Flow status after invalid registration target = %q, want awaiting_content", status)
	}
}
