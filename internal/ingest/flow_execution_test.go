package ingest

import "testing"

func TestCollectPlannedFlowObjects(t *testing.T) {
	t.Parallel()
	first := []preparedObject{{id: "first"}, {id: "second"}}
	second := []preparedObject{{id: "third"}, {id: "fourth"}}
	targets := []flowRegistrationTarget{
		{flowID: "flow-a", resultIndex: 0, objects: first},
		{flowID: "flow-b", resultIndex: 1, objects: second},
	}
	got := collectPlannedFlowObjects(targets)
	if got == nil || len(got) != 4 {
		t.Fatalf("collectPlannedFlowObjects() = %#v", got)
	}
	if got[0].id != "first" || got[3].id != "fourth" {
		t.Fatalf("collectPlannedFlowObjects() = %#v", got)
	}
}
