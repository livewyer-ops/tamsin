package ingest

import (
	"testing"

	"github.com/livewyer-ops/tamsin/internal/progress"
)

func TestTransferTotalsIncludeEveryFlow(t *testing.T) {
	t.Parallel()
	latest := make(map[progress.Phase]progress.Snapshot)
	report := progressFunc(func(s progress.Snapshot) { latest[s.Phase] = s })
	tracker := progress.NewTracker(report, progress.Scope{}, progress.PhaseStore, progress.PhaseVerify)
	p := Pipeline{config: Config{VerificationMode: VerificationReadback}}
	p.expectTransfers(progress.WithTracker(t.Context(), tracker), []flowRegistrationTarget{
		{objects: []preparedObject{{size: 5}, {size: 7}}},
		{objects: []preparedObject{{size: 11}}},
	})
	for _, phase := range []progress.Phase{progress.PhaseStore, progress.PhaseVerify} {
		s := latest[phase]
		if !s.TotalsFinal || s.TotalObjects != 3 || s.TotalBytes != 23 {
			t.Errorf("%s totals = %+v", phase, s)
		}
	}
}
