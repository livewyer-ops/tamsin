package progress

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTrackerPublishesCumulativePhases(t *testing.T) {
	t.Parallel()
	var snapshots []Snapshot
	tracker := NewTracker(Observer(func(snapshot Snapshot) { snapshots = append(snapshots, snapshot) }),
		Scope{InputIndex: 2, Input: "programme.ts"}, PhaseStore, PhaseVerify)
	if err := tracker.SetTotals(PhaseStore, 2, 30, true); err != nil {
		t.Fatal(err)
	}
	if err := tracker.Advance(PhaseStore, 1, 10); err != nil {
		t.Fatal(err)
	}
	if err := tracker.Advance(PhaseStore, 1, 20); err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 5 {
		t.Fatalf("published %d snapshots, want 5", len(snapshots))
	}
	current := snapshots[len(snapshots)-1]
	if current.Phase != PhaseStore || current.CompletedObjects != 2 || current.CompletedBytes != 30 || !current.TotalsFinal {
		t.Fatalf("published store snapshot = %#v", current)
	}
	for index := 1; index < len(snapshots); index++ {
		if snapshots[index].Revision <= snapshots[index-1].Revision {
			t.Fatalf("revisions are not increasing: %#v", snapshots)
		}
	}
}

func TestTrackerRejectsInvalidOrReopenedTotals(t *testing.T) {
	t.Parallel()
	tracker := NewTracker(Discard{}, Scope{InputIndex: 0}, PhaseStore)
	if err := tracker.SetTotals(PhaseStore, 1, 10, true); err != nil {
		t.Fatal(err)
	}
	if err := tracker.Advance(PhaseStore, 2, 11); err == nil {
		t.Fatal("advance beyond final totals succeeded")
	}
	if err := tracker.SetTotals(PhaseStore, 2, 20, false); err == nil {
		t.Fatal("final totals were reopened")
	}
	if err := tracker.SetTotals("other", 0, 0, false); err == nil {
		t.Fatal("unknown phase succeeded")
	}
}

func TestTrackerIsConcurrencySafe(t *testing.T) {
	t.Parallel()
	var current Snapshot
	tracker := NewTracker(Observer(func(snapshot Snapshot) { current = snapshot }), Scope{InputIndex: 0}, PhaseStore)
	const workers = 32
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := tracker.Advance(PhaseStore, 1, 1); err != nil {
				t.Errorf("Advance() error = %v", err)
			}
		}()
	}
	group.Wait()
	if current.CompletedObjects != workers || current.CompletedBytes != workers {
		t.Fatalf("concurrent totals = %#v", current)
	}
}

func TestTrackerContextAssociation(t *testing.T) {
	t.Parallel()
	tracker := NewTracker(nil, Scope{InputIndex: 1}, PhaseStore)
	ctx := WithTracker(context.Background(), tracker)
	if got := FromContext(ctx); got != tracker {
		t.Fatalf("FromContext() = %p, want %p", got, tracker)
	}
	if got := FromContext(context.Background()); got != nil {
		t.Fatalf("empty context returned %p", got)
	}
}

func TestPlainReporterEmitsStartsSealedTotalsAndCompletion(t *testing.T) {
	t.Parallel()
	now := time.Unix(1, 0)
	var output bytes.Buffer
	reporter := New(&output, Options{Mode: ModePlain, Interval: time.Hour, Clock: func() time.Time { return now }})
	reporter.Report(Snapshot{Scope: Scope{InputIndex: 0, Input: "file:///tmp/programme.ts"}, Phase: PhaseStore, Revision: 1})
	reporter.Report(Snapshot{Scope: Scope{InputIndex: 0, Input: "file:///tmp/programme.ts"}, Phase: PhaseStore, Revision: 2, TotalsFinal: true, TotalObjects: 2, TotalBytes: 2048})
	reporter.Report(Snapshot{Scope: Scope{InputIndex: 0, Input: "file:///tmp/programme.ts"}, Phase: PhaseStore, Revision: 3, TotalsFinal: true, CompletedObjects: 1, TotalObjects: 2, CompletedBytes: 1024, TotalBytes: 2048})
	reporter.Report(Snapshot{Scope: Scope{InputIndex: 0, Input: "file:///tmp/programme.ts"}, Phase: PhaseStore, Revision: 4, TotalsFinal: true, CompletedObjects: 2, TotalObjects: 2, CompletedBytes: 2048, TotalBytes: 2048})
	reporter.Close()
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 3 || !strings.Contains(lines[0], "programme.ts analysing") ||
		!strings.Contains(lines[1], "0/2 objects") || !strings.Contains(lines[2], "2/2 objects") {
		t.Fatalf("progress output = %q", output.String())
	}
}

func TestPlainReporterThrottlesAndSanitises(t *testing.T) {
	t.Parallel()
	now := time.Unix(1, 0)
	var output bytes.Buffer
	reporter := New(&output, Options{Mode: ModeAuto, Interval: time.Minute, Clock: func() time.Time { return now }})
	scope := Scope{InputIndex: 0, Input: "file:///tmp/%1B%5B31masset.ts"}
	reporter.Report(Snapshot{Scope: scope, Phase: PhaseStore, Revision: 1, TotalsFinal: true, TotalObjects: 3, TotalBytes: 3})
	reporter.Report(Snapshot{Scope: scope, Phase: PhaseStore, Revision: 2, TotalsFinal: true, CompletedObjects: 1, TotalObjects: 3, CompletedBytes: 1, TotalBytes: 3})
	now = now.Add(time.Minute)
	reporter.Report(Snapshot{Scope: scope, Phase: PhaseStore, Revision: 3, TotalsFinal: true, CompletedObjects: 2, TotalObjects: 3, CompletedBytes: 2, TotalBytes: 3})
	if strings.Contains(output.String(), "\x1b") || strings.Count(strings.TrimSpace(output.String()), "\n") != 1 {
		t.Fatalf("unsafe or unthrottled output = %q", output.String())
	}
}

func TestNoneReportsNothing(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	reporter := New(&output, Options{Mode: ModeNone})
	reporter.Report(Snapshot{Scope: Scope{InputIndex: 0}, Phase: PhaseStore, Revision: 1})
	reporter.Close()
	if output.Len() != 0 {
		t.Fatalf("none output = %q", output.String())
	}
}
