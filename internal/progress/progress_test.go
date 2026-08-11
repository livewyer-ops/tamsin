package progress

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTrackerPublishesIndependentCumulativePhases(t *testing.T) {
	t.Parallel()
	var (
		lock     sync.Mutex
		reported []Snapshot
	)
	observer := Observer(func(snapshot Snapshot) {
		lock.Lock()
		defer lock.Unlock()
		reported = append(reported, snapshot)
	})
	scope := Scope{InputIndex: 2, Input: "file:///media/programme.ts"}
	tracker := NewTracker(observer, scope, PhaseStore, PhaseVerify)
	if err := tracker.SetTotals(PhaseStore, 4, 4000, true); err != nil {
		t.Fatal(err)
	}
	if err := tracker.SetTotals(PhaseVerify, 4, 4000, true); err != nil {
		t.Fatal(err)
	}
	if err := tracker.Advance(PhaseStore, 2, 2000); err != nil {
		t.Fatal(err)
	}
	if err := tracker.Advance(PhaseVerify, 1, 1000); err != nil {
		t.Fatal(err)
	}

	store, ok := tracker.Current(PhaseStore)
	if !ok {
		t.Fatal("store phase missing")
	}
	verify, ok := tracker.Current(PhaseVerify)
	if !ok {
		t.Fatal("verify phase missing")
	}
	if store.CompletedObjects != 2 || store.TotalObjects != 4 || store.CompletedBytes != 2000 || store.TotalBytes != 4000 {
		t.Fatalf("store snapshot = %+v", store)
	}
	if verify.CompletedObjects != 1 || verify.TotalObjects != 4 || verify.CompletedBytes != 1000 || verify.TotalBytes != 4000 {
		t.Fatalf("verify snapshot = %+v", verify)
	}
	if store.Scope != scope || verify.Scope != scope {
		t.Fatalf("scope was not preserved: store=%+v verify=%+v", store.Scope, verify.Scope)
	}

	lock.Lock()
	defer lock.Unlock()
	if len(reported) != 6 {
		t.Fatalf("reported %d snapshots, want two unknown, two sealed, and two advances: %#v", len(reported), reported)
	}
	if reported[0].TotalsFinal || reported[1].TotalsFinal {
		t.Fatalf("initial totals must be explicitly unknown: %#v", reported[:2])
	}
	for index, snapshot := range reported {
		if err := snapshot.Validate(); err != nil {
			t.Fatalf("invalid published snapshot %+v: %v", snapshot, err)
		}
		if snapshot.Revision != uint64(index+1) {
			t.Fatalf("tracker-wide revision %d = %d, want %d: %#v", index, snapshot.Revision, index+1, reported)
		}
	}
}

func TestTrackerRejectsWorkBeyondFinalTotal(t *testing.T) {
	t.Parallel()
	tracker := NewTracker(Discard{}, Scope{InputIndex: 0}, PhaseStore)
	if err := tracker.SetTotals(PhaseStore, 1, 10, true); err != nil {
		t.Fatal(err)
	}
	if err := tracker.Advance(PhaseStore, 1, 10); err != nil {
		t.Fatal(err)
	}
	if err := tracker.Advance(PhaseStore, 1, 0); err == nil {
		t.Fatal("advance beyond a sealed Object total succeeded")
	}
	if err := tracker.SetTotals(PhaseStore, 0, 0, true); err == nil {
		t.Fatal("final total below completed work succeeded")
	}
	if err := tracker.SetTotals(PhaseStore, 2, 20, false); err == nil {
		t.Fatal("sealed totals were reopened")
	}
	current, _ := tracker.Current(PhaseStore)
	if current.CompletedObjects != 1 || current.CompletedBytes != 10 {
		t.Fatalf("rejected mutations changed state: %+v", current)
	}
}

func TestSnapshotRejectsMissingRevision(t *testing.T) {
	t.Parallel()
	if err := (Snapshot{Scope: Scope{InputIndex: 0}, Phase: PhaseStore}).Validate(); err == nil || !strings.Contains(err.Error(), "revision") {
		t.Fatalf("missing revision validation = %v", err)
	}
}

func TestTrackerAllowsUnknownTotalsToGrow(t *testing.T) {
	t.Parallel()
	tracker := NewTracker(Discard{}, Scope{InputIndex: 0}, PhaseStore)
	if err := tracker.Advance(PhaseStore, 2, 20); err != nil {
		t.Fatal(err)
	}
	unknown, _ := tracker.Current(PhaseStore)
	if unknown.TotalsFinal {
		t.Fatal("an unsealed plan became final")
	}
	if err := unknown.Validate(); err != nil {
		t.Fatalf("completed work is valid while totals remain unknown: %v", err)
	}
	if err := tracker.SetTotals(PhaseStore, 3, 30, true); err != nil {
		t.Fatal(err)
	}
	sealed, _ := tracker.Current(PhaseStore)
	if !sealed.TotalsFinal || sealed.TotalObjects != 3 || sealed.TotalBytes != 30 {
		t.Fatalf("sealed snapshot = %+v", sealed)
	}
}

func TestTrackerIsConcurrencySafeAndOrdered(t *testing.T) {
	t.Parallel()
	var (
		lock      sync.Mutex
		revisions []uint64
	)
	tracker := NewTracker(Observer(func(snapshot Snapshot) {
		if snapshot.Phase != PhaseStore {
			return
		}
		lock.Lock()
		defer lock.Unlock()
		revisions = append(revisions, snapshot.Revision)
	}), Scope{InputIndex: 0}, PhaseStore)
	if err := tracker.SetTotals(PhaseStore, 400, 4000, true); err != nil {
		t.Fatal(err)
	}

	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 50 {
				if err := tracker.Advance(PhaseStore, 1, 10); err != nil {
					t.Errorf("advance: %v", err)
					return
				}
			}
		}()
	}
	workers.Wait()
	current, _ := tracker.Current(PhaseStore)
	if current.CompletedObjects != 400 || current.CompletedBytes != 4000 {
		t.Fatalf("lost updates: %+v", current)
	}
	lock.Lock()
	defer lock.Unlock()
	for index := 1; index < len(revisions); index++ {
		if revisions[index] <= revisions[index-1] {
			t.Fatalf("observer saw revisions out of order at %d: %v", index, revisions)
		}
	}
}

func TestFanoutFeedsRendererAndStructuredObserver(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	var structured []Snapshot
	reporter := Fanout(
		New(&output, Options{Mode: ModePlain, MinInterval: time.Nanosecond}),
		Observer(func(snapshot Snapshot) { structured = append(structured, snapshot) }),
	)
	tracker := NewTracker(reporter, Scope{InputIndex: 0, Input: "file:///first.ts"}, PhaseStore)
	if err := tracker.SetTotals(PhaseStore, 1, 100, true); err != nil {
		t.Fatal(err)
	}
	if err := tracker.Advance(PhaseStore, 1, 100); err != nil {
		t.Fatal(err)
	}
	reporter.Close()

	if len(structured) != 3 {
		t.Fatalf("structured observer received %d snapshots, want 3", len(structured))
	}
	if !strings.Contains(output.String(), "1/1 objects") {
		t.Fatalf("human renderer did not receive the same completion: %q", output.String())
	}
	for _, snapshot := range structured {
		encoded, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(encoded, []byte(`"input_index":0`)) || !bytes.Contains(encoded, []byte(`"totals_final"`)) {
			t.Fatalf("snapshot is not usable as structured output: %s", encoded)
		}
	}
}

func TestTrackersKeepConcurrentInputScopesDistinct(t *testing.T) {
	t.Parallel()
	var (
		lock sync.Mutex
		seen = make(map[int]Snapshot)
	)
	reporter := Observer(func(snapshot Snapshot) {
		if !snapshot.TotalsFinal || snapshot.CompletedObjects != snapshot.TotalObjects {
			return
		}
		lock.Lock()
		defer lock.Unlock()
		seen[snapshot.Scope.InputIndex] = snapshot
	})

	var workers sync.WaitGroup
	for index := range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			tracker := NewTracker(reporter, Scope{InputIndex: index, Input: "same-name.ts"}, PhaseStore)
			if err := tracker.SetTotals(PhaseStore, 1, int64(index+1), true); err != nil {
				t.Errorf("set totals: %v", err)
				return
			}
			if err := tracker.Advance(PhaseStore, 1, int64(index+1)); err != nil {
				t.Errorf("advance: %v", err)
			}
		}()
	}
	workers.Wait()
	lock.Lock()
	defer lock.Unlock()
	if len(seen) != 4 {
		t.Fatalf("concurrent scopes collapsed: %#v", seen)
	}
	for index, snapshot := range seen {
		if snapshot.Scope.InputIndex != index || snapshot.CompletedBytes != int64(index+1) {
			t.Fatalf("scope %d = %+v", index, snapshot)
		}
	}
}

func TestTTYRendererUsesStablePhaseRowsAndCleanClose(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	now := time.Time{}
	line := New(&output, Options{
		Mode: ModeTTY, Width: 100, MinInterval: time.Nanosecond,
		Getenv: func(string) string { return "" },
		Clock:  func() time.Time { now = now.Add(time.Second); return now },
	})
	scope := Scope{InputIndex: 0, Input: "file:///tmp/first-ingest.ts"}
	tracker := NewTracker(line, scope, PhaseStore, PhaseVerify)
	if err := tracker.SetTotals(PhaseStore, 13, 1514152, true); err != nil {
		t.Fatal(err)
	}
	if err := tracker.SetTotals(PhaseVerify, 13, 1514152, true); err != nil {
		t.Fatal(err)
	}
	if err := tracker.Advance(PhaseStore, 7, 788480); err != nil {
		t.Fatal(err)
	}
	if err := tracker.Advance(PhaseVerify, 6, 672144); err != nil {
		t.Fatal(err)
	}
	line.Close()

	rendered := output.String()
	if !strings.Contains(rendered, "storing") || !strings.Contains(rendered, "verifying") {
		t.Fatalf("separate phases were not rendered: %q", rendered)
	}
	if strings.Contains(rendered, "25/26") || strings.Contains(rendered, "2.9 MiB") {
		t.Fatalf("renderer doubled logical work: %q", rendered)
	}
	if !strings.Contains(rendered, "7/13 objects") || !strings.Contains(rendered, "6/13 objects") {
		t.Fatalf("phase counters missing: %q", rendered)
	}
	if !strings.Contains(rendered, "\x1b[1A") || !strings.Contains(rendered, "\x1b[2K") {
		t.Fatalf("TTY renderer did not redraw its small live region: %q", rendered)
	}
	if strings.Contains(rendered, "\x1b[3") {
		t.Fatalf("renderer emitted colour despite its colour-free contract: %q", rendered)
	}
	if !strings.HasSuffix(rendered, "\n") {
		t.Fatalf("Close did not establish a clean result boundary: %q", rendered)
	}
	before := output.Len()
	line.Close()
	if output.Len() != before {
		t.Fatal("Close is not idempotent")
	}
}

func TestTTYRendererSuspendsAndRedrawsAroundDiagnostics(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	reporter := New(&output, Options{
		Mode: ModeTTY, Width: 80, MinInterval: time.Nanosecond,
		Getenv: func(string) string { return "" },
	})
	line, ok := reporter.(*Line)
	if !ok {
		t.Fatalf("TTY reporter type = %T", reporter)
	}
	tracker := NewTracker(line, Scope{InputIndex: 0, Input: "file:///programme.ts"}, PhaseStore)
	if _, err := io.WriteString(line, "warning: unsupported metadata\n"); err != nil {
		t.Fatal(err)
	}
	if err := tracker.SetTotals(PhaseStore, 1, 100, true); err != nil {
		t.Fatal(err)
	}
	line.Close()
	rendered := output.String()
	if !strings.Contains(rendered, "\x1b[2Kwarning: unsupported metadata\n\r") {
		t.Fatalf("diagnostic was not written between clear and redraw: %q", rendered)
	}
	if strings.Contains(rendered, "warning: unsupported metadata\n\r\x1b[2K") {
		t.Fatalf("redraw cleared the diagnostic it had just written: %q", rendered)
	}
	if strings.Contains(rendered, "analysingwarning") || strings.Contains(rendered, "metadata[1]") {
		t.Fatalf("diagnostic and progress were spliced together: %q", rendered)
	}
}

func TestRendererShowsAnalysisUntilTransferTotalsAreKnown(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	now := time.Time{}
	reporter := New(&output, Options{
		Mode: ModePlain, Width: 80, MinInterval: time.Nanosecond,
		Clock: func() time.Time { now = now.Add(time.Second); return now },
	})
	tracker := NewTracker(reporter, Scope{InputIndex: 0, Input: "file:///tmp/programme.ts"}, PhaseStore, PhaseVerify)
	initial := output.String()
	if !strings.Contains(initial, "analysing") {
		t.Fatalf("initial store snapshot did not describe analysis: %q", initial)
	}
	if strings.Contains(initial, "verify") || strings.Contains(initial, "total unknown") {
		t.Fatalf("initial verify/unknown transfer row leaked before the plan was sealed: %q", initial)
	}
	if lines := strings.Count(initial, "\n"); lines != 1 {
		t.Fatalf("initial progress wrote %d rows, want one analysis row: %q", lines, initial)
	}

	if err := tracker.SetTotals(PhaseStore, 2, 200, true); err != nil {
		t.Fatal(err)
	}
	if err := tracker.SetTotals(PhaseVerify, 2, 200, true); err != nil {
		t.Fatal(err)
	}
	planned := output.String()[len(initial):]
	if !strings.Contains(planned, "storing") || !strings.Contains(planned, "verifying") {
		t.Fatalf("sealed plan did not expose both transfer phases: %q", planned)
	}
}

func TestTermDumbDegradesTTYToPlain(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	reporter := New(&output, Options{
		Mode: ModeTTY, MinInterval: time.Nanosecond,
		Getenv: func(key string) string {
			if key == "TERM" {
				return "dumb"
			}
			return ""
		},
	})
	tracker := NewTracker(reporter, Scope{InputIndex: 0, Input: "input.ts"}, PhaseStore)
	if err := tracker.SetTotals(PhaseStore, 1, 1, true); err != nil {
		t.Fatal(err)
	}
	reporter.Close()
	if strings.Contains(output.String(), "\r") || strings.Contains(output.String(), "\x1b") {
		t.Fatalf("TERM=dumb received terminal controls: %q", output.String())
	}
	if !strings.Contains(output.String(), "\n") {
		t.Fatalf("plain progress was not newline-delimited: %q", output.String())
	}
}

func TestNoColorEnvironmentDoesNotChangeMeaning(t *testing.T) {
	t.Parallel()
	for _, noColor := range []string{"", "1"} {
		var output bytes.Buffer
		reporter := New(&output, Options{
			Mode: ModePlain, MinInterval: time.Nanosecond,
			Getenv: func(key string) string {
				if key == "NO_COLOR" {
					return noColor
				}
				return ""
			},
		})
		tracker := NewTracker(reporter, Scope{InputIndex: 0, Input: "input.ts"}, PhaseVerify)
		if err := tracker.SetTotals(PhaseVerify, 1, 10, true); err != nil {
			t.Fatal(err)
		}
		if err := tracker.Advance(PhaseVerify, 1, 10); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(output.String(), "\x1b[") {
			t.Fatalf("NO_COLOR=%q output contains ANSI controls: %q", noColor, output.String())
		}
		if !strings.Contains(output.String(), "verify") {
			t.Fatalf("NO_COLOR=%q removed semantic status: %q", noColor, output.String())
		}
	}
}

func TestWidthTiersPreserveCounters(t *testing.T) {
	t.Parallel()
	snapshot := Snapshot{
		Scope: Scope{InputIndex: 11, Input: "file:///a/very-long-programme-name.ts"}, Phase: PhaseStore,
		CompletedObjects: 7, TotalObjects: 13, CompletedBytes: 788480, TotalBytes: 1514152, TotalsFinal: true,
	}
	for _, width := range []int{32, 50, 80, 120} {
		rendered := renderSnapshot(snapshot, width)
		if runeLen(rendered) > width {
			t.Fatalf("width %d rendered %d columns: %q", width, runeLen(rendered), rendered)
		}
		if !strings.Contains(rendered, "7/13") {
			t.Fatalf("width %d lost the authoritative Object counter: %q", width, rendered)
		}
	}
}

func TestProgressWidthUsesTerminalColumnsForUnicode(t *testing.T) {
	t.Parallel()
	for _, width := range []int{4, 6, 10} {
		fitted := fitRunes("节目🎬-programme.ts", width)
		if columns := runeLen(fitted); columns > width {
			t.Fatalf("width %d produced %d terminal columns: %q", width, columns, fitted)
		}
	}
}

func TestDisplayLabelRejectsPercentEncodedTerminalControls(t *testing.T) {
	t.Parallel()
	label := displayLabel("file:///tmp/programme-%1B%5B31m.ts")
	if strings.ContainsAny(label, "\x1b\r\n") {
		t.Fatalf("decoded locator injected a terminal control: %q", label)
	}
	if !strings.Contains(label, "programme-") {
		t.Fatalf("safe filename context was lost: %q", label)
	}
}

func TestRendererDerivesRateAndETAWithoutChangingSemanticCounters(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	now := time.Time{}
	reporter := New(&output, Options{
		Mode: ModePlain, Width: 120, MinInterval: time.Nanosecond, PlainInterval: time.Nanosecond,
		Clock: func() time.Time { now = now.Add(time.Second); return now },
	})
	tracker := NewTracker(reporter, Scope{InputIndex: 0, Input: "file:///programme.ts"}, PhaseStore)
	if err := tracker.SetTotals(PhaseStore, 2, 2000, true); err != nil {
		t.Fatal(err)
	}
	if err := tracker.Advance(PhaseStore, 1, 1000); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	if !strings.Contains(rendered, "1/2 objects") || !strings.Contains(rendered, "1000 B/s") || !strings.Contains(rendered, "eta 1s") {
		t.Fatalf("wide progress omitted derived rate/ETA or semantic counters: %q", rendered)
	}
}

func TestRendererConsultsWidthOnEveryDraw(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	width := 120
	reporter := New(&output, Options{
		Mode: ModePlain, Width: width, WidthFunc: func() int { return width },
		MinInterval: time.Nanosecond, PlainInterval: time.Nanosecond,
	})
	scope := Scope{InputIndex: 0, Input: "file:///a/very-long-programme-name.ts"}
	reporter.Report(Snapshot{
		Scope: scope, Phase: PhaseStore, Revision: 1,
		TotalObjects: 2, TotalBytes: 200, TotalsFinal: true,
	})
	width = 40
	reporter.Report(Snapshot{
		Scope: scope, Phase: PhaseStore, Revision: 2,
		CompletedObjects: 1, TotalObjects: 2, CompletedBytes: 100, TotalBytes: 200, TotalsFinal: true,
	})
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("rendered %d lines, want two: %q", len(lines), output.String())
	}
	if runeLen(lines[1]) > width || !strings.Contains(lines[1], "1/2") {
		t.Fatalf("resized row does not fit %d columns with its counter: %q", width, lines[1])
	}
}

func TestAnalysisRenderingIncludesElapsedTime(t *testing.T) {
	t.Parallel()
	snapshot := Snapshot{Scope: Scope{InputIndex: 0, Input: "file:///programme.ts"}, Phase: PhaseStore, Revision: 1}
	rendered := renderSnapshotWithRate(snapshot, 80, 0, 1200*time.Millisecond)
	if !strings.Contains(rendered, "analysing · 1s") {
		t.Fatalf("analysis row omitted elapsed time: %q", rendered)
	}
}

func TestTTYRefreshesElapsedAnalysisWithoutNewSnapshots(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	reporter := New(&output, Options{
		Mode: ModeTTY, Width: 80, RefreshInterval: 5 * time.Millisecond,
		Getenv: func(string) string { return "" },
	})
	reporter.Report(Snapshot{
		Scope: Scope{InputIndex: 0, Input: "file:///programme.ts"}, Phase: PhaseStore, Revision: 1,
	})
	time.Sleep(18 * time.Millisecond)
	reporter.Close()
	if strings.Count(output.String(), "analysing") < 2 {
		t.Fatalf("live analysis did not refresh without a semantic update: %q", output.String())
	}
}

func TestTTYDrawsSealedTotalsImmediatelyAfterAnalysis(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	now := time.Unix(1, 0)
	reporter := New(&output, Options{
		Mode: ModeTTY, Width: 80, MinInterval: time.Hour, RefreshInterval: time.Hour,
		Getenv: func(string) string { return "" },
		Clock:  func() time.Time { return now },
	})
	tracker := NewTracker(reporter, Scope{InputIndex: 0, Input: "file:///programme.ts"}, PhaseStore)
	before := output.Len()
	if err := tracker.SetTotals(PhaseStore, 2, 200, true); err != nil {
		t.Fatal(err)
	}
	reporter.Close()
	transition := output.String()[before:]
	if !strings.Contains(transition, "storing") || !strings.Contains(transition, "0/2 objects") {
		t.Fatalf("sealed plan remained visually stuck in analysis: %q", transition)
	}
}

func TestTTYRendererRetainsOnlyActiveRows(t *testing.T) {
	t.Parallel()
	line := newLine(io.Discard, defaultOptions(Options{
		Mode: ModeTTY, Width: 80, MinInterval: time.Nanosecond, RefreshInterval: time.Hour,
		Getenv: func(string) string { return "" },
	}), ModeTTY)
	const inputs = 1_000
	for index := range inputs {
		line.Report(Snapshot{
			Scope: Scope{InputIndex: index, Input: "input.ts"}, Phase: PhaseStore, Revision: 1,
			CompletedObjects: 1, TotalObjects: 1, CompletedBytes: 1, TotalBytes: 1, TotalsFinal: true,
		})
	}
	line.lock.Lock()
	active := len(line.latest) + len(line.firstSeen) + len(line.rates) + len(line.rateState) + len(line.lastPlain)
	completed := line.completed[PhaseStore]
	terminalRevisions := len(line.finished)
	line.lock.Unlock()
	line.Close()
	if active != 0 {
		t.Fatalf("renderer retained %d active-state entries after every input completed", active)
	}
	if completed != inputs || terminalRevisions != inputs {
		t.Fatalf("completion state = %d aggregate, %d revisions; want %d", completed, terminalRevisions, inputs)
	}
}

func TestPlainProgressEmitsMilestonesInsteadOfEveryObject(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	reporter := New(&output, Options{Mode: ModePlain, PlainInterval: time.Hour})
	tracker := NewTracker(reporter, Scope{InputIndex: 0, Input: "file:///programme.ts"}, PhaseStore)
	if err := tracker.SetTotals(PhaseStore, 100, 100, true); err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if err := tracker.Advance(PhaseStore, 1, 1); err != nil {
			t.Fatal(err)
		}
	}
	reporter.Close()
	if lines := strings.Count(output.String(), "\n"); lines != 3 {
		t.Fatalf("plain progress wrote %d lines, want analysis, sealed plan, and completion: %q", lines, output.String())
	}
}

func TestRendererRejectsStaleRevision(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	now := time.Time{}
	reporter := New(&output, Options{
		Mode: ModePlain, MinInterval: time.Nanosecond,
		Clock: func() time.Time { now = now.Add(time.Second); return now },
	})
	base := Snapshot{
		Scope: Scope{InputIndex: 0, Input: "input.ts"}, Phase: PhaseStore,
		TotalObjects: 2, TotalBytes: 2, TotalsFinal: true,
	}
	newer := base
	newer.CompletedObjects, newer.CompletedBytes, newer.Revision = 2, 2, 4
	older := base
	older.CompletedObjects, older.CompletedBytes, older.Revision = 1, 1, 3
	reporter.Report(newer)
	reporter.Report(older)
	if strings.Contains(output.String(), "1/2 objects") {
		t.Fatalf("stale revision regressed display: %q", output.String())
	}
}

func TestTrackerContextPreservesPerInputAssociation(t *testing.T) {
	t.Parallel()
	tracker := NewTracker(Discard{}, Scope{InputIndex: 3}, PhaseStore)
	ctx := WithTracker(context.Background(), tracker)
	if got := FromContext(ctx); got != tracker {
		t.Fatalf("FromContext = %p, want %p", got, tracker)
	}
	if got := FromContext(context.WithoutCancel(ctx)); got != tracker {
		t.Fatal("detached recovery context lost progress association")
	}
	if got := FromContext(context.Background()); got != nil {
		t.Fatalf("unbound context returned %p", got)
	}
}

func TestIsTerminalRejectsNonTerminals(t *testing.T) {
	t.Parallel()
	file, err := os.Create(filepath.Join(t.TempDir(), "captured.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if IsTerminal(file) {
		t.Fatal("a regular file must not be treated as a terminal")
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	if IsTerminal(writer) {
		t.Fatal("a pipe must not be treated as a terminal")
	}
	if IsTerminal(&bytes.Buffer{}) {
		t.Fatal("a non-file writer must not be treated as a terminal")
	}
}

func TestNoneReportsNothing(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	reporter := New(&output, Options{Mode: ModeNone})
	reporter.Report(Snapshot{Scope: Scope{InputIndex: 0}, Phase: PhaseStore})
	reporter.Close()
	if output.Len() != 0 {
		t.Fatalf("none mode wrote %q", output.String())
	}
}

func TestHumanBytes(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		bytes int64
		want  string
	}{
		{0, "0 B"},
		{999, "999 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1024 * 1024, "1.0 MiB"},
		{3 * 1024 * 1024 * 1024, "3.0 GiB"},
	} {
		if got := humanBytes(testCase.bytes); got != testCase.want {
			t.Errorf("humanBytes(%d) = %q, want %q", testCase.bytes, got, testCase.want)
		}
	}
}
