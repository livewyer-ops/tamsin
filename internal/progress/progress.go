// Package progress publishes semantic ingest progress and provides small
// terminal renderers for operators.
//
// The semantic Snapshot is the contract. Renderers are consumers of that
// contract, just like an NDJSON publisher or a GUI launched as a child
// process. Keeping transfer accounting out of presentation prevents a human
// progress bar and a machine consumer from developing different meanings for
// "objects" or "bytes".
package progress

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// Phase identifies an independently measured transfer phase. An Object is
// counted once in each phase it actually enters; store and verification are
// never added together into a fictional larger Object count.
type Phase string

const (
	PhaseStore  Phase = "store"
	PhaseVerify Phase = "verify"
)

func (p Phase) valid() bool { return p == PhaseStore || p == PhaseVerify }

// Scope correlates progress with one stable resolved input. InputIndex is
// zero-based and is authoritative when concurrent inputs have identical
// labels. Input must already be safe for operator and structured output (for
// example, with URL credentials removed). FlowID is optional because an input
// plan can span several elemental Flows.
type Scope struct {
	InputIndex int    `json:"input_index"`
	Input      string `json:"input"`
	FlowID     string `json:"flow_id,omitempty"`
}

// Snapshot is one cumulative view of one input and one transfer phase.
//
// TotalsFinal is deliberately independent from the total values. Until it is
// true, consumers may show completed work but must not derive a percentage or
// ETA: segmentation and input discovery may still increase the totals.
// Revision increases within a Tracker and lets asynchronous consumers reject
// a stale snapshot without treating cross-input arrival order as meaningful.
type Snapshot struct {
	Scope            Scope  `json:"scope"`
	Phase            Phase  `json:"phase"`
	CompletedObjects int    `json:"completed_objects"`
	TotalObjects     int    `json:"total_objects"`
	CompletedBytes   int64  `json:"completed_bytes"`
	TotalBytes       int64  `json:"total_bytes"`
	TotalsFinal      bool   `json:"totals_final"`
	Revision         uint64 `json:"revision"`
}

// Validate checks the invariants relied on by both terminal and structured
// consumers.
func (s Snapshot) Validate() error {
	if s.Scope.InputIndex < 0 {
		return errors.New("progress input index cannot be negative")
	}
	if !s.Phase.valid() {
		return fmt.Errorf("unsupported progress phase %q", s.Phase)
	}
	if s.Revision == 0 {
		return errors.New("progress revision must be positive")
	}
	if s.CompletedObjects < 0 || s.TotalObjects < 0 || s.CompletedBytes < 0 || s.TotalBytes < 0 {
		return errors.New("progress counters cannot be negative")
	}
	if s.TotalsFinal && s.CompletedObjects > s.TotalObjects {
		return fmt.Errorf("completed objects %d exceed final total %d", s.CompletedObjects, s.TotalObjects)
	}
	if s.TotalsFinal && s.CompletedBytes > s.TotalBytes {
		return fmt.Errorf("completed bytes %d exceed final total %d", s.CompletedBytes, s.TotalBytes)
	}
	return nil
}

// Reporter receives semantic snapshots. Implementations must be safe for
// concurrent use because inputs and Media Objects are processed in parallel.
// Close is idempotent and establishes the boundary before a permanent result
// is written.
type Reporter interface {
	Report(Snapshot)
	Close()
}

// Discard reports nothing.
type Discard struct{}

func (Discard) Report(Snapshot) {}
func (Discard) Close()          {}

// Observer adapts a function to Reporter. It is the integration point for an
// NDJSON event publisher: Fanout an Observer with a terminal renderer and both
// receive exactly the same semantic snapshots. A directly used callback must
// be concurrency-safe; Fanout serializes callbacks even when given one child.
type Observer func(Snapshot)

func (o Observer) Report(snapshot Snapshot) {
	if o != nil {
		o(snapshot)
	}
}
func (Observer) Close() {}

// Fanout returns a Reporter that forwards every snapshot and Close to each
// supplied reporter, in order. It is safe to include nil reporters.
func Fanout(reporters ...Reporter) Reporter {
	filtered := make([]Reporter, 0, len(reporters))
	for _, reporter := range reporters {
		if reporter != nil {
			filtered = append(filtered, reporter)
		}
	}
	switch len(filtered) {
	case 0:
		return Discard{}
	default:
		return &fanout{reporters: filtered}
	}
}

type fanout struct {
	lock      sync.Mutex
	reporters []Reporter
	closed    bool
}

func (f *fanout) Report(snapshot Snapshot) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.closed {
		return
	}
	for _, reporter := range f.reporters {
		reporter.Report(snapshot)
	}
}

func (f *fanout) Close() {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	for _, reporter := range f.reporters {
		reporter.Close()
	}
}

type phaseState struct {
	completedObjects int
	totalObjects     int
	completedBytes   int64
	totalBytes       int64
	totalsFinal      bool
	revision         uint64
}

// Tracker converts concurrent, incremental transfer completions into ordered
// cumulative snapshots for one input. A Tracker does not own or close its
// Reporter; one Reporter is normally shared by all inputs in a run.
type Tracker struct {
	reporter Reporter
	scope    Scope

	// publish guarantees revisions from this input reach an Observer in order
	// without imposing an order across concurrent inputs. State has a separate
	// lock so an Observer may inspect Current without deadlocking publication.
	publish  sync.Mutex
	state    sync.Mutex
	states   map[Phase]phaseState
	revision uint64
}

// NewTracker creates a per-input tracker and immediately publishes an unknown-
// total snapshot for each requested phase. Duplicate phases are ignored.
func NewTracker(reporter Reporter, scope Scope, phases ...Phase) *Tracker {
	if reporter == nil {
		reporter = Discard{}
	}
	tracker := &Tracker{reporter: reporter, scope: scope, states: make(map[Phase]phaseState)}
	seen := make(map[Phase]struct{}, len(phases))
	for _, phase := range phases {
		if !phase.valid() {
			continue
		}
		if _, duplicate := seen[phase]; duplicate {
			continue
		}
		seen[phase] = struct{}{}
		tracker.revision++
		tracker.states[phase] = phaseState{revision: tracker.revision}
		reporter.Report(Snapshot{Scope: scope, Phase: phase, Revision: tracker.revision})
	}
	return tracker
}

// SetTotals publishes the latest discovered total for phase. final should be
// true only once this input's Object plan for that phase cannot grow further.
func (t *Tracker) SetTotals(phase Phase, objects int, bytes int64, final bool) error {
	if !phase.valid() {
		return fmt.Errorf("unsupported progress phase %q", phase)
	}
	if objects < 0 || bytes < 0 {
		return errors.New("progress totals cannot be negative")
	}
	t.publish.Lock()
	defer t.publish.Unlock()
	t.state.Lock()
	state := t.states[phase]
	if state.totalsFinal {
		if !final || objects != state.totalObjects || bytes != state.totalBytes {
			t.state.Unlock()
			return errors.New("final progress totals cannot be reopened or changed")
		}
		t.state.Unlock()
		return nil
	}
	if objects < state.totalObjects || bytes < state.totalBytes {
		t.state.Unlock()
		return errors.New("progress totals cannot decrease")
	}
	if final && (objects < state.completedObjects || bytes < state.completedBytes) {
		t.state.Unlock()
		return errors.New("final progress totals cannot be lower than completed work")
	}
	state.totalObjects = objects
	state.totalBytes = bytes
	state.totalsFinal = final
	t.revision++
	state.revision = t.revision
	t.states[phase] = state
	snapshot := t.snapshot(phase, state)
	t.state.Unlock()
	t.reporter.Report(snapshot)
	return nil
}

// Advance records completed work for phase and publishes a cumulative
// snapshot. It rejects an increment that would exceed a sealed total.
func (t *Tracker) Advance(phase Phase, objects int, bytes int64) error {
	if !phase.valid() {
		return fmt.Errorf("unsupported progress phase %q", phase)
	}
	if objects < 0 || bytes < 0 {
		return errors.New("progress increments cannot be negative")
	}
	t.publish.Lock()
	defer t.publish.Unlock()
	t.state.Lock()
	state := t.states[phase]
	nextObjects := state.completedObjects + objects
	nextBytes := state.completedBytes + bytes
	if state.totalsFinal && (nextObjects > state.totalObjects || nextBytes > state.totalBytes) {
		t.state.Unlock()
		return errors.New("completed progress cannot exceed final totals")
	}
	state.completedObjects = nextObjects
	state.completedBytes = nextBytes
	t.revision++
	state.revision = t.revision
	t.states[phase] = state
	snapshot := t.snapshot(phase, state)
	t.state.Unlock()
	t.reporter.Report(snapshot)
	return nil
}

// Current returns the latest cumulative snapshot for phase.
func (t *Tracker) Current(phase Phase) (Snapshot, bool) {
	t.state.Lock()
	defer t.state.Unlock()
	state, ok := t.states[phase]
	if !ok {
		return Snapshot{}, false
	}
	return t.snapshot(phase, state), true
}

func (t *Tracker) snapshot(phase Phase, state phaseState) Snapshot {
	return Snapshot{
		Scope: t.scope, Phase: phase,
		CompletedObjects: state.completedObjects, TotalObjects: state.totalObjects,
		CompletedBytes: state.completedBytes, TotalBytes: state.totalBytes,
		TotalsFinal: state.totalsFinal, Revision: state.revision,
	}
}

type trackerContextKey struct{}

// WithTracker binds an input tracker to the input's operation context. Derived
// transfer and detached-recovery contexts retain the association, so every
// completion remains correlated under concurrency.
func WithTracker(ctx context.Context, tracker *Tracker) context.Context {
	if tracker == nil {
		return ctx
	}
	return context.WithValue(ctx, trackerContextKey{}, tracker)
}

// FromContext returns the input tracker bound by WithTracker, if any.
func FromContext(ctx context.Context) *Tracker {
	tracker, _ := ctx.Value(trackerContextKey{}).(*Tracker)
	return tracker
}

// Mode controls progress presentation independently from result and log
// formats.
type Mode string

const (
	ModeAuto  Mode = "auto"
	ModeTTY   Mode = "tty"
	ModePlain Mode = "plain"
	ModeNone  Mode = "none"
)

// Options configures a progress renderer. Width is a rendering hint rather
// than a semantic constraint. Getenv and Clock are injectable for deterministic
// tests; production callers normally leave them nil.
type Options struct {
	Mode  Mode
	Width int
	// WidthFunc is consulted for every draw so a live renderer follows terminal
	// resize. A positive result overrides Width; invalid results retain the last
	// usable width. Tests and non-terminal callers may leave it nil.
	WidthFunc   func() int
	MinInterval time.Duration
	// PlainInterval bounds append-only heartbeat noise. Exact phase starts,
	// sealed totals, and completion bypass it.
	PlainInterval time.Duration
	// RefreshInterval updates elapsed analysis time in a live TTY even while
	// media probing emits no new semantic snapshots.
	RefreshInterval time.Duration
	Getenv          func(string) string
	Clock           func() time.Time
}

// minRedraw prevents fast local transfers turning rendering into the dominant
// work. Final snapshots always bypass the throttle.
const minRedraw = 100 * time.Millisecond

const (
	plainHeartbeat  = 30 * time.Second
	analysisRefresh = time.Second
)

// New returns a TTY, plain, or silent Reporter. Auto uses a live region only
// for a capable terminal and otherwise emits plain newline-delimited progress.
// TERM=dumb always selects plain output. Renderers intentionally emit no
// colour, so NO_COLOR is honoured without changing the semantic content.
func New(writer io.Writer, options Options) Reporter {
	options = defaultOptions(options)
	switch options.Mode {
	case ModeNone:
		return Discard{}
	case ModePlain:
		return newLine(writer, options, ModePlain)
	case ModeTTY:
		if isDumb(options.Getenv) {
			return newLine(writer, options, ModePlain)
		}
		return newLine(writer, options, ModeTTY)
	case ModeAuto, "":
		if IsTerminal(writer) && !isDumb(options.Getenv) {
			return newLine(writer, options, ModeTTY)
		}
		return newLine(writer, options, ModePlain)
	default:
		return Discard{}
	}
}

// Line is an inline renderer. TTY mode owns a small redrawable block; plain
// mode writes throttled cumulative snapshots separated by newlines. It never
// enters the alternate screen and never emits colour control sequences.
type Line struct {
	writer          io.Writer
	mode            Mode
	width           int
	widthFunc       func() int
	clock           func() time.Time
	delay           time.Duration
	plainDelay      time.Duration
	refreshInterval time.Duration

	lock      sync.Mutex
	latest    map[renderKey]Snapshot
	firstSeen map[renderKey]time.Time
	rates     map[renderKey]float64
	rateState map[renderKey]rateSample
	lastPlain map[renderKey]time.Time
	completed map[Phase]int
	// finished keeps only a terminal tombstone. Completed snapshots leave the
	// live rendering maps immediately, but a delayed stale update must not
	// reopen a row which already reached its terminal total.
	finished    map[renderKey]struct{}
	lastDraw    time.Time
	lastRows    int
	closed      bool
	refreshStop chan struct{}
	refreshDone chan struct{}
}

// Write lets diagnostics share the live renderer's serializer. In TTY mode it
// temporarily removes the transient rows, writes the complete diagnostic, and
// redraws the latest progress. This prevents warnings and errors from being
// spliced into a progress row without hiding them from the operator.
func (l *Line) Write(payload []byte) (int, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.mode != ModeTTY || l.closed || l.lastRows == 0 {
		return l.writer.Write(payload)
	}
	l.clearTTY()
	l.lastRows = 0
	written, err := l.writer.Write(payload)
	if err == nil && written != len(payload) {
		err = io.ErrShortWrite
	}
	if err == nil && written == len(payload) {
		l.drawTTY()
	}
	return written, err
}

type rateSample struct {
	at          time.Time
	bytes       int64
	initialized bool
}

type renderKey struct {
	inputIndex int
	flowID     string
	phase      Phase
}

// NewLine preserves the original constructor while using the phase-aware live
// renderer. TERM=dumb degrades it to plain output.
func NewLine(writer io.Writer) *Line {
	options := defaultOptions(Options{Mode: ModeTTY})
	mode := ModeTTY
	if isDumb(options.Getenv) {
		mode = ModePlain
	}
	return newLine(writer, options, mode)
}

func newLine(writer io.Writer, options Options, mode Mode) *Line {
	if writer == nil {
		writer = io.Discard
	}
	line := &Line{
		writer: writer, mode: mode, width: options.Width, widthFunc: options.WidthFunc,
		clock: options.Clock, delay: options.MinInterval, plainDelay: options.PlainInterval,
		refreshInterval: options.RefreshInterval,
		latest:          make(map[renderKey]Snapshot), firstSeen: make(map[renderKey]time.Time), rates: make(map[renderKey]float64),
		rateState: make(map[renderKey]rateSample), lastPlain: make(map[renderKey]time.Time),
		completed: make(map[Phase]int), finished: make(map[renderKey]struct{}),
	}
	if mode == ModeTTY {
		line.refreshStop = make(chan struct{})
		line.refreshDone = make(chan struct{})
		go line.refreshLoop()
	}
	return line
}

func defaultOptions(options Options) Options {
	if options.Getenv == nil {
		options.Getenv = os.Getenv
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.MinInterval <= 0 {
		options.MinInterval = minRedraw
	}
	if options.PlainInterval <= 0 {
		options.PlainInterval = plainHeartbeat
	}
	if options.RefreshInterval <= 0 {
		options.RefreshInterval = analysisRefresh
	}
	if options.Width <= 0 {
		if columns, err := strconv.Atoi(options.Getenv("COLUMNS")); err == nil && columns > 0 {
			options.Width = columns
		} else {
			options.Width = 80
		}
	}
	return options
}

func isDumb(getenv func(string) string) bool {
	return strings.EqualFold(strings.TrimSpace(getenv("TERM")), "dumb")
}

// IsTerminal reports whether writer is attached to a character device.
func IsTerminal(writer io.Writer) bool {
	file, ok := writer.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// Report updates one stable phase row. Invalid snapshots are ignored rather
// than allowing optional presentation to fail an ingest.
func (l *Line) Report(snapshot Snapshot) {
	if snapshot.Validate() != nil {
		return
	}
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.closed {
		return
	}
	key := renderKey{inputIndex: snapshot.Scope.InputIndex, flowID: snapshot.Scope.FlowID, phase: snapshot.Phase}
	if _, finished := l.finished[key]; finished {
		return
	}
	current, exists := l.latest[key]
	if exists && snapshot.Revision > 0 && current.Revision > 0 && snapshot.Revision <= current.Revision {
		return
	}
	l.latest[key] = snapshot
	now := l.clock()
	if !exists {
		l.firstSeen[key] = now
	}
	l.updateRate(key, snapshot, now)
	width := l.currentWidth()
	complete := snapshot.TotalsFinal && snapshot.CompletedObjects == snapshot.TotalObjects && snapshot.CompletedBytes == snapshot.TotalBytes
	phaseTransition := snapshot.TotalsFinal && (!exists || !current.TotalsFinal)
	if l.mode == ModePlain {
		if hiddenSnapshot(snapshot) {
			return
		}
		if previous := l.lastPlain[key]; !complete && !phaseTransition && !previous.IsZero() && now.Sub(previous) < l.plainDelay {
			return
		}
		l.lastPlain[key] = now
		_, _ = fmt.Fprintln(l.writer, renderSnapshotWithRate(snapshot, width, l.rates[key], now.Sub(l.firstSeen[key])))
		if complete {
			l.completeKey(key)
		}
		return
	}
	if complete {
		l.completeKey(key)
	}
	if !complete && !phaseTransition && !l.lastDraw.IsZero() && now.Sub(l.lastDraw) < l.delay {
		return
	}
	l.lastDraw = now
	l.drawTTY()
}

// Close removes the TTY live region and emits a line boundary before the
// permanent result. This is intentional even when stdout and stderr are later
// combined by a PTY recorder: progress can never become "objectsINGESTED".
func (l *Line) Close() {
	l.lock.Lock()
	if l.closed {
		done := l.refreshDone
		l.lock.Unlock()
		if done != nil {
			<-done
		}
		return
	}
	l.closed = true
	if l.refreshStop != nil {
		close(l.refreshStop)
	}
	if l.mode == ModeTTY && l.lastRows > 0 {
		l.clearTTY()
		_, _ = io.WriteString(l.writer, "\n")
		l.lastRows = 0
	}
	done := l.refreshDone
	l.lock.Unlock()
	if done != nil {
		<-done
	}
}

func (l *Line) refreshLoop() {
	defer close(l.refreshDone)
	ticker := time.NewTicker(l.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-l.refreshStop:
			return
		case <-ticker.C:
			l.lock.Lock()
			if l.closed {
				l.lock.Unlock()
				return
			}
			if l.hasAnalysingRowLocked() {
				l.lastDraw = l.clock()
				l.drawTTY()
			}
			l.lock.Unlock()
		}
	}
}

func (l *Line) hasAnalysingRowLocked() bool {
	for _, snapshot := range l.latest {
		if snapshot.Phase == PhaseStore && !snapshot.TotalsFinal &&
			snapshot.CompletedObjects == 0 && snapshot.CompletedBytes == 0 {
			return true
		}
	}
	return false
}

func (l *Line) drawTTY() {
	rows := l.rows()
	if len(rows) == 0 {
		if l.lastRows > 0 {
			l.clearTTY()
			l.lastRows = 0
		}
		return
	}
	if l.lastRows > 0 {
		l.clearTTY()
	}
	_, _ = io.WriteString(l.writer, "\r"+strings.Join(rows, "\n"))
	l.lastRows = len(rows)
}

// clearTTY clears from the current last row back to the first row, leaving the
// cursor at column zero of the first row.
func (l *Line) clearTTY() {
	_, _ = io.WriteString(l.writer, "\r\x1b[2K")
	for row := 1; row < l.lastRows; row++ {
		_, _ = io.WriteString(l.writer, "\x1b[1A\r\x1b[2K")
	}
}

func (l *Line) rows() []string {
	width := l.currentWidth()
	now := l.clock()
	snapshots := make([]Snapshot, 0, len(l.latest))
	for _, snapshot := range l.latest {
		if hiddenSnapshot(snapshot) {
			continue
		}
		snapshots = append(snapshots, snapshot)
	}
	sort.Slice(snapshots, func(i, j int) bool {
		left, right := snapshots[i], snapshots[j]
		if left.Scope.InputIndex != right.Scope.InputIndex {
			return left.Scope.InputIndex < right.Scope.InputIndex
		}
		if left.Scope.FlowID != right.Scope.FlowID {
			return left.Scope.FlowID < right.Scope.FlowID
		}
		return phaseOrder(left.Phase) < phaseOrder(right.Phase)
	})
	rows := make([]string, 0, len(snapshots)+1)
	if completed := l.completedRow(width); completed != "" {
		rows = append(rows, completed)
	}
	for _, snapshot := range snapshots {
		key := renderKey{inputIndex: snapshot.Scope.InputIndex, flowID: snapshot.Scope.FlowID, phase: snapshot.Phase}
		rows = append(rows, renderSnapshotWithRate(snapshot, width, l.rates[key], now.Sub(l.firstSeen[key])))
	}
	return rows
}

func (l *Line) completeKey(key renderKey) {
	delete(l.latest, key)
	delete(l.firstSeen, key)
	delete(l.rates, key)
	delete(l.rateState, key)
	delete(l.lastPlain, key)
	l.finished[key] = struct{}{}
	l.completed[key.phase]++
}

func (l *Line) completedRow(width int) string {
	var parts []string
	if count := l.completed[PhaseStore]; count > 0 {
		parts = append(parts, fmt.Sprintf("store %d", count))
	}
	if count := l.completed[PhaseVerify]; count > 0 {
		parts = append(parts, fmt.Sprintf("verify %d", count))
	}
	if len(parts) == 0 {
		return ""
	}
	return fitRunes("complete  "+strings.Join(parts, " · "), width)
}

func phaseOrder(phase Phase) int {
	if phase == PhaseStore {
		return 0
	}
	return 1
}

func renderSnapshot(snapshot Snapshot, width int) string {
	return renderSnapshotWithRate(snapshot, width, 0, 0)
}

func renderSnapshotWithRate(snapshot Snapshot, width int, bytesPerSecond float64, elapsed time.Duration) string {
	if snapshot.Phase == PhaseStore && !snapshot.TotalsFinal && snapshot.CompletedObjects == 0 && snapshot.CompletedBytes == 0 {
		index := fmt.Sprintf("[%d]", snapshot.Scope.InputIndex+1)
		status := "analysing · " + compactElapsed(elapsed)
		if width < 60 {
			return fitRunes(index+" "+status, width)
		}
		label := index + " " + displayLabel(snapshot.Scope.Input)
		available := width - runeLen(status) - 2
		return fitRunes(fitRunes(label, available)+"  "+status, width)
	}
	phase := "storing"
	shortPhase := "store"
	if snapshot.Phase == PhaseVerify {
		phase = "verifying"
		shortPhase = "verify"
	}
	index := fmt.Sprintf("[%d]", snapshot.Scope.InputIndex+1)
	objects := fmt.Sprintf("%d objects", snapshot.CompletedObjects)
	bytes := humanBytes(snapshot.CompletedBytes)
	if snapshot.TotalsFinal {
		objects = fmt.Sprintf("%d/%d objects", snapshot.CompletedObjects, snapshot.TotalObjects)
		bytes = fmt.Sprintf("%s/%s", bytes, humanBytes(snapshot.TotalBytes))
	}

	switch {
	case width < 50:
		if snapshot.TotalsFinal {
			return fitRunes(fmt.Sprintf("%s %s %s", index, shortPhase, objects), width)
		}
		return fitRunes(fmt.Sprintf("%s %s %d objects (total unknown)", index, shortPhase, snapshot.CompletedObjects), width)
	case width < 80:
		if snapshot.TotalsFinal {
			return fitRunes(fmt.Sprintf("%s %s %s · %s", index, shortPhase, objects, bytes), width)
		}
		return fitRunes(fmt.Sprintf("%s %s %s · total unknown", index, shortPhase, bytes), width)
	default:
		status := fmt.Sprintf("%-9s %s · %s", phase, objects, bytes)
		if !snapshot.TotalsFinal {
			status = fmt.Sprintf("%-9s %s · %s · total unknown", phase, objects, bytes)
		}
		if bytesPerSecond > 0 {
			status += " · " + humanBytes(int64(bytesPerSecond)) + "/s"
			if snapshot.TotalsFinal && snapshot.CompletedBytes < snapshot.TotalBytes {
				remaining := float64(snapshot.TotalBytes-snapshot.CompletedBytes) / bytesPerSecond
				if remaining > 0 {
					status += " · eta " + compactETA(time.Duration(remaining*float64(time.Second)))
				}
			}
		}
		label := index + " " + displayLabel(snapshot.Scope.Input)
		available := width - runeLen(status) - 2
		if available < runeLen(index) {
			return fitRunes(index+" "+status, width)
		}
		return fitRunes(fitRunes(label, available)+"  "+status, width)
	}
}

func compactElapsed(duration time.Duration) string {
	if duration < 0 {
		duration = 0
	}
	if duration < time.Second {
		return duration.Round(100 * time.Millisecond).String()
	}
	return duration.Round(time.Second).String()
}

func (l *Line) currentWidth() int {
	if l.widthFunc != nil {
		if width := l.widthFunc(); width > 0 {
			l.width = width
		}
	}
	return l.width
}

func (l *Line) updateRate(key renderKey, snapshot Snapshot, now time.Time) {
	if !snapshot.TotalsFinal {
		return
	}
	sample := l.rateState[key]
	if !sample.initialized {
		l.rateState[key] = rateSample{at: now, bytes: snapshot.CompletedBytes, initialized: true}
		return
	}
	elapsed := now.Sub(sample.at)
	bytes := snapshot.CompletedBytes - sample.bytes
	if bytes <= 0 || elapsed < 500*time.Millisecond {
		return
	}
	l.rates[key] = float64(bytes) / elapsed.Seconds()
	l.rateState[key] = rateSample{at: now, bytes: snapshot.CompletedBytes, initialized: true}
}

func compactETA(duration time.Duration) string {
	if duration < time.Second {
		return "<1s"
	}
	if duration < time.Minute {
		return duration.Round(time.Second).String()
	}
	if duration < time.Hour {
		return duration.Round(10 * time.Second).String()
	}
	return duration.Round(time.Minute).String()
}

func hiddenSnapshot(snapshot Snapshot) bool {
	return snapshot.Phase == PhaseVerify && !snapshot.TotalsFinal &&
		snapshot.CompletedObjects == 0 && snapshot.CompletedBytes == 0
}

func displayLabel(input string) string {
	input = sanitize(input)
	parsed, err := url.Parse(input)
	if err == nil && parsed.Path != "" {
		if base := path.Base(parsed.Path); base != "." && base != "/" {
			// url.Parse decodes percent escapes in Path, so sanitize again after
			// extracting it; %1B must never become a terminal control sequence.
			return sanitize(base)
		}
	}
	if input == "" {
		return "input"
	}
	return sanitize(input)
}

// sanitize prevents a media locator from injecting terminal control
// sequences. Structured consumers still receive the safe, unabridged Scope.
func sanitize(value string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
}

func fitRunes(value string, width int) string {
	if width <= 0 {
		return ""
	}
	if runeLen(value) <= width {
		return value
	}
	if width == 1 {
		return "…"
	}
	available := width - 1
	used := 0
	end := 0
	for index, character := range value {
		characterWidth := terminalRuneWidth(character)
		if used+characterWidth > available {
			break
		}
		used += characterWidth
		end = index + len(string(character))
	}
	return value[:end] + "…"
}

func runeLen(value string) int {
	width := 0
	for _, character := range value {
		width += terminalRuneWidth(character)
	}
	return width
}

func terminalRuneWidth(character rune) int {
	if character == 0 || character == '\u200d' || unicode.IsControl(character) ||
		unicode.Is(unicode.Mn, character) || unicode.Is(unicode.Me, character) {
		return 0
	}
	if character >= 0x1100 && (character <= 0x115f ||
		character == 0x2329 || character == 0x232a ||
		(character >= 0x2e80 && character <= 0xa4cf && character != 0x303f) ||
		(character >= 0xac00 && character <= 0xd7a3) ||
		(character >= 0xf900 && character <= 0xfaff) ||
		(character >= 0xfe10 && character <= 0xfe19) ||
		(character >= 0xfe30 && character <= 0xfe6f) ||
		(character >= 0xff00 && character <= 0xff60) ||
		(character >= 0xffe0 && character <= 0xffe6) ||
		(character >= 0x1f300 && character <= 0x1faff) ||
		(character >= 0x20000 && character <= 0x3fffd)) {
		return 2
	}
	return 1
}

func humanBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	value, exponent := float64(bytes), 0
	for value >= unit && exponent < 4 {
		value /= unit
		exponent++
	}
	return fmt.Sprintf("%.1f %ciB", value, "KMGT"[exponent-1])
}
