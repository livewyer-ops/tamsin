// Package progress publishes cumulative ingest progress and renders concise
// append-only status lines for people.
package progress

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"
	"unicode"
)

// Phase identifies an independently measured transfer phase.
type Phase string

const (
	PhaseStore  Phase = "store"
	PhaseVerify Phase = "verify"
)

func (p Phase) valid() bool { return p == PhaseStore || p == PhaseVerify }

// Scope correlates progress with one resolved input.
type Scope struct {
	InputIndex int    `json:"input_index"`
	Input      string `json:"input"`
	FlowID     string `json:"flow_id,omitempty"`
}

// Snapshot is one cumulative view of one input and transfer phase.
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

// Validate checks the invariants relied on by progress consumers.
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
// concurrent use.
type Reporter interface {
	Report(Snapshot)
	Close()
}

type Discard struct{}

func (Discard) Report(Snapshot) {}
func (Discard) Close()          {}

// Observer adapts a function to Reporter.
type Observer func(Snapshot)

func (o Observer) Report(snapshot Snapshot) {
	if o != nil {
		o(snapshot)
	}
}
func (Observer) Close() {}

type phaseState struct {
	completedObjects int
	totalObjects     int
	completedBytes   int64
	totalBytes       int64
	totalsFinal      bool
	revision         uint64
}

// Tracker converts concurrent increments into ordered cumulative snapshots for
// one input. It does not own or close its Reporter.
type Tracker struct {
	reporter Reporter
	scope    Scope
	publish  sync.Mutex
	state    sync.Mutex
	states   map[Phase]phaseState
	revision uint64
}

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

func (t *Tracker) snapshot(phase Phase, state phaseState) Snapshot {
	return Snapshot{
		Scope: t.scope, Phase: phase,
		CompletedObjects: state.completedObjects, TotalObjects: state.totalObjects,
		CompletedBytes: state.completedBytes, TotalBytes: state.totalBytes,
		TotalsFinal: state.totalsFinal, Revision: state.revision,
	}
}

type trackerContextKey struct{}

func WithTracker(ctx context.Context, tracker *Tracker) context.Context {
	if tracker == nil {
		return ctx
	}
	return context.WithValue(ctx, trackerContextKey{}, tracker)
}

func FromContext(ctx context.Context) *Tracker {
	tracker, _ := ctx.Value(trackerContextKey{}).(*Tracker)
	return tracker
}

// Mode controls append-only progress reporting.
type Mode string

const (
	ModeAuto  Mode = "auto"
	ModePlain Mode = "plain"
	ModeNone  Mode = "none"
)

type Options struct {
	Mode     Mode
	Interval time.Duration
	Clock    func() time.Time
}

const defaultInterval = 30 * time.Second

// New returns a plain or silent reporter. The CLI decides when auto should be
// silent, keeping output-format policy out of this package.
func New(writer io.Writer, options Options) Reporter {
	if options.Mode == ModeNone {
		return Discard{}
	}
	if options.Mode != "" && options.Mode != ModeAuto && options.Mode != ModePlain {
		return Discard{}
	}
	if writer == nil {
		writer = io.Discard
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.Interval <= 0 {
		options.Interval = defaultInterval
	}
	return &Line{
		writer: writer, clock: options.Clock, interval: options.Interval,
		latest: make(map[renderKey]Snapshot), lastWrite: make(map[renderKey]time.Time),
		finished: make(map[renderKey]struct{}),
	}
}

// Line serialises diagnostics and append-only progress on the same writer.
type Line struct {
	writer   io.Writer
	clock    func() time.Time
	interval time.Duration

	lock      sync.Mutex
	latest    map[renderKey]Snapshot
	lastWrite map[renderKey]time.Time
	finished  map[renderKey]struct{}
	closed    bool
}

type renderKey struct {
	inputIndex int
	flowID     string
	phase      Phase
}

func (l *Line) Write(payload []byte) (int, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.writer.Write(payload)
}

func (l *Line) Report(snapshot Snapshot) {
	if snapshot.Validate() != nil || hiddenSnapshot(snapshot) {
		return
	}
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.closed {
		return
	}
	key := renderKey{snapshot.Scope.InputIndex, snapshot.Scope.FlowID, snapshot.Phase}
	if _, done := l.finished[key]; done {
		return
	}
	previous, exists := l.latest[key]
	if exists && snapshot.Revision <= previous.Revision {
		return
	}
	l.latest[key] = snapshot
	now := l.clock()
	complete := snapshot.TotalsFinal && snapshot.CompletedObjects == snapshot.TotalObjects && snapshot.CompletedBytes == snapshot.TotalBytes
	sealed := snapshot.TotalsFinal && (!exists || !previous.TotalsFinal)
	if !exists || sealed || complete || now.Sub(l.lastWrite[key]) >= l.interval {
		_, _ = fmt.Fprintln(l.writer, renderSnapshot(snapshot))
		l.lastWrite[key] = now
	}
	if complete {
		l.finished[key] = struct{}{}
		delete(l.latest, key)
		delete(l.lastWrite, key)
	}
}

func (l *Line) Close() {
	l.lock.Lock()
	l.closed = true
	l.lock.Unlock()
}

func renderSnapshot(snapshot Snapshot) string {
	label := displayLabel(snapshot.Scope.Input)
	prefix := fmt.Sprintf("[%d] %s", snapshot.Scope.InputIndex+1, label)
	if snapshot.Phase == PhaseStore && !snapshot.TotalsFinal && snapshot.CompletedObjects == 0 && snapshot.CompletedBytes == 0 {
		return prefix + " analysing"
	}
	verb := "storing"
	if snapshot.Phase == PhaseVerify {
		verb = "verifying"
	}
	objects := fmt.Sprintf("%d objects", snapshot.CompletedObjects)
	bytes := humanBytes(snapshot.CompletedBytes)
	if snapshot.TotalsFinal {
		objects = fmt.Sprintf("%d/%d objects", snapshot.CompletedObjects, snapshot.TotalObjects)
		bytes = fmt.Sprintf("%s/%s", bytes, humanBytes(snapshot.TotalBytes))
	}
	return fmt.Sprintf("%s %s %s, %s", prefix, verb, objects, bytes)
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
			return sanitize(base)
		}
	}
	if input == "" {
		return "input"
	}
	return input
}

func sanitize(value string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
}

func humanBytes(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}
	value, exponent := float64(bytes), 0
	for value >= 1024 && exponent < 4 {
		value /= 1024
		exponent++
	}
	return fmt.Sprintf("%.1f %ciB", value, "KMGT"[exponent-1])
}
