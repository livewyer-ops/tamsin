// Package presentation renders stable ingest outcomes for people.
//
// It deliberately owns no terminal detection, progress, logging, or colour
// policy. The CLI resolves those concerns and passes explicit options. This
// keeps the permanent receipt deterministic when it is redirected, tested, or
// embedded by another command.
package presentation

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/livewyer-ops/tamsin/internal/ingest"
	"github.com/livewyer-ops/tamsin/internal/observability"
)

const defaultHumanWidth = 80

// HumanOptions controls the permanent human receipt. Width is the usable
// terminal width in columns; values below one select a deterministic 80-column
// default. Verbose expands provenance and per-Object details. Quiet suppresses
// only clean success and dry-run receipts: warnings and failures are never
// hidden. Color decorates status headings only; all meaning remains in words.
// The caller is responsible for resolving auto/always/never, NO_COLOR, and
// terminal capability before setting Color.
type HumanOptions struct {
	Width   int
	Verbose bool
	Quiet   bool
	Color   bool
}

// WriteHuman writes a grouped, permanent human receipt for batch. It never
// emits tabs, cursor movement, or progress state, and writes every UUID in
// full. Structured output is a separate protocol and must not be derived by
// parsing this presentation.
func WriteHuman(writer io.Writer, batch ingest.BatchResult, metrics observability.Snapshot, options HumanOptions) error {
	if writer == nil {
		return errors.New("human output writer is nil")
	}
	width := options.Width
	if width < 1 {
		width = defaultHumanWidth
	}
	output := &humanWriter{writer: writer, width: width, color: options.Color}

	type visibleResult struct {
		result   ingest.Result
		state    resultState
		severity severity
	}
	visible := make([]visibleResult, 0, len(batch.Results))
	hasWarning := false
	for _, result := range batch.Results {
		state := inspectResult(result)
		level := resultSeverity(result, state)
		if level != severitySuccess {
			hasWarning = true
		}
		if options.Quiet && level == severitySuccess {
			continue
		}
		visible = append(visible, visibleResult{result: result, state: state, severity: level})
	}

	// Quiet is allowed to make a clean invocation completely silent. Terminal
	// cleanup counters are safety signals and override quiet even if a malformed
	// or partial batch did not carry the corresponding Result.
	metricsNeedAttention := metrics.Retracted > 0 || metrics.Stranded > 0
	if options.Quiet && len(visible) == 0 && !metricsNeedAttention {
		return nil
	}

	for index, item := range visible {
		if index > 0 {
			output.blank()
		}
		duration := ""
		if len(batch.Results) == 1 && metrics.Elapsed > 0 {
			duration = humanDuration(metrics.Elapsed)
		}
		writeResult(output, item.result, item.state, item.severity, duration, options.Verbose)
	}

	if len(visible) > 0 {
		output.blank()
	}
	writeBatchFooter(output, batch, metrics, hasWarning || metricsNeedAttention, options.Verbose)
	return output.err
}

type severity uint8

const (
	severitySuccess severity = iota
	severityWarning
	severityFailure
)

type resultState struct {
	stranded              []objectState
	retracted             []objectState
	registrationUncertain []objectState
	indeterminateFlows    []ingest.FlowResult
	unattemptedFlows      []ingest.FlowResult
	totalObjects          int
}

type objectState struct {
	flow   ingest.FlowResult
	object ingest.ObjectResult
}

func inspectResult(result ingest.Result) resultState {
	var state resultState
	for _, flow := range result.Flows {
		switch flow.Disposition {
		case ingest.FlowIndeterminate:
			state.indeterminateFlows = append(state.indeterminateFlows, flow)
		case ingest.FlowUnattempted:
			state.unattemptedFlows = append(state.unattemptedFlows, flow)
		}
		state.totalObjects += flowObjectCount(flow)
		for _, object := range flow.Objects {
			entry := objectState{flow: flow, object: object}
			switch object.Status {
			case ingest.ObjectStatusStranded:
				state.stranded = append(state.stranded, entry)
			case ingest.ObjectStatusRetracted:
				state.retracted = append(state.retracted, entry)
			case ingest.ObjectStatusRetractionIndeterminate:
				state.registrationUncertain = append(state.registrationUncertain, entry)
			}
		}
	}
	return state
}

func resultSeverity(result ingest.Result, state resultState) severity {
	if result.Status == ingest.ResultStatusFailed || len(state.stranded) > 0 ||
		len(state.registrationUncertain) > 0 || len(state.indeterminateFlows) > 0 ||
		result.Verification == ingest.VerificationFailedStranded {
		return severityFailure
	}
	if result.Status == ingest.ResultStatusPlanned {
		return severitySuccess
	}
	if result.Verification == ingest.VerificationNotRequested ||
		result.Verification == ingest.VerificationNotReached ||
		result.Verification == ingest.VerificationFailedRetracted ||
		result.Verification == ingest.VerificationFailedStranded ||
		len(state.retracted) > 0 || len(state.unattemptedFlows) > 0 {
		return severityWarning
	}
	return severitySuccess
}

func writeResult(output *humanWriter, result ingest.Result, state resultState, level severity, duration string, verbose bool) {
	heading := resultHeading(result, state)
	output.resultHeading(heading, inputLabel(result.Input), duration, level)
	output.blank()

	if len(result.Flows) > 0 {
		for _, flow := range orderedFlows(result) {
			writeFlow(output, result.RootFlowID, flow, verbose)
		}
	} else {
		output.line(2, "no Flow was committed")
	}

	writeAbnormalDetails(output, state)
	if result.Failure != nil {
		output.keyValue(2, "failure", result.Failure.Message)
		if verbose {
			output.keyValue(2, "failure code", result.Failure.Code)
		}
	}

	if len(result.Flows) > 0 || result.Failure != nil {
		output.blank()
	}
	if state.totalObjects > 0 {
		output.line(2, objectOutcomeSummary(result, state))
	}
	if profile := joinedVersion(result.Profile, result.ProfileVersion); profile != "" {
		output.keyValue(2, "profile", profile)
	}
	if result.SHA256 != "" {
		output.keyValue(2, "input SHA-256", result.SHA256)
	}

	if verbose {
		if result.Input != "" && result.Input != inputLabel(result.Input) {
			output.keyValue(2, "input", result.Input)
		}
		if result.Bytes > 0 {
			output.keyValue(2, "input size", humanBytes(result.Bytes))
		}
		if result.FFmpegVersion != "" {
			output.keyValue(2, "FFmpeg", result.FFmpegVersion)
		}
		if result.MediaToolchain != "" {
			output.keyValue(2, "media toolchain", result.MediaToolchain)
		}
	}
}

func resultHeading(result ingest.Result, state resultState) string {
	if len(state.stranded) > 0 || result.Verification == ingest.VerificationFailedStranded {
		return "INGEST FAILED - OBJECTS STRANDED: ACTION REQUIRED"
	}
	if len(state.registrationUncertain) > 0 {
		return "INGEST FAILED - OBJECT STATE INDETERMINATE: ACTION REQUIRED"
	}
	if len(state.indeterminateFlows) > 0 {
		return "INGEST FAILED - FLOW STATE INDETERMINATE: ACTION REQUIRED"
	}
	if result.Status == ingest.ResultStatusFailed {
		if result.Failure != nil && result.Failure.ActionRequired {
			return "INGEST FAILED - ACTION REQUIRED"
		}
		switch result.Verification {
		case ingest.VerificationFailedRetracted:
			return "INGEST FAILED - UNVERIFIED OBJECTS RETRACTED"
		case ingest.VerificationNotReached:
			return "INGEST FAILED - NOT VERIFIED"
		default:
			return "INGEST FAILED"
		}
	}

	action := strings.ToUpper(strings.TrimSpace(string(result.Status)))
	if action == "" {
		action = "INGEST COMPLETE"
	}
	if action == "PLANNED" {
		return "PLANNED - NO CHANGES MADE"
	}
	switch result.Verification {
	case ingest.VerificationVerified:
		return action + " AND VERIFIED"
	case ingest.VerificationNotRequested:
		return action + " - VERIFICATION NOT REQUESTED"
	case ingest.VerificationNotReached:
		return action + " - NOT VERIFIED"
	case ingest.VerificationFailedRetracted:
		return action + " - UNVERIFIED OBJECTS RETRACTED"
	case ingest.VerificationFailedStranded:
		return action + " - OBJECTS STRANDED: ACTION REQUIRED"
	default:
		return action
	}
}

func orderedFlows(result ingest.Result) []ingest.FlowResult {
	ordered := make([]ingest.FlowResult, 0, len(result.Flows))
	for _, flow := range result.Flows {
		if flow.FlowID == result.RootFlowID {
			ordered = append(ordered, flow)
		}
	}
	for _, flow := range result.Flows {
		if flow.FlowID != result.RootFlowID {
			ordered = append(ordered, flow)
		}
	}
	return ordered
}

func writeFlow(output *humanWriter, rootFlowID string, flow ingest.FlowResult, verbose bool) {
	objects := flowObjectCount(flow)
	label := strings.TrimSpace(flow.Role)
	switch flow.Kind {
	case ingest.FlowKindCollection:
		label = "collection"
	case ingest.FlowKindMuxed:
		label = "multiplex"
	}
	if flow.FlowID == rootFlowID && flow.Kind == "" {
		if objects == 0 {
			label = "collection"
		} else if label == "" {
			label = "flow"
		}
	} else if label == "" {
		label = "flow"
	}
	if label == "multi" && objects == 0 {
		label = "collection"
	}

	objectCount := ""
	if objects > 0 {
		objectCount = countLabel(objects, "media object", "media objects")
	}
	output.flow(label, flow.FlowID, objectCount)

	if verbose {
		if flow.SourceID != "" {
			output.keyValue(4, "source", flow.SourceID)
		}
		if flow.Disposition != "" {
			output.keyValue(4, "disposition", string(flow.Disposition))
		}
		for _, object := range flow.Objects {
			writeObject(output, object)
		}
		return
	}

	switch flow.Disposition {
	case ingest.FlowIndeterminate:
		output.line(4, "state indeterminate - inspect this Flow before retrying")
	case ingest.FlowUnattempted:
		output.line(4, "not attempted")
	}
	for _, object := range flow.Objects {
		switch object.Status {
		case ingest.ObjectStatusStranded, ingest.ObjectStatusRetracted, ingest.ObjectStatusRetractionIndeterminate:
			output.keyValue(4, string(object.Status)+" object", object.ObjectID)
		}
	}
}

func flowObjectCount(flow ingest.FlowResult) int {
	if flow.ObjectSummary.Total > 0 || len(flow.Objects) == 0 {
		return flow.ObjectSummary.Total
	}
	return len(flow.Objects)
}

func writeObject(output *humanWriter, object ingest.ObjectResult) {
	label := string(object.Status)
	if label == "" {
		label = "object"
	} else {
		label += " object"
	}
	output.keyValue(4, label, object.ObjectID)
	if object.Timerange != "" {
		output.keyValue(6, "timerange", object.Timerange)
	}
	if object.Bytes > 0 {
		output.keyValue(6, "size", humanBytes(object.Bytes))
	}
	if object.SHA256 != "" {
		output.keyValue(6, "SHA-256", object.SHA256)
	}
}

func writeAbnormalDetails(output *humanWriter, state resultState) {
	if len(state.stranded) > 0 {
		output.line(2, "Action: inspect or retract every stranded media object before retrying.")
	}
	if len(state.registrationUncertain) > 0 {
		output.line(2, "Action: determine whether each indeterminate media object was registered before retrying.")
	}
	if len(state.indeterminateFlows) > 0 {
		output.line(2, "Action: determine whether each indeterminate Flow update committed before retrying.")
	}
	if len(state.retracted) > 0 && len(state.stranded) == 0 {
		output.line(2, "Unverified media objects were retracted; correct the cause before retrying.")
	}
}

func objectOutcomeSummary(result ingest.Result, state resultState) string {
	count := countLabel(state.totalObjects, "media object", "media objects")
	if result.Status == ingest.ResultStatusPlanned {
		return count + " planned"
	}
	switch result.Verification {
	case ingest.VerificationVerified:
		return count + " verified"
	case ingest.VerificationNotRequested:
		return count + " stored; verification was not requested"
	case ingest.VerificationNotReached:
		return count + "; verification did not complete"
	case ingest.VerificationFailedRetracted:
		return count + "; unverified objects were retracted"
	case ingest.VerificationFailedStranded:
		return count + "; one or more unverified objects remain registered"
	default:
		return count
	}
}

func writeBatchFooter(output *humanWriter, batch ingest.BatchResult, metrics observability.Snapshot, hasWarning, verbose bool) {
	succeeded, failed := batchCounts(batch)
	total := succeeded + failed
	heading := "COMPLETE"
	level := severitySuccess
	summary := countLabel(succeeded, "input succeeded", "inputs succeeded")
	if total > 0 {
		summary = strconv.Itoa(succeeded) + "/" + strconv.Itoa(total) + " inputs succeeded"
	}
	if failed > 0 {
		level = severityFailure
		if succeeded == 0 {
			heading = "FAILED"
		} else {
			heading = "COMPLETE WITH ERRORS"
		}
		summary += "; " + countLabel(failed, "failed", "failed")
	} else if hasWarning {
		level = severityWarning
		heading = "COMPLETE WITH WARNINGS"
	}
	output.footerHeading(heading, summary, level)

	if metrics.BytesUploaded > 0 && metrics.BytesUploaded == metrics.BytesVerified {
		output.line(2, humanBytes(metrics.BytesUploaded)+" stored and verified")
	} else {
		if metrics.BytesUploaded > 0 {
			output.line(2, humanBytes(metrics.BytesUploaded)+" stored")
		}
		if metrics.BytesVerified > 0 {
			output.line(2, humanBytes(metrics.BytesVerified)+" verified by readback")
		}
	}
	if len(batch.Results) > 1 && metrics.Verified > 0 {
		output.line(2, countLabel64(metrics.Verified, "media object verified", "media objects verified"))
	}
	if metrics.Retries > 0 {
		output.line(2, countLabel64(metrics.Retries, "retry", "retries"))
	}
	if metrics.Retracted > 0 {
		output.line(2, countLabel64(metrics.Retracted, "media object retracted", "media objects retracted"))
	}
	if metrics.Stranded > 0 {
		output.line(2, countLabel64(metrics.Stranded, "media object stranded", "media objects stranded"))
	}
	if len(batch.Results) > 1 && metrics.Elapsed > 0 {
		output.keyValue(2, "elapsed", humanDuration(metrics.Elapsed))
	}
	if batch.RunID != "" {
		output.keyValue(2, "run", batch.RunID)
	}

	if verbose {
		if metrics.BytesStaged > 0 {
			output.keyValue(2, "staged", humanBytes(metrics.BytesStaged))
		}
		if batch.SchemaVersion != "" {
			output.keyValue(2, "result schema", batch.SchemaVersion)
		}
		if tool := joinedVersion(batch.ToolVersion, batch.ToolCommit); tool != "" {
			output.keyValue(2, "tool", tool)
		}
		if batch.ToolBuildDate != "" {
			output.keyValue(2, "tool build", batch.ToolBuildDate)
		}
		if batch.ProfileVersion != "" {
			output.keyValue(2, "profile contract", batch.ProfileVersion)
		}
	}
}

func batchCounts(batch ingest.BatchResult) (int, int) {
	if batch.Succeeded >= 0 && batch.Failed >= 0 && batch.Succeeded+batch.Failed == len(batch.Results) {
		return batch.Succeeded, batch.Failed
	}
	var succeeded, failed int
	for _, result := range batch.Results {
		if result.Status == ingest.ResultStatusFailed {
			failed++
		} else {
			succeeded++
		}
	}
	return succeeded, failed
}

func inputLabel(input string) string {
	parsed, err := url.Parse(input)
	if err == nil && parsed.Scheme == "file" && parsed.Path != "" {
		if name := filepath.Base(filepath.FromSlash(parsed.Path)); name != "." && name != string(filepath.Separator) {
			// url.Parse decodes Path escapes. Re-sanitize so a filename such as
			// %1B%5B31m cannot introduce a terminal control sequence.
			return sanitizeHumanText(name)
		}
	}
	return sanitizeHumanText(input)
}

func joinedVersion(name, version string) string {
	name = strings.TrimSpace(name)
	version = strings.TrimSpace(version)
	switch {
	case name == "":
		return version
	case version == "":
		return name
	default:
		return name + "@" + version
	}
}

func countLabel(count int, singular, plural string) string {
	if count == 1 {
		return "1 " + singular
	}
	return strconv.Itoa(count) + " " + plural
}

func countLabel64(count int64, singular, plural string) string {
	if count == 1 {
		return "1 " + singular
	}
	return strconv.FormatInt(count, 10) + " " + plural
}

func humanBytes(bytes int64) string {
	if bytes < 1024 {
		return strconv.FormatInt(bytes, 10) + " B"
	}
	value := float64(bytes)
	units := [...]string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	unit := units[0]
	for index := 0; index < len(units); index++ {
		value /= 1024
		unit = units[index]
		if value < 1024 || index == len(units)-1 {
			break
		}
	}
	precision := 2
	if value >= 100 {
		precision = 0
	} else if value >= 10 {
		precision = 1
	}
	formatted := strconv.FormatFloat(value, 'f', precision, 64)
	if strings.Contains(formatted, ".") {
		formatted = strings.TrimRight(strings.TrimRight(formatted, "0"), ".")
	}
	return formatted + " " + unit
}

func humanDuration(duration time.Duration) string {
	if duration < 0 {
		duration = 0
	}
	resolution := time.Second
	if duration < time.Second {
		resolution = time.Millisecond
	} else if duration < time.Minute {
		resolution = 100 * time.Millisecond
	}
	return duration.Round(resolution).String()
}

type humanWriter struct {
	writer io.Writer
	width  int
	color  bool
	err    error
}

func (w *humanWriter) blank() {
	w.writePhysical("")
}

func (w *humanWriter) line(indent int, text string) {
	if w.err != nil {
		return
	}
	text = sanitizeHumanText(text)
	indent = min(max(indent, 0), max(w.width-1, 0))
	prefix := strings.Repeat(" ", indent)
	for _, line := range wrap(text, max(w.width-indent, 1)) {
		w.writePhysical(prefix + line)
	}
}

func (w *humanWriter) keyValue(indent int, key, value string) {
	key = strings.TrimSpace(sanitizeHumanText(key))
	value = strings.TrimSpace(sanitizeHumanText(value))
	if key == "" {
		w.line(indent, value)
		return
	}
	if value == "" {
		w.line(indent, key)
		return
	}
	combined := key + " " + value
	if visibleWidth(strings.Repeat(" ", indent)+combined) <= w.width {
		w.line(indent, combined)
		return
	}
	w.line(indent, key)
	valueIndent := indent + 2
	if visibleWidth(value)+valueIndent > w.width && visibleWidth(value)+indent <= w.width {
		// Keep UUIDs intact at 40 columns: once their label has moved to its
		// own line, extra decorative indentation must not force the value to
		// wrap.
		valueIndent = indent
	}
	w.line(valueIndent, value)
}

func (w *humanWriter) flow(label, id, objects string) {
	label, id, objects = sanitizeHumanText(label), sanitizeHumanText(id), sanitizeHumanText(objects)
	parts := []string{label}
	if id != "" {
		parts = append(parts, id)
	}
	if objects != "" {
		parts = append(parts, objects)
	}
	combined := strings.Join(parts, "  ")
	if visibleWidth("  "+combined) <= w.width {
		w.line(2, combined)
		return
	}
	w.line(2, label)
	if id != "" {
		w.line(4, id)
	}
	if objects != "" {
		w.line(4, objects)
	}
}

func (w *humanWriter) resultHeading(heading, input, duration string, level severity) {
	heading, input, duration = sanitizeHumanText(heading), sanitizeHumanText(input), sanitizeHumanText(duration)
	plain := heading
	if input != "" {
		plain += "  " + input
	}
	if duration != "" {
		plain += "  " + duration
	}
	if visibleWidth(plain) <= w.width {
		w.writeStatusLine(heading, strings.TrimPrefix(plain, heading), level)
		return
	}
	w.writeStatusHeading(heading, level)
	if input != "" {
		w.keyValue(2, "input", input)
	}
	if duration != "" {
		w.keyValue(2, "elapsed", duration)
	}
}

func (w *humanWriter) footerHeading(heading, summary string, level severity) {
	heading, summary = sanitizeHumanText(heading), sanitizeHumanText(summary)
	plain := heading
	if summary != "" {
		plain += "  " + summary
	}
	if visibleWidth(plain) <= w.width {
		w.writeStatusLine(heading, strings.TrimPrefix(plain, heading), level)
		return
	}
	w.writeStatusHeading(heading, level)
	if summary != "" {
		w.line(2, summary)
	}
}

func (w *humanWriter) writeStatusHeading(heading string, level severity) {
	for _, line := range wrap(heading, w.width) {
		w.writeStatusLine(line, "", level)
	}
}

func (w *humanWriter) writeStatusLine(heading, suffix string, level severity) {
	heading, suffix = sanitizeHumanText(heading), sanitizeHumanText(suffix)
	if !w.color {
		w.writePhysical(heading + suffix)
		return
	}
	code := "\x1b[1;32m"
	switch level {
	case severityWarning:
		code = "\x1b[1;33m"
	case severityFailure:
		code = "\x1b[1;31m"
	}
	w.writePhysical(code + heading + "\x1b[0m" + suffix)
}

func sanitizeHumanText(value string) string {
	return strings.Map(func(character rune) rune {
		if unicode.IsControl(character) {
			return ' '
		}
		return character
	}, value)
}

func (w *humanWriter) writePhysical(text string) {
	if w.err != nil {
		return
	}
	_, w.err = fmt.Fprintln(w.writer, text)
}

func wrap(text string, width int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return []string{""}
	}
	width = max(width, 1)
	words := strings.Fields(text)
	lines := make([]string, 0, 1)
	current := ""
	for _, word := range words {
		for visibleWidth(word) > width {
			if current != "" {
				lines = append(lines, current)
				current = ""
			}
			prefix, rest := splitRunes(word, width)
			lines = append(lines, prefix)
			word = rest
		}
		if word == "" {
			continue
		}
		if current == "" {
			current = word
			continue
		}
		if visibleWidth(current)+1+visibleWidth(word) <= width {
			current += " " + word
			continue
		}
		lines = append(lines, current)
		current = word
	}
	if current != "" {
		lines = append(lines, current)
	}
	return lines
}

func splitRunes(text string, columns int) (string, string) {
	if columns < 1 {
		return "", text
	}
	used := 0
	for index, character := range text {
		characterWidth := runeWidth(character)
		if used+characterWidth > columns {
			if index == 0 {
				_, size := utf8.DecodeRuneInString(text)
				return text[:size], text[size:]
			}
			return text[:index], text[index:]
		}
		used += characterWidth
	}
	return text, ""
}

func visibleWidth(text string) int {
	width := 0
	for _, character := range text {
		width += runeWidth(character)
	}
	return width
}

// runeWidth implements the small terminal-width subset this renderer needs
// without adding a dependency solely for presentation. It covers combining
// marks and the East Asian/emoji ranges which otherwise make a Unicode input
// label overflow even when its rune count fits.
func runeWidth(character rune) int {
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
