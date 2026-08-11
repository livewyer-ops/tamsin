package resultjournal

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/google/uuid"
	"github.com/livewyer-ops/tamsin/internal/ingest"
	"github.com/livewyer-ops/tamsin/internal/source"
)

type syncWriter interface {
	io.Writer
	Sync() error
}

// Writer records one invocation as JSON Lines in a newly created file. Every
// line is synced before the method returns. The start record makes the complete
// input manifest durable before work is scheduled; an interrupted run is
// recognizable by missing result indexes and the absence of its summary.
type Writer struct {
	mu       sync.Mutex
	sink     syncWriter
	closer   io.Closer
	contract ingest.ResultContract
	total    int
	inputs   []string
	seen     map[int]struct{}
	failed   error
	final    bool
	closed   bool
}

type recordMetadata struct {
	SchemaVersion  string `json:"schema_version"`
	ToolVersion    string `json:"tool_version"`
	ToolCommit     string `json:"tool_commit"`
	ToolBuildDate  string `json:"tool_build_date,omitempty"`
	ProfileVersion string `json:"profile_version"`
	RunID          string `json:"run_id"`
}

type manifestInput struct {
	Index int    `json:"index"`
	Input string `json:"input"`
}

type startRecord struct {
	recordMetadata
	RecordType string          `json:"record_type"`
	Total      int             `json:"total"`
	Inputs     []manifestInput `json:"inputs"`
}

type objectRecord struct {
	recordMetadata
	RecordType string              `json:"record_type"`
	Index      int                 `json:"index"`
	FlowID     string              `json:"flow_id"`
	Object     ingest.ObjectResult `json:"object"`
}

type inputRecord struct {
	recordMetadata
	RecordType string        `json:"record_type"`
	Index      int           `json:"index"`
	Result     ingest.Result `json:"result"`
}

type summary struct {
	Total     int    `json:"total"`
	Succeeded int    `json:"succeeded"`
	Failed    int    `json:"failed"`
	Outcome   string `json:"outcome"`
}

type summaryRecord struct {
	recordMetadata
	RecordType string  `json:"record_type"`
	Summary    summary `json:"summary"`
}

func Open(path string, contract ingest.ResultContract, items []source.Item) (*Writer, error) {
	// A journal is one invocation's recovery artifact. Refusing every existing
	// pathname (including symlinks) prevents accidental corruption of an
	// arbitrary file and avoids pretending concurrent appenders are coordinated.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open result journal: %w", err)
	}
	inputs := make([]string, len(items))
	for index, item := range items {
		inputs[index] = ingest.SafeInputURI(item.URI)
	}
	writer, err := New(file, contract, inputs)
	if err != nil {
		return nil, cleanupFailedOpen(file, path, err)
	}
	writer.closer = file
	// Sync the directory after the start record itself is synced. On return, the
	// pathname and the invocation manifest are both durable before scheduling.
	if runtime.GOOS != "windows" {
		directory, openErr := os.Open(filepath.Dir(path))
		if openErr != nil {
			return nil, cleanupFailedOpen(file, path, fmt.Errorf("open result journal directory: %w", openErr))
		}
		syncErr := directory.Sync()
		closeErr := directory.Close()
		if syncErr != nil || closeErr != nil {
			return nil, cleanupFailedOpen(file, path,
				fmt.Errorf("sync result journal directory: %w", errors.Join(syncErr, closeErr)))
		}
	}
	return writer, nil
}

// cleanupFailedOpen removes only the path this invocation created with
// O_EXCL. Open has not returned, so no work can have been scheduled and the
// incomplete file is not a useful recovery artifact. Closing first also makes
// the cleanup work on platforms that do not permit unlinking an open file.
func cleanupFailedOpen(file *os.File, path string, cause error) error {
	closeErr := file.Close()
	removeErr := os.Remove(path)
	if closeErr != nil {
		closeErr = fmt.Errorf("close incomplete result journal: %w", closeErr)
	}
	if removeErr != nil {
		removeErr = fmt.Errorf("remove incomplete result journal: %w", removeErr)
	}
	return errors.Join(cause, closeErr, removeErr)
}

// New is exposed for deterministic durability and failure tests. Production
// callers normally use Open.
func New(sink syncWriter, contract ingest.ResultContract, inputs []string) (*Writer, error) {
	if sink == nil {
		return nil, errors.New("result journal sink is required")
	}
	if contract.SchemaVersion == "" || contract.ToolVersion == "" || contract.ToolCommit == "" || contract.ProfileVersion == "" {
		return nil, errors.New("result journal contract versions are required")
	}
	if _, err := uuid.Parse(contract.RunID); err != nil {
		return nil, fmt.Errorf("result journal run ID must be a UUID: %w", err)
	}
	writer := &Writer{
		sink: sink, contract: contract, total: len(inputs),
		inputs: append([]string(nil), inputs...), seen: make(map[int]struct{}),
	}
	manifest := make([]manifestInput, len(inputs))
	for index, input := range inputs {
		manifest[index] = manifestInput{Index: index, Input: input}
	}
	if err := writeSyncedJSON(sink, startRecord{
		recordMetadata: writer.metadata(), RecordType: "start", Total: len(inputs), Inputs: manifest,
	}); err != nil {
		writer.failed = fmt.Errorf("write result journal start record: %w", err)
		return nil, writer.failed
	}
	return writer, nil
}

// WriteObjectBatch appends one record per terminal Object and performs one
// durability sync for the complete committed batch.
func (w *Writer) WriteObjectBatch(index int, flowID string, objects []ingest.ObjectResult) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ready(); err != nil {
		return err
	}
	if index < 0 || index >= w.total {
		return fmt.Errorf("result journal index %d is outside the %d-input manifest", index, w.total)
	}
	if _, err := uuid.Parse(flowID); err != nil {
		return fmt.Errorf("result journal Flow ID must be a UUID: %w", err)
	}
	if len(objects) == 0 {
		return nil
	}
	records := make([]any, len(objects))
	for objectIndex, object := range objects {
		if _, err := uuid.Parse(object.ObjectID); err != nil {
			return fmt.Errorf("result journal Object ID must be a UUID: %w", err)
		}
		records[objectIndex] = objectRecord{
			recordMetadata: w.metadata(), RecordType: "object", Index: index,
			FlowID: flowID, Object: object,
		}
	}
	if err := writeSyncedJSONBatch(w.sink, records); err != nil {
		w.failed = fmt.Errorf("write result journal Object batch for input %d: %w", index, err)
		return w.failed
	}
	return nil
}

func (w *Writer) WriteInput(index int, result ingest.Result) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ready(); err != nil {
		return err
	}
	if index < 0 || index >= w.total {
		return fmt.Errorf("result journal index %d is outside the %d-input manifest", index, w.total)
	}
	if result.Input != w.inputs[index] {
		return fmt.Errorf("result journal input %d does not match its start manifest", index)
	}
	if _, exists := w.seen[index]; exists {
		return fmt.Errorf("result journal already contains input %d for run %s", index, w.contract.RunID)
	}
	record := inputRecord{
		recordMetadata: w.metadata(), RecordType: "input", Index: index, Result: result,
	}
	if err := writeSyncedJSON(w.sink, record); err != nil {
		w.failed = fmt.Errorf("write result journal input %d: %w", index, err)
		return w.failed
	}
	w.seen[index] = struct{}{}
	return nil
}

// WriteResult remains as a source-compatible alias for library callers. Its
// durable v2 record type is input.
func (w *Writer) WriteResult(index int, result ingest.Result) error {
	return w.WriteInput(index, result)
}

func (w *Writer) WriteSummary(batch ingest.BatchResult, runErr error, interrupted bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ready(); err != nil {
		return err
	}
	if batch.SchemaVersion != w.contract.SchemaVersion || batch.ToolVersion != w.contract.ToolVersion ||
		batch.ToolCommit != w.contract.ToolCommit || batch.ToolBuildDate != w.contract.ToolBuildDate ||
		batch.ProfileVersion != w.contract.ProfileVersion || batch.RunID != w.contract.RunID {
		return errors.New("result journal summary contract does not match its result records")
	}
	if len(batch.Results) != w.total || len(w.seen) != len(batch.Results) {
		return fmt.Errorf("result journal has %d terminal inputs, batch has %d", len(w.seen), len(batch.Results))
	}
	for index := range batch.Results {
		if _, exists := w.seen[index]; !exists {
			return fmt.Errorf("result journal is missing terminal input %d", index)
		}
	}
	if batch.Succeeded < 0 || batch.Failed < 0 || batch.Succeeded+batch.Failed != len(batch.Results) {
		return errors.New("result journal summary counts do not cover the batch")
	}
	outcome := "completed"
	if interrupted {
		outcome = "interrupted"
	} else if runErr != nil {
		outcome = "failed"
	}
	record := summaryRecord{
		recordMetadata: w.metadata(), RecordType: "summary",
		Summary: summary{Total: len(batch.Results), Succeeded: batch.Succeeded, Failed: batch.Failed, Outcome: outcome},
	}
	if err := writeSyncedJSON(w.sink, record); err != nil {
		w.failed = fmt.Errorf("write result journal summary: %w", err)
		return w.failed
	}
	w.final = true
	return nil
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.closer != nil {
		return w.closer.Close()
	}
	return nil
}

func (w *Writer) metadata() recordMetadata {
	return recordMetadata{
		SchemaVersion: w.contract.SchemaVersion, ToolVersion: w.contract.ToolVersion,
		ToolCommit: w.contract.ToolCommit, ToolBuildDate: w.contract.ToolBuildDate,
		ProfileVersion: w.contract.ProfileVersion, RunID: w.contract.RunID,
	}
}

func (w *Writer) ready() error {
	if w.closed {
		return errors.New("result journal is closed")
	}
	if w.failed != nil {
		return fmt.Errorf("result journal previously failed: %w", w.failed)
	}
	if w.final {
		return errors.New("result journal summary has already been written")
	}
	return nil
}

func writeSyncedJSON(sink syncWriter, value any) error {
	return writeSyncedJSONBatch(sink, []any{value})
}

func writeSyncedJSONBatch(sink syncWriter, values []any) error {
	var batch []byte
	for _, value := range values {
		line, err := json.Marshal(value)
		if err != nil {
			return err
		}
		line = append(line, '\n')
		batch = append(batch, line...)
	}
	for len(batch) > 0 {
		written, writeErr := sink.Write(batch)
		if writeErr != nil {
			return writeErr
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		batch = batch[written:]
	}
	return sink.Sync()
}
