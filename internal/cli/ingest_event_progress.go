package cli

import (
	"sort"
)

// flushProgressLoop drains a capacity-one wake signal into a latest-value
// mailbox. A slow pipe therefore delays only this goroutine; transfer workers
// continue replacing cumulative progress while lossless lifecycle records keep
// their synchronous write acknowledgement.
func (o *ingestEventOutput) flushProgressLoop() {
	defer close(o.progressDone)
	for {
		select {
		case <-o.progressStop:
			return
		case <-o.progressWake:
			select {
			case <-o.progressStop:
				return
			default:
			}
			o.mu.Lock()
			if o.err == nil && !o.finished {
				_ = o.flushProgressLocked(nil)
			}
			o.mu.Unlock()
		}
	}
}

// flushProgressLocked emits the latest retained snapshots, optionally for one
// input. Store and verify share an input revision counter, so retained values
// are ordered by revision rather than phase before reaching the reducer.
func (o *ingestEventOutput) flushProgressLocked(input *int) error {
	o.progressMu.Lock()
	pending := make([]pendingProgressEvent, 0, len(o.pendingProgress))
	for key, event := range o.pendingProgress {
		if input != nil && key.input != *input {
			continue
		}
		pending = append(pending, event)
		delete(o.pendingProgress, key)
	}
	o.progressMu.Unlock()

	sort.Slice(pending, func(left, right int) bool {
		leftIndex, rightIndex := *pending[left].scope.InputIndex, *pending[right].scope.InputIndex
		if leftIndex != rightIndex {
			return leftIndex < rightIndex
		}
		if pending[left].event.Revision != pending[right].event.Revision {
			return pending[left].event.Revision < pending[right].event.Revision
		}
		return pending[left].event.Phase < pending[right].event.Phase
	})
	for _, queued := range pending {
		if err := o.emitLocked(queued.scope, queued.event); err != nil {
			return err
		}
	}
	return nil
}

func (o *ingestEventOutput) sealProgressInputLocked(index int) {
	o.progressMu.Lock()
	o.progressSealed[index] = true
	o.progressMu.Unlock()
}

func (o *ingestEventOutput) sealAllProgressLocked() {
	o.progressMu.Lock()
	for index := range o.declared {
		o.progressSealed[index] = true
	}
	o.progressMu.Unlock()
}

func (o *ingestEventOutput) stopProgressLocked() {
	o.progressMu.Lock()
	if !o.progressStopped {
		o.progressStopped = true
		close(o.progressStop)
	}
	o.progressMu.Unlock()
}
