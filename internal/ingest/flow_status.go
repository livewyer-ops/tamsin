package ingest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/livewyer-ops/tamsin/internal/tamsschema"
)

const (
	flowStatusAwaitingContent = "awaiting_content"
	flowStatusIngesting       = "ingesting"
	flowStatusClosedComplete  = "closed_complete"
)

// Each input owns its Flow status changes while it holds the graph lock.
type flowChanges map[string]struct {
	status string
	opened bool
}

// setFlowStatus performs a profile-safe status transition. Profile-backed GET
// responses are expanded, but their PUT form must contain profile_id instead
// of technical metadata, so every lifecycle write uses the same projection as
// initial Flow creation.
func (p *Pipeline) setFlowStatus(ctx context.Context, flowID, status string, changes flowChanges) error {
	if p.config.DryRunMode != DryRunOff || !p.apiVersion.SupportsFlowProfiles() {
		return nil
	}
	change := changes[flowID]
	if change.status == status {
		return nil
	}

	existing, err := p.client.Flow(ctx, flowID)
	if err != nil {
		return fmt.Errorf("read Flow %s for status transition: %w", flowID, err)
	}
	if status == flowStatusClosedComplete && stringField(existing, "status") == "replication_in_progress" {
		return nil
	}
	if stringField(existing, "status") == status {
		change.status = status
		changes[flowID] = change
		return nil
	}
	effective := maps.Clone(existing)
	effective["status"] = status
	profileID := stringField(existing, "profile_id")
	request := flowPutProjection(effective, profileID)
	if p.storeIsNewerMinor() {
		// The Flow came from a store newer than the pinned schemas; only the
		// value Tamsin is changing is checked.
		if err := tamsschema.ValidateFlowStatus(p.apiVersion, status); err != nil {
			return fmt.Errorf("flow %s status %s is not a pinned status value at %w", flowID, status, err)
		}
	} else {
		if err := tamsschema.ValidateFlowGet(p.apiVersion, effective); err != nil {
			return fmt.Errorf("flow %s status %s produces invalid expanded metadata at %w", flowID, status, err)
		}
		if err := tamsschema.ValidateFlowPut(p.apiVersion, request); err != nil {
			return fmt.Errorf("flow %s status %s produces invalid PUT metadata at %w", flowID, status, err)
		}
	}
	if _, err := p.client.PutFlow(ctx, flowID, request); err != nil {
		return fmt.Errorf("set Flow %s status to %s: %w", flowID, status, err)
	}
	change.status = status
	change.opened = change.opened || status == flowStatusIngesting
	changes[flowID] = change
	return nil
}

func (p *Pipeline) setFlowGraphStatus(ctx context.Context, graph flowGraph, status string) error {
	var failures []error
	for _, member := range graph.flows {
		if err := p.setFlowStatus(ctx, member.id, status, graph.changes); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// recoverFlowStatuses gets a short cancellation-independent window to leave a
// failed or interrupted graph honest. It never masks the primary ingest error.
func (p *Pipeline) recoverFlowStatuses(ctx context.Context, graph flowGraph) error {
	if p.config.DryRunMode != DryRunOff || !p.apiVersion.SupportsFlowProfiles() {
		return nil
	}
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	// Only a Flow this run created or moved to ingesting is restored: a
	// resume that changed nothing leaves closed_complete Flows, and another
	// client's replication_in_progress, exactly as it found them. The Flow
	// that owns the media is restored first, so a short window fixes what
	// matters.
	var failures []error
	for _, member := range graph.openedFlows() {
		if err := p.setFlowStatus(recoveryCtx, member.id, flowStatusAwaitingContent, graph.changes); err != nil {
			failures = append(failures, err)
		}
	}
	if err := errors.Join(failures...); err != nil {
		return fmt.Errorf("restore failed Flow graph to awaiting_content: %w", err)
	}
	return nil
}

func (graph flowGraph) openedFlows() []graphFlow {
	var owners, others []graphFlow
	for _, member := range graph.flows {
		if !graph.changes[member.id].opened {
			continue
		}
		if member.ownsMedia {
			owners = append(owners, member)
		} else {
			others = append(others, member)
		}
	}
	return append(owners, others...)
}

// finishFlowStatus closes a written graph whose ingest succeeded, or leaves a
// failed one awaiting content.
func (p *Pipeline) finishFlowStatus(ctx context.Context, graph flowGraph, graphName string, returnErr *error) {
	if *returnErr != nil {
		*returnErr = errors.Join(*returnErr, p.recoverFlowStatuses(ctx, graph))
		return
	}
	if err := p.setFlowGraphStatus(ctx, graph, flowStatusClosedComplete); err != nil {
		recoveryErr := p.recoverFlowStatuses(ctx, graph)
		*returnErr = withFailure(FailureCodeFlowWriteFailed, FailureMessageFlowWriteFailed, true,
			errors.Join(fmt.Errorf("close completed %s: %w", graphName, err), recoveryErr))
	}
}
