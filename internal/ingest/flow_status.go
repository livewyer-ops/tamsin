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

// setFlowStatus performs a profile-safe status transition. Profile-backed GET
// responses are expanded, but their PUT form must contain profile_id instead
// of technical metadata, so every lifecycle write uses the same projection as
// initial Flow creation.
func (p *Pipeline) setFlowStatus(ctx context.Context, flowID, status string) error {
	if p.config.DryRunMode != DryRunOff || !p.apiVersion.SupportsFlowProfiles() {
		return nil
	}
	p.flowStatusMu.Lock()
	if p.flowStatuses[flowID] == status {
		p.flowStatusMu.Unlock()
		return nil
	}
	p.flowStatusMu.Unlock()

	existing, err := p.client.Flow(ctx, flowID)
	if err != nil {
		return fmt.Errorf("read Flow %s for status transition: %w", flowID, err)
	}
	if stringField(existing, "status") == status {
		p.rememberFlowStatus(flowID, status)
		return nil
	}
	effective := maps.Clone(existing)
	effective["status"] = status
	profileID := stringField(existing, "profile_id")
	request := flowPutProjection(effective, profileID)
	if err := tamsschema.ValidateFlowGet(p.apiVersion, effective); err != nil {
		return fmt.Errorf("flow %s status %s produces invalid expanded metadata at %w", flowID, status, err)
	}
	if err := tamsschema.ValidateFlowPut(p.apiVersion, request); err != nil {
		return fmt.Errorf("flow %s status %s produces invalid PUT metadata at %w", flowID, status, err)
	}
	if _, err := p.client.PutFlow(ctx, flowID, request); err != nil {
		return fmt.Errorf("set Flow %s status to %s: %w", flowID, status, err)
	}
	p.rememberFlowStatus(flowID, status)
	return nil
}

func (p *Pipeline) rememberFlowStatus(flowID, status string) {
	p.flowStatusMu.Lock()
	p.flowStatuses[flowID] = status
	p.flowStatusMu.Unlock()
}

func (p *Pipeline) setFlowGraphStatus(ctx context.Context, graph flowGraph, status string) error {
	var failures []error
	for _, member := range graph.flows {
		if err := p.setFlowStatus(ctx, member.id, status); err != nil {
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
	if err := p.setFlowGraphStatus(recoveryCtx, graph, flowStatusAwaitingContent); err != nil {
		return fmt.Errorf("restore failed Flow graph to awaiting_content: %w", err)
	}
	return nil
}

func (p *Pipeline) finishRollingFlowStatus(ctx context.Context, graph flowGraph, returnErr *error) {
	if *returnErr != nil {
		*returnErr = errors.Join(*returnErr, p.recoverFlowStatuses(ctx, graph))
		return
	}
	if err := p.setFlowGraphStatus(ctx, graph, flowStatusClosedComplete); err != nil {
		recoveryErr := p.recoverFlowStatuses(ctx, graph)
		*returnErr = withFailure(FailureCodeFlowWriteFailed, FailureMessageFlowWriteFailed, true,
			errors.Join(fmt.Errorf("close completed rolling Flow graph: %w", err), recoveryErr))
	}
}
