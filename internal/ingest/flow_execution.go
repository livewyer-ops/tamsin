package ingest

import (
	"context"
	"errors"

	"github.com/livewyer-ops/tamsin/internal/source"
)

type flowRegistrationTarget struct {
	flowID      string
	resultIndex int
	objects     []preparedObject
	role        string
}

// executeFlowPlan commits one input's Flow graph. The phases run in a fixed
// order: nothing may be written before the graph is planned and validated, and
// no Media Object may be registered against a Flow that was not written first.
func (p *Pipeline) executeFlowPlan(ctx context.Context, inputURI string, graph flowGraph, storageID string,
	targets []flowRegistrationTarget, results []FlowResult) (returnErr error) {
	planned, err := p.planFlowWrites(ctx, graph, results)
	if err != nil {
		return err
	}
	if p.config.DryRunMode != DryRunOff {
		return nil
	}
	p.expectTransfers(ctx, collectPlannedFlowObjects(targets))
	if err := p.writeFlowGraph(ctx, planned, results); err != nil {
		return err
	}
	defer p.finishFlowStatus(ctx, graph, "Flow graph", &returnErr)
	for _, target := range targets {
		if target.role != "" {
			p.logger.Info("ingesting essence", "input", inputURI, "flow_id", target.flowID, "role", target.role)
		}
		if err := p.registerFlow(ctx, target.flowID, target.objects, results[target.resultIndex].Objects, storageID); err != nil {
			return withFailure(FailureCodeTAMSRegistrationFailed, FailureMessageTAMSRegistrationFailed, true, err)
		}
	}
	return nil
}

func (p *Pipeline) planFlowWrites(ctx context.Context, graph flowGraph, results []FlowResult) ([]plannedFlowWrite, error) {
	planned, err := p.planFlowGraph(ctx, graph)
	if err != nil {
		var unavailable *source.StreamUnavailableError
		if errors.As(err, &unavailable) {
			return nil, classifyPrepareFailure(err)
		}
		return nil, withFailure(FailureCodeFlowPlanFailed, FailureMessageFlowPlanFailed, true, err)
	}
	return planned, p.observeFlowPlans(ctx, graph, planned, results)
}

func (p *Pipeline) writeFlowGraph(ctx context.Context, planned []plannedFlowWrite, results []FlowResult) error {
	if p.config.DryRunMode != DryRunOff {
		return nil
	}
	if err := p.commitFlowGraphObserved(ctx, planned, results); err != nil {
		return withFailure(FailureCodeFlowWriteFailed, FailureMessageFlowWriteFailed, true, err)
	}
	return nil
}

func collectPlannedFlowObjects(targets []flowRegistrationTarget) []preparedObject {
	total := 0
	for index := range targets {
		total += len(targets[index].objects)
	}
	objects := make([]preparedObject, 0, total)
	for _, target := range targets {
		objects = append(objects, target.objects...)
	}
	return objects
}
