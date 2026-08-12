package ingest

import (
	"context"
	"errors"
	"fmt"
)

type flowRegistrationTarget struct {
	flowID      string
	resultIndex int
	objects     []preparedObject
	role        string
}

type flowExecutionState struct {
	graph      flowGraph
	inputURI   string
	targets    []flowRegistrationTarget
	storageID  string
	results    []FlowResult
	allObjects []preparedObject
	planned    []plannedFlowWrite
}

// executeFlowPlan commits one input's Flow graph. The phases run in a fixed
// order: nothing may be written before the graph is planned and validated, and
// no Media Object may be registered against a Flow that was not written first.
func (p *Pipeline) executeFlowPlan(ctx context.Context, inputURI string, graph flowGraph, storageID string, targets []flowRegistrationTarget, results []FlowResult) error {
	state := flowExecutionState{
		graph:     graph,
		inputURI:  inputURI,
		storageID: storageID,
		targets:   targets,
		results:   results,
	}
	if err := planFlowWrites(ctx, p, &state); err != nil {
		return err
	}
	if err := awaitFlowTransfers(ctx, p, &state); err != nil {
		return err
	}
	if err := writeFlowGraph(ctx, p, &state); err != nil {
		return err
	}
	if err := registerFlowObjects(ctx, p, &state); err != nil {
		return errors.Join(err, p.recoverFlowStatuses(ctx, graph))
	}
	if err := p.setFlowGraphStatus(ctx, graph, flowStatusClosedComplete); err != nil {
		recoveryErr := p.recoverFlowStatuses(ctx, graph)
		return withFailure(FailureCodeFlowWriteFailed, FailureMessageFlowWriteFailed, true,
			errors.Join(fmt.Errorf("close completed Flow graph: %w", err), recoveryErr))
	}
	return nil
}

func planFlowWrites(ctx context.Context, p *Pipeline, state *flowExecutionState) error {
	planned, err := p.planFlowGraph(ctx, state.graph)
	if err != nil {
		return withFailure(FailureCodeFlowPlanFailed, FailureMessageFlowPlanFailed, true, err)
	}
	state.planned = planned
	if err := p.observeFlowPlans(ctx, state.graph, planned, state.results); err != nil {
		return err
	}
	state.allObjects = collectPlannedFlowObjects(state.targets)
	return nil
}

func awaitFlowTransfers(ctx context.Context, p *Pipeline, state *flowExecutionState) error {
	if p.config.DryRun {
		return nil
	}
	p.expectTransfers(ctx, state.allObjects)
	return nil
}

func writeFlowGraph(ctx context.Context, p *Pipeline, state *flowExecutionState) error {
	if p.config.DryRun {
		return nil
	}
	if err := p.commitFlowGraphObserved(ctx, state.planned, state.results); err != nil {
		return withFailure(FailureCodeFlowWriteFailed, FailureMessageFlowWriteFailed, true, err)
	}
	return nil
}

func registerFlowObjects(ctx context.Context, p *Pipeline, state *flowExecutionState) error {
	if p.config.DryRun {
		return nil
	}
	for _, target := range state.targets {
		if target.resultIndex < 0 || target.resultIndex >= len(state.results) {
			return fmt.Errorf("internal flow target index %d is outside results length %d", target.resultIndex, len(state.results))
		}
		if target.role != "" {
			p.logger.Info("ingesting essence",
				"input", state.inputURI, "flow_id", target.flowID, "role", target.role)
		}
		if err := p.registerFlow(ctx, target.flowID, target.objects, state.results[target.resultIndex].Objects, state.storageID); err != nil {
			return withFailure(FailureCodeTAMSRegistrationFailed, FailureMessageTAMSRegistrationFailed, true, err)
		}
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

func flowExecutionTarget(flowID string, resultIndex int, objects []preparedObject, role string) flowRegistrationTarget {
	return flowRegistrationTarget{flowID: flowID, resultIndex: resultIndex, objects: objects, role: role}
}
