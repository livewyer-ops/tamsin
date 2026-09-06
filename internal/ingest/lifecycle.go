package ingest

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// FlowKind is the media ownership role a Flow has in one ingest graph. It is
// deliberately explicit: an empty Result.Role cannot distinguish a
// mono-essence root Flow from a muxed Flow which owns the same Object count.
type FlowKind string

const (
	FlowKindEssence    FlowKind = "essence"
	FlowKindCollection FlowKind = "collection"
	FlowKindMuxed      FlowKind = "muxed"
)

// FlowPlan is the safe, immutable projection published after the complete
// effective Flow graph validates and before any remote mutation or Object
// transfer begins. Exact Object totals arrive incrementally instead of making
// planning retain or predict an entire rendered output.
type FlowPlan struct {
	FlowID            string
	SourceID          string
	Kind              FlowKind
	Role              string
	Root              bool
	ParentFlowID      string
	Format            string
	Container         string
	TAMSFlowProfileID string
}

// LifecycleObserver separates durable process lifecycle from optional
// progress. Implementations must be concurrency-safe; returning an error asks
// the Pipeline to stop scheduling work while it still reports every terminal
// Result during reconciliation.
type LifecycleObserver interface {
	InputStarted(index int) error
	FlowPlanned(index int, plan FlowPlan) error
	ObjectsCompleted(index int, flowID string, objects []ObjectResult) error
}

type inputIndexContextKey struct{}

func withInputIndex(ctx context.Context, index int) context.Context {
	return context.WithValue(ctx, inputIndexContextKey{}, index)
}

func inputIndexFromContext(ctx context.Context) (int, bool) {
	index, ok := ctx.Value(inputIndexContextKey{}).(int)
	return index, ok
}

func (p *Pipeline) observeInputStarted(index int) error {
	if p.config.LifecycleObserver == nil {
		return nil
	}
	if err := p.config.LifecycleObserver.InputStarted(index); err != nil {
		return withFailure(FailureCodeOutputFailed, FailureMessageOutputFailed, false,
			fmt.Errorf("publish input start: %w", err))
	}
	return nil
}

func (p *Pipeline) observeObjectBatch(ctx context.Context, flowID string,
	results []ObjectResult, objectIDs map[string]struct{}) error {
	if p.config.LifecycleObserver == nil {
		return nil
	}
	index, ok := inputIndexFromContext(ctx)
	if !ok {
		return errors.New("publish Object results: input index is missing")
	}
	batch := make([]ObjectResult, 0, len(objectIDs))
	for resultIndex := range results {
		if results[resultIndex].reported {
			continue
		}
		if _, selected := objectIDs[results[resultIndex].ObjectID]; !selected {
			continue
		}
		finalizeObjectResult(&results[resultIndex])
		results[resultIndex].reported = true
		batch = append(batch, publicObjectResult(results[resultIndex]))
	}
	if len(batch) == 0 {
		return nil
	}
	if err := p.config.LifecycleObserver.ObjectsCompleted(index, flowID, batch); err != nil {
		return withFailure(FailureCodeOutputFailed, FailureMessageOutputFailed, false,
			fmt.Errorf("publish %d Object results for Flow %s: %w", len(batch), flowID, err))
	}
	return nil
}

func (p *Pipeline) observeRemainingObjects(index int, flows []FlowResult) error {
	if p.config.LifecycleObserver == nil {
		return nil
	}
	var failures []error
	for flowIndex := range flows {
		batch := make([]ObjectResult, 0)
		for objectIndex := range flows[flowIndex].Objects {
			object := &flows[flowIndex].Objects[objectIndex]
			if object.reported {
				continue
			}
			if object.Status == ObjectStatusPlanned && p.config.DryRunMode == DryRunOff {
				object.Disposition = ObjectDispositionUnattempted
			}
			finalizeObjectResult(object)
			object.reported = true
			batch = append(batch, publicObjectResult(*object))
		}
		if len(batch) == 0 {
			continue
		}
		if err := p.config.LifecycleObserver.ObjectsCompleted(index, flows[flowIndex].FlowID, batch); err != nil {
			failures = append(failures, fmt.Errorf("publish terminal Object results for Flow %s: %w",
				flows[flowIndex].FlowID, err))
		}
	}
	if len(failures) == 0 {
		return nil
	}
	return withFailure(FailureCodeOutputFailed, FailureMessageOutputFailed, false, errors.Join(failures...))
}

func publicObjectResult(object ObjectResult) ObjectResult {
	object.reported = false
	return object
}

func (p *Pipeline) observeFlowPlans(ctx context.Context, graph flowGraph, planned []plannedFlowWrite, results []FlowResult) error {
	for _, write := range planned {
		for resultIndex := range results {
			if results[resultIndex].FlowID == write.member.id {
				results[resultIndex].TAMSFlowProfileID = write.member.profileID
				break
			}
		}
	}
	if p.config.LifecycleObserver == nil {
		return nil
	}
	index, ok := inputIndexFromContext(ctx)
	if !ok {
		return fmt.Errorf("publish Flow plan: input index is missing")
	}
	rootID := graph.collectorID
	if rootID == "" && len(graph.flows) == 1 {
		rootID = graph.flows[0].id
	}
	for _, write := range planned {
		plan := FlowPlan{
			FlowID: write.member.id, SourceID: stringField(write.effective, "source_id"),
			Kind: flowPlanKind(graph, write.member), Format: stringField(write.effective, "format"),
			Container:         stringField(write.effective, "container"),
			TAMSFlowProfileID: write.member.profileID,
		}
		plan.Role = flowPlanRole(plan.Kind, write.member.role, plan.Format)
		plan.Root = plan.FlowID == rootID
		if !plan.Root {
			plan.ParentFlowID = rootID
		}
		if err := p.config.LifecycleObserver.FlowPlanned(index, plan); err != nil {
			return withFailure(FailureCodeOutputFailed, FailureMessageOutputFailed, false,
				fmt.Errorf("publish Flow plan %s: %w", plan.FlowID, err))
		}
	}
	return nil
}

func flowPlanKind(graph flowGraph, member graphFlow) FlowKind {
	if member.id == graph.collectorID && !member.ownsMedia {
		return FlowKindCollection
	}
	if strings.EqualFold(stringField(member.flow, "format"), "urn:x-nmos:format:multi") && member.ownsMedia {
		return FlowKindMuxed
	}
	return FlowKindEssence
}

func flowPlanRole(kind FlowKind, graphRole, format string) string {
	if kind != FlowKindEssence {
		return ""
	}
	role := strings.TrimSpace(graphRole)
	if role != "" && role != "single" && role != "multi" {
		return role
	}
	if value, ok := strings.CutPrefix(strings.ToLower(strings.TrimSpace(format)), "urn:x-nmos:format:"); ok && value != "" {
		return value
	}
	return "data"
}

func stringField(flow map[string]any, key string) string {
	value, _ := flow[key].(string)
	return strings.TrimSpace(value)
}
