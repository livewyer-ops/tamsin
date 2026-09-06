package ingest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/tams"
	"github.com/livewyer-ops/tamsin/internal/tamsschema"
)

// planFlowGraph resolves every final effective Flow before writing any of
// them. That means schema and association failures cannot leave a prefix of the
// graph in the service, and it makes preservation decisions from one coherent
// read of the graph rather than interleaving reads with replacements.
func (p *Pipeline) planFlowGraph(ctx context.Context, graph flowGraph) ([]plannedFlowWrite, error) {
	var err error
	graph, err = p.assignFlowProfiles(graph)
	if err != nil {
		return nil, err
	}
	planned := make([]plannedFlowWrite, 0, len(graph.flows))
	for _, member := range graph.flows {
		member, err = p.expandFlowProfile(ctx, member)
		if err != nil {
			return nil, err
		}
		plan, err := p.planFlowWrite(ctx, member)
		if err != nil {
			return nil, err
		}
		if err := tamsschema.ValidateFlowGet(p.apiVersion, plan.effective); err != nil {
			return nil, fmt.Errorf(
				"final Flow metadata for %s (%s) is not valid against pinned TAMS %d.%d at %w",
				member.id, member.role, tams.SpecMajor, tams.SpecMinor, err)
		}
		if err := tamsschema.ValidateFlowPut(p.apiVersion, plan.request); err != nil {
			return nil, fmt.Errorf("flow PUT metadata for %s (%s) is not valid against TAMS %s at %w",
				member.id, member.role, p.apiVersion, err)
		}
		planned = append(planned, plan)
	}
	if err := validateFlowGraph(graph, planned); err != nil {
		return nil, fmt.Errorf("final Flow graph is not valid: %w", err)
	}
	return planned, nil
}

// planFlowWrite applies the ownership rule without mutating TAMS. A PUT
// replaces a Flow, so the final value must preserve everything this run does
// not own. The dry-run path has no store to read and validates the generated
// value as-is.
func (p *Pipeline) planFlowWrite(ctx context.Context, member graphFlow) (plannedFlowWrite, error) {
	plan := plannedFlowWrite{member: member, effective: member.flow, changed: true}
	plan.request = flowPutProjection(member.flow, member.profileID)
	if p.config.DryRunMode != DryRunOff {
		return plan, nil
	}
	existing, err := p.client.Flow(ctx, member.id)
	if err != nil {
		var httpErr *tams.HTTPError
		if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusNotFound {
			// Writing anyway would risk replacing metadata that is there but
			// could not be read, and that cannot be undone.
			return plannedFlowWrite{}, fmt.Errorf("read flow %s before planning the Flow graph: %w", member.id, err)
		}
		return plan, nil
	}

	plan.existed = true
	existingSourceID := stringField(existing, "source_id")
	generatedSourceID := stringField(member.flow, "source_id")
	if existingSourceID != generatedSourceID {
		return plannedFlowWrite{}, fmt.Errorf(
			"flow %s already belongs to source_id %q; refusing to replace it with source_id %q",
			member.id, existingSourceID, generatedSourceID)
	}
	existingProfileID := stringField(existing, "profile_id")
	if existingProfileID != member.profileID {
		return plannedFlowWrite{}, fmt.Errorf(
			"flow %s already exists with profile_id %q; refusing to attach, repoint, or remove immutable Profile identity %q",
			member.id, existingProfileID, member.profileID)
	}
	plan.effective = preserveForeignMetadata(existing, member.flow, p.config.FlowMetadata)
	plan.request = flowPutProjection(plan.effective, member.profileID)
	plan.changed = !equalJSONValues(plan.effective, existing)
	return plan, nil
}

// writeFlow commits one already validated member through the same
// ownership-aware path for elemental Flows and collectors alike.
func (p *Pipeline) writeFlow(ctx context.Context, plan plannedFlowWrite) error {
	if !plan.changed {
		p.logger.Debug("flow already describes this ingest", "flow_id", plan.member.id)
		return nil
	}
	action, verb := "creating", "create"
	if plan.existed {
		action, verb = "updating", "update"
	}
	p.logger.Info(action+" flow", "flow_id", plan.member.id, "role", plan.member.role)
	if _, err := p.client.PutFlow(ctx, plan.member.id, plan.request); err != nil {
		return fmt.Errorf("%s flow %s: %w", verb, plan.member.id, err)
	}
	return nil
}

// commitFlowGraph writes children before their collector, as required by the
// Collection Item schema. TAMS has no transaction spanning Flow PUTs. A failed
// PUT can therefore leave an unavoidable partial metadata mutation (including
// the ambiguous case where the service committed but the response was lost),
// which is reported explicitly. No Object has been allocated at this point.
func (p *Pipeline) commitFlowGraphObserved(ctx context.Context, planned []plannedFlowWrite, results []FlowResult) error {
	for _, plan := range planned {
		if !plan.changed {
			setFlowDisposition(results, plan.member.id, FlowUnchanged)
		}
	}
	written := make([]string, 0, len(planned))
	for index, plan := range planned {
		if err := p.writeFlow(ctx, plan); err != nil {
			setFlowDisposition(results, plan.member.id, FlowIndeterminate)
			for _, pending := range planned[index+1:] {
				if pending.changed {
					setFlowDisposition(results, pending.member.id, FlowUnattempted)
				}
			}
			confirmed := "no earlier Flow write was required"
			if len(written) > 0 {
				confirmed = "Flows written before the failure: " + strings.Join(written, ", ")
			}
			return fmt.Errorf(
				"flow graph may be partially written (%s; the failing PUT may also have committed); no Media Objects were allocated: %w",
				confirmed, err)
		}
		if plan.changed {
			written = append(written, plan.member.id)
			setFlowDisposition(results, plan.member.id, FlowWritten)
		}
	}
	return nil
}

func setFlowDisposition(results []FlowResult, flowID string, disposition FlowDisposition) {
	for index := range results {
		if results[index].FlowID == flowID {
			results[index].Disposition = disposition
			return
		}
	}
}

func validateFlowGraph(graph flowGraph, planned []plannedFlowWrite) error {
	if len(planned) != len(graph.flows) || len(planned) == 0 {
		return errors.New("/ must contain every planned Flow")
	}
	byID := make(map[string]plannedFlowWrite, len(planned))
	for _, plan := range planned {
		member := plan.member
		if byID[member.id].member.id != "" {
			return fmt.Errorf("/id duplicates Flow %s", member.id)
		}
		byID[member.id] = plan
		if plan.effective["id"] != member.id {
			return fmt.Errorf("/id for Flow %s must equal its request identifier", member.id)
		}
		container, hasContainer := plan.effective["container"].(string)
		if member.ownsMedia && (!hasContainer || container == "") {
			return fmt.Errorf("/container for media-owning Flow %s is required", member.id)
		}
		if !member.ownsMedia {
			if _, present := plan.effective["container"]; present {
				return fmt.Errorf("/container for association-only Flow %s must be absent", member.id)
			}
		}
		if _, present := plan.effective["container_mapping"]; present {
			return fmt.Errorf("/container_mapping for Flow %s must be on its parent Collection Item", member.id)
		}
	}

	if graph.collectorID == "" {
		if len(planned) != 1 {
			return errors.New("/flow_collection is missing a collector for multiple Flows")
		}
		if _, present := planned[0].effective["flow_collection"]; present {
			return errors.New("/flow_collection must be absent for a single Flow")
		}
		return nil
	}

	collector, present := byID[graph.collectorID]
	if !present {
		return fmt.Errorf("/flow_collection collector %s is missing", graph.collectorID)
	}
	if collector.effective["format"] != "urn:x-nmos:format:multi" {
		return fmt.Errorf("/format for collector %s must be urn:x-nmos:format:multi", graph.collectorID)
	}
	items, ok := collector.effective["flow_collection"].([]map[string]any)
	if !ok {
		return fmt.Errorf("/flow_collection for collector %s must be an array", graph.collectorID)
	}
	expected := make([]graphFlow, 0, len(graph.flows)-1)
	for _, member := range graph.flows {
		if member.id != graph.collectorID {
			expected = append(expected, member)
			if _, present := byID[member.id].effective["flow_collection"]; present {
				return fmt.Errorf("/flow_collection must be absent from collected Flow %s", member.id)
			}
		}
	}
	if len(items) != len(expected) {
		return fmt.Errorf("/flow_collection has %d items, want %d", len(items), len(expected))
	}
	for index, member := range expected {
		item := items[index]
		base := fmt.Sprintf("/flow_collection/%d", index)
		if item["id"] != member.id {
			return fmt.Errorf("%s/id = %v, want %s", base, item["id"], member.id)
		}
		if item["role"] != member.role {
			return fmt.Errorf("%s/role = %v, want %s", base, item["role"], member.role)
		}
		mapping, hasMapping := item["container_mapping"]
		switch graph.storage {
		case media.EssenceStorageIndependent:
			if hasMapping {
				return fmt.Errorf("%s/container_mapping must be absent after demultiplexing", base)
			}
		case media.EssenceStorageMuxed, "":
			if member.containerMapping == nil || !hasMapping || !equalJSONValues(mapping, member.containerMapping) {
				return fmt.Errorf("%s/container_mapping does not match the input track", base)
			}
		default:
			return fmt.Errorf("/ uses unsupported essence storage %q", graph.storage)
		}
	}
	return nil
}

// descriptiveFields are written when a Flow is created and then left alone.
//
// Everything else Tamsin generates describes the media -- codec, container,
// essence parameters, bit rates -- and has to stay accurate, so a later run
// updates it. These two describe the content to a person, and a person may well
// have improved on the neutral generated values. Overwriting a curated label on
// every resume would be its own kind of data loss.
//
// An operator can still set them deliberately through --flow-metadata, which is
// an instruction rather than a by-product.
var descriptiveFields = [...]string{"label", "description"}

// preserveForeignMetadata overlays what this run generated onto what the store
// already holds, keeping anything the run does not own.
//
// A tag Tamsin writes carries its own prefix, so one already in the store under
// that prefix but absent from this run is a leftover of Tamsin's own and is
// dropped. Anything else belongs to somebody, and is kept.
func preserveForeignMetadata(existing, generated, operatorOverrides tams.Flow) tams.Flow {
	merged := make(tams.Flow, len(existing)+len(generated))
	for key, value := range existing {
		merged[key] = value
	}
	// These fields describe which Flow owns Media Objects and how the graph is
	// connected. Their absence is meaningful, so merely overlaying generated
	// values would preserve a stale arrangement across a muxed/independent
	// rewrite. They are always Tamsin-owned and operator overrides are rejected.
	for _, field := range [...]string{"container", "flow_collection", "container_mapping"} {
		if _, generatedHere := generated[field]; !generatedHere {
			delete(merged, field)
		}
	}
	for key, value := range generated {
		merged[key] = value
	}
	for _, field := range descriptiveFields {
		if _, asked := operatorOverrides[field]; asked {
			continue
		}
		if value, present := existing[field]; present {
			merged[field] = value
		}
	}

	existingTags, hasExisting := existing["tags"].(map[string]any)
	if !hasExisting {
		return merged
	}
	generatedTags, _ := generated["tags"].(map[string]any)
	sources := make(map[string]struct{})
	for _, tagSet := range []map[string]any{existingTags, generatedTags} {
		collectProvenanceSources(sources, tagSet[media.ProvenanceSourcesTag])
		// Migrate the singular tag written by earlier builds when this Flow is
		// next touched, without losing where that ingest came from.
		collectProvenanceSources(sources, tagSet[media.TagPrefix+"source"])
	}
	tags := make(map[string]any, len(existingTags)+len(generatedTags))
	for name, value := range existingTags {
		if strings.HasPrefix(name, media.TagPrefix) {
			continue
		}
		tags[name] = value
	}
	for name, value := range generatedTags {
		tags[name] = value
	}
	delete(tags, media.TagPrefix+"source")
	if len(sources) > 0 {
		ordered := make([]string, 0, len(sources))
		for source := range sources {
			ordered = append(ordered, source)
		}
		sort.Strings(ordered)
		tags[media.ProvenanceSourcesTag] = ordered
	}
	merged["tags"] = tags
	return merged
}

func collectProvenanceSources(destination map[string]struct{}, value any) {
	add := func(source string) {
		if source = strings.TrimSpace(source); source != "" {
			destination[source] = struct{}{}
		}
	}
	switch sources := value.(type) {
	case string:
		add(sources)
	case []string:
		for _, source := range sources {
			add(source)
		}
	case []any:
		for _, source := range sources {
			if text, ok := source.(string); ok {
				add(text)
			}
		}
	}
}
