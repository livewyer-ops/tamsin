package ingest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/livewyer-ops/tamsin/contracts"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

type flowProfileAssignment struct {
	selector string
	index    int
	indexed  bool
	id       string
}

var profileTechnicalFields = [...]string{
	"format", "codec", "container", "avg_bit_rate", "segment_duration", "container_mapping", "essence_parameters",
}

func parseFlowProfileAssignments(values []string) ([]flowProfileAssignment, []string, error) {
	assignments := make([]flowProfileAssignment, 0, len(values))
	normalized := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			return nil, nil, errors.New("TAMS Flow Profile assignment cannot be empty")
		}
		selector, idText, hasSelector := strings.Cut(value, "=")
		if !hasSelector {
			idText = selector
			selector = ""
		}
		parsedID, err := uuid.Parse(strings.TrimSpace(idText))
		if err != nil {
			return nil, nil, fmt.Errorf("TAMS Flow Profile %q must end in a UUID: %w", raw, err)
		}
		assignment := flowProfileAssignment{selector: strings.ToLower(strings.TrimSpace(selector)), index: -1, id: parsedID.String()}
		if assignment.selector != "" {
			base, indexText, indexed := strings.Cut(assignment.selector, ":")
			if !validProfileSelector(base) {
				return nil, nil, fmt.Errorf("TAMS Flow Profile selector %q must be video, audio, image, or data", selector)
			}
			assignment.selector = base
			if indexed {
				index, err := strconv.Atoi(indexText)
				if err != nil || index < 0 || strconv.Itoa(index) != indexText {
					return nil, nil, fmt.Errorf("TAMS Flow Profile selector %q needs a zero-based canonical index", selector)
				}
				assignment.index, assignment.indexed = index, true
			}
		}
		selectorKey := assignment.selector
		if assignment.indexed {
			selectorKey += ":" + strconv.Itoa(assignment.index)
		}
		if _, duplicate := seen[selectorKey]; duplicate {
			return nil, nil, fmt.Errorf("TAMS Flow Profile selector %q is assigned more than once", selectorKey)
		}
		seen[selectorKey] = struct{}{}
		assignments = append(assignments, assignment)
		prefix := ""
		if selectorKey != "" {
			prefix = selectorKey + "="
		}
		normalized = append(normalized, prefix+assignment.id)
	}
	sort.Strings(normalized)
	return assignments, normalized, nil
}

func validProfileSelector(selector string) bool {
	return selector == "video" || selector == "audio" || selector == "image" || selector == "data"
}

func profileSelectorForFormat(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "urn:x-nmos:format:video":
		return "video"
	case "urn:x-nmos:format:audio":
		return "audio"
	case "urn:x-tam:format:image":
		return "image"
	case "urn:x-nmos:format:data":
		return "data"
	default:
		return ""
	}
}

func (p *Pipeline) assignFlowProfiles(graph flowGraph) (flowGraph, error) {
	if len(p.profileAssignments) == 0 {
		return graph, nil
	}
	if !p.apiVersion.SupportsFlowProfiles() {
		return flowGraph{}, fmt.Errorf("TAMS Flow Profiles require TAMS 8.2 or newer; store reports %s", p.apiVersion)
	}
	bySelector := make(map[string][]int)
	var eligible []int
	for index := range graph.flows {
		selector := profileSelectorForFormat(stringField(graph.flows[index].flow, "format"))
		if selector == "" {
			continue
		}
		eligible = append(eligible, index)
		bySelector[selector] = append(bySelector[selector], index)
	}
	assigned := make(map[int]string, len(p.profileAssignments))
	for _, assignment := range p.profileAssignments {
		var matches []int
		if assignment.selector == "" {
			matches = eligible
		} else {
			matches = bySelector[assignment.selector]
		}
		var memberIndex int
		switch {
		case assignment.indexed && assignment.index >= len(matches):
			return flowGraph{}, fmt.Errorf("TAMS Flow Profile selector %s:%d matched no Flow", assignment.selector, assignment.index)
		case assignment.indexed:
			memberIndex = matches[assignment.index]
		case len(matches) != 1:
			selector := assignment.selector
			if selector == "" {
				selector = "bare UUID"
			}
			return flowGraph{}, fmt.Errorf("TAMS Flow Profile selector %s matched %d eligible Flows; use a format:index selector", selector, len(matches))
		default:
			memberIndex = matches[0]
		}
		if previous := assigned[memberIndex]; previous != "" {
			return flowGraph{}, fmt.Errorf("TAMS Flow Profile assignments %s and %s select the same Flow", previous, assignment.id)
		}
		assigned[memberIndex] = assignment.id
		graph.flows[memberIndex].profileID = assignment.id
	}
	return graph, nil
}

func (p *Pipeline) runFlowProfilePreflight(ctx context.Context) error {
	service, err := p.client.Service(ctx)
	if err != nil {
		return withFailure(FailureCodePreflightFailed, FailureMessagePreflightFailed, true,
			fmt.Errorf("read TAMS service information for Flow Profile dry-run: %w", err))
	}
	if err := p.checkAPIVersion(service); err != nil {
		return withFailure(FailureCodePreflightFailed, FailureMessagePreflightIncompatible, true, err)
	}
	if !p.apiVersion.SupportsFlowProfiles() {
		return withFailure(FailureCodePreflightFailed, FailureMessagePreflightIncompatible, true,
			fmt.Errorf("TAMS Flow Profiles require TAMS 8.2 or newer; store reports %s", p.apiVersion))
	}
	return nil
}

func (p *Pipeline) loadFlowProfile(ctx context.Context, profileID string) (tams.Profile, error) {
	p.profileMu.Lock()
	defer p.profileMu.Unlock()
	if cached := p.profileCache[profileID]; cached != nil {
		return cached, nil
	}
	profile, err := p.client.Profile(ctx, profileID)
	if err != nil {
		return nil, fmt.Errorf("read TAMS Flow Profile %s: %w", profileID, err)
	}
	if err := contracts.ValidateProfile(profile); err != nil {
		return nil, fmt.Errorf("TAMS Flow Profile %s is invalid at %w", profileID, err)
	}
	if stringField(profile, "id") != profileID {
		return nil, fmt.Errorf("TAMS Flow Profile %s returned mismatched id %q", profileID, stringField(profile, "id"))
	}
	p.profileCache[profileID] = profile
	return profile, nil
}

func (p *Pipeline) expandFlowProfile(ctx context.Context, member graphFlow) (graphFlow, error) {
	if member.profileID == "" {
		return member, nil
	}
	profile, err := p.loadFlowProfile(ctx, member.profileID)
	if err != nil {
		return graphFlow{}, err
	}
	metadata, ok := profile["flow_metadata"].(map[string]any)
	if !ok {
		return graphFlow{}, fmt.Errorf("TAMS Flow Profile %s /flow_metadata is not an object", member.profileID)
	}
	for _, field := range profileTechnicalFields {
		if field == "avg_bit_rate" {
			continue
		}
		generated, generatedPresent := member.flow[field]
		required, requiredPresent := metadata[field]
		mismatch := firstJSONValueMismatch(appendJSONPointer("/flow_metadata", field),
			generated, generatedPresent, required, requiredPresent)
		if mismatch != nil {
			return graphFlow{}, fmt.Errorf(
				"flow %s does not exactly match TAMS Flow Profile %s at %s (generated=%s profile=%s)",
				member.id, member.profileID, mismatch.path,
				formatJSONMismatchValue(mismatch.generated, mismatch.generatedPresent),
				formatJSONMismatchValue(mismatch.profile, mismatch.profilePresent))
		}
	}
	expanded := maps.Clone(member.flow)
	for key, value := range metadata {
		expanded[key] = value
	}
	expanded["profile_id"] = member.profileID
	member.flow = expanded
	return member, nil
}

func flowPutProjection(effective tams.Flow, profileID string) tams.Flow {
	request := maps.Clone(effective)
	if profileID == "" {
		return request
	}
	for _, field := range profileTechnicalFields {
		delete(request, field)
	}
	request["profile_id"] = profileID
	return request
}
