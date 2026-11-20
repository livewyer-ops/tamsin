package ingest

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

// sourceDerivationRetry is how long to wait before asking once more for a
// Source the store has not yet derived from a Flow it just accepted.
const sourceDerivationRetry = 250 * time.Millisecond

// populateSources gives the Sources the store derives for this run's Flows a
// human-readable identity, as AppNote 0007 describes: a label and description
// where the Source has none, and Tamsin's own provenance tags. A label or
// description an operator has already set is left alone, and a Source that
// cannot be read or written is a warning, not a failed ingest: the Flows and
// their media are complete without it.
func (p *Pipeline) populateSources(ctx context.Context, planned []plannedFlowWrite) {
	if p.config.DryRunMode != DryRunOff || p.client == nil {
		return
	}
	// Each Source is a document of its own, so Sources are populated side by
	// side, as many across the run as the transfer budget is large. The
	// requests for one Source stay in order.
	var sources sync.WaitGroup
	seen := make(map[string]bool, len(planned))
	for _, plan := range planned {
		flow := plan.effective
		sourceID := stringField(flow, "source_id")
		if sourceID == "" || seen[sourceID] {
			continue
		}
		seen[sourceID] = true
		select {
		case p.sourceSlots <- struct{}{}:
		case <-ctx.Done():
			// A slot held by another input is not worth waiting for once
			// this one has been cancelled.
			p.logger.Warn("source metadata was not populated", "source_id", sourceID, "error", ctx.Err())
			continue
		}
		sources.Go(func() {
			defer func() { <-p.sourceSlots }()
			p.populateSource(ctx, sourceID, flow)
		})
	}
	sources.Wait()
}

func (p *Pipeline) populateSource(ctx context.Context, sourceID string, flow tams.Flow) {
	source, err := p.readDerivedSource(ctx, sourceID)
	if err != nil {
		p.logger.Warn("source metadata was not populated", "source_id", sourceID, "error", err)
		return
	}
	if source == nil {
		p.logger.Warn("source has not been derived from its Flow yet; label, description and tags were not set", "source_id", sourceID)
		return
	}
	if err := p.writeSourceIdentity(ctx, sourceID, source, flow); err != nil {
		p.logger.Warn("source metadata was not populated", "source_id", sourceID, "error", err)
	}
}

func (p *Pipeline) readDerivedSource(ctx context.Context, sourceID string) (tams.Source, error) {
	for attempt := range 2 {
		source, err := p.client.Source(ctx, sourceID)
		if err == nil {
			return source, nil
		}
		var httpErr *tams.HTTPError
		if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusNotFound {
			return nil, err
		}
		if attempt == 0 {
			if err := sleepDuration(ctx, sourceDerivationRetry); err != nil {
				return nil, err
			}
		}
	}
	return nil, nil
}

func (p *Pipeline) writeSourceIdentity(ctx context.Context, sourceID string, source tams.Source, flow tams.Flow) error {
	if stringField(source, "label") == "" {
		if label := stringField(flow, "label"); label != "" {
			if err := p.client.PutSourceLabel(ctx, sourceID, label); err != nil {
				return err
			}
		}
	}
	if stringField(source, "description") == "" {
		if description := stringField(flow, "description"); description != "" {
			if err := p.client.PutSourceDescription(ctx, sourceID, description); err != nil {
				return err
			}
		}
	}
	existingTags, _ := source["tags"].(map[string]any)
	flowTags, _ := flow["tags"].(map[string]any)
	names := make([]string, 0, len(flowTags))
	for name := range flowTags {
		if strings.HasPrefix(name, media.TagPrefix) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		if existing, present := existingTags[name]; present && equalJSONValues(existing, flowTags[name]) {
			continue
		}
		if err := p.client.PutSourceTag(ctx, sourceID, name, flowTags[name]); err != nil {
			return err
		}
	}
	return nil
}

func sleepDuration(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
