package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/livewyer-ops/tamsin/internal/tams"
)

// startupState carries what the startup phases below read from and write to.
// The phases run in a fixed order and each depends on what the previous one
// left behind, so the order in runStartupPreflight is load-bearing.
type startupState struct {
	service     map[string]any
	serviceErr  error
	backends    []tams.StorageBackend
	backendsErr error
	storageID   string
}

// runStartupPreflight resolves everything that must hold before any Flow or
// Object is mutated, so an unusable service or a mistyped backend is a startup
// error rather than a partially applied ingest.
func (p *Pipeline) runStartupPreflight(ctx context.Context) (string, error) {
	state := &startupState{storageID: p.config.StorageID}
	if err := issueStartupRequests(ctx, p, state); err != nil {
		return "", err
	}
	// Check both request outcomes before interpreting either response. A
	// transport failure is the primary fact; validating a concurrently returned
	// document first can hide it behind a secondary compatibility error.
	if err := checkServiceRequest(state); err != nil {
		return "", err
	}
	if err := checkStorageBackends(state); err != nil {
		return "", err
	}
	if err := checkServiceCompatibility(p, state); err != nil {
		return "", err
	}
	if err := checkServiceLifetimes(p, state); err != nil {
		return "", err
	}
	if err := selectStorage(state); err != nil {
		return "", err
	}
	return state.storageID, nil
}

func issueStartupRequests(ctx context.Context, p *Pipeline, state *startupState) error {
	var startup sync.WaitGroup
	startup.Add(2)
	go func() {
		defer startup.Done()
		state.service, state.serviceErr = p.client.Service(ctx)
	}()
	go func() {
		defer startup.Done()
		state.backends, state.backendsErr = p.client.StorageBackends(ctx)
	}()
	startup.Wait()
	// A parent cancellation is a run interruption. Per-request client
	// deadlines below remain typed TAMS preflight failures while the parent
	// context is live; do not conflate those two timeout domains.
	return ctx.Err()
}

func checkServiceRequest(state *startupState) error {
	if state.serviceErr != nil {
		return withFailure(FailureCodePreflightFailed, FailureMessagePreflightFailed, true,
			fmt.Errorf("read TAMS service information: %w", state.serviceErr))
	}
	return nil
}

func checkServiceCompatibility(p *Pipeline, state *startupState) error {
	if err := p.checkAPIVersion(state.service); err != nil {
		return withFailure(FailureCodePreflightFailed, FailureMessagePreflightIncompatible, true, err)
	}
	return nil
}

func checkServiceLifetimes(p *Pipeline, state *startupState) error {
	limits, err := tams.ParseServiceLimits(state.service)
	if err != nil {
		return withFailure(FailureCodePreflightFailed, FailureMessageTransferLifetimeInvalid, true,
			fmt.Errorf("validate TAMS service lifetimes: %w", err))
	}
	p.limits = limits
	p.logger.Debug("store lifetimes",
		"object_registration", p.limits.ObjectRegistration, "presigned_url", p.limits.PresignedURL)
	return nil
}

func checkStorageBackends(state *startupState) error {
	if state.backendsErr != nil {
		return withFailure(FailureCodePreflightFailed, FailureMessagePreflightFailed, true,
			fmt.Errorf("read TAMS storage backends: %w", state.backendsErr))
	}
	return nil
}

func selectStorage(state *startupState) error {
	selection, err := ResolveStorageBackend(state.backends, state.storageID)
	if err != nil {
		return withFailure(FailureCodeStorageUnavailable, FailureMessageStorageUnavailable, true, err)
	}
	state.storageID = selection.Backend.ID
	return nil
}

// APICompatibility is the result of comparing a service document with the
// pinned TAMS specification. Unknown is deliberately non-fatal for parity with
// ingest: api_version is required upstream, but older deployed services have
// omitted it and the operations may still be usable.
type APICompatibility struct {
	StoreVersion  string
	TargetVersion string
	Relationship  string
	Warning       string
}

// AssessAPIVersion applies the startup compatibility policy shared by ingest
// and doctor. A different major revision is incompatible; older and newer
// minor revisions remain usable and are made visible to the operator.
func AssessAPIVersion(service map[string]any) (APICompatibility, error) {
	assessment := APICompatibility{
		TargetVersion: fmt.Sprintf("%d.%d", tams.SpecMajor, tams.SpecMinor),
	}
	version, err := tams.ParseAPIVersion(service)
	if err != nil {
		assessment.Relationship = "unknown"
		assessment.Warning = err.Error()
		return assessment, nil
	}
	assessment.StoreVersion = version.String()
	if !version.SupportsSpec() {
		assessment.Relationship = "incompatible"
		return assessment, fmt.Errorf(
			"this store implements TAMS %s, and tamsin is written against %s; a differing major version is not compatible",
			version, assessment.TargetVersion)
	}
	switch {
	case version.Predates():
		assessment.Relationship = "older"
		assessment.Warning = "store implements an older TAMS revision than tamsin targets"
	case version.Minor > tams.SpecMinor:
		assessment.Relationship = "newer"
	default:
		assessment.Relationship = "exact"
	}
	return assessment, nil
}

// StorageSelection records the backend the startup preflight resolved.
type StorageSelection struct {
	Backend   tams.StorageBackend
	Requested bool
}

// ResolveStorageBackend validates an explicit backend or selects the service's
// single default. Resolving it before any Flow/Object mutation makes a typo or
// an unconfigured store a startup error rather than a partial ingest.
func ResolveStorageBackend(backends []tams.StorageBackend, requested string) (StorageSelection, error) {
	if requested != "" {
		for _, backend := range backends {
			if backend.ID == requested {
				return StorageSelection{Backend: backend, Requested: true}, nil
			}
		}
		return StorageSelection{}, errors.New("requested TAMS storage backend is not available")
	}

	var selected *tams.StorageBackend
	for index := range backends {
		if !backends[index].DefaultStorage {
			continue
		}
		if selected != nil {
			return StorageSelection{}, errors.New("TAMS service reports multiple default storage backends")
		}
		selected = &backends[index]
	}
	if selected == nil {
		return StorageSelection{}, fmt.Errorf("TAMS service has no default storage backend; use --storage-id")
	}
	if selected.ID == "" {
		return StorageSelection{}, fmt.Errorf("TAMS service default storage backend has no ID")
	}
	return StorageSelection{Backend: *selected}, nil
}
