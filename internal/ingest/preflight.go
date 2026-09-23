package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/livewyer-ops/tamsin/internal/tams"
)

// runStartupPreflight resolves everything that must hold before any Flow or
// Object is mutated, so an unusable service or a mistyped backend is a startup
// error rather than a partially applied ingest.
func (p *Pipeline) runStartupPreflight(ctx context.Context) (string, error) {
	var (
		startup     sync.WaitGroup
		service     map[string]any
		serviceErr  error
		backends    []tams.StorageBackend
		backendsErr error
	)
	startup.Add(2)
	go func() {
		defer startup.Done()
		service, serviceErr = p.client.Service(ctx)
	}()
	go func() {
		defer startup.Done()
		backends, backendsErr = p.client.StorageBackends(ctx)
	}()
	startup.Wait()
	// A parent cancellation is a run interruption. Per-request client
	// deadlines below remain typed TAMS preflight failures while the parent
	// context is live; do not conflate those two timeout domains.
	if err := ctx.Err(); err != nil {
		return "", err
	}
	// Check both request outcomes before interpreting either response. A
	// transport failure is the primary fact; validating a concurrently returned
	// document first can hide it behind a secondary compatibility error.
	if serviceErr != nil {
		return "", withFailure(FailureCodePreflightFailed, FailureMessagePreflightFailed, true,
			fmt.Errorf("read TAMS service information: %w", serviceErr))
	}
	if backendsErr != nil {
		return "", withFailure(FailureCodePreflightFailed, FailureMessagePreflightFailed, true,
			fmt.Errorf("read TAMS storage backends: %w", backendsErr))
	}
	// Compatibility is checked before lifetimes so an unreadable or wrong-major
	// service is reported as such, rather than as a missing lifetime field.
	if err := p.checkAPIVersion(service); err != nil {
		return "", withFailure(FailureCodePreflightFailed, FailureMessagePreflightIncompatible, true, err)
	}
	limits, err := tams.ParseServiceLimits(service)
	if err != nil {
		return "", withFailure(FailureCodePreflightFailed, FailureMessageTransferLifetimeInvalid, true,
			fmt.Errorf("validate TAMS service lifetimes: %w", err))
	}
	p.limits = limits
	p.logger.Debug("store lifetimes",
		"object_registration", p.limits.ObjectRegistration, "presigned_url", p.limits.PresignedURL)
	selection, err := ResolveStorageBackend(backends, p.config.StorageID)
	if err != nil {
		return "", withFailure(FailureCodeStorageUnavailable, FailureMessageStorageUnavailable, true, err)
	}
	return selection.Backend.ID, nil
}

// APICompatibility is the result of comparing a service document with the
// pinned TAMS specification. api_version is a required capability boundary:
// missing or malformed values fail before any mutation rather than making the
// client guess which wire contract to send.
type APICompatibility struct {
	StoreVersion  string
	TargetVersion string
	Relationship  string
	Warning       string
	Version       tams.APIVersion
}

// AssessAPIVersion applies the startup compatibility policy shared by ingest
// and doctor. TAMS 8.1 is the compatibility floor, 8.2 is the target, and
// newer 8.x revisions retain the additive-minor compatibility rule.
func AssessAPIVersion(service map[string]any) (APICompatibility, error) {
	assessment := APICompatibility{
		TargetVersion: fmt.Sprintf("%d.%d", tams.SpecMajor, tams.SpecMinor),
	}
	version, err := tams.ParseAPIVersion(service)
	if err != nil {
		assessment.Relationship = "invalid"
		return assessment, err
	}
	assessment.Version = version
	assessment.StoreVersion = version.String()
	if !version.SupportsSpec() {
		assessment.Relationship = "incompatible"
		if version.Major != tams.SpecMajor {
			return assessment, fmt.Errorf(
				"this store implements TAMS %s, and tamsin is written against %s; a differing major version is not compatible",
				version, assessment.TargetVersion)
		}
		return assessment, fmt.Errorf(
			"this store implements TAMS %s; tamsin supports TAMS %d.%d and newer %d.x revisions",
			version, tams.SpecMajor, tams.CompatibilityMinor, tams.SpecMajor)
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
