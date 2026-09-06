package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/livewyer-ops/tamsin/internal/auth"
	"github.com/livewyer-ops/tamsin/internal/ingest"
	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/tams"
	"github.com/livewyer-ops/tamsin/internal/version"
	"github.com/spf13/cobra"
)

const DoctorReportSchemaVersion = "1.0"

const doctorToolVersionTimeout = 5 * time.Second

const (
	doctorPass = "pass"
	doctorFail = "fail"
	doctorSkip = "skipped"
)

type doctorProfile struct {
	Selection       string `json:"selection"`
	Name            string `json:"name,omitempty"`
	Version         string `json:"version,omitempty"`
	SegmentDuration string `json:"segment_duration,omitempty"`
	SegmentFormat   string `json:"segment_format,omitempty"`
	EssenceStorage  string `json:"essence_storage,omitempty"`
	RequiresFFmpeg  bool   `json:"requires_ffmpeg"`
}

type doctorCheck struct {
	Name   string         `json:"name"`
	Status string         `json:"status"`
	Detail map[string]any `json:"detail,omitempty"`
	Error  string         `json:"error,omitempty"`
}

type doctorResult struct {
	SchemaVersion string        `json:"schema_version"`
	Status        string        `json:"status"`
	Tamsin        string        `json:"tamsin"`
	ToolVersion   string        `json:"tool_version"`
	ToolCommit    string        `json:"tool_commit"`
	ToolBuildDate string        `json:"tool_build_date,omitempty"`
	Go            string        `json:"go"`
	OS            string        `json:"os"`
	Arch          string        `json:"arch"`
	Profile       doctorProfile `json:"profile"`
	Online        bool          `json:"online"`
	Endpoint      string        `json:"endpoint,omitempty"`
	Auth          string        `json:"auth,omitempty"`
	Checks        []doctorCheck `json:"checks"`
}

type doctorFailure struct{ exitCode int }

type doctorRun struct {
	report   doctorResult
	failures []doctorFailure
}

func newDoctorRun(online bool) *doctorRun {
	return &doctorRun{report: doctorResult{
		SchemaVersion: DoctorReportSchemaVersion, Status: doctorPass,
		Tamsin: version.String(), ToolVersion: version.Version,
		ToolCommit: version.SourceCommit(), ToolBuildDate: version.BuildDate(),
		Go: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH,
		Online: online, Checks: make([]doctorCheck, 0, 11),
	}}
}

func (r *doctorRun) pass(name string, detail map[string]any) {
	r.report.Checks = append(r.report.Checks, doctorCheck{Name: name, Status: doctorPass, Detail: detail})
}

func (r *doctorRun) fail(name string, err error, exitCode int, detail map[string]any) {
	r.report.Status = doctorFail
	r.report.Checks = append(r.report.Checks, doctorCheck{
		Name: name, Status: doctorFail, Detail: detail, Error: err.Error(),
	})
	r.failures = append(r.failures, doctorFailure{exitCode: exitCode})
}

func (r *doctorRun) skip(name, reason string) {
	r.report.Checks = append(r.report.Checks, doctorCheck{
		Name: name, Status: doctorSkip, Detail: map[string]any{"reason": reason},
	})
}

// exitCode makes multi-failure behavior stable. Configuration/usage problems
// take precedence, followed by missing media tools, authentication/policy,
// remote TAMS validation, and finally other local runtime failures.
func (r *doctorRun) exitCode() int {
	for _, candidate := range []int{ExitUsage, ExitMedia, ExitAuth, ExitRemote, ExitGeneral} {
		for _, failure := range r.failures {
			if failure.exitCode == candidate {
				return candidate
			}
		}
	}
	return ExitGeneral
}

type doctorFlagValues struct {
	online            bool
	profile           string
	segmentDuration   time.Duration
	segmentFormat     string
	essenceStorage    string
	ffmpegArgs        []string
	tempDirectory     string
	stagingByteBudget string
	storageID         string
}

type doctorSettings struct {
	doctorFlagValues
	stagingBytes int64
}

func (a *application) doctorCommand() *cobra.Command {
	raw := &doctorFlagValues{}
	command := &cobra.Command{
		Use:   "doctor",
		Short: "Check local ingest readiness and optional read-only TAMS preflight",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(command *cobra.Command, _ []string) error {
			return a.runDoctor(command, raw)
		},
	}
	flags := command.Flags()
	flags.BoolVar(&raw.online, "online", false, "also run the read-only TAMS startup preflight")
	addTreatmentFlags(command, &raw.profile, &raw.segmentDuration, &raw.segmentFormat,
		&raw.essenceStorage, &raw.ffmpegArgs)
	addReadinessFlags(command, &raw.tempDirectory, &raw.stagingByteBudget, &raw.storageID)
	return command
}

func (a *application) doctorSettings(command *cobra.Command, raw *doctorFlagValues) doctorSettings {
	settings := doctorSettings{doctorFlagValues: *raw}
	settings.profile = a.configString(command, "profile", "ingest.profile")
	settings.segmentDuration = a.configDuration(command, "segment-duration", "ingest.segment_duration")
	settings.segmentFormat = a.configString(command, "segment-format", "ingest.segment_format")
	settings.essenceStorage = a.configString(command, "essence-storage", "ingest.essence_storage")
	settings.ffmpegArgs = a.configStringArray(command, "ffmpeg-arg", "media.ffmpeg_args")
	settings.tempDirectory = a.configString(command, "temp-dir", "ingest.temp_directory")
	settings.stagingByteBudget = a.configString(command, "staging-byte-budget", "ingest.staging_byte_budget")
	settings.storageID = a.configString(command, "storage-id", "ingest.storage_id")
	return settings
}

func (a *application) runDoctor(command *cobra.Command, raw *doctorFlagValues) error {
	run := newDoctorRun(raw.online)
	if a.doctorConfigErr != nil {
		run.report.Profile.Selection = raw.profile
		run.fail("configuration", a.doctorConfigErr, ExitUsage, nil)
		for _, name := range []string{
			"profile", "staging", "ffprobe", "ffmpeg", "authentication", "service",
			"api_compatibility", "service_lifetimes", "storage_backends", "storage_selection",
		} {
			run.skip(name, "configuration did not resolve safely")
		}
		return a.finishDoctor(command, run)
	}

	settings := a.doctorSettings(command, raw)
	var configErr error
	if settings.storageID != "" {
		if _, err := uuid.Parse(settings.storageID); err != nil {
			configErr = errors.Join(configErr, fmt.Errorf("storage ID must be a UUID: %w", err))
		}
	}
	if configErr != nil {
		run.fail("configuration", configErr, ExitUsage, nil)
	} else {
		detail := map[string]any{"source": "defaults, environment, flags"}
		if a.configFile != "" {
			detail["config_file"] = a.configFile
		}
		if a.configFileWarning != "" {
			detail["warning"] = a.configFileWarning
		}
		run.pass("configuration", detail)
	}

	allProfiles := strings.TrimSpace(settings.profile) == ""
	profile, profileErr := ingest.Profile{}, error(nil)
	if !allProfiles {
		profile, profileErr = a.resolvedConfigProfile(command)
	}
	run.report.Profile.Selection = settings.profile
	if profileErr != nil {
		run.fail("profile", profileErr, ExitUsage, nil)
	} else if allProfiles {
		run.report.Profile = doctorProfile{RequiresFFmpeg: true}
		run.pass("profile", map[string]any{
			"selection_required_for_ingest": true,
			"available_profiles":            versionedProfileSelections(),
			"requires_ffmpeg":               true,
		})
	} else {
		requiresFFmpeg := ingest.TreatmentRequiresFFmpeg(profile)
		run.report.Profile = doctorProfile{
			Selection: settings.profile, Name: profile.Name, Version: profile.Version,
			SegmentDuration: profile.SegmentDuration.String(), SegmentFormat: string(profile.SegmentFormat),
			EssenceStorage: string(profile.EssenceStorage), RequiresFFmpeg: requiresFFmpeg,
		}
		run.pass("profile", map[string]any{
			"name": profile.Name, "version": profile.Version,
			"segment_duration": profile.SegmentDuration.String(),
			"segment_format":   string(profile.SegmentFormat), "essence_storage": string(profile.EssenceStorage),
			"requires_ffmpeg": requiresFFmpeg,
		})
	}

	stagingBytes, budgetErr := parseByteSize(settings.stagingByteBudget)
	settings.stagingBytes = stagingBytes
	if budgetErr != nil {
		run.fail("staging", budgetErr, ExitUsage, nil)
	} else if inspection, err := ingest.InspectStaging(settings.tempDirectory, stagingBytes); err != nil {
		run.fail("staging", err, ExitGeneral, stagingDetail(inspection, settings.stagingByteBudget))
	} else {
		run.pass("staging", stagingDetail(inspection, settings.stagingByteBudget))
	}

	ffprobe := media.FFprobe{Executable: a.v.GetString("media.ffprobe")}
	if report, err := doctorToolVersion(command.Context(), ffprobe.Version); err != nil || media.ValidateToolVersion(report, "FFprobe") != nil {
		run.fail("ffprobe", safeMediaToolError("FFprobe"), ExitMedia, nil)
	} else {
		run.pass("ffprobe", map[string]any{"available": true, "minimum_version": "5.1"})
	}
	if profileErr != nil {
		run.skip("ffmpeg", "profile did not resolve, so the media-writing requirement is unknown")
	} else if !allProfiles && !ingest.TreatmentRequiresFFmpeg(profile) {
		run.skip("ffmpeg", "resolved treatment uploads source bytes without FFmpeg")
	} else {
		ffmpeg := media.FFmpeg{Executable: a.v.GetString("media.ffmpeg")}
		if report, err := doctorToolVersion(command.Context(), ffmpeg.Version); err != nil || media.ValidateToolVersion(report, "FFmpeg") != nil {
			run.fail("ffmpeg", safeMediaToolError("FFmpeg"), ExitMedia, nil)
		} else {
			run.pass("ffmpeg", map[string]any{"available": true, "minimum_version": "5.1"})
		}
	}

	if raw.online && configErr == nil {
		a.runDoctorOnline(command.Context(), run, settings)
	} else if configErr != nil {
		for _, name := range []string{"authentication", "service", "api_compatibility", "service_lifetimes", "storage_backends", "storage_selection"} {
			run.skip(name, "configuration did not resolve safely")
		}
	} else {
		for _, name := range []string{"authentication", "service", "api_compatibility", "service_lifetimes", "storage_backends", "storage_selection"} {
			run.skip(name, "online checks were not requested")
		}
	}

	return a.finishDoctor(command, run)
}

func (a *application) finishDoctor(command *cobra.Command, run *doctorRun) error {
	if err := a.writeDoctorReport(run.report); err != nil {
		return withExit(ExitGeneral, err)
	}
	if err := command.Context().Err(); err != nil {
		return withExit(ExitInterrupted, errors.New("doctor was interrupted"))
	}
	if run.report.Status == doctorFail {
		return withExit(run.exitCode(), errors.New("doctor checks failed; see report for details"))
	}
	return nil
}

func doctorToolVersion(ctx context.Context, version func(context.Context) (string, error)) (string, error) {
	checkCtx, cancel := context.WithTimeout(ctx, doctorToolVersionTimeout)
	defer cancel()
	return version(checkCtx)
}

func stagingDetail(inspection ingest.StagingInspection, configured string) map[string]any {
	detail := map[string]any{"configured_budget": configured}
	if inspection.Directory != "" {
		detail["directory"] = inspection.Directory
		detail["filesystem_free_bytes"] = inspection.FilesystemFreeBytes
		detail["configured_budget_bytes"] = inspection.ConfiguredBudgetBytes
		detail["effective_budget_bytes"] = inspection.EffectiveBudgetBytes
	}
	return detail
}

func (a *application) runDoctorOnline(ctx context.Context, run *doctorRun, settings doctorSettings) {
	endpoint := strings.TrimRight(a.v.GetString("endpoint"), "/")
	if endpoint == "" {
		run.fail("authentication", errors.New("--online requires --endpoint"), ExitUsage, nil)
		for _, name := range []string{"service", "api_compatibility", "service_lifetimes", "storage_backends", "storage_selection"} {
			run.skip(name, "no TAMS endpoint was configured")
		}
		return
	}
	run.report.Endpoint = auth.RedactURL(endpoint)

	cleanEndpoint, endpointToken, extractErr := auth.ExtractURLToken(endpoint)
	if extractErr == nil {
		config := a.authenticationConfig(cleanEndpoint, endpointToken)
		mode, modeErr := config.ResolveMode()
		if modeErr == nil {
			run.report.Auth = string(mode)
		}
		// Keep pure credential/transport validation authoritative. An unsafe
		// endpoint is a readiness failure even when Doctor would also need a
		// pre-obtained authorization code; NewRoundTripper repeats the same
		// validation immediately before any network work.
		if modeErr == nil && config.Validate(mode) == nil && mode == auth.ModeOAuthCode && config.OAuthCode == "" {
			run.fail("authentication", errors.New(
				"doctor is non-interactive; OAuth authorization-code mode requires a pre-obtained --oauth-code"), ExitAuth,
				map[string]any{"endpoint": run.report.Endpoint, "mode": run.report.Auth})
			for _, name := range []string{"service", "api_compatibility", "service_lifetimes", "storage_backends", "storage_selection"} {
				run.skip(name, "authentication requires an external authorization-code step")
			}
			return
		}
	}
	client, mode, err := a.tamsClient(ctx, endpoint, a.httpTransport(0, 0), nil)
	if err != nil {
		run.fail("authentication", err, ExitAuth, map[string]any{
			"endpoint": run.report.Endpoint, "mode": run.report.Auth,
		})
		for _, name := range []string{"service", "api_compatibility", "service_lifetimes", "storage_backends", "storage_selection"} {
			run.skip(name, "authentication or credential-transport policy failed")
		}
		return
	}
	run.report.Auth = string(mode)

	var (
		wait        sync.WaitGroup
		service     map[string]any
		serviceErr  error
		backends    []tams.StorageBackend
		backendsErr error
	)
	wait.Add(2)
	go func() {
		defer wait.Done()
		service, serviceErr = client.Service(ctx)
	}()
	go func() {
		defer wait.Done()
		backends, backendsErr = client.StorageBackends(ctx)
	}()
	wait.Wait()

	authStatus := authenticationRejectionStatus(serviceErr, backendsErr)
	if serviceErr == nil || backendsErr == nil {
		run.pass("authentication", map[string]any{
			"endpoint": run.report.Endpoint, "mode": run.report.Auth,
			"evidence": "at least one read-only endpoint accepted the request",
		})
	} else if authStatus != 0 {
		run.fail("authentication", fmt.Errorf("TAMS rejected the request with %s", http.StatusText(authStatus)), ExitAuth,
			map[string]any{"endpoint": run.report.Endpoint, "mode": run.report.Auth, "status_code": authStatus})
	} else {
		run.skip("authentication", "no successful TAMS response confirmed the configured credentials")
	}

	if serviceErr != nil {
		run.fail("service", safeDoctorRemoteError("GET /service", serviceErr), remoteExitCode(serviceErr), nil)
		run.skip("api_compatibility", "service information was unavailable")
		run.skip("service_lifetimes", "service information was unavailable")
	} else {
		run.pass("service", map[string]any{"request": "GET /service"})
		assessment, compatibilityErr := ingest.AssessAPIVersion(service)
		detail := map[string]any{
			"target_version": assessment.TargetVersion, "relationship": assessment.Relationship,
		}
		if assessment.StoreVersion != "" {
			detail["store_version"] = assessment.StoreVersion
		}
		if assessment.Warning != "" {
			detail["warning"] = genericAPIWarning(assessment.Relationship)
		}
		if compatibilityErr != nil {
			run.fail("api_compatibility", compatibilityErr, ExitRemote, detail)
		} else {
			run.pass("api_compatibility", detail)
		}

		limits, limitsErr := tams.ParseServiceLimits(service)
		if limitsErr != nil {
			run.fail("service_lifetimes", safeLifetimeError(limitsErr), ExitRemote, nil)
		} else {
			run.pass("service_lifetimes", map[string]any{
				"object_registration": limits.ObjectRegistration.String(),
				"presigned_url":       limits.PresignedURL.String(),
			})
		}
	}

	if backendsErr != nil {
		run.fail("storage_backends", safeDoctorRemoteError("GET /service/storage-backends", backendsErr), remoteExitCode(backendsErr), nil)
		run.skip("storage_selection", "storage backends were unavailable")
	} else {
		run.pass("storage_backends", map[string]any{"count": len(backends), "request": "GET /service/storage-backends"})
		selection, selectionErr := ingest.ResolveStorageBackend(backends, settings.storageID)
		if selectionErr != nil {
			run.fail("storage_selection", selectionErr, ExitRemote, nil)
		} else {
			run.pass("storage_selection", map[string]any{
				"source": map[bool]string{true: "requested", false: "default"}[selection.Requested],
			})
		}
	}
}

func authenticationRejectionStatus(errs ...error) int {
	for _, err := range errs {
		var httpErr *tams.HTTPError
		if errors.As(err, &httpErr) && (httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden) {
			return httpErr.StatusCode
		}
	}
	return 0
}

func remoteExitCode(err error) int {
	if authenticationRejectionStatus(err) != 0 {
		return ExitAuth
	}
	return ExitRemote
}

func safeDoctorRemoteError(operation string, err error) error {
	var httpErr *tams.HTTPError
	if errors.As(err, &httpErr) {
		return fmt.Errorf("%s returned %d %s", operation, httpErr.StatusCode, http.StatusText(httpErr.StatusCode))
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	// The TAMS client suppresses raw transport errors and redacts URL query
	// values. Keeping its diagnostic preserves actionable request context.
	return err
}

func safeLifetimeError(err error) error {
	var limitErr *tams.ServiceLimitError
	if !errors.As(err, &limitErr) {
		return errors.New("TAMS service reported invalid lifetime guarantees")
	}
	switch limitErr.Field {
	case "min_presigned_url_timeout":
		return errors.New("TAMS service reported an invalid /min_presigned_url_timeout")
	case "min_object_timeout":
		return errors.New("TAMS service reported an invalid /min_object_timeout")
	default:
		return errors.New("TAMS service reported invalid lifetime guarantees")
	}
}

func genericAPIWarning(relationship string) string {
	if relationship == "older" {
		return "store implements an older compatible TAMS minor revision"
	}
	return "service api_version was missing or malformed"
}

func safeMediaToolError(name string) error {
	// A configured executable controls both its path and stderr. Neither belongs
	// in a support-oriented report because either can contain credential
	// material. The check name and this remediation remain actionable without
	// reproducing subprocess-controlled text.
	return fmt.Errorf("%s is unavailable or its version check failed; verify the configured executable", name)
}

func (a *application) writeDoctorReport(report doctorResult) error {
	if strings.EqualFold(a.v.GetString("format"), "human") {
		profile := report.Profile.Name + "@" + report.Profile.Version
		if report.Profile.Name == "" {
			profile = "all (selection required for ingest)"
		}
		if _, err := fmt.Fprintf(a.stdout,
			"doctor\tstatus=%s\ttamsin=%q\tgo=%q\tos=%s\tarch=%s\tprofile=%s\n",
			report.Status, report.Tamsin, report.Go, report.OS, report.Arch,
			profile); err != nil {
			return err
		}
		for _, check := range report.Checks {
			if _, err := fmt.Fprintf(a.stdout, "%s\t%s", strings.ToUpper(check.Status), check.Name); err != nil {
				return err
			}
			if len(check.Detail) > 0 {
				detail, err := json.Marshal(check.Detail)
				if err != nil {
					return err
				}
				if _, err := fmt.Fprintf(a.stdout, "\tdetail=%s", detail); err != nil {
					return err
				}
			}
			if check.Error != "" {
				if _, err := fmt.Fprintf(a.stdout, "\terror=%q", check.Error); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintln(a.stdout); err != nil {
				return err
			}
		}
		return nil
	}
	encoder := json.NewEncoder(a.stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(report)
}
