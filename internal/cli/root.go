package cli

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/livewyer-ops/tamsin/internal/auth"
	"github.com/livewyer-ops/tamsin/internal/ingest"
	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/netio"
	"github.com/livewyer-ops/tamsin/internal/observability"
	"github.com/livewyer-ops/tamsin/internal/presentation"
	"github.com/livewyer-ops/tamsin/internal/progress"
	"github.com/livewyer-ops/tamsin/internal/resultjournal"
	"github.com/livewyer-ops/tamsin/internal/source"
	"github.com/livewyer-ops/tamsin/internal/tams"
	"github.com/livewyer-ops/tamsin/internal/version"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// defaultSegmentDuration targets Media Objects that are, in the words of TAMS
// AppNote 0001, "typically short (on the order of seconds) and independently
// decodable". Cuts land on keyframes, so a long GOP raises the floor and actual
// Segments vary around this target. Set --segment-duration 0 to store an input
// as a single Media Object instead.
const defaultSegmentDuration = 10 * time.Second

const maxConfigFileBytes = 2 << 20

type application struct {
	v                *viper.Viper
	persistentFlags  *pflag.FlagSet
	stdin            io.Reader
	stdout           io.Writer
	stderr           io.Writer
	runID            string
	configFile       string
	ingestInvocation bool
	// ingestTerminalFrozen marks the point after which all terminal projections
	// share one cancellation decision. Execute must not let a later caller
	// cancellation rewrite only the process exit code.
	ingestTerminalFrozen bool
	events               *ingestEventOutput
	humanReceipt         bool
	// doctorConfigErr lets doctor render configuration failures inside its
	// versioned check report. Other commands retain the ordinary pre-run usage
	// error and never execute with invalid configuration.
	doctorConfigErr error
}

func Execute(ctx context.Context, arguments []string, stdin io.Reader, stdout, stderr io.Writer) int {
	app := &application{v: viper.New(), stdin: stdin, stdout: stdout, stderr: stderr, runID: uuid.NewString()}
	explicitFormat, requestedJSON := requestedIngestFormat(arguments)
	command := app.rootCommand()
	// Command discovery happens before Cobra parses flags so even a malformed
	// ingest invocation can honour an explicitly requested machine protocol.
	// Without this, an unknown flag returned before Args/PreRunE and wrappers
	// received an empty stdout stream plus an unstructured stderr sentence.
	target, _, _ := command.Find(arguments)
	app.ingestInvocation = target == command || (target != nil && target.Name() == "ingest")
	command.SetArgs(arguments)
	command.SetIn(stdin)
	command.SetOut(stdout)
	command.SetErr(stderr)
	if err := command.ExecuteContext(ctx); err != nil {
		code := exitCode(err)
		// The caller-owned context is the only authoritative run interruption.
		// Explicit ExitErrors still distinguish child request deadlines while the
		// parent remains live.
		if ctx.Err() != nil && !app.ingestTerminalFrozen {
			code = ExitInterrupted
		}
		machineJSON := strings.EqualFold(app.v.GetString("format"), "json")
		if explicitFormat {
			machineJSON = requestedJSON
		}
		if app.ingestInvocation && machineJSON {
			if resolvedCode, eventErr := app.finishBootstrapEvents(ctx, err, code); eventErr == nil && app.events != nil && app.events.Finished() {
				return resolvedCode
			} else if eventErr != nil {
				err = errors.Join(err, fmt.Errorf("write structured ingest failure: %w", eventErr))
				code = ExitGeneral
			}
		}
		if app.ingestInvocation && app.humanReceipt {
			return code
		}
		message := err.Error()
		var public interface{ PublicMessage() string }
		if errors.As(err, &public) {
			message = public.PublicMessage()
		}
		_, _ = fmt.Fprintf(stderr, "tamsin: %s\n", strconv.QuoteToGraphic(message))
		return code
	}
	return ExitOK
}

// requestedIngestFormat discovers the last explicit --format value without
// depending on pflag's parse position. Cobra stops at a malformed/unknown flag,
// but a supervising process still needs its explicitly requested JSON failure
// protocol when --format appears later in argv. Values after -- are positional
// media arguments and are intentionally ignored.
func requestedIngestFormat(arguments []string) (found, jsonRequested bool) {
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if argument == "--" {
			break
		}
		switch {
		case argument == "--format" && index+1 < len(arguments):
			index++
			found = true
			jsonRequested = strings.EqualFold(arguments[index], "json")
		case strings.HasPrefix(argument, "--format="):
			found = true
			jsonRequested = strings.EqualFold(strings.TrimPrefix(argument, "--format="), "json")
		}
	}
	return found, jsonRequested
}

func (a *application) rootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           "tamsin [flags] [input] [TAMS endpoint]",
		Short:         "Ingest media into a Time-addressable Media Store",
		Long:          "Tamsin resolves local, manifest, HTTP, and S3 inputs, creates TAMS Flows, and uploads verified Media Objects.",
		Version:       version.String(),
		SilenceUsage:  true,
		SilenceErrors: true,
		Args: usageArgs(func(command *cobra.Command, args []string) error {
			a.ingestInvocation = true
			return cobra.MaximumNArgs(2)(command, args)
		}),
	}
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error { return withExit(ExitUsage, err) })

	a.configureDefaults()
	a.addPersistentFlags(root)
	root.PersistentPreRunE = func(command *cobra.Command, _ []string) error {
		a.ingestInvocation = command == root || command.Name() == "ingest"
		if command.Annotations[helpGroupAnnotation] == "true" ||
			command.Annotations[configIndependentAnnotation] == "true" {
			return nil
		}
		if err := a.loadConfig(); err != nil {
			if command.Name() == "doctor" {
				a.doctorConfigErr = err
				return nil
			}
			return withExit(ExitUsage, err)
		}
		if err := a.validateConfigEnvironment(); err != nil {
			if command.Name() == "doctor" {
				a.doctorConfigErr = err
				return nil
			}
			return withExit(ExitUsage, err)
		}
		if command.Name() == "doctor" {
			// Doctor reports invalid local readiness flags in their own profile,
			// staging, or configuration checks. Validate the underlying config here,
			// but permit a valid higher-precedence Doctor flag to repair it.
			if a.validateConfigValues(nil) != nil {
				effectiveErr := a.validateConfigValues(command)
				if effectiveErr == nil {
					return nil
				}
				a.doctorConfigErr = effectiveErr
			}
			return nil
		}
		if err := a.validateConfigValues(command); err != nil {
			return withExit(ExitUsage, err)
		}
		return nil
	}

	rootFlags := addIngestFlags(root)
	root.RunE = func(command *cobra.Command, args []string) error {
		a.ingestInvocation = true
		return a.runIngest(command, args, rootFlags)
	}

	root.AddCommand(a.ingestCommand())
	root.AddCommand(retiredAPICommand())
	root.AddCommand(a.configCommand())
	root.AddCommand(a.doctorCommand())
	root.AddCommand(a.profilesCommand())
	root.AddCommand(a.completionCommand(root))
	return root
}

// retiredAPICommand prevents a pre-split invocation from being interpreted as
// an implicit ingest of a local file named "api". It is hidden because it has
// no functionality and is not part of TAMSin's command surface.
func retiredAPICommand() *cobra.Command {
	const migration = "tamsin api has moved to tamsctl; remove the api command segment (for example: tamsctl flow get ...)"
	return &cobra.Command{
		Use:                "api",
		Short:              migration,
		Long:               migration,
		Hidden:             true,
		DisableFlagParsing: true,
		Annotations:        map[string]string{configIndependentAnnotation: "true"},
		RunE: func(*cobra.Command, []string) error {
			return withExit(ExitUsage, errors.New(migration))
		},
	}
}

func (a *application) ingestCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "ingest [flags] [input] [TAMS endpoint]",
		Short: "Create one Flow graph per resolved input and ingest its media",
		Args: usageArgs(func(command *cobra.Command, args []string) error {
			a.ingestInvocation = true
			return cobra.MaximumNArgs(2)(command, args)
		}),
	}
	ingestFlags := addIngestFlags(command)
	command.RunE = func(command *cobra.Command, args []string) error {
		a.ingestInvocation = true
		return a.runIngest(command, args, ingestFlags)
	}
	return command
}

func (a *application) addPersistentFlags(command *cobra.Command) {
	flags := command.PersistentFlags()
	a.persistentFlags = flags
	flags.String("config", "", "configuration file (default: $XDG_CONFIG_HOME/tamsin/config.yaml)")
	flags.StringP("endpoint", "o", "", "TAMS API endpoint")
	flags.String("format", "human", "result format: human or json")
	flags.String("progress", "auto", "progress reporting: auto, tty, plain, or none")
	flags.String("color", "auto", "color output: auto, always, or never")
	flags.BoolP("quiet", "q", false, "suppress successful human output")
	flags.BoolP("verbose", "v", false, "show expanded human result details")
	flags.String("log-format", "text", "diagnostic log format: text or json")
	flags.String("log-level", "info", "diagnostic level: debug, info, warn, or error")
	flags.Duration("timeout", 30*time.Second, "per-request timeout for TAMS metadata operations")
	flags.Duration("transfer-timeout", 0,
		"optional deadline for a complete media transfer (0 disables)")
	flags.Duration("transfer-idle-timeout", netio.DefaultIdleTimeout,
		"maximum time a network media transfer may make no progress")
	flags.Int("retries", 3, "retry count for safe HTTP operations")
	flags.Bool("insecure-skip-verify", false, "skip TLS certificate verification (unsafe)")
	flags.String("ffprobe", "ffprobe", "ffprobe executable")
	flags.String("ffmpeg", "ffmpeg", "ffmpeg executable")

	flags.String("auth", string(auth.ModeAuto), "authentication: auto, none, basic, bearer, url-token, oauth-client, or oauth-code")
	flags.String("username", "", "HTTP basic username")
	flags.String("password", "", "HTTP basic password (prefer TAMSIN_AUTH_PASSWORD)")
	flags.String("token", "", "bearer token (prefer TAMSIN_AUTH_TOKEN)")
	flags.String("url-token", "", "TAMS access_token value (prefer endpoint URL or environment)")
	flags.String("token-url", "", "OAuth token endpoint")
	flags.String("authorization-url", "", "OAuth authorization endpoint")
	flags.String("client-id", "", "OAuth client ID")
	flags.String("client-secret", "", "OAuth client secret (prefer TAMSIN_AUTH_CLIENT_SECRET)")
	flags.String("redirect-url", "http://127.0.0.1:53682/callback", "OAuth authorization-code redirect URL")
	flags.StringSlice("scope", nil, "OAuth scope (repeat or comma-separate)")
	flags.String("oauth-code", "", "pre-obtained OAuth authorization code")
	flags.String("pkce-verifier", "", "PKCE verifier for a pre-obtained OAuth code (prefer environment)")
	flags.Bool("allow-insecure-auth-loopback", false,
		"allow credentials over HTTP to explicit loopback hosts (unsafe)")

	for _, definition := range configDefinitions() {
		if definition.flag == "" {
			continue
		}
		flag := flags.Lookup(definition.flag)
		if flag == nil {
			panic("configuration definition names an unknown persistent flag: " + definition.flag)
		}
		if err := a.v.BindPFlag(definition.key, flag); err != nil {
			panic(err)
		}
	}
}

func (a *application) loadConfig() error {
	filename := a.v.GetString("config")
	if filename == "" {
		configDirectory, err := os.UserConfigDir()
		if err == nil {
			candidate := filepath.Join(configDirectory, "tamsin", "config.yaml")
			if _, statErr := os.Lstat(candidate); statErr == nil {
				filename = candidate
			} else if !errors.Is(statErr, os.ErrNotExist) {
				return fmt.Errorf("inspect default configuration %q: %w", candidate, statErr)
			}
		}
	}
	if filename == "" {
		return nil
	}
	data, err := readConfigFile(filename)
	if err != nil {
		return fmt.Errorf("read configuration %q: %w", filename, err)
	}
	if err := a.validateConfigFile(data); err != nil {
		return fmt.Errorf("read configuration %q: %w", filename, err)
	}
	a.v.SetConfigFile(filename)
	// The public contract is YAML, independent of a mounted file's suffix.
	// Parsing the already-validated bytes also avoids reading a different file
	// if the path is replaced between validation and precedence resolution.
	a.v.SetConfigType("yaml")
	if err := a.v.ReadConfig(bytes.NewReader(data)); err != nil {
		return fmt.Errorf("read configuration %q: %w", filename, err)
	}
	a.configFile = filename
	return nil
}

func readConfigFile(filename string) ([]byte, error) {
	info, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("configuration must be a regular file")
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err = file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("configuration must be a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxConfigFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxConfigFileBytes {
		return nil, errors.New("configuration exceeds 2 MiB")
	}
	return data, nil
}

func (a *application) validateGlobalConfig() error {
	switch strings.ToLower(a.v.GetString("format")) {
	case "human", "json":
	default:
		return fmt.Errorf("invalid result format %q", a.v.GetString("format"))
	}
	switch strings.ToLower(a.v.GetString("color")) {
	case "auto", "always", "never":
	default:
		return fmt.Errorf("invalid color mode %q", a.v.GetString("color"))
	}
	switch strings.ToLower(a.v.GetString("log.format")) {
	case "text", "json":
	default:
		return fmt.Errorf("invalid log format %q", a.v.GetString("log.format"))
	}
	switch strings.ToLower(a.v.GetString("log.level")) {
	case "debug", "info", "warn", "warning", "error":
	default:
		return fmt.Errorf("invalid log level %q", a.v.GetString("log.level"))
	}
	switch strings.ToLower(a.v.GetString("progress")) {
	case "auto", "tty", "plain", "none":
	default:
		return fmt.Errorf("invalid progress mode %q", a.v.GetString("progress"))
	}
	switch auth.Mode(a.v.GetString("auth.mode")) {
	case auth.ModeAuto, auth.ModeNone, auth.ModeBasic, auth.ModeBearer, auth.ModeURLToken, auth.ModeOAuthClient, auth.ModeOAuthCode:
	default:
		return fmt.Errorf("invalid authentication mode %q", a.v.GetString("auth.mode"))
	}
	if timeout := a.v.GetDuration("http.timeout"); timeout <= 0 {
		return errors.New("HTTP timeout must be positive")
	}
	if timeout := a.v.GetDuration("http.transfer_timeout"); timeout < 0 {
		return errors.New("transfer timeout cannot be negative")
	}
	if timeout := a.v.GetDuration("http.transfer_idle_timeout"); timeout <= 0 {
		return errors.New("transfer idle timeout must be positive")
	}
	if retries := a.v.GetInt("http.retries"); retries < 0 || retries > 20 {
		return errors.New("HTTP retries must be between 0 and 20")
	}
	return nil
}

type ingestFlagValues struct {
	inputs            []string
	profile           string
	profileVersion    string
	concurrency       int
	transfers         int
	probeConcurrency  int
	dryRun            string
	verify            string
	tempDirectory     string
	stagingByteBudget string
	stagingBytes      int64
	maxInputs         int
	segmentDuration   time.Duration
	segmentFormat     string
	essenceStorage    string
	ffmpegArgs        []string
	start             string
	storageID         string
	flowID            string
	sourceID          string
	metadataFile      string
	tamsFlowProfiles  []string
	journal           string
	stdinName         string
	inputHeaders      []string
	s3Region          string
	s3Endpoint        string
	s3PathStyle       bool
	ffprobe           string
	ffmpeg            string
}

type ingestLifecycleOutput struct {
	events  *ingestEventOutput
	journal *resultjournal.Writer
}

func (o *ingestLifecycleOutput) InputStarted(index int) error {
	if o.events == nil {
		return nil
	}
	return o.events.InputStarted(index)
}

func (o *ingestLifecycleOutput) FlowPlanned(index int, plan ingest.FlowPlan) error {
	if o.events == nil {
		return nil
	}
	return o.events.FlowPlanned(index, plan)
}

func (o *ingestLifecycleOutput) ObjectsCompleted(index int, flowID string, objects []ingest.ObjectResult) error {
	var eventErr, journalErr error
	if o.events != nil {
		eventErr = o.events.ObjectsCompleted(index, flowID, objects)
	}
	if o.journal != nil {
		journalErr = o.journal.WriteObjectBatch(index, flowID, objects)
	}
	return errors.Join(eventErr, journalErr)
}

func addIngestFlags(command *cobra.Command) *ingestFlagValues {
	values := &ingestFlagValues{}
	flags := command.Flags()
	flags.StringArrayVarP(&values.inputs, "input", "i", nil, "input path or URI (repeatable)")
	flags.StringVar(&values.profile, "profile", "", profileFlagDescription())
	registerProfileCompletion(command)
	// Left at zero so --help does not print a number that is only true on the
	// machine that printed it. The real default is resolved from configuration
	// below, the same way --probe-concurrency does it.
	flags.IntVarP(&values.concurrency, "concurrency", "j", 0,
		"maximum concurrent input ingests (default: CPU count, at most 8)")
	flags.IntVar(&values.transfers, "transfers", 0,
		"maximum Media Object uploads and verifications in flight across the whole run (default: --concurrency)")
	flags.IntVar(&values.probeConcurrency, "probe-concurrency", 0,
		"maximum queued FFprobe measurements (local media processes are capped at two)")
	flags.StringVar(&values.dryRun, "dry-run", string(ingest.DryRunOff),
		"local-only planning mode: fast or exact")
	flags.Lookup("dry-run").NoOptDefVal = string(ingest.DryRunFast)
	flags.StringVar(&values.verify, "verify", string(ingest.VerificationAuto),
		"Object integrity policy: auto, readback, or none")
	flags.Lookup("verify").NoOptDefVal = string(ingest.VerificationAuto)
	flags.StringVar(&values.tempDirectory, "temp-dir", "", "staging directory")
	flags.StringVar(&values.stagingByteBudget, "staging-byte-budget", "auto",
		"global temporary-media budget: auto or a byte size such as 80GiB")
	flags.IntVar(&values.maxInputs, "max-inputs", source.DefaultMaxInputs,
		"maximum unique inputs after directory, manifest, and S3 prefix expansion")
	flags.DurationVarP(&values.segmentDuration, "segment-duration", "d", defaultSegmentDuration,
		"target duration of each TAMS Flow Segment; 0 disables segmentation, leaving storage to decide whole input or whole essence")
	flags.StringVar(&values.segmentFormat, "segment-format", string(media.SegmentFormatSource),
		"container for Flow Segments: source or mpegts")
	flags.StringVar(&values.essenceStorage, "essence-storage", string(media.EssenceStorageIndependent),
		"how a muxed input is stored: independent (one Flow per essence) or muxed (keep the multiplex)")
	flags.StringArrayVar(&values.ffmpegArgs, "ffmpeg-arg", nil, "additional explicit FFmpeg argument (repeatable)")
	flags.StringVar(&values.start, "start", "0:0", "Flow start as a TAMS timestamp")
	flags.StringVar(&values.storageID, "storage-id", "", "target TAMS storage backend ID")
	flags.StringVar(&values.flowID, "flow-id", "", "Flow UUID for a single resolved input")
	flags.StringVar(&values.sourceID, "source-id", "", "Source UUID for a single resolved input")
	flags.StringVar(&values.metadataFile, "flow-metadata", "", "JSON Flow metadata overrides")
	flags.StringArrayVar(&values.tamsFlowProfiles, "tams-flow-profile", nil,
		"TAMS 8.2 Flow Profile assignment as [video|audio|image|data[:N]=]UUID (repeatable)")
	flags.StringVar(&values.journal, "journal", "", "create a new one-run durable JSONL result file")
	flags.StringVar(&values.stdinName, "stdin-name", "stdin.bin",
		"filename hint; explicitly selects stdin unless input is configured or passed with --input")
	flags.StringArrayVar(&values.inputHeaders, "input-header", nil, "HTTP input header as 'Name: value' (repeatable)")
	flags.StringVar(&values.s3Region, "s3-region", "", "AWS region override for S3 inputs")
	flags.StringVar(&values.s3Endpoint, "s3-endpoint", "", "S3-compatible endpoint URL")
	flags.BoolVar(&values.s3PathStyle, "s3-path-style", false, "use path-style S3 addressing")
	return values
}

func (a *application) runIngest(command *cobra.Command, args []string, raw *ingestFlagValues) (returnErr error) {
	runCtx, cancel := context.WithCancelCause(command.Context())
	defer cancel(nil)

	var (
		options   *ingestFlagValues
		batch     ingest.BatchResult
		haveBatch bool
		run       *observability.Run
	)
	if strings.EqualFold(a.v.GetString("format"), "json") {
		output, err := newIngestEventOutput(a.stdout, a.runID, cancel)
		if err != nil {
			return withExit(ExitGeneral, err)
		}
		a.events = output
		// Only the caller-owned command context represents a protocol-level
		// interruption. Internal cancellation (journal/event sink failure) stops
		// pipeline work but must remain a failed run, not manufacture a signal.
		output.WatchCancellation(command.Context())
		defer func() {
			metrics := observability.Snapshot{}
			if run != nil {
				metrics = run.Snapshot()
			}
			var completed *ingest.BatchResult
			if haveBatch {
				completed = &batch
			}
			requestedCode := exitCode(returnErr)
			resolvedCode, err := output.Finish(completed, returnErr, requestedCode, options, metrics, command.Context())
			if err != nil {
				returnErr = withExit(ExitGeneral, errors.Join(returnErr, fmt.Errorf("finish ingest event stream: %w", err)))
			} else {
				a.ingestTerminalFrozen = true
			}
			if err == nil && resolvedCode != requestedCode {
				cause := context.Cause(runCtx)
				if cause == nil {
					cause = context.Canceled
				}
				returnErr = withExit(resolvedCode, errors.Join(returnErr, cause))
			}
		}()
	}

	var endpoint string
	var err error
	options, endpoint, err = a.ingestOptions(command, args, raw)
	if err != nil {
		return withExit(ExitUsage, err)
	}
	if a.events != nil {
		if err := a.events.Start(options); err != nil {
			return withExit(ExitGeneral, err)
		}
	}

	reporter := a.reporter()
	if a.events != nil && !strings.EqualFold(a.v.GetString("progress"), "none") {
		reporter = a.events
	}
	defer reporter.Close()
	logger, err := a.loggerFor(reporter)
	if err != nil {
		return withExit(ExitUsage, err)
	}
	run = observability.New(a.runID, logger)
	if a.events != nil {
		run.SetRetryObserver(a.events.Retry)
	}
	logger = run.Logger()
	defer func() {
		if returnErr != nil {
			run.Failure(returnErr)
		}
	}()
	transport := a.httpTransport()
	inputHeaders, err := parseHeaders(options.inputHeaders)
	if err != nil {
		return withExit(ExitUsage, err)
	}
	retryClient := retryablehttp.NewClient()
	retryClient.Logger = nil
	retryClient.RetryMax = a.v.GetInt("http.retries")
	retryClient.Backoff = observedSourceBackoff(run, retryClient.RetryMax)
	// Source bodies are whole media. The optional absolute deadline remains on
	// the client; byte-level idle detection is applied to each body by source.
	retryClient.HTTPClient = &http.Client{Transport: transport, Timeout: a.v.GetDuration("http.transfer_timeout")}
	inputHTTPClient := retryClient.StandardClient()
	awsHTTPClient := &http.Client{
		Transport: transport, Timeout: a.v.GetDuration("http.transfer_timeout"), CheckRedirect: rejectRedirect,
	}
	resolver := source.New(source.Config{
		HTTPClient: inputHTTPClient, HTTPHeaders: inputHeaders, Stdin: a.stdin, StdinName: options.stdinName,
		S3:                  source.S3Config{Region: options.s3Region, Endpoint: options.s3Endpoint, UsePathStyle: options.s3PathStyle, HTTPClient: awsHTTPClient},
		TransferIdleTimeout: a.v.GetDuration("http.transfer_idle_timeout"), MetadataTimeout: a.v.GetDuration("http.timeout"),
		MaxInputs: options.maxInputs, Observability: run,
	})
	items, err := resolver.Resolve(runCtx, options.inputs)
	if err != nil {
		return withExit(ExitSource, err)
	}
	if a.events != nil {
		if err := a.events.Declare(items); err != nil {
			return withExit(ExitGeneral, err)
		}
	}
	if (options.flowID != "" || options.sourceID != "") && len(items) != 1 {
		return withExit(ExitUsage, errors.New("explicit flow-id and source-id may only be used with one resolved input"))
	}

	start, err := media.ParseTimestamp(options.start)
	if err != nil {
		return withExit(ExitUsage, err)
	}
	metadata, err := readFlowMetadata(options.metadataFile)
	if err != nil {
		return withExit(ExitUsage, err)
	}

	var client *tams.Client
	if ingest.DryRunMode(options.dryRun) == ingest.DryRunOff || len(options.tamsFlowProfiles) > 0 {
		if endpoint == "" {
			return withExit(ExitUsage, errors.New("TAMS endpoint is required for ingest and Flow Profile validation; use --endpoint or a second positional argument"))
		}
		client, _, err = a.tamsClient(runCtx, endpoint, transport, run)
		if err != nil {
			return withExit(ExitAuth, err)
		}
	}
	lifecycleOutput := &ingestLifecycleOutput{events: a.events}
	var lifecycleObserver ingest.LifecycleObserver
	if a.events != nil || options.journal != "" {
		lifecycleObserver = lifecycleOutput
	}
	pipeline, err := ingest.New(ingest.Config{
		Observability: run, LifecycleObserver: lifecycleObserver,
		Profile: options.profile, ProfileVersion: options.profileVersion,
		Concurrency: options.concurrency, Transfers: options.transfers, ProbeConcurrency: options.probeConcurrency, Retries: a.v.GetInt("http.retries"),
		DryRunMode: ingest.DryRunMode(options.dryRun), VerificationMode: ingest.VerificationMode(options.verify),
		TempDirectory: options.tempDirectory, StagingByteBudget: options.stagingBytes,
		SegmentDuration: options.segmentDuration, SegmentFormat: media.SegmentFormat(options.segmentFormat), EssenceStorage: media.EssenceStorage(options.essenceStorage), FFmpegArgs: options.ffmpegArgs, Start: start, StorageID: options.storageID,
		FlowID: options.flowID, SourceID: options.sourceID, FlowMetadata: metadata, TAMSFlowProfiles: options.tamsFlowProfiles,
	}, client, media.FFprobe{Executable: options.ffprobe}, media.FFmpeg{Executable: options.ffmpeg}, logger, reporter)
	if err != nil {
		return withExit(ExitUsage, err)
	}
	var journal *resultjournal.Writer
	if options.journal != "" {
		journal, err = resultjournal.Open(options.journal, pipeline.ResultContract(), items)
		if err != nil {
			return withExit(ExitGeneral, err)
		}
		lifecycleOutput.journal = journal
	}
	var journalResultErr error
	var observe ingest.ResultObserver
	if journal != nil || a.events != nil {
		observe = func(index int, result ingest.Result) error {
			var eventErr error
			if a.events != nil {
				eventErr = a.events.Result(index, result)
			}
			if journal != nil && journalResultErr == nil {
				journalResultErr = journal.WriteInput(index, result)
			}
			return errors.Join(eventErr, journalResultErr)
		}
	}
	var pipelineErr error
	batch, pipelineErr = pipeline.RunObserved(runCtx, items, observe)
	haveBatch = true
	// The transient region must be closed before either human or structured
	// permanent output begins. This also protects PTY recorders which combine
	// stdout and stderr into one byte stream.
	reporter.Close()
	terminal := terminalDecisionFromContext(command.Context())
	if a.events != nil {
		terminal = a.events.FreezeTerminal(command.Context())
	}
	// This is the single terminal linearization point for the journal, human or
	// NDJSON result, and process status. A later caller cancellation is treated
	// as shutdown after terminalization began and cannot rewrite one projection.
	a.ingestTerminalFrozen = true
	var journalErr error
	if journal != nil {
		journalErr = errors.Join(journalResultErr,
			journal.WriteSummary(batch, pipelineErr, terminal.interrupted), journal.Close())
	}

	var resultErr error
	switch {
	case terminal.interrupted:
		resultErr = withExit(ExitInterrupted, terminal.cause)
	case journalErr != nil:
		resultErr = withExit(ExitGeneral, safeProcessFailure(
			ingest.FailureCodeJournalWrite, ingest.FailureMessageJournalWrite, true, journalErr))
	case pipelineErr != nil:
		if a.events != nil && a.events.Err() != nil {
			resultErr = withExit(ExitGeneral, pipelineErr)
		} else {
			resultErr = withExit(ExitRemote, pipelineErr)
		}
	case batch.Failed > 0:
		resultErr = withExit(ExitPartial, fmt.Errorf("%d of %d inputs failed", batch.Failed, len(batch.Results)))
	}
	if a.events == nil {
		if outputErr := a.writeBatch(batch, run.Snapshot()); outputErr != nil {
			return withExit(ExitGeneral, outputErr)
		}
		a.humanReceipt = journalErr == nil && resultErr != nil && (batch.Failed > 0 || pipelineErr != nil)
	}
	return resultErr
}

func (a *application) ingestOptions(command *cobra.Command, args []string, raw *ingestFlagValues) (*ingestFlagValues, string, error) {
	options := *raw
	options.inputs = stringArrayOption(command.Flags(), "input", raw.inputs, a.configStrings("input"))
	options.profile = stringOption(command.Flags(), "profile", raw.profile, a.v.GetString("ingest.profile"))
	options.concurrency = intOption(command.Flags(), "concurrency", raw.concurrency, a.v.GetInt("ingest.concurrency"))
	options.transfers = intOption(command.Flags(), "transfers", raw.transfers, a.v.GetInt("ingest.transfers"))
	options.probeConcurrency = intOption(command.Flags(), "probe-concurrency", raw.probeConcurrency, a.v.GetInt("ingest.probe_concurrency"))
	options.dryRun = stringOption(command.Flags(), "dry-run", raw.dryRun, a.v.GetString("ingest.dry_run"))
	options.verify = stringOption(command.Flags(), "verify", raw.verify, a.v.GetString("ingest.verify"))
	options.tempDirectory = stringOption(command.Flags(), "temp-dir", raw.tempDirectory, a.v.GetString("ingest.temp_directory"))
	options.stagingByteBudget = stringOption(command.Flags(), "staging-byte-budget", raw.stagingByteBudget, a.v.GetString("ingest.staging_byte_budget"))
	options.maxInputs = intOption(command.Flags(), "max-inputs", raw.maxInputs, a.v.GetInt("ingest.max_inputs"))
	options.segmentDuration = durationOption(command.Flags(), "segment-duration", raw.segmentDuration, a.v.GetDuration("ingest.segment_duration"))
	options.segmentFormat = stringOption(command.Flags(), "segment-format", raw.segmentFormat, a.v.GetString("ingest.segment_format"))
	options.essenceStorage = stringOption(command.Flags(), "essence-storage", raw.essenceStorage, a.v.GetString("ingest.essence_storage"))
	options.ffmpegArgs = stringArrayOption(command.Flags(), "ffmpeg-arg", raw.ffmpegArgs, a.configStrings("media.ffmpeg_args"))
	options.start = stringOption(command.Flags(), "start", raw.start, a.v.GetString("ingest.start"))
	options.storageID = stringOption(command.Flags(), "storage-id", raw.storageID, a.v.GetString("ingest.storage_id"))
	options.flowID = stringOption(command.Flags(), "flow-id", raw.flowID, a.v.GetString("ingest.flow_id"))
	options.sourceID = stringOption(command.Flags(), "source-id", raw.sourceID, a.v.GetString("ingest.source_id"))
	options.metadataFile = stringOption(command.Flags(), "flow-metadata", raw.metadataFile, a.v.GetString("ingest.flow_metadata"))
	options.tamsFlowProfiles = stringArrayOption(command.Flags(), "tams-flow-profile", raw.tamsFlowProfiles, a.configStrings("ingest.tams_flow_profiles"))
	options.journal = stringOption(command.Flags(), "journal", raw.journal, a.v.GetString("ingest.journal"))
	options.stdinName = stringOption(command.Flags(), "stdin-name", raw.stdinName, a.v.GetString("source.stdin_name"))
	options.inputHeaders = stringArrayOption(command.Flags(), "input-header", raw.inputHeaders, a.configStrings("source.http_headers"))
	options.s3Region = stringOption(command.Flags(), "s3-region", raw.s3Region, a.v.GetString("source.s3_region"))
	options.s3Endpoint = stringOption(command.Flags(), "s3-endpoint", raw.s3Endpoint, a.v.GetString("source.s3_endpoint"))
	options.s3PathStyle = boolOption(command.Flags(), "s3-path-style", raw.s3PathStyle, a.v.GetBool("source.s3_path_style"))
	options.ffprobe = a.v.GetString("media.ffprobe")
	options.ffmpeg = a.v.GetString("media.ffmpeg")
	// An explicit name is an unambiguous declaration that the operator intends
	// to pipe media. Keep the configured/default hint non-selecting so a bare
	// ingest still fails immediately instead of waiting on an interactive stdin.
	if len(options.inputs) == 0 && command.Flags().Changed("stdin-name") {
		options.inputs = []string{"-"}
	}

	if strings.TrimSpace(options.profile) == "" {
		return nil, "", errors.New("ingest profile is required; choose one with --profile (run `tamsin profiles` to compare them)")
	}
	profile, err := resolveTreatment(treatmentSettings{
		selection:               options.profile,
		segmentDuration:         options.segmentDuration,
		segmentFormat:           media.SegmentFormat(options.segmentFormat),
		essenceStorage:          media.EssenceStorage(options.essenceStorage),
		ffmpegArgs:              options.ffmpegArgs,
		segmentDurationExplicit: a.ingestOptionExplicit(command.Flags(), "segment-duration", "ingest.segment_duration"),
		segmentFormatExplicit:   a.ingestOptionExplicit(command.Flags(), "segment-format", "ingest.segment_format"),
		essenceStorageExplicit:  a.ingestOptionExplicit(command.Flags(), "essence-storage", "ingest.essence_storage"),
	})
	if err != nil {
		return nil, "", err
	}
	options.profile = profile.Name
	options.profileVersion = profile.Version
	options.segmentDuration = profile.SegmentDuration
	options.segmentFormat = string(profile.SegmentFormat)
	options.essenceStorage = string(profile.EssenceStorage)
	if err := ingest.DryRunMode(options.dryRun).Validate(); err != nil {
		return nil, "", err
	}
	if err := ingest.VerificationMode(options.verify).Validate(); err != nil {
		return nil, "", err
	}

	endpoint := a.v.GetString("endpoint")
	switch {
	case len(options.inputs) == 0 && len(args) == 2:
		options.inputs = []string{args[0]}
		if endpoint == "" {
			endpoint = args[1]
		} else {
			return nil, "", errors.New("endpoint was provided both by configuration/flag and positional argument")
		}
	case len(options.inputs) == 0 && len(args) == 1:
		options.inputs = []string{args[0]}
	case len(options.inputs) > 0 && len(args) == 1:
		if endpoint != "" {
			return nil, "", errors.New("unexpected positional argument when --endpoint is already set")
		}
		endpoint = args[0]
	case len(options.inputs) > 0 && len(args) > 1:
		return nil, "", errors.New("with --input, provide at most one positional TAMS endpoint")
	}
	if len(options.inputs) == 0 {
		return nil, "", errors.New("at least one input is required")
	}
	if options.concurrency <= 0 {
		return nil, "", errors.New("concurrency must be positive")
	}
	if options.maxInputs <= 0 {
		return nil, "", errors.New("max-inputs must be positive")
	}
	stagingBytes, err := parseByteSize(options.stagingByteBudget)
	if err != nil {
		return nil, "", err
	}
	options.stagingBytes = stagingBytes
	return &options, strings.TrimRight(endpoint, "/"), nil
}

// ingestOptionExplicit reports whether an individual media setting came from
// an operator rather than from its built-in default. Named profiles are
// applied first, then only these explicit values override them.
func (a *application) ingestOptionExplicit(flags *pflag.FlagSet, flagName, key string) bool {
	return flags.Changed(flagName) || a.configValueExplicit(key)
}

func (a *application) tamsClient(ctx context.Context, endpoint string, base *http.Transport,
	run *observability.Run) (*tams.Client, auth.Mode, error) {
	// Peer response bodies are untrusted and may reflect credentials or signed
	// values. CLI output never needs them; status and typed TAMS fields carry the
	// safe operational signal while direct library users may opt into bodies.
	return a.tamsClientWithErrorPolicy(ctx, endpoint, base, true, run)
}

// tamsClientWithErrorPolicy keeps authentication and TLS construction shared
// while allowing doctor to suppress every untrusted response body. A doctor
// report is commonly attached to support tickets and must never reproduce a
// peer's response secret.
func (a *application) tamsClientWithErrorPolicy(ctx context.Context, endpoint string, base *http.Transport,
	suppressErrorBody bool, run *observability.Run) (*tams.Client, auth.Mode, error) {
	cleanEndpoint, endpointToken, err := auth.ExtractURLToken(endpoint)
	if err != nil {
		return nil, "", err
	}
	config := a.authenticationConfig(cleanEndpoint, endpointToken)
	transport, mode, err := auth.NewRoundTripper(ctx, config, base, a.stderr)
	if err != nil {
		return nil, "", err
	}
	client, err := tams.New(tams.Config{
		Endpoint: cleanEndpoint, Transport: transport, ExternalTransport: base,
		Timeout: a.v.GetDuration("http.timeout"), TransferTimeout: a.v.GetDuration("http.transfer_timeout"), TransferIdleTimeout: a.v.GetDuration("http.transfer_idle_timeout"),
		Retries: a.v.GetInt("http.retries"), UserAgent: "tamsin/" + version.Version,
		RedactValues:      config.RedactionValues(),
		SuppressErrorBody: suppressErrorBody || mode == auth.ModeOAuthClient || mode == auth.ModeOAuthCode,
		Observability:     run,
	})
	if err != nil {
		return nil, "", err
	}
	return client, mode, nil
}

func observedSourceBackoff(run *observability.Run, retries int) retryablehttp.Backoff {
	return func(minimum, maximum time.Duration, attempt int, response *http.Response) time.Duration {
		delay := retryablehttp.DefaultBackoff(minimum, maximum, attempt, response)
		statusCode := 0
		var cause error
		if response != nil {
			statusCode = response.StatusCode
		} else {
			// retryablehttp does not pass its transport error to Backoff. A fixed
			// sentinel retains the useful class without ever accepting the raw
			// provider/transport message into the logging path.
			cause = errors.New("source transport failure")
		}
		run.Retry(observability.OperationSourceRequest, attempt+2, retries+1, statusCode, cause, delay)
		return delay
	}
}

func (a *application) authenticationConfig(endpoint, endpointToken string) auth.Config {
	config := auth.Config{
		Mode: auth.Mode(a.v.GetString("auth.mode")), Endpoint: endpoint,
		Username: a.v.GetString("auth.username"), Password: a.v.GetString("auth.password"),
		BearerToken: a.v.GetString("auth.token"), URLToken: a.v.GetString("auth.url_token"), TokenURL: a.v.GetString("auth.token_url"),
		AuthURL: a.v.GetString("auth.authorization_url"), ClientID: a.v.GetString("auth.client_id"), ClientSecret: a.v.GetString("auth.client_secret"),
		RedirectURL: a.v.GetString("auth.redirect_url"), Scopes: a.configStrings("auth.scopes"), OAuthCode: a.v.GetString("auth.code"),
		PKCEVerifier: a.v.GetString("auth.pkce_verifier"), Timeout: a.v.GetDuration("http.timeout"),
		AllowInsecureLoopback: a.v.GetBool("auth.allow_insecure_loopback"),
	}
	if config.URLToken == "" {
		config.URLToken = endpointToken
	}
	return config
}

func (a *application) httpTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// These bound connection setup and response headers. Body stalls are a
	// separate state and are handled by the progress watchdog around each media
	// transfer; ResponseHeaderTimeout cannot see a body after headers arrive.
	transport.ResponseHeaderTimeout = 30 * time.Second
	transport.TLSClientConfig = &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: a.v.GetBool("http.insecure_skip_verify"), //nolint:gosec -- explicit operator option
	}
	return transport
}

// reporter decides whether an operator is watching. Progress is drawn only to
// stderr and only when stderr is a terminal, because a redrawing line in a
// captured log is noise and on stdout it would corrupt the result.
func (a *application) reporter() progress.Reporter {
	if strings.EqualFold(a.v.GetString("format"), "json") || a.v.GetBool("quiet") {
		return progress.Discard{}
	}
	return progress.New(a.stderr, progress.Options{
		Mode:      progress.Mode(strings.ToLower(a.v.GetString("progress"))),
		Width:     outputWidth(a.stderr),
		WidthFunc: func() int { return outputWidth(a.stderr) },
	})
}

// loggerFor routes diagnostics through the progress renderer's serializer.
// This lets a live renderer clear and restore its rows without changing the
// operator's configured log level or interleaving records with progress.
func (a *application) loggerFor(reporter progress.Reporter) (*slog.Logger, error) {
	writer := a.stderr
	if line, drawing := reporter.(*progress.Line); drawing {
		writer = line
	}
	return a.loggerTo(writer)
}

func (a *application) loggerTo(writer io.Writer) (*slog.Logger, error) {
	var level slog.Level
	switch strings.ToLower(a.v.GetString("log.level")) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("invalid log level %q", a.v.GetString("log.level"))
	}
	options := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	switch strings.ToLower(a.v.GetString("log.format")) {
	case "text":
		handler = slog.NewTextHandler(writer, options)
	case "json":
		handler = slog.NewJSONHandler(writer, options)
	default:
		return nil, fmt.Errorf("invalid log format %q", a.v.GetString("log.format"))
	}
	return slog.New(handler), nil
}

func (a *application) writeBatch(batch ingest.BatchResult, metrics observability.Snapshot) error {
	switch strings.ToLower(a.v.GetString("format")) {
	case "human":
		return presentation.WriteHuman(a.stdout, batch, metrics, a.humanOptions())
	case "json":
		return errors.New("structured ingest output must be written as an event stream")
	default:
		return fmt.Errorf("invalid result format %q", a.v.GetString("format"))
	}
}

func readFlowMetadata(filename string) (tams.Flow, error) {
	if filename == "" {
		return nil, nil
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open Flow metadata: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (2<<20)+1))
	if err != nil {
		return nil, fmt.Errorf("read Flow metadata: %w", err)
	}
	if len(data) > 2<<20 {
		return nil, errors.New("flow metadata exceeds 2 MiB")
	}
	var metadata tams.Flow
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("decode Flow metadata: %w", err)
	}
	return metadata, nil
}

func parseHeaders(values []string) (http.Header, error) {
	headers := make(http.Header)
	for index, value := range values {
		name, headerValue, ok := strings.Cut(value, ":")
		name = strings.TrimSpace(name)
		headerValue = strings.TrimSpace(headerValue)
		if !ok || name == "" || headerValue == "" {
			return nil, fmt.Errorf("input header %d must use 'Name: value' syntax", index+1)
		}
		headers.Add(name, headerValue)
	}
	return headers, nil
}

func stringOption(flags *pflag.FlagSet, name, flagValue, configured string) string {
	if flags.Changed(name) {
		return flagValue
	}
	return configured
}

func intOption(flags *pflag.FlagSet, name string, flagValue, configured int) int {
	if flags.Changed(name) {
		return flagValue
	}
	return configured
}

func boolOption(flags *pflag.FlagSet, name string, flagValue, configured bool) bool {
	if flags.Changed(name) {
		return flagValue
	}
	return configured
}

func durationOption(flags *pflag.FlagSet, name string, flagValue, configured time.Duration) time.Duration {
	if flags.Changed(name) {
		return flagValue
	}
	return configured
}

func stringArrayOption(flags *pflag.FlagSet, name string, flagValue, configured []string) []string {
	if flags.Changed(name) {
		return flagValue
	}
	return configured
}

func rejectRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}
