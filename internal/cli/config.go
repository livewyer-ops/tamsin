package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/livewyer-ops/tamsin/internal/auth"
	"github.com/livewyer-ops/tamsin/internal/ingest"
	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.yaml.in/yaml/v3"
)

type configKind uint8

const (
	configString configKind = iota
	configBool
	configInt
	configDuration
	configStrings
)

type configDefinition struct {
	key         string
	kind        configKind
	flag        string
	secret      bool
	redactURL   bool
	fileAllowed bool
	// defaultValue replaces the default registered with the flag.
	defaultValue any
}

type configPosition struct {
	line   int
	column int
}

// settings is the complete configuration resolver used by the CLI. It stores
// only validated file values and resolves changed flags, non-empty environment
// variables, file values, then the defaults registered with the flags.
type settings struct {
	definitions map[string]configDefinition
	file        map[string]any
	defaults    map[string]any
	flags       *pflag.FlagSet
}

func newSettings() *settings {
	return &settings{
		definitions: configDefinitionMap(),
		file:        make(map[string]any),
		defaults:    make(map[string]any),
	}
}

// bind resolves s through the root command's persistent flags and records
// every setting's default from the flag that sets it.
func (s *settings) bind(root *cobra.Command) {
	s.flags = root.PersistentFlags()
	for key, definition := range s.definitions {
		if definition.defaultValue != nil {
			s.defaults[key] = definition.defaultValue
			continue
		}
		flags := root.PersistentFlags()
		if flags.Lookup(definition.flag) == nil {
			flags = root.Flags()
		}
		s.defaults[key] = flagValue(flags, definition)
	}
}

// forCommand returns s as seen by command, whose changed flags take
// precedence over every other source.
func (s *settings) forCommand(command *cobra.Command) *settings {
	view := *s
	view.flags = command.Flags()
	return &view
}

func (s *settings) flagChanged(key string) bool {
	if s.flags == nil {
		return false
	}
	flag := s.flags.Lookup(s.definitions[key].flag)
	return flag != nil && flag.Changed
}

// explicit reports whether key was set rather than left at its default.
func (s *settings) explicit(key string) bool {
	if _, ok := s.file[key]; ok || s.flagChanged(key) {
		return true
	}
	value, exists := os.LookupEnv(configEnvironmentName(key))
	return exists && value != ""
}

func (s *settings) value(key string) any {
	definition := s.definitions[key]
	if s.flagChanged(key) {
		return flagValue(s.flags, definition)
	}
	if raw, ok := os.LookupEnv(configEnvironmentName(key)); ok && raw != "" {
		if value, err := environmentValue(configEnvironmentName(key), raw, definition.kind); err == nil {
			return value
		}
	}
	if value, ok := s.file[key]; ok {
		return value
	}
	return s.defaults[key]
}

func flagValue(flags *pflag.FlagSet, definition configDefinition) any {
	switch definition.kind {
	case configString:
		value, _ := flags.GetString(definition.flag)
		return value
	case configBool:
		value, _ := flags.GetBool(definition.flag)
		return value
	case configInt:
		value, _ := flags.GetInt(definition.flag)
		return value
	case configDuration:
		value, _ := flags.GetDuration(definition.flag)
		return value
	case configStrings:
		// --scope is comma-separated; the other lists repeat.
		if value, err := flags.GetStringArray(definition.flag); err == nil {
			return value
		}
		value, _ := flags.GetStringSlice(definition.flag)
		return value
	default:
		return nil
	}
}

func (s *settings) GetString(key string) string {
	value, _ := s.value(key).(string)
	return value
}

func (s *settings) GetBool(key string) bool {
	value, _ := s.value(key).(bool)
	return value
}

func (s *settings) GetInt(key string) int {
	value, _ := s.value(key).(int)
	return value
}

func (s *settings) GetDuration(key string) time.Duration {
	value, _ := s.value(key).(time.Duration)
	return value
}

func (s *settings) GetStringSlice(key string) []string {
	values, _ := s.value(key).([]string)
	return append([]string(nil), values...)
}

// configDefinitions is the single allow-list for file and TAMSIN_* settings.
func configDefinitions() []configDefinition {
	return []configDefinition{
		{key: "auth.allow_insecure_loopback", kind: configBool, flag: "allow-insecure-auth-loopback", fileAllowed: true},
		{key: "auth.client_id", kind: configString, flag: "client-id", fileAllowed: true},
		{key: "auth.client_secret", kind: configString, flag: "client-secret", secret: true, fileAllowed: true},
		{key: "auth.code", kind: configString, flag: "oauth-code", secret: true, fileAllowed: true},
		{key: "auth.mode", kind: configString, flag: "auth", fileAllowed: true},
		{key: "auth.password", kind: configString, flag: "password", secret: true, fileAllowed: true},
		{key: "auth.pkce_verifier", kind: configString, flag: "pkce-verifier", secret: true, fileAllowed: true},
		{key: "auth.redirect_url", kind: configString, flag: "redirect-url", redactURL: true, fileAllowed: true},
		{key: "auth.scopes", kind: configStrings, flag: "scope", fileAllowed: true},
		{key: "auth.token", kind: configString, flag: "token", secret: true, fileAllowed: true},
		{key: "auth.token_url", kind: configString, flag: "token-url", redactURL: true, fileAllowed: true},
		{key: "auth.url_token", kind: configString, flag: "url-token", secret: true, fileAllowed: true},
		{key: "auth.username", kind: configString, flag: "username", fileAllowed: true},
		{key: "color", kind: configString, flag: "color", fileAllowed: true},
		// config selects a file before a file can be read, so accepting it inside
		// that file would be misleading. It remains a supported flag/env key.
		{key: "config", kind: configString, flag: "config"},
		{key: "endpoint", kind: configString, flag: "endpoint", redactURL: true, fileAllowed: true},
		{key: "format", kind: configString, flag: "format", fileAllowed: true},
		{key: "http.insecure_skip_verify", kind: configBool, flag: "insecure-skip-verify", fileAllowed: true},
		{key: "http.retries", kind: configInt, flag: "retries", fileAllowed: true},
		{key: "http.timeout", kind: configDuration, flag: "timeout", fileAllowed: true},
		{key: "http.transfer_idle_timeout", kind: configDuration, flag: "transfer-idle-timeout", fileAllowed: true},
		{key: "http.transfer_timeout", kind: configDuration, flag: "transfer-timeout", fileAllowed: true},
		{key: "ingest.concurrency", kind: configInt, flag: "concurrency", defaultValue: min(runtime.GOMAXPROCS(0), 8), fileAllowed: true},
		{key: "ingest.dry_run", kind: configString, flag: "dry-run", fileAllowed: true},
		{key: "ingest.essence_storage", kind: configString, flag: "essence-storage", fileAllowed: true},
		{key: "ingest.flow_id", kind: configString, flag: "flow-id", fileAllowed: true},
		{key: "ingest.flow_metadata", kind: configString, flag: "flow-metadata", fileAllowed: true},
		{key: "ingest.input_mode", kind: configString, flag: "input-mode", fileAllowed: true},
		{key: "ingest.max_inputs", kind: configInt, flag: "max-inputs", fileAllowed: true},
		{key: "ingest.probe_concurrency", kind: configInt, flag: "probe-concurrency", fileAllowed: true},
		{key: "ingest.profile", kind: configString, flag: "profile", fileAllowed: true},
		{key: "ingest.segment_duration", kind: configDuration, flag: "segment-duration", fileAllowed: true},
		{key: "ingest.segment_format", kind: configString, flag: "segment-format", fileAllowed: true},
		{key: "ingest.source_id", kind: configString, flag: "source-id", fileAllowed: true},
		{key: "ingest.staging_byte_budget", kind: configString, flag: "staging-byte-budget", fileAllowed: true},
		{key: "ingest.start", kind: configString, flag: "start", fileAllowed: true},
		{key: "ingest.storage_id", kind: configString, flag: "storage-id", fileAllowed: true},
		{key: "ingest.tams_flow_profiles", kind: configStrings, flag: "tams-flow-profile", fileAllowed: true},
		{key: "ingest.temp_directory", kind: configString, flag: "temp-dir", fileAllowed: true},
		{key: "ingest.transfers", kind: configInt, flag: "transfers", fileAllowed: true},
		{key: "ingest.verify", kind: configString, flag: "verify", fileAllowed: true},
		{key: "input", kind: configStrings, flag: "input", fileAllowed: true},
		{key: "log.format", kind: configString, flag: "log-format", fileAllowed: true},
		{key: "log.level", kind: configString, flag: "log-level", fileAllowed: true},
		{key: "media.ffmpeg", kind: configString, flag: "ffmpeg", fileAllowed: true},
		// FFmpeg arguments may carry headers, cookies, signed URLs, or provider
		// options whose credential-bearing positions Tamsin cannot predict.
		{key: "media.ffmpeg_args", kind: configStrings, flag: "ffmpeg-arg", secret: true, fileAllowed: true},
		{key: "media.ffprobe", kind: configString, flag: "ffprobe", fileAllowed: true},
		{key: "progress", kind: configString, flag: "progress", fileAllowed: true},
		{key: "quiet", kind: configBool, flag: "quiet", fileAllowed: true},
		// Headers can contain bearer credentials, cookies, or signed values whose
		// names Tamsin cannot predict. Treat the whole setting as secret.
		{key: "source.http_headers", kind: configStrings, flag: "input-header", secret: true, fileAllowed: true},
		{key: "source.s3_endpoint", kind: configString, flag: "s3-endpoint", redactURL: true, fileAllowed: true},
		{key: "source.s3_path_style", kind: configBool, flag: "s3-path-style", fileAllowed: true},
		{key: "source.s3_region", kind: configString, flag: "s3-region", fileAllowed: true},
		{key: "source.stdin_name", kind: configString, flag: "stdin-name", fileAllowed: true},
		{key: "verbose", kind: configBool, flag: "verbose", fileAllowed: true},
	}
}

func configDefinitionMap() map[string]configDefinition {
	definitions := configDefinitions()
	result := make(map[string]configDefinition, len(definitions))
	for _, definition := range definitions {
		result[definition.key] = definition
	}
	return result
}

func configEnvironmentName(key string) string {
	replacer := strings.NewReplacer(".", "_", "-", "_")
	return "TAMSIN_" + strings.ToUpper(replacer.Replace(key))
}

func decodeConfigFile(data []byte) (map[string]any, error) {
	values := make(map[string]any)
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		if errors.Is(err, io.EOF) {
			return values, nil
		}
		return nil, fmt.Errorf("decode YAML: %w", err)
	}
	if len(document.Content) > 0 {
		root := dereferenceYAMLNode(document.Content[0])
		if root.Kind != yaml.MappingNode {
			return nil, fmt.Errorf("configuration document must be a mapping, got %s", yamlKindName(root))
		}
		if err := decodeConfigMapping(root, "", configDefinitionMap(), make(map[string]configPosition), values); err != nil {
			return nil, err
		}
	}

	var trailing yaml.Node
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, fmt.Errorf("decode trailing YAML document: %w", err)
		}
		return nil, errors.New("configuration must contain exactly one YAML document")
	}
	return values, nil
}

func decodeConfigMapping(node *yaml.Node, prefix string, definitions map[string]configDefinition, seen map[string]configPosition, values map[string]any) error {
	for index := 0; index < len(node.Content); index += 2 {
		keyNode := dereferenceYAMLNode(node.Content[index])
		valueNode := dereferenceYAMLNode(node.Content[index+1])
		if keyNode.Kind != yaml.ScalarNode || keyNode.Tag != "!!str" {
			return fmt.Errorf("configuration key at line %d, column %d must be a string", keyNode.Line, keyNode.Column)
		}
		if keyNode.Value == "<<" {
			return fmt.Errorf("YAML merge keys are not supported at line %d, column %d; spell out the configuration keys", keyNode.Line, keyNode.Column)
		}

		key := strings.ToLower(keyNode.Value)
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		if previous, duplicate := seen[path]; duplicate {
			return fmt.Errorf("configuration key %q at line %d, column %d duplicates line %d, column %d", path, keyNode.Line, keyNode.Column, previous.line, previous.column)
		}
		seen[path] = configPosition{line: keyNode.Line, column: keyNode.Column}

		if definition, exists := definitions[path]; exists {
			if !definition.fileAllowed {
				return fmt.Errorf("configuration key %q cannot be set inside a file; use --%s or %s", path, definition.flag, configEnvironmentName(path))
			}
			if err := validateYAMLValue(path, valueNode, definition.kind); err != nil {
				return err
			}
			var value any
			if err := valueNode.Decode(&value); err != nil {
				return fmt.Errorf("decode configuration key %q: %w", path, err)
			}
			switch definition.kind {
			case configDuration:
				// Validation permits a duration string or unitless zero only.
				value = time.Duration(0)
				if valueNode.Tag == "!!str" {
					value, _ = time.ParseDuration(valueNode.Value)
				}
			case configStrings:
				var items []string
				if err := valueNode.Decode(&items); err != nil {
					return err
				}
				value = items
			}
			values[path] = value
			continue
		}

		if hasConfigChildren(path, definitions) {
			if valueNode.Kind != yaml.MappingNode {
				return fmt.Errorf("configuration key %q at line %d, column %d must be a mapping, got %s", path, valueNode.Line, valueNode.Column, yamlKindName(valueNode))
			}
			if err := decodeConfigMapping(valueNode, path, definitions, seen, values); err != nil {
				return err
			}
			continue
		}

		return fmt.Errorf("unknown configuration key %q at line %d, column %d", path, keyNode.Line, keyNode.Column)
	}
	return nil
}

func validateYAMLValue(path string, node *yaml.Node, kind configKind) error {
	want := configKindName(kind)
	switch kind {
	case configString:
		if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
			return configTypeError(path, node, want)
		}
	case configBool:
		if node.Kind != yaml.ScalarNode || node.Tag != "!!bool" {
			return configTypeError(path, node, want)
		}
		if _, err := strconv.ParseBool(node.Value); err != nil {
			return fmt.Errorf("configuration key %q at line %d, column %d is not a valid boolean", path, node.Line, node.Column)
		}
	case configInt:
		if node.Kind != yaml.ScalarNode || node.Tag != "!!int" {
			return configTypeError(path, node, want)
		}
		var value int
		if err := node.Decode(&value); err != nil {
			return fmt.Errorf("configuration key %q at line %d, column %d is not a valid integer: %w", path, node.Line, node.Column, err)
		}
	case configDuration:
		if node.Kind == yaml.ScalarNode && node.Tag == "!!int" {
			var value int
			if err := node.Decode(&value); err == nil && value == 0 {
				return nil
			}
			return fmt.Errorf("configuration key %q at line %d, column %d must use units (for example 30s); only unitless 0 is allowed", path, node.Line, node.Column)
		}
		if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
			return configTypeError(path, node, want)
		}
		if _, err := time.ParseDuration(node.Value); err != nil {
			return fmt.Errorf("configuration key %q at line %d, column %d is not a valid duration: %w", path, node.Line, node.Column, err)
		}
	case configStrings:
		if node.Kind != yaml.SequenceNode {
			return configTypeError(path, node, want)
		}
		for index, child := range node.Content {
			child = dereferenceYAMLNode(child)
			if child.Kind != yaml.ScalarNode || child.Tag != "!!str" {
				return fmt.Errorf("configuration key %q element %d at line %d, column %d must be a string, got %s", path, index, child.Line, child.Column, yamlKindName(child))
			}
		}
	default:
		panic("unknown configuration kind")
	}
	return nil
}

func configTypeError(path string, node *yaml.Node, want string) error {
	return fmt.Errorf("configuration key %q at line %d, column %d must be %s, got %s", path, node.Line, node.Column, want, yamlKindName(node))
}

func configKindName(kind configKind) string {
	switch kind {
	case configString:
		return "a string"
	case configBool:
		return "a boolean"
	case configInt:
		return "an integer"
	case configDuration:
		return "a duration string or integer 0"
	case configStrings:
		return "a list of strings"
	default:
		return "the expected type"
	}
}

func yamlKindName(node *yaml.Node) string {
	switch node.Kind {
	case yaml.MappingNode:
		return "mapping"
	case yaml.SequenceNode:
		return "list"
	case yaml.ScalarNode:
		switch node.Tag {
		case "!!str":
			return "string"
		case "!!bool":
			return "boolean"
		case "!!int":
			return "integer"
		case "!!float":
			return "number"
		case "!!null":
			return "null"
		default:
			return "scalar"
		}
	case yaml.AliasNode:
		return "alias"
	default:
		return "value"
	}
}

func dereferenceYAMLNode(node *yaml.Node) *yaml.Node {
	for node != nil && node.Kind == yaml.AliasNode && node.Alias != nil {
		node = node.Alias
	}
	return node
}

func hasConfigChildren(path string, definitions map[string]configDefinition) bool {
	prefix := path + "."
	for key := range definitions {
		if strings.HasPrefix(key, prefix) && definitions[key].fileAllowed {
			return true
		}
	}
	return false
}

func (a *application) validateConfigEnvironment() error {
	definitions := configDefinitionMap()
	known := make(map[string]configDefinition, len(definitions))
	for _, definition := range definitions {
		name := configEnvironmentName(definition.key)
		known[name] = definition
	}

	environment := os.Environ()
	sort.Strings(environment)
	for _, entry := range environment {
		name, value, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(name, "TAMSIN_") {
			continue
		}
		definition, exists := known[name]
		if !exists {
			return fmt.Errorf("unsupported environment variable %q", name)
		}
		// Empty known variables are unset. Unknown empty names still fail because
		// mounted configuration may become non-empty later.
		if value == "" {
			continue
		}
		if _, err := environmentValue(name, value, definition.kind); err != nil {
			return err
		}
	}
	return nil
}

func environmentValue(name, value string, kind configKind) (any, error) {
	switch kind {
	case configBool:
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("environment variable %s must be a boolean", name)
		}
		return parsed, nil
	case configInt:
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return nil, fmt.Errorf("environment variable %s must be an integer", name)
		}
		return parsed, nil
	case configDuration:
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("environment variable %s must be a duration", name)
		}
		return parsed, nil
	case configStrings:
		return decodeEnvironmentStrings(name, value)
	default:
		return value, nil
	}
}

// decodeEnvironmentStrings accepts whitespace-separated values and JSON arrays
// for entries that contain spaces or commas.
func decodeEnvironmentStrings(name, value string) ([]string, error) {
	trimmed := strings.TrimSpace(value)
	if !strings.HasPrefix(trimmed, "[") {
		return strings.Fields(value), nil
	}
	var values []string
	if err := json.Unmarshal([]byte(trimmed), &values); err != nil || values == nil {
		return nil, fmt.Errorf("environment variable %s must be a JSON array of strings", name)
	}
	return values, nil
}

// validate checks every setting the CLI resolves before a command runs.
func (s *settings) validate() error {
	if err := s.validateGlobal(); err != nil {
		return err
	}
	if concurrency := s.GetInt("ingest.concurrency"); concurrency <= 0 || concurrency > 256 {
		return errors.New("concurrency must be between 1 and 256")
	}
	if transfers := s.GetInt("ingest.transfers"); transfers < 0 || transfers > 256 {
		return errors.New("transfers must be between 0 and 256")
	}
	if concurrency := s.GetInt("ingest.probe_concurrency"); concurrency < 0 || concurrency > 256 {
		return errors.New("probe-concurrency must be between 0 and 256")
	}
	if maximum := s.GetInt("ingest.max_inputs"); maximum <= 0 {
		return errors.New("max-inputs must be positive")
	}
	if err := ingest.DryRunMode(s.GetString("ingest.dry_run")).Validate(); err != nil {
		return err
	}
	if err := ingest.InputMode(s.GetString("ingest.input_mode")).Validate(); err != nil {
		return err
	}
	if err := ingest.VerificationMode(s.GetString("ingest.verify")).Validate(); err != nil {
		return err
	}
	if _, err := parseByteSize(s.GetString("ingest.staging_byte_budget")); err != nil {
		return err
	}
	if duration := s.GetDuration("ingest.segment_duration"); duration < 0 {
		return errors.New("segment duration cannot be negative")
	}
	if err := media.SegmentFormat(s.GetString("ingest.segment_format")).Validate(); err != nil {
		return err
	}
	if err := media.EssenceStorage(s.GetString("ingest.essence_storage")).Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(s.GetString("ingest.profile")) != "" {
		if _, err := s.treatment(); err != nil {
			return err
		}
	}
	if _, err := media.ParseTimestamp(s.GetString("ingest.start")); err != nil {
		return err
	}
	for _, identifier := range []struct{ key, label string }{
		{key: "ingest.flow_id", label: "flow ID"},
		{key: "ingest.source_id", label: "source ID"},
		{key: "ingest.storage_id", label: "storage ID"},
	} {
		value := s.GetString(identifier.key)
		if value == "" {
			continue
		}
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("%s must be a UUID: %w", identifier.label, err)
		}
	}
	if _, err := parseHeaders(s.GetStringSlice("source.http_headers")); err != nil {
		return err
	}
	return s.validateURLs()
}

func (s *settings) validateGlobal() error {
	switch strings.ToLower(s.GetString("format")) {
	case "human", "json":
	default:
		return fmt.Errorf("invalid result format %q", s.GetString("format"))
	}
	switch strings.ToLower(s.GetString("color")) {
	case "auto", "always", "never":
	default:
		return fmt.Errorf("invalid color mode %q", s.GetString("color"))
	}
	switch strings.ToLower(s.GetString("log.format")) {
	case "text", "json":
	default:
		return fmt.Errorf("invalid log format %q", s.GetString("log.format"))
	}
	switch strings.ToLower(s.GetString("log.level")) {
	case "debug", "info", "warn", "warning", "error":
	default:
		return fmt.Errorf("invalid log level %q", s.GetString("log.level"))
	}
	switch strings.ToLower(s.GetString("progress")) {
	case "auto", "plain", "none":
	default:
		return fmt.Errorf("invalid progress mode %q", s.GetString("progress"))
	}
	switch auth.Mode(s.GetString("auth.mode")) {
	case auth.ModeAuto, auth.ModeNone, auth.ModeBasic, auth.ModeBearer, auth.ModeURLToken, auth.ModeOAuthClient, auth.ModeOAuthCode:
	default:
		return fmt.Errorf("invalid authentication mode %q", s.GetString("auth.mode"))
	}
	if timeout := s.GetDuration("http.timeout"); timeout <= 0 {
		return errors.New("HTTP timeout must be positive")
	}
	if timeout := s.GetDuration("http.transfer_timeout"); timeout < 0 {
		return errors.New("transfer timeout cannot be negative")
	}
	if timeout := s.GetDuration("http.transfer_idle_timeout"); timeout <= 0 {
		return errors.New("transfer idle timeout must be positive")
	}
	if retries := s.GetInt("http.retries"); retries < 0 || retries > 20 {
		return errors.New("HTTP retries must be between 0 and 20")
	}
	return nil
}

// treatment resolves the selected profile with every explicitly set media
// override applied, independently of any input.
func (s *settings) treatment() (ingest.Profile, error) {
	ffmpegArgs := s.GetStringSlice("media.ffmpeg_args")
	overrides := ingest.ProfileOverrides{FFmpegArgs: len(ffmpegArgs) > 0}
	if s.explicit("ingest.segment_duration") {
		segmentDuration := s.GetDuration("ingest.segment_duration")
		overrides.SegmentDuration = &segmentDuration
	}
	if s.explicit("ingest.segment_format") {
		segmentFormat := media.SegmentFormat(s.GetString("ingest.segment_format"))
		overrides.SegmentFormat = &segmentFormat
	}
	if s.explicit("ingest.essence_storage") {
		essenceStorage := media.EssenceStorage(s.GetString("ingest.essence_storage"))
		overrides.EssenceStorage = &essenceStorage
	}
	profile, err := ingest.ResolveProfile(s.GetString("ingest.profile"), overrides)
	if err != nil {
		return ingest.Profile{}, err
	}
	if err := ingest.ValidateTreatment(profile, ffmpegArgs); err != nil {
		return ingest.Profile{}, err
	}
	return profile, nil
}

func (s *settings) validateURLs() error {
	if endpoint := s.GetString("endpoint"); endpoint != "" {
		if _, err := validateTAMSEndpoint(endpoint); err != nil {
			return err
		}
	}
	for _, configuredURL := range []struct {
		label string
		value string
	}{
		{label: "OAuth token URL", value: s.GetString("auth.token_url")},
		{label: "OAuth redirect URL", value: s.GetString("auth.redirect_url")},
		{label: "S3 endpoint", value: s.GetString("source.s3_endpoint")},
	} {
		if configuredURL.value == "" {
			continue
		}
		if err := validateAbsoluteHTTPURL(configuredURL.label, configuredURL.value); err != nil {
			return err
		}
	}
	return nil
}

func validateTAMSEndpoint(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" {
		return "", errors.New("TAMS endpoint must be a valid absolute HTTP(S) URL")
	}
	if parsed.User != nil {
		return "", errors.New("TAMS endpoint must not contain userinfo; configure basic authentication explicitly")
	}
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return "", errors.New("TAMS endpoint has an invalid query string")
	}
	token := query.Get("access_token")
	query.Del("access_token")
	if len(query) != 0 || parsed.Fragment != "" {
		return "", errors.New("TAMS endpoint must not contain a query or fragment other than access_token")
	}
	return token, nil
}

func validateAbsoluteHTTPURL(label, raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("%s must be a valid absolute HTTP(S) URL", label)
	}
	if parsed.User != nil {
		return fmt.Errorf("%s must not contain userinfo", label)
	}
	return nil
}
