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
	"github.com/livewyer-ops/tamsin/internal/netio"
	"github.com/livewyer-ops/tamsin/internal/source"
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
	key          string
	kind         configKind
	defaultValue any
	flag         string
	secret       bool
	redactURL    bool
	fileAllowed  bool
}

type configPosition struct {
	line   int
	column int
}

// settings is the complete configuration resolver used by the CLI. It stores
// only validated file values and resolves changed persistent flags,
// non-empty environment variables, file values, then defaults.
type settings struct {
	definitions map[string]configDefinition
	file        map[string]any
	flags       *pflag.FlagSet
}

func newSettings() *settings {
	return &settings{
		definitions: configDefinitionMap(),
		file:        make(map[string]any),
	}
}

func (s *settings) InConfig(key string) bool {
	_, ok := s.file[key]
	return ok
}

func (s *settings) value(key string) any {
	definition, known := s.definitions[key]
	if known && definition.flag != "" && s.flags != nil {
		if flag := s.flags.Lookup(definition.flag); flag != nil && flag.Changed {
			return flagValue(s.flags, definition)
		}
	}
	if raw, ok := os.LookupEnv(configEnvironmentName(key)); ok && raw != "" {
		if value, err := environmentValue(configEnvironmentName(key), raw, definition.kind); err == nil {
			return value
		}
	}
	if value, ok := s.file[key]; ok {
		return value
	}
	return definition.defaultValue
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
		{key: "auth.allow_insecure_loopback", kind: configBool, defaultValue: false, flag: "allow-insecure-auth-loopback", fileAllowed: true},
		{key: "auth.client_id", kind: configString, defaultValue: "", flag: "client-id", fileAllowed: true},
		{key: "auth.client_secret", kind: configString, defaultValue: "", flag: "client-secret", secret: true, fileAllowed: true},
		{key: "auth.code", kind: configString, defaultValue: "", flag: "oauth-code", secret: true, fileAllowed: true},
		{key: "auth.mode", kind: configString, defaultValue: string(auth.ModeAuto), flag: "auth", fileAllowed: true},
		{key: "auth.password", kind: configString, defaultValue: "", flag: "password", secret: true, fileAllowed: true},
		{key: "auth.pkce_verifier", kind: configString, defaultValue: "", flag: "pkce-verifier", secret: true, fileAllowed: true},
		{key: "auth.redirect_url", kind: configString, defaultValue: "http://127.0.0.1:53682/callback", flag: "redirect-url", redactURL: true, fileAllowed: true},
		{key: "auth.scopes", kind: configStrings, defaultValue: []string{}, flag: "scope", fileAllowed: true},
		{key: "auth.token", kind: configString, defaultValue: "", flag: "token", secret: true, fileAllowed: true},
		{key: "auth.token_url", kind: configString, defaultValue: "", flag: "token-url", redactURL: true, fileAllowed: true},
		{key: "auth.url_token", kind: configString, defaultValue: "", flag: "url-token", secret: true, fileAllowed: true},
		{key: "auth.username", kind: configString, defaultValue: "", flag: "username", fileAllowed: true},
		{key: "color", kind: configString, defaultValue: "auto", flag: "color", fileAllowed: true},
		// config selects a file before a file can be read, so accepting it inside
		// that file would be misleading. It remains a supported flag/env key.
		{key: "config", kind: configString, defaultValue: "", flag: "config"},
		{key: "endpoint", kind: configString, defaultValue: "", flag: "endpoint", redactURL: true, fileAllowed: true},
		{key: "format", kind: configString, defaultValue: "human", flag: "format", fileAllowed: true},
		{key: "http.insecure_skip_verify", kind: configBool, defaultValue: false, flag: "insecure-skip-verify", fileAllowed: true},
		{key: "http.retries", kind: configInt, defaultValue: 3, flag: "retries", fileAllowed: true},
		{key: "http.timeout", kind: configDuration, defaultValue: 30 * time.Second, flag: "timeout", fileAllowed: true},
		{key: "http.transfer_idle_timeout", kind: configDuration, defaultValue: netio.DefaultIdleTimeout, flag: "transfer-idle-timeout", fileAllowed: true},
		{key: "http.transfer_timeout", kind: configDuration, defaultValue: time.Duration(0), flag: "transfer-timeout", fileAllowed: true},
		{key: "ingest.concurrency", kind: configInt, defaultValue: min(runtime.GOMAXPROCS(0), 8), fileAllowed: true},
		{key: "ingest.dry_run", kind: configString, defaultValue: string(ingest.DryRunOff), fileAllowed: true},
		{key: "ingest.essence_storage", kind: configString, defaultValue: string(media.EssenceStorageIndependent), fileAllowed: true},
		{key: "ingest.flow_id", kind: configString, defaultValue: "", fileAllowed: true},
		{key: "ingest.flow_metadata", kind: configString, defaultValue: "", fileAllowed: true},
		{key: "ingest.max_inputs", kind: configInt, defaultValue: source.DefaultMaxInputs, fileAllowed: true},
		{key: "ingest.probe_concurrency", kind: configInt, defaultValue: 2, fileAllowed: true},
		{key: "ingest.profile", kind: configString, defaultValue: "", fileAllowed: true},
		{key: "ingest.segment_duration", kind: configDuration, defaultValue: defaultSegmentDuration, fileAllowed: true},
		{key: "ingest.segment_format", kind: configString, defaultValue: string(media.SegmentFormatSource), fileAllowed: true},
		{key: "ingest.source_id", kind: configString, defaultValue: "", fileAllowed: true},
		{key: "ingest.staging_byte_budget", kind: configString, defaultValue: "auto", fileAllowed: true},
		{key: "ingest.start", kind: configString, defaultValue: "0:0", fileAllowed: true},
		{key: "ingest.storage_id", kind: configString, defaultValue: "", fileAllowed: true},
		{key: "ingest.tams_flow_profiles", kind: configStrings, defaultValue: []string{}, fileAllowed: true},
		{key: "ingest.temp_directory", kind: configString, defaultValue: "", fileAllowed: true},
		{key: "ingest.transfers", kind: configInt, defaultValue: 0, fileAllowed: true},
		{key: "ingest.verify", kind: configString, defaultValue: string(ingest.VerificationAuto), fileAllowed: true},
		{key: "input", kind: configStrings, defaultValue: []string{}, fileAllowed: true},
		{key: "log.format", kind: configString, defaultValue: "text", flag: "log-format", fileAllowed: true},
		{key: "log.level", kind: configString, defaultValue: "info", flag: "log-level", fileAllowed: true},
		{key: "media.ffmpeg", kind: configString, defaultValue: "ffmpeg", flag: "ffmpeg", fileAllowed: true},
		// FFmpeg arguments may carry headers, cookies, signed URLs, or provider
		// options whose credential-bearing positions Tamsin cannot predict.
		{key: "media.ffmpeg_args", kind: configStrings, defaultValue: []string{}, secret: true, fileAllowed: true},
		{key: "media.ffprobe", kind: configString, defaultValue: "ffprobe", flag: "ffprobe", fileAllowed: true},
		{key: "progress", kind: configString, defaultValue: "auto", flag: "progress", fileAllowed: true},
		{key: "quiet", kind: configBool, defaultValue: false, flag: "quiet", fileAllowed: true},
		// Headers can contain bearer credentials, cookies, or signed values whose
		// names Tamsin cannot predict. Treat the whole setting as secret.
		{key: "source.http_headers", kind: configStrings, defaultValue: []string{}, secret: true, fileAllowed: true},
		{key: "source.s3_endpoint", kind: configString, defaultValue: "", redactURL: true, fileAllowed: true},
		{key: "source.s3_path_style", kind: configBool, defaultValue: false, fileAllowed: true},
		{key: "source.s3_region", kind: configString, defaultValue: "", fileAllowed: true},
		{key: "source.stdin_name", kind: configString, defaultValue: "stdin.bin", fileAllowed: true},
		{key: "verbose", kind: configBool, defaultValue: false, flag: "verbose", fileAllowed: true},
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

func (a *application) configureDefaults() {
	if a.v == nil {
		a.v = newSettings()
	}
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
		if err := validateEnvironmentValue(name, value, definition.kind); err != nil {
			return err
		}
	}
	return nil
}

func validateEnvironmentValue(name, value string, kind configKind) error {
	_, err := environmentValue(name, value, kind)
	return err
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

func (a *application) configStrings(key string) []string {
	return a.v.GetStringSlice(key)
}

func commandFlagChanged(command *cobra.Command, name string) bool {
	if command == nil || name == "" {
		return false
	}
	flag := command.Flags().Lookup(name)
	return flag != nil && flag.Changed
}

func (a *application) configString(command *cobra.Command, flag, key string) string {
	if commandFlagChanged(command, flag) {
		value, err := command.Flags().GetString(flag)
		if err != nil {
			panic(err)
		}
		return value
	}
	return a.v.GetString(key)
}

func (a *application) configInt(command *cobra.Command, flag, key string) int {
	if commandFlagChanged(command, flag) {
		value, err := command.Flags().GetInt(flag)
		if err != nil {
			panic(err)
		}
		return value
	}
	return a.v.GetInt(key)
}

func (a *application) configDuration(command *cobra.Command, flag, key string) time.Duration {
	if commandFlagChanged(command, flag) {
		value, err := command.Flags().GetDuration(flag)
		if err != nil {
			panic(err)
		}
		return value
	}
	return a.v.GetDuration(key)
}

func (a *application) configStringArray(command *cobra.Command, flag, key string) []string {
	if commandFlagChanged(command, flag) {
		value, err := command.Flags().GetStringArray(flag)
		if err != nil {
			panic(err)
		}
		return value
	}
	return a.configStrings(key)
}

func (a *application) validateConfigValues(command *cobra.Command) error {
	if err := a.validateGlobalConfig(); err != nil {
		return err
	}
	if concurrency := a.configInt(command, "concurrency", "ingest.concurrency"); concurrency <= 0 || concurrency > 256 {
		return errors.New("concurrency must be between 1 and 256")
	}
	if transfers := a.configInt(command, "transfers", "ingest.transfers"); transfers < 0 || transfers > 256 {
		return errors.New("transfers must be between 0 and 256")
	}
	if concurrency := a.configInt(command, "probe-concurrency", "ingest.probe_concurrency"); concurrency < 0 || concurrency > 256 {
		return errors.New("probe-concurrency must be between 0 and 256")
	}
	if maximum := a.configInt(command, "max-inputs", "ingest.max_inputs"); maximum <= 0 {
		return errors.New("max-inputs must be positive")
	}
	if err := ingest.DryRunMode(a.configString(command, "dry-run", "ingest.dry_run")).Validate(); err != nil {
		return err
	}
	if err := ingest.VerificationMode(a.configString(command, "verify", "ingest.verify")).Validate(); err != nil {
		return err
	}
	if _, err := parseByteSize(a.configString(command, "staging-byte-budget", "ingest.staging_byte_budget")); err != nil {
		return err
	}
	if duration := a.configDuration(command, "segment-duration", "ingest.segment_duration"); duration < 0 {
		return errors.New("segment duration cannot be negative")
	}
	if err := media.SegmentFormat(a.configString(command, "segment-format", "ingest.segment_format")).Validate(); err != nil {
		return err
	}
	if err := media.EssenceStorage(a.configString(command, "essence-storage", "ingest.essence_storage")).Validate(); err != nil {
		return err
	}
	if selection := a.configString(command, "profile", "ingest.profile"); strings.TrimSpace(selection) != "" {
		if _, err := a.resolvedConfigProfile(command); err != nil {
			return err
		}
	}
	if _, err := media.ParseTimestamp(a.configString(command, "start", "ingest.start")); err != nil {
		return err
	}
	for _, identifier := range []struct {
		key, flag, label string
	}{
		{key: "ingest.flow_id", flag: "flow-id", label: "flow ID"},
		{key: "ingest.source_id", flag: "source-id", label: "source ID"},
		{key: "ingest.storage_id", flag: "storage-id", label: "storage ID"},
	} {
		value := a.configString(command, identifier.flag, identifier.key)
		if value == "" {
			continue
		}
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("%s must be a UUID: %w", identifier.label, err)
		}
	}
	if _, err := parseHeaders(a.configStringArray(command, "input-header", "source.http_headers")); err != nil {
		return err
	}
	if err := a.validateConfiguredURLs(command); err != nil {
		return err
	}
	return nil
}

type treatmentSettings struct {
	selection               string
	segmentDuration         time.Duration
	segmentFormat           media.SegmentFormat
	essenceStorage          media.EssenceStorage
	ffmpegArgs              []string
	segmentDurationExplicit bool
	segmentFormatExplicit   bool
	essenceStorageExplicit  bool
}

// resolveTreatment is the input-independent media-policy validator shared by
// doctor and ingest.
func resolveTreatment(settings treatmentSettings) (ingest.Profile, error) {
	overrides := ingest.ProfileOverrides{FFmpegArgs: len(settings.ffmpegArgs) > 0}
	if settings.segmentDurationExplicit {
		overrides.SegmentDuration = &settings.segmentDuration
	}
	if settings.segmentFormatExplicit {
		overrides.SegmentFormat = &settings.segmentFormat
	}
	if settings.essenceStorageExplicit {
		overrides.EssenceStorage = &settings.essenceStorage
	}
	profile, err := ingest.ResolveProfile(settings.selection, overrides)
	if err != nil {
		return ingest.Profile{}, err
	}
	if err := ingest.ValidateTreatment(profile, settings.ffmpegArgs); err != nil {
		return ingest.Profile{}, err
	}
	return profile, nil
}

// resolvedConfigProfile applies the active command's local overrides before
// file/environment values. Commands without treatment flags naturally inspect
// the effective configured treatment.
func (a *application) resolvedConfigProfile(command *cobra.Command) (ingest.Profile, error) {
	return resolveTreatment(treatmentSettings{
		selection:               a.configString(command, "profile", "ingest.profile"),
		segmentDuration:         a.configDuration(command, "segment-duration", "ingest.segment_duration"),
		segmentFormat:           media.SegmentFormat(a.configString(command, "segment-format", "ingest.segment_format")),
		essenceStorage:          media.EssenceStorage(a.configString(command, "essence-storage", "ingest.essence_storage")),
		ffmpegArgs:              a.configStringArray(command, "ffmpeg-arg", "media.ffmpeg_args"),
		segmentDurationExplicit: a.configOptionExplicit(command, "segment-duration", "ingest.segment_duration"),
		segmentFormatExplicit:   a.configOptionExplicit(command, "segment-format", "ingest.segment_format"),
		essenceStorageExplicit:  a.configOptionExplicit(command, "essence-storage", "ingest.essence_storage"),
	})
}

func (a *application) configOptionExplicit(command *cobra.Command, flag, key string) bool {
	return commandFlagChanged(command, flag) || a.configValueExplicit(key)
}

func (a *application) configValueExplicit(key string) bool {
	if a.v.InConfig(key) {
		return true
	}
	value, exists := os.LookupEnv(configEnvironmentName(key))
	return exists && value != ""
}

func (a *application) validateConfiguredURLs(command *cobra.Command) error {
	if endpoint := a.v.GetString("endpoint"); endpoint != "" {
		if _, err := validateTAMSEndpoint(endpoint); err != nil {
			return err
		}
	}
	for _, configuredURL := range []struct {
		label string
		value string
	}{
		{label: "OAuth token URL", value: a.v.GetString("auth.token_url")},
		{label: "OAuth redirect URL", value: a.v.GetString("auth.redirect_url")},
		{label: "S3 endpoint", value: a.configString(command, "s3-endpoint", "source.s3_endpoint")},
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
