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

// configDefinitions is the single allow-list for file and TAMSIN_* settings.
// Keep it sorted: effective output and validation diagnostics then remain
// deterministic, and a new runtime setting cannot be added without making an
// explicit decision about its type, default, and disclosure policy here.
func configDefinitions() []configDefinition {
	return []configDefinition{
		{key: "auth.allow_insecure_loopback", kind: configBool, defaultValue: false, flag: "allow-insecure-auth-loopback", fileAllowed: true},
		{key: "auth.authorization_url", kind: configString, defaultValue: "", flag: "authorization-url", redactURL: true, fileAllowed: true},
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
		{key: "ingest.journal", kind: configString, defaultValue: "", fileAllowed: true},
		{key: "ingest.max_inputs", kind: configInt, defaultValue: source.DefaultMaxInputs, fileAllowed: true},
		{key: "ingest.probe_concurrency", kind: configInt, defaultValue: 2, fileAllowed: true},
		{key: "ingest.profile", kind: configString, defaultValue: "", fileAllowed: true},
		{key: "ingest.segment_duration", kind: configDuration, defaultValue: defaultSegmentDuration, fileAllowed: true},
		{key: "ingest.segment_format", kind: configString, defaultValue: string(media.SegmentFormatSource), fileAllowed: true},
		{key: "ingest.source_id", kind: configString, defaultValue: "", fileAllowed: true},
		{key: "ingest.staging_byte_budget", kind: configString, defaultValue: "auto", fileAllowed: true},
		{key: "ingest.start", kind: configString, defaultValue: "0:0", fileAllowed: true},
		{key: "ingest.storage_id", kind: configString, defaultValue: "", fileAllowed: true},
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
	a.v.SetEnvPrefix("TAMSIN")
	a.v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	a.v.AutomaticEnv()
	for _, definition := range configDefinitions() {
		a.v.SetDefault(definition.key, definition.defaultValue)
	}
}

func (a *application) validateConfigFile(data []byte) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("decode YAML: %w", err)
	}
	if len(document.Content) > 0 {
		root := dereferenceYAMLNode(document.Content[0])
		if root.Kind != yaml.MappingNode {
			return fmt.Errorf("configuration document must be a mapping, got %s", yamlKindName(root))
		}
		if err := validateConfigMapping(root, "", configDefinitionMap(), make(map[string]configPosition)); err != nil {
			return err
		}
	}

	var trailing yaml.Node
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("decode trailing YAML document: %w", err)
		}
		return errors.New("configuration must contain exactly one YAML document")
	}
	return nil
}

func validateConfigMapping(node *yaml.Node, prefix string, definitions map[string]configDefinition, seen map[string]configPosition) error {
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
			continue
		}

		if hasConfigChildren(path, definitions) {
			if valueNode.Kind != yaml.MappingNode {
				return fmt.Errorf("configuration key %q at line %d, column %d must be a mapping, got %s", path, valueNode.Line, valueNode.Column, yamlKindName(valueNode))
			}
			if err := validateConfigMapping(valueNode, path, definitions, seen); err != nil {
				return err
			}
			continue
		}

		correction := closestConfigKey(path, configCandidatePaths(definitions))
		if correction != "" {
			return fmt.Errorf("unknown configuration key %q at line %d, column %d; did you mean %q?", path, keyNode.Line, keyNode.Column, correction)
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

func configCandidatePaths(definitions map[string]configDefinition) []string {
	candidates := make(map[string]struct{}, len(definitions)*2)
	for key, definition := range definitions {
		if !definition.fileAllowed {
			continue
		}
		candidates[key] = struct{}{}
		parts := strings.Split(key, ".")
		for index := 1; index < len(parts); index++ {
			candidates[strings.Join(parts[:index], ".")] = struct{}{}
		}
	}
	result := make([]string, 0, len(candidates))
	for candidate := range candidates {
		result = append(result, candidate)
	}
	sort.Strings(result)
	return result
}

func closestConfigKey(input string, candidates []string) string {
	inputRunes := []rune(input)
	threshold := max(2, len(inputRunes)/3)
	best := ""
	bestDistance := -1
	for _, candidate := range candidates {
		candidateRunes := []rune(candidate)
		// Length difference is a lower bound for Levenshtein distance. Skip
		// impossible suggestions before scanning a potentially hostile key.
		if difference := len(inputRunes) - len(candidateRunes); difference > threshold || difference < -threshold {
			continue
		}
		distance := editDistance(inputRunes, candidateRunes)
		if bestDistance < 0 || distance < bestDistance || distance == bestDistance && candidate < best {
			best = candidate
			bestDistance = distance
		}
	}
	if bestDistance < 0 || bestDistance > threshold {
		return ""
	}
	return best
}

func editDistance(a, b []rune) int {
	previous := make([]int, len(b)+1)
	current := make([]int, len(b)+1)
	for index := range previous {
		previous[index] = index
	}
	for leftIndex, leftRune := range a {
		current[0] = leftIndex + 1
		for rightIndex, rightRune := range b {
			cost := 0
			if leftRune != rightRune {
				cost = 1
			}
			current[rightIndex+1] = min(
				previous[rightIndex+1]+1,
				current[rightIndex]+1,
				previous[rightIndex]+cost,
			)
		}
		previous, current = current, previous
	}
	return previous[len(b)]
}

func (a *application) validateConfigEnvironment() error {
	definitions := configDefinitionMap()
	known := make(map[string]configDefinition, len(definitions))
	knownNames := make([]string, 0, len(definitions))
	for _, definition := range definitions {
		name := configEnvironmentName(definition.key)
		known[name] = definition
		knownNames = append(knownNames, name)
	}
	sort.Strings(knownNames)

	environment := os.Environ()
	sort.Strings(environment)
	for _, entry := range environment {
		name, value, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(name, "TAMSIN_") {
			continue
		}
		definition, exists := known[name]
		if !exists {
			correction := closestConfigKey(name, knownNames)
			if correction != "" {
				return fmt.Errorf("unsupported environment variable %q; did you mean %q?", name, correction)
			}
			return fmt.Errorf("unsupported environment variable %q", name)
		}
		// Viper deliberately treats an empty known variable as unset. It still
		// has to be a known name: a misspelled empty Secret/ConfigMap key is just
		// as likely to become non-empty on the next deployment.
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
	switch kind {
	case configBool:
		if _, err := strconv.ParseBool(value); err != nil {
			return fmt.Errorf("environment variable %s must be a boolean", name)
		}
	case configInt:
		if _, err := strconv.Atoi(value); err != nil {
			return fmt.Errorf("environment variable %s must be an integer", name)
		}
	case configDuration:
		if _, err := time.ParseDuration(value); err != nil {
			return fmt.Errorf("environment variable %s must be a duration", name)
		}
	case configStrings:
		if _, err := decodeEnvironmentStrings(name, value); err != nil {
			return err
		}
	}
	return nil
}

// decodeEnvironmentStrings retains Viper's established whitespace-separated
// scalar behavior while adding JSON arrays as the lossless representation for
// values which contain spaces or commas. A leading '[' is unambiguously an
// attempt to use the documented array form and is therefore validated strictly.
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

// configStrings applies the same precedence as Viper without accepting its
// lossy strings.Fields conversion for JSON-encoded environment arrays.
func (a *application) configStrings(key string) []string {
	if definition, exists := configDefinitionMap()[key]; exists && definition.flag != "" && a.persistentFlags != nil {
		if flag := a.persistentFlags.Lookup(definition.flag); flag != nil && flag.Changed {
			return a.v.GetStringSlice(key)
		}
	}
	if value, exists := os.LookupEnv(configEnvironmentName(key)); exists && value != "" {
		values, err := decodeEnvironmentStrings(configEnvironmentName(key), value)
		if err == nil {
			return values
		}
		// Execution validates the environment before reading effective values.
		// Keep this defensive path inert for direct package callers.
		return nil
	}
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

// resolveTreatment is the one input-independent media-policy validator used by
// config inspection, doctor, and ingest. Keeping the precedence-specific value
// collection outside this function prevents those command surfaces from
// disagreeing about whether the same effective treatment can take effect.
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
		{label: "OAuth authorization URL", value: a.v.GetString("auth.authorization_url")},
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

func (a *application) validateAuthConfiguration() error {
	cleanEndpoint := ""
	endpointToken := ""
	if endpoint := a.v.GetString("endpoint"); endpoint != "" {
		var err error
		endpointToken, err = validateTAMSEndpoint(endpoint)
		if err != nil {
			return err
		}
		cleanEndpoint, _, err = auth.ExtractURLToken(endpoint)
		if err != nil {
			return err
		}
	}
	config := a.authenticationConfig(cleanEndpoint, endpointToken)
	mode, err := config.ResolveMode()
	if err != nil {
		return err
	}
	if err := config.Validate(mode); err != nil {
		return err
	}
	return nil
}

type effectiveConfigValue struct {
	Value        any    `json:"value"`
	Source       string `json:"source"`
	SourceDetail string `json:"source_detail,omitempty"`
	ResolvedFrom string `json:"resolved_from,omitempty"`
}

type effectiveConfigResult struct {
	ConfigFile string                          `json:"config_file,omitempty"`
	Effective  map[string]effectiveConfigValue `json:"effective"`
}

func (a *application) configCommand() *cobra.Command {
	command := helpGroupCommand("config", "Validate and inspect configuration")
	command.AddCommand(a.configValidateCommand())
	command.AddCommand(a.configShowCommand())
	return command
}

func (a *application) configValidateCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate the effective configuration without running an ingest",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := a.validateAuthConfiguration(); err != nil {
				return withExit(ExitUsage, err)
			}
			result := map[string]any{"status": "valid"}
			if a.configFile != "" {
				result["config_file"] = a.configFile
			}
			return a.writeValue(result)
		},
	}
}

func (a *application) configShowCommand() *cobra.Command {
	var effective bool
	command := &cobra.Command{
		Use:   "show --effective",
		Short: "Show redacted effective values and their provenance",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(command *cobra.Command, _ []string) error {
			if !effective {
				return withExit(ExitUsage, errors.New("config show requires --effective"))
			}
			return a.writeValue(a.effectiveConfig(command))
		},
	}
	command.Flags().BoolVar(&effective, "effective", false, "show resolved values after precedence is applied")
	return command
}

func (a *application) effectiveConfig(command *cobra.Command) effectiveConfigResult {
	result := effectiveConfigResult{
		ConfigFile: a.configFile,
		Effective:  make(map[string]effectiveConfigValue),
	}
	profile, err := a.resolvedConfigProfile(command)
	if err != nil {
		// Persistent pre-run validation already reports this to users. Retain a
		// defensive fallback for direct unit callers rather than panicking.
		profile = ingest.Profile{
			Name: ingest.ProfileCustom, Version: ingest.ProfileVersion,
			SegmentDuration: a.v.GetDuration("ingest.segment_duration"),
			SegmentFormat:   media.SegmentFormat(a.v.GetString("ingest.segment_format")),
			EssenceStorage:  media.EssenceStorage(a.v.GetString("ingest.essence_storage")),
		}
	}
	for _, definition := range configDefinitions() {
		value := a.effectiveConfigValue(definition, profile)
		source, detail := a.configSource(command, definition)
		result.Effective[definition.key] = effectiveConfigValue{
			Value: value, Source: source, SourceDetail: detail, ResolvedFrom: a.configResolution(definition, profile),
		}
	}
	return result
}

func (a *application) configResolution(definition configDefinition, profile ingest.Profile) string {
	if definition.key == "ingest.transfers" && a.v.GetInt(definition.key) == 0 {
		return "ingest.concurrency"
	}
	if definition.key == "ingest.probe_concurrency" && a.v.GetInt(definition.key) == 0 {
		return "automatic media process budget"
	}
	switch definition.key {
	case "ingest.segment_duration", "ingest.segment_format", "ingest.essence_storage":
		if !a.configValueExplicit(definition.key) {
			return "ingest.profile"
		}
	case "ingest.profile":
		if profile.Name == ingest.ProfileCustom {
			return "ingest.profile + explicit media overrides"
		}
	}
	return ""
}

func (a *application) effectiveConfigValue(definition configDefinition, profile ingest.Profile) any {
	if definition.key == "config" && a.configFile != "" {
		return a.configFile
	}
	if definition.key == "ingest.transfers" && a.v.GetInt(definition.key) == 0 {
		return a.v.GetInt("ingest.concurrency")
	}
	if definition.key == "ingest.probe_concurrency" && a.v.GetInt(definition.key) == 0 {
		return 2
	}
	switch definition.key {
	case "ingest.profile":
		return profile.Name
	case "ingest.segment_duration":
		return profile.SegmentDuration.String()
	case "ingest.segment_format":
		return string(profile.SegmentFormat)
	case "ingest.essence_storage":
		return string(profile.EssenceStorage)
	}
	var value any
	switch definition.kind {
	case configString:
		value = a.v.GetString(definition.key)
	case configBool:
		value = a.v.GetBool(definition.key)
	case configInt:
		value = a.v.GetInt(definition.key)
	case configDuration:
		value = a.v.GetDuration(definition.key).String()
	case configStrings:
		value = a.configStrings(definition.key)
	default:
		panic("unknown configuration kind")
	}

	if definition.secret {
		switch typed := value.(type) {
		case string:
			if typed != "" {
				return "<redacted>"
			}
		case []string:
			redacted := make([]string, len(typed))
			for index := range typed {
				redacted[index] = "<redacted>"
			}
			return redacted
		}
	}
	if definition.redactURL {
		if typed, ok := value.(string); ok && typed != "" {
			return redactConfiguredURL(typed)
		}
	}
	if definition.key == "input" {
		inputs := value.([]string)
		redacted := make([]string, len(inputs))
		for index, input := range inputs {
			redacted[index] = redactConfiguredURL(input)
		}
		return redacted
	}
	return value
}

func redactConfiguredURL(value string) string {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" {
		return value
	}
	return auth.RedactURL(value)
}

func (a *application) configSource(command *cobra.Command, definition configDefinition) (string, string) {
	if definition.flag != "" {
		if flag := command.Flag(definition.flag); flag != nil && flag.Changed {
			return "flag", "--" + definition.flag
		}
	}
	environment := configEnvironmentName(definition.key)
	if value, exists := os.LookupEnv(environment); exists && value != "" {
		return "env", environment
	}
	if definition.fileAllowed && a.v.InConfig(definition.key) {
		return "file", a.configFile
	}
	return "default", ""
}
