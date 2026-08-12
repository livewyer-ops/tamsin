package contracts

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/livewyer-ops/tamsin/internal/tams"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// The runtime validators use the same immutable documents as the conformance
// tests. TAMS 8.1 remains the compatibility floor while 8.2 is the target, so
// both revisions are embedded rather than silently validating an older service
// against a newer request contract.
//
//go:embed schemas/v8.1/*.json schemas/v8.2/*.json
var runtimeSchemaFS embed.FS

const (
	schemaBase81 = "https://raw.githubusercontent.com/bbc/tams/98d307b09b5ebf79278aa7d3aad53295154e2c17/api/schemas/"
	schemaBase82 = "https://raw.githubusercontent.com/bbc/tams/ebb18b09cc6effe70a3464fc0282e9663d0a583f/api/schemas/"
)

type schemaRevision struct {
	directory string
	base      string
	flowPut   string
	flowGet   string
}

var revisions = map[int]*schemaCache{
	1: {revision: schemaRevision{directory: "schemas/v8.1", base: schemaBase81, flowPut: "flow.json", flowGet: "flow.json"}},
	2: {revision: schemaRevision{directory: "schemas/v8.2", base: schemaBase82, flowPut: "flow-put.json", flowGet: "flow-get.json"}},
}

type schemaCache struct {
	revision schemaRevision
	once     sync.Once
	compiler *jsonschema.Compiler
	err      error
	mu       sync.Mutex
	compiled map[string]*jsonschema.Schema
}

// ValidateFlow is retained for callers that want the target write contract.
// Version-aware ingest should call ValidateFlowPut explicitly.
func ValidateFlow(flow tams.Flow) error {
	return ValidateFlowPut(tams.APIVersion{Major: tams.SpecMajor, Minor: tams.SpecMinor}, flow)
}

// ValidateFlowPut checks the exact request representation accepted by a TAMS
// service. TAMS 8.2 deliberately separates the compact Profile-backed write
// form from the expanded read form.
func ValidateFlowPut(version tams.APIVersion, flow tams.Flow) error {
	revision, err := revisionFor(version)
	if err != nil {
		return err
	}
	return revision.validate(revision.revision.flowPut, flow)
}

// ValidateFlowGet checks an expanded Flow representation returned by a store
// or constructed locally while planning a Profile-backed write.
func ValidateFlowGet(version tams.APIVersion, flow tams.Flow) error {
	revision, err := revisionFor(version)
	if err != nil {
		return err
	}
	return revision.validate(revision.revision.flowGet, flow)
}

// ValidateProfile checks an immutable TAMS 8.2 Flow Profile.
func ValidateProfile(profile tams.Profile) error {
	return revisions[2].validate("profile.json", profile)
}

func revisionFor(version tams.APIVersion) (*schemaCache, error) {
	if version.Major != tams.SpecMajor || version.Minor < tams.CompatibilityMinor {
		return nil, fmt.Errorf("no embedded TAMS schema for API version %s", version)
	}
	if version.Minor == tams.CompatibilityMinor {
		return revisions[1], nil
	}
	return revisions[2], nil
}

func (cache *schemaCache) load() {
	cache.once.Do(func() {
		compiler := jsonschema.NewCompiler()
		entries, err := runtimeSchemaFS.ReadDir(cache.revision.directory)
		if err != nil {
			cache.err = fmt.Errorf("read embedded TAMS schemas: %w", err)
			return
		}
		for _, entry := range entries {
			file, err := runtimeSchemaFS.Open(cache.revision.directory + "/" + entry.Name())
			if err != nil {
				cache.err = fmt.Errorf("open embedded TAMS schema %s: %w", entry.Name(), err)
				return
			}
			document, decodeErr := jsonschema.UnmarshalJSON(file)
			_ = file.Close()
			if decodeErr != nil {
				cache.err = fmt.Errorf("decode embedded TAMS schema %s: %w", entry.Name(), decodeErr)
				return
			}
			if err := compiler.AddResource(cache.revision.base+entry.Name(), document); err != nil {
				cache.err = fmt.Errorf("register embedded TAMS schema %s: %w", entry.Name(), err)
				return
			}
		}
		cache.compiler = compiler
		cache.compiled = make(map[string]*jsonschema.Schema)
	})
}

func (cache *schemaCache) schema(name string) (*jsonschema.Schema, error) {
	cache.load()
	if cache.err != nil {
		return nil, cache.err
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if schema := cache.compiled[name]; schema != nil {
		return schema, nil
	}
	schema, err := cache.compiler.Compile(cache.revision.base + name)
	if err != nil {
		return nil, err
	}
	cache.compiled[name] = schema
	return schema, nil
}

func (cache *schemaCache) validate(name string, value any) error {
	schema, err := cache.schema(name)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode TAMS metadata: %w", err)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("decode TAMS metadata for validation: %w", err)
	}
	if err := schema.Validate(instance); err != nil {
		var validation *jsonschema.ValidationError
		if !errors.As(err, &validation) {
			return err
		}
		leaf := mostSpecific(validation)
		return fmt.Errorf("%s: %s", jsonPointer(leaf.InstanceLocation), compactValidationMessage(leaf.Error()))
	}
	return nil
}

func mostSpecific(validation *jsonschema.ValidationError) *jsonschema.ValidationError {
	var leaves []*jsonschema.ValidationError
	collectValidationLeaves(validation, &leaves)
	if len(leaves) == 0 {
		return validation
	}
	frequency := make(map[string]int, len(leaves))
	for _, leaf := range leaves {
		frequency[jsonPointer(leaf.InstanceLocation)]++
	}
	best := leaves[0]
	for _, candidate := range leaves[1:] {
		candidateFrequency := frequency[jsonPointer(candidate.InstanceLocation)]
		bestFrequency := frequency[jsonPointer(best.InstanceLocation)]
		if candidateFrequency > bestFrequency ||
			(candidateFrequency == bestFrequency && len(candidate.InstanceLocation) > len(best.InstanceLocation)) {
			best = candidate
		}
	}
	return best
}

func collectValidationLeaves(validation *jsonschema.ValidationError, leaves *[]*jsonschema.ValidationError) {
	if len(validation.Causes) == 0 {
		*leaves = append(*leaves, validation)
		return
	}
	for _, cause := range validation.Causes {
		collectValidationLeaves(cause, leaves)
	}
}

func jsonPointer(parts []string) string {
	if len(parts) == 0 {
		return "/"
	}
	escaped := make([]string, len(parts))
	for index := range parts {
		escaped[index] = strings.ReplaceAll(strings.ReplaceAll(parts[index], "~", "~0"), "/", "~1")
	}
	return "/" + strings.Join(escaped, "/")
}

func compactValidationMessage(message string) string {
	message = strings.TrimSpace(message)
	if _, remainder, found := strings.Cut(message, ": "); found {
		return remainder
	}
	return message
}
