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

// The schemas used at runtime are the same vendored, commit-pinned documents
// exercised by the contract tests. Keeping the bytes in the binary makes
// metadata preflight deterministic and prevents an ingest from depending on
// the availability or current contents of github.com/bbc/tams.
//
//go:embed schemas/*.json
var runtimeSchemaFS embed.FS

const schemaBase = "https://raw.githubusercontent.com/bbc/tams/" +
	"98d307b09b5ebf79278aa7d3aad53295154e2c17/api/schemas/"

var (
	flowSchemaOnce sync.Once
	flowSchema     *jsonschema.Schema
	flowSchemas    map[string]*jsonschema.Schema
	flowSchemaErr  error
)

// ValidateFlow checks the exact JSON representation sent by Tamsin against the
// pinned TAMS Flow schema. The returned error names the most specific JSON
// Pointer available, so an operator can correct --flow-metadata without having
// to interpret the validator's schema traversal.
func ValidateFlow(flow tams.Flow) error {
	flowSchemaOnce.Do(func() {
		compiler := jsonschema.NewCompiler()
		entries, err := runtimeSchemaFS.ReadDir("schemas")
		if err != nil {
			flowSchemaErr = fmt.Errorf("read embedded TAMS schemas: %w", err)
			return
		}
		for _, entry := range entries {
			file, err := runtimeSchemaFS.Open("schemas/" + entry.Name())
			if err != nil {
				flowSchemaErr = fmt.Errorf("open embedded TAMS schema %s: %w", entry.Name(), err)
				return
			}
			document, decodeErr := jsonschema.UnmarshalJSON(file)
			_ = file.Close()
			if decodeErr != nil {
				flowSchemaErr = fmt.Errorf("decode embedded TAMS schema %s: %w", entry.Name(), decodeErr)
				return
			}
			if err := compiler.AddResource(schemaBase+entry.Name(), document); err != nil {
				flowSchemaErr = fmt.Errorf("register embedded TAMS schema %s: %w", entry.Name(), err)
				return
			}
		}
		flowSchema, flowSchemaErr = compiler.Compile(schemaBase + "flow.json")
		if flowSchemaErr != nil {
			return
		}
		flowSchemas = make(map[string]*jsonschema.Schema, 5)
		for format, name := range map[string]string{
			"urn:x-nmos:format:video": "flow-video.json",
			"urn:x-nmos:format:audio": "flow-audio.json",
			"urn:x-tam:format:image":  "flow-image.json",
			"urn:x-nmos:format:data":  "flow-data.json",
			"urn:x-nmos:format:multi": "flow-multi.json",
		} {
			flowSchemas[format], flowSchemaErr = compiler.Compile(schemaBase + name)
			if flowSchemaErr != nil {
				return
			}
		}
	})
	if flowSchemaErr != nil {
		return flowSchemaErr
	}

	encoded, err := json.Marshal(flow)
	if err != nil {
		return fmt.Errorf("encode Flow metadata: %w", err)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("decode Flow metadata for validation: %w", err)
	}
	schema := flowSchema
	if format, ok := flow["format"].(string); ok && flowSchemas[format] != nil {
		// flow.json is a oneOf across five variants. Validating the known variant
		// avoids reporting a deeper but irrelevant error from the last nonmatching
		// branch (for example /format:multi for an invalid video generation).
		schema = flowSchemas[format]
	}
	if err := schema.Validate(instance); err != nil {
		var validation *jsonschema.ValidationError
		if !errors.As(err, &validation) {
			return err
		}
		leaf := mostSpecific(validation)
		pointer := jsonPointer(leaf.InstanceLocation)
		return fmt.Errorf("%s: %s", pointer, compactValidationMessage(leaf.Error()))
	}
	return nil
}

func mostSpecific(validation *jsonschema.ValidationError) *jsonschema.ValidationError {
	best := validation
	for _, cause := range validation.Causes {
		candidate := mostSpecific(cause)
		if len(candidate.InstanceLocation) > len(best.InstanceLocation) ||
			(len(candidate.InstanceLocation) == len(best.InstanceLocation) && len(candidate.Causes) == 0) {
			best = candidate
		}
	}
	return best
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
