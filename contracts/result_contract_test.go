package contracts_test

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const tamsinSchemaBase = "https://tamsin.livewyer.io/contracts/"

var publishedTamsinSchemas = []string{
	"doctor-report-v1.json",
	"ingest-events-v2.json",
	"profiles-report-v1.json",
}

func compileTamsinSchema(t *testing.T, name string) *jsonschema.Schema {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	for _, filename := range publishedTamsinSchemas {
		file, err := os.Open("tamsin/" + filename)
		if err != nil {
			t.Fatalf("open schema %s: %v", filename, err)
		}
		document, err := jsonschema.UnmarshalJSON(file)
		_ = file.Close()
		if err != nil {
			t.Fatalf("decode schema %s: %v", filename, err)
		}
		if err := compiler.AddResource(tamsinSchemaBase+filename, document); err != nil {
			t.Fatalf("add schema %s: %v", filename, err)
		}
	}
	schema, err := compiler.Compile(tamsinSchemaBase + name)
	if err != nil {
		t.Fatalf("compile schema %s: %v", name, err)
	}
	return schema
}

func TestPublishedSchemasUseCanonicalIdentifiers(t *testing.T) {
	t.Parallel()
	entries, err := os.ReadDir("tamsin")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(publishedTamsinSchemas) {
		t.Fatalf("published schema count = %d, want %d", len(entries), len(publishedTamsinSchemas))
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			t.Fatalf("unexpected contract entry %q", entry.Name())
		}
		data, err := os.ReadFile("tamsin/" + entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]any
		if err := json.Unmarshal(data, &document); err != nil {
			t.Fatalf("decode %s: %v", entry.Name(), err)
		}
		want := tamsinSchemaBase + entry.Name()
		if got := document["$id"]; got != want {
			t.Errorf("%s $id = %v, want %q", entry.Name(), got, want)
		}
		compileTamsinSchema(t, entry.Name())
	}
}

func validate(t *testing.T, schema *jsonschema.Schema, value any) error {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal instance: %v", err)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("decode instance: %v", err)
	}
	return schema.Validate(instance)
}
