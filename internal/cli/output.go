package cli

import (
	"encoding/json"
)

// writeJSON renders one finite command result. Ingest has its own streaming
// event and receipt writers and does not pass through this function.
func (a *application) writeJSON(value any) error {
	encoder := json.NewEncoder(a.stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}
