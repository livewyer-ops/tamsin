package cli

import (
	"encoding/json"
	"fmt"
	"strings"
)

// writeValue renders finite command results. Ingest has its own streaming
// event and receipt writers and does not pass through this function.
func (a *application) writeValue(value any) error {
	if strings.EqualFold(a.v.GetString("format"), "human") {
		data, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(a.stdout, string(data))
		return err
	}
	encoder := json.NewEncoder(a.stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}
