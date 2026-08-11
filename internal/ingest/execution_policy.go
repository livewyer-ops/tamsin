package ingest

import "fmt"

// DryRunMode controls how much local work is performed without remote
// mutation. Fast validates the source and Flow graph but does not render media;
// exact executes the complete local renderer and Object preparation path.
type DryRunMode string

const (
	DryRunOff   DryRunMode = "off"
	DryRunFast  DryRunMode = "fast"
	DryRunExact DryRunMode = "exact"
)

func (mode DryRunMode) Validate() error {
	switch mode {
	case DryRunOff, DryRunFast, DryRunExact:
		return nil
	default:
		return fmt.Errorf("unsupported dry-run mode %q: use off, fast, or exact", mode)
	}
}

// VerificationMode selects the integrity evidence required after upload.
// Auto prefers trustworthy storage SHA-256 evidence and otherwise reads the
// registered Object back; readback always downloads; none is explicit opt-out.
type VerificationMode string

const (
	VerificationAuto     VerificationMode = "auto"
	VerificationReadback VerificationMode = "readback"
	VerificationNone     VerificationMode = "none"
)

func (mode VerificationMode) Validate() error {
	switch mode {
	case VerificationAuto, VerificationReadback, VerificationNone:
		return nil
	default:
		return fmt.Errorf("unsupported verification mode %q: use auto, readback, or none", mode)
	}
}
