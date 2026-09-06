package ingest

import "testing"

func TestExecutionPolicyModes(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		valid []interface{ Validate() error }
		bad   interface{ Validate() error }
	}{
		{
			name: "dry run",
			valid: []interface{ Validate() error }{
				DryRunOff, DryRunFast, DryRunExact,
			},
			bad: DryRunMode("approximate"),
		},
		{
			name: "verification",
			valid: []interface{ Validate() error }{
				VerificationAuto, VerificationReadback, VerificationNone,
			},
			bad: VerificationMode("best-effort"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			for _, valid := range test.valid {
				if err := valid.Validate(); err != nil {
					t.Errorf("valid mode rejected: %v", err)
				}
			}
			if err := test.bad.Validate(); err == nil {
				t.Fatal("unknown mode unexpectedly accepted")
			}
		})
	}
}
