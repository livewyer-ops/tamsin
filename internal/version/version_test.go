package version

import "testing"

func TestInjectedBuildProvenanceIsReported(t *testing.T) {
	originalVersion, originalCommit, originalDate := Version, Commit, Date
	t.Cleanup(func() {
		Version, Commit, Date = originalVersion, originalCommit, originalDate
	})

	Version = "dev"
	Commit = "abc123-dirty"
	Date = "2026-08-09T22:00:00Z"
	if got := SourceCommit(); got != Commit {
		t.Fatalf("SourceCommit() = %q, want %q", got, Commit)
	}
	if got := BuildDate(); got != Date {
		t.Fatalf("BuildDate() = %q, want %q", got, Date)
	}
	if got := String(); got != "dev (abc123-dirty, 2026-08-09T22:00:00Z)" {
		t.Fatalf("String() = %q", got)
	}
}
