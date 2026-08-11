package cli

import (
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/livewyer-ops/tamsin/internal/ingest"
	"github.com/spf13/cobra"
)

const ProfilesReportSchemaVersion = "1.0"

type profileReport struct {
	Name                 string `json:"name"`
	Version              string `json:"version"`
	Selection            string `json:"selection"`
	EssenceStorage       string `json:"essence_storage"`
	SegmentDuration      string `json:"segment_duration"`
	SegmentFormat        string `json:"segment_format"`
	FFmpeg               string `json:"ffmpeg"`
	SourceBytesPreserved bool   `json:"source_bytes_preserved"`
	ObjectPattern        string `json:"object_pattern"`
	IntendedUse          string `json:"intended_use"`
	ResourceNote         string `json:"resource_note"`
}

type profilesReport struct {
	SchemaVersion        string          `json:"schema_version"`
	ProfilePolicyVersion string          `json:"profile_policy_version"`
	Profiles             []profileReport `json:"profiles"`
}

func (a *application) profilesCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "profiles",
		Short: "List built-in ingest profiles and their resource trade-offs",
		Args:  usageArgs(cobra.NoArgs),
		Annotations: map[string]string{
			configIndependentAnnotation: "true",
		},
		RunE: func(_ *cobra.Command, _ []string) error {
			return a.writeProfiles(profileCatalogueReport())
		},
	}
}

func profileCatalogueReport() profilesReport {
	definitions := ingest.BuiltInProfiles()
	report := profilesReport{
		SchemaVersion: ProfilesReportSchemaVersion, ProfilePolicyVersion: ingest.ProfilePolicyVersion,
		Profiles: make([]profileReport, 0, len(definitions)),
	}
	for _, definition := range definitions {
		report.Profiles = append(report.Profiles, profileReport{
			Name: definition.Name, Version: definition.Version,
			Selection:       definition.Name + "@" + definition.Version,
			EssenceStorage:  string(definition.EssenceStorage),
			SegmentDuration: definition.SegmentDuration.String(),
			SegmentFormat:   string(definition.SegmentFormat), FFmpeg: string(definition.FFmpegUse),
			SourceBytesPreserved: definition.SourceBytesPreserved,
			ObjectPattern:        definition.ObjectPattern, IntendedUse: definition.IntendedUse,
			ResourceNote: definition.ResourceNote,
		})
	}
	return report
}

func profileFlagDescription() string {
	names := make([]string, 0, len(ingest.BuiltInProfiles()))
	for _, definition := range ingest.BuiltInProfiles() {
		names = append(names, definition.Name)
	}
	return "versioned ingest profile: " + strings.Join(names, ", ")
}

func versionedProfileSelections() []string {
	definitions := ingest.BuiltInProfiles()
	selections := make([]string, 0, len(definitions))
	for _, definition := range definitions {
		selections = append(selections, definition.Name+"@"+definition.Version)
	}
	return selections
}

func registerProfileCompletion(command *cobra.Command) {
	err := command.RegisterFlagCompletionFunc("profile", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		definitions := ingest.BuiltInProfiles()
		values := make([]string, 0, len(definitions))
		for _, definition := range definitions {
			values = append(values, definition.Name+"\t"+definition.IntendedUse)
		}
		return values, cobra.ShellCompDirectiveNoFileComp
	})
	if err != nil {
		panic(err)
	}
}

func (a *application) writeProfiles(report profilesReport) error {
	if !strings.EqualFold(a.v.GetString("format"), "human") {
		return a.writeValue(report)
	}
	writer := tabwriter.NewWriter(a.stdout, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(writer, "PROFILE\tSTORAGE\tTARGET\tFORMAT\tFFMPEG\tOBJECT PATTERN"); err != nil {
		return err
	}
	for _, profile := range report.Profiles {
		target := profile.SegmentDuration
		if target == "0s" {
			target = "whole"
		}
		format := profile.SegmentFormat
		if format == "source" && profile.SourceBytesPreserved {
			format = "source bytes"
		} else if format == "source" {
			format = "source-family"
		}
		ffmpeg := profile.FFmpeg
		if ffmpeg == string(ingest.FFmpegMultiEssenceOnly) {
			ffmpeg = "conditional"
		}
		if _, err := fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s\t%s\n",
			profile.Selection, profile.EssenceStorage, target, format, ffmpeg,
			profile.ObjectPattern); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(a.stdout, "\nUse and resource notes (Object counts are nominal; keyframes determine actual boundaries):"); err != nil {
		return err
	}
	for _, profile := range report.Profiles {
		if _, err := fmt.Fprintf(a.stdout, "  %s\n    use: %s\n    impact: %s\n", profile.Selection, profile.IntendedUse, profile.ResourceNote); err != nil {
			return err
		}
	}
	return nil
}
