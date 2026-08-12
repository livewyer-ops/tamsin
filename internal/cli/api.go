package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/livewyer-ops/tamsin/internal/tams"
	"github.com/spf13/cobra"
)

func (a *application) apiCommand() *cobra.Command {
	command := helpGroupCommand("api", "Execute TAMS upload and ingest API operations")
	command.AddCommand(a.apiServiceCommand())
	command.AddCommand(a.apiStorageBackendsCommand())
	command.AddCommand(a.apiFlowProfileCommand())
	command.AddCommand(a.apiFlowCommand())
	command.AddCommand(a.apiStorageCommand())
	command.AddCommand(a.apiSegmentCommand())
	command.AddCommand(a.apiObjectCommand())
	command.AddCommand(a.apiRequestCommand())
	return command
}

func (a *application) apiFlowProfileCommand() *cobra.Command {
	command := helpGroupCommand("flow-profile", "List, inspect, or create TAMS 8.2 Flow Profiles")
	command.AddCommand(a.apiFlowProfileListCommand())
	command.AddCommand(a.apiFlowProfileGetCommand())
	command.AddCommand(a.apiFlowProfileCreateCommand())
	return command
}

func (a *application) apiFlowProfileListCommand() *cobra.Command {
	var format, codec, label string
	command := &cobra.Command{
		Use:   "list",
		Short: "List immutable Flow Profiles",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(command *cobra.Command, _ []string) error {
			client, err := a.clientForCommand(command)
			if err != nil {
				return err
			}
			value, err := client.Profiles(command.Context(), tams.ProfileListOptions{
				Format: format, Codec: codec, Label: label,
			})
			if err != nil {
				return withExit(ExitRemote, err)
			}
			return a.writeValue(value)
		},
	}
	command.Flags().StringVar(&format, "format", "", "filter by single-essence format URN")
	command.Flags().StringVar(&codec, "codec", "", "filter by codec media type")
	command.Flags().StringVar(&label, "label", "", "filter by exact Profile label")
	return command
}

func (a *application) apiFlowProfileGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "get PROFILE_ID",
		Short: "Get immutable Flow Profile metadata",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, args []string) error {
			client, err := a.clientForCommand(command)
			if err != nil {
				return err
			}
			value, err := client.Profile(command.Context(), args[0])
			if err != nil {
				return withExit(ExitRemote, err)
			}
			return a.writeValue(value)
		},
	}
}

func (a *application) apiFlowProfileCreateCommand() *cobra.Command {
	var filename string
	command := &cobra.Command{
		Use:   "create PROFILE_ID",
		Short: "Create an immutable Flow Profile",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, args []string) error {
			var body tams.Profile
			if err := a.readJSONFile(filename, &body); err != nil {
				return withExit(ExitUsage, err)
			}
			body["id"] = args[0]
			client, err := a.clientForCommand(command)
			if err != nil {
				return err
			}
			value, err := client.CreateProfile(command.Context(), args[0], body)
			if err != nil {
				return withExit(ExitRemote, err)
			}
			return a.writeValue(value)
		},
	}
	command.Flags().StringVarP(&filename, "file", "f", "-", "Profile JSON file or - for stdin")
	return command
}

func (a *application) apiServiceCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "service",
		Short: "Get TAMS service information",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(command *cobra.Command, _ []string) error {
			client, err := a.clientForCommand(command)
			if err != nil {
				return err
			}
			value, err := client.Service(command.Context())
			if err != nil {
				return withExit(ExitRemote, err)
			}
			return a.writeValue(value)
		},
	}
}

func (a *application) apiStorageBackendsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "storage-backends",
		Short: "List TAMS storage backends",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(command *cobra.Command, _ []string) error {
			client, err := a.clientForCommand(command)
			if err != nil {
				return err
			}
			value, err := client.StorageBackends(command.Context())
			if err != nil {
				return withExit(ExitRemote, err)
			}
			return a.writeValue(value)
		},
	}
}

func (a *application) apiFlowCommand() *cobra.Command {
	command := helpGroupCommand("flow", "Get or create a TAMS Flow")
	command.AddCommand(a.apiFlowGetCommand())
	command.AddCommand(a.apiFlowPutCommand())
	return command
}

func (a *application) apiFlowGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "get FLOW_ID",
		Short: "Get Flow metadata",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, args []string) error {
			client, err := a.clientForCommand(command)
			if err != nil {
				return err
			}
			value, err := client.Flow(command.Context(), args[0])
			if err != nil {
				return withExit(ExitRemote, err)
			}
			return a.writeValue(value)
		},
	}
}

func (a *application) apiFlowPutCommand() *cobra.Command {
	var filename string
	command := &cobra.Command{
		Use:   "put FLOW_ID",
		Short: "Create or replace Flow metadata",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, args []string) error {
			var body tams.Flow
			if err := a.readJSONFile(filename, &body); err != nil {
				return withExit(ExitUsage, err)
			}
			body["id"] = args[0]
			client, err := a.clientForCommand(command)
			if err != nil {
				return err
			}
			value, err := client.PutFlow(command.Context(), args[0], body)
			if err != nil {
				return withExit(ExitRemote, err)
			}
			if value == nil {
				value = body
			}
			return a.writeValue(value)
		},
	}
	command.Flags().StringVarP(&filename, "file", "f", "-", "Flow JSON file or - for stdin")
	return command
}

func (a *application) apiStorageCommand() *cobra.Command {
	command := helpGroupCommand("storage", "Allocate TAMS Media Object storage")
	command.AddCommand(a.apiStorageAllocateCommand())
	return command
}

func (a *application) apiStorageAllocateCommand() *cobra.Command {
	var objectIDs []string
	var storageID, contentType string
	var limit int
	var presigned bool
	command := &cobra.Command{
		Use:   "allocate FLOW_ID",
		Short: "Allocate upload URLs for a Flow",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, args []string) error {
			limitSet := command.Flags().Changed("limit")
			if (len(objectIDs) > 0) == limitSet {
				return withExit(ExitUsage, errors.New("provide exactly one of --object-id or --limit"))
			}
			if limitSet && limit <= 0 {
				return withExit(ExitUsage, errors.New("--limit must be positive"))
			}
			client, err := a.clientForCommand(command)
			if err != nil {
				return err
			}
			request := tams.StorageRequest{
				ObjectIDs: objectIDs, StorageID: storageID, Limit: limit, ContentType: contentType,
			}
			if command.Flags().Changed("presigned") {
				request.Presigned = &presigned
			}
			value, err := client.AllocateStorage(command.Context(), args[0], request)
			if err != nil {
				return withExit(ExitRemote, err)
			}
			return a.writeValue(value)
		},
	}
	command.Flags().StringArrayVar(&objectIDs, "object-id", nil, "requested Object ID (repeatable)")
	command.Flags().StringVar(&storageID, "storage-id", "", "storage backend ID")
	command.Flags().StringVar(&contentType, "content-type", "", "init Object media type (TAMS 8.2)")
	command.Flags().BoolVar(&presigned, "presigned", false, "request presigned upload URLs (TAMS 8.2; use --presigned=false to refuse them)")
	command.Flags().IntVar(&limit, "limit", 0, "number of server-assigned Object IDs")
	return command
}

func (a *application) apiSegmentCommand() *cobra.Command {
	command := helpGroupCommand("segment", "List, register, or delete TAMS Flow Segments")
	command.AddCommand(a.apiSegmentListCommand())
	command.AddCommand(a.apiSegmentDeleteCommand())
	command.AddCommand(a.apiSegmentRegisterCommand())
	return command
}

func (a *application) apiSegmentListCommand() *cobra.Command {
	var objectID string
	var includeDownloadURLs bool
	command := &cobra.Command{
		Use:   "list FLOW_ID",
		Short: "List Flow Segments",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, args []string) error {
			client, err := a.clientForCommand(command)
			if err != nil {
				return err
			}
			value, err := client.ListSegments(command.Context(), args[0], tams.SegmentListOptions{
				ObjectID: objectID, IncludeDownloadURLs: includeDownloadURLs,
			})
			if err != nil {
				return withExit(ExitRemote, err)
			}
			return a.writeValue(value)
		},
	}
	command.Flags().StringVar(&objectID, "object-id", "", "filter by Object ID")
	command.Flags().BoolVar(&includeDownloadURLs, "include-download-urls", false,
		"include presigned download URLs and verbose storage metadata")
	return command
}

func (a *application) apiSegmentDeleteCommand() *cobra.Command {
	var timerange, deleteObjectID string
	command := &cobra.Command{
		Use:   "delete FLOW_ID",
		Short: "Delete Flow Segments covered by a timerange",
		Long: "Deletes the Flow Segment matching --timerange and --object-id. The command waits until " +
			"that exact Segment is absent. TAMS also deletes " +
			"any Media Object left unreferenced by the removal.",
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, args []string) error {
			client, err := a.clientForCommand(command)
			if err != nil {
				return err
			}
			if err := client.DeleteSegments(command.Context(), args[0], tams.SegmentDeleteOptions{
				Timerange: timerange, ObjectID: deleteObjectID,
			}); err != nil {
				return withExit(ExitRemote, err)
			}
			return nil
		},
	}
	// The TAMS default of "_" is the unbounded timerange, so an operator must
	// opt into deleting a whole Flow's Segments rather than reaching it by
	// omitting the flag.
	command.Flags().StringVar(&timerange, "timerange", "", "TAMS timerange to delete, or _ for the whole Flow")
	command.Flags().StringVar(&deleteObjectID, "object-id", "", "limit deletion to one exact Object ID")
	if err := command.MarkFlagRequired("timerange"); err != nil {
		panic(err)
	}
	if err := command.MarkFlagRequired("object-id"); err != nil {
		panic(err)
	}
	return command
}

func (a *application) apiSegmentRegisterCommand() *cobra.Command {
	var filename string
	command := &cobra.Command{
		Use:   "register FLOW_ID",
		Short: "Register a Flow Segment",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, args []string) error {
			var body tams.SegmentRequest
			if err := a.readJSONFile(filename, &body); err != nil {
				return withExit(ExitUsage, err)
			}
			client, err := a.clientForCommand(command)
			if err != nil {
				return err
			}
			if err := client.RegisterSegment(command.Context(), args[0], body); err != nil {
				return withExit(ExitRemote, err)
			}
			return a.writeValue(map[string]any{"status": "registered", "flow_id": args[0], "object_id": body.ObjectID})
		},
	}
	command.Flags().StringVarP(&filename, "file", "f", "-", "Segment JSON file or - for stdin")
	return command
}

func (a *application) apiObjectCommand() *cobra.Command {
	command := helpGroupCommand("object", "Inspect Objects and manage Object instances")
	command.AddCommand(a.apiObjectGetCommand())
	command.AddCommand(a.apiObjectInstanceCommand())
	return command
}

func (a *application) apiObjectGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "get OBJECT_ID",
		Short: "Get Object information",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, args []string) error {
			client, err := a.clientForCommand(command)
			if err != nil {
				return err
			}
			value, err := client.Object(command.Context(), args[0])
			if err != nil {
				return withExit(ExitRemote, err)
			}
			return a.writeValue(value)
		},
	}
}

func (a *application) apiObjectInstanceCommand() *cobra.Command {
	command := helpGroupCommand("instance", "Register or delete Object instances")
	command.AddCommand(a.apiObjectInstanceRegisterCommand())
	command.AddCommand(a.apiObjectInstanceDeleteCommand())
	return command
}

func (a *application) apiObjectInstanceRegisterCommand() *cobra.Command {
	var storageID, instanceURL, label string
	command := &cobra.Command{
		Use:   "register OBJECT_ID",
		Short: "Register a controlled or external Object instance",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, args []string) error {
			if (storageID == "") == (instanceURL == "") {
				return withExit(ExitUsage, errors.New("provide exactly one of --storage-id or --url"))
			}
			if instanceURL != "" && label == "" {
				return withExit(ExitUsage, errors.New("--label is required with --url"))
			}
			if storageID != "" && label != "" {
				return withExit(ExitUsage, errors.New("--label may only be used with --url"))
			}
			client, err := a.clientForCommand(command)
			if err != nil {
				return err
			}
			request := tams.ObjectInstanceRequest{StorageID: storageID, URL: instanceURL, Label: label}
			if err := client.RegisterObjectInstance(command.Context(), args[0], request); err != nil {
				return withExit(ExitRemote, err)
			}
			return a.writeValue(map[string]any{"status": "registered", "object_id": args[0]})
		},
	}
	command.Flags().StringVar(&storageID, "storage-id", "", "controlled storage backend ID")
	command.Flags().StringVar(&instanceURL, "url", "", "external Object URL")
	command.Flags().StringVar(&label, "label", "", "instance label")
	return command
}

func (a *application) apiObjectInstanceDeleteCommand() *cobra.Command {
	var deleteStorageID, deleteLabel string
	command := &cobra.Command{
		Use:   "delete OBJECT_ID",
		Short: "Delete one Object instance",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(command *cobra.Command, args []string) error {
			if (deleteStorageID == "") == (deleteLabel == "") {
				return withExit(ExitUsage, errors.New("provide exactly one of --storage-id or --label"))
			}
			client, err := a.clientForCommand(command)
			if err != nil {
				return err
			}
			if err := client.DeleteObjectInstance(command.Context(), args[0], deleteStorageID, deleteLabel); err != nil {
				return withExit(ExitRemote, err)
			}
			return a.writeValue(map[string]any{"status": "deleted", "object_id": args[0]})
		},
	}
	command.Flags().StringVar(&deleteStorageID, "storage-id", "", "controlled storage backend ID")
	command.Flags().StringVar(&deleteLabel, "label", "", "external instance label")
	return command
}

func (a *application) apiRequestCommand() *cobra.Command {
	var filename string
	command := &cobra.Command{
		Use:   "request METHOD PATH",
		Short: "Execute a raw JSON request against the pinned TAMS API",
		Args:  usageArgs(cobra.ExactArgs(2)),
		RunE: func(command *cobra.Command, args []string) error {
			var body []byte
			var err error
			if filename != "" {
				body, err = a.readFile(filename)
				if err != nil {
					return withExit(ExitUsage, err)
				}
			}
			client, err := a.clientForCommand(command)
			if err != nil {
				return err
			}
			value, err := client.RawJSON(command.Context(), strings.ToUpper(args[0]), args[1], body)
			if err != nil {
				return withExit(ExitRemote, err)
			}
			if len(value) == 0 {
				return a.writeValue(map[string]any{"status": "success"})
			}
			return a.writeRawJSON(value)
		},
	}
	command.Flags().StringVarP(&filename, "file", "f", "", "JSON request body file or - for stdin")
	return command
}

func (a *application) clientForCommand(command *cobra.Command) (*tams.Client, error) {
	endpoint := strings.TrimRight(a.v.GetString("endpoint"), "/")
	if endpoint == "" {
		return nil, withExit(ExitUsage, errors.New("TAMS endpoint is required; use --endpoint"))
	}
	client, _, err := a.tamsClient(command.Context(), endpoint, a.httpTransport(), nil)
	if err != nil {
		return nil, withExit(ExitAuth, err)
	}
	return client, nil
}

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

func (a *application) writeRawJSON(value []byte) error {
	if !json.Valid(value) {
		return errors.New("TAMS returned invalid JSON")
	}
	if strings.EqualFold(a.v.GetString("format"), "human") {
		var indented bytes.Buffer
		if err := json.Indent(&indented, value, "", "  "); err != nil {
			return err
		}
		_, err := fmt.Fprintln(a.stdout, indented.String())
		return err
	}
	_, err := fmt.Fprintln(a.stdout, string(value))
	return err
}

func (a *application) readJSONFile(filename string, destination any) error {
	data, err := a.readFile(filename)
	if err != nil {
		return err
	}
	// Flow and Segment request bodies are object contracts. JSON null decodes
	// successfully into a nil map or a zero-value struct; the former panics when
	// Flow PUT supplies its path ID, and neither is a meaningful request. Keep
	// arbitrary valid JSON, including null, available through `api request`.
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return errors.New("JSON request must be an object, not null")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode JSON request: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("JSON request contains multiple values")
		}
		return fmt.Errorf("decode trailing JSON request content: %w", err)
	}
	return nil
}

func (a *application) readFile(filename string) ([]byte, error) {
	var reader io.Reader
	if filename == "-" {
		reader = a.stdin
	} else {
		file, err := os.Open(filename)
		if err != nil {
			return nil, fmt.Errorf("open request body: %w", err)
		}
		defer file.Close()
		reader = file
	}
	data, err := io.ReadAll(io.LimitReader(reader, (2<<20)+1))
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}
	if len(data) > 2<<20 {
		return nil, errors.New("request body exceeds 2 MiB")
	}
	return data, nil
}
