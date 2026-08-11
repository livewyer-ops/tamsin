package ingest

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"sync"

	"github.com/google/uuid"
	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/tams"
)

const identityEncoding = "tamsin/deterministic-id/v1"

// generatedRootFlowID deliberately has no locator or display name input. The
// staged content digest is already in profileKey, alongside every setting that
// changes the resulting media graph. A refreshed signature, a different mount
// point, or a manifest that reaches the same bytes is provenance, not identity.
func generatedRootFlowID(profileKey, mediaKey string) string {
	return framedID("flow/content-profile/v1", profileKey, mediaKey)
}

// generatedChildFlowID anchors every derived member to the root Flow. Besides
// making relations and positions explicit collision domains, this means an
// operator-supplied --flow-id stabilizes the whole graph rather than leaving
// its children dependent on an expiring input URL.
func generatedChildFlowID(rootFlowID, relation, position string) string {
	return framedID("flow/child/v1", rootFlowID, relation, position)
}

func framedID(parts ...string) string {
	return uuid.NewSHA1(idNamespace, identityName(parts...)).String()
}

// identityName frames every component by byte length before UUIDv5 hashes it.
// NUL-joining strings is ambiguous when a component itself contains NUL: the
// sequences {"a\x00b", "c"} and {"a", "b\x00c"} become byte-identical. Length
// framing makes the encoding injective and the first component remains the
// explicit, versioned recipe domain.
func identityName(parts ...string) []byte {
	name := make([]byte, 0, len(identityEncoding)+len(parts)*8)
	name = append(name, identityEncoding...)
	name = binary.AppendUvarint(name, uint64(len(parts)))
	for _, part := range parts {
		name = binary.AppendUvarint(name, uint64(len(part)))
		name = append(name, part...)
	}
	return name
}

func identityFingerprint(domain string, parts ...string) string {
	framed := identityName(append([]string{domain}, parts...)...)
	digest := sha256.Sum256(framed)
	return "sha256:" + hex.EncodeToString(digest[:])
}

type mediaInterpretationIdentity struct {
	Root               tams.Flow                     `json:"root"`
	Format             string                        `json:"format"`
	Codec              string                        `json:"codec"`
	Container          string                        `json:"container"`
	Start              int64                         `json:"start"`
	Duration           int64                         `json:"duration"`
	ContentType        string                        `json:"content_type"`
	SegmentContainer   media.SegmentContainer        `json:"segment_container"`
	ContainerSupported bool                          `json:"container_supported"`
	UnsupportedCodecs  []media.UnsupportedCodec      `json:"unsupported_codecs,omitempty"`
	Collected          []collectedFlowInterpretation `json:"collected,omitempty"`
}

type collectedFlowInterpretation struct {
	Role               string         `json:"role"`
	Flow               tams.Flow      `json:"flow"`
	ContainerMapping   map[string]any `json:"container_mapping,omitempty"`
	ContainerSupported bool           `json:"container_supported"`
	StreamIndex        int            `json:"stream_index"`
	Offset             int64          `json:"offset"`
}

// mediaInterpretationFingerprint captures only the normalized facts that
// BuildFlow and segmentation use. It excludes identifiers, labels, descriptions
// and provenance, so a filename can affect identity only when probing that name
// actually changes the media graph. This matters for raw formats and stdin:
// FFmpeg may legitimately interpret identical bytes differently from a naming
// hint, and those graphs must not claim one Flow ID.
func mediaInterpretationFingerprint(flow tams.Flow, info media.FlowInfo) (string, error) {
	identity := mediaInterpretationIdentity{
		Root: flowIdentityFields(flow), Format: info.Format, Codec: info.Codec,
		Container: info.Container, Start: info.Start, Duration: info.Duration,
		ContentType: info.ContentType, SegmentContainer: info.SegmentContainer,
		ContainerSupported: info.ContainerSupported,
		UnsupportedCodecs:  info.UnsupportedCodecs,
		Collected:          make([]collectedFlowInterpretation, len(info.Collected)),
	}
	for index, collected := range info.Collected {
		identity.Collected[index] = collectedFlowInterpretation{
			Role: collected.Role, Flow: flowIdentityFields(collected.Flow),
			ContainerMapping:   collected.ContainerMapping,
			ContainerSupported: collected.ContainerSupported,
			StreamIndex:        collected.StreamIndex,
			Offset:             collected.Offset,
		}
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	return identityFingerprint("media-interpretation/v1", string(encoded)), nil
}

func flowIdentityFields(flow tams.Flow) tams.Flow {
	technical := make(tams.Flow, len(flow))
	for key, value := range flow {
		switch key {
		case "id", "source_id", "label", "description", "tags":
			continue
		default:
			technical[key] = value
		}
	}
	return technical
}

type graphLock struct {
	mutex sync.Mutex
	users int
}

// acquireGraph returns an unlock function for one root Flow. Entries are
// reference-counted so a long-running Pipeline does not retain one mutex per
// input forever, while a waiter can never observe its lock removed underneath
// it.
func (p *Pipeline) acquireGraph(rootFlowID string) func() {
	p.graphLocksMu.Lock()
	lock := p.graphLocks[rootFlowID]
	if lock == nil {
		lock = &graphLock{}
		p.graphLocks[rootFlowID] = lock
	}
	lock.users++
	p.graphLocksMu.Unlock()

	lock.mutex.Lock()
	return func() {
		lock.mutex.Unlock()
		p.graphLocksMu.Lock()
		lock.users--
		if lock.users == 0 {
			delete(p.graphLocks, rootFlowID)
		}
		p.graphLocksMu.Unlock()
	}
}
