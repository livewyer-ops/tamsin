package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/livewyer-ops/tamsin/internal/media"
	"github.com/livewyer-ops/tamsin/internal/source"
)

const (
	autoStagingPercent      = 80
	segmentAllowancePercent = 105
	segmentAllowanceFloor   = 1 << 20
	// Rolling output is committed and removed as FFmpeg progresses. Reserving
	// more than this per active input reduces concurrency without improving
	// safety; one larger Segment can still extend the lease or fail with a
	// precise capacity error.
	rollingOutputWindowBytes = int64(512 << 20)
)

type freeSpaceFunc func(string) (int64, error)

// StagingInspection is the local capacity contract an ingest will start with.
// ConfiguredBudgetBytes is zero for the automatic policy; EffectiveBudgetBytes
// is the actual ledger after applying that policy to current filesystem space.
type StagingInspection struct {
	Directory             string
	FilesystemFreeBytes   int64
	ConfiguredBudgetBytes int64
	EffectiveBudgetBytes  int64
}

// InspectStaging validates that the configured staging directory exists, is
// writable, and has the requested capacity. The short-lived probe file is the
// only reliable cross-platform writability test for ACLs and mounted volumes;
// it is removed before this function returns.
func InspectStaging(tempRoot string, configured int64) (StagingInspection, error) {
	return inspectStaging(tempRoot, configured, nil)
}

func inspectStaging(tempRoot string, configured int64, available freeSpaceFunc) (StagingInspection, error) {
	if configured < 0 {
		return StagingInspection{}, errors.New("staging byte budget cannot be negative")
	}
	manager, err := newStagingManager(tempRoot, configured, available)
	if err != nil {
		return StagingInspection{}, err
	}
	inspection := StagingInspection{
		Directory: manager.root, FilesystemFreeBytes: manager.initialFree,
		ConfiguredBudgetBytes: configured, EffectiveBudgetBytes: manager.limit,
	}
	if manager.initialFree == 0 || manager.limit == 0 {
		return inspection, fmt.Errorf("staging directory %q has no available space", manager.root)
	}
	if configured > manager.initialFree {
		return inspection, fmt.Errorf(
			"configured staging byte budget %s exceeds the %s currently free at %q",
			formatBytes(configured), formatBytes(manager.initialFree), manager.root)
	}
	info, err := os.Stat(manager.root)
	if err != nil {
		return inspection, fmt.Errorf("inspect staging directory %q: %w", manager.root, err)
	}
	// Permission bits make an ownerless/read-only volume fail predictably even
	// when doctor itself happens to run as root. CreateTemp below additionally
	// checks ACLs, mount flags, and other platform policy.
	if info.Mode().Perm()&0o222 == 0 {
		return inspection, fmt.Errorf("staging directory %q is not writable", manager.root)
	}
	probe, err := os.CreateTemp(manager.root, ".tamsin-doctor-write-")
	if err != nil {
		return inspection, fmt.Errorf("staging directory %q is not writable: %w", manager.root, err)
	}
	name := probe.Name()
	if closeErr := probe.Close(); closeErr != nil {
		_ = os.Remove(name)
		return inspection, fmt.Errorf("close staging writability probe in %q: %w", manager.root, closeErr)
	}
	if err := os.Remove(name); err != nil {
		return inspection, fmt.Errorf("remove staging writability probe in %q: %w", manager.root, err)
	}
	return inspection, nil
}

// stagingManager coordinates capacity reservations across every concurrent
// input. Reserving the estimated peak before an input starts is deliberate:
// incrementally acquiring space can deadlock when several downloads each hold
// a partial file and all wait for the others to release enough for the next
// block.
type stagingManager struct {
	root             string
	limit            int64
	initialFree      int64
	spaceAvailable   freeSpaceFunc
	mu               sync.Mutex
	reserved         int64
	capacityReleased chan struct{}
}

type stagingLease struct {
	manager  *stagingManager
	label    string
	reserved int64

	mu        sync.Mutex
	used      int64
	artifacts map[string]int64
	released  bool
}

type stagingCapacityError struct {
	Label      string
	Required   int64
	Available  int64
	Budget     int64
	Filesystem int64
	Root       string
}

func (e *stagingCapacityError) Error() string {
	return fmt.Sprintf(
		"staging preflight for %s requires %s, but %s is available (global budget %s; filesystem %s free at %q); free staging space, raise --staging-byte-budget if the filesystem has room, reduce --concurrency, or use -d 0 --essence-storage muxed",
		e.Label, formatBytes(e.Required), formatBytes(e.Available), formatBytes(e.Budget), formatBytes(e.Filesystem), e.Root)
}

func newStagingManager(tempRoot string, configured int64, available freeSpaceFunc) (*stagingManager, error) {
	root := tempRoot
	if root == "" {
		root = os.TempDir()
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve staging directory %q: %w", root, err)
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return nil, fmt.Errorf("inspect staging directory %q: %w", absolute, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("staging path %q is not a directory", absolute)
	}
	if available == nil {
		available = filesystemAvailable
	}
	free, err := available(absolute)
	if err != nil {
		return nil, fmt.Errorf("inspect free space in staging directory %q: %w", absolute, err)
	}
	if free < 0 {
		return nil, fmt.Errorf("staging directory %q reported a negative available size", absolute)
	}

	limit := configured
	if limit == 0 {
		// Leave meaningful room for logs, runtime scratch data, and other
		// processes when the operator has not chosen an explicit quota.
		limit = free / 100 * autoStagingPercent
		if limit == 0 {
			limit = free
		}
	}
	if limit > free {
		limit = free
	}
	return &stagingManager{
		root: absolute, limit: limit,
		initialFree: free, spaceAvailable: available, capacityReleased: make(chan struct{}),
	}, nil
}

func (m *stagingManager) reserve(ctx context.Context, item source.Item, config Config) (*stagingLease, int64, int64, error) {
	label := safeURI(item.URI)
	required, unknown, err := estimatedStagingRequirement(item, config)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("estimate staging for %s: %w", label, err)
	}
	if unknown {
		// An unknown-length stream cannot make a trustworthy partial
		// reservation. Give it the whole budget and enforce that ceiling while
		// bytes arrive; this serialises staging of such inputs but cannot
		// deadlock or let two unbounded streams consume the same promise.
		required = m.limit
	}
	if config.InputMode == InputStream {
		required = min(required, m.limit)
	}
	if required == 0 && !unknown {
		return &stagingLease{manager: m, label: label, artifacts: make(map[string]int64)}, 0, m.availableCapacity(), nil
	}
	if required == 0 {
		return nil, 1, 0, m.capacityError(label, 1, 0)
	}
	if required > m.limit {
		return nil, required, m.availableCapacity(), m.capacityError(label, required, m.availableCapacity())
	}

	for {
		m.mu.Lock()
		budgetAvailable := m.limit - m.reserved
		wait := m.capacityReleased
		m.mu.Unlock()

		free, spaceErr := m.spaceAvailable(m.root)
		if spaceErr != nil {
			return nil, required, 0, fmt.Errorf("inspect free space in staging directory %q: %w", m.root, spaceErr)
		}
		available := min(budgetAvailable, free)
		if required <= available {
			m.mu.Lock()
			// Another waiter may have reserved between the free-space check and
			// this lock. Recheck the authoritative in-process budget.
			if required <= m.limit-m.reserved {
				m.reserved += required
				m.mu.Unlock()
				return &stagingLease{
					manager: m, label: label, reserved: required,
					artifacts: make(map[string]int64),
				}, required, available, nil
			}
			wait = m.capacityReleased
			m.mu.Unlock()
		}

		m.mu.Lock()
		noActiveReservations := m.reserved == 0
		m.mu.Unlock()
		if noActiveReservations {
			return nil, required, available, m.capacityError(label, required, available)
		}
		select {
		case <-ctx.Done():
			return nil, required, available, ctx.Err()
		case <-wait:
		}
	}
}

func (m *stagingManager) availableCapacity() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.limit - m.reserved
}

func (m *stagingManager) capacityError(label string, required, available int64) error {
	free, err := m.spaceAvailable(m.root)
	if err != nil {
		free = 0
	}
	return &stagingCapacityError{
		Label: label, Required: required, Available: max(available, 0),
		Budget: m.limit, Filesystem: free, Root: m.root,
	}
}

// extend grows a lease after an output exceeds its estimate. It never waits:
// FFmpeg cannot be safely paused while it has open output files, and waiting
// here would recreate the partial-allocation deadlock avoided by reserve.
func (m *stagingManager) extend(label string, extra, required int64) error {
	if extra <= 0 {
		return nil
	}
	m.mu.Lock()
	available := m.limit - m.reserved
	if extra > available {
		m.mu.Unlock()
		return m.capacityError(label, required, available)
	}
	m.reserved += extra
	m.mu.Unlock()
	return nil
}

func (m *stagingManager) release(bytes int64) {
	if bytes <= 0 {
		return
	}
	m.mu.Lock()
	m.reserved -= bytes
	if m.reserved < 0 {
		m.reserved = 0
	}
	close(m.capacityReleased)
	m.capacityReleased = make(chan struct{})
	m.mu.Unlock()
}

func (l *stagingLease) add(bytes int64) error {
	if l == nil || bytes <= 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return errors.New("staging capacity lease is already released")
	}
	required, err := checkedAdd(l.used, bytes)
	if err != nil {
		return fmt.Errorf("staging size: %w", err)
	}
	if required > l.reserved {
		extra := required - l.reserved
		if l.manager == nil {
			l.reserved = required
		} else if err := l.manager.extend(l.label, extra, required); err != nil {
			return err
		} else {
			l.reserved += extra
		}
	}
	l.used = required
	return nil
}

// reserveAdditional obtains a known allowance before an external process
// starts writing. It is used after probing reveals that a local, zero-duration
// input really is multiplexed and needs essence extraction. Local bytes are not
// staged, so the lease holds nothing while it waits and cannot participate in
// the partial-reservation deadlock described above.
func (l *stagingLease) reserveAdditional(ctx context.Context, bytes int64) error {
	if l == nil || bytes <= 0 {
		return nil
	}
	l.mu.Lock()
	if l.manager == nil {
		l.reserved += bytes
		l.mu.Unlock()
		return nil
	}
	currentReserved := l.reserved
	target, err := checkedAdd(currentReserved, bytes)
	manager := l.manager
	label := l.label
	l.mu.Unlock()
	if err != nil {
		return err
	}
	if target > manager.limit {
		return manager.capacityError(label, target, manager.availableCapacity())
	}

	for {
		manager.mu.Lock()
		budgetAvailable := manager.limit - manager.reserved
		wait := manager.capacityReleased
		manager.mu.Unlock()
		free, spaceErr := manager.spaceAvailable(manager.root)
		if spaceErr != nil {
			return fmt.Errorf("inspect free space in staging directory %q: %w", manager.root, spaceErr)
		}
		available := min(budgetAvailable, free)
		if bytes <= available {
			manager.mu.Lock()
			if bytes <= manager.limit-manager.reserved {
				manager.reserved += bytes
				manager.mu.Unlock()
				l.mu.Lock()
				l.reserved += bytes
				l.mu.Unlock()
				return nil
			}
			wait = manager.capacityReleased
			manager.mu.Unlock()
		}
		manager.mu.Lock()
		otherReservations := manager.reserved
		manager.mu.Unlock()
		if otherReservations <= currentReserved {
			return manager.capacityError(label, target, available)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait:
		}
	}
}

func (l *stagingLease) subtract(bytes int64) {
	if l == nil || bytes <= 0 {
		return
	}
	l.mu.Lock()
	l.used -= bytes
	if l.used < 0 {
		l.used = 0
	}
	l.mu.Unlock()
}

func (l *stagingLease) setArtifact(path string, size int64) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	previous := l.artifacts[path]
	l.mu.Unlock()
	if size > previous {
		if err := l.add(size - previous); err != nil {
			return err
		}
	} else if size < previous {
		l.subtract(previous - size)
	}
	l.mu.Lock()
	l.artifacts[path] = size
	l.mu.Unlock()
	return nil
}

func (l *stagingLease) removeArtifact(path string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	size := l.artifacts[path]
	delete(l.artifacts, path)
	l.mu.Unlock()
	l.subtract(size)
}

func (l *stagingLease) reservedHeadroom() int64 {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return max(l.reserved-l.used, 0)
}

func (l *stagingLease) release() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return
	}
	l.released = true
	reserved := l.reserved
	l.mu.Unlock()
	if l.manager != nil {
		l.manager.release(reserved)
	}
}

type stagingWriter struct {
	lease       *stagingLease
	destination io.Writer
}

func (w stagingWriter) Write(data []byte) (int, error) {
	if err := w.lease.add(int64(len(data))); err != nil {
		return 0, err
	}
	written, err := w.destination.Write(data)
	if written < len(data) {
		w.lease.subtract(int64(len(data) - written))
	}
	return written, err
}

func estimatedStagingRequirement(item source.Item, config Config) (required int64, unknown bool, err error) {
	if config.InputMode == InputStream {
		if config.DryRunMode == DryRunFast {
			return 0, false, nil
		}
		if item.Size >= rollingOutputWindowBytes {
			return rollingOutputWindowBytes, false, nil
		}
		allowance, err := percentageWithFloor(item.Size, segmentAllowancePercent, segmentAllowanceFloor)
		return min(allowance, rollingOutputWindowBytes), false, err
	}
	remote := item.LocalPath == ""
	dryRunMode := config.DryRunMode
	if dryRunMode == "" {
		dryRunMode = DryRunOff
	}
	renders := dryRunMode != DryRunFast
	// A local zero-duration input needs no temporary bytes when probing reveals
	// one essence. If it is multiplexed, ingestIndependently reserves the output
	// allowance after that fact is known and before FFmpeg starts. Remote inputs
	// reserve conservatively up front because they already hold a staged source;
	// several such partial reservations cannot safely wait for an upgrade.
	generated := renders && (config.SegmentDuration > 0 ||
		(remote && config.EssenceStorage == media.EssenceStorageIndependent))
	if !remote && !generated {
		return 0, false, nil
	}
	rolling := generated && config.SegmentDuration > 0
	if item.Size < 0 || (generated && len(config.FFmpegArgs) > 0 && !rolling) {
		return 0, true, nil
	}
	if remote {
		required = max(item.Size, 1)
	}
	if generated {
		allowance, allowanceErr := percentageWithFloor(item.Size, segmentAllowancePercent, segmentAllowanceFloor)
		if allowanceErr != nil {
			return 0, false, allowanceErr
		}
		if rolling {
			allowance = min(allowance, rollingOutputWindowBytes)
		}
		required, err = checkedAdd(required, allowance)
		if err != nil {
			return 0, false, err
		}
	}
	return required, false, nil
}

func percentageWithFloor(value int64, percent int64, floor int64) (int64, error) {
	if value < 0 || percent <= 0 {
		return 0, errors.New("invalid staging estimate")
	}
	if value > math.MaxInt64/percent {
		return 0, errors.New("staging estimate exceeds the supported byte range")
	}
	estimate := value * percent / 100
	minimum, err := checkedAdd(value, floor)
	if err != nil {
		return 0, err
	}
	return max(estimate, minimum), nil
}

func checkedAdd(left, right int64) (int64, error) {
	if left < 0 || right < 0 || left > math.MaxInt64-right {
		return 0, errors.New("staging estimate exceeds the supported byte range")
	}
	return left + right, nil
}

func directoryBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			// A rolling sink removes committed Segment files while the capacity
			// monitor walks the directory. Disappearing entries no longer consume
			// staging and are therefore safe to omit from this sample.
			if errors.Is(walkErr, os.ErrNotExist) && path != root {
				return nil
			}
			return walkErr
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		total, err = checkedAdd(total, info.Size())
		return err
	})
	return total, err
}

func formatBytes(bytes int64) string {
	const unit = int64(1024)
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	divisor := unit
	exponent := 0
	for value := bytes / unit; value >= unit && exponent < 5; value /= unit {
		divisor *= unit
		exponent++
	}
	return fmt.Sprintf("%.1f %ciB (%d bytes)", float64(bytes)/float64(divisor), "KMGTPE"[exponent], bytes)
}
