package flowcollect

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const signedPlanMaxBytes = 8 << 20

type planHistoryEntry struct {
	registry *Registry
	payload  []byte
}

type PlanHistory struct {
	mu        sync.RWMutex
	planPath  string
	publicKey []byte
	dir       string
	max       int
	entries   map[uint64]planHistoryEntry
	active    *Registry
	usedLKG   bool
}

type PlanRefreshResult struct {
	Changed  bool
	Revision uint64
	Pruned   int
}

func OpenPlanHistory(planPath, publicKeyPath, dir string, maxEntries int, now time.Time) (*PlanHistory, error) {
	planPath = filepath.Clean(strings.TrimSpace(planPath))
	publicKeyPath = filepath.Clean(strings.TrimSpace(publicKeyPath))
	dir = filepath.Clean(strings.TrimSpace(dir))
	if planPath == "." || publicKeyPath == "." || dir == "." || maxEntries <= 0 || now.IsZero() {
		return nil, errors.New("plan path, public key, history dir, max entries, and current time are required")
	}
	publicKey, err := readBoundedFile(publicKeyPath, 64<<10)
	if err != nil {
		return nil, fmt.Errorf("read flow plan public key: %w", err)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create flow plan history dir: %w", err)
	}
	history := &PlanHistory{planPath: planPath, publicKey: bytes.Clone(publicKey), dir: dir, max: maxEntries, entries: make(map[uint64]planHistoryEntry)}
	if err := history.load(publicKey); err != nil {
		return nil, err
	}
	activeEnvelope, activeReadErr := readBoundedFile(planPath, signedPlanMaxBytes)
	if activeReadErr == nil {
		verified, verifyErr := verifySignedPlanPayload(activeEnvelope, publicKey)
		if verifyErr == nil {
			registry, compileErr := CompilePlan(verified.plan, now)
			if compileErr == nil {
				if err := history.activate(activeEnvelope, verified, registry); err != nil {
					return nil, err
				}
				return history, nil
			}
			activeReadErr = compileErr
		} else {
			activeReadErr = verifyErr
		}
	}
	registry := history.latestActive(now)
	if registry == nil {
		return nil, fmt.Errorf("load active flow plan and LKG fallback: %w", activeReadErr)
	}
	history.active = registry
	history.usedLKG = true
	return history, nil
}

func (h *PlanHistory) load(publicKey []byte) error {
	entries, err := os.ReadDir(h.dir)
	if err != nil {
		return fmt.Errorf("read flow plan history dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".plan") {
			names = append(names, entry.Name())
		}
	}
	if len(names) > h.max+1 {
		return fmt.Errorf("flow plan history has %d entries, startup overflow limit is %d", len(names), h.max+1)
	}
	sort.Strings(names)
	collectorID := ""
	for _, name := range names {
		revision, err := planHistoryRevision(name)
		if err != nil {
			return err
		}
		envelope, err := readBoundedFile(filepath.Join(h.dir, name), signedPlanMaxBytes)
		if err != nil {
			return fmt.Errorf("read flow plan history %s: %w", name, err)
		}
		verified, err := verifySignedPlanPayload(envelope, publicKey)
		if err != nil {
			return fmt.Errorf("verify flow plan history %s: %w", name, err)
		}
		if verified.plan.Revision != revision {
			return fmt.Errorf("flow plan history %s revision does not match payload", name)
		}
		registry, err := compileHistoricalPlan(verified.plan)
		if err != nil {
			return fmt.Errorf("compile flow plan history %s: %w", name, err)
		}
		if collectorID == "" {
			collectorID = registry.Plan().CollectorID
		} else if registry.Plan().CollectorID != collectorID {
			return fmt.Errorf("flow plan history %s changes collector identity", name)
		}
		h.entries[revision] = planHistoryEntry{registry: registry, payload: bytes.Clone(verified.payload)}
	}
	return nil
}

func (h *PlanHistory) activate(envelope []byte, verified verifiedSignedPlan, registry *Registry) error {
	revision := registry.Plan().Revision
	for cachedRevision, entry := range h.entries {
		if entry.registry.Plan().CollectorID != registry.Plan().CollectorID {
			return errors.New("active flow plan changes collector identity")
		}
		if cachedRevision > revision {
			return fmt.Errorf("active flow plan revision %d is older than cached revision %d", revision, cachedRevision)
		}
	}
	if existing, ok := h.entries[revision]; ok {
		if !bytes.Equal(existing.payload, verified.payload) {
			return fmt.Errorf("flow plan revision %d payload is immutable", revision)
		}
	} else {
		if len(h.entries) >= h.max+1 {
			return fmt.Errorf("flow plan history startup overflow limit %d reached", h.max+1)
		}
		if err := h.persist(revision, envelope); err != nil {
			return err
		}
		h.entries[revision] = planHistoryEntry{registry: registry, payload: bytes.Clone(verified.payload)}
	}
	h.active = registry
	return nil
}

func (h *PlanHistory) persist(revision uint64, envelope []byte) error {
	if len(envelope) == 0 || len(envelope) > signedPlanMaxBytes {
		return errors.New("signed flow plan exceeds history size limit")
	}
	temporary, err := os.CreateTemp(h.dir, ".plan-*.tmp")
	if err != nil {
		return fmt.Errorf("create flow plan history temp file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o640); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("chmod flow plan history temp file: %w", err)
	}
	if err := writeFull(temporary, envelope); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write flow plan history: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync flow plan history: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close flow plan history: %w", err)
	}
	destination := filepath.Join(h.dir, fmt.Sprintf("%020d.plan", revision))
	if err := os.Rename(temporaryPath, destination); err != nil {
		return fmt.Errorf("install flow plan history: %w", err)
	}
	directory, err := os.Open(h.dir)
	if err != nil {
		return fmt.Errorf("open flow plan history dir: %w", err)
	}
	err = errors.Join(directory.Sync(), directory.Close())
	if err != nil {
		return fmt.Errorf("sync flow plan history dir: %w", err)
	}
	return nil
}

func (h *PlanHistory) latestActive(now time.Time) *Registry {
	h.mu.RLock()
	defer h.mu.RUnlock()
	revisions := make([]uint64, 0, len(h.entries))
	for revision := range h.entries {
		revisions = append(revisions, revision)
	}
	sort.Slice(revisions, func(i, j int) bool { return revisions[i] < revisions[j] })
	for index := len(revisions) - 1; index >= 0; index-- {
		entry := h.entries[revisions[index]]
		registry, err := CompilePlan(entry.registry.Plan(), now)
		if err == nil {
			return registry
		}
	}
	return nil
}

func (h *PlanHistory) Active() *Registry {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.active
}

func (h *PlanHistory) RevalidateActive(now time.Time) (*Registry, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.active == nil {
		return nil, errors.New("active flow plan is unavailable")
	}
	registry, err := CompilePlan(h.active.Plan(), now)
	if err != nil {
		return nil, err
	}
	h.active = registry
	entry := h.entries[registry.Plan().Revision]
	entry.registry = registry
	h.entries[registry.Plan().Revision] = entry
	return registry, nil
}

func (h *PlanHistory) Resolve(revision uint64) (*Registry, bool) {
	if h == nil || revision == 0 {
		return nil, false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	entry, ok := h.entries[revision]
	return entry.registry, ok
}

func (h *PlanHistory) Revisions() []uint64 {
	if h == nil {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	revisions := make([]uint64, 0, len(h.entries))
	for revision := range h.entries {
		revisions = append(revisions, revision)
	}
	sort.Slice(revisions, func(i, j int) bool { return revisions[i] < revisions[j] })
	return revisions
}

func (h *PlanHistory) UsedLKG() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.usedLKG
}

// Refresh reads one exact signed-plan envelope from the configured delivery
// path, verifies and compiles it, validates external dependencies without
// holding the history lock, then makes the envelope durable before switching
// the active registry. The previous active revision is retained for this
// transition so datagrams admitted immediately before the caller swaps its
// data-plane pointer always remain replayable.
func (h *PlanHistory) Refresh(now time.Time, references func() (map[uint64]struct{}, error), validate func([]*Registry) error) (PlanRefreshResult, error) {
	if h == nil || now.IsZero() {
		return PlanRefreshResult{}, errors.New("flow plan history and current time are required")
	}
	if validate == nil {
		return PlanRefreshResult{}, errors.New("flow plan refresh validator is required")
	}
	h.mu.RLock()
	planPath := h.planPath
	publicKey := bytes.Clone(h.publicKey)
	h.mu.RUnlock()
	if planPath == "" || len(publicKey) == 0 {
		return PlanRefreshResult{}, errors.New("flow plan refresh source is unavailable")
	}
	envelope, err := readBoundedFile(planPath, signedPlanMaxBytes)
	if err != nil {
		return PlanRefreshResult{}, newPlanRefreshFailure("persist", "PLAN_FILE_READ_FAILED", "delivered plan file could not be read", fmt.Errorf("read refreshed flow plan: %w", err))
	}
	verified, err := verifySignedPlanPayload(envelope, publicKey)
	if err != nil {
		return PlanRefreshResult{}, newPlanRefreshFailure("verify", "PLAN_SIGNATURE_INVALID", "delivered plan signature or metadata is invalid", fmt.Errorf("verify refreshed flow plan: %w", err))
	}
	registry, err := CompilePlan(verified.plan, now)
	if err != nil {
		return PlanRefreshResult{}, newPlanRefreshFailure("compatibility", "PLAN_SCHEMA_INCOMPATIBLE", "delivered plan is incompatible with this collector", fmt.Errorf("compile refreshed flow plan: %w", err))
	}

	h.mu.RLock()
	changed, registries, err := h.prepareRefreshLocked(verified, registry, nil)
	h.mu.RUnlock()
	if err != nil {
		return PlanRefreshResult{}, newPlanRefreshFailure("compatibility", "PLAN_ACTIVATION_REJECTED", "plan could not be activated by this collector", err)
	}
	var referenced map[uint64]struct{}
	if changed {
		if references == nil {
			return PlanRefreshResult{}, errors.New("flow plan refresh WAL reference provider is required")
		}
		referenced, err = references()
		if err != nil {
			return PlanRefreshResult{}, newPlanRefreshFailure("persist", "WAL_REFERENCE_READ_FAILED", "pending WAL plan references could not be read", fmt.Errorf("read pending WAL plan references: %w", err))
		}
		h.mu.RLock()
		changed, registries, err = h.prepareRefreshLocked(verified, registry, referenced)
		h.mu.RUnlock()
		if err != nil {
			return PlanRefreshResult{}, newPlanRefreshFailure("compatibility", "PLAN_ACTIVATION_REJECTED", "plan could not be activated by this collector", err)
		}
		if err := validate(registries); err != nil {
			return PlanRefreshResult{}, newPlanRefreshFailure("dependency", "KAFKA_CONTRACT", "plan dependencies are unavailable", fmt.Errorf("validate refreshed flow plan dependencies: %w", err))
		}
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	changed, _, err = h.prepareRefreshLocked(verified, registry, referenced)
	if err != nil {
		return PlanRefreshResult{}, newPlanRefreshFailure("compatibility", "PLAN_ACTIVATION_REJECTED", "plan could not be activated by this collector", err)
	}
	if !changed {
		h.active = registry
		entry := h.entries[registry.Plan().Revision]
		entry.registry = registry
		h.entries[registry.Plan().Revision] = entry
		h.usedLKG = false
		return PlanRefreshResult{Revision: registry.Plan().Revision}, nil
	}
	pruned, err := h.installRefreshLocked(envelope, verified, registry, referenced)
	if err != nil {
		return PlanRefreshResult{}, newPlanRefreshFailure("persist", "PLAN_HISTORY_PERSIST_FAILED", "plan runtime state could not be made durable", err)
	}
	return PlanRefreshResult{Changed: changed, Revision: registry.Plan().Revision, Pruned: pruned}, nil
}

func (h *PlanHistory) prepareRefreshLocked(verified verifiedSignedPlan, registry *Registry, referenced map[uint64]struct{}) (bool, []*Registry, error) {
	if h.active == nil || registry == nil {
		return false, nil, errors.New("active and refreshed flow plans are required")
	}
	revision := registry.Plan().Revision
	collectorID := registry.Plan().CollectorID
	for cachedRevision, entry := range h.entries {
		if entry.registry.Plan().CollectorID != collectorID {
			return false, nil, errors.New("refreshed flow plan changes collector identity")
		}
		if cachedRevision > revision {
			return false, nil, fmt.Errorf("refreshed flow plan revision %d is older than cached revision %d", revision, cachedRevision)
		}
	}
	if h.active.Plan().CollectorID != collectorID {
		return false, nil, errors.New("refreshed flow plan changes active collector identity")
	}
	changed := h.active.Plan().Revision != revision
	if existing, ok := h.entries[revision]; ok {
		if !bytes.Equal(existing.payload, verified.payload) {
			return false, nil, fmt.Errorf("flow plan revision %d payload is immutable", revision)
		}
	} else {
		changed = true
	}
	retained, err := h.refreshRetainedLocked(revision, referenced)
	if err != nil {
		return false, nil, err
	}
	if len(retained) > h.max {
		return false, nil, fmt.Errorf("flow plan refresh requires %d retained revisions, history limit is %d", len(retained), h.max)
	}
	registries := make([]*Registry, 0, len(h.entries)+1)
	for _, entry := range h.entries {
		registries = append(registries, entry.registry)
	}
	if _, exists := h.entries[revision]; !exists {
		registries = append(registries, registry)
	}
	return changed, registries, nil
}

func (h *PlanHistory) refreshRetainedLocked(candidateRevision uint64, referenced map[uint64]struct{}) (map[uint64]struct{}, error) {
	retained := make(map[uint64]struct{}, len(referenced)+3)
	retained[candidateRevision] = struct{}{}
	retained[h.active.Plan().Revision] = struct{}{}
	highestRevision := candidateRevision
	for revision := range h.entries {
		if revision > highestRevision {
			highestRevision = revision
		}
	}
	retained[highestRevision] = struct{}{}
	for revision := range referenced {
		if revision == 0 {
			return nil, errors.New("pending WAL references plan revision zero")
		}
		if revision != candidateRevision {
			if _, exists := h.entries[revision]; !exists {
				return nil, fmt.Errorf("pending WAL references unavailable plan revision %d", revision)
			}
		}
		retained[revision] = struct{}{}
	}
	return retained, nil
}

func (h *PlanHistory) installRefreshLocked(envelope []byte, verified verifiedSignedPlan, registry *Registry, referenced map[uint64]struct{}) (int, error) {
	revision := registry.Plan().Revision
	if _, exists := h.entries[revision]; !exists {
		if len(h.entries) >= h.max+1 {
			return 0, fmt.Errorf("flow plan history overflow limit %d reached", h.max+1)
		}
		if err := h.persist(revision, envelope); err != nil {
			return 0, err
		}
		h.entries[revision] = planHistoryEntry{registry: registry, payload: bytes.Clone(verified.payload)}
	}
	retained, err := h.refreshRetainedLocked(revision, referenced)
	if err != nil {
		return 0, err
	}
	victims := make([]uint64, 0, len(h.entries))
	for cachedRevision := range h.entries {
		if _, keep := retained[cachedRevision]; !keep {
			victims = append(victims, cachedRevision)
		}
	}
	sort.Slice(victims, func(i, j int) bool { return victims[i] < victims[j] })
	for _, victim := range victims {
		if err := os.Remove(filepath.Join(h.dir, fmt.Sprintf("%020d.plan", victim))); err != nil {
			return len(victims), fmt.Errorf("remove unreferenced flow plan revision %d: %w", victim, err)
		}
		delete(h.entries, victim)
	}
	if len(victims) > 0 {
		directory, err := os.Open(h.dir)
		if err != nil {
			return len(victims), fmt.Errorf("open flow plan history dir after refresh: %w", err)
		}
		if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
			return len(victims), fmt.Errorf("sync flow plan history refresh: %w", err)
		}
	}
	h.active = registry
	entry := h.entries[revision]
	entry.registry = registry
	h.entries[revision] = entry
	h.usedLKG = false
	return len(victims), nil
}

// Prune retains the active revision, the highest revision as an anti-rollback
// watermark, and every revision referenced by pending WAL. OpenPlanHistory
// permits one temporary entry above max so a newly signed active plan can be
// made durable before this recovery-time reconciliation.
func (h *PlanHistory) Prune(referenced map[uint64]struct{}) (int, error) {
	if h == nil {
		return 0, errors.New("flow plan history is required")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.active == nil {
		return 0, errors.New("active flow plan is unavailable")
	}
	activeRevision := h.active.Plan().Revision
	if _, exists := h.entries[activeRevision]; !exists {
		return 0, fmt.Errorf("active flow plan revision %d is absent from history", activeRevision)
	}
	retained := make(map[uint64]struct{}, len(referenced)+2)
	retained[activeRevision] = struct{}{}
	highestRevision := uint64(0)
	for revision := range h.entries {
		if revision > highestRevision {
			highestRevision = revision
		}
	}
	retained[highestRevision] = struct{}{}
	for revision := range referenced {
		if revision == 0 {
			return 0, errors.New("pending WAL references plan revision zero")
		}
		if _, exists := h.entries[revision]; !exists {
			return 0, fmt.Errorf("pending WAL references unavailable plan revision %d", revision)
		}
		retained[revision] = struct{}{}
	}
	if len(retained) > h.max {
		return 0, fmt.Errorf("pending WAL requires %d plan revisions, history limit is %d", len(retained), h.max)
	}
	victims := make([]uint64, 0, len(h.entries)-len(retained))
	for revision := range h.entries {
		if _, keep := retained[revision]; !keep {
			victims = append(victims, revision)
		}
	}
	sort.Slice(victims, func(i, j int) bool { return victims[i] < victims[j] })
	for _, revision := range victims {
		path := filepath.Join(h.dir, fmt.Sprintf("%020d.plan", revision))
		if err := os.Remove(path); err != nil {
			return 0, fmt.Errorf("remove unreferenced flow plan revision %d: %w", revision, err)
		}
		delete(h.entries, revision)
	}
	if len(victims) > 0 {
		directory, err := os.Open(h.dir)
		if err != nil {
			return 0, fmt.Errorf("open flow plan history dir after prune: %w", err)
		}
		if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
			return 0, fmt.Errorf("sync flow plan history prune: %w", err)
		}
	}
	return len(victims), nil
}

func planHistoryRevision(name string) (uint64, error) {
	if len(name) != 25 || !strings.HasSuffix(name, ".plan") {
		return 0, fmt.Errorf("invalid flow plan history file name %q", name)
	}
	revision, err := strconv.ParseUint(strings.TrimSuffix(name, ".plan"), 10, 64)
	if err != nil || revision == 0 {
		return 0, fmt.Errorf("invalid flow plan history file name %q", name)
	}
	return revision, nil
}

func readBoundedFile(path string, maxBytes int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() <= 0 || info.Size() > maxBytes {
		return nil, fmt.Errorf("file size %d is outside 1..%d", info.Size(), maxBytes)
	}
	data := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, err
	}
	return data, nil
}
