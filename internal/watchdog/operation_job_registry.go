package watchdog

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// PLAT-04B: a controlled registry of operation-job handlers, one per job type,
// plus a scheduler that launches a bounded worker pool per type. Modules
// register their handler with an explicit concurrency budget instead of the
// runtime hardcoding a single worker, so no second job state machine has to
// be built elsewhere and each type's fan-out is capped.

type OperationJobRegistration struct {
	JobType     string
	Handler     OperationJobHandler
	Concurrency int           // workers for this type; default 1
	LeaseFor    time.Duration // default 30s
	MaxAttempts uint32        // default 5
	RetryBase   time.Duration // default 30s
}

type OperationJobHandlerRegistry struct {
	mu      sync.Mutex
	entries map[string]OperationJobRegistration
}

func NewOperationJobHandlerRegistry() *OperationJobHandlerRegistry {
	return &OperationJobHandlerRegistry{entries: map[string]OperationJobRegistration{}}
}

// Register adds a handler for a job type. Duplicate types and empty handlers
// are rejected so a misconfiguration fails at startup rather than silently
// dropping jobs. Concurrency is clamped to at least 1.
func (r *OperationJobHandlerRegistry) Register(reg OperationJobRegistration) error {
	if r == nil {
		return errors.New("operation job registry is nil")
	}
	if reg.JobType == "" || reg.Handler == nil {
		return errors.New("operation job registration requires a job type and handler")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[reg.JobType]; exists {
		return fmt.Errorf("operation job type %q is already registered", reg.JobType)
	}
	if reg.Concurrency < 1 {
		reg.Concurrency = 1
	}
	r.entries[reg.JobType] = reg
	return nil
}

func (r *OperationJobHandlerRegistry) registrations() []OperationJobRegistration {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]OperationJobRegistration, 0, len(r.entries))
	for _, entry := range r.entries {
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].JobType < out[j].JobType })
	return out
}

// StartOperationJobScheduler launches, per registered type, `Concurrency`
// workers that share the type's lease/retry policy. Each worker gets a
// distinct owner so lease ownership is unambiguous across the pool. It returns
// the number of workers started. Workers stop when ctx is done.
func StartOperationJobScheduler(ctx context.Context, repo OperationJobRepository, registry *OperationJobHandlerRegistry, ownerBase string, logf func(string, ...any)) int {
	if repo == nil || registry == nil {
		return 0
	}
	if ownerBase == "" {
		ownerBase = "watchdog-hub"
	}
	started := 0
	for _, reg := range registry.registrations() {
		for i := 0; i < reg.Concurrency; i++ {
			worker := &OperationJobWorker{
				Repo:        repo,
				JobType:     reg.JobType,
				Owner:       fmt.Sprintf("%s/%s/%d", ownerBase, reg.JobType, i),
				Handler:     reg.Handler,
				LeaseFor:    reg.LeaseFor,
				MaxAttempts: reg.MaxAttempts,
				RetryBase:   reg.RetryBase,
				Logf:        logf,
			}
			go worker.Run(ctx)
			started++
		}
	}
	return started
}
