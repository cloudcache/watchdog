# Collector Fleet Rollout / Canary — Design

Status: **proposal** (design only; not implemented). Owner: platform. Tracks
tasklist P1 item "collector enrollment … fleet rollout/canary 完整闭环 → 余项:
fleet rollout/canary (preview/canary/rollback, copy old spec → higher version,
expiry/kill switch)".

## 1. Where we are (the gap)

The plan model today is strictly **per-collector**:

- `collector_plan_revisions` — one row per `(tenant, collector, config_version)`,
  status `validated → active → retired`, carrying `spec_json/spec_hash`,
  signature, `not_before/expires_at`, `supersedes_config_version`, and a
  `row_version` for optimistic concurrency. Repo: `CreateCollectorPlanRevision`,
  `ActivateCollectorPlanRevision` (guarded by `ExpectedCollectorRowVersion` +
  `ExpectedPlanRowVersion`), `AcknowledgeCollectorPlan`,
  `RecordCollectorPlanFailure`, `GetActiveCollectorPlan`.
- Delivery is collector-facing only: `GET /collectors/{id}/plan` (fetch my
  active plan) + `POST /collectors/{id}/plan-ack`. **There is no operator-facing
  plan-management API** — no way to create/validate/activate a revision, and no
  concept of applying one change across many collectors.
- `collector_agents` selection axes that already exist: `tenant_id`,
  `module_key`, `agent_type`, `status`, `config_version`, capabilities. There is
  **no** tags/labels column and no fleet/group entity.

So "fleet canary" is not a slice on top of the current model — it needs a new
rollout entity + state machine + an operator API. This document freezes that
shape so it can be built in independently-verifiable slices without the Flow
session having to guess the contract.

## 2. Principles

- **Reuse, don't fork.** A rollout *drives* per-collector `collector_plan_revisions`
  activation; it does not become a second source of plan truth. The collector
  delivery path (`GET /plan`) is unchanged — it always serves the collector's own
  active revision. The rollout is the thing that decides *when* each collector's
  new revision becomes active.
- **Per-collector safety stays.** Every activation still goes through
  `ActivateCollectorPlanRevision` with its row-version guards; the rollout never
  bypasses them.
- **Idempotent, resumable, killable.** A rollout is a long-running controlled
  process: it must survive restarts, never double-apply, and stop instantly.

## 3. Data model (new)

Two tables. Both tenant-scoped, both FK to `tenants`.

### `collector_plan_rollouts`
One row per rollout.

| column | notes |
|---|---|
| `id` CHAR(26) PK | |
| `tenant_id` | FK tenants |
| `module_key` | the module the plan spec is for |
| `selector_json` | fleet selection (see §4) — frozen at create time |
| `spec_json` / `spec_hash` | the desired new spec applied to every target |
| `plan_schema_version` | validated against each target's capability range |
| `strategy_json` | canary %, wave size, min-soak, failure budget (see §5) |
| `status` | `draft \| previewing \| canarying \| rolling \| paused \| completed \| rolled_back \| killed` |
| `expires_at` | rollout auto-pauses if not complete by this time |
| `created_by` / `updated_by` | |
| `row_version` BIGINT | optimistic concurrency on control ops |
| `created_at` / `updated_at` / `completed_at` | |

### `collector_plan_rollout_targets`
One row per (rollout, collector) — the per-collector progress ledger.

| column | notes |
|---|---|
| `rollout_id` + `collector_id` | composite PK; FK rollout (CASCADE), FK collector_agents |
| `wave` SMALLINT | which wave this target is in (0 = canary) |
| `config_version` | the revision created for this collector by the rollout |
| `prior_config_version` | what it was active on before — the rollback target |
| `status` | `pending \| revision_created \| activated \| acked \| failed \| reverted \| skipped` |
| `failure_reason` | populated on `failed` |
| `activated_at` / `acked_at` | fed by the existing ack path |
| `row_version` | |

`skipped` = a target that failed preview validation (schema/capability
mismatch) and is excluded from the rollout rather than blocking it.

## 4. Fleet selection

`selector_json` is a typed, frozen selector evaluated **once at create time**
into the target set (so the fleet can't drift mid-rollout). v1 axes, all
`AND`-combined, all already columns on `collector_agents`:

```
{ "module_key": "...", "agent_type": "...", "status": "active",
  "collector_ids": ["...", "..."]  // optional explicit allowlist/pin }
```

Deliberately **not** in v1: label/tag selectors (needs a new column + indexing —
a follow-up), dynamic re-evaluation.

## 5. Strategy & state machine

`strategy_json`:
```
{ "canary_count": 1,        // or canary_percent
  "wave_size": 10,          // collectors advanced per wave after canary
  "min_soak": "10m",        // healthy dwell before advancing
  "failure_budget": 0 }     // failed targets tolerated before auto-pause
```

**Rollout lifecycle** (operator- or scheduler-driven transitions):

```
draft ──preview──▶ previewing ──ok──▶ canarying ──advance──▶ rolling ──▶ completed
  │                    │                  │                     │
  └── (edit spec) ◀────┘             (pause/kill)          (pause/kill)
                                          │                     │
                                          ▼                     ▼
                                        paused ◀────────────── paused
                                          │
                                   (resume / rollback / kill)
```

- **preview**: for each selected collector, run the same validation the plan
  create path runs (schema range vs. the collector's declared capability range,
  spec well-formedness). Targets that fail → `skipped`. No revisions created,
  nothing activated. Result: "N will apply, M will be skipped, here's why."
- **canarying**: create + activate revisions for the canary subset only (wave 0),
  via `CreateCollectorPlanRevision` + `ActivateCollectorPlanRevision`. Watch acks
  (`AcknowledgeCollectorPlan`) and failures (`RecordCollectorPlanFailure`).
- **advance** (manual v1, scheduler later): once the current wave has soaked
  `min_soak` with failures ≤ `failure_budget`, create+activate the next
  `wave_size` targets. Repeat until all done → `completed`.
- **pause**: stop advancing; already-activated targets stay.
- **kill switch**: pause **and** revert every `activated`/`acked` target to its
  `prior_config_version` (re-activate the prior revision). Terminal-ish → `killed`.
- **rollback**: from `paused`/`completed`, revert all targets to
  `prior_config_version` → `rolled_back`.
- **expiry**: a background reaper (the existing maintenance reaper, §7) moves any
  non-terminal rollout past `expires_at` to `paused` and emits an audit event —
  never auto-advances or auto-reverts (safety: humans decide).

Every transition is guarded by the rollout `row_version` (If-Match style) so two
operators can't drive it concurrently, and writes an audit event.

## 6. API surface (operator-facing, new)

Plan management (the missing base layer):
- `POST /api/v1/collectors/{id}/plan-revisions` — create a draft revision from a
  spec (validate, sign). **Copy old spec → higher version** is this endpoint with
  `{ "from_config_version": N }`: clone that revision's spec into a new draft at
  `max(config_version)+1`.
- `GET /api/v1/collectors/{id}/plan-revisions` — list revisions (keyset paged).
- `POST /api/v1/collectors/{id}/plan-revisions/{v}/activate` — activate one
  (single-collector path; wraps `ActivateCollectorPlanRevision`).

Rollouts:
- `POST /api/v1/plan-rollouts` — create (module, selector, spec, strategy, expiry) → `draft`.
- `POST /api/v1/plan-rollouts/{id}/preview` — run §5 preview.
- `GET /api/v1/plan-rollouts` / `GET /api/v1/plan-rollouts/{id}` — status + per-target progress (paged).
- `POST /api/v1/plan-rollouts/{id}/advance|pause|resume|rollback|kill` — control, `If-Match` on rollout row_version.

Permissions: rollout create/control require collector `configure`/`admin` on the
tenant; preview/get require `view`. Reuses the existing permission model.

## 7. Integration with what exists

- Revisions are still created/activated through the existing repo methods and
  row-version guards — the rollout only orchestrates them.
- Ack/failure already flow in via `AcknowledgeCollectorPlan` /
  `RecordCollectorPlanFailure`; the rollout controller reads them (by
  `(collector, config_version)`) to update `collector_plan_rollout_targets`.
- Delivery (`GET /plan`) is untouched.
- Expiry uses the existing `PeriodicMaintenance` reaper (add a `plan_rollouts`
  task) — not the operation-jobs lease machinery, matching the other reapers.
- Advancing waves on a schedule (phase 3) should ride the **operation-jobs**
  runtime (leased, resumable, checkpointed) rather than a bespoke loop —
  depends on PLAT-04G (job progress/checkpoint) landing first, so v1 keeps
  advance **manual**.

## 8. Phasing (independently shippable slices)

1. **Plan-revision management API** — create (incl. clone `from_config_version`),
   list, activate. Pure reuse of existing repo methods + a new operator API +
   gated MySQL/API tests. *Unblocks everything and is useful on its own.*
2. **Rollout tables + create + preview** — migration for the two tables, create
   endpoint, preview (validation-only, no activation). Gated MySQL for selection
   + preview skip/apply classification.
3. **Canary + manual advance + status** — activate wave 0, read acks/failures,
   advance/pause, per-target ledger, status endpoint + frontend rollout view.
4. **Kill switch + rollback + expiry reaper** — revert semantics + the reaper task.
5. **(later) Scheduler-driven advance** — once PLAT-04G lands; label selectors.

## 9. Open decisions (need a call before building past phase 1)

- **Canary health signal**: ack + absence-of-failure only, or also require a
  fresh healthy heartbeat / a metrics threshold before advancing?
- **Selector v1**: is module_key + agent_type + status + explicit list enough, or
  are labels needed on day one (adds a column + management UI)?
- **Config_version namespace**: rollout-created revisions share the collector's
  single `config_version` sequence — confirm that's fine vs. a separate lane.
- **Kill revert atomicity**: best-effort per-target revert (some may already be
  offline) — is "reverted where reachable, flagged where not" acceptable?
