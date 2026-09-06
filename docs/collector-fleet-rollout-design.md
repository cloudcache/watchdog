# Collector Fleet Rollout / Canary — Design

Status: **Phases 0–2 and 3A implemented; Phases 3B–5 proposed**. Owner: platform. Tracks
tasklist P1 item "collector enrollment … fleet rollout/canary 完整闭环".

## 1. Where we are (the gap)

The plan model today is strictly **per-collector**:

- `collector_plan_revisions` — one row per `(tenant, collector, config_version)`,
  status `validated → active → retired`, carrying `spec_json/spec_hash`,
  signature, `not_before/expires_at`, `supersedes_config_version`, and a
  `row_version` for optimistic concurrency. Repo: `CreateCollectorPlanRevision`,
  `ActivateCollectorPlanRevision` (guarded by `ExpectedCollectorRowVersion` +
  `ExpectedPlanRowVersion`), `AcknowledgeCollectorPlan`,
  `RecordCollectorPlanFailure`, `GetActiveCollectorPlan`.
- Delivery remains collector-facing: `GET /collectors/{id}/plan` (fetch my
  active plan) + `POST /collectors/{id}/plan-ack`. Phase 1 added the independent
  operator create/list/activate API; Phase 2 adds fleet rollout create/preview
  without changing machine delivery or activating anything during preview.
- The repository deliberately accepts only a revision carrying an internal
  proof produced after Ed25519 verification. Phase 0 now supplies the runtime
  signer and trust-key registry; the remaining gap is the operator-facing API
  that validates input and invokes that signer. An HTTP handler must never
  construct the proof itself, accept a client public key as a trust root, or
  place private key material in `collector_plan_revisions`.
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

### 2.1 Phase 0 signer/trust contract (implemented)

- Scope is platform-global, while every signed plan payload still binds
  `tenant_id + collector_id + config_version + spec_hash + validity window`.
  A key therefore cannot move a signed plan between tenants or collectors.
- Deployment config is an atomic pair:
  `collector_plan_signing.key_id` and `private_key_file` (or
  `WATCHDOG_COLLECTOR_PLAN_SIGNING_KEY_ID` and
  `WATCHDOG_COLLECTOR_PLAN_SIGNING_PRIVATE_KEY_FILE`). Empty disables plan
  creation without breaking existing plan delivery. The Ed25519 private key
  file must be regular and owner-only; only the signing capability is exposed
  through `CollectorPlanSigner`.
- Migration `052_collector_plan_trust_keys.sql` adds two platform tables:
  `collector_plan_signing_keys` is the immutable public-key/lifecycle ledger
  (`active | retiring | revoked`, one active key, public material never reused),
  and singleton `collector_plan_trust_state` stores canonical bundle JSON,
  checksum and monotonically increasing generation. No private key, secret
  reference, tenant credential or plan payload is stored in either table.
- Startup atomically activates the configured public key. A new key retires the
  previous active key through `MAX(expires_at)` of its still-startable
  `validated|active` plans; with no such plan it is revoked immediately. A
  background `PeriodicMaintenance` task compacts expired retiring keys to
  revoked. Rolling back a deployment to a used/retiring/revoked key ID fails
  closed; rollback requires a fresh key ID.
- Authenticated collectors fetch
  `GET /api/v1/collectors/{collector_id}/trust-bundle`. The exact canonical JSON
  is returned with `ETag`, `X-Watchdog-Trust-Generation` and checksum; identical
  `If-None-Match` returns 304. Collector `flowplan.TrustStore` atomically accepts
  a higher generation, permits identical replay, rejects rollback/conflict and
  refuses a retiring key at `trust_until` even if a stale bundle remains cached.
- Key changes do not rewrite historical plans. New signing checks the key is
  still active and its public material still matches MySQL; a concurrent rotate
  or revoke therefore fences an already-running signer before it can create a
  new revision.

## 3. Data model (new)

Two tables. Both tenant-scoped, both FK to `tenants`.

### `collector_plan_rollouts`
One row per rollout.

| column | notes |
|---|---|
| `id` CHAR(26) PK | |
| `tenant_id` | FK tenants |
| `module_key` | the module the plan spec is for |
| `rollout_schema_version` | server-owned rollout contract version; v1 is `1` |
| `selector_json` | fleet selection (see §4) — frozen at create time |
| `spec_json` / `spec_hash` | the desired new spec applied to every target |
| `plan_schema_version` | validated against each target's capability range |
| `strategy_json` | canary %, wave size, min-soak, failure budget (see §5) |
| `status` | `draft \| previewed \| canarying \| rolling \| paused \| completed \| rolled_back \| killed` |
| `expires_at` | rollout auto-pauses if not complete by this time |
| `created_by` / `updated_by` | |
| `row_version` BIGINT | optimistic concurrency on control ops |
| `created_at` / `updated_at` / `completed_at` | |

### `collector_plan_rollout_targets`
One row per (rollout, collector) — the per-collector progress ledger.

| column | notes |
|---|---|
| `rollout_id` + `collector_id` | composite PK; FK rollout (CASCADE), FK collector_agents |
| `wave` INT UNSIGNED | which wave this target is in (0 = canary); `4294967295` is reserved for skipped targets |
| `config_version` | the revision created for this collector by the rollout |
| `prior_config_version` | what it was active on before — the rollback target |
| `status` | `pending \| revision_created \| activated \| acked \| failed \| reverted \| skipped` |
| `failure_reason` | populated on `failed` |
| `activated_at` / `acked_at` | fed by the existing ack path |
| `row_version` | |

`skipped` = a target that failed preview validation (schema/capability
mismatch) and is excluded from the rollout rather than blocking it.

## 4. Fleet selection

`selector_json` is typed and frozen at create time, then evaluated **once by
preview** into the durable target ledger (so the fleet cannot drift after an
operator has reviewed the preview). v1 axes, all
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
{ "canary_count": 1,
  "wave_size": 10,          // collectors advanced per wave after canary
  "min_soak_seconds": 600,  // healthy dwell before advancing; 1s..7d
  "failure_budget": 0 }     // failed targets tolerated before auto-pause
```

`canary_count` and `wave_size` are 1..10,000. `failure_budget` is an absolute
per-wave count and must be smaller than both values, so an all-failed canary or
wave can never qualify to advance.

**Rollout lifecycle** (operator- or scheduler-driven transitions):

```
draft ──preview──▶ previewed ──start──▶ canarying ──advance──▶ rolling ──▶ completed
  │                    │                  │                     │
  └── (edit spec) ◀────┘             (pause/kill)          (pause/kill)
                                          │                     │
                                          ▼                     ▼
                                        paused ◀────────────── paused
                                          │
                                   (resume / rollback / kill)
```

- **preview**: in one transaction, materialize the tenant/module/type/status/ID
  intersection in deterministic collector-ID order and run the same validation the plan
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

Plan management (implemented in Phase 1):
- `POST /api/v1/collectors/{id}/plan-revisions` — validate and sign a new
  immutable `validated` revision. The body is exactly one of
  `{plan_schema_version,spec,expires_at,not_before?}` or
  `{from_config_version,expires_at,not_before?}`. Clone copies only the source
  spec and schema; it receives a new ID, signature and
  `max(existing config_version, collector head)+1`. Tenant, collector, actor,
  status, hashes, signing key and signature are server-derived; unknown fields
  are rejected and the response deliberately omits spec/signature material.
- `GET /api/v1/collectors/{id}/plan-revisions?limit=&cursor=` — revisions newest
  first with an opaque `config_version` keyset cursor; limit is 1–200.
- `POST /api/v1/collectors/{id}/plan-revisions/{v}/activate` — activate one
  revision. `If-Match` carries the quoted plan `row_version`; body
  `{collector_row_version}` supplies the independently-read collector guard.
  A stale guard is 412; an invalid lifecycle/schema/time transition is 409.

Creation is serialized by the collector row lock. The active signing-key row is
then locked and checked against the runtime signer before the signature and
insert occur, so concurrent creates allocate distinct versions and a concurrent
key rotation/revocation cannot race a now-retiring key into storage. This phase
reuses `collector_agents`, `collector_plan_revisions`, `audit_logs` and the Phase
0 key tables; it intentionally creates no migration and leaves machine plan
delivery unchanged.

Rollouts (Phase 2 implemented):
- `POST /api/v1/plan-rollouts` — create strict
  `{selector,plan_schema_version,spec,strategy,expires_at}` → `draft`; tenant,
  actor, status, IDs and all hashes are server-derived. The response deliberately
  omits `spec`, returning only its hash plus normalized selector/strategy.
- `POST /api/v1/plan-rollouts/{id}/preview` — run §5 preview, requires quoted
  rollout `row_version` in `If-Match`, returns matched/eligible/skipped/wave
  counts and advances the rollout row to `previewed` without creating a plan
  revision or changing any collector head.

Rollouts (Phase 3A implemented):
- `GET /api/v1/plan-rollouts` — tenant-scoped offset paging (`limit` 1–200,
  bounded `offset`), text/module/status filters and a fixed sort allowlist. The
  response omits the plan spec and returns its hash only.
- `GET /api/v1/plan-rollouts/{id}` — rollout metadata plus counts for matched,
  eligible and every target lifecycle state; returns the rollout `row_version`
  as `ETag`.
- `GET /api/v1/plan-rollouts/{id}/targets` — server-side paging/search and
  status/current-health/wave filters, including `canary` and `skipped` wave
  aliases. Each row joins the target ledger to the collector's current name,
  type, health, last-seen and config-version facts; the immutable historical
  `prior_config_version` remains sourced from the ledger.

Rollouts (Phase 3B+ planned):
- `POST /api/v1/plan-rollouts/{id}/advance|pause|resume|rollback|kill` — control, `If-Match` on rollout row_version.

Permissions: create requires tenant `configure`; preview and future lifecycle
controls require tenant `operate`; read/list endpoints require tenant `view`.
Reuses the existing permission model.

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

0. **[done] Control-plane signer and trust-key lifecycle** — resolve an active signing
   key by stable key ID from a secret reference, keep public keys in
   active/retiring/revoked states, publish a monotonically-versioned agent trust
   bundle with overlap at least as long as the longest still-startable plan, and
   expose only a signer interface to plan creation. Private key bytes never enter
   MySQL plan rows, API DTOs, logs, or audit detail. Real tests cover rotation,
   retiring overlap, revoked-key rejection, missing key fail-closed and agent
   bundle generation rollback rejection.
1. **[done] Plan-revision management API** — create (including clone
   `from_config_version`), keyset list and dual-guard activate. The runtime
   signer is the only signing capability; version allocation and active-key
   fencing occur in the repository transaction. API/unit tests plus an isolated
   real-MySQL concurrent create→clone→list→activate test are the delivery gate.
2. **[done] Rollout tables + create + preview** — migration 053 owns the two
   tables; strict create and optimistic-lock preview are wired into production.
   Real MySQL tests cover selector intersection, deterministic canary/waves,
   schema-incompatible skip, empty-result rollback, stale replay, no revision or
   collector-head mutation, audit, migration replay and fresh-install parity.
3A. **[done] Read-only rollout observability** — rollout list/detail and
   per-target list provide tenant-scoped bounded paging, allowlisted
   filter/sort fields, lifecycle summaries and current collector health without
   returning `spec_json`. This slice reuses migration 053 tables and their
   indexes, so it intentionally creates no empty migration. Unit/API tests and
   isolated real-MySQL paging/filter/summary/tenant-isolation scenarios are the
   delivery gate.
3B. **Canary + manual advance** — create/activate wave 0 revisions, reconcile
   acks/failures, then advance/pause under optimistic locking; frontend rollout
   view consumes the Phase 3A read contract.
4. **Kill switch + rollback + expiry reaper** — revert semantics + the reaper task.
5. **(later) Scheduler-driven advance** — once PLAT-04G lands; label selectors.

## 9. Frozen and open decisions

- **Canary health signal**: ack + absence-of-failure only, or also require a
  fresh healthy heartbeat / a metrics threshold before advancing?
- **Frozen in Phase 2 — selector v1**: module_key + optional agent_type +
  active/pending status + optional explicit collector allowlist, all intersected;
  labels require a later indexed management contract.
- **Frozen in Phase 2 — config version**: rollout-created revisions share each
  collector's single monotonic `config_version` sequence; rollout targets only
  record the resulting version and never create a second version lane.
- **Kill revert atomicity**: best-effort per-target revert (some may already be
  offline) — is "reverted where reachable, flagged where not" acceptable?

Phases 0–2 are no longer open product decisions: they implement the existing
repository trust and per-collector lifecycle boundaries. The
implemented secret backend is an owner-only file; a future KMS/Vault adapter may
replace only that loader without changing key state, bundle or signer contracts.
