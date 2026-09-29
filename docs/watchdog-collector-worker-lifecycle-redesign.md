# Collector / Worker Lifecycle — Review & Redesign

> **Drift from code (reviewed 2026-09-28):** 5 statements below are behind or at odds with the code — the code is authoritative; see "Drift from code (reviewed 2026-09-28)" at the end.

> **SUPERSEDED for the agent-lifecycle half (2026-09-28).** Item 2's proposals
> below (persistent install/kind-scoped *registration token* table, a `pending →
> approve` gate, keeping one-time enrollment as legacy) were **not** the shipped
> design. What shipped instead is a **single global shared token**
> (`agents.shared_token` / `WATCHDOG_AGENT_SHARED_TOKEN`): agents self-register
> idempotently with it and are authenticated by it; the one-time enrollment
> tokens **and** per-agent credentials were removed outright (migration `0050`
> drops `agent_enrollment_tokens` + `agent_credentials`), `activate-agent.sh` and
> the UI now provision the shared-token file, heartbeats no longer write health
> (derived from run reports + staleness), and streaming collectors/workers report
> per-window workload into `agent_runs.summary_json`. Read the "current state"
> audit below as history; for the implemented model see §5.3 of
> `watchdog-kiss-architecture.md` and migration `0050`. Item 1 (SNMP auto-discover loop)
> is unaffected and shipped as described.

Status: **review + proposed redesign (no code yet)**. Owner: platform.
Scope: close two runtime loops that the UI opens but never connects to the
data plane —

1. **Target (SNMP device) collection loop** — a host added in the UI is never
   collected (the deployed symptom: `172.57.1.2` on 192.168.1.18 sits idle
   while `103.83.64.2` collects).
2. **Agent / worker lifecycle** — the SNMP collector, flow collector and flow
   worker have no "self-start → register → admin-approve → CRUD/status" loop.

This document reviews the current code, then specifies a redesign. It
**extends** the frozen KISS-04B model
([agent-plan-delivery-design.md](agent-plan-delivery-design.md)) rather than
replacing it — the Ed25519 signed-plan machinery stays for tunables; the
redesign adds config-file registration, an approval gate, reversible disable,
and first-class fleet assignment on top. §9 records the locked decisions
(2026-09-18).

---

## 1. TL;DR — the two gaps

Both gaps have the same shape: **something is created in the management UI, but
nothing wires it to the process that would act on it.**

| | Created in UI | Runtime that should act | Missing link |
|---|---|---|---|
| Item 1 | a `devices` row (SNMP host) | `watchdog-snmp-collector` poll loop | discovery → `snmp_collection_recipes`; the poll loop only sees devices that already have recipes |
| Item 2 | an `agents` row (optional, via enrollment) | the collector/worker process | the process runs *headless* from config/plan files; registration is optional; there is no approval, no reversible disable, no real status/CRUD |

---

## 2. Current state — target collection loop (item 1)

### 2.1 The chain, and where it breaks

```
add host            assign profile         discover                    poll
network-device-form  network-discover      POST .../snmp/discover      watchdog-snmp-collector
createDevice   ──►   PATCH .../snmp    ──►  importSNMPDiscovery    ──►  ListDueDevices → poll
(INSERT only)        (snmp_profile_id)      (writes recipes)            (reads recipes)
```

- **Add is insert-only.** `createDevice` ([devices.go:273](../internal/server/devices.go)) writes one `devices` row. No profile, no discovery, no recipes, no scheduling.
- **Discovery is the *only* thing that writes recipes**, and it is manual: `POST /api/v1/devices/:id/snmp/discover` ([snmp_discovery.go:27](../internal/server/snmp_discovery.go), perm `device.discover`) or the CLI `watchdog-snmp-collector -discover -device=<id>`. It requires a profile (`409 "device has no SNMP profile"`) and network reachability (else `status='down'`, no recipes). It runs **inline in the Gin server** (`s.snmpDiscovery.Discover`), so the *server* — not the collector — must be able to reach the device over SNMP.
- **The poll loop is an INNER JOIN on recipes.** `ListDueDevices` ([snmp_collector_runtime.go:200](../internal/server/snmp_collector_runtime.go)):
  ```sql
  SELECT r.device_id FROM snmp_collection_recipes r JOIN devices d ON d.id=r.device_id
  WHERE r.enabled=1 AND d.disabled=0 AND d.kind='network' AND d.snmp_profile_id IS NOT NULL ...
  ```
  A device with a profile but **zero recipe rows produces no rows → it is never polled.**
- **There is no auto-discovery anywhere** (confirmed 5 ways): `RunLoop` only ever calls `RunDue` (poll), never `DiscoverDevice`; the collector runtime is invoked only from `cmd/watchdog-snmp-collector`, never from `internal/server`; server boot spawns only address/flow/vpn opjob workers (no SNMP scheduler); device creation enqueues no job. The dev/deployed collector runs `--discover=false --poll=true --loop=true` ([Makefile:123](../Makefile)) — pure polling.

### 2.2 Diagnosis of `172.57.1.2`

It was added (and shows its device ID as a fallback name because `sys_name` is
empty), but never successfully discovered → 0 recipes → invisible to the poll
loop, forever. `103.83.64.2` was discovered → has recipes → polled.

**"The profile looks bound but wasn't applied."** The profile-persistence path
is *correct* — the edit form sends `snmp_profile_id`
([network-device-form.tsx:208](../frontend/src/components/routes/network-device-form.tsx))
→ `deviceMutation.SNMPProfileID` (`json:"snmp_profile_id"`) → `updateDevice`
persists it ([devices.go:379](../internal/server/devices.go)). There is **no
persistence bug**. But two things create the "bound-but-nothing-happens"
impression:

1. **Binding a profile triggers nothing.** Saving a profile does not run
   discovery, so no recipes are created and the device is never polled. The form
   marks the profile *required* and shows "discovered inventory", implying that
   setting it leads to collection — it does not.
2. **The form auto-selects the sole profile**
   ([network-device-form.tsx:158](../frontend/src/components/routes/network-device-form.tsx):
   `profiles.length === 1 ? profileID(profiles[0]) : ""`). The dropdown *shows* a
   profile "bound" before Save, so the UI can look bound while the DB value is
   still `NULL`.

The DB check disambiguates: `snmp_profile_id = NULL` → the auto-select display
trap (never saved); `snmp_profile_id` set but `recipes = 0` → saved fine but
**discovery never ran** (the core gap). Confirm on the box:

```sql
SELECT d.host, d.disabled, d.snmp_profile_id, d.status, d.status_reason, d.sys_name, d.last_polled_at,
  (SELECT COUNT(*) FROM snmp_collection_recipes r WHERE r.device_id=d.id) AS recipes
FROM devices d WHERE d.host IN ('172.57.1.2','103.83.64.2')\G
```

Expect `172.57.1.2 → recipes=0` (likely `snmp_profile_id=NULL`, `status=pending`).
**Immediate unblock (no code):** UI → *Discover Network Device* → select
`172.57.1.2` → pick a matching SNMP profile → *Discover*. On success the
already-running collector polls it within one interval.

---

## 3. Current state — agent/worker lifecycle (item 2)

### 3.1 The three processes bootstrap headless; registration is optional

| Process | Mandatory bootstrap | Agent registry | When registry is engaged |
|---|---|---|---|
| `watchdog-snmp-collector` | YAML config (MySQL + ClickHouse + SNMP); reads devices/recipes **directly from MySQL** | optional | only if `-agent-plan-public-key`/`-lkg`/`-enrollment-token-file` given ([main.go:186](../cmd/watchdog-snmp-collector/main.go)) |
| `watchdog-flow-collect` | signed **plan file** (`-plan` + `-plan-public-key`, hard-required) + Kafka flags | optional | only if `-control-plane-url` given ([main.go:84](../cmd/watchdog-flow-collect/main.go)) |
| `watchdog-flow-worker` | signed **plan file(s)** + Kafka + ClickHouse + enrichment version source; `worker.env` supplies flags; needs its own enrichment-sync token/mTLS | optional | only if a `-agent-plan-*`/`-enrollment` flag given ([main.go:361](../cmd/watchdog-flow-worker/main.go)) |

**Consequence:** in a normal deployment (e.g. `make dev-snmp-collector`, no
agent flags) the processes never register and never appear in the agent UI. The
agent registry is a **parallel, optional, elaborate** subsystem most deployments
skip. That is the deepest reason the loop feels "not closed": the running
collectors and the manageable "agents" are two different things.

### 3.2 The registry that *does* exist (when you opt in)

- **Bootstrap per process:** control-plane URL + stable agent id + **one-time enrollment token file** + Ed25519 **plan public-key file** + LKG file (+ plan file / kafka for flow). The `activate-agent.sh` systemd script wires all of these ([deploy/systemd/activate-agent.sh](../deploy/systemd/activate-agent.sh)).
- **Register:** `POST /api/v1/agents/register` with the one-time enrollment token → durable credential written to disk, enrollment file deleted (single-use; `agent_enrollment_tokens.consumed_at`).
- **State machine:** single `status` enum `registered → active → draining → revoked` (`0003_agents.sql`, `0012_agent_registry_lifecycle.sql`).
  - **No approval gate:** the **first heartbeat auto-promotes** `registered → active` (`status=IF(status='registered','active',status)`, [agents.go:601](../internal/server/agents.go)). Whoever holds a valid enrollment token becomes active on first contact.
  - **No reversible disable:** migration `0012` folded `disabled → revoked`, and `normalizeAgentStatus` still aliases `disabled → revoked` ([agents.go:941](../internal/server/agents.go)). `revoke` is **terminal** — `updateAgent` rejects un-revoking ([agents.go:329](../internal/server/agents.go)). `draining` is only ever set manually.
- **Plans:** Ed25519-signed **immutable** `agent_plans` + monotonic `plan_version` + `agent_plan_acks` + LKG. They tune only process knobs (SNMP interval/limit; flow socket/buffer/fetch/CH-block sizes) — **not** what to collect.
- **Binding:** `agent_bindings` is **1 agent ↔ 1 device** (unique per agent). That fits a per-host system agent but **not** a fleet-wide SNMP/flow collector that serves many devices. Today the SNMP collector ignores bindings entirely and polls every device with recipes straight from MySQL.
- **UI** ([agents.tsx](../frontend/src/components/routes/agents.tsx), [agent-form.tsx](../frontend/src/components/routes/agent-form.tsx)): create, edit (status registered/active/draining, binding/mode/endpoint), rotate credential, revoke, delete, enroll (one-time token + public key), publish/view plans, view runs. **No approve, no enable/disable toggle.**

### 3.3 Endpoints (for reference)

- **Data-plane** (`/api/v1`, no RBAC, auth by agent credential / enrollment token): `POST /agents/register`, `POST /agents/:id/heartbeat`, `POST /agents/:id/status`, `POST /agents/:id/errors`, `GET /agents/:id/plan`, `POST /agents/:id/plan-acks`.
- **Management** (RBAC `agent.view` / `agent.manage`): `GET/POST /agents`, `GET/PATCH/DELETE /agents/:id`, `GET /agents/:id/runs`, `GET/POST /agents/:id/plans`, `GET /agents/:id/plans/:version`, `POST /agents/:id/credentials/rotate`, `POST /agents/:id/revoke`, `POST /agents/enrollment-tokens`, `GET /agents/plan-public-key`, `POST /agents/plan-rollouts[...]`.

---

## 4. Target model (your spec, restated precisely)

1. **Self-start:** the operator starts each collector/worker process (systemd or by hand). The backend never starts processes.
2. **Config-file register:** each process reads one config file with the **backend domain-or-ip:port** + a **token**; on start it phones home and registers; the backend **verifies the token**.
3. **UI approves access:** a freshly-registered process lands in a **pending** state and does no real work until an admin **approves** it.
4. **UI CRUD + status:** admin can **disable / enable** (reversible), **edit**, **delete**, and **view status** of each agent/worker.

---

## 5. Gap: current → target

| Target | Current | Change needed |
|---|---|---|
| one config file: `backend_url` + `token` | multi-file bootstrap (enrollment + plan pubkey + LKG + plan file + kafka) | consolidate to one config file; make registry the default path, not opt-in |
| self-register with a verifiable token | one-time enrollment token, consumed once | persistent **registration token** (install- or kind-scoped); self-announce |
| **pending → admin approve → active** | first heartbeat auto-activates | add `pending` state; gate data-plane authorization on approval |
| **reversible disable / enable** | `disabled` aliases terminal `revoked` | add reversible `disabled` state distinct from `revoked` |
| edit name / tunables / scope | edit exists; tunables via signed plans | fold tunables into the editable agent record (or keep plans — see D2) |
| delete | delete exists | keep |
| rich status | list + health + runs exist | add "what am I collecting / last poll / current errors" |
| (item 1) add host → collected | manual discover, no auto, server-side discovery | agent-executed discovery + auto-schedule + per-device status |

---

## 6. Redesign — agent/worker lifecycle

### 6.1 One config file, registration is the default path

```yaml
# /etc/watchdog/agent.yaml  (0600, owner-only)
backend_url: http://192.168.1.18:8091      # domain or ip:port
token:       <registration-token>          # verified on register
kind:        snmp                           # snmp | flow_collect | flow_worker | system
agent_id:    snmp-sandun-01                 # optional stable id; auto-generated if absent
name:        "Sandun IDC SNMP collector"    # optional label shown in UI
```

On start every process: `POST /agents/register {token, kind, agent_id?, name?,
capabilities, software_version, host}` → backend verifies the token → upserts an
`agents` row in **`pending`** → returns a durable per-agent credential (written
to a sibling file, `0600`). The process then heartbeats and shows up in the UI
as *pending* with its self-reported identity. **Data-plane secrets** (MySQL/CH/
Kafka) are decision **D3**: either kept in the process's own config (direct
access, as SNMP does today) or delivered after approval.

### 6.2 State machine (admin state ⟂ health)

```
                approve
   pending ───────────────► active ◄─────┐
      │                        │  ▲       │ enable
      │ reject/delete   disable│  │       │
      ▼                        ▼  └───────┘
   (deleted)               disabled
      ▲                        │
      └──────── delete ────────┘
   any state ── revoke ──► revoked (terminal; credential killed)
```

- **pending** — registered, awaiting approval. Credential authorizes **only** heartbeat/status (so the UI can see it), **never** work assignment or data writes.
- **active** — approved and enabled. Pulls its assignment, collects, reports.
- **disabled** — approved but administratively paused (**reversible**). The process keeps running and heartbeating; the backend answers "stand down / no work"; the process idles (does not exit). *Enable* resumes with no restart.
- **revoked** — terminal, as today: all credentials killed; the next 401/403 makes the process drain and exit without restart.
- **health** (online / degraded / offline) stays orthogonal, derived from heartbeat freshness — unchanged from today.

### 6.3 Backend / DDL / UI deltas (shape, not code)

- **DDL:** reintroduce `disabled` as a first-class reversible status and add `pending`; stop aliasing `disabled → revoked`; add `approved_at` / `approved_by`. `agent_credentials` unchanged. Registration tokens: a new small table (or reuse `agent_enrollment_tokens` made multi-use + rotatable) keyed by install/kind, storing only SHA-256.
- **Endpoints (new/changed):** `POST /agents/:id/approve`, `POST /agents/:id/disable`, `POST /agents/:id/enable` (all `agent.manage`); `POST /agents/register` gains token-kind verification and lands `pending`; `authenticateAgent` gains a work-authorization check (`active` only) separate from the heartbeat-authorization check (`pending`/`active`/`disabled`). Keep `revoke`/`delete`/`rotate`.
- **UI:** agents list gets a *Pending* section with **Approve / Reject**; each active agent gets **Disable/Enable** toggle, **Edit**, **Delete**, and a **status panel** (kind, host, version, health, last-seen, current assignment, last poll, recent errors/runs). Enrollment dialog is replaced by "show the registration token + the one-line config file."

### 6.4 Relationship to KISS-04B (per D2: keep signing, add lifecycle on top)

Nothing in the signed-plan model is removed. Two concerns are kept separate:

- **Identity + authorization (new):** the config-file **registration token**
  gets a process registered as `pending`; **approval** promotes it to `active`;
  `disabled` reversibly pauses it. This is the new lifecycle layer.
- **Tunables (unchanged):** once `active`, an agent still fetches its Ed25519
  **signed immutable plan** (`GET /agents/:id/plan`), verifies it, applies it,
  ACKs it, and caches an LKG — exactly as KISS-04B specifies.

So an approved agent has *both* a durable credential (from registration) and a
signed plan (for tunables). The one-time enrollment token becomes optional/
legacy: the registration token + approval replaces it as the default onboarding
path, but the enrollment endpoint stays for existing `activate-agent.sh`
deployments (§10).

---

## 7. Redesign — target collection loop (item 1)

Give a device an explicit **collection state** and close the chain:

```
created ─(profile set)─► scheduled ─(agent discovers)─► collecting
                             │                              │
                             └────────► failed ◄────────────┘  (with reason + backoff)
```

- **Trigger on bind (fixes the dead end):** when a device is created/updated **with an `snmp_profile_id`**, the backend enqueues a **discovery job** (reuse the existing `operation_jobs` machinery already used for address/vpn work) instead of doing nothing. Binding a profile now *means* "collect this."
- **Two cadences (LibreNMS parity).** LibreNMS runs *discovery* (~6h) separately from the *poller* (~5min); Watchdog today has neither on a schedule. Add a periodic **re-discovery** pass (default ~6h, per-device/per-profile configurable) distinct from the existing **poll** loop (~1min), so new interfaces / sensors / sysName / topology changes are picked up over time rather than only once.
- **Manual re-discover UI (LibreNMS parity).** A per-device **"Re-discover now"** action (and a bulk action) surfaced directly on the device/target page — replacing today's buried separate *Discover Network Device* page. This is the "UI 来修正" you asked for.
- **Reconcile + backoff:** the reconcile pass re-enqueues discovery for devices that have a profile but zero/stale recipes, or whose last discovery failed, with exponential backoff so an unreachable host does not hammer the collector.
- **Who discovers:** an **approved SNMP agent** executes the discovery job over its authenticated channel and imports recipes — *not* the Gin server. This fixes the reachability mismatch (§2.1): the agent lives on the network that can reach the device; the server may not. (Interim compatibility: keep the current inline server-side discover endpoint working.)
- **Status surfaced (fixes the "looks bound / silently idle" confusion):** persist and show per-device discovery state + `last_discovered_at` + `last_error` (*never discovered / last ok N ago / failed: <reason>*), and drop the form's silent auto-select of the sole profile (§2.2) so "profile bound" always reflects the DB.

This also resolves the `agent_bindings` mismatch (§3.2): a fleet SNMP agent is
bound to a **scope** (all `network` devices, or a device group/location), not a
single device.

---

## 8. Fleet assignment & flow-source (sflow target) management (D3)

Agents/workers each own a **subset** of targets. The two data planes are at very
different stages, so treat them separately.

### 8.1 Flow (sflow/netflow/IPFIX) — model + API exist, **UI is missing**

The model is already built and is *not* passive-only under the hood:

- **`flow_exporter_bindings`** ([0006_flow_exporter_bindings.sql](../deploy/schema/mysql/0006_flow_exporter_bindings.sql)): a flow exporter is a *capability of a device*, keyed by `(protocol, source_prefix, observation_domain_key)`, carrying **`collector_agent_id`** (the exporter→collector assignment you asked for), `sampling_mode`/`default_sampling_rate`/`sampling_rules_json`, `enabled`, and `published_plan_version` (tracks which signed plan version this binding is in).
- **CRUD API exists:** `GET/POST/GET:id/PATCH/DELETE` on `/flow/devices` **and** `/flow/exporter-bindings` ([router.go:203](../internal/server/router.go), perms `flow.device.view`/`flow.device.manage`), handlers in [flow_exporters.go](../internal/server/flow_exporters.go); `validateFlowCollector` checks the assigned collector.
- **Signed plan is generated per collector** from these bindings ([flow_exporters.go](../internal/server/flow_exporters.go) → `flowplan.SourceBinding`), so once a binding names a `collector_agent_id`, that collector's plan admits it.

**What's actually missing:** a **frontend page**. The `flow_source` nav item is
`FlowReports` (a *query* view), not management ([main.tsx:243](../frontend/src/components/routes/main.tsx)); no route drives the exporter API. So "sflow target 增删改查列" = build the UI:

- a **Flow Sources** page (new nav entry, distinct from the flow *reports*): list exporter bindings with protocol / source_prefix / observation-domain / assigned collector / enabled / plan-version, and add/edit/delete forms over the existing API.
- surface **collector assignment** (`collector_agent_id`) as a picker of approved flow-collect agents; show published vs pending plan version so an operator sees when a change still needs a plan publish.
- because sFlow is *passive receive*, an exporter that sends packets but has no matching binding should surface as an **"unclaimed source"** (seen-on-the-wire but not configured) so the operator can one-click create a binding for it — this is the piece that turns passive receive into a managed CRUD list.

### 8.2 SNMP — assignment is **new**

The SNMP collector polls **every** device with recipes (`ListDueDevices` has no
agent filter) and `devices` has no `collector_agent_id`. To give SNMP the same
subset-ownership as flow:

- add a device→SNMP-agent assignment (mirror flow: a `collector_agent_id` on `devices`, or a small assignment table if a device may be multi-homed) with an "unassigned → default collector" bucket so single-agent installs keep working;
- `ListDueDevices` (and the discovery job dispatch, §7) filter by the **calling agent's** assigned scope;
- surface the assignment in the device/target UI (assign a host or a device-group/location to a specific SNMP collector).

### 8.3 How the two loops connect

Once collectors are approved agents (§6) with a scope (§8.1/§8.2), item 1 (§7)
rides on item 2: the backend hands an approved SNMP agent its **scoped**
discovery+poll assignment, the agent executes and reports per-device status, and
the UI shows both *agent* status (§6) and *device/flow-source* status (§7/§8).

---

## 9. Decisions (locked 2026-09-18)

- **D1 — Registration token model → shared token + approval gate.** One
  install-/kind-scoped registration token in the config file; any process with
  it lands `pending` and does nothing until an admin approves. Approval is the
  security boundary; the token is rotatable; pending rows are inert. Mitigate
  token spray with rate-limiting + rotation.
- **D2 — Keep the Ed25519 signed-plan model; add the new lifecycle *on top*.**
  Enrollment/signed immutable plans/ACK/LKG stay as-is for tunable delivery.
  The redesign *adds* the config-file registration token, the `pending →
  approve` gate, and reversible `disabled` — it does **not** remove signing.
  KISS-04B's tamper-evident/offline-verifiable plan invariants are preserved.
  (Registration token and approval govern *identity + authorization*; signed
  plans continue to govern *tunables*. Two separate concerns, both kept.)
- **D3 — Fleet assignment is a first-class feature (not "as-is").** Agents/
  workers each own a *subset* of targets. This is §8. Flow already models it
  (`flow_exporter_bindings.collector_agent_id`) and only lacks a UI; SNMP needs
  a new device→agent assignment; **sflow targets need a full CRUD/list UI** on
  the existing API.
- **D4 — Item 1 → auto-discover + reconcile + status.** Enqueue discovery when a
  device gets a profile; a bounded reconcile loop retries devices with a profile
  but 0/stale recipes; per-device collection state shown in the UI. §7.

---

## 10. Compatibility / what not to break

- Keep `POST /devices/:id/snmp/discover` working during the transition (item 1 interim unblock).
- Keep the existing enrollment/plan endpoints until D2 is decided; if D2=(a), deprecate rather than delete abruptly (existing `activate-agent.sh` deployments).
- Do not grant the Gin process root/systemctl — process start stays with systemd/the operator (unchanged invariant, still compatible with "手动自主启动").
- ClickHouse-boot gotcha unchanged: the SNMP collector needs `WATCHDOG_CLICKHOUSE_PASSWORD_FILE` at start.

## 11. Phased path (no code yet; decisions locked)

Each phase is independently shippable and verifiable.

1. **P0 (this doc)** — review + locked decisions D1–D4.
2. **P1 — item 1 close (D4), LibreNMS-parity — ✅ IMPLEMENTED (2026-09-18).** A server-side **discovery reconcile loop** ([internal/server/snmp_discovery_reconcile.go](../internal/server/snmp_discovery_reconcile.go)) discovers network devices that have an SNMP profile but no enabled recipes, and re-discovers healthy devices every `RediscoverInterval` (default 6h; scan 1m; 15m failure backoff; concurrency 4), reusing the existing inline discovery engine via a shared, ctx-based `discoverAndImport` core. New config: `snmp.auto_discover` (default true) + `snmp.rediscover_interval`. The per-device **"Re-discover now"** action already existed on the device page; added: discovery **status/reason surfaced** in the device header, and **removed the form's silent auto-select** of the sole profile ([network-device-form.tsx](../frontend/src/components/routes/network-device-form.tsx)). Verified: `go build ./...`, `go test ./internal/server/`, frontend `tsc`. *(Interim: the existing Discover page/endpoint keeps working; server-side discovery is unchanged in reachability — agent-side discovery is a later phase.)*
3. **P2 — agent lifecycle (D1 identity/approval + D2 keep signing):** add `pending` + reversible `disabled` states (stop aliasing `disabled → revoked`); `approve`/`disable`/`enable` endpoints + UI; registration lands `pending`; work-authorization gated on `active` while heartbeat-authorization allows `pending`/`disabled`. Signed-plan fetch/apply/ACK unchanged.
4. **P3 — config-file register (D1):** single `agent.yaml` (`backend_url` + registration `token` + `kind`); registration-token table (SHA-256, rotatable); make registration the default path for all three binaries; keep one-time enrollment + `activate-agent.sh` as legacy.
5. **P4 — fleet assignment (D3):** SNMP device→agent assignment (`ListDueDevices`/discovery filtered by the calling agent's scope; "unassigned → default") + a **Flow Sources** UI over the existing `/flow/exporter-bindings` API, incl. collector-agent picker, plan-version status, and an "unclaimed source" list for passive sFlow exporters.

---

## Drift from code (reviewed 2026-09-28)

The 2026-09-28 full project review checked this document against current code, migrations and commits. The items below are superseded by the implementation, renamed, or not yet implemented. **The code is authoritative**; the body is kept as design history.

- **Status line (:18):** P1 (SNMP auto-discovery) is implemented (`f65f56f68`); item 2 is superseded by the shared-token model (`aa870f65d`, `0050`); P4 is partially open.
- **§7 auto-discovery (:241-251):** shipped in reduced form — an in-process `watchdog-server` ticker (1m scan, batch 50, concurrency 4, fixed 15m in-memory backoff), with no operation_job and no persisted collection state beyond `devices.status/status_reason`. The top banner's "shipped as described" applies to §11 P1, not to all of §7.
- **Flow assignment (:264-277,312-316):** worker→device assignment has shipped — `flow_worker_device_bindings` (`0048`), `/api/v1/flow/worker-device-bindings`, a UI and targeted publications (`313a0ac02`). The exporter CRUD UI and SNMP device→agent assignment remain open.
- **Collector plan (:270):** the server only validates exporter bindings with a throw-away `CompilePlan`; `published_plan_version` is never set, and flow-collect still loads a hand-supplied `-plan` file.
- **Phased path (:336-338):** P2/P3 are superseded (no approval gate; shared-token self-registration); P4 is partial (flow worker bindings delivered).
