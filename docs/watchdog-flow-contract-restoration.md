# Flow API Contract Restoration (KISS-06 fidelity fix)

> **Drift from code (reviewed 2026-09-28):** 6 statements below are behind or at odds with the code — the code is authoritative; see "Drift from code (reviewed 2026-09-28)" at the end.

## Problem

The KISS-06 flow migration (hub `internal/watchdog`, net/http → v2 `internal/server`,
Gin) was supposed to be a **faithful de-tenant + PB→Gin** move: keep the HTTP wire
contract byte-identical (routes, request JSON, response JSON, query-param names),
change only internals (drop PB, drop tenant, QueryGateway → direct flowquery
runners). It did **not** — several endpoints were re-authored, so the (unchanged,
hub-era) frontend 404/400s against v2.

**Mandate:** restore the hub wire contract on the v2 backend so the **frontend needs
zero changes**, while keeping the KISS internals (no PB, no tenant, no gateway
machinery — the hub envelope becomes a plain decode DTO that maps to direct runner
calls). Also port the capabilities that were dropped entirely (incomplete
migration, not just drift). User decisions (2026-09-10): "restore contract **and**
implement dropped features"; "I take everything incl. exports" (coordinate the
generic `/api/v1/exports/*` with the parallel router session).

## Deviation map (from the 4-way contract audit, hub vs v2 vs frontend)

Faithful already (no fix): `/flow/records/search`, `/flow/records/facets`,
`/flow/overseas/query` (except StorageV2 `uses_raw`), `/flow/filters`
catalog/validate/complete, `/flow/vpn/rules` CRUD, `/flow/geo/*`,
`/flow/vpn/findings` get + disposition.

Broken:
- **`/flow/query`** (400): gateway envelope + nested `parameters{}` → flat
  `flowAggregateInput`; `value_layer`→`view`; dropped `operator_selection`,
  `address_set_filter`, `address_set_endpoint`, `direction_split`, `time_windows`;
  `meta` ~15 fields → `{source,step_seconds}`.
- **`/flow/reports/query`** (404): route renamed → `/flow/reports`; same envelope→flat
  request break; response dropped `data.versions`, `watermark.latest_complete_bucket`,
  `meta.unit`, `meta.versions`; panel `meta` typed→freeform gin.H, `completeness`
  flattened, `unit`/`as_of`/`versions` missing, extra `source`/`uses_raw`.
- **`/flow/vpn/findings` list** (400): `parseInventoryPage` rejects the frontend's
  `sort_by`/`sort_direction`/`filter.<field>`; sort-column set narrowed; DTO dropped
  `dimension_snapshot_id`/`geo_version`/`classification_version`/`evidence_schema_version`/`expires_at`;
  facets response dropped `field`; list dropped `meta.sort`.
- **saved-filters** (400): de-tenant renamed the **wire value** `share_scope`/`scope`
  `tenant`→`shared` (should stay `tenant`).
- **exports** (404/400): `/flow/records/exports` deleted; `/flow/exports` now requires
  a `{kind}` the frontend doesn't send (frontend posts `{query,format}`); the generic
  `/api/v1/exports/*` lifecycle (list/detail/download/retry/cancel/delete) serves
  **SNMP only** so flow/vpn export jobs are invisible/404; `retry`+`delete` dropped
  everywhere; response `ExportTask` (Pascal+snake, target/port/aggregation/value_mode/
  value_layer/period_type/step) → slim snake `flowExportResponse`.
- **error envelope** (all, cross-cutting): `{error:{code,message,retryable,details}}`
  with UPPER_SNAKE codes → `{error:{code,message}}` with lowercase codes.

Dropped capabilities (must implement for "works fully"): operator-selection,
address-set-combination queries, `direction_split` (no flowquery equivalent —
needs engine support or per-direction emulation exposed under the same wire field),
`time_windows` on `/flow/query`, StorageV2 recent-raw hybrid (overseas + reports),
export `retry`/`delete`, generic exports lifecycle for non-SNMP jobs.

## Remediation phases

- **P1 Reports** — route `/flow/reports/query`; decode the hub envelope (`QueryRequest`
  + `parameters.report` = `flowReportSpec`) → internal report build; emit hub
  `{data:flowReportData, meta:QueryResultMeta}` with typed/complete panel meta
  (nested `completeness`, `unit`, `as_of`, `versions` on every panel).
- **P2 Query** — route stays `/flow/query`; decode the same envelope + `parameters`
  aggregate grammar; restore full `meta`; port operator-selection, address-set,
  time-windows; direction-split (engine or emulation under the `direction_split`
  field).
- **P3 Findings list** — replace `parseInventoryPage` here with hub
  `parseVPNFindingListFilter` semantics (`sort_by`/`sort_direction` + `filter.<field>`,
  full sort columns, default window_end desc); restore `meta.sort`, DTO fields,
  facets `field`.
- **P4 saved-filters** — restore `tenant` as the `share_scope`/`scope` wire value
  (accept in filter + body, emit in response).
- **P5 Error envelope** — restore `{error:{code,message,retryable,details}}` + hub
  code vocabulary.
- **P6 Exports** — re-add `POST /flow/records/exports`; accept the hub `{query,format}`
  bodies (no required `kind` — infer from the envelope/dataset); make the generic
  `/api/v1/exports/*` cover flow+vpn+snmp jobs; re-add `retry`+`delete`; emit the
  `ExportTask` shape. **Coordinate `/exports` + router.go with the parallel session.**

## Status

- Audit complete (4 subagents; full deviation reports in session transcript).
- Frontend edit I made earlier (flow-reports URL/body) was **reverted** — frontend
  stays untouched.
- **P1a done (green):** route restored to `POST /flow/reports/query`; hub envelope
  (`flowReportQueryInput` + nested `report` spec + `operator_selection`) decoded and
  mapped to the internal request; `operator_selection` honored on the wire, execution
  rejected faithfully (needs the operator-binding service). `flow_reports.go`.
- **P3 done (green):** `listVPNFindings` no longer uses `parseInventoryPage`; accepts
  `q`/`from`/`to`/`filter.<field>`/`sort_by`/`sort_direction`/`limit`/`offset` (full
  sort-column set, default `window_end desc`), emits `meta.sort`; facets response
  restored `field`. `flow_vpn_findings_api.go`.
- **P4 done (green):** saved-filter `share_scope`/`scope` wire value restored
  `shared`→`tenant` (const + validation + SQL + tests). `flow_saved_filters.go`.
- **P2 core done (green):** `/flow/query` now decodes the hub envelope
  (`flowQueryEnvelope` + nested `parameters`) and emits the full `meta`
  (`flowQueryResultMeta`: unit/source/value_layer/timezone/step + completeness);
  `time_windows` threaded. The three special modes (`operator_selection`,
  `address_set_*`, `direction_split`) are accepted on the wire and rejected
  faithfully at execution (503) — the common Explorer query works.
  `flow_query_envelope.go`, `handlers_flow.go`.
- **Remaining:**
  - **P6.1 done (green):** export create restored to the hub `{query,format}` contract —
    `/flow/exports` accepts a QueryRequest envelope and dispatches report (when
    `query.parameters.report` is present) vs aggregate/joint query; `/flow/records/exports`
    re-added for detail export; `buildFlowExportPayload` split into per-flavor builders.
    Router-free (registered on the flow route group). `flow_exports.go`, `flow_query_envelope.go`.
  - **P6.2 done (green):** generic `/api/v1/exports/*` lifecycle now covers flow+snmp
    jobs. `exports.go` (`registerExportRoutes`) wires list/detail/download/cancel/retry/delete;
    `listExports` merges every export job type newest-first (`mergeExportPage` top-K helper),
    `exportView` dispatches by opjob job type to the per-type view (flow export jobs no longer
    invisible behind the SNMP-only binding), `getExport`/`downloadExport`/`cancelExport`
    delegate to the per-type handlers (which authorize), `retryExport` re-enqueues the frozen
    spec with a fresh idempotency key, `deleteExport` cancels + removes the artifact + drops
    the row. Router edit was a **single-line swap** (the inline SNMP-only `/exports` block →
    `s.registerExportRoutes(auth)`), minimizing overlap with the parallel router session.
    Unit tests `TestExportViewDispatch` + `TestMergeExportPage` (DB-free). `exports.go`,
    `router.go`, `exports_test.go`. Note: SNMP-metric create stays `POST /exports`
    (`device.view`); `listSNMPExports` is retained (still used by `snmp_a2_integration_test.go`).
  - **Query special modes** — `direction_split` and `address_set_*` **done (green)**:
    `flow_query_modes.go` composes both over the existing runners (a faithful port of
    the hub gateway's `queryDirections`/`queryJointDirections` and `queryAddressSets`) —
    direction split runs two per-direction total queries merged under a synthetic
    "direction" dimension (with the flow_records joint fallback for non-aggregate typed
    filters); address-set runs the already-instantiated `s.flowQuery.addressSet` runner
    at a fixed 60s step, echoing the resolved filter/endpoint via the ported
    `marshalFlowAddressSetResult`. `queryFlow` now dispatches to them (503 guards
    removed). Unit tests `TestDirectionSplitAllowed`/`TestMergeFlowDirectionResults`/
    `TestFlowAddressSetLabel`/`TestMarshalFlowAddressSetResult` (DB-free).
  - **`operator_selection` done (green):** `flow_operator_query.go` re-derives the
    operator-classification binding against KISS's redesigned tables (the hub's
    `flow_enrichment_publications`/`_acks` became `dimension_snapshot_activations` +
    `dimension_snapshot_acks`; `collector_agents`→`agents`; de-tenanted). Under one
    repeatable-read snapshot `resolveFlowOperatorQueryBinding` reads
    `isp_operators`→flow_isp_id/enabled, the ranged address-dimension activation
    timeline (`module_key='flow',dimension_key='address'`, LEAD over effective_from,
    coverage-at-start check), active flow workers (`agents kind='flow_worker'
    status='active'`), and the **installed-on-every-active-worker gate**
    (`dimension_snapshot_acks state='installed'` JOIN `agents` — the join is exact
    because the flow worker registers agent id == ack worker id). `applyFlowOperatorSelection`
    (shared by `/flow/query` + `/flow/reports/query`) injects the `isp=<flow_isp_id>`
    predicate + `Filters.DimensionSnapshotIDs` pin (pure `injectFlowOperatorConstraints`).
    User chose full fidelity incl. the readiness gate (returns 503 until enrichment is
    rolled out). `classification_version` is intentionally NOT pinned — KISS doesn't
    persist it server-side and the worker pairs it 1:1 with the address snapshot id, so
    the snapshot pin transitively pins it. Operator+address_set is rejected (400,
    incompatible). Unit `TestFlowFilterReferencesISP`/`TestInjectFlowOperatorConstraints`;
    integration `TestResolveFlowOperatorQueryBinding` (6 scenarios, validated against the
    real schema via a throwaway DB). All three query special modes are now complete.
  - **Response polish (low, non-blocking):** reports `data.versions`,
    `watermark.latest_complete_bucket`, `meta.unit`/`versions`, nested panel
    `completeness` — all optional in the frontend types.
  - **P5 error envelope done (green):** the hub envelope is `{error:{code, message,
    retryable, details?}}` with **lowercase** canonical codes (`internal/watchdog/api.go`
    — the 4-agent audit's "UPPER_SNAKE" claim was wrong; only the flow query-execution
    subset used `QUERY_*` uppercase via `query_gateway.go`). Restored: `fail()` now
    delegates to `failDetails()`, both emitting `retryable` (the hub's exact rule —
    `status == 429 || status >= 500`) + optional `details` (omitempty) —
    `handlers_session.go`. This is global/additive across every domain (no code-value
    changes; canonical codes already matched). `writeFlowQueryError` restores the hub's
    `QUERY_*` vocabulary (`QUERY_INVALID`/`QUERY_RANGE_LIMIT`/`QUERY_INCOMPLETE`/
    `QUERY_PERMISSION_DENIED`/`QUERY_TIMEOUT`/`QUERY_PROVIDER_FAILURE`) with the correct
    per-code status + `details{field, flow_code}` — `flow_query.go`. Unit tests
    `TestFailEnvelopeRetryable`/`TestFailDetailsEnvelope`/`TestWriteFlowQueryErrorVocabulary`;
    full package suite green (additive change breaks no domain). **Follow-up (parallel
    session's hot files — coordinate, don't edit live):** the auth-middleware envelopes
    (`rbac.go:177/191/202`) and Gin's default 404/405 (`router.go` NoRoute/NoMethod)
    still bypass `fail()`; both are cosmetic (401/403/404 never retryable, and a missing
    `retryable` deserializes to false) so they were intentionally left for the rbac/router
    owner rather than clobbered.
- Note: nothing is live until `:8091` is rebuilt+restarted. The full-suite panic in
  `aggregate_graphs_test.go` (uint64/float64) is the parallel session's untracked WIP,
  not this work.

## Key files

Hub (contract source of truth): `internal/watchdog/api_query_gateway.go`,
`query_gateway.go`, `query_provider_flow.go`, `flow_reports.go`, `api_exports.go`,
`api_flow_exports.go`, `api_flow_detail_exports.go`, `export_flow_detail.go`,
`api_flow_vpn_management.go`, `api_flow_saved_filters.go`, `repository.go`.
V2: `internal/server/handlers_flow.go`, `flow_reports.go`, `flow_report_*.go`,
`flow_query.go`, `flow_table.go`, `flow_vpn_findings_api.go`, `flow_saved_filters.go`,
`flow_exports.go`, `router.go`, `platform_operations.go`, `snmp_exports.go`.
Frontend (hub-aligned contract): `frontend/src/components/routes/flow-reports.tsx`,
`traffic-matrix.tsx`, `flow-vpn.tsx`, `flow-saved-filters.tsx`, `exports.tsx`,
`export-detail.tsx`, `export-types.ts`; `frontend/src/lib/flow-report-model.ts`,
`flow-record-model.ts`.

---

## Drift from code (reviewed 2026-09-28)

The 2026-09-28 full project review checked this document against current code, migrations and commits. The items below are superseded by the implementation, renamed, or not yet implemented. **The code is authoritative**; the body is kept as design history.

- **P4 share_scope (:71,93-94):** superseded — the contract is private/shared: migration `0032` rewrites 'tenant' to 'shared' and adds a CHECK; server and frontend both use "shared", so "zero frontend change" no longer holds here.
- **Operator gate (:130-146):** reads the `flow_enrichment_publications` timeline plus per-publication targets/acks (targeted workers only) and pins snapshot IDs and classification versions (`93570d239`, `313a0ac02`); v2 deployment ACKs are not checked yet.
- **Direction split (:120-125):** one aggregate query with DimensionDirection; non-aggregate filters use one joint scan (`00be2cdb5`).
- **StorageV2 hybrid (:23,54):** restored — marker-coverage boundary plus raw tail for aggregate, reports, direction and overseas; overseas returns `uses_raw`.
- **fail() envelope (:165-170):** requirePermission, auth and CSRF now call `fail()`; only Gin's default NoRoute/NoMethod responses remain unwrapped.
- **Contract source and P3 (:37-38,69,175-181):** `internal/watchdog` was deleted on 2026-09-16 (`71e9bf39f`); the v2 files are the contract now. The P3 DTO fields (dimension_snapshot_id/geo_version/classification_version/evidence_schema_version/expires_at) were not restored — still open.
