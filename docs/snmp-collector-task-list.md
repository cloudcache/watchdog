# SNMP Collector Task List

> **Archived (2026-09-28):** historical task list, superseded by KISS-03A/KISS-08 — `internal/watchdog` was deleted (`71e9bf39f`), samples go to ClickHouse (`012`) via `internal/snmpch`, recipes live in `deploy/schema/mysql/0016`, and continuous discovery is the server reconcile loop, not tenant jobs claimed by the collector. See `docs/kiss03-snmp-clickhouse-design.md`; individual tasks are not updated.

Status values:

- `[todo]` not started
- `[doing]` actively being changed
- `[done]` implemented and checked
- `[blocked]` cannot move without a concrete missing input

## Tasks

1. `[done]` Write this task list.
   - Acceptance: task file exists and every task has a testable output.

2. `[done]` Add collector database schema.
   - Output: `deploy/migration/mysql/007_snmp_collector.sql`
   - Acceptance: schema defines OS definitions, module definitions, device module state, collection recipes, state translations, trap handlers, and SNMP events.

3. `[done]` Add collector domain types.
   - Output: `internal/watchdog/snmp_collector_types.go`
   - Acceptance: code defines OS fingerprints, OS matches, module definitions, pre-cache requests, collection recipes, raw samples, query requests, and trap types.

4. `[done]` Add collector repository contract.
   - Output: `internal/watchdog/snmp_collector_repository.go`
   - Acceptance: repository exposes definition reads, recipe writes/reads, module state writes, trap handler reads, and event writes.

5. `[done]` Add MySQL repository implementation.
   - Output: `internal/watchdog/mysql_snmp_collector_repository.go`
   - Acceptance: MySQL store satisfies the collector repository interface and never reads vendor metric lookup records.

6. `[done]` Add gosnmp query engine.
   - Output: `internal/watchdog/snmp_query_engine.go`
   - Acceptance: engine supports Get and Walk with v2c and v3 profile data, request timeout, context name, max OIDs, max repetitions, and no business projection.

7. `[done]` Add ports discovery module.
   - Output: `internal/watchdog/snmp_discovery_ports.go`
   - Acceptance: module discovers IF-MIB ports and creates raw counter/state recipes.

8. `[done]` Add raw poller.
   - Output: `internal/watchdog/snmp_poller.go`
   - Acceptance: poller consumes collection recipes, chunks GET requests, writes raw samples, and never writes Bps metrics.

9. `[done]` Add raw sample writer.
   - Output: `internal/watchdog/snmp_raw_writer.go`
   - Acceptance: writer renders only raw counter, gauge, state, and string-safe metrics for VictoriaMetrics.

10. `[done]` Add trap dispatcher and first handlers.
    - Output: `internal/watchdog/snmp_trap_dispatcher.go`, `internal/watchdog/snmp_trap_handlers.go`
    - Acceptance: linkUp, linkDown, authenticationFailure, coldStart, warmStart, and bgpBackwardTransition handlers create events and targeted poll requests.

11. `[done]` Add collector projection for traffic rate.
    - Output: `internal/watchdog/snmp_projection.go`
    - Acceptance: collector projection builds traffic rate queries from raw octet counters and requires tenant/device scope.

12. `[done]` Add collector-focused tests.
    - Output: `internal/watchdog/snmp_collector_test.go`
    - Acceptance: `go test ./internal/watchdog -run 'TestSNMPCollector'` passes.

13. `[done]` Run full package tests.
    - Output: full package test result.
    - Acceptance: `go test ./internal/watchdog` passes.

14. `[done]` Add OS detection matcher.
    - Output: `internal/watchdog/snmp_os_detection.go`
    - Acceptance: matcher evaluates sysObjectID, sysObjectID regex, sysDescr regex, sysName regex, and negative rules from OS definitions.

15. `[done]` Add discovery engine.
    - Output: `internal/watchdog/snmp_discovery_engine.go`
    - Acceptance: engine reads core facts, detects OS, runs registered discovery modules, and returns assets plus recipes.

16. `[done]` Add definition importer skeleton.
    - Output: `internal/watchdog/snmp_definition_importer.go`
    - Acceptance: importer converts structured OS/module/trap definitions into collector repository records and never emits vendor metric lookup records.

17. `[done]` Add sensors discovery module skeleton.
    - Output: `internal/watchdog/snmp_discovery_sensors.go`
    - Acceptance: module supports ENTITY-SENSOR-MIB recipes with divisor/multiplier metadata and raw sensor metrics.

18. `[done]` Add BGP discovery module skeleton.
    - Output: `internal/watchdog/snmp_discovery_bgp.go`
    - Acceptance: module supports generic BGP4-MIB peer recipes and stores context_name in recipes.

19. `[done]` Add collector discovery importer.
    - Output: `internal/watchdog/snmp_collector_discovery_import.go`
    - Acceptance: importer persists device updates, ports, sensors, physical entities, BGP sessions, VLANs, LAGs, events, device module status, and collection recipes from `SNMPCollectorDiscoveryResult`.

20. `[done]` Add default collector module registry.
    - Output: `internal/watchdog/snmp_collector_registry.go`
    - Acceptance: registry returns ports, sensors, and bgp modules by name and builds a discovery engine without vendor metric lookup records.

21. `[done]` Add due recipe poll runner.
    - Output: `internal/watchdog/snmp_poll_runner.go`
    - Acceptance: runner loads due recipes per tenant, groups them by device, executes poller jobs, and marks recipe poll result.

22. `[done]` Integrate collector poll runner into backend runtime.
    - Output: `internal/watchdog/runtime.go`
    - Acceptance: runtime constructs the new collector poll runner with MySQL repositories, gosnmp query engine, and VictoriaMetrics raw writer.

23. `[done]` Run original target regression tests.
    - Output: target and network API test result.
    - Acceptance: target creation/list/update/delete and network target summaries still pass.

24. `[done]` Add and run collector command for an existing target.
    - Output: `cmd/watchdog-snmp-collector/main.go`
    - Acceptance: command can run discovery, import recipes, execute polling, and report counts for a selected tenant/device.

25. `[done]` Start the new collector from the dev API and expose raw-counter traffic charts.
    - Output: `cmd/watchdog-dev-server/main.go`, `internal/watchdog/runtime.go`, `internal/watchdog/api_metrics.go`, `internal/watchdog/snmp_projection.go`
    - Acceptance: dev API starts the new collector by default, old SNMP pull remains stopped, collector logs successful polling, MySQL `snmp_collection_recipes.last_polled_at` advances, and `/api/v1/metrics/query` returns traffic series by projecting raw octet counters.

26. `[done]` Close the production discovery-job lifecycle in the independent collector.
    - Output: `cmd/watchdog-snmp-collector/main.go`, `internal/watchdog/discovery_scheduler.go`, `internal/watchdog/mysql_discovery_repository.go`
    - Acceptance: `--loop` requires no single device ID, claims tenant-scoped pending jobs immediately using the database clock, atomically skips jobs claimed by another worker, records completion/error, and continues recipe polling.

27. `[done]` Make network-device state and failure output reflect SNMP results.
    - Output: `internal/watchdog/api_network.go`, `internal/watchdog/discovery_scheduler.go`, network-device forms and event log.
    - Acceptance: a new/changed device is pending, successful discovery sets it up, failed discovery sets it down and writes a `discovery_failed` event; discovery-owned status cannot be manually set to up, and an unknown vendor renders the generic network icon instead of a broken image.

28. `[done]` Make LibreNMS OS-definition import lossless and fail closed.
    - Output: `internal/watchdog/snmp_definition_parser.go`, `internal/watchdog/snmp_definition_yaml.go`, `internal/watchdog/snmp_os_detection.go`.
    - Acceptance: scalar and list conditions, contains/regex conditions, PCRE delimiters, and negative rules retain LibreNMS semantics; an invalid source file reports its path instead of being silently skipped; the reference checkout imports 803/803 definitions and classifies `.1.3.6.1.4.1.2636` as Junos rather than EdgeSwitch.

29. `[done]` Preserve every discovered sensor identity.
    - Output: `deploy/migration/mysql/015_network_sensor_identity.sql`.
    - Acceptance: sensors sharing class/index but exposing different OIDs no longer overwrite each other; the MX480 discovery persists 212 discovered sensors with 212 distinct OIDs.

30. `[done]` Replace OS-name BGP selection with MIB-capability providers.
    - Design: provider descriptors declare peer and AFI/SAFI column symbols; actual table presence and coverage select the provider.
    - Code: `internal/watchdog/snmp_discovery_bgp_provider.go`, recursive MIB directory registration in `snmp_mib.go`.
    - Unit test: a numeric custom provider discovers IPv4 and IPv6 while the fingerprint contains an unrelated OS name.
    - Integration test: the MX480 selects `BGP4-V2-MIB-JUNIPER` from the configured MIB tree and persists 18 IPv4 plus 17 IPv6 sessions.
    - Regression: generic BGP4-MIB and existing vendor compatibility collectors remain fallback paths.

31. `[done]` Add capability-driven interface IPv4/IPv6 inventory.
    - Design: prefer RFC 4293 `ipAddressTable`; use legacy IPv4/IPv6 MIB tables only for missing families.
    - Code: add `network_interface_addresses`, discovery/import/repository/API contracts, and stable port association.
    - Unit test: decode IPv4 and IPv6 rows from one IP-MIB table without vendor or OS branching.
    - Integration test: the MX480 persists 17 IPv4 and 27 IPv6 addresses.
    - Change test: rediscovery replaces the device snapshot and removes stale addresses.

32. `[done]` Correct location and freshness semantics for standalone SNMP collection.
    - Design: location is the raw `sysLocation.0` value with explicit target-label fallback; last seen is the newest successful recipe poll.
    - Code: core fingerprint, network summary API, and network-device list/detail rendering.
    - Tests: summary API covers standalone collector freshness and location fallback remains non-fabricating.

33. `[done]` Standardize every network-device detail dataset on VTable interaction.
    - Scope: Ports, Health, Switching VLAN/LAG, BGP, Inventory, Events, and Alerts.
    - Code: shared `PagedVTable` with search, 25/50/100 pagination, and per-column filters.
    - Unit test: filter popover placement stays inside the viewport above/below and at both horizontal edges.
    - Build test: the production site TypeScript build succeeds.
    - Acceptance: each listed dataset has independent search/filter/page state and empty/loading states; a filter popover never overflows the viewport.

34. `[done]` Complete release regression and local handoff.
    - Change design review: confirm no protocol collector branches on OS/vendor/model strings.
    - Integration test: run discovery/import/poll against the configured real target and verify API rows for addresses, BGP families, last seen, and location.
    - Regression test: run Go full suite, site unit tests, site production build, migration validation, and `git diff --check`.
    - Known baseline: `go test ./...` still cannot compile `internal/hub_test` because its removed `internal/tests` fixture helpers are referenced; the changed `internal/watchdog` package and full production build pass.
    - Operations: build/restart the 8090 hub and independent collector, then commit the reviewed change set.
