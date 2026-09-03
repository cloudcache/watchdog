# SNMP Collector Task List

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
