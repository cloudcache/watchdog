-- SPDX-FileCopyrightText: 2026 Watchdog contributors
-- SPDX-License-Identifier: AGPL-3.0-only

-- raw is ORDER BY toStartOfHour(event_time), so the sparse primary index prunes
-- a sub-hour query only to the whole containing hour: a live 5-minute window
-- selected 757/757 granules (the entire hour). A minmax skip index over the real
-- event_time prunes within the hour. Verified on production new parts: the same
-- 5-minute window dropped to 122/757 granules (~6x less read, 877K vs ~6.2M
-- rows) at ~50 KiB index size. ADD INDEX is metadata-only; existing parts get it
-- on merge or MATERIALIZE INDEX, new parts build it on insert. This is the cheap
-- 80% of the raw time-clustering win; the full 5-minute ORDER BY rebuild remains
-- a separate P3 (lifecycle review §11.2-F, FL5M-12).
ALTER TABLE watchdog_flow.flow_records
  ADD INDEX IF NOT EXISTS flow_event_time_minmax event_time TYPE minmax GRANULARITY 1;
