-- SPDX-FileCopyrightText: 2026 Watchdog contributors
-- SPDX-License-Identifier: AGPL-3.0-only

-- Migration 016 was already released with UInt64 generation totals. Keep that
-- migration byte-for-byte immutable and widen the counters in a forward-only
-- migration so large historical reclassification windows cannot overflow.
ALTER TABLE watchdog_flow.flow_reclassification_generations
  MODIFY COLUMN raw_bytes Decimal(39, 0),
  MODIFY COLUMN raw_packets Decimal(39, 0),
  MODIFY COLUMN estimated_bytes Decimal(39, 0),
  MODIFY COLUMN estimated_packets Decimal(39, 0);
