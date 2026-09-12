-- KISS-06 VPN Tier-1 behavioral signals: the source-side address-library prefix
-- and the window's median bytes-per-packet, so flowvpn.Match can gate on
-- LocalPrefixIDs and a packet-size range (small-packet proxy heuristic). Both
-- default empty/zero, which the scorer treats as "unknown" and fails closed on.
ALTER TABLE watchdog_flow.flow_vpn_candidates
  ADD COLUMN IF NOT EXISTS local_prefix_id LowCardinality(String) AFTER remote_prefix_id,
  ADD COLUMN IF NOT EXISTS packet_bytes_p50 UInt64 DEFAULT 0 AFTER max_duration_ms;
