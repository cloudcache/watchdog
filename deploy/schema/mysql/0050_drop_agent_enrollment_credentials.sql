-- Stage 2 of the agent simplification removes the per-agent enrollment/credential
-- mechanism in favour of a single installation-wide shared token
-- (config: agents.shared_token / env WATCHDOG_AGENT_SHARED_TOKEN). Agents register
-- idempotently with the shared token and are authenticated by it; there are no
-- one-time enrollment tokens and no per-agent credentials any more.
--
-- Both tables are leaf children (FK -> agents ON DELETE CASCADE); nothing else
-- references them, so dropping them is safe. This migration is additive and never
-- edits 0003_agents.sql, so already-applied installations keep a stable checksum.
DROP TABLE IF EXISTS agent_enrollment_tokens;
DROP TABLE IF EXISTS agent_credentials;
