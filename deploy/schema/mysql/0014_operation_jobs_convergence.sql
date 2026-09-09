-- KISS-04/KISS-05: there is one operation-job state machine. 0010 introduced
-- the complete lease/retry/checkpoint engine as async_jobs because the early
-- v2 baseline still contained an unused, incomplete operation_jobs draft.
-- No runtime ever wrote the draft table. Preserve the complete table and its
-- rows, remove the draft, and give the engine its canonical name.

DROP TABLE IF EXISTS operation_jobs;
RENAME TABLE async_jobs TO operation_jobs;
