-- RawFlow is replayed from Kafka and decoded downstream. Collector-local
-- decoder/quality checkpoint restoration was removed with the legacy data
-- plane, so its control-plane receipt table no longer has a producer or reader.
DROP TABLE IF EXISTS collector_state_restore_receipts;
