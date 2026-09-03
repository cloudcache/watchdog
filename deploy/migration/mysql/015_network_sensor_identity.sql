ALTER TABLE network_device_sensors
  DROP INDEX uq_network_device_sensors_index,
  ADD UNIQUE KEY uq_network_device_sensors_identity (
    tenant_id,
    device_id,
    sensor_class,
    sensor_index,
    oid(191)
  );
