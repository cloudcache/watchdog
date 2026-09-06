package watchdog

import (
	"context"
	"testing"
)

func TestMySQLDeviceSensorPages(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO targets (id, tenant_id, name, kind, host, status) VALUES
		  ('tgt_sensor_a', ?, 'Sensor A', 'network', '192.0.2.30', 'up'),
		  ('tgt_sensor_b', ?, 'Sensor B', 'network', '192.0.2.31', 'up')
	`, tenant, tenant); err != nil {
		t.Fatal(err)
	}
	for _, device := range []NetworkDevice{
		{ID: "dev_sensor_a", TenantID: tenant, TargetID: "tgt_sensor_a", SysName: "sensor-a", Vendor: "V", Model: "M", SNMPPort: 161},
		{ID: "dev_sensor_b", TenantID: tenant, TargetID: "tgt_sensor_b", SysName: "sensor-b", Vendor: "V", Model: "M", SNMPPort: 161},
	} {
		if _, err := store.UpsertDevice(ctx, device); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.UpsertDeviceSensors(ctx, []NetworkDeviceSensor{
		{ID: "sensor_temp_ok", TenantID: tenant, DeviceID: "dev_sensor_a", SensorIndex: 1, Class: "temperature", Name: "Temp inlet", OID: ".1.1", Unit: "C", Value: 25, Status: "ok"},
		{ID: "sensor_temp_warn", TenantID: tenant, DeviceID: "dev_sensor_a", SensorIndex: 2, Class: "temperature", Name: "Temp outlet", OID: ".1.2", Unit: "C", Value: 80, Status: "warning"},
		{ID: "sensor_temp_crit", TenantID: tenant, DeviceID: "dev_sensor_a", SensorIndex: 3, Class: "temperature", Name: "Temp CPU", OID: ".1.3", Unit: "C", Value: 100, Status: "critical"},
		{ID: "sensor_fan_bad", TenantID: tenant, DeviceID: "dev_sensor_a", SensorIndex: 4, Class: "fan", Name: "Fan tray", OID: ".1.4", Unit: "rpm", Value: 0, Status: "down"},
		{ID: "sensor_other", TenantID: tenant, DeviceID: "dev_sensor_b", SensorIndex: 1, Class: "temperature", Name: "Other device", OID: ".2.1", Unit: "C", Value: 90, Status: "critical"},
	}); err != nil {
		t.Fatal(err)
	}

	sensors, total, err := store.ListDeviceSensorsPage(ctx, tenant, "dev_sensor_a", DeviceSensorQuery{
		Class: "temperature", Health: "problem", Sort: "value", Desc: true, Limit: 1, Offset: 1,
	})
	if err != nil || total != 2 || len(sensors) != 1 || sensors[0].ID != "sensor_temp_warn" {
		t.Fatalf("sensor page=%+v total=%d err=%v", sensors, total, err)
	}
	sensors, total, err = store.ListDeviceSensorsPage(ctx, tenant, "dev_sensor_a", DeviceSensorQuery{
		Search: "outlet", Status: "warning", Sort: "name", Limit: 10,
	})
	if err != nil || total != 1 || len(sensors) != 1 || sensors[0].ID != "sensor_temp_warn" {
		t.Fatalf("sensor search=%+v total=%d err=%v", sensors, total, err)
	}
	sensors, total, err = store.ListDeviceSensorsPage(ctx, tenant, "dev_sensor_a", DeviceSensorQuery{Search: "%", Sort: "class", Limit: 10})
	if err != nil || total != 0 || len(sensors) != 0 {
		t.Fatalf("escaped sensor search=%+v total=%d err=%v", sensors, total, err)
	}
	counts, err := store.CountDeviceSensors(ctx, tenant, "dev_sensor_a")
	if err != nil || counts.Total != 4 || counts.Problems != 3 {
		t.Fatalf("sensor counts=%+v err=%v", counts, err)
	}
}
