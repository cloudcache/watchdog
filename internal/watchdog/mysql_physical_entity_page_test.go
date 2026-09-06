package watchdog

import (
	"context"
	"testing"
)

func TestMySQLListDevicePhysicalEntitiesPage(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	const targetID, deviceID = ID("target_inventory_page"), ID("device_inventory_page")
	if _, err := db.ExecContext(ctx, `INSERT INTO targets (id, tenant_id, name, kind, host, status) VALUES (?, ?, 'Inventory Device', 'network', 'inventory.example', 'up')`, targetID, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertDevice(ctx, NetworkDevice{ID: deviceID, TenantID: tenant, TargetID: targetID, SysName: "inventory-device", SNMPPort: 161}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertDevicePhysicalEntities(ctx, tenant, deviceID, []PhysicalEntity{
		{Index: 1, Name: "Chassis", Description: "Core chassis", Class: "module", ManufacturerName: "Acme", ModelName: "Router-X", SerialNumber: "CH-001", HardwareRevision: "A", IsFRU: false},
		{Index: 2, Name: "PSU Bravo", Description: "Primary supply", Class: "powerSupply", ManufacturerName: "Beta", ModelName: "PSU-2", SerialNumber: "PS-002", HardwareRevision: "B", IsFRU: true},
		{Index: 3, Name: "PSU Alpha", Description: "Backup supply", Class: "powerSupply", ManufacturerName: "Acme", ModelName: "PSU-1", SerialNumber: "PS-001", HardwareRevision: "A", IsFRU: true},
		{Index: 4, Name: "Fan", Description: "Cooling", Class: "fan", ManufacturerName: "CoolCo", ModelName: "FAN-1", SerialNumber: "FN-001", HardwareRevision: "C", IsFRU: true},
	}); err != nil {
		t.Fatal(err)
	}

	indices := func(entities []PhysicalEntity) []uint64 {
		result := make([]uint64, 0, len(entities))
		for _, entity := range entities {
			result = append(result, entity.Index)
		}
		return result
	}
	page := func(query PhysicalEntityQuery) ([]uint64, int) {
		t.Helper()
		entities, total, err := store.ListDevicePhysicalEntitiesPage(ctx, tenant, deviceID, query)
		if err != nil {
			t.Fatalf("page %+v: %v", query, err)
		}
		return indices(entities), total
	}

	if got, total := page(PhysicalEntityQuery{Limit: 2}); !equalUint64s(got, []uint64{1, 2}) || total != 4 {
		t.Fatalf("first page = %v total=%d", got, total)
	}
	if got, total := page(PhysicalEntityQuery{Limit: 2, Offset: 2}); !equalUint64s(got, []uint64{3, 4}) || total != 4 {
		t.Fatalf("second page = %v total=%d", got, total)
	}
	if got, total := page(PhysicalEntityQuery{Search: "backup", Limit: 10}); !equalUint64s(got, []uint64{3}) || total != 1 {
		t.Fatalf("search page = %v total=%d", got, total)
	}
	fru := true
	if got, total := page(PhysicalEntityQuery{Class: "powerSupply", FRU: &fru, Sort: "name", Limit: 10}); !equalUint64s(got, []uint64{3, 2}) || total != 2 {
		t.Fatalf("filtered page = %v total=%d", got, total)
	}
	if got, total := page(PhysicalEntityQuery{Sort: "manufacturer", Desc: true, Limit: 10}); !equalUint64s(got, []uint64{4, 2, 3, 1}) || total != 4 {
		t.Fatalf("descending page = %v total=%d", got, total)
	}
}

func equalUint64s(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
