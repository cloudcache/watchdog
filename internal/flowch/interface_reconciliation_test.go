// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type interfaceReconciliationExecutor struct {
	t     *testing.T
	query ch.Query
}

func (e *interfaceReconciliationExecutor) Do(ctx context.Context, query ch.Query) error {
	e.query = query
	results := query.Result.(proto.Results)
	for _, result := range results {
		switch result.Name {
		case "bucket":
			result.Data.(*proto.ColDateTime).Append(time.Date(2026, 9, 19, 2, 0, 0, 0, time.UTC))
		case "device_id":
			result.Data.(*proto.ColStr).Append("device-a")
		case "port_id":
			result.Data.(*proto.ColStr).Append("port-72")
		case "if_index":
			result.Data.(*proto.ColUInt32).Append(72)
		case "flow_in_bytes":
			result.Data.(*proto.ColUInt64).Append(900)
		case "flow_out_bytes":
			result.Data.(*proto.ColUInt64).Append(1900)
		case "flow_records":
			result.Data.(*proto.ColUInt64).Append(20)
		case "flow_unknown":
			result.Data.(*proto.ColUInt64).Append(1)
		case "counter_in_bytes":
			result.Data.(*proto.ColUInt64).Append(1000)
		case "counter_out_bytes":
			result.Data.(*proto.ColUInt64).Append(2000)
		case "counter_in_coverage", "counter_out_coverage", "snmp_coverage":
			result.Data.(*proto.ColFloat64).Append(1)
		case "counter_in_reset", "counter_out_reset", "snmp_reset", "snmp_gap":
			result.Data.(*proto.ColUInt8).Append(0)
		case "snmp_in_bytes":
			result.Data.(*proto.ColUInt64).Append(1100)
		case "snmp_out_bytes":
			result.Data.(*proto.ColUInt64).Append(2200)
		default:
			e.t.Fatalf("unhandled result column %q", result.Name)
		}
	}
	return query.OnResult(ctx, proto.Block{Columns: len(results), Rows: 1})
}

func TestReadInterfaceReconciliationUsesDeviceIfIndexAndPublishedSNMP(t *testing.T) {
	exec := &interfaceReconciliationExecutor{t: t}
	reader := &NativeInserter{executor: exec}
	from := time.Date(2026, 9, 19, 2, 0, 0, 0, time.UTC)
	result, err := reader.ReadInterfaceReconciliation(context.Background(), InterfaceReconciliationRequest{
		Scopes: []InterfaceReconciliationScope{{DeviceID: "device-a", PortID: "port-72", IfIndex: 72}},
		From:   from, To: from.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Buckets) != 1 {
		t.Fatalf("buckets=%+v", result.Buckets)
	}
	bucket := result.Buckets[0]
	if bucket.FlowToCounterInRatio != 0.9 || !bucket.FlowToCounterInValid || bucket.FlowToCounterOutRatio != 0.95 || !bucket.FlowToCounterOutValid {
		t.Fatalf("unexpected Flow/counter ratios: %+v", bucket)
	}
	if bucket.FlowToSNMPInRatio != 900.0/1100 || bucket.CounterToSNMPOutRatio != 2000.0/2200 {
		t.Fatalf("unexpected SNMP ratios: %+v", bucket)
	}
	for _, required := range []string{
		"records.device_id=scope.device_id", "records.egress_if_index=scope.if_index",
		"counters.device_id=scope.device_id", "counters.if_index=scope.if_index",
		"values.generation=published.generation", "values.port_id=scope.port_id",
	} {
		if !strings.Contains(exec.query.Body, required) {
			t.Fatalf("query is missing %q", required)
		}
	}
	if exec.query.ExternalTable != "interface_reconciliation_scope" || len(exec.query.ExternalData) != 3 {
		t.Fatalf("unexpected bounded scope: table=%q columns=%d", exec.query.ExternalTable, len(exec.query.ExternalData))
	}
}

func TestInterfaceReconciliationValidationAndZeroDenominator(t *testing.T) {
	if ratio, valid := evidenceRatio(10, 0); ratio != 0 || valid {
		t.Fatalf("zero denominator ratio=%v valid=%v", ratio, valid)
	}
	if _, err := normalizeInterfaceReconciliationScopes([]InterfaceReconciliationScope{
		{DeviceID: "device-a", PortID: "port-a", IfIndex: 72},
		{DeviceID: "device-a", PortID: "port-b", IfIndex: 72},
	}); err == nil {
		t.Fatal("conflicting port mapping was accepted")
	}
	reader := &NativeInserter{executor: &interfaceReconciliationExecutor{t: t}}
	from := time.Date(2026, 9, 19, 2, 1, 0, 0, time.UTC)
	if _, err := reader.ReadInterfaceReconciliation(context.Background(), InterfaceReconciliationRequest{
		Scopes: []InterfaceReconciliationScope{{DeviceID: "device-a", PortID: "port-72", IfIndex: 72}},
		From:   from, To: from.Add(5 * time.Minute),
	}); err == nil {
		t.Fatal("unaligned reconciliation interval was accepted")
	}
	budget := DefaultInterfaceReconciliationBudget()
	budget.MaxResultRows = 1
	if _, err := reader.ReadInterfaceReconciliation(context.Background(), InterfaceReconciliationRequest{
		Scopes: []InterfaceReconciliationScope{{DeviceID: "device-a", PortID: "port-72", IfIndex: 72}},
		From:   from.Truncate(5 * time.Minute), To: from.Truncate(5 * time.Minute).Add(10 * time.Minute), Budget: budget,
	}); err == nil {
		t.Fatal("result row budget was not enforced")
	}
}
