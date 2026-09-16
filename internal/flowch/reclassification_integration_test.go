// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"math"
	"net/netip"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

// TestRealClickHouseHistoricalReclassification proves the complete derived
// generation contract against ClickHouse: bounded cursor paging, retry-safe
// inserts, natural Kafka coordinate coverage, count/counter conservation and
// a durable completion marker. It owns an isolated database and never touches
// development data.
func TestRealClickHouseHistoricalReclassification(t *testing.T) {
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_reclassification")
	eventTime := time.Date(2026, 9, 6, 2, 0, 0, 0, time.UTC)
	first := integrationRecord(1, eventTime.Add(10*time.Second), "source-city", 100)
	second := integrationRecord(2, eventTime.Add(20*time.Second), "source-city", 200)
	first.Dimensions.SnapshotID, second.Dimensions.SnapshotID = "source-dimension", "source-dimension"
	first.ClassificationVersion, second.ClassificationVersion = 1, 1
	insertIntegrationBatch(t, ctx, native, integrationBatch(40, eventTime.Add(time.Minute), first, second))

	runner, err := NewReclassificationRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	overflowTime := eventTime.Add(2 * time.Hour)
	overflowFirst := integrationRecord(3, overflowTime.Add(10*time.Second), "source-city", 1)
	overflowSecond := integrationRecord(4, overflowTime.Add(20*time.Second), "source-city", 1)
	for _, record := range []*flowworker.EnrichedRecord{&overflowFirst, &overflowSecond} {
		record.Dimensions.SnapshotID = "source-overflow"
		record.ClassificationVersion = 1
		record.RawBytes, record.RawPackets = math.MaxUint64, math.MaxUint64
		record.EstimatedBytes, record.EstimatedPackets = math.MaxUint64, math.MaxUint64
	}
	insertIntegrationBatch(t, ctx, native, integrationBatch(41, overflowTime.Add(time.Minute), overflowFirst))
	insertIntegrationBatch(t, ctx, native, integrationBatch(42, overflowTime.Add(2*time.Minute), overflowSecond))
	overflowSpec := ReclassificationSpec{
		ID: "reclassification-overflow", Generation: 1,
		SourcePublicationID: "publication-overflow", TargetPublicationID: "publication-overflow-target", View: "customer",
		WindowStart: overflowTime, WindowEnd: overflowTime.Add(time.Hour),
		SourceDimensionSnapshotID: "source-overflow", SourceClassificationVersion: 1,
	}
	overflowEvidence, overflowErr := runner.SourceEvidence(ctx, overflowSpec)
	if got := mustReclassificationEvidence(t, overflowEvidence, overflowErr); !got.Equal(ReclassificationEvidence{
		RecordCount: 2, RawBytes: "36893488147419103230", RawPackets: "36893488147419103230",
		EstimatedBytes: "36893488147419103230", EstimatedPackets: "36893488147419103230", EstimatedValidRecords: 2,
	}) {
		t.Fatalf("overflow-safe evidence=%+v", got)
	}
	spec := ReclassificationSpec{
		ID: "reclassification-it", Generation: 7,
		SourcePublicationID: "publication-source", TargetPublicationID: "publication-target", View: "customer",
		WindowStart: eventTime, WindowEnd: eventTime.Add(time.Hour),
		SourceDimensionSnapshotID: "source-dimension", SourceClassificationVersion: 1,
	}
	wantEvidence := ReclassificationEvidence{
		RecordCount: 2, RawBytes: "300", RawPackets: "2",
		EstimatedBytes: "3000", EstimatedPackets: "20", EstimatedValidRecords: 2,
	}
	sourceEvidence, sourceEvidenceErr := runner.SourceEvidence(ctx, spec)
	if got := mustReclassificationEvidence(t, sourceEvidence, sourceEvidenceErr); !got.Equal(wantEvidence) {
		t.Fatalf("source evidence=%+v, want %+v", got, wantEvidence)
	}
	if diff, diffErr := runner.CoordinateDiff(ctx, spec); diffErr != nil || diff != 2 {
		t.Fatalf("empty output coordinate diff=%d error=%v, want 2", diff, diffErr)
	}

	classification, err := flowdimension.CompileClassification(flowdimension.ClassificationDefinition{
		Version: 2, EffectiveFrom: eventTime, DimensionSnapshotID: "target-dimension",
		HomeProvince: "330000", HomeCity: "330100", OverseasIncludesHMT: true,
		InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	target := reclassificationTestSnapshot{metadata: flowdimension.SnapshotMetadata{
		SchemaVersion: flowdimension.BundleSchemaVersion, SnapshotID: "target-dimension", Version: 2, EffectiveFrom: eventTime,
	}}
	cursor := ReclassificationCursor{}
	pageNumber := uint64(0)
	for {
		page, readErr := runner.ReadSourcePage(ctx, spec, cursor, 1)
		if readErr != nil {
			t.Fatal(readErr)
		}
		for index, raw := range page.Records {
			reclassified, reclassifyErr := flowworker.ReclassifyRecord(raw, target, classification)
			if reclassifyErr != nil {
				t.Fatal(reclassifyErr)
			}
			page.Batches[index].Records[0] = reclassified
		}
		if len(page.Batches) > 0 {
			pageNumber++
			if err := runner.InsertPage(ctx, spec, pageNumber, page.Batches); err != nil {
				t.Fatal(err)
			}
			// Simulate an ambiguous acknowledgement. The stable page token and
			// natural coordinate key must make the retry converge.
			if err := runner.InsertPage(ctx, spec, pageNumber, page.Batches); err != nil {
				t.Fatal(err)
			}
			cursor = page.Cursor
		}
		if page.Done {
			break
		}
	}

	outputEvidence, outputEvidenceErr := runner.OutputEvidence(ctx, spec)
	if got := mustReclassificationEvidence(t, outputEvidence, outputEvidenceErr); !got.Equal(wantEvidence) {
		t.Fatalf("output evidence=%+v, want %+v", got, wantEvidence)
	}
	if diff, diffErr := runner.CoordinateDiff(ctx, spec); diffErr != nil || diff != 0 {
		t.Fatalf("complete output coordinate diff=%d error=%v", diff, diffErr)
	}
	if err := runner.WriteCompletionMarker(ctx, spec); err != nil {
		t.Fatal(err)
	}
	if err := runner.WriteCompletionMarker(ctx, spec); err != nil {
		t.Fatal(err)
	}
	if exists, markerErr := runner.MarkerExists(ctx, spec); markerErr != nil || !exists {
		t.Fatalf("marker exists=%t error=%v", exists, markerErr)
	}
	assertReclassificationProjection(t, ctx, native, spec)
	assertReclassificationQueries(t, ctx, native, spec)
}

type reclassificationTestSnapshot struct {
	metadata flowdimension.SnapshotMetadata
}

func (s reclassificationTestSnapshot) Metadata() flowdimension.SnapshotMetadata { return s.metadata }

func (s reclassificationTestSnapshot) ClassifyEndpoints(source, destination netip.Addr) flowdimension.ClassifiedEndpoints {
	return flowdimension.ClassifiedEndpoints{
		SnapshotID: s.metadata.SnapshotID, Version: s.metadata.Version, Direction: flowdimension.DirectionOut, Business: "reclassified-business",
		Local:  flowdimension.EndpointDimension{IP: source, Side: flowdimension.EndpointSrc, PrefixID: "target-local", PrefixCIDR: "10.0.0.0/8"},
		Remote: flowdimension.EndpointDimension{IP: destination, Side: flowdimension.EndpointDst, PrefixID: "target-remote", PrefixCIDR: "203.0.113.0/24"},
	}
}

func (reclassificationTestSnapshot) ResolveAddress(address netip.Addr) (flowdimension.AddressSnapshotResolution, bool) {
	if !address.IsValid() {
		return flowdimension.AddressSnapshotResolution{}, false
	}
	geo := flowdimension.GeoInfo{Country: "US", CountryID: "US", ContinentID: "NorthAmerica", Version: "target-dimension", Source: flowdimension.GeoSchemaV2, ASN: 64512}
	return flowdimension.AddressSnapshotResolution{SupplierGeo: geo, CustomerGeo: geo, SupplierASN: 64512, CustomerASN: 64512}, true
}

func mustReclassificationEvidence(t *testing.T, evidence ReclassificationEvidence, err error) ReclassificationEvidence {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return evidence
}

func assertReclassificationProjection(t *testing.T, ctx context.Context, native *NativeInserter, spec ReclassificationSpec) {
	t.Helper()
	var rows, versions proto.ColUInt64
	query := ch.Query{
		Body:       `SELECT count(), uniqExact(classification_version) FROM flow_reclassified_records FINAL WHERE reclassification_id={id:String} AND reclassification_generation={generation:UInt64}`,
		Parameters: reclassificationParameters(spec),
		Result:     proto.Results{{Name: "count()", Data: &rows}, {Name: "uniqExact(classification_version)", Data: &versions}},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error { return nil }
	if err := native.executor.Do(ctx, query); err != nil {
		t.Fatal(err)
	}
	if rows.Rows() != 1 || rows[0] != 2 || versions[0] != 1 {
		t.Fatalf("derived projection rows=%v classification versions=%v", rows, versions)
	}
}

func assertReclassificationQueries(t *testing.T, ctx context.Context, native *NativeInserter, spec ReclassificationSpec) {
	t.Helper()
	runner, err := flowquery.NewJointRunner(native.executor)
	if err != nil {
		t.Fatal(err)
	}
	for _, view := range []flowquery.View{flowquery.ViewCustomer, flowquery.ViewSupplier} {
		compiled, compileErr := flowquery.CompileJoint(flowquery.Scope{AllowedViews: []flowquery.View{view}}, flowquery.JointRequest{
			From: spec.WindowStart, To: spec.WindowEnd, Metric: flowquery.MetricRawBytes,
			Dimensions: []flowquery.Dimension{flowquery.DimensionCategory}, View: view,
			TopN: 10, IncludeOther: true, TargetPoints: 300, Timezone: "UTC",
			Reclassification: &flowquery.ReclassificationSource{ID: spec.ID, Generation: spec.Generation},
		}, spec.WindowEnd.Add(time.Hour))
		if compileErr != nil {
			t.Fatal(compileErr)
		}
		result, runErr := runner.Run(ctx, compiled)
		if runErr != nil {
			t.Fatal(runErr)
		}
		if len(result.Points) != 1 || result.Points[0].DimensionValues[0] != "overseas" || result.Points[0].Value != 300 || result.Points[0].ReceivedRecords != 2 {
			t.Fatalf("%s historical query result=%+v", view, result)
		}
	}
}
