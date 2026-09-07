package watchdog

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

func (p ClickHouseFlowQueryProvider) queryAddressSets(
	ctx context.Context,
	request QueryProviderRequest,
	parameters flowAggregateQueryParameters,
	view flowquery.View,
) (QueryProviderResult, error) {
	if p.AddressSetRunner == nil {
		return QueryProviderResult{}, ErrQueryProviderUnavailable
	}
	if request.StepSeconds != 0 && request.StepSeconds != uint32(time.Minute/time.Second) {
		return QueryProviderResult{}, &QueryGatewayError{
			Code: QueryErrorInvalidRequest, Message: "synchronous address-set combinations use a fixed 60-second step",
		}
	}
	compiled, err := flowquery.CompileAddressSet(flowquery.Scope{
		TenantID: string(request.TenantID), AllowedViews: []flowquery.View{view},
	}, flowquery.AddressSetRequest{
		From: request.From, To: request.To, Bucket: flowquery.BucketOneMinute,
		Metric: parameters.Metric, View: view, Endpoint: parameters.AddressSetEndpoint,
		Sets: parameters.AddressSetFilter,
		Filters: flowquery.DetailFilters{
			Directions: parameters.Filters.Directions, Categories: parameters.Filters.Categories,
			Businesses: parameters.Filters.Businesses, TargetIDs: parameters.Filters.TargetIDs,
			DeviceIDs: parameters.Filters.DeviceIDs, ExporterIDs: parameters.Filters.ExporterIDs,
		},
	}, p.now())
	if err != nil {
		return QueryProviderResult{}, mapFlowQueryError(err)
	}
	result, err := p.AddressSetRunner.Run(ctx, compiled)
	if err != nil {
		return QueryProviderResult{}, mapFlowQueryError(err)
	}
	if uint64(len(result.Points)) > uint64(request.Limit) {
		return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorRowLimit, Message: "Flow address-set result exceeds the query row limit"}
	}
	label := flowAddressSetLabel(result)
	points := make([]flowquery.Point, 0, len(result.Points))
	for _, point := range result.Points {
		points = append(points, flowquery.Point{
			Bucket: point.Bucket, DimensionValue: label, DimensionSnapshotID: point.DimensionSnapshotID,
			GeoVersion: point.GeoVersion, ClassificationVersion: point.ClassificationVersion,
			Value: point.Value, ReceivedRecords: point.ReceivedRecords,
			UnknownSamplingRecords: point.UnknownSamplingRecords, QualityRecords: point.QualityRecords,
			SamplingCompleteness:      point.SamplingCompleteness,
			SamplingCompletenessKnown: point.SamplingCompletenessKnown,
			QualityRecordRatio:        point.QualityRecordRatio, QualityRecordRatioKnown: point.QualityRecordRatioKnown,
		})
	}
	plan := &flowquery.AggregatePlan{
		RequestedFrom: request.From.UTC(), RequestedTo: request.To.UTC(),
		EffectiveFrom: compiled.From, EffectiveTo: compiled.To,
		Source: flowquery.BucketFlowRecords, SourceStep: time.Minute, Interval: time.Minute,
		SourceSeconds: 60, StepSeconds: 60, TargetPoints: parameters.TargetPoints,
	}
	publicResult := flowquery.Result{
		Points: points, Metric: result.Metric,
		Dimension: flowquery.DimensionDefinition{Kind: flowquery.DimensionAddressSet, Additive: false},
		Plan:      plan, MixedVersions: result.MixedVersions, VersionCount: result.VersionCount,
	}
	data, err := marshalFlowAddressSetResult(publicResult, parameters.Table, result)
	if err != nil {
		return QueryProviderResult{}, fmt.Errorf("marshal Flow address-set result: %w", err)
	}
	completeness := QueryCompleteness{
		AvailableFrom: &compiled.From, AvailableTo: &compiled.To, CompleteRatio: 1, Partial: true,
		UnknownRatio: flowUnknownSamplingRatio(points),
		Warnings:     []string{"address-set combinations are deduplicated from flow_records; end-to-end ingest coverage is reported separately"},
	}
	if result.MixedVersions {
		completeness.Warnings = append(completeness.Warnings, "result contains multiple dimension or classification versions")
	}
	return QueryProviderResult{
		Data: data, Unit: result.Metric.Unit, Timezone: "UTC", StepSeconds: 60, AsOf: p.now(),
		Versions: flowResultVersions(points), Completeness: completeness,
	}, nil
}

func flowAddressSetLabel(result flowquery.AddressSetResult) string {
	if len(result.Sets.IncludeAny) == 1 && len(result.Sets.IncludeAll) == 0 && len(result.Sets.ExcludeAny) == 0 {
		return result.Sets.IncludeAny[0]
	}
	return "address-set combination"
}
