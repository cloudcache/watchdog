package watchdog

import (
	"sort"
	"time"
)

func applyTrafficViewResponse(req MetricsQueryRequest, response VictoriaMetricsResponse) VictoriaMetricsResponse {
	bucket := trafficViewBucket(req.TrafficView)
	if bucket <= 0 || !isSNMPTrafficMetric(req.Metric) {
		return response
	}
	response = cloneVMResponse(response)
	for i := range response.Data.Result {
		response.Data.Result[i].Values = maxBucketValues(response.Data.Result[i].Values, bucket)
	}
	return response
}

func maxBucketValues(values []VMValue, bucket time.Duration) []VMValue {
	if bucket <= 0 || len(values) == 0 {
		return values
	}
	type bucketValue struct {
		t     time.Time
		value float64
		set   bool
	}
	buckets := map[int64]bucketValue{}
	for _, sample := range values {
		keyTime := sample.Time.UTC().Truncate(bucket)
		key := keyTime.Unix()
		current := buckets[key]
		if !current.set || sample.Value > current.value {
			buckets[key] = bucketValue{t: keyTime, value: sample.Value, set: true}
		}
	}
	keys := make([]int64, 0, len(buckets))
	for key := range buckets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	result := make([]VMValue, 0, len(keys))
	for _, key := range keys {
		value := buckets[key]
		result = append(result, VMValue{Time: value.t, Value: value.value})
	}
	return result
}
