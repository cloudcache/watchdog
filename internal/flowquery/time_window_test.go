package flowquery

import (
	"strings"
	"testing"
	"time"
)

func TestInLocalTimeWindowsHandlesCrossMidnightAndISOWeekdays(t *testing.T) {
	windows := []LocalTimeWindow{{Days: []uint8{1}, StartLocal: "20:00", EndLocal: "02:00"}}
	for _, test := range []struct {
		name string
		at   string
		want bool
	}{
		{"monday evening", "2026-09-07T12:30:00Z", true},
		{"tuesday inherited after midnight", "2026-09-07T16:30:00Z", true},
		{"end is exclusive", "2026-09-07T18:00:00Z", false},
		{"tuesday evening is not monday window", "2026-09-08T12:30:00Z", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			at, err := time.Parse(time.RFC3339, test.at)
			if err != nil {
				t.Fatal(err)
			}
			if got := InLocalTimeWindows(at, windows, "Asia/Singapore"); got != test.want {
				t.Fatalf("InLocalTimeWindows() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCompileFlowTimeWindowsUsesTypedTimezoneAndBucketColumns(t *testing.T) {
	from := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	windows := []LocalTimeWindow{{Days: []uint8{1, 2, 3, 4, 5}, StartLocal: "12:00", EndLocal: "14:00"}}
	compiled, err := Compile(Scope{AllowedViews: []View{ViewCustomer}}, Request{
		From: from, To: from.Add(time.Hour), Bucket: BucketOneMinute, Metric: MetricEstimatedBPS,
		Dimension: DimensionCategory, View: ViewCustomer, TopN: 20, IncludeOther: true,
		Timezone: "Asia/Singapore", TimeWindows: windows,
	}, from.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(compiled.Query.Body, "toTimeZone(bucket, {time_window_timezone:String})") ||
		queryParameter(compiled.Query, "time_window_timezone") != "'Asia/Singapore'" ||
		queryParameter(compiled.Query, "time_window_0_start") != "'720'" {
		t.Fatalf("query=%s parameters=%+v", compiled.Query.Body, compiled.Query.Parameters)
	}

	joint, err := CompileJoint(Scope{AllowedViews: []View{ViewCustomer}}, JointRequest{
		From: from, To: from.Add(time.Hour), Metric: MetricEstimatedBPS,
		Dimensions: []Dimension{DimensionSourceIP}, View: ViewCustomer, TopN: 20, IncludeOther: true,
		Timezone: "Asia/Singapore", TimeWindows: windows,
	}, from.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(joint.Query.Body, "toTimeZone(event_time, {time_window_timezone:String})") {
		t.Fatalf("joint query = %s", joint.Query.Body)
	}
}

func TestCompileFlowTimeWindowsRejectsInvalidWindow(t *testing.T) {
	from := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	_, err := Compile(Scope{}, Request{
		From: from, To: from.Add(time.Hour), Bucket: BucketOneMinute, Metric: MetricEstimatedBPS,
		Dimension: DimensionCategory, View: ViewCustomer, TopN: 20,
		Timezone: "UTC", TimeWindows: []LocalTimeWindow{{Days: []uint8{1}, StartLocal: "12:00", EndLocal: "12:00"}},
	}, from.Add(2*time.Hour))
	if !IsRequestError(err, "time_windows", ErrorInvalid) {
		t.Fatalf("error = %v", err)
	}
}
