// SPDX-License-Identifier: AGPL-3.0-only

package billing

import (
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/snmpch"
)

func TestCommonCompleteBillingRatesUsesOneSharedDenominator(t *testing.T) {
	from := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	period := Period{DateFrom: from, DateTo: from.Add(10 * time.Minute)}
	snmpResult := snmpch.BillingResult{From: period.DateFrom, To: period.DateTo, ExpectedBuckets: 2, ExpectedPorts: 1,
		Buckets: []snmpch.BillingBucket{
			{Time: from, SelectedBPS: 1000, PresentPorts: 1, Coverage: 1},
			{Time: from.Add(5 * time.Minute), SelectedBPS: 2000, PresentPorts: 1, Coverage: 1},
		}}
	flowResult := flowch.FlowBillingResult{From: period.DateFrom, To: period.DateTo, Layers: []flowch.BillingLayerResult{
		{Layer: "raw", ExpectedBuckets: 2, RateBuckets: testRateBuckets(from, 800, 900, true, false)},
		{Layer: "supplier", ExpectedBuckets: 2, RateBuckets: testRateBuckets(from, 700, 800, true, true)},
		{Layer: "customer", ExpectedBuckets: 2, RateBuckets: testRateBuckets(from, 600, 700, true, true)},
	}}
	rates, count, err := commonCompleteBillingRates(period, snmpResult, flowResult)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(rates[LayerSNMP]) != 1 || rates[LayerSNMP][0].Value != 1000 || rates[LayerRaw][0].Value != 800 || rates[LayerSupplier][0].Value != 700 || rates[LayerCustomer][0].Value != 600 {
		t.Fatalf("common rates=%+v count=%d", rates, count)
	}
}

func TestCommonCompleteBillingRatesExcludesSNMPGapFromEveryLayer(t *testing.T) {
	from := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	period := Period{DateFrom: from, DateTo: from.Add(5 * time.Minute)}
	snmpResult := snmpch.BillingResult{From: from, To: period.DateTo, ExpectedBuckets: 1, ExpectedPorts: 1,
		Buckets: []snmpch.BillingBucket{{Time: from, SelectedBPS: 1000, PresentPorts: 1, Coverage: .8, Gap: true}}}
	flowResult := flowch.FlowBillingResult{From: from, To: period.DateTo, Layers: []flowch.BillingLayerResult{
		{Layer: "raw", ExpectedBuckets: 1, RateBuckets: testRateBuckets(from, 800, 0, true, false)},
		{Layer: "supplier", ExpectedBuckets: 1, RateBuckets: testRateBuckets(from, 700, 0, true, false)},
		{Layer: "customer", ExpectedBuckets: 1, RateBuckets: testRateBuckets(from, 600, 0, true, false)},
	}}
	rates, count, err := commonCompleteBillingRates(period, snmpResult, flowResult)
	if err != nil || count != 0 || len(rates[LayerRaw]) != 0 || len(rates[LayerSNMP]) != 0 {
		t.Fatalf("gap rates=%+v count=%d err=%v", rates, count, err)
	}
}

func TestBillingReaderWindowMustExactlyMatchPeriod(t *testing.T) {
	from := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	period := Period{DateFrom: from, DateTo: from.Add(time.Hour)}
	if !billingReadWindowsMatch(period, snmpch.BillingResult{From: period.DateFrom, To: period.DateTo}, flowch.FlowBillingResult{From: period.DateFrom, To: period.DateTo}) {
		t.Fatal("matching window was rejected")
	}
	if billingReadWindowsMatch(period, snmpch.BillingResult{From: period.DateFrom.Add(time.Minute), To: period.DateTo}, flowch.FlowBillingResult{From: period.DateFrom, To: period.DateTo}) {
		t.Fatal("shifted SNMP window was accepted")
	}
}

func TestCommonCompleteBillingRatesRejectsUntimestampedFallback(t *testing.T) {
	from := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	period := Period{DateFrom: from, DateTo: from.Add(5 * time.Minute)}
	snmpResult := snmpch.BillingResult{From: from, To: period.DateTo, ExpectedBuckets: 1, ExpectedPorts: 1,
		Buckets: []snmpch.BillingBucket{{Time: from, SelectedBPS: 1000, PresentPorts: 1, Coverage: 1}}}
	flowResult := flowch.FlowBillingResult{From: from, To: period.DateTo, Layers: []flowch.BillingLayerResult{
		{Layer: "raw", ExpectedBuckets: 1, SelectedRates: []float64{800}, UnknownSamplingRecords: 1},
		{Layer: "supplier", ExpectedBuckets: 1, SelectedRates: []float64{700}},
		{Layer: "customer", ExpectedBuckets: 1, SelectedRates: []float64{600}},
	}}
	if _, _, err := commonCompleteBillingRates(period, snmpResult, flowResult); err == nil {
		t.Fatal("untimestamped SelectedRates fallback was accepted")
	}
}

func testRateBuckets(from time.Time, first, second float64, firstComplete, secondComplete bool) []flowch.BillingRateBucket {
	items := []flowch.BillingRateBucket{{Time: from, SelectedBPS: first, Observed: true, Complete: firstComplete}}
	if second != 0 || secondComplete {
		items = append(items, flowch.BillingRateBucket{Time: from.Add(5 * time.Minute), SelectedBPS: second, Observed: true, Complete: secondComplete})
	}
	return items
}
