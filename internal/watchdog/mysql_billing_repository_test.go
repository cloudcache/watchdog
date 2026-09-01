package watchdog

import "testing"

func TestNormalizeBillingAccountDefaults(t *testing.T) {
	account := normalizeBillingAccount(BillingAccount{})
	if account.Status != BillingStatusActive {
		t.Fatalf("status = %s", account.Status)
	}
	if account.BillingDay != 1 {
		t.Fatalf("billing day = %d", account.BillingDay)
	}
	if account.Aggregation != AggregationP95FiveMinute {
		t.Fatalf("aggregation = %s", account.Aggregation)
	}
	if account.ValueMode != ExportValueCorrected {
		t.Fatalf("value mode = %s", account.ValueMode)
	}
}

func TestNormalizeBillingAccountPreservesExplicitValues(t *testing.T) {
	account := normalizeBillingAccount(BillingAccount{
		Status:      BillingStatusPaused,
		BillingDay:  15,
		Aggregation: AggregationTotalBytes,
		ValueMode:   ExportValueBoth,
	})
	if account.Status != BillingStatusPaused || account.BillingDay != 15 || account.Aggregation != AggregationTotalBytes || account.ValueMode != ExportValueBoth {
		t.Fatalf("account = %#v", account)
	}
}

func TestNormalizeBillingPeriodDefaults(t *testing.T) {
	period := normalizeBillingPeriod(BillingPeriod{})
	if period.Status != BillingPeriodOpen {
		t.Fatalf("status = %s", period.Status)
	}
}
