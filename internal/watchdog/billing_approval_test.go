package watchdog

import "testing"

func TestApproveBillingPeriodRequiresComputed(t *testing.T) {
	period, err := ApproveBillingPeriod(BillingPeriod{Status: BillingPeriodComputed})
	if err != nil {
		t.Fatalf("ApproveBillingPeriod() error = %v", err)
	}
	if period.Status != BillingPeriodApproved {
		t.Fatalf("status = %s", period.Status)
	}
	if _, err := ApproveBillingPeriod(BillingPeriod{Status: BillingPeriodOpen}); err == nil {
		t.Fatal("expected open approval error")
	}
}

func TestVoidBillingPeriodAllowsOpenOrComputed(t *testing.T) {
	for _, status := range []BillingPeriodStatus{BillingPeriodOpen, BillingPeriodComputed} {
		period, err := VoidBillingPeriod(BillingPeriod{Status: status})
		if err != nil {
			t.Fatalf("VoidBillingPeriod(%s) error = %v", status, err)
		}
		if period.Status != BillingPeriodVoid {
			t.Fatalf("status = %s", period.Status)
		}
	}
}

func TestVoidBillingPeriodRejectsApproved(t *testing.T) {
	if _, err := VoidBillingPeriod(BillingPeriod{Status: BillingPeriodApproved}); err == nil {
		t.Fatal("expected approved void error")
	}
}
