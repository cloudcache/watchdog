package watchdog

import "errors"

func ApproveBillingPeriod(period BillingPeriod) (BillingPeriod, error) {
	if period.Status != BillingPeriodComputed {
		return BillingPeriod{}, errors.New("only computed billing period can be approved")
	}
	period.Status = BillingPeriodApproved
	return period, nil
}

func VoidBillingPeriod(period BillingPeriod) (BillingPeriod, error) {
	switch period.Status {
	case BillingPeriodOpen, BillingPeriodComputed:
		period.Status = BillingPeriodVoid
		return period, nil
	default:
		return BillingPeriod{}, errors.New("billing period cannot be voided from current status")
	}
}
