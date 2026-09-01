package watchdog

import "errors"

type BillingComputation struct {
	Account       BillingAccount
	Period        BillingPeriod
	ComputedValue float64
	TotalBytes    uint64
	QuotaExceeded bool
}

func ComputeBillingPeriod(account BillingAccount, period BillingPeriod, samples []Sample) (BillingComputation, error) {
	account = normalizeBillingAccount(account)
	period = normalizeBillingPeriod(period)
	if period.BillingAccountID != "" && period.BillingAccountID != account.ID {
		return BillingComputation{}, errors.New("billing period does not belong to account")
	}
	if len(sampleValues(samples)) == 0 {
		return BillingComputation{}, errors.New("no samples")
	}
	// Samples are bps rates; volume is their time integral, never their sum.
	totalBytes := estimateBillingBytes(samples, billingStep(period))
	var value float64
	if account.Aggregation == AggregationTotalBytes {
		value = float64(totalBytes)
	} else {
		aggregated, err := Aggregate(samples, account.Aggregation)
		if err != nil {
			return BillingComputation{}, err
		}
		value = aggregated
	}
	return BillingComputation{
		Account:       account,
		Period:        period,
		ComputedValue: value,
		TotalBytes:    totalBytes,
		QuotaExceeded: account.QuotaBytes > 0 && totalBytes > account.QuotaBytes,
	}, nil
}
