// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package watchdog

import (
	"context"
	"time"
)

// These methods preserve the address publication interface while the shared
// store owns the only lifecycle implementation.
func (p *MySQLAddressDimensionPublisher) ApproveAddressDimension(ctx context.Context, tenantID, actorID ID, expected uint64, approval AddressDimensionApproval) (AddressDimensionSnapshot, error) {
	if p == nil {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	return p.ApproveDimensionPublication(ctx, tenantID, actorID, expected, approval)
}

func (p *MySQLAddressDimensionPublisher) RejectAddressDimension(ctx context.Context, tenantID, actorID, snapshotID ID, expected uint64, reason string) (AddressDimensionSnapshot, error) {
	if p == nil {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	return p.RejectDimensionPublication(ctx, tenantID, actorID, snapshotID, expected, reason)
}

func (p *MySQLAddressDimensionPublisher) ActivateAddressDimension(ctx context.Context, tenantID, actorID ID, request AddressDimensionActivationRequest) (AddressDimensionActivation, error) {
	if p == nil {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalid
	}
	return p.ActivateDimensionPublication(ctx, tenantID, actorID, request)
}

func (p *MySQLAddressDimensionPublisher) RollbackAddressDimension(ctx context.Context, tenantID, actorID ID, request AddressDimensionRollbackRequest) (AddressDimensionActivation, error) {
	if p == nil {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalid
	}
	return p.RollbackDimensionPublication(ctx, tenantID, actorID, request)
}

func (p *MySQLAddressDimensionPublisher) RetireAddressDimension(ctx context.Context, tenantID, actorID ID, request AddressDimensionRetireRequest) (AddressDimensionSnapshot, error) {
	if p == nil {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	return p.RetireDimensionPublication(ctx, tenantID, actorID, request)
}

func (p *MySQLAddressDimensionPublisher) GetAddressDimensionActivationAt(ctx context.Context, tenantID ID, eventTime time.Time) (AddressDimensionActivation, error) {
	if p == nil {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalid
	}
	return p.GetDimensionPublicationActivationAt(ctx, tenantID, eventTime)
}

func (p *MySQLAddressDimensionPublisher) ReportAddressDimensionAcknowledgement(ctx context.Context, acknowledgement AddressDimensionAcknowledgement) (AddressDimensionAcknowledgement, error) {
	if p == nil {
		return AddressDimensionAcknowledgement{}, ErrAddressDimensionInvalid
	}
	return p.ReportDimensionPublicationAcknowledgement(ctx, acknowledgement)
}

func (p *MySQLAddressDimensionPublisher) ReportAddressDimensionReference(ctx context.Context, reference AddressDimensionReference) (AddressDimensionReference, error) {
	if p == nil {
		return AddressDimensionReference{}, ErrAddressDimensionInvalid
	}
	return p.ReportDimensionPublicationReference(ctx, reference)
}

var _ AddressDimensionLifecycle = (*MySQLAddressDimensionPublisher)(nil)
