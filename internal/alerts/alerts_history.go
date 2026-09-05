package alerts

import (
	"context"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

// On triggered alert record delete, set matching alert history record to resolved
func (am *AlertManager) resolveHistoryOnAlertDelete(e *core.RecordEvent) error {
	if !e.Record.GetBool("triggered") {
		return e.Next()
	}
	am.resolveAlertHistory(e.App, e.Record.Id)
	return e.Next()
}

// On alert record update, update alert history record
func (am *AlertManager) updateHistoryOnAlertUpdate(e *core.RecordEvent) error {
	original := e.Record.Original()
	new := e.Record

	originalTriggered := original.GetBool("triggered")
	newTriggered := new.GetBool("triggered")

	// no need to update alert history if triggered state has not changed
	if originalTriggered == newTriggered {
		return e.Next()
	}

	// if new state is triggered, create new alert history record
	if newTriggered {
		am.createAlertHistory(e.App, new)
		return e.Next()
	}

	// if new state is not triggered, check for matching alert history record and set it to resolved
	am.resolveAlertHistory(e.App, new.Id)
	return e.Next()
}

// createAlertHistory records a triggered alert in MySQL when the store is wired,
// otherwise in the legacy PocketBase collection.
func (am *AlertManager) createAlertHistory(app core.App, alertRecord *core.Record) {
	if am.historyStore != nil {
		if err := am.historyStore.CreateAlertHistoryForExternalSubject(context.Background(), "pocketbase",
			alertRecord.GetString("user"), alertRecord.Id, alertRecord.GetString("system"),
			alertRecord.GetString("name"), alertRecord.GetFloat("value")); err != nil {
			am.hub.Logger().Error("Failed to save alert history", "err", err)
		}
		return
	}
	_, _ = createAlertHistoryRecord(app, alertRecord)
}

// resolveAlertHistory resolves the open history row for an alert, in MySQL when
// the store is wired, otherwise in the legacy PocketBase collection.
func (am *AlertManager) resolveAlertHistory(app core.App, alertRecordID string) {
	if am.historyStore != nil {
		if err := am.historyStore.ResolveAlertHistory(context.Background(), alertRecordID); err != nil {
			am.hub.Logger().Error("Failed to resolve alert history", "err", err)
		}
		return
	}
	_ = resolveAlertHistoryRecord(app, alertRecordID)
}

// resolveAlertHistoryRecord sets the resolved field to the current time
func resolveAlertHistoryRecord(app core.App, alertRecordID string) error {
	alertHistoryRecord, err := app.FindFirstRecordByFilter("alerts_history", "alert_id={:alert_id} && resolved=null", dbx.Params{"alert_id": alertRecordID})
	if err != nil || alertHistoryRecord == nil {
		return err
	}
	alertHistoryRecord.Set("resolved", time.Now().UTC())
	err = app.Save(alertHistoryRecord)
	if err != nil {
		app.Logger().Error("Failed to resolve alert history", "err", err)
	}
	return err
}

// createAlertHistoryRecord creates a new alert history record
func createAlertHistoryRecord(app core.App, alertRecord *core.Record) (alertHistoryRecord *core.Record, err error) {
	alertHistoryCollection, err := app.FindCachedCollectionByNameOrId("alerts_history")
	if err != nil {
		return nil, err
	}
	alertHistoryRecord = core.NewRecord(alertHistoryCollection)
	alertHistoryRecord.Set("alert_id", alertRecord.Id)
	alertHistoryRecord.Set("user", alertRecord.GetString("user"))
	alertHistoryRecord.Set("system", alertRecord.GetString("system"))
	alertHistoryRecord.Set("name", alertRecord.GetString("name"))
	alertHistoryRecord.Set("value", alertRecord.GetFloat("value"))
	err = app.Save(alertHistoryRecord)
	if err != nil {
		app.Logger().Error("Failed to save alert history", "err", err)
	}
	return alertHistoryRecord, err
}
