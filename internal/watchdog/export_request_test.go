package watchdog

import (
	"testing"
	"time"
)

func TestValidateExportRequestAllowsCustomStep(t *testing.T) {
	err := ValidateExportRequest(ExportRequestValidation{
		Task: ExportTask{
			PeriodType:  PeriodCustom,
			RangeStart:  time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			RangeEnd:    time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC),
			Step:        15 * time.Minute,
			Aggregation: AggregationP95FiveMinute,
			ValueMode:   ExportValueCorrected,
			Format:      ExportFormatCSV,
		},
		Access: AccessRequest{
			TenantID: "tenant-a",
			UserID:   "user-a",
			Resource: ResourceRef{Type: ResourcePort, ID: "port-a", ParentID: "target-a"},
		},
		Grants: []Permission{{
			TenantID:     "tenant-a",
			SubjectType:  SubjectUser,
			SubjectID:    "user-a",
			ResourceType: ResourcePort,
			ResourceID:   "port-a",
			Actions:      []Action{ActionExport},
		}},
		CollectionStep: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("ValidateExportRequest() error = %v", err)
	}
}

func TestValidateExportRequestRejectsRawWithoutAdmin(t *testing.T) {
	err := ValidateExportRequest(ExportRequestValidation{
		Task: ExportTask{
			PeriodType:  PeriodMonth,
			RangeStart:  time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			RangeEnd:    time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
			Step:        5 * time.Minute,
			Aggregation: AggregationP95FiveMinute,
			ValueMode:   ExportValueRaw,
			Format:      ExportFormatCSV,
		},
		Access: AccessRequest{
			TenantID: "tenant-a",
			UserID:   "user-a",
			Resource: ResourceRef{Type: ResourcePort, ID: "port-a", ParentID: "target-a"},
		},
		Grants: []Permission{{
			TenantID:     "tenant-a",
			SubjectType:  SubjectUser,
			SubjectID:    "user-a",
			ResourceType: ResourcePort,
			ResourceID:   "port-a",
			Actions:      []Action{ActionExport},
		}},
		CollectionStep: 5 * time.Minute,
	})
	if err == nil {
		t.Fatal("expected raw export permission error")
	}
}

func TestValidateExportRequestRejectsStepBelowCollectionStep(t *testing.T) {
	err := ValidateExportRequest(ExportRequestValidation{
		Task: ExportTask{
			PeriodType:  PeriodFixed,
			RangeStart:  time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			RangeEnd:    time.Date(2026, 6, 1, 1, 0, 0, 0, time.UTC),
			Step:        time.Minute,
			Aggregation: AggregationAverageFiveMinute,
			ValueMode:   ExportValueCorrected,
			Format:      ExportFormatCSV,
		},
		Access: AccessRequest{
			TenantID: "tenant-a",
			UserID:   "user-a",
			Resource: ResourceRef{Type: ResourcePort, ID: "port-a", ParentID: "target-a"},
		},
		Grants: []Permission{{
			TenantID:     "tenant-a",
			SubjectType:  SubjectUser,
			SubjectID:    "user-a",
			ResourceType: ResourcePort,
			ResourceID:   "port-a",
			Actions:      []Action{ActionExport},
		}},
		CollectionStep: 5 * time.Minute,
	})
	if err == nil {
		t.Fatal("expected collection step error")
	}
}
