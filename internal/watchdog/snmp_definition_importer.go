package watchdog

import "context"

import "github.com/cloudcache/watchdog/internal/snmpdomain"

type SNMPDefinitionImport = snmpdomain.DefinitionImport

type SNMPDefinitionImportReport struct {
	OSDefinitions     int
	ModuleDefinitions int
	StateTranslations int
	TrapHandlers      int
}

type SNMPDefinitionImporter struct {
	Repository SNMPCollectorRepository
}

func (i SNMPDefinitionImporter) Import(ctx context.Context, input SNMPDefinitionImport) (SNMPDefinitionImportReport, error) {
	var report SNMPDefinitionImportReport
	for _, definition := range input.OSDefinitions {
		if _, err := i.Repository.UpsertSNMPOSDefinition(ctx, definition); err != nil {
			return report, err
		}
		report.OSDefinitions++
	}
	for _, definition := range input.ModuleDefinitions {
		if _, err := i.Repository.UpsertSNMPModuleDefinition(ctx, definition); err != nil {
			return report, err
		}
		report.ModuleDefinitions++
	}
	for _, translation := range input.StateTranslations {
		if _, err := i.Repository.UpsertSNMPStateTranslation(ctx, translation); err != nil {
			return report, err
		}
		report.StateTranslations++
	}
	for _, handler := range input.TrapHandlers {
		if _, err := i.Repository.UpsertSNMPTrapHandler(ctx, handler); err != nil {
			return report, err
		}
		report.TrapHandlers++
	}
	return report, nil
}
