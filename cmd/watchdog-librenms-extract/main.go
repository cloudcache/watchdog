package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/cloudcache/watchdog/internal/watchdog"
)

func main() {
	configPath := flag.String("config", "", "path to watchdog YAML config")
	librenmsPath := flag.String("path", "", "path to LibreNMS checkout")
	sourceVersion := flag.String("source-version", "", "LibreNMS source version label")
	flag.Parse()

	if *librenmsPath == "" {
		log.Fatal("path to LibreNMS checkout is required (-path)")
	}
	cfg, err := watchdog.LoadBackendConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	runtime, err := watchdog.NewBackendRuntime(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer runtime.Close()

	importData, err := watchdog.ParseLibrenmsDefinitions(*librenmsPath, *sourceVersion)
	if err != nil {
		log.Fatal(err)
	}

	importer := watchdog.SNMPDefinitionImporter{Repository: runtime.Store}
	report, err := importer.Import(context.Background(), importData)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("librenms definitions imported: os=%d modules=%d states=%d traps=%d\n",
		report.OSDefinitions, report.ModuleDefinitions, report.StateTranslations, report.TrapHandlers)
}
