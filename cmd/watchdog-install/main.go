package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/cloudcache/watchdog/internal/watchdog"
)

func main() {
	configPath := flag.String("config", "", "path to watchdog YAML config")
	initSQLPath := flag.String("init-sql", "install/init.sql", "path to fresh-install SQL")
	lockPath := flag.String("lock", ".watchdog.lock", "local install lock path")
	flag.Parse()

	cfg, err := watchdog.LoadBackendConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	result, err := watchdog.RunInstall(context.Background(), watchdog.InstallOptions{
		Config:     cfg,
		ConfigPath: *configPath,
		InitSQL:    *initSQLPath,
		LockPath:   *lockPath,
	})
	if err != nil {
		log.Fatal(err)
	}
	if result.LockExisted {
		fmt.Fprintf(os.Stdout, "watchdog already installed; refreshed database marker from %s\n", result.LockPath)
		return
	}
	fmt.Fprintf(os.Stdout, "watchdog installed; executed %d SQL statements, applied %d migrations, and wrote %s\n", result.StatementsExecuted, len(result.MigrationsApplied), result.LockPath)
}
