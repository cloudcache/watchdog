package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cloudcache/watchdog/internal/watchdog"
)

func main() {
	configPath := flag.String("config", "", "path to watchdog YAML config")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := watchdog.LoadBackendConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	runtime, err := watchdog.NewBackendRuntime(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer runtime.Close()
	if err := runtime.RunAggregateGraphRollup(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
