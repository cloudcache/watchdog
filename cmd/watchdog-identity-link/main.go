package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/cloudcache/watchdog/internal/watchdog"
)

func main() {
	configPath := flag.String("config", "", "path to watchdog YAML config")
	tenantID := flag.String("tenant", "", "MySQL tenant id")
	userID := flag.String("user", "", "MySQL user id")
	provider := flag.String("provider", "pocketbase", "authentication provider")
	externalSubject := flag.String("subject", "", "external authentication subject (PocketBase user record id)")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg, err := watchdog.LoadBackendConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	store, err := watchdog.OpenMySQLStore(ctx, cfg.MySQL)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	if err := store.LinkExternalIdentity(ctx, watchdog.ID(*tenantID), watchdog.ID(*userID), *provider, *externalSubject); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("linked %s subject %s to tenant %s user %s\n", *provider, *externalSubject, *tenantID, *userID)
}
