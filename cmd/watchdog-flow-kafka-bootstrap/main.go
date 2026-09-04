package main

import (
	"flag"
	"log"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
	"github.com/cloudcache/watchdog/internal/watchdog"
)

func main() {
	configPath := flag.String("config", "", "path to watchdog YAML config")
	apply := flag.Bool("apply", false, "create missing flow topics; never alter existing topics")
	flag.Parse()

	cfg, err := watchdog.LoadWatchdogConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	if err := cfg.FlowCollect.ValidateRuntime(); err != nil {
		log.Fatal(err)
	}
	registry, err := flowcollect.LoadSignedPlan(cfg.FlowCollect.PlanFile, cfg.FlowCollect.PlanPublicKeyFile, time.Now())
	if err != nil {
		log.Fatal(err)
	}
	collectorID := registry.Plan().CollectorID
	if !*apply {
		if err := flowcollect.VerifyKafkaTopicContractsForRegistry(cfg.FlowCollect, registry, collectorID); err != nil {
			log.Fatal(err)
		}
		log.Printf("Kafka flow topic contracts are valid for collector=%s", collectorID)
		return
	}
	created, err := flowcollect.BootstrapKafkaTopics(cfg.FlowCollect, registry, collectorID)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Kafka flow topic bootstrap complete: collector=%s created=%d", collectorID, created)
}
