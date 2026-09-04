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
	aclPrincipal := flag.String("acl-principal", "", "Kafka User: principal whose exact runtime ACL profile is verified")
	aclProfile := flag.String("acl-profile", string(flowcollect.KafkaPrincipalProfileFlowCollect), "runtime ACL profile: flow-collect or state-cleanup")
	applyACLs := flag.Bool("apply-acls", false, "create missing exact ACLs; refuse unexpected grants and never delete ACLs")
	flag.Parse()
	if *applyACLs && *aclPrincipal == "" {
		log.Fatal("-apply-acls requires -acl-principal")
	}

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
	} else {
		created, err := flowcollect.BootstrapKafkaTopics(cfg.FlowCollect, registry, collectorID)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Kafka flow topic bootstrap complete: collector=%s created=%d", collectorID, created)
	}

	if *aclPrincipal == "" {
		return
	}
	profile := flowcollect.KafkaPrincipalProfile(*aclProfile)
	if !*applyACLs {
		if err := flowcollect.VerifyKafkaPrincipalACLs(cfg.FlowCollect.Kafka, *aclPrincipal, profile, collectorID); err != nil {
			log.Fatal(err)
		}
		revision, err := flowcollect.KafkaPrincipalACLPolicyRevision(cfg.FlowCollect.Kafka, profile)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Kafka principal ACL profile is valid: principal=%s profile=%s policy_revision=%s", *aclPrincipal, profile, revision)
		return
	}
	created, revision, err := flowcollect.BootstrapKafkaPrincipalACLs(cfg.FlowCollect.Kafka, *aclPrincipal, profile, collectorID)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Kafka principal ACL bootstrap complete: principal=%s profile=%s created=%d policy_revision=%s", *aclPrincipal, profile, created, revision)
}
