package flowcollect

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

// BootstrapKafkaTopics creates only absent topics and then verifies the same
// contract enforced by runtime startup. It never alters an existing topic.
// Run it with a separate provisioning principal; never grant its permissions
// to the long-running flow-collect identity.
func BootstrapKafkaTopics(config Config, registry *Registry, clientID string) (int, error) {
	if registry == nil {
		return 0, errors.New("flow registry is required for Kafka topic bootstrap")
	}
	specs, err := kafkaTopicSpecsForRegistries(config, []*Registry{registry})
	if err != nil {
		return 0, err
	}
	saramaConfig, err := buildKafkaClientConfig(config.Kafka, clientID+"-topic-bootstrap")
	if err != nil {
		return 0, err
	}
	saramaConfig.Admin.Timeout = config.Kafka.TopicContract.CheckTimeout
	saramaConfig.Metadata.AllowAutoTopicCreation = false
	if err := saramaConfig.Validate(); err != nil {
		return 0, fmt.Errorf("validate Kafka topic-bootstrap client: %w", err)
	}
	admin, err := sarama.NewClusterAdmin(config.Kafka.Brokers, saramaConfig)
	if err != nil {
		return 0, fmt.Errorf("create Kafka topic-bootstrap client: %w", err)
	}
	created, operationErr := bootstrapKafkaTopicContracts(admin, specs)
	if operationErr == nil {
		deadline := time.Now().Add(config.Kafka.TopicContract.CheckTimeout)
		for {
			operationErr = verifyKafkaTopicContracts(admin, specs)
			if operationErr == nil || created == 0 || !time.Now().Before(deadline) {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	return created, errors.Join(operationErr, admin.Close())
}

func bootstrapKafkaTopicContracts(admin kafkaTopicBootstrapAdmin, specs []kafkaTopicSpec) (int, error) {
	if admin == nil || len(specs) == 0 {
		return 0, errors.New("Kafka topic bootstrap admin and contracts are required")
	}
	names := make([]string, len(specs))
	byName := make(map[string]kafkaTopicSpec, len(specs))
	for index, spec := range specs {
		if spec.name == "" {
			return 0, errors.New("Kafka topic contract contains an empty topic name")
		}
		if _, duplicate := byName[spec.name]; duplicate {
			return 0, fmt.Errorf("Kafka topic %q is assigned more than one flow role", spec.name)
		}
		names[index] = spec.name
		byName[spec.name] = spec
	}
	metadata, err := admin.DescribeTopics(names)
	if err != nil {
		return 0, fmt.Errorf("describe Kafka flow topics before bootstrap: %w", err)
	}
	existing := make(map[string]struct{}, len(metadata))
	for _, topic := range metadata {
		if topic == nil {
			return 0, errors.New("Kafka topic metadata contains a nil entry")
		}
		if _, expected := byName[topic.Name]; !expected {
			return 0, fmt.Errorf("Kafka returned unexpected topic metadata %q", topic.Name)
		}
		if _, duplicate := existing[topic.Name]; duplicate {
			return 0, fmt.Errorf("Kafka returned duplicate metadata for topic %q", topic.Name)
		}
		if topic.Err == sarama.ErrUnknownTopicOrPartition {
			continue
		}
		if topic.Err != sarama.ErrNoError {
			return 0, fmt.Errorf("Kafka topic %q cannot be inspected before bootstrap: %s", topic.Name, topic.Err)
		}
		existing[topic.Name] = struct{}{}
	}
	created := 0
	for _, spec := range specs {
		if _, ok := existing[spec.name]; ok {
			continue
		}
		detail := kafkaTopicDetail(spec)
		if err := admin.CreateTopic(spec.name, detail, false); err != nil {
			if errors.Is(err, sarama.ErrTopicAlreadyExists) {
				continue
			}
			return created, fmt.Errorf("create Kafka topic %q: %w", spec.name, err)
		}
		created++
	}
	return created, nil
}

func kafkaTopicDetail(spec kafkaTopicSpec) *sarama.TopicDetail {
	configs := map[string]*string{
		"cleanup.policy":                 stringPointer(spec.cleanupPolicy),
		"max.message.bytes":              stringPointer(strconv.Itoa(spec.maxMessageBytes)),
		"min.insync.replicas":            stringPointer(strconv.Itoa(spec.minInSyncReplica)),
		"unclean.leader.election.enable": stringPointer("false"),
	}
	if spec.minRetention > 0 {
		configs["retention.ms"] = stringPointer(strconv.FormatInt(spec.minRetention.Milliseconds(), 10))
	}
	if spec.deleteRetention > 0 {
		configs["delete.retention.ms"] = stringPointer(strconv.FormatInt(spec.deleteRetention.Milliseconds(), 10))
	}
	return &sarama.TopicDetail{NumPartitions: int32(spec.partitions), ReplicationFactor: int16(spec.minReplication), ConfigEntries: configs}
}

func stringPointer(value string) *string {
	return &value
}
