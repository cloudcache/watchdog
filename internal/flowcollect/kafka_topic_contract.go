package flowcollect

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

const kafkaRecordOverheadBytes = 64 << 10

type kafkaTopicAdmin interface {
	DescribeTopics([]string) ([]*sarama.TopicMetadata, error)
	DescribeConfig(sarama.ConfigResource) ([]sarama.ConfigEntry, error)
	Close() error
}

type kafkaTopicBootstrapAdmin interface {
	kafkaTopicAdmin
	CreateTopic(string, *sarama.TopicDetail, bool) error
}

type kafkaTopicSpec struct {
	name             string
	partitions       int
	exactPartitions  bool
	cleanupPolicy    string
	minRetention     time.Duration
	deleteRetention  time.Duration
	maxMessageBytes  int
	minReplication   int
	minInSyncReplica int
}

// VerifyKafkaTopicContracts is a startup fence. It intentionally performs no
// topic creation or mutation: the collector's runtime principal only needs
// describe/read/write/idempotent-write permissions, while provisioning stays
// with a separate infrastructure principal.
func VerifyKafkaTopicContracts(config Config, plans *PlanHistory, collectorID string) error {
	if plans == nil {
		return errors.New("flow plan history is required for Kafka topic verification")
	}
	specs, err := kafkaTopicSpecs(config, plans)
	if err != nil {
		return err
	}
	return verifyKafkaTopicSpecsRemote(config.Kafka, specs, collectorID+"-topic-check")
}

// VerifyKafkaTopicContractsForRegistry is used by the offline provisioning
// command. Runtime startup uses VerifyKafkaTopicContracts so retained plans
// referenced by WAL are included as well.
func VerifyKafkaTopicContractsForRegistry(config Config, registry *Registry, collectorID string) error {
	if registry == nil {
		return errors.New("flow registry is required for Kafka topic verification")
	}
	specs, err := kafkaTopicSpecsForRegistries(config, []*Registry{registry})
	if err != nil {
		return err
	}
	return verifyKafkaTopicSpecsRemote(config.Kafka, specs, collectorID+"-topic-check")
}

// VerifyKafkaTopicContractsForRegistries validates a prospective runtime plan
// together with every retained history revision before the plan is activated.
func VerifyKafkaTopicContractsForRegistries(config Config, registries []*Registry, collectorID string) error {
	specs, err := kafkaTopicSpecsForRegistries(config, registries)
	if err != nil {
		return err
	}
	return verifyKafkaTopicSpecsRemote(config.Kafka, specs, collectorID+"-topic-check")
}

func verifyKafkaTopicSpecsRemote(config KafkaConfig, specs []kafkaTopicSpec, clientID string) error {
	saramaConfig, err := buildKafkaClientConfig(config, clientID)
	if err != nil {
		return err
	}
	saramaConfig.Admin.Timeout = config.TopicContract.CheckTimeout
	saramaConfig.Metadata.AllowAutoTopicCreation = false
	if err := saramaConfig.Validate(); err != nil {
		return fmt.Errorf("validate Kafka topic-check client: %w", err)
	}
	admin, err := sarama.NewClusterAdmin(config.Brokers, saramaConfig)
	if err != nil {
		return fmt.Errorf("create Kafka topic-check client: %w", err)
	}
	return errors.Join(verifyKafkaTopicContracts(admin, specs), admin.Close())
}

func kafkaTopicSpecs(config Config, plans *PlanHistory) ([]kafkaTopicSpec, error) {
	if plans == nil {
		return nil, errors.New("flow plan history is required")
	}
	registries := make([]*Registry, 0, len(plans.Revisions()))
	for _, revision := range plans.Revisions() {
		registry, ok := plans.Resolve(revision)
		if !ok {
			return nil, fmt.Errorf("flow plan revision %d disappeared during topic verification", revision)
		}
		registries = append(registries, registry)
	}
	return kafkaTopicSpecsForRegistries(config, registries)
}

func kafkaTopicSpecsForRegistries(config Config, registries []*Registry) ([]kafkaTopicSpec, error) {
	requiredNormalizedPartitions := config.Kafka.TopicContract.NormalizedPartitions
	for _, registry := range registries {
		if registry == nil {
			return nil, errors.New("flow registry is required")
		}
		for _, partition := range registry.Plan().PartitionMap {
			if required := int(partition) + 1; required > requiredNormalizedPartitions {
				requiredNormalizedPartitions = required
			}
		}
	}
	topics := config.Kafka.TopicContract
	return []kafkaTopicSpec{
		{name: config.Kafka.NormalizedTopic, partitions: requiredNormalizedPartitions, cleanupPolicy: "delete", minRetention: topics.NormalizedMinRetention, maxMessageBytes: config.NormalizedBatch.MaxBytes + kafkaRecordOverheadBytes, minReplication: topics.MinReplicationFactor, minInSyncReplica: topics.MinInSyncReplicas},
		{name: config.Kafka.CollectStateTopic, partitions: topics.CollectStatePartitions, exactPartitions: true, cleanupPolicy: "compact", deleteRetention: topics.CollectStateDeleteRetention, maxMessageBytes: collectStateMaxBytes + kafkaRecordOverheadBytes, minReplication: topics.MinReplicationFactor, minInSyncReplica: topics.MinInSyncReplicas},
		{name: config.Kafka.DecodeDLQTopic, partitions: topics.DecodeDLQPartitions, cleanupPolicy: "delete", minRetention: topics.DecodeDLQMinRetention, maxMessageBytes: config.MaxDatagramBytes + kafkaRecordOverheadBytes, minReplication: topics.MinReplicationFactor, minInSyncReplica: topics.MinInSyncReplicas},
		{name: config.Kafka.QuarantineTopic, partitions: topics.QuarantinePartitions, cleanupPolicy: "delete", minRetention: topics.QuarantineMinRetention, maxMessageBytes: config.MaxDatagramBytes + kafkaRecordOverheadBytes, minReplication: topics.MinReplicationFactor, minInSyncReplica: topics.MinInSyncReplicas},
	}, nil
}

func verifyKafkaTopicContracts(admin kafkaTopicAdmin, specs []kafkaTopicSpec) error {
	if admin == nil || len(specs) == 0 {
		return errors.New("Kafka topic admin and contracts are required")
	}
	names := make([]string, len(specs))
	byName := make(map[string]kafkaTopicSpec, len(specs))
	for index, spec := range specs {
		if spec.name == "" {
			return errors.New("Kafka topic contract contains an empty topic name")
		}
		if _, duplicate := byName[spec.name]; duplicate {
			return fmt.Errorf("Kafka topic %q is assigned more than one flow role", spec.name)
		}
		names[index] = spec.name
		byName[spec.name] = spec
	}
	metadata, err := admin.DescribeTopics(names)
	if err != nil {
		return fmt.Errorf("describe Kafka flow topics: %w", err)
	}
	if len(metadata) != len(specs) {
		return fmt.Errorf("describe Kafka flow topics returned %d topics, expected %d", len(metadata), len(specs))
	}
	seen := make(map[string]struct{}, len(metadata))
	for _, topic := range metadata {
		if topic == nil {
			return errors.New("Kafka topic metadata contains a nil entry")
		}
		spec, ok := byName[topic.Name]
		if !ok {
			return fmt.Errorf("Kafka returned unexpected topic metadata %q", topic.Name)
		}
		if _, duplicate := seen[topic.Name]; duplicate {
			return fmt.Errorf("Kafka returned duplicate metadata for topic %q", topic.Name)
		}
		seen[topic.Name] = struct{}{}
		if topic.Err != sarama.ErrNoError {
			return fmt.Errorf("Kafka topic %q is unavailable: %s", topic.Name, topic.Err)
		}
		if err := verifyKafkaTopicMetadata(topic, spec); err != nil {
			return err
		}
		entries, err := admin.DescribeConfig(sarama.ConfigResource{Type: sarama.TopicResource, Name: topic.Name})
		if err != nil {
			return fmt.Errorf("describe Kafka topic %q configuration (DESCRIBE_CONFIGS ACL is required): %w", topic.Name, err)
		}
		if err := verifyKafkaTopicConfig(topic.Name, entries, spec); err != nil {
			return err
		}
	}
	return nil
}

func verifyKafkaTopicMetadata(topic *sarama.TopicMetadata, spec kafkaTopicSpec) error {
	actual := len(topic.Partitions)
	if spec.exactPartitions && actual != spec.partitions {
		return fmt.Errorf("Kafka topic %q has %d partitions, expected exactly %d; keyed compacted state topics cannot be expanded in place", topic.Name, actual, spec.partitions)
	}
	if !spec.exactPartitions && actual < spec.partitions {
		return fmt.Errorf("Kafka topic %q has %d partitions, requires at least %d", topic.Name, actual, spec.partitions)
	}
	ids := make([]int, 0, actual)
	for _, partition := range topic.Partitions {
		if partition == nil {
			return fmt.Errorf("Kafka topic %q has nil partition metadata", topic.Name)
		}
		if partition.Err != sarama.ErrNoError || partition.Leader < 0 || len(partition.OfflineReplicas) != 0 {
			return fmt.Errorf("Kafka topic %q partition %d is not fully available", topic.Name, partition.ID)
		}
		if len(partition.Replicas) < spec.minReplication {
			return fmt.Errorf("Kafka topic %q partition %d replication factor is %d, requires at least %d", topic.Name, partition.ID, len(partition.Replicas), spec.minReplication)
		}
		if len(partition.Isr) < spec.minInSyncReplica {
			return fmt.Errorf("Kafka topic %q partition %d has %d in-sync replicas, requires at least %d", topic.Name, partition.ID, len(partition.Isr), spec.minInSyncReplica)
		}
		ids = append(ids, int(partition.ID))
	}
	sort.Ints(ids)
	for expected, id := range ids {
		if id != expected {
			return fmt.Errorf("Kafka topic %q partition IDs are not contiguous from zero", topic.Name)
		}
	}
	return nil
}

func verifyKafkaTopicConfig(topic string, entries []sarama.ConfigEntry, spec kafkaTopicSpec) error {
	values := make(map[string]string, len(entries))
	for _, entry := range entries {
		values[entry.Name] = strings.TrimSpace(entry.Value)
	}
	cleanup, err := requiredKafkaConfig(values, "cleanup.policy")
	if err != nil {
		return fmt.Errorf("Kafka topic %q: %w", topic, err)
	}
	policies := strings.Split(cleanup, ",")
	if len(policies) != 1 || strings.TrimSpace(policies[0]) != spec.cleanupPolicy {
		return fmt.Errorf("Kafka topic %q cleanup.policy=%q, expected exactly %q", topic, cleanup, spec.cleanupPolicy)
	}
	if err := requireKafkaConfigAtLeast(values, "min.insync.replicas", int64(spec.minInSyncReplica), false); err != nil {
		return fmt.Errorf("Kafka topic %q: %w", topic, err)
	}
	if err := requireKafkaConfigAtLeast(values, "max.message.bytes", int64(spec.maxMessageBytes), false); err != nil {
		return fmt.Errorf("Kafka topic %q: %w", topic, err)
	}
	unclean, err := requiredKafkaConfig(values, "unclean.leader.election.enable")
	if err != nil {
		return fmt.Errorf("Kafka topic %q: %w", topic, err)
	}
	if parsed, parseErr := strconv.ParseBool(unclean); parseErr != nil || parsed {
		return fmt.Errorf("Kafka topic %q unclean.leader.election.enable must be false", topic)
	}
	if spec.minRetention > 0 {
		if err := requireKafkaConfigAtLeast(values, "retention.ms", spec.minRetention.Milliseconds(), true); err != nil {
			return fmt.Errorf("Kafka topic %q: %w", topic, err)
		}
	}
	if spec.deleteRetention > 0 {
		if err := requireKafkaConfigAtLeast(values, "delete.retention.ms", spec.deleteRetention.Milliseconds(), false); err != nil {
			return fmt.Errorf("Kafka topic %q: %w", topic, err)
		}
	}
	return nil
}

func requiredKafkaConfig(values map[string]string, name string) (string, error) {
	value, ok := values[name]
	if !ok || value == "" {
		return "", fmt.Errorf("required effective config %s is absent", name)
	}
	return value, nil
}

func requireKafkaConfigAtLeast(values map[string]string, name string, minimum int64, allowUnlimited bool) error {
	value, err := requiredKafkaConfig(values, name)
	if err != nil {
		return err
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < -1 {
		return fmt.Errorf("effective config %s=%q is invalid", name, value)
	}
	if allowUnlimited && parsed == -1 {
		return nil
	}
	if parsed < minimum {
		return fmt.Errorf("effective config %s=%d requires at least %d", name, parsed, minimum)
	}
	return nil
}
