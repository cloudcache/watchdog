package flowcollect

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

type fakeKafkaTopicAdmin struct {
	metadata []*sarama.TopicMetadata
	configs  map[string][]sarama.ConfigEntry
	err      error
	closed   bool
}

func (f *fakeKafkaTopicAdmin) DescribeTopics([]string) ([]*sarama.TopicMetadata, error) {
	return f.metadata, f.err
}

func (f *fakeKafkaTopicAdmin) DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
	entries, ok := f.configs[resource.Name]
	if !ok {
		return nil, errors.New("not authorized")
	}
	return entries, nil
}

func (f *fakeKafkaTopicAdmin) Close() error {
	f.closed = true
	return nil
}

func TestVerifyKafkaTopicContractsAcceptsProductionContract(t *testing.T) {
	specs := testKafkaTopicSpecs()
	admin := validKafkaTopicAdmin(specs)
	if err := verifyKafkaTopicContracts(admin, specs); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyKafkaTopicContractsRejectsCompactedTopicPartitionExpansion(t *testing.T) {
	specs := testKafkaTopicSpecs()
	admin := validKafkaTopicAdmin(specs)
	admin.metadata[1] = testKafkaTopicMetadata(specs[1].name, specs[1].partitions+1, specs[1].minReplication, specs[1].minInSyncReplica)
	err := verifyKafkaTopicContracts(admin, specs)
	if err == nil || !strings.Contains(err.Error(), "cannot be expanded in place") {
		t.Fatalf("expected state partition drift to fail closed, got %v", err)
	}
}

func TestVerifyKafkaTopicContractsRejectsUnsafeConfigAndACLFailure(t *testing.T) {
	specs := testKafkaTopicSpecs()
	admin := validKafkaTopicAdmin(specs)
	admin.configs[specs[1].name] = testKafkaTopicConfig("compact,delete", specs[1])
	if err := verifyKafkaTopicContracts(admin, specs); err == nil || !strings.Contains(err.Error(), "cleanup.policy") {
		t.Fatalf("expected unsafe cleanup policy rejection, got %v", err)
	}

	admin = validKafkaTopicAdmin(specs)
	delete(admin.configs, specs[0].name)
	if err := verifyKafkaTopicContracts(admin, specs); err == nil || !strings.Contains(err.Error(), "DESCRIBE_CONFIGS ACL") {
		t.Fatalf("expected ACL diagnostic, got %v", err)
	}
}

func TestVerifyKafkaTopicContractsRejectsUnavailableReplica(t *testing.T) {
	specs := testKafkaTopicSpecs()
	admin := validKafkaTopicAdmin(specs)
	admin.metadata[0].Partitions[0].OfflineReplicas = []int32{3}
	if err := verifyKafkaTopicContracts(admin, specs); err == nil || !strings.Contains(err.Error(), "not fully available") {
		t.Fatalf("expected unavailable replica rejection, got %v", err)
	}
}

func TestKafkaTopicSpecsCoverHistoricalPartitionMaps(t *testing.T) {
	now := time.Now()
	plan := validPlan(now)
	plan.PartitionMap[0] = 17
	registry, err := compileHistoricalPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	history := &PlanHistory{entries: map[uint64]planHistoryEntry{plan.Revision: {registry: registry}}}
	config := DefaultConfig()
	config.Kafka.TopicContract.NormalizedPartitions = 4
	specs, err := kafkaTopicSpecs(config, history)
	if err != nil {
		t.Fatal(err)
	}
	if specs[0].partitions != 18 {
		t.Fatalf("normalized contract does not cover retained plan map: %d", specs[0].partitions)
	}
}

func testKafkaTopicSpecs() []kafkaTopicSpec {
	return []kafkaTopicSpec{
		{name: "normalized", partitions: 4, cleanupPolicy: "delete", minRetention: 24 * time.Hour, maxMessageBytes: 1 << 20, minReplication: 3, minInSyncReplica: 2},
		{name: "state", partitions: 2, exactPartitions: true, cleanupPolicy: "compact", deleteRetention: 24 * time.Hour, maxMessageBytes: 2 << 20, minReplication: 3, minInSyncReplica: 2},
		{name: "dlq", partitions: 2, cleanupPolicy: "delete", minRetention: 48 * time.Hour, maxMessageBytes: 1 << 20, minReplication: 3, minInSyncReplica: 2},
		{name: "quarantine", partitions: 2, cleanupPolicy: "delete", minRetention: 24 * time.Hour, maxMessageBytes: 1 << 20, minReplication: 3, minInSyncReplica: 2},
	}
}

func validKafkaTopicAdmin(specs []kafkaTopicSpec) *fakeKafkaTopicAdmin {
	admin := &fakeKafkaTopicAdmin{configs: make(map[string][]sarama.ConfigEntry, len(specs))}
	for _, spec := range specs {
		admin.metadata = append(admin.metadata, testKafkaTopicMetadata(spec.name, spec.partitions, spec.minReplication, spec.minInSyncReplica))
		admin.configs[spec.name] = testKafkaTopicConfig(spec.cleanupPolicy, spec)
	}
	return admin
}

func testKafkaTopicMetadata(name string, partitions, replication, inSync int) *sarama.TopicMetadata {
	metadata := &sarama.TopicMetadata{Name: name}
	for partition := 0; partition < partitions; partition++ {
		replicas := make([]int32, replication)
		for index := range replicas {
			replicas[index] = int32(index + 1)
		}
		metadata.Partitions = append(metadata.Partitions, &sarama.PartitionMetadata{ID: int32(partition), Leader: 1, Replicas: replicas, Isr: append([]int32(nil), replicas[:inSync]...)})
	}
	return metadata
}

func testKafkaTopicConfig(cleanup string, spec kafkaTopicSpec) []sarama.ConfigEntry {
	retention := spec.minRetention.Milliseconds()
	if retention == 0 {
		retention = (7 * 24 * time.Hour).Milliseconds()
	}
	deleteRetention := spec.deleteRetention.Milliseconds()
	if deleteRetention == 0 {
		deleteRetention = (24 * time.Hour).Milliseconds()
	}
	return []sarama.ConfigEntry{
		{Name: "cleanup.policy", Value: cleanup},
		{Name: "retention.ms", Value: formatInt64(retention)},
		{Name: "delete.retention.ms", Value: formatInt64(deleteRetention)},
		{Name: "max.message.bytes", Value: formatInt64(int64(spec.maxMessageBytes))},
		{Name: "min.insync.replicas", Value: formatInt64(int64(spec.minInSyncReplica))},
		{Name: "unclean.leader.election.enable", Value: "false"},
	}
}

func formatInt64(value int64) string {
	return strconv.FormatInt(value, 10)
}
