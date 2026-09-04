package flowcollect

import (
	"errors"
	"strings"
	"testing"

	"github.com/Shopify/sarama"
)

type fakeKafkaACLAdmin struct {
	bindings  []kafkaACLBinding
	listErr   error
	createErr error
	closed    bool
}

func (admin *fakeKafkaACLAdmin) ListAcls(filter sarama.AclFilter) ([]sarama.ResourceAcls, error) {
	if admin.listErr != nil {
		return nil, admin.listErr
	}
	resources := make([]sarama.ResourceAcls, 0, len(admin.bindings))
	for _, binding := range admin.bindings {
		if filter.Principal != nil && binding.principal != *filter.Principal {
			continue
		}
		resource := binding.resourceACL()
		resources = append(resources, *resource)
	}
	return resources, nil
}

func (admin *fakeKafkaACLAdmin) CreateACLs(resources []*sarama.ResourceAcls) error {
	if admin.createErr != nil {
		return admin.createErr
	}
	for _, resource := range resources {
		for _, acl := range resource.Acls {
			admin.bindings = append(admin.bindings, kafkaACLBinding{
				resourceType: resource.ResourceType, resourceName: resource.ResourceName,
				pattern: resource.ResourcePatternType, principal: acl.Principal, host: acl.Host,
				operation: acl.Operation, permission: acl.PermissionType,
			})
		}
	}
	return nil
}

func (admin *fakeKafkaACLAdmin) Close() error {
	admin.closed = true
	return nil
}

func TestKafkaFlowCollectACLProfileIsExactAndIncludesTopicConfigRead(t *testing.T) {
	config := testKafkaACLConfig()
	bindings, err := expectedKafkaPrincipalACLs(config, "User:collector-a", KafkaPrincipalProfileFlowCollect)
	if err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 14 {
		t.Fatalf("flow-collect ACL count=%d, want 14", len(bindings))
	}
	for _, topic := range []string{config.NormalizedTopic, config.CollectStateTopic, config.DecodeDLQTopic, config.QuarantineTopic} {
		if !containsKafkaACL(bindings, sarama.AclResourceTopic, topic, sarama.AclOperationDescribeConfigs) {
			t.Fatalf("topic %q is missing DESCRIBE_CONFIGS required by startup contract verification", topic)
		}
	}
	if !containsKafkaACL(bindings, sarama.AclResourceTopic, config.CollectStateTopic, sarama.AclOperationRead) {
		t.Fatal("flow-collect cannot restore its compacted state")
	}
	if !containsKafkaACL(bindings, sarama.AclResourceCluster, "kafka-cluster", sarama.AclOperationIdempotentWrite) {
		t.Fatal("flow-collect cannot use its required idempotent producer")
	}
	for _, binding := range bindings {
		if binding.operation == sarama.AclOperationCreate || binding.operation == sarama.AclOperationAlter || binding.operation == sarama.AclOperationDelete || binding.operation == sarama.AclOperationAlterConfigs || binding.operation == sarama.AclOperationAll {
			t.Fatalf("unsafe mutation operation leaked into runtime profile: %s", binding.safeString())
		}
	}
}

func TestKafkaStateCleanupACLProfileOnlyTouchesCollectState(t *testing.T) {
	config := testKafkaACLConfig()
	config.NormalizedTopic = ""
	config.DecodeDLQTopic = ""
	config.QuarantineTopic = ""
	bindings, err := expectedKafkaPrincipalACLs(config, "User:cleanup-a", KafkaPrincipalProfileStateCleanup)
	if err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 4 {
		t.Fatalf("state-cleanup ACL count=%d, want 4", len(bindings))
	}
	for _, binding := range bindings {
		if binding.resourceType == sarama.AclResourceTopic && binding.resourceName != config.CollectStateTopic {
			t.Fatalf("state cleanup can access unrelated topic: %s", binding.safeString())
		}
		if binding.operation == sarama.AclOperationDescribeConfigs {
			t.Fatalf("state cleanup received unused config permission: %s", binding.safeString())
		}
	}
}

func TestVerifyKafkaPrincipalACLsRejectsExtraAndWildcardGrants(t *testing.T) {
	config := testKafkaACLConfig()
	expected, err := expectedKafkaPrincipalACLs(config, "User:collector-a", KafkaPrincipalProfileFlowCollect)
	if err != nil {
		t.Fatal(err)
	}
	admin := &fakeKafkaACLAdmin{bindings: append([]kafkaACLBinding(nil), expected...)}
	if err := verifyKafkaPrincipalACLs(admin, "User:collector-a", expected); err != nil {
		t.Fatal(err)
	}

	admin.bindings = append(admin.bindings, newKafkaACLBinding(sarama.AclResourceTopic, "unrelated", "User:collector-a", sarama.AclOperationRead))
	if err := verifyKafkaPrincipalACLs(admin, "User:collector-a", expected); err == nil || !strings.Contains(err.Error(), "outside the exact profile") {
		t.Fatalf("extra direct ACL was accepted: %v", err)
	}

	admin.bindings = append([]kafkaACLBinding(nil), expected...)
	admin.bindings = append(admin.bindings, newKafkaACLBinding(sarama.AclResourceTopic, "*", "User:*", sarama.AclOperationAll))
	if err := verifyKafkaPrincipalACLs(admin, "User:collector-a", expected); err == nil || !strings.Contains(err.Error(), `principal="User:*"`) {
		t.Fatalf("inherited wildcard ACL was accepted: %v", err)
	}
}

func TestBootstrapKafkaPrincipalACLsCreatesOnlyMissingAndRefusesDrift(t *testing.T) {
	config := testKafkaACLConfig()
	expected, err := expectedKafkaPrincipalACLs(config, "User:collector-a", KafkaPrincipalProfileFlowCollect)
	if err != nil {
		t.Fatal(err)
	}
	admin := &fakeKafkaACLAdmin{bindings: append([]kafkaACLBinding(nil), expected[:3]...)}
	created, err := bootstrapKafkaPrincipalACLs(admin, "User:collector-a", expected)
	if err != nil {
		t.Fatal(err)
	}
	if created != len(expected)-3 {
		t.Fatalf("created=%d, want %d", created, len(expected)-3)
	}
	if err := verifyKafkaPrincipalACLs(admin, "User:collector-a", expected); err != nil {
		t.Fatalf("bootstrapped profile did not verify: %v", err)
	}
	created, err = bootstrapKafkaPrincipalACLs(admin, "User:collector-a", expected)
	if err != nil || created != 0 {
		t.Fatalf("idempotent bootstrap created=%d err=%v", created, err)
	}

	admin.bindings = append(admin.bindings, newKafkaACLBinding(sarama.AclResourceCluster, "kafka-cluster", "User:collector-a", sarama.AclOperationAlter))
	before := len(admin.bindings)
	created, err = bootstrapKafkaPrincipalACLs(admin, "User:collector-a", expected)
	if err == nil || created != 0 || len(admin.bindings) != before {
		t.Fatalf("drifted ACLs were mutated: created=%d before=%d after=%d err=%v", created, before, len(admin.bindings), err)
	}
}

func TestKafkaPrincipalACLPolicyRevisionBindsProfileAndTopicsNotPrincipal(t *testing.T) {
	config := testKafkaACLConfig()
	first, err := KafkaPrincipalACLPolicyRevision(config, KafkaPrincipalProfileFlowCollect)
	if err != nil {
		t.Fatal(err)
	}
	second, err := KafkaPrincipalACLPolicyRevision(config, KafkaPrincipalProfileFlowCollect)
	if err != nil || first != second || !strings.HasPrefix(first, kafkaACLPolicySchema+":sha256:") {
		t.Fatalf("unstable policy revision: first=%q second=%q err=%v", first, second, err)
	}
	cleanup, err := KafkaPrincipalACLPolicyRevision(config, KafkaPrincipalProfileStateCleanup)
	if err != nil || cleanup == first {
		t.Fatalf("profile was not bound into revision: flow=%q cleanup=%q err=%v", first, cleanup, err)
	}
	config.NormalizedTopic += ".v2"
	changed, err := KafkaPrincipalACLPolicyRevision(config, KafkaPrincipalProfileFlowCollect)
	if err != nil || changed == first {
		t.Fatalf("topic was not bound into revision: before=%q after=%q err=%v", first, changed, err)
	}
}

func TestKafkaPrincipalACLValidationRejectsUnsafeInputAndAdminIdentity(t *testing.T) {
	config := testKafkaACLConfig()
	for _, principal := range []string{"", "collector-a", "User:*", " User:collector-a", "User:collector-a\n"} {
		if _, err := expectedKafkaPrincipalACLs(config, principal, KafkaPrincipalProfileFlowCollect); err == nil {
			t.Fatalf("unsafe principal %q was accepted", principal)
		}
	}
	if _, err := expectedKafkaPrincipalACLs(config, "User:collector-a", "unknown"); err == nil {
		t.Fatal("unknown profile was accepted")
	}

	config.Brokers = []string{"kafka:9093"}
	config.TLS = true
	config.TLSCertFile = ""
	config.TLSKeyFile = ""
	if _, _, err := newKafkaACLAdmin(config, "User:collector-a", KafkaPrincipalProfileFlowCollect, "acl-test"); err == nil || !strings.Contains(err.Error(), "dedicated mTLS") {
		t.Fatalf("ACL administration without mTLS identity was accepted: %v", err)
	}
}

func TestKafkaPrincipalACLListErrorsAreBoundedAndContextual(t *testing.T) {
	config := testKafkaACLConfig()
	expected, err := expectedKafkaPrincipalACLs(config, "User:collector-a", KafkaPrincipalProfileFlowCollect)
	if err != nil {
		t.Fatal(err)
	}
	admin := &fakeKafkaACLAdmin{listErr: errors.New("not authorized")}
	if err := verifyKafkaPrincipalACLs(admin, "User:collector-a", expected); err == nil || !strings.Contains(err.Error(), `principal "User:collector-a"`) {
		t.Fatalf("unexpected list error: %v", err)
	}
}

func TestKafkaPrincipalACLListRejectsDuplicateBindings(t *testing.T) {
	binding := newKafkaACLBinding(sarama.AclResourceTopic, "state", "User:collector-a", sarama.AclOperationRead)
	admin := &fakeKafkaACLAdmin{bindings: []kafkaACLBinding{binding, binding}}
	if _, err := listKafkaPrincipalACLs(admin, "User:collector-a"); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate ACL binding was accepted: %v", err)
	}
}

func testKafkaACLConfig() KafkaConfig {
	return KafkaConfig{
		NormalizedTopic: "normalized", CollectStateTopic: "state",
		DecodeDLQTopic: "dlq", QuarantineTopic: "quarantine",
		TopicContract: KafkaTopicContractConfig{CheckTimeout: 1},
	}
}

func containsKafkaACL(bindings []kafkaACLBinding, resourceType sarama.AclResourceType, resourceName string, operation sarama.AclOperation) bool {
	for _, binding := range bindings {
		if binding.resourceType == resourceType && binding.resourceName == resourceName && binding.operation == operation {
			return true
		}
	}
	return false
}
