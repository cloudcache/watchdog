package flowcollect

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/Shopify/sarama"
)

type KafkaPrincipalProfile string

const (
	KafkaPrincipalProfileFlowCollect  KafkaPrincipalProfile = "flow-collect"
	KafkaPrincipalProfileStateCleanup KafkaPrincipalProfile = "state-cleanup"

	kafkaACLPolicySchema = "watchdog-kafka-acl-v1"
)

type kafkaACLAdmin interface {
	ListAcls(sarama.AclFilter) ([]sarama.ResourceAcls, error)
	CreateACLs([]*sarama.ResourceAcls) error
	Close() error
}

type kafkaACLBinding struct {
	resourceType sarama.AclResourceType
	resourceName string
	pattern      sarama.AclResourcePatternType
	principal    string
	host         string
	operation    sarama.AclOperation
	permission   sarama.AclPermissionType
}

// KafkaPrincipalACLPolicyRevision returns the stable revision of the exact
// least-privilege ACL policy. The principal is intentionally excluded so one
// provider policy revision applies to every collector using the same topics.
func KafkaPrincipalACLPolicyRevision(config KafkaConfig, profile KafkaPrincipalProfile) (string, error) {
	bindings, err := expectedKafkaPrincipalACLs(config, "User:revision-placeholder", profile)
	if err != nil {
		return "", err
	}
	lines := make([]string, 0, len(bindings)+1)
	lines = append(lines, kafkaACLPolicySchema+"\x00"+string(profile))
	for _, binding := range bindings {
		lines = append(lines, binding.policyLine())
	}
	digest := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return fmt.Sprintf("%s:sha256:%x", kafkaACLPolicySchema, digest), nil
}

// VerifyKafkaPrincipalACLs verifies the complete direct and wildcard ACL view
// with a provisioning identity. Runtime collector credentials must not be able
// to list ACLs.
func VerifyKafkaPrincipalACLs(config KafkaConfig, principal string, profile KafkaPrincipalProfile, clientID string) error {
	admin, expected, err := newKafkaACLAdmin(config, principal, profile, clientID+"-acl-check")
	if err != nil {
		return err
	}
	return errors.Join(verifyKafkaPrincipalACLs(admin, principal, expected), admin.Close())
}

// BootstrapKafkaPrincipalACLs creates only missing exact ACLs. It refuses to
// mutate a principal with extra direct ACLs or any wildcard-principal ACL,
// because silently preserving those grants would falsely claim least privilege.
func BootstrapKafkaPrincipalACLs(config KafkaConfig, principal string, profile KafkaPrincipalProfile, clientID string) (int, string, error) {
	admin, expected, err := newKafkaACLAdmin(config, principal, profile, clientID+"-acl-bootstrap")
	if err != nil {
		return 0, "", err
	}
	revision, err := KafkaPrincipalACLPolicyRevision(config, profile)
	if err != nil {
		return 0, "", errors.Join(err, admin.Close())
	}
	created, operationErr := bootstrapKafkaPrincipalACLs(admin, principal, expected)
	if operationErr == nil {
		deadline := time.Now().Add(config.TopicContract.CheckTimeout)
		for {
			operationErr = verifyKafkaPrincipalACLs(admin, principal, expected)
			if operationErr == nil || created == 0 || !time.Now().Before(deadline) {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
	return created, revision, errors.Join(operationErr, admin.Close())
}

func newKafkaACLAdmin(config KafkaConfig, principal string, profile KafkaPrincipalProfile, clientID string) (kafkaACLAdmin, []kafkaACLBinding, error) {
	if len(config.Brokers) == 0 {
		return nil, nil, errors.New("Kafka brokers are required for ACL administration")
	}
	if config.TopicContract.CheckTimeout <= 0 {
		return nil, nil, errors.New("Kafka ACL administration requires a positive topic contract check timeout")
	}
	if !config.TLS || config.TLSCertFile == "" || config.TLSKeyFile == "" {
		return nil, nil, errors.New("Kafka ACL administration requires a dedicated mTLS certificate and key")
	}
	expected, err := expectedKafkaPrincipalACLs(config, principal, profile)
	if err != nil {
		return nil, nil, err
	}
	saramaConfig, err := buildKafkaClientConfig(config, clientID)
	if err != nil {
		return nil, nil, err
	}
	saramaConfig.Admin.Timeout = config.TopicContract.CheckTimeout
	saramaConfig.Metadata.AllowAutoTopicCreation = false
	if err := saramaConfig.Validate(); err != nil {
		return nil, nil, fmt.Errorf("validate Kafka ACL admin client: %w", err)
	}
	admin, err := sarama.NewClusterAdmin(config.Brokers, saramaConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("create Kafka ACL admin client: %w", err)
	}
	return admin, expected, nil
}

func expectedKafkaPrincipalACLs(config KafkaConfig, principal string, profile KafkaPrincipalProfile) ([]kafkaACLBinding, error) {
	if err := validateKafkaPrincipal(principal); err != nil {
		return nil, err
	}
	var permissions map[string][]sarama.AclOperation
	var topics []string
	switch profile {
	case KafkaPrincipalProfileFlowCollect:
		topics = []string{config.NormalizedTopic, config.CollectStateTopic, config.DecodeDLQTopic, config.QuarantineTopic}
		permissions = map[string][]sarama.AclOperation{
			config.NormalizedTopic:   {sarama.AclOperationWrite, sarama.AclOperationDescribe, sarama.AclOperationDescribeConfigs},
			config.CollectStateTopic: {sarama.AclOperationRead, sarama.AclOperationWrite, sarama.AclOperationDescribe, sarama.AclOperationDescribeConfigs},
			config.DecodeDLQTopic:    {sarama.AclOperationWrite, sarama.AclOperationDescribe, sarama.AclOperationDescribeConfigs},
			config.QuarantineTopic:   {sarama.AclOperationWrite, sarama.AclOperationDescribe, sarama.AclOperationDescribeConfigs},
		}
	case KafkaPrincipalProfileStateCleanup:
		topics = []string{config.CollectStateTopic}
		permissions = map[string][]sarama.AclOperation{
			config.CollectStateTopic: {sarama.AclOperationRead, sarama.AclOperationWrite, sarama.AclOperationDescribe},
		}
	default:
		return nil, fmt.Errorf("unsupported Kafka principal profile %q", profile)
	}
	seen := make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		if strings.TrimSpace(topic) == "" || topic != strings.TrimSpace(topic) {
			return nil, errors.New("all Kafka topics referenced by the principal profile must be non-empty and normalized")
		}
		if _, duplicate := seen[topic]; duplicate {
			return nil, fmt.Errorf("Kafka topic %q cannot serve more than one profile role", topic)
		}
		seen[topic] = struct{}{}
	}

	bindings := make([]kafkaACLBinding, 0, 16)
	for topic, operations := range permissions {
		for _, operation := range operations {
			bindings = append(bindings, newKafkaACLBinding(sarama.AclResourceTopic, topic, principal, operation))
		}
	}
	bindings = append(bindings, newKafkaACLBinding(sarama.AclResourceCluster, "kafka-cluster", principal, sarama.AclOperationIdempotentWrite))
	sortKafkaACLBindings(bindings)
	return bindings, nil
}

func newKafkaACLBinding(resourceType sarama.AclResourceType, resourceName, principal string, operation sarama.AclOperation) kafkaACLBinding {
	return kafkaACLBinding{
		resourceType: resourceType, resourceName: resourceName,
		pattern: sarama.AclPatternLiteral, principal: principal, host: "*",
		operation: operation, permission: sarama.AclPermissionAllow,
	}
}

func validateKafkaPrincipal(principal string) error {
	if principal != strings.TrimSpace(principal) || !strings.HasPrefix(principal, "User:") || len(principal) <= len("User:") || len(principal) > 1024 {
		return errors.New("Kafka principal must be a normalized non-empty User: principal of at most 1024 bytes")
	}
	if principal == "User:*" {
		return errors.New("Kafka wildcard principal cannot receive a collector profile")
	}
	for _, char := range principal {
		if unicode.IsControl(char) {
			return errors.New("Kafka principal cannot contain control characters")
		}
	}
	return nil
}

func verifyKafkaPrincipalACLs(admin kafkaACLAdmin, principal string, expected []kafkaACLBinding) error {
	actual, err := listKafkaPrincipalACLs(admin, principal)
	if err != nil {
		return err
	}
	missing, unexpected := diffKafkaPrincipalACLs(expected, actual)
	if len(unexpected) > 0 {
		return fmt.Errorf("Kafka principal %q has %d ACLs outside the exact profile (first: %s)", principal, len(unexpected), unexpected[0].safeString())
	}
	if len(missing) > 0 {
		return fmt.Errorf("Kafka principal %q is missing %d required ACLs (first: %s)", principal, len(missing), missing[0].safeString())
	}
	return nil
}

func bootstrapKafkaPrincipalACLs(admin kafkaACLAdmin, principal string, expected []kafkaACLBinding) (int, error) {
	actual, err := listKafkaPrincipalACLs(admin, principal)
	if err != nil {
		return 0, err
	}
	missing, unexpected := diffKafkaPrincipalACLs(expected, actual)
	if len(unexpected) > 0 {
		return 0, fmt.Errorf("refusing to modify Kafka principal %q with %d ACLs outside the exact profile (first: %s)", principal, len(unexpected), unexpected[0].safeString())
	}
	if len(missing) == 0 {
		return 0, nil
	}
	resources := make([]*sarama.ResourceAcls, 0, len(missing))
	for _, binding := range missing {
		resources = append(resources, binding.resourceACL())
	}
	if err := admin.CreateACLs(resources); err != nil {
		return 0, fmt.Errorf("create Kafka principal ACLs: %w", err)
	}
	return len(missing), nil
}

func listKafkaPrincipalACLs(admin kafkaACLAdmin, principal string) ([]kafkaACLBinding, error) {
	if admin == nil {
		return nil, errors.New("Kafka ACL admin is required")
	}
	principals := []string{principal, "User:*"}
	bindings := make([]kafkaACLBinding, 0, 16)
	seen := make(map[kafkaACLBinding]struct{}, 16)
	for _, selected := range principals {
		resources, err := admin.ListAcls(sarama.AclFilter{
			ResourceType: sarama.AclResourceAny, ResourcePatternTypeFilter: sarama.AclPatternAny,
			Principal: stringPointer(selected), Operation: sarama.AclOperationAny,
			PermissionType: sarama.AclPermissionAny,
		})
		if err != nil {
			return nil, fmt.Errorf("list Kafka ACLs for principal %q: %w", selected, err)
		}
		for _, resource := range resources {
			for _, acl := range resource.Acls {
				if acl == nil {
					return nil, fmt.Errorf("Kafka ACL response for principal %q contains a nil entry", selected)
				}
				if acl.Principal != selected {
					return nil, fmt.Errorf("Kafka ACL response for principal %q contains principal %q", selected, acl.Principal)
				}
				binding := kafkaACLBinding{
					resourceType: resource.ResourceType, resourceName: resource.ResourceName,
					pattern: resource.ResourcePatternType, principal: acl.Principal, host: acl.Host,
					operation: acl.Operation, permission: acl.PermissionType,
				}
				if _, duplicate := seen[binding]; duplicate {
					return nil, fmt.Errorf("Kafka ACL response for principal %q contains duplicate %s", selected, binding.safeString())
				}
				seen[binding] = struct{}{}
				bindings = append(bindings, binding)
			}
		}
	}
	sortKafkaACLBindings(bindings)
	return bindings, nil
}

func diffKafkaPrincipalACLs(expected, actual []kafkaACLBinding) (missing, unexpected []kafkaACLBinding) {
	want := make(map[kafkaACLBinding]struct{}, len(expected))
	got := make(map[kafkaACLBinding]struct{}, len(actual))
	for _, binding := range expected {
		want[binding] = struct{}{}
	}
	for _, binding := range actual {
		got[binding] = struct{}{}
		if _, ok := want[binding]; !ok {
			unexpected = append(unexpected, binding)
		}
	}
	for _, binding := range expected {
		if _, ok := got[binding]; !ok {
			missing = append(missing, binding)
		}
	}
	sortKafkaACLBindings(missing)
	sortKafkaACLBindings(unexpected)
	return missing, unexpected
}

func sortKafkaACLBindings(bindings []kafkaACLBinding) {
	sort.Slice(bindings, func(left, right int) bool {
		return bindings[left].sortKey() < bindings[right].sortKey()
	})
}

func (binding kafkaACLBinding) sortKey() string {
	return fmt.Sprintf("%03d\x00%s\x00%03d\x00%s\x00%s\x00%03d\x00%03d", binding.resourceType, binding.resourceName, binding.pattern, binding.principal, binding.host, binding.operation, binding.permission)
}

func (binding kafkaACLBinding) policyLine() string {
	return fmt.Sprintf("%d\x00%s\x00%d\x00%s\x00%d\x00%d", binding.resourceType, binding.resourceName, binding.pattern, binding.host, binding.operation, binding.permission)
}

func (binding kafkaACLBinding) safeString() string {
	return fmt.Sprintf("resource=%s/%q pattern=%s principal=%q host=%q operation=%s permission=%s", binding.resourceType.String(), binding.resourceName, binding.pattern.String(), binding.principal, binding.host, binding.operation.String(), binding.permission.String())
}

func (binding kafkaACLBinding) resourceACL() *sarama.ResourceAcls {
	return &sarama.ResourceAcls{
		Resource: sarama.Resource{ResourceType: binding.resourceType, ResourceName: binding.resourceName, ResourcePatternType: binding.pattern},
		Acls:     []*sarama.Acl{{Principal: binding.principal, Host: binding.host, Operation: binding.operation, PermissionType: binding.permission}},
	}
}
