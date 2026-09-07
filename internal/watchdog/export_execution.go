package watchdog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	ExportExecutionContractVersion = uint16(1)
	ExportExecutionPayloadVersion  = 1
	ExportExecutionJobType         = "export_execute"
	defaultExportDatasetKey        = "network.snmp_interface"
	defaultExportRetentionSeconds  = uint32(7 * 24 * 60 * 60)
	ExportArtifactSchemaVersion    = uint16(1)
)

type exportQuerySnapshot struct {
	SchemaVersion int          `json:"schema_version"`
	Query         QueryRequest `json:"query"`
	Aggregation   Aggregation  `json:"aggregation"`
}

type exportVersionSnapshot struct {
	SchemaVersion            int    `json:"schema_version"`
	DatasetDescriptorHash    string `json:"dataset_descriptor_hash"`
	QueryPolicyVersion       uint64 `json:"query_policy_version"`
	CorrectionSnapshotHash   string `json:"correction_snapshot_hash,omitempty"`
	CompletenessStepSeconds  uint32 `json:"completeness_step_seconds"`
	CompletenessMissingRatio string `json:"completeness_missing_ratio"`
	CompletenessMode         string `json:"completeness_mode,omitempty"`
	SnapshotComplete         bool   `json:"snapshot_complete"`
}

type exportAuthorizationSnapshot struct {
	SchemaVersion  int          `json:"schema_version"`
	SubjectID      ID           `json:"subject_id"`
	RequiredAction Action       `json:"required_action"`
	ResourceType   ResourceType `json:"resource_type"`
	ResourceID     ID           `json:"resource_id"`
	ParentID       ID           `json:"parent_id,omitempty"`
	Admin          bool         `json:"admin"`
}

type exportExecutionJobPayload struct {
	ExportID     ID     `json:"export_id"`
	QueryHash    string `json:"query_hash"`
	SnapshotHash string `json:"snapshot_hash"`
}

type ExportExecutionAuthorizationRepository interface {
	GetTenant(ctx context.Context, tenantID ID) (Tenant, error)
	GetTenantUser(ctx context.Context, tenantID, userID ID) (User, error)
	ListUserRoleIDs(ctx context.Context, tenantID, userID ID) ([]ID, error)
	ListPermissionsForUser(ctx context.Context, tenantID, userID ID) ([]Permission, error)
	IsUserTenantAdmin(ctx context.Context, tenantID, userID ID) (bool, error)
}

type ExportExecutionJobDependencies struct {
	Authorization ExportExecutionAuthorizationRepository
	Network       NetworkRepository
}

func prepareExportExecutionTask(ctx context.Context, gateway *QueryGateway, network NetworkRepository, auth AuthContext, metric string, collectionStep time.Duration, task ExportTask) (ExportTask, error) {
	if gateway == nil {
		return task, nil
	}
	task = normalizeExportTask(task)
	if task.ValueLayer == "" {
		task.ValueLayer = inferExportValueLayer(task)
	}
	descriptor, ok := gateway.Datasets.Get(defaultExportDatasetKey)
	if !ok {
		return ExportTask{}, errors.New("export dataset is not registered")
	}
	if err := gateway.requireModuleEnabled(ctx, auth.TenantID, descriptor.ModuleKey); err != nil {
		return ExportTask{}, err
	}
	if !datasetSupportsQueryLayer(descriptor, task.ValueLayer) {
		return ExportTask{}, errors.New("export dataset does not support the requested value layer")
	}
	policy, err := gateway.effectivePolicy(ctx, auth.TenantID, descriptor)
	if err != nil {
		return ExportTask{}, fmt.Errorf("load export query policy: %w", err)
	}
	if !policy.Enabled || !policy.allows(task.ValueLayer) {
		return ExportTask{}, errors.New("export dataset or value layer is disabled")
	}
	if !queryLayerAuthorized(auth, task.ValueLayer) {
		return ExportTask{}, errors.New("query permission for the export value layer is required")
	}
	if task.RangeEnd.Sub(task.RangeStart) > timeDurationSeconds(policy.MaxRangeSeconds) {
		return ExportTask{}, errors.New("export range exceeds the dataset policy")
	}

	metric = strings.TrimSpace(metric)
	if metric == "" {
		metric = MetricSNMPIfInBps
	}
	parametersValue := victoriaMetricsQueryParameters{Metric: metric, TargetID: task.TargetID, PortID: task.PortID}
	if task.PortID == "" && task.ValueLayer != QueryValueRaw {
		if network == nil {
			return ExportTask{}, errors.New("network inventory is required for target-level corrected export")
		}
		device, err := network.GetDeviceByTarget(ctx, auth.TenantID, task.TargetID)
		if err != nil {
			return ExportTask{}, fmt.Errorf("resolve export target device: %w", err)
		}
		parametersValue.DeviceID = device.ID
	}
	parameters, err := json.Marshal(parametersValue)
	if err != nil {
		return ExportTask{}, err
	}
	queryStep := ExportQueryStep(task, collectionStep)
	query, err := normalizeQueryRequest(QueryRequest{
		Dataset: defaultExportDatasetKey, From: task.RangeStart, To: task.RangeEnd,
		StepSeconds: uint32(queryStep / time.Second), Limit: policy.MaxResultRows,
		ValueLayer: task.ValueLayer, RequireComplete: false, Parameters: parameters,
	})
	if err != nil {
		return ExportTask{}, err
	}
	queryJSON, queryHash, err := canonicalJSONHash(exportQuerySnapshot{SchemaVersion: 1, Query: query, Aggregation: task.Aggregation})
	if err != nil {
		return ExportTask{}, err
	}
	_, descriptorHash, err := canonicalJSONHash(descriptor)
	if err != nil {
		return ExportTask{}, err
	}
	correctionHash, err := exportCorrectionSnapshotHash(ctx, network, auth.TenantID, parametersValue, task.ValueLayer)
	if err != nil {
		return ExportTask{}, err
	}
	versionsJSON, _, err := canonicalJSONHash(exportVersionSnapshot{
		SchemaVersion: 1, DatasetDescriptorHash: descriptorHash,
		QueryPolicyVersion: policy.RowVersion, CorrectionSnapshotHash: correctionHash,
		CompletenessStepSeconds: uint32(queryStep / time.Second), CompletenessMissingRatio: "0.3",
		SnapshotComplete: true,
	})
	if err != nil {
		return ExportTask{}, err
	}
	access := exportAccessRequest(auth, task)
	authorizationJSON, _, err := canonicalJSONHash(exportAuthorizationSnapshot{
		SchemaVersion: 1, SubjectID: auth.UserID, RequiredAction: exportLayerAction(task.ValueLayer),
		ResourceType: access.Resource.Type, ResourceID: access.Resource.ID,
		ParentID: access.Resource.ParentID, Admin: auth.IsAdmin,
	})
	if err != nil {
		return ExportTask{}, err
	}
	task.ContractVersion = ExportExecutionContractVersion
	task.DatasetKey = descriptor.Key
	task.QueryJSON = queryJSON
	task.QueryHash = queryHash
	task.VersionsJSON = versionsJSON
	task.AuthorizationJSON = authorizationJSON
	if task.RetentionSeconds == 0 {
		task.RetentionSeconds = defaultExportRetentionSeconds
	}
	return task, validateExportExecutionTask(task)
}

func validateExportExecutionTask(task ExportTask) error {
	if task.ContractVersion == 0 {
		return nil
	}
	if task.ContractVersion != ExportExecutionContractVersion {
		return fmt.Errorf("unsupported export contract version %d", task.ContractVersion)
	}
	if strings.TrimSpace(task.DatasetKey) == "" || len(task.DatasetKey) > 128 {
		return errors.New("export dataset_key is required")
	}
	if task.ValueLayer != QueryValueRaw && task.ValueLayer != QueryValueSupplier && task.ValueLayer != QueryValueCustomer {
		return errors.New("export value_layer must be raw, supplier or customer")
	}
	if task.Format != ExportFormatCSV && task.Format != ExportFormatParquet {
		return errors.New("export format must be csv or parquet")
	}
	if task.RetentionSeconds < 3600 || task.RetentionSeconds > 31_536_000 {
		return errors.New("export retention_seconds must be between 3600 and 31536000")
	}
	_, hash, err := canonicalJSONHash(json.RawMessage(task.QueryJSON))
	if err != nil {
		return errors.New("export query_json must be valid JSON")
	}
	// MySQL's native JSON column preserves the value but is allowed to reorder
	// object members and add whitespace when it is read back. Integrity is
	// therefore defined by the canonical semantic hash, not byte-for-byte JSON
	// formatting at the storage boundary.
	if hash != task.QueryHash {
		return errors.New("export query snapshot or hash is not canonical")
	}
	for name, raw := range map[string]json.RawMessage{"versions_json": task.VersionsJSON, "authorization_json": task.AuthorizationJSON} {
		if len(raw) == 0 || !json.Valid(raw) {
			return fmt.Errorf("export %s must be valid JSON", name)
		}
	}
	var snapshot exportQuerySnapshot
	if err := decodeStrictJSON(task.QueryJSON, &snapshot); err != nil {
		return fmt.Errorf("decode export query snapshot: %w", err)
	}
	if snapshot.SchemaVersion != 1 || snapshot.Query.Dataset != task.DatasetKey || snapshot.Query.ValueLayer != task.ValueLayer ||
		!snapshot.Query.From.Equal(task.RangeStart.UTC()) || !snapshot.Query.To.Equal(task.RangeEnd.UTC()) ||
		snapshot.Aggregation != task.Aggregation {
		return errors.New("export query snapshot does not match the task projection")
	}
	switch task.DatasetKey {
	case FlowTrafficDataset:
		if task.ValueLayer != QueryValueCustomer || task.TargetID != "" || task.PortID != "" ||
			task.Step != time.Duration(snapshot.Query.StepSeconds)*time.Second {
			return errors.New("Flow export task projection is invalid")
		}
		parameters, err := decodeFlowAggregateQueryParameters(snapshot.Query.Parameters)
		if err != nil || parameters.Table != nil {
			return errors.New("Flow export query parameters are invalid")
		}
	case FlowRecordDetailDataset:
		if err := validateFlowDetailExportSnapshot(task, snapshot); err != nil {
			return err
		}
	case FlowVPNFindingsDataset:
		if task.ValueLayer != QueryValueCustomer || task.TargetID != "" || task.PortID != "" || task.Step != 0 ||
			snapshot.Query.StepSeconds != 0 || snapshot.Query.Cursor != "" {
			return errors.New("VPN finding export task projection is invalid")
		}
		var parameters vpnFindingExportParameters
		if err := decodeStrictJSON(snapshot.Query.Parameters, &parameters); err != nil {
			return errors.New("VPN finding export query parameters are invalid")
		}
		if _, _, err := normalizeVPNFindingExportParameters(parameters); err != nil {
			return errors.New("VPN finding export query parameters are invalid")
		}
		if snapshot.Query.Limit == 0 || snapshot.Query.Limit > maxVPNFindingExportRows ||
			validateVPNFindingRange(snapshot.Query.From, snapshot.Query.To) != nil {
			return errors.New("VPN finding export query range or limit is invalid")
		}
	default:
		if snapshot.Query.StepSeconds == 0 {
			return errors.New("export query snapshot step is required")
		}
		var parameters victoriaMetricsQueryParameters
		if err := decodeStrictJSON(snapshot.Query.Parameters, &parameters); err != nil || parameters.TargetID != task.TargetID || parameters.PortID != task.PortID {
			return errors.New("export query resource does not match the task projection")
		}
	}
	var versions exportVersionSnapshot
	if err := decodeStrictJSON(task.VersionsJSON, &versions); err != nil || versions.SchemaVersion != 1 || !versions.SnapshotComplete {
		return errors.New("export version snapshot is incomplete")
	}
	if len(versions.DatasetDescriptorHash) != 64 || versions.CompletenessStepSeconds != snapshot.Query.StepSeconds {
		return errors.New("export completeness snapshot does not match its query")
	}
	switch task.DatasetKey {
	case FlowTrafficDataset:
		if versions.CompletenessMode != flowExportCompletenessMode || versions.CompletenessMissingRatio != "0" || versions.CorrectionSnapshotHash != "" {
			return errors.New("Flow export completeness snapshot is invalid")
		}
	case FlowRecordDetailDataset:
		if versions.CompletenessMode != flowDetailExportCompletenessMode || versions.CompletenessMissingRatio != "0" || versions.CorrectionSnapshotHash != "" || versions.QueryPolicyVersion != 0 {
			return errors.New("Flow detail export version snapshot is invalid")
		}
	case FlowVPNFindingsDataset:
		if versions.CompletenessMode != vpnFindingExportCompletenessMode || versions.CompletenessMissingRatio != "0" || versions.CorrectionSnapshotHash != "" || versions.QueryPolicyVersion != 0 {
			return errors.New("VPN finding export version snapshot is invalid")
		}
	default:
		if (versions.CompletenessMode != "" && versions.CompletenessMode != "sample_series") || versions.CompletenessMissingRatio != "0.3" {
			return errors.New("export sample completeness snapshot is invalid")
		}
	}
	var authorization exportAuthorizationSnapshot
	if err := decodeStrictJSON(task.AuthorizationJSON, &authorization); err != nil || authorization.SchemaVersion != 1 ||
		authorization.SubjectID != task.CreatedBy || authorization.RequiredAction != exportRequiredAction(task) {
		return errors.New("export authorization snapshot does not match the task")
	}
	wantResource := exportAccessRequest(AuthContext{TenantID: task.TenantID, UserID: task.CreatedBy}, task).Resource
	if authorization.ResourceType != wantResource.Type || authorization.ResourceID != wantResource.ID || authorization.ParentID != wantResource.ParentID {
		return errors.New("export authorization resource does not match the task")
	}
	return nil
}

func exportCompletenessPolicyFromSnapshot(task ExportTask) (CompletenessPolicy, error) {
	if task.ContractVersion != ExportExecutionContractVersion {
		return CompletenessPolicy{}, errors.New("export completeness snapshot requires contract version 1")
	}
	if task.DatasetKey == FlowTrafficDataset || task.DatasetKey == FlowRecordDetailDataset {
		return CompletenessPolicy{}, errors.New("Flow exports use query-result completeness")
	}
	var versions exportVersionSnapshot
	if err := decodeStrictJSON(task.VersionsJSON, &versions); err != nil {
		return CompletenessPolicy{}, fmt.Errorf("decode export completeness snapshot: %w", err)
	}
	missingRatio, err := strconv.ParseFloat(versions.CompletenessMissingRatio, 64)
	if err != nil || missingRatio < 0 || missingRatio > 1 {
		return CompletenessPolicy{}, errors.New("export completeness missing ratio is invalid")
	}
	if versions.CompletenessStepSeconds == 0 {
		return CompletenessPolicy{}, errors.New("export completeness step is invalid")
	}
	return CompletenessPolicy{
		Start:           task.RangeStart,
		End:             task.RangeEnd,
		CollectionStep:  time.Duration(versions.CompletenessStepSeconds) * time.Second,
		MaxMissingRatio: missingRatio,
	}, nil
}

func normalizeLegacyExportTaskSnapshot(task ExportTask) (ExportTask, error) {
	task = normalizeExportTask(task)
	task.ContractVersion = 0
	if task.DatasetKey == "" {
		task.DatasetKey = defaultExportDatasetKey
	}
	if task.ValueLayer == "" {
		task.ValueLayer = inferExportValueLayer(task)
	}
	if len(task.QueryJSON) == 0 {
		query, hash, err := canonicalJSONHash(map[string]any{
			"schema_version": 0, "legacy": true, "target_id": task.TargetID, "port_id": task.PortID,
			"range_start": task.RangeStart.UTC(), "range_end": task.RangeEnd.UTC(),
			"step_seconds": uint32(task.Step / time.Second), "aggregation": task.Aggregation,
			"legacy_value_mode": task.ValueMode,
		})
		if err != nil {
			return ExportTask{}, err
		}
		task.QueryJSON, task.QueryHash = query, hash
	}
	if len(task.VersionsJSON) == 0 {
		task.VersionsJSON = json.RawMessage(`{"dataset_descriptor":"legacy-v0","query_policy":"unknown","snapshot_complete":false}`)
	}
	if len(task.AuthorizationJSON) == 0 {
		var err error
		task.AuthorizationJSON, _, err = canonicalJSONHash(map[string]any{
			"schema_version": 0, "legacy": true, "subject_id": task.CreatedBy,
			"required_action": exportLayerAction(task.ValueLayer),
		})
		if err != nil {
			return ExportTask{}, err
		}
	}
	if task.RetentionSeconds == 0 {
		task.RetentionSeconds = defaultExportRetentionSeconds
	}
	return task, nil
}

func inferExportValueLayer(task ExportTask) QueryValueLayer {
	switch task.ValueMode {
	case ExportValueRaw, ExportValueBoth:
		return QueryValueRaw
	default:
		return QueryValueCustomer
	}
}

func exportLayerAction(layer QueryValueLayer) Action {
	switch layer {
	case QueryValueRaw:
		return ActionExportRaw
	case QueryValueSupplier:
		return ActionExportSupplier
	default:
		return ActionExportCustomer
	}
}

func canonicalJSONHash(value any) (json.RawMessage, string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, "", err
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, "", err
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(canonical)
	return canonical, hex.EncodeToString(sum[:]), nil
}

func encodeExportExecutionPayload(task ExportTask) (json.RawMessage, string, error) {
	snapshotHash, err := exportExecutionSnapshotHash(task)
	if err != nil {
		return nil, "", err
	}
	payload, err := EncodeJobPayload(ExportExecutionPayloadVersion, exportExecutionJobPayload{
		ExportID: task.ID, QueryHash: task.QueryHash, SnapshotHash: snapshotHash,
	})
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(payload)
	return payload, hex.EncodeToString(sum[:]), nil
}

func NewExportExecutionJobHandler(repo ExportRepository, worker ExportWorker, deps ExportExecutionJobDependencies) OperationJobHandler {
	return func(ctx context.Context, job OperationJob) (string, error) {
		var payload exportExecutionJobPayload
		if err := DecodeJobPayload(job.CheckpointJSON, ExportExecutionPayloadVersion, &payload); err != nil {
			return "", err
		}
		if payload.ExportID == "" || len(payload.QueryHash) != 64 || len(payload.SnapshotHash) != 64 {
			return "", TerminalJobError(errors.New("export execution payload is incomplete"))
		}
		task, err := repo.GetExportTask(ctx, job.TenantID, payload.ExportID)
		if err != nil {
			return "", err
		}
		if task.ContractVersion != ExportExecutionContractVersion || task.OperationJobID != job.ID || task.QueryHash != payload.QueryHash {
			return "", TerminalJobError(errors.New("export execution snapshot does not match its operation job"))
		}
		if err := validateExportExecutionTask(task); err != nil {
			return "", TerminalJobError(err)
		}
		snapshotHash, err := exportExecutionSnapshotHash(task)
		if err != nil || snapshotHash != payload.SnapshotHash {
			return "", TerminalJobError(errors.New("export execution snapshot integrity check failed"))
		}
		auth, err := loadCurrentExportAuthorization(ctx, deps.Authorization, task)
		if err != nil {
			return "", err
		}
		if !canExecuteExportTask(auth, task) {
			return "", TerminalJobError(errors.New("export permission has been revoked"))
		}
		if err := verifyExportCorrectionSnapshot(ctx, deps.Network, task); err != nil {
			return "", TerminalJobError(err)
		}
		ctx = ContextWithAuth(ctx, auth)
		if err := worker.RunTask(ctx, task); err != nil {
			var queryErr *QueryGatewayError
			if errors.As(err, &queryErr) && !queryErr.Retryable {
				return "", TerminalJobError(err)
			}
			return "", err
		}
		completed, err := repo.GetExportTask(ctx, job.TenantID, task.ID)
		if err != nil {
			return "", err
		}
		return completed.FileRef, nil
	}
}

func exportExecutionSnapshotHash(task ExportTask) (string, error) {
	_, hash, err := canonicalJSONHash(struct {
		ContractVersion   uint16          `json:"contract_version"`
		DatasetKey        string          `json:"dataset_key"`
		QueryHash         string          `json:"query_hash"`
		Query             json.RawMessage `json:"query"`
		ValueLayer        QueryValueLayer `json:"value_layer"`
		Versions          json.RawMessage `json:"versions"`
		Authorization     json.RawMessage `json:"authorization"`
		Format            ExportFormat    `json:"format"`
		RetentionSeconds  uint32          `json:"retention_seconds"`
		ArtifactSchemaVer uint16          `json:"artifact_schema_version"`
	}{
		ContractVersion: task.ContractVersion, DatasetKey: task.DatasetKey, QueryHash: task.QueryHash,
		Query: task.QueryJSON, ValueLayer: task.ValueLayer, Versions: task.VersionsJSON,
		Authorization: task.AuthorizationJSON, Format: task.Format, RetentionSeconds: task.RetentionSeconds,
		ArtifactSchemaVer: ExportArtifactSchemaVersion,
	})
	return hash, err
}

func loadCurrentExportAuthorization(ctx context.Context, repo ExportExecutionAuthorizationRepository, task ExportTask) (AuthContext, error) {
	if repo == nil {
		return AuthContext{}, TerminalJobError(errors.New("export authorization repository is not configured"))
	}
	tenant, err := repo.GetTenant(ctx, task.TenantID)
	if err != nil {
		return AuthContext{}, fmt.Errorf("load export tenant: %w", err)
	}
	user, err := repo.GetTenantUser(ctx, task.TenantID, task.CreatedBy)
	if err != nil {
		return AuthContext{}, fmt.Errorf("load export principal: %w", err)
	}
	if !strings.EqualFold(tenant.Status, "active") || !strings.EqualFold(user.Status, "active") {
		return AuthContext{}, TerminalJobError(errors.New("export principal or tenant is disabled"))
	}
	roles, err := repo.ListUserRoleIDs(ctx, task.TenantID, task.CreatedBy)
	if err != nil {
		return AuthContext{}, fmt.Errorf("load export roles: %w", err)
	}
	grants, err := repo.ListPermissionsForUser(ctx, task.TenantID, task.CreatedBy)
	if err != nil {
		return AuthContext{}, fmt.Errorf("load export permissions: %w", err)
	}
	admin, err := repo.IsUserTenantAdmin(ctx, task.TenantID, task.CreatedBy)
	if err != nil {
		return AuthContext{}, fmt.Errorf("load export admin status: %w", err)
	}
	return AuthContext{TenantID: task.TenantID, UserID: task.CreatedBy, RoleIDs: roles, Grants: grants, IsAdmin: admin}, nil
}

func exportCorrectionSnapshotHash(ctx context.Context, network NetworkRepository, tenantID ID, parameters victoriaMetricsQueryParameters, layer QueryValueLayer) (string, error) {
	if layer == QueryValueRaw {
		return "", nil
	}
	if network == nil {
		return "", errors.New("network inventory is required for corrected export")
	}
	portIDs := []ID{parameters.PortID}
	if parameters.PortID == "" {
		if parameters.DeviceID == "" {
			return "", errors.New("corrected target export requires a device")
		}
		ports, err := network.ListPorts(ctx, tenantID, parameters.DeviceID)
		if err != nil {
			return "", fmt.Errorf("list export ports: %w", err)
		}
		portIDs = portIDs[:0]
		for _, port := range ports {
			portIDs = append(portIDs, port.ID)
		}
	}
	slices.Sort(portIDs)
	type policySnapshot struct {
		PortID ID         `json:"port_id"`
		Policy PortPolicy `json:"policy"`
	}
	policies := make([]policySnapshot, 0, len(portIDs))
	for _, portID := range portIDs {
		policy, err := network.GetPortPolicy(ctx, tenantID, portID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("load export port policy %s: %w", portID, err)
		}
		policies = append(policies, policySnapshot{PortID: portID, Policy: policy.Normalize()})
	}
	_, hash, err := canonicalJSONHash(policies)
	return hash, err
}

func verifyExportCorrectionSnapshot(ctx context.Context, network NetworkRepository, task ExportTask) error {
	var querySnapshot exportQuerySnapshot
	if err := decodeStrictJSON(task.QueryJSON, &querySnapshot); err != nil {
		return err
	}
	var versions exportVersionSnapshot
	if err := decodeStrictJSON(task.VersionsJSON, &versions); err != nil {
		return err
	}
	if task.DatasetKey == FlowTrafficDataset {
		if versions.CompletenessMode != flowExportCompletenessMode || versions.CorrectionSnapshotHash != "" {
			return errors.New("Flow export version snapshot is invalid")
		}
		return nil
	}
	if task.DatasetKey == FlowRecordDetailDataset {
		if versions.CompletenessMode != flowDetailExportCompletenessMode || versions.CorrectionSnapshotHash != "" {
			return errors.New("Flow detail export version snapshot is invalid")
		}
		return nil
	}
	if task.DatasetKey == FlowVPNFindingsDataset {
		if versions.CompletenessMode != vpnFindingExportCompletenessMode || versions.CorrectionSnapshotHash != "" {
			return errors.New("VPN finding export version snapshot is invalid")
		}
		return nil
	}
	var parameters victoriaMetricsQueryParameters
	if err := decodeStrictJSON(querySnapshot.Query.Parameters, &parameters); err != nil {
		return err
	}
	hash, err := exportCorrectionSnapshotHash(ctx, network, task.TenantID, parameters, task.ValueLayer)
	if err != nil {
		return err
	}
	if hash != versions.CorrectionSnapshotHash {
		return errors.New("export correction snapshot changed; refusing to reinterpret the task")
	}
	return nil
}

func decodeStrictJSON(data []byte, target any) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return ensureDashboardJSONEOF(decoder)
}

func timeDurationSeconds(seconds uint32) time.Duration {
	return time.Duration(seconds) * time.Second
}
