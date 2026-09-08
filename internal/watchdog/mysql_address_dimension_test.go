package watchdog

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	_ "github.com/go-sql-driver/mysql"
)

func TestMySQLAddressDimensionPreviewPublishAndDraftCAS(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_MYSQL_TEST_DSN to run address dimension integration test")
	}
	ctx := context.Background()
	store, err := OpenMySQLStore(ctx, MySQLConfig{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := ApplyMySQLMigrations(ctx, store.db); err != nil {
		t.Fatal(err)
	}
	runEmbeddedMigrationAgain(t, store.db, "047")
	runEmbeddedMigrationAgain(t, store.db, "048")
	tenantID := ID("01JADDRESSDIMENSIONTENANT1")
	userID := ID("01JADDRESSDIMENSIONUSER001")
	_, _ = store.db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	defer store.db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	if _, err := store.db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'Address Dimension', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id) VALUES (?, ?, 'dimension@test.invalid', 'Dimension', 'active', 'test', 'dimension-test')`, userID, tenantID); err != nil {
		t.Fatal(err)
	}
	operator, err := store.CreateISPOperator(ctx, ISPOperator{
		TenantID: tenantID, Code: "CT", Name: "China Telecom", Category: "carrier", ASNs: []uint32{4809, 4134}, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := store.UpsertAddressPrefix(ctx, AddressPrefix{
		ID: "00000000-0000-4000-8000-000000000001", TenantID: tenantID, CIDR: "10.0.0.0/8",
		OperatorID: operator.ID, Labels: map[string]string{"flow": "local", "business": "private"}, Source: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	createBaseImport := func(checksum string, rowCountV4, rowCountV6 uint64) ID {
		t.Helper()
		importID, idErr := newManagementID()
		if idErr != nil {
			t.Fatal(idErr)
		}
		if _, insertErr := store.db.ExecContext(ctx, `
			INSERT INTO address_imports (
				id, tenant_id, source_slot, format, original_name, artifact_ref,
				checksum_sha256, size_bytes, status, row_count_v4, row_count_v6, created_by
			) VALUES (?, ?, 'combined', 'mmdb', 'base.mmdb', 'address-import://base', ?, 1, 'ready', ?, ?, ?)
		`, importID, tenantID, checksum, rowCountV4, rowCountV6, userID); insertErr != nil {
			t.Fatal(insertErr)
		}
		return importID
	}
	baseImport1 := createBaseImport("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, 2)
	baseSlot, err := store.ActivateAddressImport(ctx, tenantID, baseImport1, userID, 0)
	if err != nil {
		t.Fatal(err)
	}
	objects := DiskDimensionObjectStore{Dir: t.TempDir(), MaxBytes: 1 << 20}
	publisher, err := NewMySQLAddressDimensionPublisher(store, objects)
	if err != nil {
		t.Fatal(err)
	}
	effective := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	preview, err := publisher.PreviewAddressDimension(ctx, tenantID, effective)
	if err != nil {
		t.Fatal(err)
	}
	if preview.OperatorCount != 1 || preview.BundleSchemaVersion != flowdimension.BundleSchemaVersion {
		t.Fatalf("preview operator contract = %#v", preview)
	}
	prefix.Labels["business"] = "changed"
	prefix, err = store.UpdateAddressPrefix(ctx, prefix, prefix.RowVersion)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.PublishAddressDimension(ctx, tenantID, userID, AddressDimensionPublishRequest{EffectiveFrom: effective, PreviewDigest: preview.DraftDigest}); !errors.Is(err, ErrAddressDimensionDraftChanged) {
		t.Fatalf("expected draft CAS error, got %v", err)
	}
	preview, err = publisher.PreviewAddressDimension(ctx, tenantID, effective)
	if err != nil {
		t.Fatal(err)
	}
	operator.Name = "China Telecom updated"
	operator, err = store.UpdateISPOperator(ctx, operator, operator.RowVersion)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.PublishAddressDimension(ctx, tenantID, userID, AddressDimensionPublishRequest{EffectiveFrom: effective, PreviewDigest: preview.DraftDigest}); !errors.Is(err, ErrAddressDimensionDraftChanged) {
		t.Fatalf("operator update did not invalidate preview: %v", err)
	}
	preview, err = publisher.PreviewAddressDimension(ctx, tenantID, effective)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := publisher.PublishAddressDimension(ctx, tenantID, userID, AddressDimensionPublishRequest{EffectiveFrom: effective, PreviewDigest: preview.DraftDigest})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Version != 1 || snapshot.PrefixCount != 1 || snapshot.EntryCount != 2 || snapshot.Status != AddressDimensionStatusActive || snapshot.ApprovalState != AddressDimensionApprovalPending {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	if snapshot.SourcePrefixCount != 7 || len(snapshot.SourceManifest) != 1 || snapshot.SourceManifest[0].ImportID != baseImport1 || snapshot.SourceManifest[0].SlotRowVersion != baseSlot.RowVersion {
		t.Fatalf("snapshot source manifest = %#v count=%d", snapshot.SourceManifest, snapshot.SourcePrefixCount)
	}
	path, err := objects.ResolveDimensionObject(snapshot.ObjectRef)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := flowdimension.DecodeAndCompileBundle(data, snapshot.Checksum, flowdimension.CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	compiledOperator, ok := compiled.OperatorByFlowISPID(operator.FlowISPID)
	if !ok || compiledOperator.ID != string(operator.ID) || compiledOperator.Name != "China Telecom updated" || len(compiledOperator.ASNs) != 2 || compiled.Metadata().OperatorCount != 1 {
		t.Fatalf("published operator definition = %#v, %t; metadata=%+v", compiledOperator, ok, compiled.Metadata())
	}
	classified := compiled.ClassifyEndpoints(netip.MustParseAddr("10.1.2.3"), netip.MustParseAddr("203.0.113.7"))
	if classified.Direction != flowdimension.DirectionOut || classified.Business != "changed" {
		t.Fatalf("published bundle classification = %#v", classified)
	}
	items, cursor, _, err := publisher.ListAddressDimensionSnapshots(ctx, tenantID, AddressDimensionListFilter{Limit: 10})
	if err != nil || len(items) != 1 || cursor != "" || items[0].ID != snapshot.ID {
		t.Fatalf("list snapshots = %#v %q %v", items, cursor, err)
	}
	items, cursor, total, err := publisher.ListAddressDimensionSnapshots(ctx, tenantID, AddressDimensionListFilter{
		Search: snapshot.Checksum[:12], Status: AddressDimensionStatusActive, Sort: "prefixes", Desc: true, Limit: 25, TableMode: true,
	})
	if err != nil || len(items) != 1 || cursor != "" || total != 1 || items[0].ID != snapshot.ID {
		t.Fatalf("server snapshots = %#v %q total=%d err=%v", items, cursor, total, err)
	}

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keyResolver, err := NewStaticAddressDimensionPublicKeyResolver([]AddressDimensionTrustedPublicKey{{
		TenantID: tenantID, KeyID: "dimension-test-key", Key: publicKey,
	}})
	if err != nil {
		t.Fatal(err)
	}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: tenantID, UserID: userID, IsAdmin: true}, nil
		},
		AddressDimensions: publisher, DimensionLifecycle: publisher, DimensionConsumers: publisher, DimensionKeys: keyResolver,
	})
	approve := func(item AddressDimensionSnapshot) AddressDimensionSnapshot {
		t.Helper()
		signedAt := time.Now().UTC().Add(time.Second).Truncate(time.Millisecond)
		payload, payloadErr := AddressDimensionSigningPayload(item, "dimension-test-key", signedAt)
		if payloadErr != nil {
			t.Fatal(payloadErr)
		}
		body, marshalErr := json.Marshal(map[string]any{
			"signing_key_id": "dimension-test-key", "signed_at": signedAt,
			"signature": base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload)),
		})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		request := httptest.NewRequest(http.MethodPost, "/api/v1/dimensions/address/versions/"+string(item.ID)+"/actions/approve", bytes.NewReader(body))
		request.Header.Set("If-Match", quotedRowVersion(item.RowVersion))
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("approve API = %d %s", response.Code, response.Body.String())
		}
		var approved AddressDimensionSnapshot
		if decodeErr := json.Unmarshal(response.Body.Bytes(), &approved); decodeErr != nil {
			t.Fatal(decodeErr)
		}
		return approved
	}

	snapshot = approve(snapshot)
	if snapshot.ApprovalState != AddressDimensionApprovalApproved || snapshot.SignatureAlgorithm != AddressDimensionSignatureAlgorithm || snapshot.RowVersion != 2 {
		t.Fatalf("approved snapshot = %#v", snapshot)
	}
	activation, err := publisher.ActivateAddressDimension(ctx, tenantID, userID, AddressDimensionActivationRequest{
		SnapshotID: snapshot.ID, EffectiveFrom: effective, ExpectedRowVersion: snapshot.RowVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if activation.Reason != AddressDimensionActivationPublish || activation.SnapshotID != snapshot.ID {
		t.Fatalf("activation = %#v", activation)
	}
	snapshot, err = publisher.GetAddressDimensionSnapshot(ctx, tenantID, snapshot.ID)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := publisher.GetAddressDimensionActivationAt(ctx, tenantID, effective.Add(time.Minute))
	if err != nil || selected.SnapshotID != snapshot.ID {
		t.Fatalf("selected activation = %#v, %v", selected, err)
	}

	prefix.Labels["business"] = "second"
	prefix, err = store.UpdateAddressPrefix(ctx, prefix, prefix.RowVersion)
	if err != nil {
		t.Fatal(err)
	}
	effective2 := effective.Add(time.Hour)
	preview2, err := publisher.PreviewAddressDimension(ctx, tenantID, effective2)
	if err != nil {
		t.Fatal(err)
	}
	baseImport2 := createBaseImport("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 8, 3)
	lockTx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback()
	if _, err := loadAddressDimensionSources(ctx, lockTx, tenantID, true); err != nil {
		t.Fatal(err)
	}
	type activationResult struct {
		slot AddressImportSlot
		err  error
	}
	activationDone := make(chan activationResult, 1)
	go func() {
		slot, activateErr := store.ActivateAddressImport(ctx, tenantID, baseImport2, userID, baseSlot.RowVersion)
		activationDone <- activationResult{slot: slot, err: activateErr}
	}()
	select {
	case result := <-activationDone:
		t.Fatalf("source activation bypassed publication lock: %#v, %v", result.slot, result.err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := lockTx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-activationDone:
		if result.err != nil {
			t.Fatal(result.err)
		}
		baseSlot = result.slot
	case <-time.After(5 * time.Second):
		t.Fatal("source activation remained blocked after publication lock released")
	}
	if _, err := publisher.PublishAddressDimension(ctx, tenantID, userID, AddressDimensionPublishRequest{EffectiveFrom: effective2, PreviewDigest: preview2.DraftDigest}); !errors.Is(err, ErrAddressDimensionDraftChanged) {
		t.Fatalf("active import switch did not invalidate preview: %v", err)
	}
	preview2, err = publisher.PreviewAddressDimension(ctx, tenantID, effective2)
	if err != nil {
		t.Fatal(err)
	}
	snapshot2, err := publisher.PublishAddressDimension(ctx, tenantID, userID, AddressDimensionPublishRequest{EffectiveFrom: effective2, PreviewDigest: preview2.DraftDigest})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot2.SourcePrefixCount != 11 || len(snapshot2.SourceManifest) != 1 || snapshot2.SourceManifest[0].ImportID != baseImport2 || snapshot2.SourceManifest[0].SlotRowVersion != baseSlot.RowVersion {
		t.Fatalf("snapshot2 source manifest = %#v count=%d", snapshot2.SourceManifest, snapshot2.SourcePrefixCount)
	}
	if _, err := publisher.ActivateAddressDimension(ctx, tenantID, userID, AddressDimensionActivationRequest{
		SnapshotID: snapshot2.ID, EffectiveFrom: effective2, ExpectedRowVersion: snapshot2.RowVersion,
	}); !errors.Is(err, ErrAddressDimensionInvalidTransition) {
		t.Fatalf("pending snapshot activation error = %v", err)
	}
	snapshot2 = approve(snapshot2)
	if _, err := publisher.ActivateAddressDimension(ctx, tenantID, userID, AddressDimensionActivationRequest{
		SnapshotID: snapshot2.ID, EffectiveFrom: effective2, ExpectedRowVersion: snapshot2.RowVersion,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot2, err = publisher.GetAddressDimensionSnapshot(ctx, tenantID, snapshot2.ID)
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err = publisher.RetireAddressDimension(ctx, tenantID, userID, AddressDimensionRetireRequest{
		SnapshotID: snapshot.ID, ExpectedRowVersion: snapshot.RowVersion, Reason: "superseded",
	})
	if err != nil || snapshot.Status != AddressDimensionStatusRetired || snapshot.RetentionUntil == nil {
		t.Fatalf("retire old snapshot = %#v, %v", snapshot, err)
	}
	rollbackAt := effective2.Add(time.Hour)
	rollback, err := publisher.RollbackAddressDimension(ctx, tenantID, userID, AddressDimensionRollbackRequest{
		SnapshotID: snapshot.ID, EffectiveFrom: rollbackAt, ExpectedRowVersion: snapshot.RowVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rollback.Reason != AddressDimensionActivationRollback || rollback.RollbackOfSnapshotID != snapshot2.ID {
		t.Fatalf("rollback activation = %#v", rollback)
	}
	snapshot, err = publisher.GetAddressDimensionSnapshot(ctx, tenantID, snapshot.ID)
	if err != nil || snapshot.RetentionUntil != nil || snapshot.Status != AddressDimensionStatusActive {
		t.Fatalf("rollback must cancel object retention: %#v, %v", snapshot, err)
	}
	selected, err = publisher.GetAddressDimensionActivationAt(ctx, tenantID, rollbackAt)
	if err != nil || selected.SnapshotID != snapshot.ID {
		t.Fatalf("rollback selection = %#v, %v", selected, err)
	}
	if _, err := publisher.RetireAddressDimension(ctx, tenantID, userID, AddressDimensionRetireRequest{
		SnapshotID: snapshot.ID, ExpectedRowVersion: snapshot.RowVersion, Reason: "must replace latest first",
	}); !errors.Is(err, ErrAddressDimensionInvalidTransition) {
		t.Fatalf("retire latest activation error = %v", err)
	}
	if _, err := publisher.RetireAddressDimension(ctx, tenantID, userID, AddressDimensionRetireRequest{
		SnapshotID: snapshot2.ID, ExpectedRowVersion: snapshot2.RowVersion, Reason: "rolled back",
	}); err != nil {
		t.Fatal(err)
	}

	ack, err := publisher.ReportAddressDimensionAcknowledgement(ctx, AddressDimensionAcknowledgement{
		TenantID: tenantID, SnapshotID: snapshot.ID, WorkerID: "worker-a", BootID: "boot-a",
		SoftwareVersion: "1.0.0", Checksum: snapshot.Checksum, State: AddressDimensionAckFailed,
		ErrorCode: "DOWNLOAD_FAILED", ErrorMessage: "temporary failure",
	})
	if err != nil || ack.State != AddressDimensionAckFailed || ack.InstalledAt != nil || ack.RowVersion != 1 {
		t.Fatalf("failed ack = %#v, %v", ack, err)
	}
	ack, err = publisher.ReportAddressDimensionAcknowledgement(ctx, AddressDimensionAcknowledgement{
		TenantID: tenantID, SnapshotID: snapshot.ID, WorkerID: "worker-a", BootID: "boot-a",
		SoftwareVersion: "1.0.0", Checksum: snapshot.Checksum, State: AddressDimensionAckInstalled,
	})
	if err != nil || ack.State != AddressDimensionAckInstalled || ack.InstalledAt == nil || ack.RowVersion != 2 {
		t.Fatalf("installed ack = %#v, %v", ack, err)
	}
	if _, err := publisher.ReportAddressDimensionAcknowledgement(ctx, AddressDimensionAcknowledgement{
		TenantID: tenantID, SnapshotID: snapshot.ID, WorkerID: "worker-a", BootID: "boot-a",
		SoftwareVersion: "1.0.0", Checksum: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", State: AddressDimensionAckInstalled,
	}); !errors.Is(err, ErrAddressDimensionInvalid) {
		t.Fatalf("mismatched ack checksum error = %v", err)
	}
	if _, err := publisher.ReportAddressDimensionAcknowledgement(ctx, AddressDimensionAcknowledgement{
		TenantID: tenantID, SnapshotID: snapshot2.ID, WorkerID: "worker-b", BootID: "boot-b",
		SoftwareVersion: "1.1.0", Checksum: snapshot2.Checksum, State: AddressDimensionAckInstalled,
	}); err != nil {
		t.Fatal(err)
	}
	summary, err := publisher.GetAddressDimensionConsumerSummary(ctx, tenantID, snapshot.ID)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Scope != AddressDimensionConsumerScopeObserved || summary.Queryability != AddressDimensionQueryabilityPartial || summary.Observed != 2 || summary.Ready != 1 || summary.Current != 1 || summary.Ahead != 1 {
		t.Fatalf("consumer summary = %#v", summary)
	}
	statusResponse := httptest.NewRecorder()
	router.ServeHTTP(statusResponse, httptest.NewRequest(http.MethodGet, "/api/v1/dimensions/address/status?at="+rollbackAt.Format(time.RFC3339), nil))
	if statusResponse.Code != http.StatusOK || !bytes.Contains(statusResponse.Body.Bytes(), []byte(`"queryability":"partial_observed"`)) {
		t.Fatalf("consumer runtime status = %d %s", statusResponse.Code, statusResponse.Body.String())
	}
	consumerResponse := httptest.NewRecorder()
	router.ServeHTTP(consumerResponse, httptest.NewRequest(http.MethodGet, "/api/v1/dimensions/address/versions/"+string(snapshot.ID)+"/consumers?state=unreported&drift=ahead", nil))
	if consumerResponse.Code != http.StatusOK || !bytes.Contains(consumerResponse.Body.Bytes(), []byte(`"worker_id":"worker-b"`)) {
		t.Fatalf("consumer status list = %d %s", consumerResponse.Code, consumerResponse.Body.String())
	}
	ahead, cursor, err := publisher.ListAddressDimensionConsumers(ctx, tenantID, snapshot.ID, AddressDimensionConsumerFilter{Drift: AddressDimensionDriftAhead, Limit: 10})
	if err != nil || cursor != "" || len(ahead) != 1 || ahead[0].WorkerID != "worker-b" || ahead[0].TargetState != AddressDimensionConsumerUnreported || ahead[0].LatestInstalledSnapshot != snapshot2.ID {
		t.Fatalf("ahead consumers = %#v cursor=%q err=%v", ahead, cursor, err)
	}
	firstPage, cursor, err := publisher.ListAddressDimensionConsumers(ctx, tenantID, snapshot.ID, AddressDimensionConsumerFilter{Limit: 1})
	if err != nil || len(firstPage) != 1 || cursor == "" {
		t.Fatalf("consumer first page = %#v cursor=%q err=%v", firstPage, cursor, err)
	}
	secondPage, next, err := publisher.ListAddressDimensionConsumers(ctx, tenantID, snapshot.ID, AddressDimensionConsumerFilter{Limit: 1, Cursor: cursor})
	if err != nil || len(secondPage) != 1 || next != "" || secondPage[0].WorkerID == firstPage[0].WorkerID {
		t.Fatalf("consumer second page = %#v next=%q err=%v", secondPage, next, err)
	}

	reference, err := publisher.ReportAddressDimensionReference(ctx, AddressDimensionReference{
		TenantID: tenantID, SnapshotID: snapshot.ID, ConsumerKind: "flow_fact", ConsumerID: "partition-0",
		MinEventTime: effective, MaxEventTime: effective.Add(time.Hour), RetainUntil: effective.Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	reference, err = publisher.ReportAddressDimensionReference(ctx, AddressDimensionReference{
		TenantID: tenantID, SnapshotID: snapshot.ID, ConsumerKind: "flow_fact", ConsumerID: "partition-0",
		MinEventTime: effective.Add(-time.Hour), MaxEventTime: effective.Add(2 * time.Hour), RetainUntil: effective.Add(48 * time.Hour),
	})
	if err != nil || !reference.MinEventTime.Equal(effective.Add(-time.Hour)) || !reference.MaxEventTime.Equal(effective.Add(2*time.Hour)) || !reference.RetainUntil.Equal(effective.Add(48*time.Hour)) || reference.RowVersion != 2 {
		t.Fatalf("merged reference = %#v, %v", reference, err)
	}

	effective3 := rollbackAt.Add(time.Hour)
	preview3, err := publisher.PreviewAddressDimension(ctx, tenantID, effective3)
	if err != nil {
		t.Fatal(err)
	}
	snapshot3, err := publisher.PublishAddressDimension(ctx, tenantID, userID, AddressDimensionPublishRequest{EffectiveFrom: effective3, PreviewDigest: preview3.DraftDigest})
	if err != nil {
		t.Fatal(err)
	}
	snapshot3, err = publisher.RejectAddressDimension(ctx, tenantID, userID, snapshot3.ID, snapshot3.RowVersion, "review failed")
	if err != nil || snapshot3.ApprovalState != AddressDimensionApprovalRejected || snapshot3.DecisionReason != "review failed" {
		t.Fatalf("rejected snapshot = %#v, %v", snapshot3, err)
	}
	if _, err := publisher.RejectAddressDimension(ctx, tenantID, userID, snapshot3.ID, snapshot3.RowVersion, "reject twice"); !errors.Is(err, ErrAddressDimensionInvalidTransition) {
		t.Fatalf("repeated rejection error = %v", err)
	}
	var auditCount int
	if err := store.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM audit_logs
		WHERE tenant_id = ? AND resource_type = 'dimension_snapshot'
	`, tenantID).Scan(&auditCount); err != nil || auditCount < 6 {
		t.Fatalf("dimension lifecycle audit count = %d, %v", auditCount, err)
	}
	if _, err := store.db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID); err != nil {
		t.Fatalf("tenant lifecycle cascade: %v", err)
	}
	for _, table := range []string{"dimension_snapshots", "dimension_snapshot_activations", "dimension_snapshot_acks", "dimension_snapshot_references"} {
		var count int
		if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table+` WHERE tenant_id = ?`, tenantID).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s rows after tenant cascade = %d, %v", table, count, err)
		}
	}
}
