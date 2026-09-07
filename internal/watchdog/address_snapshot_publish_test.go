// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package watchdog

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

func TestNormalizeAddressSnapshotImportPrefixesUsesLongestPrefix(t *testing.T) {
	makePrefix := func(id uint64, cidr string, asn uint32) addressSnapshotImportPrefix {
		prefix := netip.MustParsePrefix(cidr)
		start, end := addressSnapshotPrefixRange(prefix)
		return addressSnapshotImportPrefix{id: id, prefix: prefix, value: flowdimension.AddressSnapshotBuildRange{Start: start, End: end, ASN: asn}}
	}
	ranges, err := normalizeAddressSnapshotImportPrefixes([]addressSnapshotImportPrefix{
		makePrefix(1, "192.0.2.0/24", 64500),
		makePrefix(2, "192.0.2.64/26", 64501),
		makePrefix(3, "2001:db8::/126", 64502),
		makePrefix(4, "2001:db8::2/127", 64503),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ranges) != 5 {
		t.Fatalf("normalized ranges = %#v", ranges)
	}
	wants := []struct {
		start string
		end   string
		asn   uint32
	}{
		{"192.0.2.0", "192.0.2.63", 64500},
		{"192.0.2.64", "192.0.2.127", 64501},
		{"192.0.2.128", "192.0.2.255", 64500},
		{"2001:db8::", "2001:db8::1", 64502},
		{"2001:db8::2", "2001:db8::3", 64503},
	}
	for index, want := range wants {
		if ranges[index].Start.String() != want.start || ranges[index].End.String() != want.end || ranges[index].ASN != want.asn {
			t.Fatalf("range %d = %#v, want %#v", index, ranges[index], want)
		}
	}
}

func TestRegisterAddressSnapshotSupplierGeoKeepsStableHierarchy(t *testing.T) {
	nodes := map[string]flowdimension.AddressSnapshotBuildGeoNode{}
	geo := registerAddressSnapshotSupplierGeo(nodes, "AS", "CN", "China", "BJ", "Beijing", "1816670", "Beijing")
	if geo.ContinentID != "supplier/continent/AS" || geo.CountryID != "supplier/country/CN" || geo.ProvinceID != "supplier/province/CN/BJ" || geo.CityID == "" {
		t.Fatalf("supplier Geo path = %#v", geo)
	}
	if len(nodes) != 4 || nodes[geo.CountryID].ParentID != geo.ContinentID || nodes[geo.CityID].ParentID != geo.ProvinceID {
		t.Fatalf("supplier Geo nodes = %#v", nodes)
	}
}

func TestMySQLAddressSnapshotBuildPublishesWADSIdempotently(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_MYSQL_TEST_DSN to run AddressSnap integration test")
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
	tenantID, _ := newIdentityID()
	actorID, _ := newIdentityID()
	importID, _ := newIdentityID()
	defer store.db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	if _, err := store.db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'AddressSnap integration', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id)
		VALUES (?, ?, ?, 'AddressSnap integration', 'active', 'test', ?)`, actorID, tenantID, string(actorID)+"@test.invalid", actorID); err != nil {
		t.Fatal(err)
	}
	item := addressImportFixture(string(importID), tenantID, actorID, AddressImportSlotCombined, "address-snapshot.mmdb")
	item.ChecksumSHA256 = strings.Repeat("b", 64)
	if _, err := store.CreateAddressImport(ctx, item); err != nil {
		t.Fatal(err)
	}
	if err := store.BeginAddressImport(ctx, tenantID, importID); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertAddressImportBatch(ctx, tenantID, importID, []AddressImportRecord{
		{Prefix: "192.0.2.0/24", ContinentCode: "AS", CountryCode: "CN", CountryName: "China", ASN: 64500, Operator: "Supplier A", Source: "mmdb"},
		{Prefix: "192.0.2.64/26", ContinentCode: "AS", CountryCode: "CN", CountryName: "China", SubdivisionCode: "BJ", SubdivisionName: "Beijing", ASN: 64501, Operator: "Supplier B", Source: "mmdb"},
		{Prefix: "2001:db8::/126", ContinentCode: "NA", CountryCode: "US", CountryName: "United States", ASN: 64502, Source: "mmdb"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteAddressImport(ctx, tenantID, importID, AddressImportMetadata{Format: AddressImportFormatMMDB, DatabaseType: "test", IPVersion: 6}, "en"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ActivateAddressImport(ctx, tenantID, importID, actorID, 0); err != nil {
		t.Fatal(err)
	}
	objects := DiskDimensionObjectStore{Dir: t.TempDir(), MaxBytes: 16 << 20}
	effective := time.Now().UTC().Add(time.Hour).Truncate(time.Minute)
	clock := effective.Add(30 * time.Minute)
	publisher, err := NewMySQLAddressDimensionPublisher(store, objects,
		WithAddressDimensionObjectRetention(time.Hour), withAddressDimensionClock(func() time.Time { return clock }))
	if err != nil {
		t.Fatal(err)
	}
	preview, err := publisher.PreviewAddressDimension(ctx, tenantID, effective)
	if err != nil {
		t.Fatal(err)
	}
	request := AddressDimensionPublishRequest{EffectiveFrom: effective, PreviewDigest: preview.DraftDigest}
	payload, err := EncodeAddressDimensionPublishJobPayload(request)
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.EnqueueOperationJob(ctx, OperationJob{
		TenantID: tenantID, JobType: AddressSnapshotBuildJob, CreatedBy: actorID,
		IdempotencyKey: "address-snapshot-lifecycle", RequestHash: strings.Repeat("c", 64), CheckpointJSON: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	jobID := job.ID
	firstLease, err := store.LeaseNextOperationJob(ctx, AddressSnapshotBuildJob, "address-builder-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewAddressSnapshotBuildJobHandler(publisher)
	resultRef, err := handler(ctx, firstLease)
	if err != nil || resultRef != "dimension-snapshot:"+string(jobID) {
		t.Fatalf("first build attempt result=%q err=%v", resultRef, err)
	}
	// Simulate a process crash after the immutable object and snapshot commit,
	// but before the operation job terminal write. A fresh lease must converge
	// on the same job/snapshot identity and checksum.
	if _, err := store.db.ExecContext(ctx, `UPDATE operation_jobs SET lease_expires_at = ? WHERE id = ?`, time.Now().UTC().Add(-time.Second), jobID); err != nil {
		t.Fatal(err)
	}
	secondLease, err := store.LeaseNextOperationJob(ctx, AddressSnapshotBuildJob, "address-builder-b", time.Minute)
	if err != nil || secondLease.ID != jobID || secondLease.AttemptCount != 2 || secondLease.LeaseToken == firstLease.LeaseToken {
		t.Fatalf("AddressSnap takeover = %+v err=%v", secondLease, err)
	}
	if err := store.CompleteOperationJobSucceeded(ctx, jobID, firstLease.LeaseToken, "stale"); !errors.Is(err, ErrOperationJobLeaseLost) {
		t.Fatalf("stale AddressSnap owner finish error = %v", err)
	}
	resultRef, err = handler(ctx, secondLease)
	if err != nil || resultRef != "dimension-snapshot:"+string(jobID) {
		t.Fatalf("takeover build result=%q err=%v", resultRef, err)
	}
	if err := store.CompleteOperationJobSucceeded(ctx, jobID, secondLease.LeaseToken, resultRef); err != nil {
		t.Fatal(err)
	}
	finishedJob, err := store.GetOperationJob(ctx, tenantID, jobID)
	if err != nil || finishedJob.Status != OperationJobStatusSucceeded || finishedJob.ResultRef != resultRef {
		t.Fatalf("finished AddressSnap job = %+v err=%v", finishedJob, err)
	}
	snapshot, err := publisher.GetAddressDimensionSnapshot(ctx, tenantID, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ID != jobID || snapshot.BuildJobID != jobID || snapshot.ObjectFormat != AddressSnapshotObjectFormat || snapshot.ObjectFormatVersion != flowdimension.AddressSnapshotFormatVersion || snapshot.BuilderVersion != AddressSnapshotBuilderVersion || snapshot.ApprovalState != AddressDimensionApprovalPending {
		t.Fatalf("AddressSnap publication = %#v", snapshot)
	}
	path, err := objects.ResolveDimensionObject(snapshot.ObjectRef)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := flowdimension.DecodeAddressSnapshot(data, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if artifact.SnapshotID != string(jobID) || len(artifact.Sources) != 1 || len(artifact.IPv4Ranges) != 3 || len(artifact.IPv6Ranges) != 1 {
		t.Fatalf("AddressSnap artifact = %#v", artifact)
	}
	if len(artifact.Operators) != 2 {
		t.Fatalf("supplier operators = %#v", artifact.Operators)
	}
	foundSupplier := false
	for _, value := range artifact.Values {
		if value.SupplierASN == 64500 && value.SupplierISPID != 0 {
			foundSupplier = true
		}
	}
	if !foundSupplier {
		t.Fatal("source-provided supplier identity was not bound to the range")
	}
	var count int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dimension_snapshots WHERE tenant_id = ? AND build_job_id = ?`, tenantID, jobID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("AddressSnap publication count = %d, %v", count, err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM address_supplier_operators WHERE tenant_id = ?`, tenantID).Scan(&count); err != nil || count != 2 {
		t.Fatalf("stable supplier operator count = %d, %v", count, err)
	}

	firstSupplierIDs := make(map[string]uint16, len(artifact.Operators))
	for _, operator := range artifact.Operators {
		firstSupplierIDs[artifact.Strings[operator.Code]] = operator.ID
	}
	secondJobID, _ := newIdentityID()
	secondEffective := effective.Add(time.Minute)
	secondPreview, err := publisher.PreviewAddressDimension(ctx, tenantID, secondEffective)
	if err != nil {
		t.Fatal(err)
	}
	secondSnapshot, err := publisher.BuildAddressSnapshotPublication(ctx, tenantID, actorID, secondJobID, AddressDimensionPublishRequest{
		EffectiveFrom: secondEffective,
		PreviewDigest: secondPreview.DraftDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	secondPath, err := objects.ResolveDimensionObject(secondSnapshot.ObjectRef)
	if err != nil {
		t.Fatal(err)
	}
	secondData, err := os.ReadFile(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	secondArtifact, err := flowdimension.DecodeAddressSnapshot(secondData, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, operator := range secondArtifact.Operators {
		code := secondArtifact.Strings[operator.Code]
		if firstSupplierIDs[code] != operator.ID {
			t.Fatalf("supplier operator %q changed ID from %d to %d", code, firstSupplierIDs[code], operator.ID)
		}
	}

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	approve := func(item AddressDimensionSnapshot) AddressDimensionSnapshot {
		t.Helper()
		signedAt := clock.UTC().Truncate(time.Millisecond)
		signingPayload, signingErr := AddressDimensionSigningPayload(item, "address-snapshot-lifecycle-key", signedAt)
		if signingErr != nil {
			t.Fatal(signingErr)
		}
		approval, verifyErr := VerifyAddressDimensionApproval(item, "address-snapshot-lifecycle-key", signedAt,
			ed25519.Sign(privateKey, signingPayload), publicKey)
		if verifyErr != nil {
			t.Fatal(verifyErr)
		}
		approved, approveErr := publisher.ApproveAddressDimension(ctx, tenantID, actorID, item.RowVersion, approval)
		if approveErr != nil {
			t.Fatal(approveErr)
		}
		return approved
	}

	snapshot = approve(snapshot)
	if _, err := publisher.ActivateAddressDimension(ctx, tenantID, actorID, AddressDimensionActivationRequest{
		SnapshotID: snapshot.ID, EffectiveFrom: effective, ExpectedRowVersion: snapshot.RowVersion,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err = publisher.GetAddressDimensionSnapshot(ctx, tenantID, snapshot.ID)
	if err != nil {
		t.Fatal(err)
	}
	secondSnapshot = approve(secondSnapshot)
	if _, err := publisher.ActivateAddressDimension(ctx, tenantID, actorID, AddressDimensionActivationRequest{
		SnapshotID: secondSnapshot.ID, EffectiveFrom: secondEffective, ExpectedRowVersion: secondSnapshot.RowVersion,
	}); err != nil {
		t.Fatal(err)
	}
	secondSnapshot, err = publisher.GetAddressDimensionSnapshot(ctx, tenantID, secondSnapshot.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = publisher.RetireAddressDimension(ctx, tenantID, actorID, AddressDimensionRetireRequest{
		SnapshotID: snapshot.ID, ExpectedRowVersion: snapshot.RowVersion, Reason: "superseded",
	})
	if err != nil || snapshot.RetentionUntil == nil {
		t.Fatalf("retire first AddressSnap = %+v err=%v", snapshot, err)
	}
	rollbackAt := secondEffective.Add(time.Minute)
	rollback, err := publisher.RollbackAddressDimension(ctx, tenantID, actorID, AddressDimensionRollbackRequest{
		SnapshotID: snapshot.ID, EffectiveFrom: rollbackAt, ExpectedRowVersion: snapshot.RowVersion,
	})
	if err != nil || rollback.SnapshotID != snapshot.ID || rollback.RollbackOfSnapshotID != secondSnapshot.ID {
		t.Fatalf("AddressSnap rollback = %+v err=%v", rollback, err)
	}
	snapshot, err = publisher.GetAddressDimensionSnapshot(ctx, tenantID, snapshot.ID)
	if err != nil || snapshot.Status != AddressDimensionStatusActive || snapshot.RetentionUntil != nil {
		t.Fatalf("rollback did not cancel first AddressSnap retention = %+v err=%v", snapshot, err)
	}
	secondSnapshot, err = publisher.RetireAddressDimension(ctx, tenantID, actorID, AddressDimensionRetireRequest{
		SnapshotID: secondSnapshot.ID, ExpectedRowVersion: secondSnapshot.RowVersion, Reason: "rolled back",
	})
	if err != nil || secondSnapshot.RetentionUntil == nil {
		t.Fatalf("retire second AddressSnap = %+v err=%v", secondSnapshot, err)
	}
	clock = clock.Add(2 * time.Hour)
	candidates, _, err := publisher.ListAddressDimensionGCCandidates(ctx, tenantID, clock, AddressDimensionGCFilter{Limit: 10})
	if err != nil || len(candidates) != 1 || candidates[0].SnapshotID != secondSnapshot.ID {
		t.Fatalf("AddressSnap GC candidates = %+v err=%v", candidates, err)
	}
	gcJob, err := EnqueueAddressDimensionObjectGC(ctx, store, candidates[0])
	if err != nil {
		t.Fatal(err)
	}
	gcLease, err := store.LeaseNextOperationJob(ctx, AddressDimensionObjectGCJob, "address-gc-a", time.Minute)
	if err != nil || gcLease.ID != gcJob.ID {
		t.Fatalf("lease AddressSnap GC job = %+v err=%v", gcLease, err)
	}
	gcResult, err := NewAddressDimensionObjectGCJobHandler(publisher, func() time.Time { return clock })(ctx, gcLease)
	if err != nil || gcResult != "dimension-object-deleted:"+string(secondSnapshot.ID) {
		t.Fatalf("AddressSnap GC result=%q err=%v", gcResult, err)
	}
	if err := store.CompleteOperationJobSucceeded(ctx, gcLease.ID, gcLease.LeaseToken, gcResult); err != nil {
		t.Fatal(err)
	}
	if _, err := objects.ResolveDimensionObject(secondSnapshot.ObjectRef); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired AddressSnap object still exists: %v", err)
	}
	deletedSnapshot, err := publisher.GetAddressDimensionSnapshot(ctx, tenantID, secondSnapshot.ID)
	if err != nil || deletedSnapshot.ObjectDeletedAt == nil {
		t.Fatalf("deleted AddressSnap metadata = %+v err=%v", deletedSnapshot, err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_logs WHERE tenant_id = ? AND action = 'dimension_object.destroyed' AND resource_id = ?`, tenantID, secondSnapshot.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("AddressSnap destruction receipts=%d err=%v", count, err)
	}
}
