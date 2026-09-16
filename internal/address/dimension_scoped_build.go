package address

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// ScopedPublicationBuild describes metadata shared by immutable objects other
// than AddressSnap. The object bytes and draft digest are supplied by Compile;
// address-specific source and prefix counters remain zero.
type ScopedPublicationBuild struct {
	SnapshotID          ID
	BuildJobID          ID
	EffectiveFrom       time.Time
	PreviewDigest       string
	ObjectFormat        string
	ObjectFormatVersion uint16
	BuilderVersion      string
	BundleSchemaVersion uint32
}

// ScopedPublicationCompiler reads and canonicalizes the editable source. It is
// called once without locks and once inside the short commit transaction. The
// second call must use FOR SHARE when lockSource is true, closing the
// preview/build/edit race without holding a transaction during object I/O.
type ScopedPublicationCompiler func(ctx context.Context, tx *sql.Tx, lockSource bool, snapshotID ID, version uint64, effectiveFrom time.Time) (data []byte, draftDigest string, entryCount uint64, err error)

// BuildScopedPublication saves a canonical object and inserts one pending
// snapshot. The operation job ID is the immutable snapshot/build identity, so a
// retry after a committed DB write converges on the same row.
func (p *Publisher) BuildScopedPublication(ctx context.Context, actorID ID, build ScopedPublicationBuild, compile ScopedPublicationCompiler) (DimensionPublicationSnapshot, error) {
	if p == nil || p.store == nil || p.objects == nil || actorID == "" || build.SnapshotID == "" ||
		build.BuildJobID == "" || build.SnapshotID != build.BuildJobID || !isUTCMinute(build.EffectiveFrom) ||
		!validSHA256Digest(build.PreviewDigest) || build.ObjectFormat != "json" || build.ObjectFormatVersion != 0 ||
		build.BuilderVersion == "" || build.BundleSchemaVersion == 0 || compile == nil || p.scope.validate() != nil {
		return DimensionPublicationSnapshot{}, ErrAddressDimensionInvalid
	}
	if existing, err := p.getAddressSnapshotByBuildJob(ctx, build.BuildJobID); err == nil {
		if !scopedPublicationMatches(existing, build) {
			return DimensionPublicationSnapshot{}, ErrAddressDimensionConflict
		}
		return existing, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return DimensionPublicationSnapshot{}, err
	}

	readTx, err := p.store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return DimensionPublicationSnapshot{}, err
	}
	defer readTx.Rollback()
	var version uint64
	if err := readTx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) + 1 FROM dimension_snapshots
		WHERE module_key = ? AND dimension_key = ?`, p.scope.ModuleKey, p.scope.DimensionKey).Scan(&version); err != nil {
		return DimensionPublicationSnapshot{}, err
	}
	data, digest, entryCount, err := compile(ctx, readTx, false, build.SnapshotID, version, build.EffectiveFrom.UTC())
	if err != nil {
		return DimensionPublicationSnapshot{}, err
	}
	if digest != build.PreviewDigest || len(data) == 0 {
		return DimensionPublicationSnapshot{}, ErrAddressDimensionDraftChanged
	}
	if err := readTx.Commit(); err != nil {
		return DimensionPublicationSnapshot{}, err
	}
	object, err := p.objects.SaveDimensionObject(ctx, build.SnapshotID, data)
	if err != nil {
		return DimensionPublicationSnapshot{}, err
	}

	tx, err := p.store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return DimensionPublicationSnapshot{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublication(ctx, tx); err != nil {
		return DimensionPublicationSnapshot{}, err
	}
	if existing, err := getAddressSnapshotByBuildJobTx(ctx, tx, p.scope, build.BuildJobID); err == nil {
		if !scopedPublicationMatches(existing, build) || existing.Checksum != object.Checksum {
			return DimensionPublicationSnapshot{}, ErrAddressDimensionConflict
		}
		if err := tx.Commit(); err != nil {
			return DimensionPublicationSnapshot{}, err
		}
		return existing, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return DimensionPublicationSnapshot{}, err
	}
	var nextVersion uint64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) + 1 FROM dimension_snapshots
		WHERE module_key = ? AND dimension_key = ?`, p.scope.ModuleKey, p.scope.DimensionKey).Scan(&nextVersion); err != nil {
		return DimensionPublicationSnapshot{}, err
	}
	if nextVersion != version {
		return DimensionPublicationSnapshot{}, ErrAddressSnapshotBuildRace
	}
	lockedData, lockedDigest, lockedEntryCount, err := compile(ctx, tx, true, build.SnapshotID, version, build.EffectiveFrom.UTC())
	if err != nil {
		return DimensionPublicationSnapshot{}, err
	}
	if lockedDigest != build.PreviewDigest || lockedEntryCount != entryCount || !bytes.Equal(lockedData, data) || sha256Checksum(lockedData) != object.Checksum {
		return DimensionPublicationSnapshot{}, ErrAddressDimensionDraftChanged
	}
	emptyManifest, _ := json.Marshal([]AddressDimensionSource{})
	_, err = tx.ExecContext(ctx, `
		INSERT INTO dimension_snapshots (
			id, module_key, dimension_key, version, effective_from,
			object_ref, object_format, object_format_version, builder_version, build_job_id,
			checksum, draft_digest, source_manifest_version, source_manifest, source_prefix_count,
			bundle_schema_version, entry_count, prefix_count, address_set_count,
			max_address_sets_per_record, status, created_by
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0, ?, ?, 0, 0, 0, 'active', NULLIF(?, ''))
	`, build.SnapshotID, p.scope.ModuleKey, p.scope.DimensionKey, version, build.EffectiveFrom.UTC(),
		object.Ref, build.ObjectFormat, build.ObjectFormatVersion, build.BuilderVersion, build.BuildJobID,
		object.Checksum, build.PreviewDigest, emptyManifest, build.BundleSchemaVersion, entryCount, actorID)
	if err != nil {
		var mysqlErr *mysqldriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return DimensionPublicationSnapshot{}, ErrAddressDimensionConflict
		}
		return DimensionPublicationSnapshot{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, actorID, build.SnapshotID, "dimension.snapshot.built", map[string]any{
		"module_key": p.scope.ModuleKey, "dimension_key": p.scope.DimensionKey, "version": version,
		"checksum": object.Checksum, "entry_count": entryCount,
	}); err != nil {
		return DimensionPublicationSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return DimensionPublicationSnapshot{}, err
	}
	return p.GetDimensionPublicationSnapshot(ctx, build.SnapshotID)
}

func scopedPublicationMatches(snapshot DimensionPublicationSnapshot, build ScopedPublicationBuild) bool {
	return snapshot.ID == build.SnapshotID && snapshot.BuildJobID == build.BuildJobID &&
		snapshot.EffectiveFrom.Equal(build.EffectiveFrom.UTC()) && snapshot.DraftDigest == build.PreviewDigest &&
		snapshot.ObjectFormat == build.ObjectFormat && snapshot.ObjectFormatVersion == build.ObjectFormatVersion &&
		snapshot.BuilderVersion == build.BuilderVersion && snapshot.BundleSchemaVersion == build.BundleSchemaVersion
}
