// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for updating physical resource provisioning info.
//!
//! Physical provisioning tracks actual physical bytes consumed (including
//! replication overhead), unlike virtual provisioning which tracks user-visible
//! sizes. The CTE supports per-level diffs (different values at project vs
//! silo/fleet levels) for deduplication of shared read-only resources.

use crate::db::column_walker::AllColumnsOf;
use crate::db::model::ByteCount;
use crate::db::model::PhysicalProvisioningCollection;
use crate::db::model::PhysicalProvisioningResource;
use crate::db::model::ResourceTypeProvisioned;
use crate::db::raw_query_builder::{QueryBuilder, TypedSqlQuery};
use crate::db::true_or_cast_error::matches_sentinel;
use const_format::concatcp;
use diesel::pg::Pg;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use nexus_db_schema::schema::physical_provisioning_collection;
use nexus_db_schema::schema::physical_provisioning_resource;
use omicron_common::api::external;
use omicron_common::api::external::MessagePair;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;

type AllColumnsOfPhysicalResource =
    AllColumnsOf<physical_provisioning_resource::table>;
type AllColumnsOfPhysicalCollection =
    AllColumnsOf<physical_provisioning_collection::table>;

/// Origin column type for dedup checking within the CTE.
#[derive(Clone, Debug)]
pub enum DedupOriginColumn {
    /// Matches `disk_type_crucible.origin_image`
    Image,
    /// Matches `disk_type_crucible.origin_snapshot`
    Snapshot,
}

impl DedupOriginColumn {
    fn column_name(&self) -> &'static str {
        match self {
            DedupOriginColumn::Image => "origin_image",
            DedupOriginColumn::Snapshot => "origin_snapshot",
        }
    }
}

/// Info needed for atomic dedup within the CTE.
///
/// When provided, the CTE atomically checks whether the given origin
/// image/snapshot is already referenced by another resource in the same
/// project/silo. If so, the `read_only` diff is zeroed out (deduped).
#[derive(Clone, Debug)]
pub enum DedupInfo {
    /// Disk dedup: checks other disks with same origin, AND (for image
    /// origins) checks if the origin image has its own
    /// physical_provisioning_resource entry.
    Disk {
        origin_column: DedupOriginColumn,
        origin_id: uuid::Uuid,
        /// The disk being created/deleted (excluded from dedup check).
        disk_id: uuid::Uuid,
    },
    /// Image delete dedup: checks if any surviving disk references this
    /// image. If so, the read_only charge is not subtracted.
    ImageDelete {
        image_id: uuid::Uuid,
    },
    /// Snapshot delete dedup: checks if any surviving disk references this
    /// snapshot. If so, the read_only charge is not subtracted.
    SnapshotDelete {
        snapshot_id: uuid::Uuid,
    },
}

const NOT_ENOUGH_CPUS_SENTINEL: &'static str =
    "Not enough cpus (physical)";
const NOT_ENOUGH_MEMORY_SENTINEL: &'static str =
    "Not enough memory (physical)";
const NOT_ENOUGH_PHYSICAL_STORAGE_SENTINEL: &'static str =
    "Not enough physical storage";

/// Translates a generic pool error to an external error based
/// on messages which may be emitted when provisioning physical resources.
pub fn from_diesel(e: DieselError) -> external::Error {
    let sentinels = [
        NOT_ENOUGH_CPUS_SENTINEL,
        NOT_ENOUGH_MEMORY_SENTINEL,
        NOT_ENOUGH_PHYSICAL_STORAGE_SENTINEL,
    ];
    if let Some(sentinel) = matches_sentinel(&e, &sentinels) {
        match sentinel {
            NOT_ENOUGH_CPUS_SENTINEL => {
                return external::Error::InsufficientCapacity {
                    message: MessagePair::new_full(
                        "vCPU Limit Exceeded: Not enough vCPUs to complete request. Either stop unused instances to free up resources or contact the rack operator to request a capacity increase.".to_string(),
                        "User tried to allocate an instance but the physical provisioning resource table indicated that there were not enough CPUs available to satisfy the request.".to_string(),
                    )
                }
            }
            NOT_ENOUGH_MEMORY_SENTINEL => {
                return external::Error::InsufficientCapacity {
                    message: MessagePair::new_full(
                        "Memory Limit Exceeded: Not enough memory to complete request. Either stop unused instances to free up resources or contact the rack operator to request a capacity increase.".to_string(),
                        "User tried to allocate an instance but the physical provisioning resource table indicated that there were not enough RAM available to satisfy the request.".to_string(),
                    )
                }
            }
            NOT_ENOUGH_PHYSICAL_STORAGE_SENTINEL => {
                return external::Error::InsufficientCapacity {
                    message: MessagePair::new_full(
                        "Physical Storage Limit Exceeded: Not enough physical storage capacity to complete request. Either remove unneeded disks and snapshots to free up resources or contact the rack operator to request a capacity increase.".to_string(),
                        "User tried to allocate a disk or snapshot but the physical provisioning resource table indicated that there was not enough physical storage available to satisfy the request.".to_string(),
                    )
                }
            }
            _ => {}
        }
    }
    nexus_db_errors::public_error_from_diesel(
        e,
        nexus_db_errors::ErrorHandler::Server,
    )
}

/// The physical resource collection is only updated when a resource is inserted
/// or deleted from the resource provisioning table. By probing for the presence
/// or absence of a resource, we can update collections at the same time as we
/// create or destroy the resource, which helps make the operation idempotent.
///
/// Unlike the virtual provisioning system, storage updates carry separate
/// diffs for project-level and silo/fleet-level to support deduplication
/// of shared read-only resources.
#[derive(Clone)]
enum UpdateKind {
    /// Insert storage provisioning. The resource stores project-level diffs
    /// for idempotency. Silo/fleet diffs may differ due to deduplication.
    InsertStorage {
        resource: PhysicalProvisioningResource,
        // Silo/fleet-level diffs (may differ from project diffs stored in
        // resource)
        silo_writable: ByteCount,
        silo_zfs_snapshot: ByteCount,
        silo_read_only: ByteCount,
        // Optional dedup info for atomic read-only deduplication
        dedup: Option<DedupInfo>,
    },
    DeleteStorage {
        id: uuid::Uuid,
        // Project-level diffs (how much to subtract from project)
        project_writable: ByteCount,
        project_zfs_snapshot: ByteCount,
        project_read_only: ByteCount,
        // Silo/fleet-level diffs
        silo_writable: ByteCount,
        silo_zfs_snapshot: ByteCount,
        silo_read_only: ByteCount,
        // Optional dedup info for atomic read-only deduplication
        dedup: Option<DedupInfo>,
    },
    InsertInstance(PhysicalProvisioningResource),
    DeleteInstance {
        id: uuid::Uuid,
        cpus_diff: i64,
        ram_diff: ByteCount,
    },
}

type SelectableSql<T> = <
    <T as diesel::Selectable<Pg>>::SelectExpression as diesel::Expression
>::SqlType;

/// Constructs a CTE for updating physical resource provisioning information
/// in all collections for a particular object.
pub struct PhysicalProvisioningCollectionUpdate {}

impl PhysicalProvisioningCollectionUpdate {
    // Generic utility for updating all collections including this resource,
    // even transitively.
    //
    // Propagated updates include:
    // - Project
    // - Silo
    // - Fleet
    fn apply_update(
        update_kind: UpdateKind,
        project_id: uuid::Uuid,
    ) -> TypedSqlQuery<SelectableSql<PhysicalProvisioningCollection>> {
        let mut query = QueryBuilder::new();

        query.sql("
WITH
  parent_silo AS (SELECT project.silo_id AS id FROM project WHERE project.id = ").param().sql("),")
            .bind::<sql_types::Uuid, _>(project_id).sql("
  all_collections
    AS (
      ((SELECT ").param().sql(" AS id) UNION (SELECT parent_silo.id AS id FROM parent_silo))
      UNION (SELECT ").param().sql(" AS id)
    ),")
            .bind::<sql_types::Uuid, _>(project_id)
            .bind::<sql_types::Uuid, _>(*nexus_db_fixed_data::FLEET_ID)
            .sql("
  quotas
    AS (
      SELECT
        silo_quotas.silo_id,
        silo_quotas.cpus,
        silo_quotas.memory_bytes AS memory,
        silo_quotas.storage_bytes AS storage,
        silo_quotas.physical_storage_bytes AS physical_storage
      FROM
        silo_quotas INNER JOIN parent_silo ON silo_quotas.silo_id = parent_silo.id
    ),
  silo_provisioned
    AS (
      SELECT
        physical_provisioning_collection.id,
        physical_provisioning_collection.cpus_provisioned,
        physical_provisioning_collection.ram_provisioned,
        physical_provisioning_collection.physical_writable_disk_bytes,
        physical_provisioning_collection.physical_zfs_snapshot_bytes,
        physical_provisioning_collection.physical_read_only_disk_bytes
      FROM
        physical_provisioning_collection
        INNER JOIN parent_silo ON physical_provisioning_collection.id = parent_silo.id
    ),");

        // Add dedup sub-CTEs if dedup info is provided for storage operations.
        let dedup = match &update_kind {
            UpdateKind::InsertStorage { dedup, .. }
            | UpdateKind::DeleteStorage { dedup, .. } => dedup.clone(),
            _ => None,
        };
        if let Some(ref dedup_info) = dedup {
            match dedup_info {
                DedupInfo::Disk { origin_column, origin_id, disk_id } => {
                    let col = origin_column.column_name();
                    // dedup_in_project: check if this origin is already
                    // referenced by another (non-deleted) disk in the same
                    // project, OR (for image origins) if the image itself has
                    // a physical_provisioning_resource entry.
                    query.sql("
  dedup_in_project AS (
    SELECT (
      EXISTS (
        SELECT 1 FROM disk_type_crucible dtc
        INNER JOIN disk d ON d.id = dtc.disk_id
        WHERE dtc.")
                        .sql(col)
                        .sql(" = ").param().sql("
        AND d.project_id = ").param().sql("
        AND d.time_deleted IS NULL
        AND d.id != ").param().sql("
      )");
                    // Also check if the origin itself has a
                    // physical_provisioning_resource entry (meaning its
                    // accounting is active). For images, join against the
                    // image table; for snapshots, join against the snapshot
                    // table.
                    match origin_column {
                        DedupOriginColumn::Image => {
                            query.sql("
      OR EXISTS (
        SELECT 1 FROM physical_provisioning_resource ppr
        INNER JOIN image i ON i.id = ppr.id
        WHERE i.id = ").param().sql("
        AND i.project_id = ").param().sql("
        AND i.time_deleted IS NULL
        AND ppr.physical_read_only_disk_bytes > 0
      )");
                            query
                                .bind::<sql_types::Uuid, _>(*origin_id)
                                .bind::<sql_types::Uuid, _>(project_id)
                                .bind::<sql_types::Uuid, _>(*disk_id)
                                .bind::<sql_types::Uuid, _>(*origin_id)
                                .bind::<sql_types::Uuid, _>(project_id);
                        }
                        DedupOriginColumn::Snapshot => {
                            query.sql("
      OR EXISTS (
        SELECT 1 FROM physical_provisioning_resource ppr
        INNER JOIN snapshot s ON s.id = ppr.id
        WHERE s.id = ").param().sql("
        AND s.project_id = ").param().sql("
        AND s.time_deleted IS NULL
        AND ppr.physical_read_only_disk_bytes > 0
      )");
                            query
                                .bind::<sql_types::Uuid, _>(*origin_id)
                                .bind::<sql_types::Uuid, _>(project_id)
                                .bind::<sql_types::Uuid, _>(*disk_id)
                                .bind::<sql_types::Uuid, _>(*origin_id)
                                .bind::<sql_types::Uuid, _>(project_id);
                        }
                    }
                    query.sql("
    ) AS already_referenced
  ),");

                    // dedup_in_silo: check if this origin is already
                    // referenced by another (non-deleted) disk anywhere in
                    // the same silo, OR (for image origins) if the image
                    // itself has a physical_provisioning_resource entry.
                    query.sql("
  dedup_in_silo AS (
    SELECT (
      EXISTS (
        SELECT 1 FROM disk_type_crucible dtc
        INNER JOIN disk d ON d.id = dtc.disk_id
        INNER JOIN project p ON d.project_id = p.id
        WHERE dtc.")
                        .sql(col)
                        .sql(" = ").param().sql("
        AND p.silo_id = (SELECT id FROM parent_silo)
        AND d.time_deleted IS NULL
        AND d.id != ").param().sql("
      )");
                    match origin_column {
                        DedupOriginColumn::Image => {
                            query.sql("
      OR EXISTS (
        SELECT 1 FROM physical_provisioning_resource ppr
        INNER JOIN image i ON i.id = ppr.id
        WHERE i.id = ").param().sql("
        AND i.silo_id = (SELECT id FROM parent_silo)
        AND i.time_deleted IS NULL
        AND ppr.physical_read_only_disk_bytes > 0
      )");
                            query
                                .bind::<sql_types::Uuid, _>(*origin_id)
                                .bind::<sql_types::Uuid, _>(*disk_id)
                                .bind::<sql_types::Uuid, _>(*origin_id);
                        }
                        DedupOriginColumn::Snapshot => {
                            // Snapshots don't have silo_id directly;
                            // join through project to find the silo.
                            query.sql("
      OR EXISTS (
        SELECT 1 FROM physical_provisioning_resource ppr
        INNER JOIN snapshot s ON s.id = ppr.id
        INNER JOIN project p ON s.project_id = p.id
        WHERE s.id = ").param().sql("
        AND p.silo_id = (SELECT id FROM parent_silo)
        AND s.time_deleted IS NULL
        AND ppr.physical_read_only_disk_bytes > 0
      )");
                            query
                                .bind::<sql_types::Uuid, _>(*origin_id)
                                .bind::<sql_types::Uuid, _>(*disk_id)
                                .bind::<sql_types::Uuid, _>(*origin_id);
                        }
                    }
                    query.sql("
    ) AS already_referenced
  ),");
                }

                DedupInfo::ImageDelete { image_id } => {
                    // dedup_in_project: check if any surviving disk
                    // references this image in the same project.
                    query.sql("
  dedup_in_project AS (
    SELECT EXISTS (
      SELECT 1 FROM disk_type_crucible dtc
      INNER JOIN disk d ON d.id = dtc.disk_id
      WHERE dtc.origin_image = ").param().sql("
      AND d.project_id = ").param().sql("
      AND d.time_deleted IS NULL
    ) AS already_referenced
  ),")
                        .bind::<sql_types::Uuid, _>(*image_id)
                        .bind::<sql_types::Uuid, _>(project_id);

                    // dedup_in_silo: check if any surviving disk references
                    // this image anywhere in the same silo.
                    query.sql("
  dedup_in_silo AS (
    SELECT EXISTS (
      SELECT 1 FROM disk_type_crucible dtc
      INNER JOIN disk d ON d.id = dtc.disk_id
      INNER JOIN project p ON d.project_id = p.id
      WHERE dtc.origin_image = ").param().sql("
      AND p.silo_id = (SELECT id FROM parent_silo)
      AND d.time_deleted IS NULL
    ) AS already_referenced
  ),")
                        .bind::<sql_types::Uuid, _>(*image_id);
                }

                DedupInfo::SnapshotDelete { snapshot_id } => {
                    // dedup_in_project: check if any surviving disk
                    // references this snapshot in the same project.
                    query.sql("
  dedup_in_project AS (
    SELECT EXISTS (
      SELECT 1 FROM disk_type_crucible dtc
      INNER JOIN disk d ON d.id = dtc.disk_id
      WHERE dtc.origin_snapshot = ").param().sql("
      AND d.project_id = ").param().sql("
      AND d.time_deleted IS NULL
    ) AS already_referenced
  ),")
                        .bind::<sql_types::Uuid, _>(*snapshot_id)
                        .bind::<sql_types::Uuid, _>(project_id);

                    // dedup_in_silo: check if any surviving disk references
                    // this snapshot anywhere in the same silo.
                    query.sql("
  dedup_in_silo AS (
    SELECT EXISTS (
      SELECT 1 FROM disk_type_crucible dtc
      INNER JOIN disk d ON d.id = dtc.disk_id
      INNER JOIN project p ON d.project_id = p.id
      WHERE dtc.origin_snapshot = ").param().sql("
      AND p.silo_id = (SELECT id FROM parent_silo)
      AND d.time_deleted IS NULL
    ) AS already_referenced
  ),")
                        .bind::<sql_types::Uuid, _>(*snapshot_id);
                }
            }
        }

        match update_kind.clone() {
            UpdateKind::InsertInstance(resource) => {
                query.sql("
  do_update
    AS (
      SELECT
        (
          (
            (
              SELECT count(*)
              FROM physical_provisioning_resource
              WHERE physical_provisioning_resource.id = ").param().sql("
              LIMIT 1
            )
            = 0
            AND CAST(
                IF(
                  (
                    ").param().sql(" = 0
                    OR (SELECT quotas.cpus FROM quotas LIMIT 1)
                      >= (
                          (SELECT silo_provisioned.cpus_provisioned FROM silo_provisioned LIMIT 1)
                          + ").param().sql(concatcp!("
                        )
                  ),
                  'TRUE',
                  '", NOT_ENOUGH_CPUS_SENTINEL, "'
                )
                  AS BOOL
              )
          )
          AND CAST(
              IF(
                (
                  ")).param().sql(" = 0
                  OR (SELECT quotas.memory FROM quotas LIMIT 1)
                    >= (
                        (SELECT silo_provisioned.ram_provisioned FROM silo_provisioned LIMIT 1)
                        + ").param().sql(concatcp!("
                      )
                ),
                'TRUE',
                '", NOT_ENOUGH_MEMORY_SENTINEL, "'
              )
                AS BOOL
            )
        )
          AS update
    ),"))
                .bind::<sql_types::Uuid, _>(resource.id)
                .bind::<sql_types::BigInt, _>(resource.cpus_provisioned)
                .bind::<sql_types::BigInt, _>(resource.cpus_provisioned)
                .bind::<sql_types::BigInt, _>(resource.ram_provisioned)
                .bind::<sql_types::BigInt, _>(resource.ram_provisioned)
            },
            UpdateKind::InsertStorage { ref resource, silo_writable, silo_zfs_snapshot, silo_read_only, .. } => {
                // For storage inserts, check physical storage quota.
                // Physical quota is nullable: NULL = no limit.
                // Total silo physical = writable + zfs_snapshot + read_only.
                query.sql("
  do_update
    AS (
      SELECT
        (
          (
            SELECT count(*)
            FROM physical_provisioning_resource
            WHERE physical_provisioning_resource.id = ").param().sql("
            LIMIT 1
          )
          = 0
          AND CAST(
              IF(
                (
                  (SELECT quotas.physical_storage FROM quotas LIMIT 1) IS NULL
                  OR (SELECT quotas.physical_storage FROM quotas LIMIT 1)
                    >= (
                        (
                          SELECT
                            silo_provisioned.physical_writable_disk_bytes
                            + silo_provisioned.physical_zfs_snapshot_bytes
                            + silo_provisioned.physical_read_only_disk_bytes
                          FROM
                            silo_provisioned
                          LIMIT
                            1
                        )
                        + ").param().sql(" + ").param().sql(" + ").param().sql(concatcp!("
                      )
                ),
                'TRUE',
                '", NOT_ENOUGH_PHYSICAL_STORAGE_SENTINEL, "'
              )
                AS BOOL
            )
        )
          AS update
    ),"))
                .bind::<sql_types::Uuid, _>(resource.id)
                .bind::<sql_types::BigInt, _>(silo_writable)
                .bind::<sql_types::BigInt, _>(silo_zfs_snapshot)
                .bind::<sql_types::BigInt, _>(silo_read_only)
            },
            UpdateKind::DeleteStorage { id, .. } => {
                query.sql("
  do_update
    AS (
      SELECT
        (
          SELECT
            count(*)
          FROM
            physical_provisioning_resource
          WHERE
            physical_provisioning_resource.id = ").param().sql("
          LIMIT
            1
        ) = 1
          AS update
    ),")
                .bind::<sql_types::Uuid, _>(id)
            },
            UpdateKind::DeleteInstance { id, .. } => {
                query.sql("
  do_update
    AS (
      SELECT
        (
          SELECT
            count(*)
          FROM
            physical_provisioning_resource
          WHERE
            physical_provisioning_resource.id = ").param().sql("
          LIMIT
            1
        ) = 1 AND
        EXISTS (
          SELECT 1
          FROM
            instance
          WHERE
            instance.id = ").param().sql("
          LIMIT 1
        )
          AS update
    ),")
                .bind::<sql_types::Uuid, _>(id)
                .bind::<sql_types::Uuid, _>(id)
            },
        };

        match update_kind.clone() {
            UpdateKind::InsertInstance(resource)
            | UpdateKind::InsertStorage { resource, .. } => query
                .sql(
                    "
  unused_cte_arm
    AS (
      INSERT
      INTO
        physical_provisioning_resource
          (
            id,
            time_modified,
            resource_type,
            physical_writable_disk_bytes,
            physical_zfs_snapshot_bytes,
            physical_read_only_disk_bytes,
            cpus_provisioned,
            ram_provisioned
          )
      VALUES
        (",
                )
                .param()
                .sql(", DEFAULT, ")
                .param()
                .sql(", ")
                .param()
                .sql(", ")
                .param()
                .sql(", ")
                .param()
                .sql(", ")
                .param()
                .sql(", ")
                .param()
                .sql(
                    ")
      ON CONFLICT
      DO
        NOTHING
      RETURNING ",
                )
                .sql(AllColumnsOfPhysicalResource::with_prefix(
                    "physical_provisioning_resource",
                ))
                .sql("),")
                .bind::<sql_types::Uuid, _>(resource.id)
                .bind::<sql_types::Text, _>(resource.resource_type)
                .bind::<sql_types::BigInt, _>(
                    resource.physical_writable_disk_bytes,
                )
                .bind::<sql_types::BigInt, _>(
                    resource.physical_zfs_snapshot_bytes,
                )
                .bind::<sql_types::BigInt, _>(
                    resource.physical_read_only_disk_bytes,
                )
                .bind::<sql_types::BigInt, _>(resource.cpus_provisioned)
                .bind::<sql_types::BigInt, _>(resource.ram_provisioned),
            UpdateKind::DeleteInstance { id, .. }
            | UpdateKind::DeleteStorage { id, .. } => query
                .sql(
                    "
  unused_cte_arm
    AS (
      DELETE FROM
        physical_provisioning_resource
      WHERE
        physical_provisioning_resource.id = ",
                )
                .param()
                .sql(
                    "
        AND (SELECT do_update.update FROM do_update LIMIT 1)
      RETURNING ",
                )
                .sql(AllColumnsOfPhysicalResource::with_prefix(
                    "physical_provisioning_resource",
                ))
                .sql("),")
                .bind::<sql_types::Uuid, _>(id),
        };

        query.sql(
            "
  physical_provisioning_collection
    AS (
      UPDATE
        physical_provisioning_collection
      SET",
        );
        match update_kind.clone() {
            UpdateKind::InsertInstance(resource) => query
                .sql(
                    "
        time_modified = current_timestamp(),
        cpus_provisioned = physical_provisioning_collection.cpus_provisioned + ",
                )
                .param()
                .sql(
                    ",
        ram_provisioned = physical_provisioning_collection.ram_provisioned + ",
                )
                .param()
                .bind::<sql_types::BigInt, _>(resource.cpus_provisioned)
                .bind::<sql_types::BigInt, _>(resource.ram_provisioned),
            UpdateKind::InsertStorage { ref resource, silo_writable, silo_zfs_snapshot, silo_read_only, ref dedup } => {
                // Use CASE to apply different diffs at project vs silo/fleet level.
                query
                .sql("
        time_modified = current_timestamp(),
        physical_writable_disk_bytes
          = physical_provisioning_collection.physical_writable_disk_bytes
          + CASE WHEN physical_provisioning_collection.id = ")
                .param()
                .sql(" THEN ")
                .param()
                .sql(" ELSE ")
                .param()
                .sql(" END,
        physical_zfs_snapshot_bytes
          = physical_provisioning_collection.physical_zfs_snapshot_bytes
          + CASE WHEN physical_provisioning_collection.id = ")
                .param()
                .sql(" THEN ")
                .param()
                .sql(" ELSE ")
                .param()
                .sql(" END,
        physical_read_only_disk_bytes
          = physical_provisioning_collection.physical_read_only_disk_bytes
          + CASE WHEN physical_provisioning_collection.id = ")
                .param()
                .sql(" THEN ");
                // Project-level read_only diff: if dedup, check dedup_in_project
                if dedup.is_some() {
                    query.sql(
                        "CASE WHEN (SELECT already_referenced FROM dedup_in_project) THEN 0 ELSE "
                    ).param().sql(" END");
                } else {
                    query.param();
                }
                query.sql(" ELSE ");
                // Silo/fleet-level read_only diff: if dedup, check dedup_in_silo
                if dedup.is_some() {
                    query.sql(
                        "CASE WHEN (SELECT already_referenced FROM dedup_in_silo) THEN 0 ELSE "
                    ).param().sql(" END");
                } else {
                    query.param();
                }
                query.sql(" END")
                .bind::<sql_types::Uuid, _>(project_id)
                .bind::<sql_types::BigInt, _>(resource.physical_writable_disk_bytes)
                .bind::<sql_types::BigInt, _>(silo_writable)
                .bind::<sql_types::Uuid, _>(project_id)
                .bind::<sql_types::BigInt, _>(resource.physical_zfs_snapshot_bytes)
                .bind::<sql_types::BigInt, _>(silo_zfs_snapshot)
                .bind::<sql_types::Uuid, _>(project_id)
                .bind::<sql_types::BigInt, _>(resource.physical_read_only_disk_bytes)
                .bind::<sql_types::BigInt, _>(silo_read_only)
            },
            UpdateKind::DeleteInstance { cpus_diff, ram_diff, .. } => query
                .sql(
                    "
        time_modified = current_timestamp(),
        cpus_provisioned = physical_provisioning_collection.cpus_provisioned - ",
                )
                .param()
                .sql(
                    ",
        ram_provisioned = physical_provisioning_collection.ram_provisioned - ",
                )
                .param()
                .bind::<sql_types::BigInt, _>(cpus_diff)
                .bind::<sql_types::BigInt, _>(ram_diff),
            UpdateKind::DeleteStorage {
                project_writable, project_zfs_snapshot, project_read_only,
                silo_writable, silo_zfs_snapshot, silo_read_only,
                ref dedup,
                ..
            } => {
                // Use CASE to apply different diffs at project vs silo/fleet level.
                query
                .sql("
        time_modified = current_timestamp(),
        physical_writable_disk_bytes
          = physical_provisioning_collection.physical_writable_disk_bytes
          - CASE WHEN physical_provisioning_collection.id = ")
                .param()
                .sql(" THEN ")
                .param()
                .sql(" ELSE ")
                .param()
                .sql(" END,
        physical_zfs_snapshot_bytes
          = physical_provisioning_collection.physical_zfs_snapshot_bytes
          - CASE WHEN physical_provisioning_collection.id = ")
                .param()
                .sql(" THEN ")
                .param()
                .sql(" ELSE ")
                .param()
                .sql(" END,
        physical_read_only_disk_bytes
          = physical_provisioning_collection.physical_read_only_disk_bytes
          - CASE WHEN physical_provisioning_collection.id = ")
                .param()
                .sql(" THEN ");
                // Project-level read_only diff: if dedup, check dedup_in_project
                if dedup.is_some() {
                    query.sql(
                        "CASE WHEN (SELECT already_referenced FROM dedup_in_project) THEN 0 ELSE "
                    ).param().sql(" END");
                } else {
                    query.param();
                }
                query.sql(" ELSE ");
                // Silo/fleet-level read_only diff: if dedup, check dedup_in_silo
                if dedup.is_some() {
                    query.sql(
                        "CASE WHEN (SELECT already_referenced FROM dedup_in_silo) THEN 0 ELSE "
                    ).param().sql(" END");
                } else {
                    query.param();
                }
                query.sql(" END")
                .bind::<sql_types::Uuid, _>(project_id)
                .bind::<sql_types::BigInt, _>(project_writable)
                .bind::<sql_types::BigInt, _>(silo_writable)
                .bind::<sql_types::Uuid, _>(project_id)
                .bind::<sql_types::BigInt, _>(project_zfs_snapshot)
                .bind::<sql_types::BigInt, _>(silo_zfs_snapshot)
                .bind::<sql_types::Uuid, _>(project_id)
                .bind::<sql_types::BigInt, _>(project_read_only)
                .bind::<sql_types::BigInt, _>(silo_read_only)
            },
        };

        query.sql("
      WHERE
        physical_provisioning_collection.id = ANY (SELECT all_collections.id FROM all_collections)
        AND (SELECT do_update.update FROM do_update LIMIT 1)
      RETURNING "
        ).sql(AllColumnsOfPhysicalCollection::with_prefix("physical_provisioning_collection")).sql("
    )
SELECT "
    ).sql(AllColumnsOfPhysicalCollection::with_prefix("physical_provisioning_collection")).sql("
FROM
  physical_provisioning_collection
");

        query.query()
    }

    /// Silo-level CTE variant: updates silo + fleet collections only (no
    /// project). Used for silo-scoped image create/delete.
    ///
    /// Only supports InsertStorage and DeleteStorage update kinds (the only
    /// ones needed for image accounting at the silo level).
    fn apply_update_silo_level(
        update_kind: UpdateKind,
        silo_id: uuid::Uuid,
    ) -> TypedSqlQuery<SelectableSql<PhysicalProvisioningCollection>> {
        let mut query = QueryBuilder::new();

        query.sql("
WITH
  parent_silo AS (SELECT ").param().sql(" AS id),")
            .bind::<sql_types::Uuid, _>(silo_id).sql("
  all_collections
    AS (
      (SELECT parent_silo.id AS id FROM parent_silo)
      UNION (SELECT ").param().sql(" AS id)
    ),")
            .bind::<sql_types::Uuid, _>(*nexus_db_fixed_data::FLEET_ID)
            .sql("
  quotas
    AS (
      SELECT
        silo_quotas.silo_id,
        silo_quotas.cpus,
        silo_quotas.memory_bytes AS memory,
        silo_quotas.storage_bytes AS storage,
        silo_quotas.physical_storage_bytes AS physical_storage
      FROM
        silo_quotas INNER JOIN parent_silo ON silo_quotas.silo_id = parent_silo.id
    ),
  silo_provisioned
    AS (
      SELECT
        physical_provisioning_collection.id,
        physical_provisioning_collection.cpus_provisioned,
        physical_provisioning_collection.ram_provisioned,
        physical_provisioning_collection.physical_writable_disk_bytes,
        physical_provisioning_collection.physical_zfs_snapshot_bytes,
        physical_provisioning_collection.physical_read_only_disk_bytes
      FROM
        physical_provisioning_collection
        INNER JOIN parent_silo ON physical_provisioning_collection.id = parent_silo.id
    ),");

        // Add dedup sub-CTEs if dedup info is provided.
        let dedup = match &update_kind {
            UpdateKind::InsertStorage { dedup, .. }
            | UpdateKind::DeleteStorage { dedup, .. } => dedup.clone(),
            _ => None,
        };
        if let Some(ref dedup_info) = dedup {
            match dedup_info {
                DedupInfo::ImageDelete { image_id } => {
                    // For silo-level image delete: check if any surviving
                    // disk references this image anywhere in the silo.
                    // dedup_in_project is unused but we emit it for
                    // consistency with the CTE shape; set it to the same
                    // as silo check.
                    query.sql("
  dedup_in_project AS (
    SELECT EXISTS (
      SELECT 1 FROM disk_type_crucible dtc
      INNER JOIN disk d ON d.id = dtc.disk_id
      INNER JOIN project p ON d.project_id = p.id
      WHERE dtc.origin_image = ").param().sql("
      AND p.silo_id = (SELECT id FROM parent_silo)
      AND d.time_deleted IS NULL
    ) AS already_referenced
  ),")
                        .bind::<sql_types::Uuid, _>(*image_id);

                    query.sql("
  dedup_in_silo AS (
    SELECT EXISTS (
      SELECT 1 FROM disk_type_crucible dtc
      INNER JOIN disk d ON d.id = dtc.disk_id
      INNER JOIN project p ON d.project_id = p.id
      WHERE dtc.origin_image = ").param().sql("
      AND p.silo_id = (SELECT id FROM parent_silo)
      AND d.time_deleted IS NULL
    ) AS already_referenced
  ),")
                        .bind::<sql_types::Uuid, _>(*image_id);
                }
                _ => {
                    // Disk dedup at silo level is not expected; panic for
                    // safety.
                    unreachable!(
                        "only ImageDelete dedup is supported at silo level"
                    );
                }
            }
        }

        match update_kind.clone() {
            UpdateKind::InsertStorage { ref resource, silo_writable, silo_zfs_snapshot, silo_read_only, .. } => {
                query.sql("
  do_update
    AS (
      SELECT
        (
          (
            SELECT count(*)
            FROM physical_provisioning_resource
            WHERE physical_provisioning_resource.id = ").param().sql("
            LIMIT 1
          )
          = 0
          AND CAST(
              IF(
                (
                  (SELECT quotas.physical_storage FROM quotas LIMIT 1) IS NULL
                  OR (SELECT quotas.physical_storage FROM quotas LIMIT 1)
                    >= (
                        (
                          SELECT
                            silo_provisioned.physical_writable_disk_bytes
                            + silo_provisioned.physical_zfs_snapshot_bytes
                            + silo_provisioned.physical_read_only_disk_bytes
                          FROM
                            silo_provisioned
                          LIMIT
                            1
                        )
                        + ").param().sql(" + ").param().sql(" + ").param().sql(concatcp!("
                      )
                ),
                'TRUE',
                '", NOT_ENOUGH_PHYSICAL_STORAGE_SENTINEL, "'
              )
                AS BOOL
            )
        )
          AS update
    ),"))
                .bind::<sql_types::Uuid, _>(resource.id)
                .bind::<sql_types::BigInt, _>(silo_writable)
                .bind::<sql_types::BigInt, _>(silo_zfs_snapshot)
                .bind::<sql_types::BigInt, _>(silo_read_only)
            },
            UpdateKind::DeleteStorage { id, .. } => {
                query.sql("
  do_update
    AS (
      SELECT
        (
          SELECT
            count(*)
          FROM
            physical_provisioning_resource
          WHERE
            physical_provisioning_resource.id = ").param().sql("
          LIMIT
            1
        ) = 1
          AS update
    ),")
                .bind::<sql_types::Uuid, _>(id)
            },
            _ => unreachable!("silo-level CTE only supports storage operations"),
        };

        match update_kind.clone() {
            UpdateKind::InsertStorage { resource, .. } => query
                .sql(
                    "
  unused_cte_arm
    AS (
      INSERT
      INTO
        physical_provisioning_resource
          (
            id,
            time_modified,
            resource_type,
            physical_writable_disk_bytes,
            physical_zfs_snapshot_bytes,
            physical_read_only_disk_bytes,
            cpus_provisioned,
            ram_provisioned
          )
      VALUES
        (",
                )
                .param()
                .sql(", DEFAULT, ")
                .param()
                .sql(", ")
                .param()
                .sql(", ")
                .param()
                .sql(", ")
                .param()
                .sql(", ")
                .param()
                .sql(", ")
                .param()
                .sql(
                    ")
      ON CONFLICT
      DO
        NOTHING
      RETURNING ",
                )
                .sql(AllColumnsOfPhysicalResource::with_prefix(
                    "physical_provisioning_resource",
                ))
                .sql("),")
                .bind::<sql_types::Uuid, _>(resource.id)
                .bind::<sql_types::Text, _>(resource.resource_type)
                .bind::<sql_types::BigInt, _>(
                    resource.physical_writable_disk_bytes,
                )
                .bind::<sql_types::BigInt, _>(
                    resource.physical_zfs_snapshot_bytes,
                )
                .bind::<sql_types::BigInt, _>(
                    resource.physical_read_only_disk_bytes,
                )
                .bind::<sql_types::BigInt, _>(resource.cpus_provisioned)
                .bind::<sql_types::BigInt, _>(resource.ram_provisioned),
            UpdateKind::DeleteStorage { id, .. } => query
                .sql(
                    "
  unused_cte_arm
    AS (
      DELETE FROM
        physical_provisioning_resource
      WHERE
        physical_provisioning_resource.id = ",
                )
                .param()
                .sql(
                    "
        AND (SELECT do_update.update FROM do_update LIMIT 1)
      RETURNING ",
                )
                .sql(AllColumnsOfPhysicalResource::with_prefix(
                    "physical_provisioning_resource",
                ))
                .sql("),")
                .bind::<sql_types::Uuid, _>(id),
            _ => unreachable!("silo-level CTE only supports storage operations"),
        };

        // For silo-level, there's no project, so storage diffs are uniform
        // across silo and fleet.
        query.sql(
            "
  physical_provisioning_collection
    AS (
      UPDATE
        physical_provisioning_collection
      SET",
        );
        match update_kind.clone() {
            UpdateKind::InsertStorage { silo_writable, silo_zfs_snapshot, silo_read_only, ref dedup, .. } => {
                query.sql("
        time_modified = current_timestamp(),
        physical_writable_disk_bytes
          = physical_provisioning_collection.physical_writable_disk_bytes + ").param().sql(",
        physical_zfs_snapshot_bytes
          = physical_provisioning_collection.physical_zfs_snapshot_bytes + ").param().sql(",
        physical_read_only_disk_bytes
          = physical_provisioning_collection.physical_read_only_disk_bytes + ");
                if dedup.is_some() {
                    query.sql(
                        "CASE WHEN (SELECT already_referenced FROM dedup_in_silo) THEN 0 ELSE "
                    ).param().sql(" END");
                } else {
                    query.param();
                }
                query
                    .bind::<sql_types::BigInt, _>(silo_writable)
                    .bind::<sql_types::BigInt, _>(silo_zfs_snapshot)
                    .bind::<sql_types::BigInt, _>(silo_read_only)
            },
            UpdateKind::DeleteStorage {
                silo_writable, silo_zfs_snapshot, silo_read_only,
                ref dedup,
                ..
            } => {
                query.sql("
        time_modified = current_timestamp(),
        physical_writable_disk_bytes
          = physical_provisioning_collection.physical_writable_disk_bytes - ").param().sql(",
        physical_zfs_snapshot_bytes
          = physical_provisioning_collection.physical_zfs_snapshot_bytes - ").param().sql(",
        physical_read_only_disk_bytes
          = physical_provisioning_collection.physical_read_only_disk_bytes - ");
                if dedup.is_some() {
                    query.sql(
                        "CASE WHEN (SELECT already_referenced FROM dedup_in_silo) THEN 0 ELSE "
                    ).param().sql(" END");
                } else {
                    query.param();
                }
                query
                    .bind::<sql_types::BigInt, _>(silo_writable)
                    .bind::<sql_types::BigInt, _>(silo_zfs_snapshot)
                    .bind::<sql_types::BigInt, _>(silo_read_only)
            },
            _ => unreachable!("silo-level CTE only supports storage operations"),
        };

        query.sql("
      WHERE
        physical_provisioning_collection.id = ANY (SELECT all_collections.id FROM all_collections)
        AND (SELECT do_update.update FROM do_update LIMIT 1)
      RETURNING "
        ).sql(AllColumnsOfPhysicalCollection::with_prefix("physical_provisioning_collection")).sql("
    )
SELECT "
    ).sql(AllColumnsOfPhysicalCollection::with_prefix("physical_provisioning_collection")).sql("
FROM
  physical_provisioning_collection
");

        query.query()
    }

    /// Insert storage provisioning at silo level (silo + fleet only).
    /// Used for silo-scoped image accounting.
    pub fn new_insert_storage_silo_level(
        id: uuid::Uuid,
        silo_writable: ByteCount,
        silo_zfs_snapshot: ByteCount,
        silo_read_only: ByteCount,
        silo_id: uuid::Uuid,
        storage_type: crate::db::datastore::StorageType,
    ) -> TypedSqlQuery<SelectableSql<PhysicalProvisioningCollection>> {
        let mut resource =
            PhysicalProvisioningResource::new(id, storage_type.into());
        // For silo-level resources, the resource record stores silo-level
        // diffs (there is no project level).
        resource.physical_writable_disk_bytes = silo_writable;
        resource.physical_zfs_snapshot_bytes = silo_zfs_snapshot;
        resource.physical_read_only_disk_bytes = silo_read_only;

        Self::apply_update_silo_level(
            UpdateKind::InsertStorage {
                resource,
                silo_writable,
                silo_zfs_snapshot,
                silo_read_only,
                dedup: None,
            },
            silo_id,
        )
    }

    /// Delete storage provisioning at silo level (silo + fleet only).
    /// Used for silo-scoped image accounting.
    pub fn new_delete_storage_silo_level(
        id: uuid::Uuid,
        silo_writable: ByteCount,
        silo_zfs_snapshot: ByteCount,
        silo_read_only: ByteCount,
        silo_id: uuid::Uuid,
    ) -> TypedSqlQuery<SelectableSql<PhysicalProvisioningCollection>> {
        Self::apply_update_silo_level(
            UpdateKind::DeleteStorage {
                id,
                project_writable: silo_writable,
                project_zfs_snapshot: silo_zfs_snapshot,
                project_read_only: silo_read_only,
                silo_writable,
                silo_zfs_snapshot,
                silo_read_only,
                dedup: None,
            },
            silo_id,
        )
    }

    /// Delete storage provisioning at silo level with dedup.
    pub fn new_delete_storage_silo_level_deduped(
        id: uuid::Uuid,
        silo_writable: ByteCount,
        silo_zfs_snapshot: ByteCount,
        silo_read_only: ByteCount,
        silo_id: uuid::Uuid,
        dedup: DedupInfo,
    ) -> TypedSqlQuery<SelectableSql<PhysicalProvisioningCollection>> {
        Self::apply_update_silo_level(
            UpdateKind::DeleteStorage {
                id,
                project_writable: silo_writable,
                project_zfs_snapshot: silo_zfs_snapshot,
                project_read_only: silo_read_only,
                silo_writable,
                silo_zfs_snapshot,
                silo_read_only,
                dedup: Some(dedup),
            },
            silo_id,
        )
    }

    /// Insert storage provisioning with per-level diffs.
    ///
    /// The `resource` contains project-level diffs (stored in the resource
    /// table for idempotency). The silo_* parameters are the silo/fleet-level
    /// diffs (may differ from project diffs due to deduplication).
    pub fn new_insert_storage(
        id: uuid::Uuid,
        project_writable: ByteCount,
        project_zfs_snapshot: ByteCount,
        project_read_only: ByteCount,
        silo_writable: ByteCount,
        silo_zfs_snapshot: ByteCount,
        silo_read_only: ByteCount,
        project_id: uuid::Uuid,
        storage_type: crate::db::datastore::StorageType,
    ) -> TypedSqlQuery<SelectableSql<PhysicalProvisioningCollection>> {
        Self::new_insert_storage_impl(
            id,
            project_writable,
            project_zfs_snapshot,
            project_read_only,
            silo_writable,
            silo_zfs_snapshot,
            silo_read_only,
            project_id,
            storage_type,
            None,
        )
    }

    /// Insert storage provisioning with atomic dedup.
    ///
    /// Like `new_insert_storage`, but atomically checks whether the given
    /// origin image/snapshot is already referenced by another disk. If so,
    /// the `read_only` diff is zeroed out at the appropriate level.
    pub fn new_insert_storage_deduped(
        id: uuid::Uuid,
        project_writable: ByteCount,
        project_zfs_snapshot: ByteCount,
        project_read_only: ByteCount,
        silo_writable: ByteCount,
        silo_zfs_snapshot: ByteCount,
        silo_read_only: ByteCount,
        project_id: uuid::Uuid,
        storage_type: crate::db::datastore::StorageType,
        dedup: DedupInfo,
    ) -> TypedSqlQuery<SelectableSql<PhysicalProvisioningCollection>> {
        Self::new_insert_storage_impl(
            id,
            project_writable,
            project_zfs_snapshot,
            project_read_only,
            silo_writable,
            silo_zfs_snapshot,
            silo_read_only,
            project_id,
            storage_type,
            Some(dedup),
        )
    }

    fn new_insert_storage_impl(
        id: uuid::Uuid,
        project_writable: ByteCount,
        project_zfs_snapshot: ByteCount,
        project_read_only: ByteCount,
        silo_writable: ByteCount,
        silo_zfs_snapshot: ByteCount,
        silo_read_only: ByteCount,
        project_id: uuid::Uuid,
        storage_type: crate::db::datastore::StorageType,
        dedup: Option<DedupInfo>,
    ) -> TypedSqlQuery<SelectableSql<PhysicalProvisioningCollection>> {
        let mut resource =
            PhysicalProvisioningResource::new(id, storage_type.into());
        resource.physical_writable_disk_bytes = project_writable;
        resource.physical_zfs_snapshot_bytes = project_zfs_snapshot;
        resource.physical_read_only_disk_bytes = project_read_only;

        Self::apply_update(
            UpdateKind::InsertStorage {
                resource,
                silo_writable,
                silo_zfs_snapshot,
                silo_read_only,
                dedup,
            },
            project_id,
        )
    }

    /// Delete storage provisioning with per-level diffs.
    pub fn new_delete_storage(
        id: uuid::Uuid,
        project_writable: ByteCount,
        project_zfs_snapshot: ByteCount,
        project_read_only: ByteCount,
        silo_writable: ByteCount,
        silo_zfs_snapshot: ByteCount,
        silo_read_only: ByteCount,
        project_id: uuid::Uuid,
    ) -> TypedSqlQuery<SelectableSql<PhysicalProvisioningCollection>> {
        Self::new_delete_storage_impl(
            id,
            project_writable,
            project_zfs_snapshot,
            project_read_only,
            silo_writable,
            silo_zfs_snapshot,
            silo_read_only,
            project_id,
            None,
        )
    }

    /// Delete storage provisioning with atomic dedup.
    pub fn new_delete_storage_deduped(
        id: uuid::Uuid,
        project_writable: ByteCount,
        project_zfs_snapshot: ByteCount,
        project_read_only: ByteCount,
        silo_writable: ByteCount,
        silo_zfs_snapshot: ByteCount,
        silo_read_only: ByteCount,
        project_id: uuid::Uuid,
        dedup: DedupInfo,
    ) -> TypedSqlQuery<SelectableSql<PhysicalProvisioningCollection>> {
        Self::new_delete_storage_impl(
            id,
            project_writable,
            project_zfs_snapshot,
            project_read_only,
            silo_writable,
            silo_zfs_snapshot,
            silo_read_only,
            project_id,
            Some(dedup),
        )
    }

    fn new_delete_storage_impl(
        id: uuid::Uuid,
        project_writable: ByteCount,
        project_zfs_snapshot: ByteCount,
        project_read_only: ByteCount,
        silo_writable: ByteCount,
        silo_zfs_snapshot: ByteCount,
        silo_read_only: ByteCount,
        project_id: uuid::Uuid,
        dedup: Option<DedupInfo>,
    ) -> TypedSqlQuery<SelectableSql<PhysicalProvisioningCollection>> {
        Self::apply_update(
            UpdateKind::DeleteStorage {
                id,
                project_writable,
                project_zfs_snapshot,
                project_read_only,
                silo_writable,
                silo_zfs_snapshot,
                silo_read_only,
                dedup,
            },
            project_id,
        )
    }

    pub fn new_insert_instance(
        id: InstanceUuid,
        cpus_diff: i64,
        ram_diff: ByteCount,
        project_id: uuid::Uuid,
    ) -> TypedSqlQuery<SelectableSql<PhysicalProvisioningCollection>> {
        let mut resource = PhysicalProvisioningResource::new(
            id.into_untyped_uuid(),
            ResourceTypeProvisioned::Instance,
        );
        resource.cpus_provisioned = cpus_diff;
        resource.ram_provisioned = ram_diff;

        Self::apply_update(
            UpdateKind::InsertInstance(resource),
            project_id,
        )
    }

    pub fn new_delete_instance(
        id: InstanceUuid,
        cpus_diff: i64,
        ram_diff: ByteCount,
        project_id: uuid::Uuid,
    ) -> TypedSqlQuery<SelectableSql<PhysicalProvisioningCollection>> {
        Self::apply_update(
            UpdateKind::DeleteInstance {
                id: id.into_untyped_uuid(),
                cpus_diff,
                ram_diff,
            },
            project_id,
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use omicron_test_utils::dev;
    use uuid::Uuid;

    // These tests are a bit of a "change detector", but they're here to help
    // with debugging too. If you change this query, it can be useful to see
    // exactly how the output SQL has been altered.

    #[tokio::test]
    async fn expectorate_query_insert_storage() {
        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let project_writable = 2048.try_into().unwrap();
        let project_zfs_snapshot = 0.try_into().unwrap();
        let project_read_only = 0.try_into().unwrap();
        let silo_writable = 2048.try_into().unwrap();
        let silo_zfs_snapshot = 0.try_into().unwrap();
        let silo_read_only = 0.try_into().unwrap();
        let storage_type = crate::db::datastore::StorageType::Disk;

        let query =
            PhysicalProvisioningCollectionUpdate::new_insert_storage(
                id,
                project_writable,
                project_zfs_snapshot,
                project_read_only,
                silo_writable,
                silo_zfs_snapshot,
                silo_read_only,
                project_id,
                storage_type,
            );
        expectorate_query_contents(
            &query,
            "tests/output/physical_provisioning_collection_update_insert_storage.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_query_delete_storage() {
        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let project_writable = 2048.try_into().unwrap();
        let project_zfs_snapshot = 0.try_into().unwrap();
        let project_read_only = 0.try_into().unwrap();
        let silo_writable = 2048.try_into().unwrap();
        let silo_zfs_snapshot = 0.try_into().unwrap();
        let silo_read_only = 0.try_into().unwrap();

        let query =
            PhysicalProvisioningCollectionUpdate::new_delete_storage(
                id,
                project_writable,
                project_zfs_snapshot,
                project_read_only,
                silo_writable,
                silo_zfs_snapshot,
                silo_read_only,
                project_id,
            );

        expectorate_query_contents(
            &query,
            "tests/output/physical_provisioning_collection_update_delete_storage.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_query_insert_instance() {
        let id = InstanceUuid::nil();
        let project_id = Uuid::nil();
        let cpus_diff = 4;
        let ram_diff = 2048.try_into().unwrap();

        let query =
            PhysicalProvisioningCollectionUpdate::new_insert_instance(
                id, cpus_diff, ram_diff, project_id,
            );

        expectorate_query_contents(
            &query,
            "tests/output/physical_provisioning_collection_update_insert_instance.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_query_delete_instance() {
        let id = InstanceUuid::nil();
        let project_id = Uuid::nil();
        let cpus_diff = 4;
        let ram_diff = 2048.try_into().unwrap();

        let query =
            PhysicalProvisioningCollectionUpdate::new_delete_instance(
                id, cpus_diff, ram_diff, project_id,
            );

        expectorate_query_contents(
            &query,
            "tests/output/physical_provisioning_collection_update_delete_instance.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_query_insert_storage_deduped() {
        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let project_writable = 2048.try_into().unwrap();
        let project_zfs_snapshot = 0.try_into().unwrap();
        let project_read_only = 1024.try_into().unwrap();
        let silo_writable = 2048.try_into().unwrap();
        let silo_zfs_snapshot = 0.try_into().unwrap();
        let silo_read_only = 1024.try_into().unwrap();
        let storage_type = crate::db::datastore::StorageType::Disk;
        let dedup = DedupInfo::Disk {
            origin_column: DedupOriginColumn::Image,
            origin_id: Uuid::nil(),
            disk_id: Uuid::nil(),
        };

        let query =
            PhysicalProvisioningCollectionUpdate::new_insert_storage_deduped(
                id,
                project_writable,
                project_zfs_snapshot,
                project_read_only,
                silo_writable,
                silo_zfs_snapshot,
                silo_read_only,
                project_id,
                storage_type,
                dedup,
            );
        expectorate_query_contents(
            &query,
            "tests/output/physical_provisioning_collection_update_insert_storage_deduped.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_query_delete_storage_deduped() {
        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let project_writable = 2048.try_into().unwrap();
        let project_zfs_snapshot = 0.try_into().unwrap();
        let project_read_only = 1024.try_into().unwrap();
        let silo_writable = 2048.try_into().unwrap();
        let silo_zfs_snapshot = 0.try_into().unwrap();
        let silo_read_only = 1024.try_into().unwrap();
        let dedup = DedupInfo::Disk {
            origin_column: DedupOriginColumn::Snapshot,
            origin_id: Uuid::nil(),
            disk_id: Uuid::nil(),
        };

        let query =
            PhysicalProvisioningCollectionUpdate::new_delete_storage_deduped(
                id,
                project_writable,
                project_zfs_snapshot,
                project_read_only,
                silo_writable,
                silo_zfs_snapshot,
                silo_read_only,
                project_id,
                dedup,
            );

        expectorate_query_contents(
            &query,
            "tests/output/physical_provisioning_collection_update_delete_storage_deduped.sql",
        )
        .await;
    }

    // Explain the possible forms of the SQL query to ensure that it
    // creates a valid SQL string.

    #[tokio::test]
    async fn explain_insert_storage() {
        let logctx = dev::test_setup_log(
            "explain_physical_provisioning_insert_storage",
        );
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let project_writable = 2048.try_into().unwrap();
        let project_zfs_snapshot = 0.try_into().unwrap();
        let project_read_only = 0.try_into().unwrap();
        let silo_writable = 2048.try_into().unwrap();
        let silo_zfs_snapshot = 0.try_into().unwrap();
        let silo_read_only = 0.try_into().unwrap();
        let storage_type = crate::db::datastore::StorageType::Disk;

        let query =
            PhysicalProvisioningCollectionUpdate::new_insert_storage(
                id,
                project_writable,
                project_zfs_snapshot,
                project_read_only,
                silo_writable,
                silo_zfs_snapshot,
                silo_read_only,
                project_id,
                storage_type,
            );
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_delete_storage() {
        let logctx = dev::test_setup_log(
            "explain_physical_provisioning_delete_storage",
        );
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let project_writable = 2048.try_into().unwrap();
        let project_zfs_snapshot = 0.try_into().unwrap();
        let project_read_only = 0.try_into().unwrap();
        let silo_writable = 2048.try_into().unwrap();
        let silo_zfs_snapshot = 0.try_into().unwrap();
        let silo_read_only = 0.try_into().unwrap();

        let query =
            PhysicalProvisioningCollectionUpdate::new_delete_storage(
                id,
                project_writable,
                project_zfs_snapshot,
                project_read_only,
                silo_writable,
                silo_zfs_snapshot,
                silo_read_only,
                project_id,
            );
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_insert_instance() {
        let logctx = dev::test_setup_log(
            "explain_physical_provisioning_insert_instance",
        );
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let id = InstanceUuid::nil();
        let project_id = Uuid::nil();
        let cpus_diff = 16.try_into().unwrap();
        let ram_diff = 2048.try_into().unwrap();

        let query =
            PhysicalProvisioningCollectionUpdate::new_insert_instance(
                id, cpus_diff, ram_diff, project_id,
            );
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_delete_instance() {
        let logctx = dev::test_setup_log(
            "explain_physical_provisioning_delete_instance",
        );
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let id = InstanceUuid::nil();
        let project_id = Uuid::nil();
        let cpus_diff = 16.try_into().unwrap();
        let ram_diff = 2048.try_into().unwrap();

        let query =
            PhysicalProvisioningCollectionUpdate::new_delete_instance(
                id, cpus_diff, ram_diff, project_id,
            );
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_insert_storage_deduped() {
        let logctx = dev::test_setup_log(
            "explain_physical_provisioning_insert_storage_deduped",
        );
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let project_writable = 2048.try_into().unwrap();
        let project_zfs_snapshot = 0.try_into().unwrap();
        let project_read_only = 1024.try_into().unwrap();
        let silo_writable = 2048.try_into().unwrap();
        let silo_zfs_snapshot = 0.try_into().unwrap();
        let silo_read_only = 1024.try_into().unwrap();
        let storage_type = crate::db::datastore::StorageType::Disk;
        let dedup = DedupInfo::Disk {
            origin_column: DedupOriginColumn::Image,
            origin_id: Uuid::nil(),
            disk_id: Uuid::nil(),
        };

        let query =
            PhysicalProvisioningCollectionUpdate::new_insert_storage_deduped(
                id,
                project_writable,
                project_zfs_snapshot,
                project_read_only,
                silo_writable,
                silo_zfs_snapshot,
                silo_read_only,
                project_id,
                storage_type,
                dedup,
            );
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_delete_storage_deduped() {
        let logctx = dev::test_setup_log(
            "explain_physical_provisioning_delete_storage_deduped",
        );
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let project_writable = 2048.try_into().unwrap();
        let project_zfs_snapshot = 0.try_into().unwrap();
        let project_read_only = 1024.try_into().unwrap();
        let silo_writable = 2048.try_into().unwrap();
        let silo_zfs_snapshot = 0.try_into().unwrap();
        let silo_read_only = 1024.try_into().unwrap();
        let dedup = DedupInfo::Disk {
            origin_column: DedupOriginColumn::Snapshot,
            origin_id: Uuid::nil(),
            disk_id: Uuid::nil(),
        };

        let query =
            PhysicalProvisioningCollectionUpdate::new_delete_storage_deduped(
                id,
                project_writable,
                project_zfs_snapshot,
                project_read_only,
                silo_writable,
                silo_zfs_snapshot,
                silo_read_only,
                project_id,
                dedup,
            );
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn expectorate_query_delete_storage_snapshot_delete_deduped() {
        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let project_writable = 0.try_into().unwrap();
        let project_zfs_snapshot = 1024.try_into().unwrap();
        let project_read_only = 1024.try_into().unwrap();
        let silo_writable = 0.try_into().unwrap();
        let silo_zfs_snapshot = 1024.try_into().unwrap();
        let silo_read_only = 1024.try_into().unwrap();
        let dedup = DedupInfo::SnapshotDelete {
            snapshot_id: Uuid::nil(),
        };

        let query =
            PhysicalProvisioningCollectionUpdate::new_delete_storage_deduped(
                id,
                project_writable,
                project_zfs_snapshot,
                project_read_only,
                silo_writable,
                silo_zfs_snapshot,
                silo_read_only,
                project_id,
                dedup,
            );

        expectorate_query_contents(
            &query,
            "tests/output/physical_provisioning_collection_update_delete_storage_snapshot_delete_deduped.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn explain_delete_storage_snapshot_delete_deduped() {
        let logctx = dev::test_setup_log(
            "explain_physical_provisioning_delete_storage_snapshot_delete_deduped",
        );
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let project_writable = 0.try_into().unwrap();
        let project_zfs_snapshot = 1024.try_into().unwrap();
        let project_read_only = 1024.try_into().unwrap();
        let silo_writable = 0.try_into().unwrap();
        let silo_zfs_snapshot = 1024.try_into().unwrap();
        let silo_read_only = 1024.try_into().unwrap();
        let dedup = DedupInfo::SnapshotDelete {
            snapshot_id: Uuid::nil(),
        };

        let query =
            PhysicalProvisioningCollectionUpdate::new_delete_storage_deduped(
                id,
                project_writable,
                project_zfs_snapshot,
                project_read_only,
                silo_writable,
                silo_zfs_snapshot,
                silo_read_only,
                project_id,
                dedup,
            );
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
