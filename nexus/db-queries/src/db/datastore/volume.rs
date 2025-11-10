// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Volume`]s.

use super::DataStore;
use crate::db;
use crate::db::datastore::OpContext;
use crate::db::datastore::REGION_REDUNDANCY_THRESHOLD;
use crate::db::datastore::RunnableQuery;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::identity::Asset;
use crate::db::model::CrucibleDataset;
use crate::db::model::Disk;
use crate::db::model::DownstairsClientStopRequestNotification;
use crate::db::model::DownstairsClientStoppedNotification;
use crate::db::model::Instance;
use crate::db::model::Region;
use crate::db::model::RegionSnapshot;
use crate::db::model::UpstairsRepairNotification;
use crate::db::model::UpstairsRepairNotificationType;
use crate::db::model::UpstairsRepairProgress;
use crate::db::model::Volume;
use crate::db::model::VolumeResourceUsage;
use crate::db::model::VolumeResourceUsageRecord;
use crate::db::model::VolumeResourceUsageType;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use anyhow::anyhow;
use anyhow::bail;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::OptionalExtension;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_types::identity::Resource;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::ResourceType;
use omicron_common::api::internal::nexus::DownstairsClientStopRequest;
use omicron_common::api::internal::nexus::DownstairsClientStopped;
use omicron_common::api::internal::nexus::RepairProgress;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::DownstairsKind;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::TypedUuid;
use omicron_uuid_kinds::UpstairsKind;
use omicron_uuid_kinds::UpstairsRepairKind;
use omicron_uuid_kinds::VolumeUuid;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use sled_agent_client::VolumeConstructionRequest;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::net::AddrParseError;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub enum VolumeCheckoutReason {
    /// Check out a read-only Volume.
    ReadOnlyCopy,

    /// Check out a Volume to modify and store back to the database.
    CopyAndModify,

    /// Check out a Volume to send to Propolis to start an instance.
    InstanceStart { vmm_id: PropolisUuid },

    /// Check out a Volume to send to a migration destination Propolis.
    InstanceMigrate { vmm_id: PropolisUuid, target_vmm_id: PropolisUuid },

    /// Check out a Volume to send to a Pantry (for background maintenance
    /// operations).
    Pantry,
}

#[derive(Debug, thiserror::Error)]
enum VolumeGetError {
    #[error("Serde error during volume_checkout: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Updated {0} database rows, expected {1}")]
    UnexpectedDatabaseUpdate(usize, usize),

    #[error("Checkout condition failed: {0}")]
    CheckoutConditionFailed(String),

    #[error("Invalid Volume: {0}")]
    InvalidVolume(String),
}

#[derive(Debug, thiserror::Error)]
enum VolumeCreationError {
    #[error("Error from Volume creation: {0}")]
    Public(Error),

    #[error("Serde error during Volume creation: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Address parsing error during Volume creation: {0}")]
    AddressParseError(#[from] AddrParseError),

    #[error("Could not match read-only resource to {0}")]
    CouldNotFindResource(SocketAddrV6),
}

enum RegionType {
    ReadWrite,
    ReadOnly,
}

#[derive(Debug, thiserror::Error)]
enum RemoveRopError {
    #[error("Error removing read-only parent: {0}")]
    Public(Error),

    #[error("Serde error removing read-only parent: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Updated {0} database rows, expected {1}")]
    UnexpectedDatabaseUpdate(usize, usize),

    #[error("Address parsing error during ROP removal: {0}")]
    AddressParseError(#[from] AddrParseError),

    #[error("Could not match read-only resource to {0}")]
    CouldNotFindResource(String),
}

#[derive(Debug, thiserror::Error)]
enum ReplaceRegionError {
    #[error("Error from Volume region replacement: {0}")]
    Public(Error),

    #[error("Serde error during Volume region replacement: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Region replacement error: {0}")]
    RegionReplacementError(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
enum ReplaceSnapshotError {
    #[error("Error from Volume snapshot replacement: {0}")]
    Public(Error),

    #[error("Serde error during Volume snapshot replacement: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Snapshot replacement error: {0}")]
    SnapshotReplacementError(#[from] anyhow::Error),

    #[error("Replaced {0} targets, expected {1}")]
    UnexpectedReplacedTargets(usize, usize),

    #[error("Updated {0} database rows, expected {1}")]
    UnexpectedDatabaseUpdate(usize, usize),

    #[error(
        "Address parsing error during Volume snapshot \
    replacement: {0}"
    )]
    AddressParseError(#[from] AddrParseError),

    #[error("Could not match read-only resource to {0}")]
    CouldNotFindResource(String),

    #[error("Multiple volume resource usage records for {0}")]
    MultipleResourceUsageRecords(String),
}

/// Crucible resources freed by previous volume deletes
#[derive(Debug, Serialize, Deserialize)]
pub struct FreedCrucibleResources {
    /// Regions that previously could not be deleted (often due to region
    /// snaphots) that were freed by a volume delete
    pub datasets_and_regions: Vec<(CrucibleDataset, Region)>,

    /// Previously soft-deleted volumes that can now be hard-deleted
    pub volumes: Vec<VolumeUuid>,
}

impl FreedCrucibleResources {
    pub fn is_empty(&self) -> bool {
        self.datasets_and_regions.is_empty() && self.volumes.is_empty()
    }
}

pub struct SourceVolume(pub VolumeUuid);
pub struct DestVolume(pub VolumeUuid);

impl DataStore {
    async fn volume_create_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<VolumeCreationError>,
        volume_id: VolumeUuid,
        vcr: VolumeConstructionRequest,
        crucible_targets: CrucibleTargets,
    ) -> Result<Volume, diesel::result::Error> {
        use nexus_db_schema::schema::volume::dsl;

        let maybe_volume: Option<Volume> = dsl::volume
            .filter(dsl::id.eq(to_db_typed_uuid(volume_id)))
            .select(Volume::as_select())
            .first_async(conn)
            .await
            .optional()?;

        // If the volume existed already, return it and do not increase usage
        // counts.
        if let Some(volume) = maybe_volume {
            return Ok(volume);
        }

        let vcr_string = serde_json::to_string(&vcr)
            .map_err(|e| err.bail(VolumeCreationError::SerdeError(e)))?;

        let volume = Volume::new(volume_id, vcr_string);

        let volume: Volume = diesel::insert_into(dsl::volume)
            .values(volume.clone())
            .returning(Volume::as_returning())
            .get_result_async(conn)
            .await
            .map_err(|e| {
                err.bail_retryable_or_else(e, |e| {
                    VolumeCreationError::Public(public_error_from_diesel(
                        e,
                        ErrorHandler::Conflict(
                            ResourceType::Volume,
                            volume.id().to_string().as_str(),
                        ),
                    ))
                })
            })?;

        // Increase the usage count for the read-only Crucible resources
        use nexus_db_schema::schema::volume_resource_usage::dsl as ru_dsl;

        for read_only_target in crucible_targets.read_only_targets {
            let read_only_target = read_only_target.parse().map_err(|e| {
                err.bail(VolumeCreationError::AddressParseError(e))
            })?;

            let maybe_usage = Self::read_only_target_to_volume_resource_usage(
                conn,
                &read_only_target,
            )
            .await?;

            match maybe_usage {
                Some(usage) => {
                    diesel::insert_into(ru_dsl::volume_resource_usage)
                        .values(VolumeResourceUsageRecord::new(
                            volume.id(),
                            usage,
                        ))
                        .execute_async(conn)
                        .await?;
                }

                None => {
                    // Something went wrong, bail out - we can't create this
                    // Volume if we can't record its resource usage correctly
                    return Err(err.bail(
                        VolumeCreationError::CouldNotFindResource(
                            read_only_target,
                        ),
                    ));
                }
            }
        }

        // After volume creation, validate invariants for all volumes
        #[cfg(any(test, feature = "testing"))]
        Self::validate_volume_invariants(&conn).await?;

        Ok(volume)
    }

    /// Return a region by matching against the dataset's address and region's
    /// port
    async fn target_to_region(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        target: &SocketAddrV6,
        region_type: RegionType,
    ) -> Result<Option<Region>, diesel::result::Error> {
        let ip: db::model::Ipv6Addr = target.ip().into();

        use nexus_db_schema::schema::crucible_dataset::dsl as dataset_dsl;
        use nexus_db_schema::schema::region::dsl as region_dsl;

        let read_only = match region_type {
            RegionType::ReadWrite => false,
            RegionType::ReadOnly => true,
        };

        dataset_dsl::crucible_dataset
            .inner_join(
                region_dsl::region
                    .on(region_dsl::dataset_id.eq(dataset_dsl::id)),
            )
            .filter(dataset_dsl::ip.eq(ip))
            .filter(
                region_dsl::port
                    .eq(Some::<db::model::SqlU16>(target.port().into())),
            )
            .filter(region_dsl::read_only.eq(read_only))
            .filter(region_dsl::deleting.eq(false))
            .select(Region::as_select())
            .get_result_async::<Region>(conn)
            .await
            .optional()
    }

    async fn read_only_target_to_volume_resource_usage(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        read_only_target: &SocketAddrV6,
    ) -> Result<Option<VolumeResourceUsage>, diesel::result::Error> {
        // Easy case: it's a region snapshot, and we can match by the snapshot
        // address directly

        let maybe_region_snapshot = {
            use nexus_db_schema::schema::region_snapshot::dsl;
            dsl::region_snapshot
                .filter(dsl::snapshot_addr.eq(read_only_target.to_string()))
                .filter(dsl::deleting.eq(false))
                .select(RegionSnapshot::as_select())
                .get_result_async::<RegionSnapshot>(conn)
                .await
                .optional()?
        };

        if let Some(region_snapshot) = maybe_region_snapshot {
            return Ok(Some(VolumeResourceUsage::RegionSnapshot {
                dataset_id: region_snapshot.dataset_id(),
                region_id: region_snapshot.region_id,
                snapshot_id: region_snapshot.snapshot_id,
            }));
        }

        // Less easy case: it's a read-only region, and we have to match by
        // dataset ip and region port

        let maybe_region = Self::target_to_region(
            conn,
            read_only_target,
            RegionType::ReadOnly,
        )
        .await?;

        if let Some(region) = maybe_region {
            return Ok(Some(VolumeResourceUsage::ReadOnlyRegion {
                region_id: region.id(),
            }));
        }

        // If the resource was hard-deleted, or in the off chance that the
        // region didn't have an assigned port, return None here.
        Ok(None)
    }

    pub async fn volume_create(
        &self,
        volume_id: VolumeUuid,
        vcr: VolumeConstructionRequest,
    ) -> CreateResult<Volume> {
        // Grab all the targets that the volume construction request references.
        // Do this outside the transaction, as the data inside volume doesn't
        // change and this would simply add to the transaction time.
        let crucible_targets = {
            let mut crucible_targets = CrucibleTargets::default();
            read_only_resources_associated_with_volume(
                &vcr,
                &mut crucible_targets,
            );
            crucible_targets
        };

        let err = OptionalError::new();
        let conn = self.pool_connection_unauthorized().await?;
        self.transaction_retry_wrapper("volume_create")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let crucible_targets = crucible_targets.clone();
                let vcr = vcr.clone();

                async move {
                    Self::volume_create_in_txn(
                        &conn,
                        err,
                        volume_id,
                        vcr,
                        crucible_targets,
                    )
                    .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        VolumeCreationError::Public(err) => err,

                        VolumeCreationError::SerdeError(err) => {
                            Error::internal_error(&format!(
                                "SerdeError error: {err}"
                            ))
                        }

                        VolumeCreationError::CouldNotFindResource(s) => {
                            Error::internal_error(&format!(
                                "CouldNotFindResource error: {s}"
                            ))
                        }

                        VolumeCreationError::AddressParseError(err) => {
                            Error::internal_error(&format!(
                                "AddressParseError error: {err}"
                            ))
                        }
                    }
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub(super) async fn volume_get_impl(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        volume_id: VolumeUuid,
    ) -> Result<Option<Volume>, diesel::result::Error> {
        use nexus_db_schema::schema::volume::dsl;
        dsl::volume
            .filter(dsl::id.eq(to_db_typed_uuid(volume_id)))
            .select(Volume::as_select())
            .first_async::<Volume>(conn)
            .await
            .optional()
    }

    /// Return a `Option<Volume>` based on id, even if it's soft deleted.
    pub async fn volume_get(
        &self,
        volume_id: VolumeUuid,
    ) -> LookupResult<Option<Volume>> {
        let conn = self.pool_connection_unauthorized().await?;
        Self::volume_get_impl(&conn, volume_id)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Delete the volume if it exists. If it was already deleted, this is a
    /// no-op.
    pub async fn volume_hard_delete(
        &self,
        volume_id: VolumeUuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::volume::dsl;

        diesel::delete(dsl::volume)
            .filter(dsl::id.eq(to_db_typed_uuid(volume_id)))
            .execute_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map(|_| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    fn volume_usage_records_for_resource_query(
        resource: VolumeResourceUsage,
    ) -> impl RunnableQuery<VolumeResourceUsageRecord> {
        use nexus_db_schema::schema::volume_resource_usage::dsl;

        match resource {
            VolumeResourceUsage::ReadOnlyRegion { region_id } => {
                dsl::volume_resource_usage
                    .filter(
                        dsl::usage_type
                            .eq(VolumeResourceUsageType::ReadOnlyRegion),
                    )
                    .filter(dsl::region_id.eq(region_id))
                    .into_boxed()
            }

            VolumeResourceUsage::RegionSnapshot {
                dataset_id,
                region_id,
                snapshot_id,
            } => dsl::volume_resource_usage
                .filter(
                    dsl::usage_type.eq(VolumeResourceUsageType::RegionSnapshot),
                )
                .filter(
                    dsl::region_snapshot_dataset_id
                        .eq(to_db_typed_uuid(dataset_id)),
                )
                .filter(dsl::region_snapshot_region_id.eq(region_id))
                .filter(dsl::region_snapshot_snapshot_id.eq(snapshot_id))
                .into_boxed(),
        }
    }

    /// For a given VolumeResourceUsage, return all found usage records for it.
    pub async fn volume_usage_records_for_resource(
        &self,
        resource: VolumeResourceUsage,
    ) -> ListResultVec<VolumeResourceUsageRecord> {
        let conn = self.pool_connection_unauthorized().await?;

        Self::volume_usage_records_for_resource_query(resource)
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// When moving a resource from one volume to another, call this to update
    /// the corresponding volume resource usage record
    pub async fn swap_volume_usage_records_for_resources(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        resource: VolumeResourceUsage,
        from_volume_id: VolumeUuid,
        to_volume_id: VolumeUuid,
    ) -> Result<(), diesel::result::Error> {
        use nexus_db_schema::schema::volume_resource_usage::dsl;

        match resource {
            VolumeResourceUsage::ReadOnlyRegion { region_id } => {
                let updated_rows = diesel::update(dsl::volume_resource_usage)
                    .filter(
                        dsl::usage_type
                            .eq(VolumeResourceUsageType::ReadOnlyRegion),
                    )
                    .filter(dsl::region_id.eq(region_id))
                    .filter(dsl::volume_id.eq(to_db_typed_uuid(from_volume_id)))
                    .set(dsl::volume_id.eq(to_db_typed_uuid(to_volume_id)))
                    .execute_async(conn)
                    .await?;

                if updated_rows == 0 {
                    return Err(diesel::result::Error::NotFound);
                }
            }

            VolumeResourceUsage::RegionSnapshot {
                dataset_id,
                region_id,
                snapshot_id,
            } => {
                let updated_rows = diesel::update(dsl::volume_resource_usage)
                    .filter(
                        dsl::usage_type
                            .eq(VolumeResourceUsageType::RegionSnapshot),
                    )
                    .filter(
                        dsl::region_snapshot_dataset_id
                            .eq(to_db_typed_uuid(dataset_id)),
                    )
                    .filter(dsl::region_snapshot_region_id.eq(region_id))
                    .filter(dsl::region_snapshot_snapshot_id.eq(snapshot_id))
                    .filter(dsl::volume_id.eq(to_db_typed_uuid(from_volume_id)))
                    .set(dsl::volume_id.eq(to_db_typed_uuid(to_volume_id)))
                    .execute_async(conn)
                    .await?;

                if updated_rows == 0 {
                    return Err(diesel::result::Error::NotFound);
                }
            }
        }

        Ok(())
    }

    async fn volume_checkout_allowed(
        reason: &VolumeCheckoutReason,
        vcr: &VolumeConstructionRequest,
        maybe_disk: Option<Disk>,
        maybe_instance: Option<Instance>,
    ) -> Result<(), VolumeGetError> {
        match reason {
            VolumeCheckoutReason::ReadOnlyCopy => {
                // When checking out to make a copy (usually for use as a
                // read-only parent), the volume must be read only. Even if a
                // call-site that uses Copy sends this copied Volume to a
                // Propolis or Pantry, the Upstairs that will be created will be
                // read-only, and will not take over from other read-only
                // Upstairs.

                match volume_is_read_only(&vcr) {
                    Ok(read_only) => {
                        if !read_only {
                            return Err(
                                VolumeGetError::CheckoutConditionFailed(
                                    String::from(
                                        "Non-read-only Volume Checkout \
                                for use Copy!",
                                    ),
                                ),
                            );
                        }

                        Ok(())
                    }

                    Err(e) => Err(VolumeGetError::InvalidVolume(e.to_string())),
                }
            }

            VolumeCheckoutReason::CopyAndModify => {
                // `CopyAndModify` is used when taking a read/write Volume,
                // modifying it (for example, when taking a snapshot, to point
                // to read-only resources), and committing it back to the DB.
                // This is a checkout of a read/write Volume, so creating an
                // Upstairs from it *may* take over from something else. The
                // call-site must ensure this doesn't happen, but we can't do
                // that here.

                Ok(())
            }

            VolumeCheckoutReason::InstanceStart { vmm_id } => {
                // Check out this volume to send to Propolis to start an
                // Instance. The VMM id in the enum must match the instance's
                // propolis_id.

                let Some(instance) = &maybe_instance else {
                    return Err(VolumeGetError::CheckoutConditionFailed(
                        format!(
                            "InstanceStart {}: instance does not exist",
                            vmm_id
                        ),
                    ));
                };

                let runtime = instance.runtime();
                match (runtime.propolis_id, runtime.dst_propolis_id) {
                    (Some(_), Some(_)) => {
                        Err(VolumeGetError::CheckoutConditionFailed(format!(
                            "InstanceStart {}: instance {} is undergoing \
                                migration",
                            vmm_id,
                            instance.id(),
                        )))
                    }

                    (None, None) => {
                        Err(VolumeGetError::CheckoutConditionFailed(format!(
                            "InstanceStart {}: instance {} has no \
                                propolis ids",
                            vmm_id,
                            instance.id(),
                        )))
                    }

                    (Some(propolis_id), None) => {
                        if propolis_id != vmm_id.into_untyped_uuid() {
                            return Err(
                                VolumeGetError::CheckoutConditionFailed(
                                    format!(
                                        "InstanceStart {}: instance {} propolis \
                                    id {} mismatch",
                                        vmm_id,
                                        instance.id(),
                                        propolis_id,
                                    ),
                                ),
                            );
                        }

                        Ok(())
                    }

                    (None, Some(dst_propolis_id)) => {
                        Err(VolumeGetError::CheckoutConditionFailed(format!(
                            "InstanceStart {}: instance {} has no \
                                propolis id but dst propolis id {}",
                            vmm_id,
                            instance.id(),
                            dst_propolis_id,
                        )))
                    }
                }
            }

            VolumeCheckoutReason::InstanceMigrate { vmm_id, target_vmm_id } => {
                // Check out this volume to send to destination Propolis to
                // migrate an Instance. Only take over from the specified source
                // VMM.

                let Some(instance) = &maybe_instance else {
                    return Err(VolumeGetError::CheckoutConditionFailed(
                        format!(
                            "InstanceMigrate {} {}: instance does not exist",
                            vmm_id, target_vmm_id
                        ),
                    ));
                };

                let runtime = instance.runtime();
                match (runtime.propolis_id, runtime.dst_propolis_id) {
                    (Some(propolis_id), Some(dst_propolis_id)) => {
                        if propolis_id != vmm_id.into_untyped_uuid()
                            || dst_propolis_id
                                != target_vmm_id.into_untyped_uuid()
                        {
                            return Err(
                                VolumeGetError::CheckoutConditionFailed(
                                    format!(
                                        "InstanceMigrate {} {}: instance {} \
                                    propolis id mismatches {} {}",
                                        vmm_id,
                                        target_vmm_id,
                                        instance.id(),
                                        propolis_id,
                                        dst_propolis_id,
                                    ),
                                ),
                            );
                        }

                        Ok(())
                    }

                    (None, None) => {
                        Err(VolumeGetError::CheckoutConditionFailed(format!(
                            "InstanceMigrate {} {}: instance {} has no \
                                propolis ids",
                            vmm_id,
                            target_vmm_id,
                            instance.id(),
                        )))
                    }

                    (Some(propolis_id), None) => {
                        // XXX is this right?
                        if propolis_id != vmm_id.into_untyped_uuid() {
                            return Err(
                                VolumeGetError::CheckoutConditionFailed(
                                    format!(
                                        "InstanceMigrate {} {}: instance {} \
                                    propolis id {} mismatch",
                                        vmm_id,
                                        target_vmm_id,
                                        instance.id(),
                                        propolis_id,
                                    ),
                                ),
                            );
                        }

                        Ok(())
                    }

                    (None, Some(dst_propolis_id)) => {
                        Err(VolumeGetError::CheckoutConditionFailed(format!(
                            "InstanceMigrate {} {}: instance {} has no \
                                propolis id but dst propolis id {}",
                            vmm_id,
                            target_vmm_id,
                            instance.id(),
                            dst_propolis_id,
                        )))
                    }
                }
            }

            VolumeCheckoutReason::Pantry => {
                // Check out this Volume to send to a Pantry, which will create
                // a read/write Upstairs, for background maintenance operations.
                // There must not be any Propolis, otherwise this will take over
                // from that and cause errors for guest OSes.

                let Some(disk) = maybe_disk else {
                    // This volume isn't backing a disk, it won't take over from
                    // a Propolis' Upstairs.
                    return Ok(());
                };

                let Some(attach_instance_id) =
                    disk.runtime().attach_instance_id
                else {
                    // The volume is backing a disk that is not attached to an
                    // instance. At this moment it won't take over from a
                    // Propolis' Upstairs, so send it to a Pantry to create an
                    // Upstairs there.  A future checkout that happens after
                    // this transaction that is sent to a Propolis _will_ take
                    // over from this checkout (sent to a Pantry), which is ok.
                    return Ok(());
                };

                let Some(instance) = maybe_instance else {
                    // The instance, which the disk that this volume backs is
                    // attached to, doesn't exist?
                    //
                    // XXX this is a Nexus bug!
                    return Err(VolumeGetError::CheckoutConditionFailed(
                        format!(
                            "Pantry: instance {} backing disk {} does not \
                            exist?",
                            attach_instance_id,
                            disk.id(),
                        ),
                    ));
                };

                if let Some(propolis_id) = instance.runtime().propolis_id {
                    // The instance, which the disk that this volume backs is
                    // attached to, exists and has an active propolis ID.  A
                    // propolis _may_ exist, so bail here - an activation from
                    // the Pantry is not allowed to take over from a Propolis.
                    Err(VolumeGetError::CheckoutConditionFailed(format!(
                        "Pantry: possible Propolis {}",
                        propolis_id
                    )))
                } else {
                    // The instance, which the disk that this volume backs is
                    // attached to, exists, but there is no active propolis ID.
                    // This is ok.
                    Ok(())
                }
            }
        }
    }

    async fn volume_checkout_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<VolumeGetError>,
        volume_id: VolumeUuid,
        reason: VolumeCheckoutReason,
    ) -> Result<Volume, diesel::result::Error> {
        use nexus_db_schema::schema::volume::dsl;

        // Grab the volume in question.
        let volume = dsl::volume
            .filter(dsl::id.eq(to_db_typed_uuid(volume_id)))
            .select(Volume::as_select())
            .get_result_async(conn)
            .await?;

        // Turn the volume.data into the VolumeConstructionRequest
        let vcr: VolumeConstructionRequest =
            serde_json::from_str(volume.data())
                .map_err(|e| err.bail(VolumeGetError::SerdeError(e)))?;

        // The VolumeConstructionRequest resulting from this checkout will have
        // its generation numbers bumped, and as result will (if it has
        // non-read-only sub-volumes) take over from previous read/write
        // activations when sent to a place that will `construct` a new Volume.
        // Depending on the checkout reason, prevent creating multiple
        // read/write Upstairs acting on the same Volume, except where the take
        // over is intended.

        let (maybe_disk, maybe_instance) = {
            use nexus_db_schema::schema::disk::dsl as disk_dsl;
            use nexus_db_schema::schema::instance::dsl as instance_dsl;

            let maybe_disk: Option<Disk> = disk_dsl::disk
                .filter(disk_dsl::time_deleted.is_null())
                .filter(disk_dsl::volume_id.eq(to_db_typed_uuid(volume_id)))
                .select(Disk::as_select())
                .get_result_async(conn)
                .await
                .optional()?;

            let maybe_instance: Option<Instance> =
                if let Some(disk) = &maybe_disk {
                    if let Some(attach_instance_id) =
                        disk.runtime().attach_instance_id
                    {
                        instance_dsl::instance
                            .filter(instance_dsl::time_deleted.is_null())
                            .filter(instance_dsl::id.eq(attach_instance_id))
                            .select(Instance::as_select())
                            .get_result_async(conn)
                            .await
                            .optional()?
                    } else {
                        // Disk not attached to an instance
                        None
                    }
                } else {
                    // Volume not associated with disk
                    None
                };

            (maybe_disk, maybe_instance)
        };

        if let Err(e) = Self::volume_checkout_allowed(
            &reason,
            &vcr,
            maybe_disk,
            maybe_instance,
        )
        .await
        {
            return Err(err.bail(e));
        }

        // Look to see if the VCR is a Volume type, and if so, look at its
        // sub_volumes. If they are of type Region, then we need to update their
        // generation numbers and record that update back to the database. We
        // return to the caller whatever the original volume data was we pulled
        // from the database.
        match vcr {
            VolumeConstructionRequest::Volume {
                id,
                block_size,
                sub_volumes,
                read_only_parent,
            } => {
                let mut update_needed = false;
                let mut new_sv = Vec::new();
                for sv in sub_volumes {
                    match sv {
                        VolumeConstructionRequest::Region {
                            block_size,
                            blocks_per_extent,
                            extent_count,
                            opts,
                            r#gen,
                        } => {
                            update_needed = true;
                            new_sv.push(VolumeConstructionRequest::Region {
                                block_size,
                                blocks_per_extent,
                                extent_count,
                                opts,
                                r#gen: r#gen + 1,
                            });
                        }
                        _ => {
                            new_sv.push(sv);
                        }
                    }
                }

                // Only update the volume data if we found the type of volume
                // that needed it.
                if update_needed {
                    // Create a new VCR and fill in the contents from what the
                    // original volume had, but with our updated sub_volume
                    // records.
                    let new_vcr = VolumeConstructionRequest::Volume {
                        id,
                        block_size,
                        sub_volumes: new_sv,
                        read_only_parent,
                    };

                    let new_volume_data = serde_json::to_string(&new_vcr)
                        .map_err(|e| err.bail(VolumeGetError::SerdeError(e)))?;

                    // Update the original volume_id with the new volume.data.
                    use nexus_db_schema::schema::volume::dsl as volume_dsl;
                    let num_updated = diesel::update(volume_dsl::volume)
                        .filter(volume_dsl::id.eq(to_db_typed_uuid(volume_id)))
                        .set(volume_dsl::data.eq(new_volume_data))
                        .execute_async(conn)
                        .await?;

                    // This should update just one row.  If it does not, then
                    // something is terribly wrong in the database.
                    if num_updated != 1 {
                        return Err(err.bail(
                            VolumeGetError::UnexpectedDatabaseUpdate(
                                num_updated,
                                1,
                            ),
                        ));
                    }
                }
            }
            VolumeConstructionRequest::Region {
                block_size: _,
                blocks_per_extent: _,
                extent_count: _,
                opts: _,
                r#gen: _,
            } => {
                // We don't support a pure Region VCR at the volume level in the
                // database, so this choice should never be encountered, but I
                // want to know if it is.
                return Err(err.bail(VolumeGetError::InvalidVolume(
                    String::from("Region not supported as a top level volume"),
                )));
            }
            VolumeConstructionRequest::File {
                id: _,
                block_size: _,
                path: _,
            }
            | VolumeConstructionRequest::Url { id: _, block_size: _, url: _ } =>
                {}
        }

        Ok(volume)
    }

    /// Checkout a copy of the Volume from the database.
    /// This action (getting a copy) will increase the generation number
    /// of Volumes of the VolumeConstructionRequest::Volume type that have
    /// sub_volumes of the VolumeConstructionRequest::Region type.
    /// This generation number increase is required for Crucible to support
    /// crash consistency.
    pub async fn volume_checkout(
        &self,
        volume_id: VolumeUuid,
        reason: VolumeCheckoutReason,
    ) -> LookupResult<Volume> {
        // We perform a transaction here, to be sure that on completion
        // of this, the database contains an updated version of the
        // volume with the generation number incremented (for the volume
        // types that require it).  The generation number (along with the
        // rest of the volume data) that was in the database is what is
        // returned to the caller.
        let err = OptionalError::new();
        let conn = self.pool_connection_unauthorized().await?;

        self.transaction_retry_wrapper("volume_checkout")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    Self::volume_checkout_in_txn(&conn, err, volume_id, reason)
                        .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        VolumeGetError::CheckoutConditionFailed(message) => {
                            return Error::conflict(message);
                        }

                        _ => {
                            return Error::internal_error(&format!(
                                "Transaction error: {}",
                                err
                            ));
                        }
                    }
                }

                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    /// Create new UUIDs for the volume construction request layers
    pub fn randomize_ids(
        vcr: &VolumeConstructionRequest,
    ) -> anyhow::Result<VolumeConstructionRequest> {
        let mut new_vcr = vcr.clone();

        let mut parts: VecDeque<&mut VolumeConstructionRequest> =
            VecDeque::new();
        parts.push_back(&mut new_vcr);

        while let Some(vcr_part) = parts.pop_front() {
            match vcr_part {
                VolumeConstructionRequest::Volume {
                    id,
                    sub_volumes,
                    read_only_parent,
                    ..
                } => {
                    *id = Uuid::new_v4();

                    for sub_volume in sub_volumes {
                        parts.push_back(sub_volume);
                    }

                    if let Some(read_only_parent) = read_only_parent {
                        parts.push_back(read_only_parent);
                    }
                }

                VolumeConstructionRequest::Url { id, .. } => {
                    *id = Uuid::new_v4();
                }

                VolumeConstructionRequest::Region { opts, .. } => {
                    if !opts.read_only {
                        // Only one volume can "own" a Region, and that volume's
                        // UUID is recorded in the region table accordingly. It
                        // is an error to make a copy of a volume construction
                        // request that references non-read-only Regions.
                        bail!(
                            "only one Volume can reference a Region \
                            non-read-only!"
                        );
                    }

                    opts.id = Uuid::new_v4();
                }

                VolumeConstructionRequest::File { id, .. } => {
                    *id = Uuid::new_v4();
                }
            }
        }

        Ok(new_vcr)
    }

    /// Checkout a copy of the Volume from the database using `volume_checkout`,
    /// then randomize the UUIDs in the construction request. Because this is a
    /// new volume, it is immediately passed to `volume_create` so that the
    /// accounting for Crucible resources stays correct. This is only valid for
    /// Volumes that reference regions read-only - it's important for accounting
    /// purposes that each region in this volume construction request is
    /// returned by `read_only_resources_associated_with_volume`.
    pub async fn volume_checkout_randomize_ids(
        &self,
        source_volume_id: SourceVolume,
        dest_volume_id: DestVolume,
        reason: VolumeCheckoutReason,
    ) -> CreateResult<Volume> {
        let volume = self.volume_checkout(source_volume_id.0, reason).await?;

        let vcr: sled_agent_client::VolumeConstructionRequest =
            serde_json::from_str(volume.data())?;

        let randomized_vcr = Self::randomize_ids(&vcr)
            .map_err(|e| Error::internal_error(&e.to_string()))?;

        self.volume_create(dest_volume_id.0, randomized_vcr).await
    }

    /// Find read/write regions for deleted volumes that do not have associated
    /// region snapshots and are not being used by any other non-deleted
    /// volumes, and return them for garbage collection
    pub async fn find_deleted_volume_regions(
        &self,
    ) -> LookupResult<FreedCrucibleResources> {
        let conn = self.pool_connection_unauthorized().await?;
        self.transaction_retry_wrapper("find_deleted_volume_regions")
            .transaction(&conn, |conn| async move {
                Self::find_deleted_volume_regions_in_txn(&conn).await
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    async fn find_deleted_volume_regions_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<FreedCrucibleResources, diesel::result::Error> {
        use nexus_db_schema::schema::crucible_dataset::dsl as dataset_dsl;
        use nexus_db_schema::schema::region::dsl as region_dsl;
        use nexus_db_schema::schema::region_snapshot::dsl;
        use nexus_db_schema::schema::volume::dsl as volume_dsl;

        // Find all read-write regions (read-only region cleanup is taken care
        // of in soft_delete_volume_in_txn!) and their associated datasets
        let unfiltered_deleted_regions = region_dsl::region
            .filter(region_dsl::read_only.eq(false))
            // the volume may be hard deleted, so use a left join here
            .left_join(
                volume_dsl::volume.on(region_dsl::volume_id.eq(volume_dsl::id)),
            )
            .inner_join(
                dataset_dsl::crucible_dataset
                    .on(region_dsl::dataset_id.eq(dataset_dsl::id)),
            )
            // where there either are no region snapshots, or the region
            // snapshot volume has deleted = true
            .left_join(
                dsl::region_snapshot.on(dsl::region_id
                    .eq(region_dsl::id)
                    .and(dsl::dataset_id.eq(dataset_dsl::id))),
            )
            .filter(dsl::deleting.eq(true).or(dsl::deleting.is_null()))
            // and return them (along with the volume so it can be hard deleted)
            .select((
                CrucibleDataset::as_select(),
                Region::as_select(),
                Option::<RegionSnapshot>::as_select(),
                // Diesel can't express a difference between
                //
                // a) the volume record existing and the nullable
                //    volume.time_deleted column being set to null
                // b) the volume record does not exist (null due to left join)
                //
                // so return an Option and check below
                Option::<Volume>::as_select(),
            ))
            .load_async(conn)
            .await?;

        let mut deleted_regions =
            Vec::with_capacity(unfiltered_deleted_regions.len());

        let mut volume_set: HashSet<VolumeUuid> =
            HashSet::with_capacity(unfiltered_deleted_regions.len());

        for (dataset, region, region_snapshot, volume) in
            unfiltered_deleted_regions
        {
            // only operate on soft deleted volumes
            let soft_deleted = match &volume {
                Some(volume) => volume.time_deleted.is_some(),
                None => false,
            };

            if !soft_deleted {
                continue;
            }

            if region_snapshot.is_some() {
                // We cannot delete this region: the presence of the region
                // snapshot record means that the Crucible agent's snapshot has
                // not been deleted yet (as the lifetime of the region snapshot
                // record is equal to or longer than the lifetime of the
                // Crucible agent's snapshot).
                //
                // This condition can occur when multiple volume delete sagas
                // run concurrently: one will decrement the crucible resources
                // (but hasn't made the appropriate DELETE calls to the
                // appropriate Agents to tombstone the running snapshots and
                // snapshots yet), and the other will be in the "delete freed
                // regions" saga node trying to delete the region. Without this
                // check, This race results in the Crucible Agent returning
                // "must delete snapshots first" and causing saga unwinds.
                //
                // Another volume delete will pick up this region and remove it.
                continue;
            }

            if let Some(volume) = &volume {
                volume_set.insert(volume.id());
            }

            deleted_regions.push((dataset, region, volume));
        }

        let regions_for_deletion: HashSet<Uuid> =
            deleted_regions.iter().map(|(_, region, _)| region.id()).collect();

        let mut volumes = Vec::with_capacity(deleted_regions.len());

        for volume_id in volume_set {
            // Do not return a volume hard-deletion if there are still lingering
            // read/write regions, unless all those lingering read/write regions
            // will be deleted from the result of returning from this function.
            let allocated_rw_regions: HashSet<Uuid> =
                Self::get_allocated_regions_query(volume_id)
                    .get_results_async::<(CrucibleDataset, Region)>(conn)
                    .await?
                    .into_iter()
                    .filter_map(|(_, region)| {
                        if !region.read_only() {
                            Some(region.id())
                        } else {
                            None
                        }
                    })
                    .collect();

            if allocated_rw_regions.is_subset(&regions_for_deletion) {
                // If all the allocated rw regions for this volume are in the
                // set of regions being returned for deletion, then we can
                // hard-delete this volume. Read-only region accounting should
                // have already been updated by soft-deleting this volume.
                //
                // Note: we'll be in this branch if allocated_rw_regions is
                // empty. I believe the only time we'll hit this empty case is
                // when the volume is fully populated with read-only resources
                // (read-only regions and region snapshots).
                volumes.push(volume_id);
            } else {
                // Not all r/w regions allocated to this volume are being
                // deleted here, so we can't hard-delete the volume yet.
            }
        }

        Ok(FreedCrucibleResources {
            datasets_and_regions: deleted_regions
                .into_iter()
                .map(|(d, r, _)| (d, r))
                .collect(),

            volumes,
        })
    }

    pub async fn read_only_resources_associated_with_volume(
        &self,
        volume_id: VolumeUuid,
    ) -> LookupResult<CrucibleTargets> {
        let volume = if let Some(volume) = self.volume_get(volume_id).await? {
            volume
        } else {
            // Volume has already been hard deleted (volume_get returns
            // soft deleted records), return that no cleanup is necessary.
            return Ok(CrucibleTargets::default());
        };

        let vcr: VolumeConstructionRequest =
            serde_json::from_str(&volume.data())?;

        let mut crucible_targets = CrucibleTargets::default();

        read_only_resources_associated_with_volume(&vcr, &mut crucible_targets);

        Ok(crucible_targets)
    }
}

#[derive(Debug, thiserror::Error)]
enum SoftDeleteError {
    #[error("Serde error decreasing Crucible resources: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Updated {0} database rows in {1}, expected 1")]
    UnexpectedDatabaseUpdate(usize, String),

    // XXX is this an error? delete volume anyway, else we're stuck?
    #[error("Could not match resource to {0}")]
    CouldNotFindResource(String),

    #[error("Address parsing error during Volume soft-delete: {0}")]
    AddressParseError(#[from] AddrParseError),

    #[error("Invalid Volume: {0}")]
    InvalidVolume(String),
}

impl DataStore {
    // See comment for `soft_delete_volume`
    async fn soft_delete_volume_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        volume_id: VolumeUuid,
        err: OptionalError<SoftDeleteError>,
    ) -> Result<CrucibleResources, diesel::result::Error> {
        // Grab the volume, and check if the volume was already soft-deleted.
        // We have to guard against the case where this function is called
        // multiple times, and that is done by soft-deleting the volume during
        // the transaction, and returning the previously serialized list of
        // resources to clean up.
        let volume = {
            use nexus_db_schema::schema::volume::dsl;

            let volume = dsl::volume
                .filter(dsl::id.eq(to_db_typed_uuid(volume_id)))
                .select(Volume::as_select())
                .get_result_async(conn)
                .await
                .optional()?;

            let volume = if let Some(v) = volume {
                v
            } else {
                // The volume was hard-deleted
                return Ok(CrucibleResources::V1(
                    CrucibleResourcesV1::default(),
                ));
            };

            if volume.time_deleted.is_some() {
                // this volume was already soft-deleted, return the existing
                // serialized CrucibleResources
                let existing_resources = match volume
                    .resources_to_clean_up
                    .as_ref()
                {
                    Some(v) => serde_json::from_str(v)
                        .map_err(|e| err.bail(e.into()))?,

                    None => {
                        // Even volumes with nothing to clean up should have a
                        // serialized CrucibleResources that contains empty
                        // vectors instead of None. Instead of panicing here
                        // though, just return the default (nothing to clean
                        // up).
                        CrucibleResources::V1(CrucibleResourcesV1::default())
                    }
                };

                return Ok(existing_resources);
            }

            volume
        };

        let vcr: VolumeConstructionRequest =
            serde_json::from_str(&volume.data())
                .map_err(|e| err.bail(e.into()))?;

        // Grab all the targets that the volume construction request references.
        // Do this _inside_ the transaction, as the data inside volumes can
        // change as a result of region / region snapshot replacement.
        let crucible_targets = {
            let mut crucible_targets = CrucibleTargets::default();
            read_only_resources_associated_with_volume(
                &vcr,
                &mut crucible_targets,
            );
            crucible_targets
        };

        // Decrease the number of references for each resource that a volume
        // references, collecting the regions and region snapshots that were
        // freed up for deletion.

        let num_read_write_subvolumes =
            match count_read_write_sub_volumes(&vcr) {
                Ok(v) => v,
                Err(e) => {
                    return Err(err.bail(SoftDeleteError::InvalidVolume(
                        format!("volume {} invalid: {e}", volume.id()),
                    )));
                }
            };

        let mut regions: Vec<Uuid> = Vec::with_capacity(
            REGION_REDUNDANCY_THRESHOLD * num_read_write_subvolumes,
        );

        let mut region_snapshots: Vec<RegionSnapshotV3> =
            Vec::with_capacity(crucible_targets.read_only_targets.len());

        // First, grab read-write regions - they're not shared, but they are
        // not candidates for deletion if there are region snapshots
        let mut read_write_targets = Vec::with_capacity(
            REGION_REDUNDANCY_THRESHOLD * num_read_write_subvolumes,
        );

        read_write_resources_associated_with_volume(
            &vcr,
            &mut read_write_targets,
        );

        for target in read_write_targets {
            let target = target
                .parse()
                .map_err(|e| err.bail(SoftDeleteError::AddressParseError(e)))?;

            let maybe_region =
                Self::target_to_region(conn, &target, RegionType::ReadWrite)
                    .await?;

            let Some(region) = maybe_region else {
                return Err(err.bail(SoftDeleteError::CouldNotFindResource(
                    format!("could not find resource for {target}"),
                )));
            };

            // Filter out regions that have any region-snapshots
            let region_snapshot_count: i64 = {
                use nexus_db_schema::schema::region_snapshot::dsl;
                dsl::region_snapshot
                    .filter(dsl::region_id.eq(region.id()))
                    .count()
                    .get_result_async::<i64>(conn)
                    .await?
            };

            if region_snapshot_count == 0 {
                regions.push(region.id());
            }
        }

        for read_only_target in &crucible_targets.read_only_targets {
            use nexus_db_schema::schema::volume_resource_usage::dsl as ru_dsl;

            let read_only_target = read_only_target
                .parse()
                .map_err(|e| err.bail(SoftDeleteError::AddressParseError(e)))?;

            let maybe_usage = Self::read_only_target_to_volume_resource_usage(
                conn,
                &read_only_target,
            )
            .await?;

            let Some(usage) = maybe_usage else {
                return Err(err.bail(SoftDeleteError::CouldNotFindResource(
                    format!("could not find resource for {read_only_target}"),
                )));
            };

            // For each read-only resource, remove the associated volume
            // resource usage record for this volume. Only return a resource for
            // deletion if no more associated volume usage records are found.
            match usage {
                VolumeResourceUsage::ReadOnlyRegion { region_id } => {
                    let updated_rows =
                        diesel::delete(ru_dsl::volume_resource_usage)
                            .filter(
                                ru_dsl::volume_id
                                    .eq(to_db_typed_uuid(volume_id)),
                            )
                            .filter(
                                ru_dsl::usage_type.eq(
                                    VolumeResourceUsageType::ReadOnlyRegion,
                                ),
                            )
                            .filter(ru_dsl::region_id.eq(Some(region_id)))
                            .execute_async(conn)
                            .await?;

                    if updated_rows != 1 {
                        return Err(err.bail(
                            SoftDeleteError::UnexpectedDatabaseUpdate(
                                updated_rows,
                                "volume_resource_usage (region)".into(),
                            ),
                        ));
                    }

                    let region_usage_left = ru_dsl::volume_resource_usage
                        .filter(
                            ru_dsl::usage_type
                                .eq(VolumeResourceUsageType::ReadOnlyRegion),
                        )
                        .filter(ru_dsl::region_id.eq(region_id))
                        .count()
                        .get_result_async::<i64>(conn)
                        .await?;

                    if region_usage_left == 0 {
                        // There are several factors that mean Nexus does _not_
                        // have to check here for region snapshots taken of
                        // read-only regions:
                        //
                        // - When a Crucible Volume receives a flush, it is only
                        //   propagated to the subvolumes of that Volume, not
                        //   the read-only parent. There's a few reasons for
                        //   this, but the main one is that a Crucible flush
                        //   changes the on-disk data, and the directory that
                        //   the downstairs of a read-only parent is serving out
                        //   of may be on read-only storage, as is the case when
                        //   serving out of a .zfs/snapshot/ directory.
                        //
                        // - Even if a Crucible flush _did_ propagate to the
                        //   read-only parent, Nexus should never directly send
                        //   the snapshot volume to a place where it will be
                        //   constructed, meaning the read-only regions of the
                        //   snapshot volume's subvolumes will never themselves
                        //   receive a flush.
                        //
                        // If either of these factors change, then that check is
                        // required here. The `validate_volume_invariants`
                        // function will return an error if a read-only region
                        // has an associated region snapshot during testing.
                        //
                        // However, don't forget to set `deleting`! These
                        // regions will be returned to the calling function for
                        // garbage collection.
                        use nexus_db_schema::schema::region::dsl;
                        let updated_rows = diesel::update(dsl::region)
                            .filter(dsl::id.eq(region_id))
                            .filter(dsl::read_only.eq(true))
                            .filter(dsl::deleting.eq(false))
                            .set(dsl::deleting.eq(true))
                            .execute_async(conn)
                            .await?;

                        if updated_rows != 1 {
                            return Err(err.bail(
                                SoftDeleteError::UnexpectedDatabaseUpdate(
                                    updated_rows,
                                    "setting deleting (region)".into(),
                                ),
                            ));
                        }

                        regions.push(region_id);
                    }
                }

                VolumeResourceUsage::RegionSnapshot {
                    dataset_id,
                    region_id,
                    snapshot_id,
                } => {
                    let updated_rows =
                        diesel::delete(ru_dsl::volume_resource_usage)
                            .filter(
                                ru_dsl::volume_id
                                    .eq(to_db_typed_uuid(volume_id)),
                            )
                            .filter(
                                ru_dsl::usage_type.eq(
                                    VolumeResourceUsageType::RegionSnapshot,
                                ),
                            )
                            .filter(
                                ru_dsl::region_snapshot_dataset_id
                                    .eq(Some(to_db_typed_uuid(dataset_id))),
                            )
                            .filter(
                                ru_dsl::region_snapshot_region_id
                                    .eq(Some(region_id)),
                            )
                            .filter(
                                ru_dsl::region_snapshot_snapshot_id
                                    .eq(Some(snapshot_id)),
                            )
                            .execute_async(conn)
                            .await?;

                    if updated_rows != 1 {
                        return Err(err.bail(
                            SoftDeleteError::UnexpectedDatabaseUpdate(
                                updated_rows,
                                "volume_resource_usage \
                                (region_snapshot)"
                                    .into(),
                            ),
                        ));
                    }

                    let region_snapshot_usage_left =
                        ru_dsl::volume_resource_usage
                            .filter(
                                ru_dsl::usage_type.eq(
                                    VolumeResourceUsageType::RegionSnapshot,
                                ),
                            )
                            .filter(
                                ru_dsl::region_snapshot_dataset_id
                                    .eq(Some(to_db_typed_uuid(dataset_id))),
                            )
                            .filter(
                                ru_dsl::region_snapshot_region_id
                                    .eq(Some(region_id)),
                            )
                            .filter(
                                ru_dsl::region_snapshot_snapshot_id
                                    .eq(Some(snapshot_id)),
                            )
                            .count()
                            .get_result_async::<i64>(conn)
                            .await?;

                    if region_snapshot_usage_left == 0 {
                        // Don't forget to set `deleting`! see: omicron#4095
                        use nexus_db_schema::schema::region_snapshot::dsl;
                        let updated_rows = diesel::update(dsl::region_snapshot)
                            .filter(
                                dsl::dataset_id
                                    .eq(to_db_typed_uuid(dataset_id)),
                            )
                            .filter(dsl::region_id.eq(region_id))
                            .filter(dsl::snapshot_id.eq(snapshot_id))
                            .filter(
                                dsl::snapshot_addr
                                    .eq(read_only_target.to_string()),
                            )
                            .filter(dsl::deleting.eq(false))
                            .set(dsl::deleting.eq(true))
                            .execute_async(conn)
                            .await?;

                        if updated_rows != 1 {
                            return Err(err.bail(
                                SoftDeleteError::UnexpectedDatabaseUpdate(
                                    updated_rows,
                                    "setting deleting (region snapshot)".into(),
                                ),
                            ));
                        }

                        region_snapshots.push(RegionSnapshotV3 {
                            dataset: dataset_id,
                            region: region_id,
                            snapshot: snapshot_id,
                        });
                    }
                }
            }
        }

        let resources_to_delete = CrucibleResources::V3(CrucibleResourcesV3 {
            regions,
            region_snapshots,
        });

        // Soft-delete the volume, and serialize the resources to delete.
        let serialized_resources = serde_json::to_string(&resources_to_delete)
            .map_err(|e| err.bail(e.into()))?;

        {
            use nexus_db_schema::schema::volume::dsl;
            let updated_rows = diesel::update(dsl::volume)
                .filter(dsl::id.eq(to_db_typed_uuid(volume_id)))
                .set((
                    dsl::time_deleted.eq(Utc::now()),
                    dsl::resources_to_clean_up.eq(Some(serialized_resources)),
                ))
                .execute_async(conn)
                .await?;

            if updated_rows != 1 {
                return Err(err.bail(
                    SoftDeleteError::UnexpectedDatabaseUpdate(
                        updated_rows,
                        "volume".into(),
                    ),
                ));
            }
        }

        // After volume deletion, validate invariants for all volumes
        #[cfg(any(test, feature = "testing"))]
        Self::validate_volume_invariants(&conn).await?;

        Ok(resources_to_delete)
    }

    /// Decrease the usage count for Crucible resources according to the
    /// contents of the volume, soft-delete the volume, and return a list of
    /// Crucible resources to clean up. Note this function must be idempotent,
    /// it is called from a saga node.
    pub async fn soft_delete_volume(
        &self,
        volume_id: VolumeUuid,
    ) -> Result<CrucibleResources, Error> {
        let err = OptionalError::new();
        let conn = self.pool_connection_unauthorized().await?;
        self.transaction_retry_wrapper("soft_delete_volume")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    Self::soft_delete_volume_in_txn(&conn, volume_id, err).await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    Error::internal_error(&format!("{err}"))
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    async fn volume_remove_rop_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<RemoveRopError>,
        volume_id: VolumeUuid,
        temp_volume_id: VolumeUuid,
    ) -> Result<bool, diesel::result::Error> {
        // Grab the volume in question. If the volume record was already deleted
        // then we can just return.
        let volume = {
            use nexus_db_schema::schema::volume::dsl;

            let volume = dsl::volume
                .filter(dsl::id.eq(to_db_typed_uuid(volume_id)))
                .select(Volume::as_select())
                .get_result_async(conn)
                .await
                .optional()?;

            let volume = if let Some(v) = volume {
                v
            } else {
                // the volume does not exist, nothing to do.
                return Ok(false);
            };

            if volume.time_deleted.is_some() {
                // this volume is deleted, so let whatever is deleting it clean
                // it up.
                return Ok(false);
            } else {
                // A volume record exists, and was not deleted, we can attempt
                // to remove its read_only_parent.
                volume
            }
        };

        // If a read_only_parent exists, remove it from volume_id, and attach it
        // to temp_volume_id.
        let vcr: VolumeConstructionRequest =
            serde_json::from_str(volume.data())
                .map_err(|e| err.bail(RemoveRopError::SerdeError(e)))?;

        match vcr {
            VolumeConstructionRequest::Volume {
                id,
                block_size,
                sub_volumes,
                read_only_parent,
            } => {
                if read_only_parent.is_none() {
                    // This volume has no read_only_parent
                    Ok(false)
                } else {
                    // Create a new VCR and fill in the contents from what the
                    // original volume had.
                    let new_vcr = VolumeConstructionRequest::Volume {
                        id,
                        block_size,
                        sub_volumes,
                        read_only_parent: None,
                    };

                    let new_volume_data = serde_json::to_string(&new_vcr)
                        .map_err(|e| err.bail(RemoveRopError::SerdeError(e)))?;

                    // Update the original volume_id with the new volume.data.
                    use nexus_db_schema::schema::volume::dsl as volume_dsl;
                    let num_updated = diesel::update(volume_dsl::volume)
                        .filter(volume_dsl::id.eq(to_db_typed_uuid(volume_id)))
                        .set(volume_dsl::data.eq(new_volume_data))
                        .execute_async(conn)
                        .await?;

                    // This should update just one row.  If it does not, then
                    // something is terribly wrong in the database.
                    if num_updated != 1 {
                        return Err(err.bail(
                            RemoveRopError::UnexpectedDatabaseUpdate(
                                num_updated,
                                1,
                            ),
                        ));
                    }

                    // Make a new VCR, with the information from our
                    // temp_volume_id, but the read_only_parent from the
                    // original volume.
                    let rop_vcr = VolumeConstructionRequest::Volume {
                        id: *temp_volume_id.as_untyped_uuid(),
                        block_size,
                        sub_volumes: vec![],
                        read_only_parent,
                    };

                    let rop_volume_data = serde_json::to_string(&rop_vcr)
                        .map_err(|e| err.bail(RemoveRopError::SerdeError(e)))?;

                    // Update the temp_volume_id with the volume data that
                    // contains the read_only_parent.
                    let num_updated = diesel::update(volume_dsl::volume)
                        .filter(
                            volume_dsl::id.eq(to_db_typed_uuid(temp_volume_id)),
                        )
                        .filter(volume_dsl::time_deleted.is_null())
                        .set(volume_dsl::data.eq(rop_volume_data))
                        .execute_async(conn)
                        .await?;

                    if num_updated != 1 {
                        return Err(err.bail(
                            RemoveRopError::UnexpectedDatabaseUpdate(
                                num_updated,
                                1,
                            ),
                        ));
                    }

                    // Update the volume resource usage record for every
                    // read-only resource in the ROP
                    let crucible_targets = {
                        let mut crucible_targets = CrucibleTargets::default();
                        read_only_resources_associated_with_volume(
                            &rop_vcr,
                            &mut crucible_targets,
                        );
                        crucible_targets
                    };

                    for read_only_target in crucible_targets.read_only_targets {
                        let read_only_target =
                            read_only_target.parse().map_err(|e| {
                                err.bail(RemoveRopError::AddressParseError(e))
                            })?;

                        let maybe_usage =
                            Self::read_only_target_to_volume_resource_usage(
                                conn,
                                &read_only_target,
                            )
                            .await?;

                        let Some(usage) = maybe_usage else {
                            return Err(err.bail(
                                RemoveRopError::CouldNotFindResource(format!(
                                    "could not find resource for \
                                    {read_only_target}"
                                )),
                            ));
                        };

                        Self::swap_volume_usage_records_for_resources(
                            conn,
                            usage,
                            volume_id,
                            temp_volume_id,
                        )
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                RemoveRopError::Public(
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::Server,
                                    ),
                                )
                            })
                        })?;
                    }

                    // After read-only parent removal, validate invariants for
                    // all volumes
                    #[cfg(any(test, feature = "testing"))]
                    Self::validate_volume_invariants(conn).await?;

                    Ok(true)
                }
            }

            VolumeConstructionRequest::File { .. }
            | VolumeConstructionRequest::Region { .. }
            | VolumeConstructionRequest::Url { .. } => {
                // Volume has a format that does not contain ROPs
                Ok(false)
            }
        }
    }

    // Here we remove the read only parent from volume_id, and attach it
    // to temp_volume_id.
    //
    // As this is part of a saga, it will be able to handle being replayed
    // If we call this twice, any work done the first time through should
    // not happen again, or be undone.
    pub async fn volume_remove_rop(
        &self,
        volume_id: VolumeUuid,
        temp_volume_id: VolumeUuid,
    ) -> Result<bool, Error> {
        // In this single transaction:
        // - Get the given volume from the volume_id from the database
        // - Extract the volume.data into a VolumeConstructionRequest (VCR)
        // - Create a new VCR, copying over anything from the original VCR,
        //   but, replacing the read_only_parent with None.
        // - Put the new VCR into volume.data, then update the volume in the
        //   database.
        // - Get the given volume from temp_volume_id from the database
        // - Extract the temp volume.data into a VCR
        // - Create a new VCR, copying over anything from the original VCR,
        //   but, replacing the read_only_parent with the read_only_parent
        //   data from original volume_id.
        // - Put the new temp VCR into the temp volume.data, update the
        //   temp_volume in the database.
        let err = OptionalError::new();
        let conn = self.pool_connection_unauthorized().await?;
        self.transaction_retry_wrapper("volume_remove_rop")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    Self::volume_remove_rop_in_txn(
                        &conn,
                        err,
                        volume_id,
                        temp_volume_id,
                    )
                    .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return Error::internal_error(&format!(
                        "Transaction error: {}",
                        err
                    ));
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    /// Return all the read-write regions in a volume whose target address
    /// matches the argument dataset's.
    pub async fn get_dataset_rw_regions_in_volume(
        &self,
        opctx: &OpContext,
        dataset_id: DatasetUuid,
        volume_id: VolumeUuid,
    ) -> LookupResult<Vec<SocketAddrV6>> {
        let conn = self.pool_connection_authorized(opctx).await?;

        let dataset = {
            use nexus_db_schema::schema::crucible_dataset::dsl;

            dsl::crucible_dataset
                .filter(dsl::id.eq(to_db_typed_uuid(dataset_id)))
                .select(CrucibleDataset::as_select())
                .first_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?
        };

        let Some(volume) = self.volume_get(volume_id).await? else {
            return Err(Error::internal_error("volume is gone!?"));
        };

        let vcr: VolumeConstructionRequest =
            serde_json::from_str(&volume.data())?;

        let mut targets: Vec<SocketAddrV6> = vec![];

        find_matching_rw_regions_in_volume(
            &vcr,
            dataset.address().ip(),
            &mut targets,
        )
        .map_err(|e| Error::internal_error(&e.to_string()))?;

        Ok(targets)
    }

    // An Upstairs is created as part of a Volume hierarchy if the Volume
    // Construction Request includes a "Region" variant. This may be at any
    // layer of the Volume, and some notifications will come from an Upstairs
    // instead of the top level of the Volume. The following functions have an
    // Upstairs ID instead of a Volume ID for this reason.

    /// Record when an Upstairs notifies us about a repair. If that record
    /// (uniquely identified by the four IDs passed in plus the notification
    /// type) exists already, do nothing.
    pub async fn upstairs_repair_notification(
        &self,
        opctx: &OpContext,
        record: UpstairsRepairNotification,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::upstairs_repair_notification::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper("upstairs_repair_notification")
            .transaction(&conn, |conn| {
                let record = record.clone();
                let err = err.clone();

                async move {
                    // Return 409 if a repair ID does not match types
                    let mismatched_record_type_count: usize =
                        dsl::upstairs_repair_notification
                            .filter(dsl::repair_id.eq(record.repair_id))
                            .filter(dsl::repair_type.ne(record.repair_type))
                            .execute_async(&conn)
                            .await?;

                    if mismatched_record_type_count > 0 {
                        return Err(err.bail(Error::conflict(&format!(
                            "existing repair type for id {} does not \
                            match {:?}!",
                            record.repair_id, record.repair_type,
                        ))));
                    }

                    match &record.notification_type {
                        UpstairsRepairNotificationType::Started => {
                            // Proceed - the insertion can succeed or fail below
                            // based on the table's primary key
                        }

                        UpstairsRepairNotificationType::Succeeded
                        | UpstairsRepairNotificationType::Failed => {
                            // However, Nexus must accept only one "finished"
                            // status - an Upstairs cannot change this and must
                            // instead perform another repair with a new repair
                            // ID.
                            let maybe_existing_finish_record: Option<
                                UpstairsRepairNotification,
                            > = dsl::upstairs_repair_notification
                                .filter(dsl::repair_id.eq(record.repair_id))
                                .filter(dsl::upstairs_id.eq(record.upstairs_id))
                                .filter(dsl::session_id.eq(record.session_id))
                                .filter(dsl::region_id.eq(record.region_id))
                                .filter(dsl::notification_type.eq_any(vec![
                                    UpstairsRepairNotificationType::Succeeded,
                                    UpstairsRepairNotificationType::Failed,
                                ]))
                                .get_result_async(&conn)
                                .await
                                .optional()?;

                            if let Some(existing_finish_record) =
                                maybe_existing_finish_record
                            {
                                if existing_finish_record.notification_type
                                    != record.notification_type
                                {
                                    return Err(err.bail(Error::conflict(
                                        "existing finish record does not match",
                                    )));
                                } else {
                                    // inserting the same record, bypass
                                    return Ok(());
                                }
                            }
                        }
                    }

                    diesel::insert_into(dsl::upstairs_repair_notification)
                        .values(record)
                        .on_conflict((
                            dsl::repair_id,
                            dsl::upstairs_id,
                            dsl::session_id,
                            dsl::region_id,
                            dsl::notification_type,
                        ))
                        .do_nothing()
                        .execute_async(&conn)
                        .await?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    async fn upstairs_repair_progress_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<Error>,
        upstairs_id: TypedUuid<UpstairsKind>,
        repair_id: TypedUuid<UpstairsRepairKind>,
        repair_progress: RepairProgress,
    ) -> Result<(), diesel::result::Error> {
        use nexus_db_schema::schema::upstairs_repair_notification::dsl as notification_dsl;
        use nexus_db_schema::schema::upstairs_repair_progress::dsl;

        // Check that there is a repair id for the upstairs id
        let matching_repair: Option<UpstairsRepairNotification> =
            notification_dsl::upstairs_repair_notification
                .filter(
                    notification_dsl::repair_id
                        .eq(nexus_db_model::to_db_typed_uuid(repair_id)),
                )
                .filter(
                    notification_dsl::upstairs_id
                        .eq(nexus_db_model::to_db_typed_uuid(upstairs_id)),
                )
                .filter(
                    notification_dsl::notification_type
                        .eq(UpstairsRepairNotificationType::Started),
                )
                .get_result_async(conn)
                .await
                .optional()?;

        if matching_repair.is_none() {
            return Err(err.bail(Error::non_resourcetype_not_found(&format!(
                "upstairs {upstairs_id} repair {repair_id} not found"
            ))));
        }

        diesel::insert_into(dsl::upstairs_repair_progress)
            .values(UpstairsRepairProgress {
                repair_id: repair_id.into(),
                time: repair_progress.time,
                current_item: repair_progress.current_item,
                total_items: repair_progress.total_items,
            })
            .execute_async(conn)
            .await?;

        Ok(())
    }

    /// Record Upstairs repair progress
    pub async fn upstairs_repair_progress(
        &self,
        opctx: &OpContext,
        upstairs_id: TypedUuid<UpstairsKind>,
        repair_id: TypedUuid<UpstairsRepairKind>,
        repair_progress: RepairProgress,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper("upstairs_repair_progress")
            .transaction(&conn, |conn| {
                let repair_progress = repair_progress.clone();
                let err = err.clone();

                async move {
                    Self::upstairs_repair_progress_in_txn(
                        &conn,
                        err,
                        upstairs_id,
                        repair_id,
                        repair_progress,
                    )
                    .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    /// Record when a Downstairs client is requested to stop, and why
    pub async fn downstairs_client_stop_request_notification(
        &self,
        opctx: &OpContext,
        upstairs_id: TypedUuid<UpstairsKind>,
        downstairs_id: TypedUuid<DownstairsKind>,
        downstairs_client_stop_request: DownstairsClientStopRequest,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::downstairs_client_stop_request_notification::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        diesel::insert_into(dsl::downstairs_client_stop_request_notification)
            .values(DownstairsClientStopRequestNotification {
                time: downstairs_client_stop_request.time,
                upstairs_id: upstairs_id.into(),
                downstairs_id: downstairs_id.into(),
                reason: downstairs_client_stop_request.reason.into(),
            })
            .on_conflict((
                dsl::time,
                dsl::upstairs_id,
                dsl::downstairs_id,
                dsl::reason,
            ))
            .do_nothing()
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// Record when a Downstairs client is stopped, and why
    pub async fn downstairs_client_stopped_notification(
        &self,
        opctx: &OpContext,
        upstairs_id: TypedUuid<UpstairsKind>,
        downstairs_id: TypedUuid<DownstairsKind>,
        downstairs_client_stopped: DownstairsClientStopped,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::downstairs_client_stopped_notification::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        diesel::insert_into(dsl::downstairs_client_stopped_notification)
            .values(DownstairsClientStoppedNotification {
                time: downstairs_client_stopped.time,
                upstairs_id: upstairs_id.into(),
                downstairs_id: downstairs_id.into(),
                reason: downstairs_client_stopped.reason.into(),
            })
            .on_conflict((
                dsl::time,
                dsl::upstairs_id,
                dsl::downstairs_id,
                dsl::reason,
            ))
            .do_nothing()
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// For a downstairs being repaired, find the most recent repair
    /// notification
    pub async fn most_recent_started_repair_notification(
        &self,
        opctx: &OpContext,
        region_id: Uuid,
    ) -> Result<Option<UpstairsRepairNotification>, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::upstairs_repair_notification::dsl;

        dsl::upstairs_repair_notification
            .filter(dsl::region_id.eq(region_id))
            .filter(
                dsl::notification_type
                    .eq(UpstairsRepairNotificationType::Started),
            )
            .order_by(dsl::time.desc())
            .limit(1)
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// For a downstairs being repaired, return all related repair notifications
    /// in order of notification time.
    pub async fn repair_notifications_for_region(
        &self,
        opctx: &OpContext,
        region_id: Uuid,
    ) -> Result<Vec<UpstairsRepairNotification>, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::upstairs_repair_notification::dsl;

        dsl::upstairs_repair_notification
            .filter(dsl::region_id.eq(region_id))
            .order_by(dsl::time.asc())
            .select(UpstairsRepairNotification::as_select())
            .get_results_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// For a repair ID, find the most recent progress notification
    pub async fn most_recent_repair_progress(
        &self,
        opctx: &OpContext,
        repair_id: TypedUuid<UpstairsRepairKind>,
    ) -> Result<Option<UpstairsRepairProgress>, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::upstairs_repair_progress::dsl;

        dsl::upstairs_repair_progress
            .filter(
                dsl::repair_id.eq(nexus_db_model::to_db_typed_uuid(repair_id)),
            )
            .order_by(dsl::time.desc())
            .limit(1)
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Return true if a volume was soft-deleted or hard-deleted
    pub async fn volume_deleted(
        &self,
        volume_id: VolumeUuid,
    ) -> Result<bool, Error> {
        match self.volume_get(volume_id).await? {
            Some(v) => Ok(v.time_deleted.is_some()),
            None => Ok(true),
        }
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct CrucibleTargets {
    pub read_only_targets: Vec<String>,
}

// Serialize this enum into the `resources_to_clean_up` column to handle
// different versions over time.
#[derive(Debug, Serialize, Deserialize)]
pub enum CrucibleResources {
    V1(CrucibleResourcesV1),
    V2(CrucibleResourcesV2),
    V3(CrucibleResourcesV3),
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CrucibleResourcesV1 {
    pub datasets_and_regions: Vec<(CrucibleDataset, Region)>,
    pub datasets_and_snapshots: Vec<(CrucibleDataset, RegionSnapshot)>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CrucibleResourcesV2 {
    pub datasets_and_regions: Vec<(CrucibleDataset, Region)>,
    pub snapshots_to_delete: Vec<RegionSnapshot>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RegionSnapshotV3 {
    dataset: DatasetUuid,
    region: Uuid,
    snapshot: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CrucibleResourcesV3 {
    #[serde(deserialize_with = "null_to_empty_list")]
    pub regions: Vec<Uuid>,

    #[serde(deserialize_with = "null_to_empty_list")]
    pub region_snapshots: Vec<RegionSnapshotV3>,
}

// Cockroach's `json_agg` will emit a `null` instead of a `[]` if a SELECT
// returns zero rows. Handle that with this function when deserializing.
fn null_to_empty_list<'de, D, T>(de: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(de)?.unwrap_or_default())
}

impl DataStore {
    /// For a CrucibleResources object, return the Regions to delete, as well as
    /// the CrucibleDataset they belong to.
    pub async fn regions_to_delete(
        &self,
        crucible_resources: &CrucibleResources,
    ) -> LookupResult<Vec<(CrucibleDataset, Region)>> {
        let conn = self.pool_connection_unauthorized().await?;

        match crucible_resources {
            CrucibleResources::V1(crucible_resources) => {
                Ok(crucible_resources.datasets_and_regions.clone())
            }

            CrucibleResources::V2(crucible_resources) => {
                Ok(crucible_resources.datasets_and_regions.clone())
            }

            CrucibleResources::V3(crucible_resources) => {
                use nexus_db_schema::schema::crucible_dataset::dsl as dataset_dsl;
                use nexus_db_schema::schema::region::dsl as region_dsl;

                region_dsl::region
                    .filter(
                        region_dsl::id
                            .eq_any(crucible_resources.regions.clone()),
                    )
                    .inner_join(
                        dataset_dsl::crucible_dataset
                            .on(region_dsl::dataset_id.eq(dataset_dsl::id)),
                    )
                    .select((CrucibleDataset::as_select(), Region::as_select()))
                    .get_results_async::<(CrucibleDataset, Region)>(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })
            }
        }
    }

    /// For a CrucibleResources object, return the RegionSnapshots to delete, as
    /// well as the CrucibleDataset they belong to.
    pub async fn snapshots_to_delete(
        &self,
        crucible_resources: &CrucibleResources,
    ) -> LookupResult<Vec<(CrucibleDataset, RegionSnapshot)>> {
        let conn = self.pool_connection_unauthorized().await?;

        match crucible_resources {
            CrucibleResources::V1(crucible_resources) => {
                Ok(crucible_resources.datasets_and_snapshots.clone())
            }

            CrucibleResources::V2(crucible_resources) => {
                use nexus_db_schema::schema::crucible_dataset::dsl;

                let mut result: Vec<_> = Vec::with_capacity(
                    crucible_resources.snapshots_to_delete.len(),
                );

                for snapshots_to_delete in
                    &crucible_resources.snapshots_to_delete
                {
                    let maybe_dataset = dsl::crucible_dataset
                        .filter(dsl::id.eq(snapshots_to_delete.dataset_id))
                        .select(CrucibleDataset::as_select())
                        .first_async(&*conn)
                        .await
                        .optional()
                        .map_err(|e| {
                            public_error_from_diesel(e, ErrorHandler::Server)
                        })?;

                    match maybe_dataset {
                        Some(dataset) => {
                            result.push((dataset, snapshots_to_delete.clone()));
                        }

                        None => {
                            return Err(Error::internal_error(&format!(
                                "could not find dataset {}!",
                                snapshots_to_delete.dataset_id,
                            )));
                        }
                    }
                }

                Ok(result)
            }

            CrucibleResources::V3(crucible_resources) => {
                use nexus_db_schema::schema::crucible_dataset::dsl as dataset_dsl;
                use nexus_db_schema::schema::region_snapshot::dsl;

                let mut datasets_and_snapshots = Vec::with_capacity(
                    crucible_resources.region_snapshots.len(),
                );

                for region_snapshots in &crucible_resources.region_snapshots {
                    let maybe_tuple = dsl::region_snapshot
                        .filter(
                            dsl::dataset_id
                                .eq(to_db_typed_uuid(region_snapshots.dataset)),
                        )
                        .filter(dsl::region_id.eq(region_snapshots.region))
                        .filter(dsl::snapshot_id.eq(region_snapshots.snapshot))
                        .inner_join(
                            dataset_dsl::crucible_dataset
                                .on(dsl::dataset_id.eq(dataset_dsl::id)),
                        )
                        .select((
                            CrucibleDataset::as_select(),
                            RegionSnapshot::as_select(),
                        ))
                        .first_async::<(CrucibleDataset, RegionSnapshot)>(
                            &*conn,
                        )
                        .await
                        .optional()
                        .map_err(|e| {
                            public_error_from_diesel(e, ErrorHandler::Server)
                        })?;

                    match maybe_tuple {
                        Some(tuple) => {
                            datasets_and_snapshots.push(tuple);
                        }

                        None => {
                            // If something else is deleting the exact same
                            // CrucibleResources (for example from a duplicate
                            // resource-delete saga) then these region_snapshot
                            // entries could be gone (because they are hard
                            // deleted). Skip missing entries, return only what
                            // we can find.
                        }
                    }
                }

                Ok(datasets_and_snapshots)
            }
        }
    }
}

/// Check if a region is present in a Volume Construction Request
fn region_in_vcr(
    vcr: &VolumeConstructionRequest,
    region: &SocketAddrV6,
) -> anyhow::Result<bool> {
    let mut parts: VecDeque<&VolumeConstructionRequest> = VecDeque::new();
    parts.push_back(vcr);

    let mut region_found = false;

    while let Some(vcr_part) = parts.pop_front() {
        match vcr_part {
            VolumeConstructionRequest::Volume { sub_volumes, .. } => {
                for sub_volume in sub_volumes {
                    parts.push_back(sub_volume);
                }

                // Skip looking at read-only parent, this function only looks
                // for R/W regions
            }

            VolumeConstructionRequest::Url { .. } => {
                // nothing required
            }

            VolumeConstructionRequest::Region { opts, .. } => {
                for target in &opts.target {
                    match target {
                        SocketAddr::V6(t) if *t == *region => {
                            region_found = true;
                            break;
                        }
                        SocketAddr::V6(_) => {}
                        SocketAddr::V4(_) => {
                            bail!("region target contains an IPv4 address");
                        }
                    }
                }
            }

            VolumeConstructionRequest::File { .. } => {
                // nothing required
            }
        }
    }

    Ok(region_found)
}

/// Check if a read-only target is present anywhere in a Volume Construction
/// Request
fn read_only_target_in_vcr(
    vcr: &VolumeConstructionRequest,
    read_only_target: &SocketAddrV6,
) -> anyhow::Result<bool> {
    struct Work<'a> {
        vcr_part: &'a VolumeConstructionRequest,
        under_read_only_parent: bool,
    }

    let mut parts: VecDeque<Work> = VecDeque::new();
    parts.push_back(Work { vcr_part: &vcr, under_read_only_parent: false });

    while let Some(work) = parts.pop_front() {
        match work.vcr_part {
            VolumeConstructionRequest::Volume {
                sub_volumes,
                read_only_parent,
                ..
            } => {
                for sub_volume in sub_volumes {
                    parts.push_back(Work {
                        vcr_part: &sub_volume,
                        under_read_only_parent: work.under_read_only_parent,
                    });
                }

                if let Some(read_only_parent) = read_only_parent {
                    parts.push_back(Work {
                        vcr_part: &read_only_parent,
                        under_read_only_parent: true,
                    });
                }
            }

            VolumeConstructionRequest::Url { .. } => {
                // nothing required
            }

            VolumeConstructionRequest::Region { opts, .. } => {
                if work.under_read_only_parent && !opts.read_only {
                    // This VCR isn't constructed properly, there's a read/write
                    // region under a read-only parent
                    bail!("read-write region under read-only parent");
                }

                for target in &opts.target {
                    match target {
                        SocketAddr::V6(t)
                            if *t == *read_only_target && opts.read_only =>
                        {
                            return Ok(true);
                        }
                        SocketAddr::V6(_) => {}
                        SocketAddr::V4(_) => {
                            bail!("region target contains an IPv4 address");
                        }
                    }
                }
            }

            VolumeConstructionRequest::File { .. } => {
                // nothing required
            }
        }
    }

    Ok(false)
}

#[derive(Clone)]
pub struct VolumeReplacementParams {
    pub volume_id: VolumeUuid,
    pub region_id: Uuid,
    pub region_addr: SocketAddrV6,
}

// types for volume_replace_snapshot and replace_read_only_target_in_vcr
// parameters

#[derive(Debug, Clone, Copy)]
pub struct VolumeWithTarget(pub VolumeUuid);

#[derive(Debug, Clone, Copy)]
pub struct ExistingTarget(pub SocketAddrV6);

#[derive(Debug, Clone, Copy)]
pub struct ReplacementTarget(pub SocketAddrV6);

#[derive(Debug, Clone, Copy)]
pub struct VolumeToDelete(pub VolumeUuid);

// The result type returned from both `volume_replace_region` and
// `volume_replace_snapshot`
#[must_use]
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum VolumeReplaceResult {
    // based on the VCRs, seems like the replacement already happened
    AlreadyHappened,

    // this call performed the replacement
    Done,

    // the "existing" volume was soft deleted
    ExistingVolumeSoftDeleted,

    // the "existing" volume was hard deleted
    ExistingVolumeHardDeleted,
}

impl DataStore {
    async fn volume_replace_region_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<ReplaceRegionError>,
        existing: VolumeReplacementParams,
        replacement: VolumeReplacementParams,
    ) -> Result<VolumeReplaceResult, diesel::result::Error> {
        // In a single transaction:
        //
        // - set the existing region's volume id to the replacement's volume id
        // - set the replacement region's volume id to the existing's volume id
        // - update the existing volume's construction request to replace the
        // existing region's SocketAddrV6 with the replacement region's
        //
        // This function's effects can be undone by calling it with swapped
        // parameters.
        //
        // # Example #
        //
        // Imagine `volume_replace_region` is called with the following,
        // pretending that UUIDs are just eight uppercase letters:
        //
        //   let existing = VolumeReplacementParams {
        //     volume_id: TARGET_VOL,
        //     region_id: TARGET_REG,
        //     region_addr: "[fd00:1122:3344:145::10]:40001",
        //   }
        //
        //   let replace = VolumeReplacementParams {
        //     volume_id: NEW_VOL,
        //     region_id: NEW_REG,
        //     region_addr: "[fd00:1122:3344:322::4]:3956",
        //   }
        //
        // In the database, the relevant records (and columns) of the region
        // table look like this prior to the transaction:
        //
        //            id | volume_id
        //  -------------| ---------
        //    TARGET_REG | TARGET_VOL
        //       NEW_REG | NEW_VOL
        //
        // TARGET_VOL has a volume construction request where one of the targets
        // list will contain TARGET_REG's address:
        //
        //   {
        //     "type": "volume",
        //     "block_size": 512,
        //     "id": "TARGET_VOL",
        //     "read_only_parent": {
        //       ...
        //     },
        //     "sub_volumes": [
        //       {
        //         ...
        //         "opts": {
        //           ...
        //           "target": [
        //             "[fd00:1122:3344:103::3]:19004",
        //             "[fd00:1122:3344:79::12]:27015",
        //             "[fd00:1122:3344:145::10]:40001"  <-----
        //           ]
        //         }
        //       }
        //     ]
        //   }
        //
        // Note it is not required for the replacement volume to exist as a
        // database record for this transaction.
        //
        // The first part of the transaction will swap the volume IDs of the
        // existing and replacement region records:
        //
        //           id | volume_id
        //  ------------| ---------
        //   TARGET_REG | NEW_VOL
        //      NEW_REG | TARGET_VOL
        //
        // The second part of the transaction will update the volume
        // construction request of TARGET_VOL by finding and replacing
        // TARGET_REG's address (in the appropriate targets array) with
        // NEW_REG's address:
        //
        //   {
        //           ...
        //           "target": [
        //             "[fd00:1122:3344:103::3]:19004",
        //             "[fd00:1122:3344:79::12]:27015",
        //             "[fd00:1122:3344:322::4]:3956"  <-----
        //           ]
        //           ...
        //   }
        //
        // After the transaction, the caller should ensure that TARGET_REG is
        // referenced (via its socket address) in NEW_VOL. For an example, this
        // is done as part of the region replacement start saga.

        // Grab the old volume first
        let maybe_old_volume = {
            volume_dsl::volume
                .filter(volume_dsl::id.eq(to_db_typed_uuid(existing.volume_id)))
                .select(Volume::as_select())
                .first_async::<Volume>(conn)
                .await
                .optional()
                .map_err(|e| {
                    err.bail_retryable_or_else(e, |e| {
                        ReplaceRegionError::Public(public_error_from_diesel(
                            e,
                            ErrorHandler::Server,
                        ))
                    })
                })?
        };

        let old_volume = if let Some(old_volume) = maybe_old_volume {
            old_volume
        } else {
            // Existing volume was hard-deleted, so return here. We can't
            // perform the region replacement now, and this will short-circuit
            // the rest of the process.

            return Ok(VolumeReplaceResult::ExistingVolumeHardDeleted);
        };

        if old_volume.time_deleted.is_some() {
            // Existing volume was soft-deleted, so return here for the same
            // reason: the region replacement process should be short-circuited
            // now.
            return Ok(VolumeReplaceResult::ExistingVolumeSoftDeleted);
        }

        let old_vcr: VolumeConstructionRequest =
            match serde_json::from_str(&old_volume.data()) {
                Ok(vcr) => vcr,
                Err(e) => {
                    return Err(err.bail(ReplaceRegionError::SerdeError(e)));
                }
            };

        // Does it look like this replacement already happened?
        let old_region_in_vcr =
            match region_in_vcr(&old_vcr, &existing.region_addr) {
                Ok(v) => v,
                Err(e) => {
                    return Err(
                        err.bail(ReplaceRegionError::RegionReplacementError(e))
                    );
                }
            };
        let new_region_in_vcr =
            match region_in_vcr(&old_vcr, &replacement.region_addr) {
                Ok(v) => v,
                Err(e) => {
                    return Err(
                        err.bail(ReplaceRegionError::RegionReplacementError(e))
                    );
                }
            };

        if !old_region_in_vcr && new_region_in_vcr {
            // It does seem like the replacement happened - if this function is
            // called twice in a row then this can happen.
            return Ok(VolumeReplaceResult::AlreadyHappened);
        } else if old_region_in_vcr && !new_region_in_vcr {
            // The replacement hasn't happened yet, but can proceed
        } else if old_region_in_vcr && new_region_in_vcr {
            // Both the old region and new region exist in this VCR. Regions are
            // not reused, so this is an illegal state: if the replacement of
            // the old region occurred, then the new region would be present
            // multiple times in the volume. We have to bail out here.
            //
            // The guards against this happening are:
            //
            // - only one replacement can occur for a volume at a time (due to
            //   the volume repair lock), and
            //
            // - region replacement does not delete the old region until the
            //   "region replacement finish" saga, which happens at the very end
            //   of the process. If it eagerly deleted the region, the crucible
            //   agent would be free to reuse the port for another region
            //   allocation, and an identical target (read: ip and port) could
            //   be confusing. Most of the time, we assume that the dataset
            //   containing that agent has been expunged, so the agent is gone,
            //   so this port reuse cannot occur
            return Err(err.bail(ReplaceRegionError::RegionReplacementError(
                anyhow!("old_region_in_vcr && new_region_in_vcr"),
            )));
        } else if !old_region_in_vcr && !new_region_in_vcr {
            // Neither the region we've been asked to replace or the new region
            // is in the VCR. This is an illegal state, as this function would
            // be performing a no-op. We have to bail out here.
            //
            // The guard against this happening is again that only one
            // replacement can occur for a volume at a time: if it was possible
            // for multiple region replacements to occur, then both would be
            // attempting to swap out the same old region for different new
            // regions:
            //
            // region replacement one:
            //
            //   volume_replace_region_in_txn(
            //     ..,
            //     existing = [fd00:1122:3344:145::10]:40001,
            //     replacement = [fd00:1122:3344:322::4]:3956,
            //   )
            //
            // region replacement two:
            //
            //   volume_replace_region_in_txn(
            //     ..,
            //     existing = [fd00:1122:3344:145::10]:40001,
            //     replacement = [fd00:1122:3344:fd1::123]:27001,
            //   )
            //
            // The one that replaced second would always land in this branch.
            return Err(err.bail(ReplaceRegionError::RegionReplacementError(
                anyhow!("!old_region_in_vcr && !new_region_in_vcr"),
            )));
        }

        use nexus_db_schema::schema::region::dsl as region_dsl;
        use nexus_db_schema::schema::volume::dsl as volume_dsl;

        // Set the existing region's volume id to the replacement's volume id
        diesel::update(region_dsl::region)
            .filter(region_dsl::id.eq(existing.region_id))
            .set(
                region_dsl::volume_id
                    .eq(to_db_typed_uuid(replacement.volume_id)),
            )
            .execute_async(conn)
            .await
            .map_err(|e| {
                err.bail_retryable_or_else(e, |e| {
                    ReplaceRegionError::Public(public_error_from_diesel(
                        e,
                        ErrorHandler::Server,
                    ))
                })
            })?;

        // Set the replacement region's volume id to the existing's volume id
        diesel::update(region_dsl::region)
            .filter(region_dsl::id.eq(replacement.region_id))
            .set(region_dsl::volume_id.eq(to_db_typed_uuid(existing.volume_id)))
            .execute_async(conn)
            .await
            .map_err(|e| {
                err.bail_retryable_or_else(e, |e| {
                    ReplaceRegionError::Public(public_error_from_diesel(
                        e,
                        ErrorHandler::Server,
                    ))
                })
            })?;

        // Update the existing volume's construction request to replace the
        // existing region's SocketAddrV6 with the replacement region's

        // Copy the old volume's VCR, changing out the old region for the new.
        let new_vcr = match replace_region_in_vcr(
            &old_vcr,
            existing.region_addr,
            replacement.region_addr,
        ) {
            Ok(new_vcr) => new_vcr,
            Err(e) => {
                return Err(
                    err.bail(ReplaceRegionError::RegionReplacementError(e))
                );
            }
        };

        let new_volume_data = serde_json::to_string(&new_vcr)
            .map_err(|e| err.bail(ReplaceRegionError::SerdeError(e)))?;

        // Update the existing volume's data
        diesel::update(volume_dsl::volume)
            .filter(volume_dsl::id.eq(to_db_typed_uuid(existing.volume_id)))
            .set(volume_dsl::data.eq(new_volume_data))
            .execute_async(conn)
            .await
            .map_err(|e| {
                err.bail_retryable_or_else(e, |e| {
                    ReplaceRegionError::Public(public_error_from_diesel(
                        e,
                        ErrorHandler::Server,
                    ))
                })
            })?;

        // After region replacement, validate invariants for all volumes
        #[cfg(any(test, feature = "testing"))]
        Self::validate_volume_invariants(conn).await?;

        Ok(VolumeReplaceResult::Done)
    }

    /// Replace a read-write region in a Volume with a new region.
    pub async fn volume_replace_region(
        &self,
        existing: VolumeReplacementParams,
        replacement: VolumeReplacementParams,
    ) -> Result<VolumeReplaceResult, Error> {
        let err = OptionalError::new();

        let conn = self.pool_connection_unauthorized().await?;
        self.transaction_retry_wrapper("volume_replace_region")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let existing = existing.clone();
                let replacement = replacement.clone();
                async move {
                    Self::volume_replace_region_in_txn(
                        &conn,
                        err,
                        existing,
                        replacement,
                    )
                    .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        ReplaceRegionError::Public(e) => e,

                        ReplaceRegionError::SerdeError(_) => {
                            Error::internal_error(&err.to_string())
                        }

                        ReplaceRegionError::RegionReplacementError(_) => {
                            Error::internal_error(&err.to_string())
                        }
                    }
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    async fn volume_replace_snapshot_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<ReplaceSnapshotError>,
        volume_id: VolumeWithTarget,
        existing: ExistingTarget,
        replacement: ReplacementTarget,
        volume_to_delete_id: VolumeToDelete,
    ) -> Result<VolumeReplaceResult, diesel::result::Error> {
        use nexus_db_schema::schema::volume::dsl as volume_dsl;
        use nexus_db_schema::schema::volume_resource_usage::dsl as ru_dsl;

        // Grab the old volume first
        let maybe_old_volume = {
            volume_dsl::volume
                .filter(volume_dsl::id.eq(to_db_typed_uuid(volume_id.0)))
                .select(Volume::as_select())
                .first_async::<Volume>(conn)
                .await
                .optional()
                .map_err(|e| {
                    err.bail_retryable_or_else(e, |e| {
                        ReplaceSnapshotError::Public(public_error_from_diesel(
                            e,
                            ErrorHandler::Server,
                        ))
                    })
                })?
        };

        let old_volume = if let Some(old_volume) = maybe_old_volume {
            old_volume
        } else {
            // Existing volume was hard-deleted, so return here. We can't
            // perform the region replacement now, and this will short-circuit
            // the rest of the process.

            return Ok(VolumeReplaceResult::ExistingVolumeHardDeleted);
        };

        if old_volume.time_deleted.is_some() {
            // Existing volume was soft-deleted, so return here for the same
            // reason: the region replacement process should be short-circuited
            // now.
            return Ok(VolumeReplaceResult::ExistingVolumeSoftDeleted);
        }

        let old_vcr: VolumeConstructionRequest =
            match serde_json::from_str(&old_volume.data()) {
                Ok(vcr) => vcr,
                Err(e) => {
                    return Err(err.bail(ReplaceSnapshotError::SerdeError(e)));
                }
            };

        // Does it look like this replacement already happened?
        let old_target_in_vcr =
            match read_only_target_in_vcr(&old_vcr, &existing.0) {
                Ok(v) => v,
                Err(e) => {
                    return Err(err.bail(
                        ReplaceSnapshotError::SnapshotReplacementError(e),
                    ));
                }
            };

        let new_target_in_vcr =
            match read_only_target_in_vcr(&old_vcr, &replacement.0) {
                Ok(v) => v,
                Err(e) => {
                    return Err(err.bail(
                        ReplaceSnapshotError::SnapshotReplacementError(e),
                    ));
                }
            };

        if !old_target_in_vcr && new_target_in_vcr {
            // It does seem like the replacement happened
            return Ok(VolumeReplaceResult::AlreadyHappened);
        }

        // Update the existing volume's construction request to replace the
        // existing target's SocketAddrV6 with the replacement target's

        // Copy the old volume's VCR, changing out the old target for the new.
        let (new_vcr, replacements) = match replace_read_only_target_in_vcr(
            &old_vcr,
            existing,
            replacement,
        ) {
            Ok(new_vcr) => new_vcr,
            Err(e) => {
                return Err(
                    err.bail(ReplaceSnapshotError::SnapshotReplacementError(e))
                );
            }
        };

        // Expect that this only happened once. If it happened multiple times,
        // question everything: how would a snapshot be used twice?!

        if replacements != 1 {
            return Err(err.bail(
                ReplaceSnapshotError::UnexpectedReplacedTargets(
                    replacements,
                    1,
                ),
            ));
        }

        let new_volume_data = serde_json::to_string(&new_vcr)
            .map_err(|e| err.bail(ReplaceSnapshotError::SerdeError(e)))?;

        // Update the existing volume's data
        diesel::update(volume_dsl::volume)
            .filter(volume_dsl::id.eq(to_db_typed_uuid(volume_id.0)))
            .set(volume_dsl::data.eq(new_volume_data))
            .execute_async(conn)
            .await
            .map_err(|e| {
                err.bail_retryable_or_else(e, |e| {
                    ReplaceSnapshotError::Public(public_error_from_diesel(
                        e,
                        ErrorHandler::Server,
                    ))
                })
            })?;

        // Make a new VCR that will stash the target to delete. The values here
        // don't matter, just that it gets fed into the volume_delete machinery
        // later.
        let vcr = VolumeConstructionRequest::Volume {
            id: *volume_to_delete_id.0.as_untyped_uuid(),
            block_size: 512,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 512,
                blocks_per_extent: 1,
                extent_count: 1,
                r#gen: 1,
                opts: sled_agent_client::CrucibleOpts {
                    id: *volume_to_delete_id.0.as_untyped_uuid(),
                    target: vec![existing.0.into()],
                    lossy: false,
                    flush_timeout: None,
                    key: None,
                    cert_pem: None,
                    key_pem: None,
                    root_cert_pem: None,
                    control: None,
                    read_only: true,
                },
            }],
            read_only_parent: None,
        };

        let volume_data = serde_json::to_string(&vcr)
            .map_err(|e| err.bail(ReplaceSnapshotError::SerdeError(e)))?;

        // Update the volume to delete data
        let num_updated = diesel::update(volume_dsl::volume)
            .filter(volume_dsl::id.eq(to_db_typed_uuid(volume_to_delete_id.0)))
            .filter(volume_dsl::time_deleted.is_null())
            .set(volume_dsl::data.eq(volume_data))
            .execute_async(conn)
            .await?;

        if num_updated != 1 {
            return Err(err.bail(
                ReplaceSnapshotError::UnexpectedDatabaseUpdate(num_updated, 1),
            ));
        }

        // Update the appropriate volume resource usage records - it could
        // either be a read-only region or a region snapshot, so determine what
        // it is first

        let maybe_existing_usage =
            Self::read_only_target_to_volume_resource_usage(conn, &existing.0)
                .await?;

        let Some(existing_usage) = maybe_existing_usage else {
            return Err(err.bail(ReplaceSnapshotError::CouldNotFindResource(
                format!("could not find resource for {}", existing.0,),
            )));
        };

        // The "existing" target moved into the volume to delete

        Self::swap_volume_usage_records_for_resources(
            conn,
            existing_usage,
            volume_id.0,
            volume_to_delete_id.0,
        )
        .await
        .map_err(|e| {
            err.bail_retryable_or_else(e, |e| {
                ReplaceSnapshotError::Public(public_error_from_diesel(
                    e,
                    ErrorHandler::Server,
                ))
            })
        })?;

        let maybe_replacement_usage =
            Self::read_only_target_to_volume_resource_usage(
                conn,
                &replacement.0,
            )
            .await?;

        let Some(replacement_usage) = maybe_replacement_usage else {
            return Err(err.bail(ReplaceSnapshotError::CouldNotFindResource(
                format!("could not find resource for {}", existing.0,),
            )));
        };

        // The intention leaving this transaction is that the correct volume
        // resource usage records exist, so:
        //
        // - if no usage record existed for the replacement usage, then create a
        //   new record that points to the volume id (this can happen if the
        //   volume to delete was blank when coming into this function)
        //
        // - if records exist for the "replacement" usage, then one of those
        //   will match the volume to delete id, so perform a swap instead to
        //   the volume id

        let existing_replacement_volume_usage_records =
            Self::volume_usage_records_for_resource_query(
                replacement_usage.clone(),
            )
            .load_async(conn)
            .await
            .map_err(|e| {
                err.bail_retryable_or_else(e, |e| {
                    ReplaceSnapshotError::Public(public_error_from_diesel(
                        e,
                        ErrorHandler::Server,
                    ))
                })
            })?
            // TODO be smart enough to .filter the above query
            .into_iter()
            .filter(|record| record.volume_id == volume_to_delete_id.0.into())
            .count();

        // The "replacement" target moved into the volume

        if existing_replacement_volume_usage_records == 0 {
            // No matching record
            let new_record =
                VolumeResourceUsageRecord::new(volume_id.0, replacement_usage);

            diesel::insert_into(ru_dsl::volume_resource_usage)
                .values(new_record)
                .execute_async(conn)
                .await
                .map_err(|e| {
                    err.bail_retryable_or_else(e, |e| {
                        ReplaceSnapshotError::Public(public_error_from_diesel(
                            e,
                            ErrorHandler::Server,
                        ))
                    })
                })?;
        } else if existing_replacement_volume_usage_records == 1 {
            // One matching record: perform swap
            Self::swap_volume_usage_records_for_resources(
                conn,
                replacement_usage,
                volume_to_delete_id.0,
                volume_id.0,
            )
            .await
            .map_err(|e| {
                err.bail_retryable_or_else(e, |e| {
                    ReplaceSnapshotError::Public(public_error_from_diesel(
                        e,
                        ErrorHandler::Server,
                    ))
                })
            })?;
        } else {
            // More than one matching record!
            return Err(err.bail(
                ReplaceSnapshotError::MultipleResourceUsageRecords(format!(
                    "{replacement_usage:?}"
                )),
            ));
        }

        // After region snapshot replacement, validate invariants for all
        // volumes
        #[cfg(any(test, feature = "testing"))]
        Self::validate_volume_invariants(conn).await?;

        Ok(VolumeReplaceResult::Done)
    }

    /// Replace a read-only target in a Volume with a new region
    ///
    /// In a single transaction:
    ///
    /// - update a volume's serialized construction request by replacing a
    ///   single target.
    ///
    /// - stash the replaced target in a "volume to delete"'s serialized
    ///   construction request
    ///
    /// Note that this transaction does _not_ update a region snapshot's volume
    /// references table! This is legal because the existing target reference is
    /// written into the volume to delete's construction request.
    ///
    /// This function's effects can be undone by calling it with swapped
    /// `existing` and `replacement` parameters.
    pub async fn volume_replace_snapshot(
        &self,
        volume_id: VolumeWithTarget,
        existing: ExistingTarget,
        replacement: ReplacementTarget,
        volume_to_delete_id: VolumeToDelete,
    ) -> Result<VolumeReplaceResult, Error> {
        let err = OptionalError::new();

        let conn = self.pool_connection_unauthorized().await?;
        self.transaction_retry_wrapper("volume_replace_snapshot")
            .transaction(&conn, |conn| {
                let err = err.clone();

                async move {
                    Self::volume_replace_snapshot_in_txn(
                        &conn,
                        err,
                        volume_id,
                        existing,
                        replacement,
                        volume_to_delete_id,
                    )
                    .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        ReplaceSnapshotError::Public(e) => e,

                        ReplaceSnapshotError::SerdeError(_)
                        | ReplaceSnapshotError::SnapshotReplacementError(_)
                        | ReplaceSnapshotError::UnexpectedReplacedTargets(
                            _,
                            _,
                        )
                        | ReplaceSnapshotError::UnexpectedDatabaseUpdate(
                            _,
                            _,
                        )
                        | ReplaceSnapshotError::AddressParseError(_)
                        | ReplaceSnapshotError::CouldNotFindResource(_)
                        | ReplaceSnapshotError::MultipleResourceUsageRecords(
                            _,
                        ) => Error::internal_error(&err.to_string()),
                    }
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }
}

/// Return the read-only targets from a VolumeConstructionRequest.
///
/// The targets of a volume construction request map to resources.
pub fn read_only_resources_associated_with_volume(
    vcr: &VolumeConstructionRequest,
    crucible_targets: &mut CrucibleTargets,
) {
    let mut parts: VecDeque<&VolumeConstructionRequest> = VecDeque::new();
    parts.push_back(&vcr);

    while let Some(vcr_part) = parts.pop_front() {
        match vcr_part {
            VolumeConstructionRequest::Volume {
                sub_volumes,
                read_only_parent,
                ..
            } => {
                for sub_volume in sub_volumes {
                    parts.push_back(sub_volume);
                }

                if let Some(read_only_parent) = read_only_parent {
                    parts.push_back(read_only_parent);
                }
            }

            VolumeConstructionRequest::Url { .. } => {
                // no action required
            }

            VolumeConstructionRequest::Region { opts, .. } => {
                for target in &opts.target {
                    if opts.read_only {
                        crucible_targets
                            .read_only_targets
                            .push(target.to_string());
                    }
                }
            }

            VolumeConstructionRequest::File { .. } => {
                // no action required
            }
        }
    }
}

/// Return the read-write targets from a VolumeConstructionRequest.
///
/// The targets of a volume construction request map to resources.
pub fn read_write_resources_associated_with_volume(
    vcr: &VolumeConstructionRequest,
    targets: &mut Vec<String>,
) {
    let mut parts: VecDeque<&VolumeConstructionRequest> = VecDeque::new();
    parts.push_back(&vcr);

    while let Some(vcr_part) = parts.pop_front() {
        match vcr_part {
            VolumeConstructionRequest::Volume { sub_volumes, .. } => {
                for sub_volume in sub_volumes {
                    parts.push_back(sub_volume);
                }

                // No need to look under read-only parent
            }

            VolumeConstructionRequest::Url { .. } => {
                // no action required
            }

            VolumeConstructionRequest::Region { opts, .. } => {
                if !opts.read_only {
                    for target in &opts.target {
                        targets.push(target.to_string());
                    }
                }
            }

            VolumeConstructionRequest::File { .. } => {
                // no action required
            }
        }
    }
}

/// Return the number of read-write subvolumes in a VolumeConstructionRequest.
pub fn count_read_write_sub_volumes(
    vcr: &VolumeConstructionRequest,
) -> anyhow::Result<usize> {
    Ok(match vcr {
        VolumeConstructionRequest::Volume { sub_volumes, .. } => {
            sub_volumes.len()
        }

        VolumeConstructionRequest::Url { .. } => 0,

        VolumeConstructionRequest::Region { .. } => {
            // We don't support a pure Region VCR at the volume
            // level in the database, so this choice should
            // never be encountered.
            bail!("Region not supported as a top level volume");
        }

        VolumeConstructionRequest::File { .. } => 0,
    })
}

/// Returns true if the sub-volumes of a Volume are all read-only
pub fn volume_is_read_only(
    vcr: &VolumeConstructionRequest,
) -> anyhow::Result<bool> {
    match vcr {
        VolumeConstructionRequest::Volume { sub_volumes, .. } => {
            for sv in sub_volumes {
                match sv {
                    VolumeConstructionRequest::Region { opts, .. } => {
                        if !opts.read_only {
                            return Ok(false);
                        }
                    }

                    _ => {
                        bail!("Saw non-Region in sub-volume {:?}", sv);
                    }
                }
            }

            Ok(true)
        }

        VolumeConstructionRequest::Region { .. } => {
            // We don't support a pure Region VCR at the volume
            // level in the database, so this choice should
            // never be encountered, but I want to know if it is.
            bail!("Region not supported as a top level volume");
        }

        VolumeConstructionRequest::File { .. } => {
            // Effectively, this is read-only, as this BlockIO implementation
            // does not have a `write` implementation. This will be hit if
            // trying to make a snapshot or image out of a
            // `YouCanBootAnythingAsLongAsItsAlpine` image source.
            Ok(true)
        }

        VolumeConstructionRequest::Url { .. } => {
            // ImageSource::Url was deprecated
            bail!("Saw VolumeConstructionRequest::Url");
        }
    }
}

/// Replace a Region in a VolumeConstructionRequest
///
/// Note that UUIDs are not randomized by this step: Crucible will reject a
/// `target_replace` call if the replacement VolumeConstructionRequest does not
/// exactly match the original, except for a single Region difference.
///
/// Note that the generation number _is_ bumped in this step, otherwise
/// `compare_vcr_for_update` will reject the update.
fn replace_region_in_vcr(
    vcr: &VolumeConstructionRequest,
    old_region: SocketAddrV6,
    new_region: SocketAddrV6,
) -> anyhow::Result<VolumeConstructionRequest> {
    let mut new_vcr = vcr.clone();

    let mut parts: VecDeque<&mut VolumeConstructionRequest> = VecDeque::new();
    parts.push_back(&mut new_vcr);

    let mut old_region_found = false;

    while let Some(vcr_part) = parts.pop_front() {
        match vcr_part {
            VolumeConstructionRequest::Volume { sub_volumes, .. } => {
                for sub_volume in sub_volumes {
                    parts.push_back(sub_volume);
                }

                // Skip looking at read-only parent, this function only replaces
                // R/W regions
            }

            VolumeConstructionRequest::Url { .. } => {
                // nothing required
            }

            VolumeConstructionRequest::Region { opts, r#gen, .. } => {
                for target in &mut opts.target {
                    if let SocketAddr::V6(target) = target {
                        if *target == old_region {
                            *target = new_region;
                            old_region_found = true;
                        }
                    }
                }

                // Bump generation number, otherwise update will be rejected
                *r#gen = *r#gen + 1;
            }

            VolumeConstructionRequest::File { .. } => {
                // nothing required
            }
        }
    }

    if !old_region_found {
        bail!("old region {old_region} not found!");
    }

    Ok(new_vcr)
}

/// Replace a read-only target in a VolumeConstructionRequest
///
/// Note that UUIDs are not randomized by this step: Crucible will reject a
/// `target_replace` call if the replacement VolumeConstructionRequest does not
/// exactly match the original, except for a single Region difference.
///
/// Note that the generation number _is not_ bumped in this step.
fn replace_read_only_target_in_vcr(
    vcr: &VolumeConstructionRequest,
    old_target: ExistingTarget,
    new_target: ReplacementTarget,
) -> anyhow::Result<(VolumeConstructionRequest, usize)> {
    struct Work<'a> {
        vcr_part: &'a mut VolumeConstructionRequest,
        under_read_only_parent: bool,
    }
    let mut new_vcr = vcr.clone();

    let mut parts: VecDeque<Work> = VecDeque::new();
    parts.push_back(Work {
        vcr_part: &mut new_vcr,
        under_read_only_parent: false,
    });

    let mut replacements = 0;

    while let Some(work) = parts.pop_front() {
        match work.vcr_part {
            VolumeConstructionRequest::Volume {
                sub_volumes,
                read_only_parent,
                ..
            } => {
                for sub_volume in sub_volumes {
                    parts.push_back(Work {
                        vcr_part: sub_volume,
                        under_read_only_parent: work.under_read_only_parent,
                    });
                }

                if let Some(read_only_parent) = read_only_parent {
                    parts.push_back(Work {
                        vcr_part: read_only_parent,
                        under_read_only_parent: true,
                    });
                }
            }

            VolumeConstructionRequest::Url { .. } => {
                // nothing required
            }

            VolumeConstructionRequest::Region { opts, .. } => {
                if work.under_read_only_parent && !opts.read_only {
                    // This VCR isn't constructed properly, there's a read/write
                    // region under a read-only parent
                    bail!("read-write region under read-only parent");
                }

                for target in &mut opts.target {
                    if let SocketAddr::V6(target) = target {
                        if *target == old_target.0 && opts.read_only {
                            *target = new_target.0;
                            replacements += 1;
                        }
                    }
                }
            }

            VolumeConstructionRequest::File { .. } => {
                // nothing required
            }
        }
    }

    if replacements == 0 {
        bail!("target {old_target:?} not found!");
    }

    Ok((new_vcr, replacements))
}

/// Find Regions in a Volume's subvolumes list whose target match the argument
/// IP, and add them to the supplied Vec.
fn find_matching_rw_regions_in_volume(
    vcr: &VolumeConstructionRequest,
    ip: &std::net::Ipv6Addr,
    matched_targets: &mut Vec<SocketAddrV6>,
) -> anyhow::Result<()> {
    let mut parts: VecDeque<&VolumeConstructionRequest> = VecDeque::new();
    parts.push_back(vcr);

    while let Some(vcr_part) = parts.pop_front() {
        match vcr_part {
            VolumeConstructionRequest::Volume { sub_volumes, .. } => {
                for sub_volume in sub_volumes {
                    parts.push_back(sub_volume);
                }
            }

            VolumeConstructionRequest::Url { .. } => {
                // nothing required
            }

            VolumeConstructionRequest::Region { opts, .. } => {
                if !opts.read_only {
                    for target in &opts.target {
                        if let SocketAddr::V6(target) = target {
                            if target.ip() == ip {
                                matched_targets.push(*target);
                            }
                        }
                    }
                }
            }

            VolumeConstructionRequest::File { .. } => {
                // nothing required
            }
        }
    }

    Ok(())
}

fn region_sets(
    vcr: &VolumeConstructionRequest,
    region_sets: &mut Vec<Vec<SocketAddrV6>>,
) {
    let mut parts: VecDeque<&VolumeConstructionRequest> = VecDeque::new();
    parts.push_back(vcr);

    while let Some(work) = parts.pop_front() {
        match work {
            VolumeConstructionRequest::Volume {
                sub_volumes,
                read_only_parent,
                ..
            } => {
                for sub_volume in sub_volumes {
                    parts.push_back(&sub_volume);
                }

                if let Some(read_only_parent) = read_only_parent {
                    parts.push_back(&read_only_parent);
                }
            }

            VolumeConstructionRequest::Url { .. } => {
                // nothing required
            }

            VolumeConstructionRequest::Region { opts, .. } => {
                let mut targets = vec![];

                for target in &opts.target {
                    match target {
                        SocketAddr::V6(v6) => {
                            targets.push(*v6);
                        }
                        SocketAddr::V4(_) => {}
                    }
                }

                if targets.len() == opts.target.len() {
                    region_sets.push(targets);
                }
            }

            VolumeConstructionRequest::File { .. } => {
                // nothing required
            }
        }
    }
}

/// Check if an ipv6 address is referenced in a Volume Construction Request
fn ipv6_addr_referenced_in_vcr(
    vcr: &VolumeConstructionRequest,
    ip: &std::net::Ipv6Addr,
) -> bool {
    let mut parts: VecDeque<&VolumeConstructionRequest> = VecDeque::new();
    parts.push_back(vcr);

    while let Some(vcr_part) = parts.pop_front() {
        match vcr_part {
            VolumeConstructionRequest::Volume {
                sub_volumes,
                read_only_parent,
                ..
            } => {
                for sub_volume in sub_volumes {
                    parts.push_back(sub_volume);
                }

                if let Some(read_only_parent) = read_only_parent {
                    parts.push_back(read_only_parent);
                }
            }

            VolumeConstructionRequest::Url { .. } => {
                // nothing required
            }

            VolumeConstructionRequest::Region { opts, .. } => {
                for target in &opts.target {
                    match target {
                        SocketAddr::V6(t) => {
                            if t.ip() == ip {
                                return true;
                            }
                        }

                        SocketAddr::V4(_) => {}
                    }
                }
            }

            VolumeConstructionRequest::File { .. } => {
                // nothing required
            }
        }
    }

    false
}

/// Check if an ipv6 net is referenced in a Volume Construction Request
fn ipv6_net_referenced_in_vcr(
    vcr: &VolumeConstructionRequest,
    net: &oxnet::Ipv6Net,
) -> bool {
    let mut parts: VecDeque<&VolumeConstructionRequest> = VecDeque::new();
    parts.push_back(vcr);

    while let Some(vcr_part) = parts.pop_front() {
        match vcr_part {
            VolumeConstructionRequest::Volume {
                sub_volumes,
                read_only_parent,
                ..
            } => {
                for sub_volume in sub_volumes {
                    parts.push_back(sub_volume);
                }

                if let Some(read_only_parent) = read_only_parent {
                    parts.push_back(read_only_parent);
                }
            }

            VolumeConstructionRequest::Url { .. } => {
                // nothing required
            }

            VolumeConstructionRequest::Region { opts, .. } => {
                for target in &opts.target {
                    match target {
                        SocketAddr::V6(t) => {
                            if net.contains(*t.ip()) {
                                return true;
                            }
                        }

                        SocketAddr::V4(_) => {}
                    }
                }
            }

            VolumeConstructionRequest::File { .. } => {
                // nothing required
            }
        }
    }

    false
}

pub enum VolumeCookedResult {
    HardDeleted,
    Ok,
    RegionSetWithAllExpungedMembers { region_set: Vec<SocketAddrV6> },
    MultipleSomeReturned { target: SocketAddrV6 },
    TargetNotFound { target: SocketAddrV6 },
}

impl DataStore {
    pub async fn find_volumes_referencing_socket_addr(
        &self,
        opctx: &OpContext,
        address: SocketAddr,
    ) -> ListResultVec<Volume> {
        opctx.check_complex_operations_allowed()?;

        let mut volumes = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        let conn = self.pool_connection_authorized(opctx).await?;

        let needle = match address {
            SocketAddr::V4(_) => {
                return Err(Error::internal_error(&format!(
                    "find_volumes_referencing_socket_addr not ipv6: {address}"
                )));
            }

            SocketAddr::V6(addr) => addr,
        };

        while let Some(p) = paginator.next() {
            use nexus_db_schema::schema::volume::dsl;

            let haystack =
                paginated(dsl::volume, dsl::id, &p.current_pagparams())
                    .select(Volume::as_select())
                    .get_results_async::<Volume>(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;

            paginator =
                p.found_batch(&haystack, &|r| *r.id().as_untyped_uuid());

            for volume in haystack {
                let vcr: VolumeConstructionRequest =
                    match serde_json::from_str(&volume.data()) {
                        Ok(vcr) => vcr,
                        Err(e) => {
                            return Err(Error::internal_error(&format!(
                                "cannot deserialize volume data for {}: {e}",
                                volume.id(),
                            )));
                        }
                    };

                let rw_reference = region_in_vcr(&vcr, &needle)
                    .map_err(|e| Error::internal_error(&e.to_string()))?;
                let ro_reference = read_only_target_in_vcr(&vcr, &needle)
                    .map_err(|e| Error::internal_error(&e.to_string()))?;

                if rw_reference || ro_reference {
                    volumes.push(volume);
                }
            }
        }

        Ok(volumes)
    }

    pub async fn find_volumes_referencing_ipv6_addr(
        &self,
        opctx: &OpContext,
        needle: std::net::Ipv6Addr,
    ) -> ListResultVec<Volume> {
        opctx.check_complex_operations_allowed()?;

        let mut volumes = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        let conn = self.pool_connection_authorized(opctx).await?;

        while let Some(p) = paginator.next() {
            use nexus_db_schema::schema::volume::dsl;

            let haystack =
                paginated(dsl::volume, dsl::id, &p.current_pagparams())
                    .select(Volume::as_select())
                    .get_results_async::<Volume>(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;

            paginator =
                p.found_batch(&haystack, &|r| *r.id().as_untyped_uuid());

            for volume in haystack {
                let vcr: VolumeConstructionRequest =
                    match serde_json::from_str(&volume.data()) {
                        Ok(vcr) => vcr,
                        Err(e) => {
                            return Err(Error::internal_error(&format!(
                                "cannot deserialize volume data for {}: {e}",
                                volume.id(),
                            )));
                        }
                    };

                if ipv6_addr_referenced_in_vcr(&vcr, &needle) {
                    volumes.push(volume);
                }
            }
        }

        Ok(volumes)
    }

    pub async fn find_volumes_referencing_ipv6_net(
        &self,
        opctx: &OpContext,
        needle: oxnet::Ipv6Net,
    ) -> ListResultVec<Volume> {
        opctx.check_complex_operations_allowed()?;

        let mut volumes = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        let conn = self.pool_connection_authorized(opctx).await?;

        while let Some(p) = paginator.next() {
            use nexus_db_schema::schema::volume::dsl;

            let haystack =
                paginated(dsl::volume, dsl::id, &p.current_pagparams())
                    .select(Volume::as_select())
                    .get_results_async::<Volume>(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;

            paginator =
                p.found_batch(&haystack, &|r| *r.id().as_untyped_uuid());

            for volume in haystack {
                let vcr: VolumeConstructionRequest =
                    match serde_json::from_str(&volume.data()) {
                        Ok(vcr) => vcr,
                        Err(e) => {
                            return Err(Error::internal_error(&format!(
                                "cannot deserialize volume data for {}: {e}",
                                volume.id(),
                            )));
                        }
                    };

                if ipv6_net_referenced_in_vcr(&vcr, &needle) {
                    volumes.push(volume);
                }
            }
        }

        Ok(volumes)
    }

    /// Returns Some(bool) depending on if a read-only target exists in a
    /// volume, None if the volume was deleted, or an error otherwise.
    pub async fn volume_references_read_only_target(
        &self,
        volume_id: VolumeUuid,
        address: SocketAddrV6,
    ) -> LookupResult<Option<bool>> {
        let Some(volume) = self.volume_get(volume_id).await? else {
            return Ok(None);
        };

        let vcr: VolumeConstructionRequest =
            match serde_json::from_str(&volume.data()) {
                Ok(vcr) => vcr,

                Err(e) => {
                    return Err(Error::internal_error(&format!(
                        "cannot deserialize volume data for {}: {e}",
                        volume.id(),
                    )));
                }
            };

        let reference =
            read_only_target_in_vcr(&vcr, &address).map_err(|e| {
                Error::internal_error(&format!(
                    "cannot deserialize volume data for {}: {e}",
                    volume.id(),
                ))
            })?;

        Ok(Some(reference))
    }

    pub async fn volume_cooked(
        &self,
        opctx: &OpContext,
        volume_id: VolumeUuid,
    ) -> LookupResult<VolumeCookedResult> {
        let Some(volume) = self.volume_get(volume_id).await? else {
            return Ok(VolumeCookedResult::HardDeleted);
        };

        let vcr: VolumeConstructionRequest =
            match serde_json::from_str(&volume.data()) {
                Ok(vcr) => vcr,

                Err(e) => {
                    return Err(Error::internal_error(&format!(
                        "cannot deserialize volume data for {}: {e}",
                        volume.id(),
                    )));
                }
            };

        let expunged_regions: Vec<Region> = vec![
            self.find_read_only_regions_on_expunged_physical_disks(opctx)
                .await?,
            self.find_read_write_regions_on_expunged_physical_disks(opctx)
                .await?,
        ]
        .into_iter()
        .flatten()
        .collect();

        let expunged_region_snapshots: Vec<RegionSnapshot> = self
            .find_region_snapshots_on_expunged_physical_disks(opctx)
            .await?;

        let region_sets = {
            let mut result = vec![];
            region_sets(&vcr, &mut result);
            result
        };

        let conn = self.pool_connection_authorized(opctx).await?;

        #[derive(PartialEq)]
        enum Checked {
            Expunged,
            Ok,
        }

        for region_set in region_sets {
            let mut checked_region_set = Vec::with_capacity(region_set.len());

            for target in &region_set {
                let maybe_ro_usage =
                    Self::read_only_target_to_volume_resource_usage(
                        &conn, &target,
                    )
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;

                let maybe_region = Self::target_to_region(
                    &conn,
                    &target,
                    RegionType::ReadWrite,
                )
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                let check = match (maybe_ro_usage, maybe_region) {
                    (Some(usage), None) => match usage {
                        VolumeResourceUsage::ReadOnlyRegion { region_id } => {
                            if expunged_regions
                                .iter()
                                .any(|region| region.id() == region_id)
                            {
                                Checked::Expunged
                            } else {
                                Checked::Ok
                            }
                        }

                        VolumeResourceUsage::RegionSnapshot {
                            dataset_id,
                            region_id,
                            snapshot_id,
                        } => {
                            if expunged_region_snapshots.iter().any(
                                |region_snapshot| {
                                    region_snapshot.dataset_id
                                        == dataset_id.into()
                                        && region_snapshot.region_id
                                            == region_id
                                        && region_snapshot.snapshot_id
                                            == snapshot_id
                                },
                            ) {
                                Checked::Expunged
                            } else {
                                Checked::Ok
                            }
                        }
                    },

                    (None, Some(region)) => {
                        let region_id = region.id();
                        if expunged_regions
                            .iter()
                            .any(|region| region.id() == region_id)
                        {
                            Checked::Expunged
                        } else {
                            Checked::Ok
                        }
                    }

                    (Some(_), Some(_)) => {
                        // This is an error: multiple resources (read/write
                        // region, read-only region, and/or a region snapshot)
                        // share the same target addr.
                        return Ok(VolumeCookedResult::MultipleSomeReturned {
                            target: *target,
                        });
                    }

                    // volume may have been deleted after `volume_get` at
                    // beginning of function, and before grabbing the expunged
                    // resources
                    (None, None) => {
                        return Ok(VolumeCookedResult::TargetNotFound {
                            target: *target,
                        });
                    }
                };

                checked_region_set.push(check);
            }

            if checked_region_set.iter().all(|x| *x == Checked::Expunged) {
                return Ok(
                    VolumeCookedResult::RegionSetWithAllExpungedMembers {
                        region_set,
                    },
                );
            }
        }

        Ok(VolumeCookedResult::Ok)
    }
}

// Add some validation that runs only for tests
#[cfg(any(test, feature = "testing"))]
impl DataStore {
    fn volume_invariant_violated(msg: String) -> diesel::result::Error {
        diesel::result::Error::DatabaseError(
            diesel::result::DatabaseErrorKind::CheckViolation,
            Box::new(msg),
        )
    }

    /// Tests each Volume to see if invariants hold
    ///
    /// If an invariant is violated, this function returns a `CheckViolation`
    /// with the text of what invariant was violated.
    pub(crate) async fn validate_volume_invariants(
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<(), diesel::result::Error> {
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );

        while let Some(p) = paginator.next() {
            use nexus_db_schema::schema::volume::dsl;
            let haystack =
                paginated(dsl::volume, dsl::id, &p.current_pagparams())
                    .select(Volume::as_select())
                    .get_results_async::<Volume>(conn)
                    .await?;

            paginator =
                p.found_batch(&haystack, &|v| *v.id().as_untyped_uuid());

            for volume in haystack {
                Self::validate_volume_has_all_resources(&conn, &volume).await?;
                Self::validate_volume_region_sets_have_unique_targets(&volume)
                    .await?;
            }
        }

        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );

        while let Some(p) = paginator.next() {
            use nexus_db_schema::schema::region::dsl;
            let haystack =
                paginated(dsl::region, dsl::id, &p.current_pagparams())
                    .select(Region::as_select())
                    .get_results_async::<Region>(conn)
                    .await?;

            paginator = p.found_batch(&haystack, &|r| r.id());

            for region in haystack {
                Self::validate_read_only_region_has_no_snapshots(&conn, region)
                    .await?;
            }
        }

        Ok(())
    }

    /// Assert that the resources that comprise non-deleted volumes have not
    /// been prematurely deleted.
    async fn validate_volume_has_all_resources(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        volume: &Volume,
    ) -> Result<(), diesel::result::Error> {
        if volume.time_deleted.is_some() {
            // Do not need to validate resources for soft-deleted volumes
            return Ok(());
        }

        let vcr: VolumeConstructionRequest =
            serde_json::from_str(&volume.data()).unwrap();

        // validate all read/write resources still exist

        let num_read_write_subvolumes = match count_read_write_sub_volumes(&vcr)
        {
            Ok(v) => v,
            Err(e) => {
                return Err(Self::volume_invariant_violated(format!(
                    "volume {} had error: {e}",
                    volume.id(),
                )));
            }
        };

        let mut read_write_targets = Vec::with_capacity(
            REGION_REDUNDANCY_THRESHOLD * num_read_write_subvolumes,
        );

        read_write_resources_associated_with_volume(
            &vcr,
            &mut read_write_targets,
        );

        for target in read_write_targets {
            let target = match target.parse() {
                Ok(t) => t,
                Err(e) => {
                    return Err(Self::volume_invariant_violated(format!(
                        "could not parse {target}: {e}"
                    )));
                }
            };

            let maybe_region = DataStore::target_to_region(
                conn,
                &target,
                RegionType::ReadWrite,
            )
            .await?;

            let Some(_region) = maybe_region else {
                return Err(Self::volume_invariant_violated(format!(
                    "could not find resource for {target}"
                )));
            };
        }

        // validate all read-only resources still exist

        let crucible_targets = {
            let mut crucible_targets = CrucibleTargets::default();
            read_only_resources_associated_with_volume(
                &vcr,
                &mut crucible_targets,
            );
            crucible_targets
        };

        for read_only_target in &crucible_targets.read_only_targets {
            let read_only_target = read_only_target.parse().map_err(|e| {
                Self::volume_invariant_violated(format!(
                    "could not parse {read_only_target}: {e}"
                ))
            })?;

            let maybe_usage =
                DataStore::read_only_target_to_volume_resource_usage(
                    conn,
                    &read_only_target,
                )
                .await?;

            let Some(_usage) = maybe_usage else {
                return Err(Self::volume_invariant_violated(format!(
                    "could not find resource for {read_only_target}"
                )));
            };
        }

        Ok(())
    }

    /// Assert that all the region sets have three distinct targets
    async fn validate_volume_region_sets_have_unique_targets(
        volume: &Volume,
    ) -> Result<(), diesel::result::Error> {
        let vcr: VolumeConstructionRequest =
            serde_json::from_str(&volume.data()).unwrap();

        let mut parts = VecDeque::new();
        parts.push_back(&vcr);

        while let Some(part) = parts.pop_front() {
            match part {
                VolumeConstructionRequest::Volume {
                    sub_volumes,
                    read_only_parent,
                    ..
                } => {
                    for sub_volume in sub_volumes {
                        parts.push_back(sub_volume);
                    }
                    if let Some(read_only_parent) = read_only_parent {
                        parts.push_back(read_only_parent);
                    }
                }

                VolumeConstructionRequest::Url { .. } => {
                    // nothing required
                }

                VolumeConstructionRequest::Region { opts, .. } => {
                    let mut set = HashSet::new();
                    let mut count = 0;

                    for target in &opts.target {
                        set.insert(target);
                        count += 1;
                    }

                    if set.len() != count {
                        return Err(Self::volume_invariant_violated(format!(
                            "volume {} has a region set with {} unique targets",
                            volume.id(),
                            set.len(),
                        )));
                    }
                }

                VolumeConstructionRequest::File { .. } => {
                    // nothing required
                }
            }
        }

        Ok(())
    }

    /// Assert that read-only regions do not have any associated region
    /// snapshots (see associated comment in `soft_delete_volume_in_txn`)
    async fn validate_read_only_region_has_no_snapshots(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        region: Region,
    ) -> Result<(), diesel::result::Error> {
        if !region.read_only() {
            return Ok(());
        }

        use nexus_db_schema::schema::volume_resource_usage::dsl;

        let matching_usage_records: Vec<VolumeResourceUsage> =
            dsl::volume_resource_usage
                .filter(
                    dsl::usage_type.eq(VolumeResourceUsageType::RegionSnapshot),
                )
                .filter(dsl::region_snapshot_region_id.eq(region.id()))
                .select(VolumeResourceUsageRecord::as_select())
                .get_results_async(conn)
                .await?
                .into_iter()
                .map(|r| r.try_into().unwrap())
                .collect();

        if !matching_usage_records.is_empty() {
            return Err(Self::volume_invariant_violated(format!(
                "read-only region {} has matching usage records: {:?}",
                region.id(),
                matching_usage_records,
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::db::datastore::REGION_REDUNDANCY_THRESHOLD;
    use crate::db::datastore::test::TestDatasets;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_config::RegionAllocationStrategy;
    use nexus_db_model::SqlU16;
    use nexus_types::external_api::params::DiskSource;
    use omicron_common::api::external::ByteCount;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::VolumeUuid;
    use sled_agent_client::CrucibleOpts;

    // Assert that Nexus will not fail to deserialize an old version of
    // CrucibleResources that was serialized before schema update 6.0.0.
    #[tokio::test]
    async fn test_deserialize_old_crucible_resources() {
        let logctx =
            dev::test_setup_log("test_deserialize_old_crucible_resources");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let datastore = db.datastore();

        // Start with a fake volume, doesn't matter if it's empty

        let volume_id = VolumeUuid::new_v4();
        let _volume = datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: *volume_id.as_untyped_uuid(),
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        // Add old CrucibleResources json in the `resources_to_clean_up` column
        // - this was before the `deleting` column / field was added to
        // ResourceSnapshot.

        {
            use nexus_db_schema::schema::volume::dsl;

            let conn = datastore.pool_connection_unauthorized().await.unwrap();

            let resources_to_clean_up = r#"{
  "V1": {
    "datasets_and_regions": [],
    "datasets_and_snapshots": [
      [
        {
          "identity": {
            "id": "844ee8d5-7641-4b04-bca8-7521e258028a",
            "time_created": "2023-12-19T21:38:34.000000Z",
            "time_modified": "2023-12-19T21:38:34.000000Z"
          },
          "time_deleted": null,
          "rcgen": 1,
          "pool_id": "81a98506-4a97-4d92-8de5-c21f6fc71649",
          "ip": "fd00:1122:3344:101::1",
          "port": 32345,
          "kind": "Crucible",
          "size_used": 10737418240
        },
        {
          "dataset_id": "b69edd77-1b3e-4f11-978c-194a0a0137d0",
          "region_id": "8d668bf9-68cc-4387-8bc0-b4de7ef9744f",
          "snapshot_id": "f548332c-6026-4eff-8c1c-ba202cd5c834",
          "snapshot_addr": "[fd00:1122:3344:101::2]:19001",
          "volume_references": 0
        }
      ]
    ]
  }
}
"#;

            diesel::update(dsl::volume)
                .filter(dsl::id.eq(to_db_typed_uuid(volume_id)))
                .set((
                    dsl::resources_to_clean_up.eq(resources_to_clean_up),
                    dsl::time_deleted.eq(Utc::now()),
                ))
                .execute_async(&*conn)
                .await
                .unwrap();
        }

        // Soft delete the volume

        let cr = datastore.soft_delete_volume(volume_id).await.unwrap();

        // Assert the contents of the returned CrucibleResources

        let datasets_and_regions =
            datastore.regions_to_delete(&cr).await.unwrap();
        let datasets_and_snapshots =
            datastore.snapshots_to_delete(&cr).await.unwrap();

        assert!(datasets_and_regions.is_empty());
        assert_eq!(datasets_and_snapshots.len(), 1);

        let region_snapshot = &datasets_and_snapshots[0].1;

        assert_eq!(
            region_snapshot.snapshot_id,
            "f548332c-6026-4eff-8c1c-ba202cd5c834".parse::<Uuid>().unwrap()
        );
        assert_eq!(region_snapshot.deleting, false);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_volume_replace_region() {
        let logctx = dev::test_setup_log("test_volume_replace_region");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let opctx = db.opctx();
        let datastore = db.datastore();
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        let _test_datasets = TestDatasets::create(
            &opctx,
            datastore.clone(),
            REGION_REDUNDANCY_THRESHOLD,
        )
        .await;

        let volume_id = VolumeUuid::new_v4();
        let volume_to_delete_id = VolumeUuid::new_v4();

        let datasets_and_regions = datastore
            .disk_region_allocate(
                &opctx,
                volume_id,
                &DiskSource::Blank { block_size: 512.try_into().unwrap() },
                ByteCount::from_gibibytes_u32(1),
                &&RegionAllocationStrategy::RandomWithDistinctSleds {
                    seed: None,
                },
            )
            .await
            .unwrap();

        let mut region_addresses: Vec<SocketAddrV6> =
            Vec::with_capacity(datasets_and_regions.len());

        for (i, (_, region)) in datasets_and_regions.iter().enumerate() {
            // `disk_region_allocate` won't put any ports in, so add fake ones
            // here
            use nexus_db_schema::schema::region::dsl;
            diesel::update(dsl::region)
                .filter(dsl::id.eq(region.id()))
                .set(dsl::port.eq(Some::<SqlU16>((100 + i as u16).into())))
                .execute_async(&*conn)
                .await
                .unwrap();

            let address: SocketAddrV6 =
                datastore.region_addr(region.id()).await.unwrap().unwrap();

            region_addresses.push(address);
        }

        // Manually create a replacement region at the first dataset
        let replacement_region = {
            let (dataset, region) = &datasets_and_regions[0];
            let region = Region::new(
                dataset.id(),
                volume_to_delete_id,
                region.block_size().try_into().unwrap(),
                region.blocks_per_extent(),
                region.extent_count(),
                111,
                false, // read-write
            );

            use nexus_db_schema::schema::region::dsl;
            diesel::insert_into(dsl::region)
                .values(region.clone())
                .execute_async(&*conn)
                .await
                .unwrap();

            region
        };

        let replacement_region_addr: SocketAddrV6 = datastore
            .region_addr(replacement_region.id())
            .await
            .unwrap()
            .unwrap();

        let _volume = datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: *volume_id.as_untyped_uuid(),
                    block_size: 512,
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        r#gen: 1,
                        opts: CrucibleOpts {
                            id: *volume_id.as_untyped_uuid(),
                            target: vec![
                                // target to replace
                                region_addresses[0].into(),
                                region_addresses[1].into(),
                                region_addresses[2].into(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: false,
                        },
                    }],
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        // Replace one

        let volume_replace_region_result = datastore
            .volume_replace_region(
                /* target */
                db::datastore::VolumeReplacementParams {
                    volume_id,
                    region_id: datasets_and_regions[0].1.id(),
                    region_addr: region_addresses[0],
                },
                /* replacement */
                db::datastore::VolumeReplacementParams {
                    volume_id: volume_to_delete_id,
                    region_id: replacement_region.id(),
                    region_addr: replacement_region_addr,
                },
            )
            .await
            .unwrap();

        assert_eq!(volume_replace_region_result, VolumeReplaceResult::Done);

        let vcr: VolumeConstructionRequest = serde_json::from_str(
            datastore.volume_get(volume_id).await.unwrap().unwrap().data(),
        )
        .unwrap();

        // Ensure the shape of the resulting VCR
        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: *volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    r#gen: 2, // generation number bumped
                    opts: CrucibleOpts {
                        id: *volume_id.as_untyped_uuid(),
                        target: vec![
                            replacement_region_addr.into(), // replaced
                            region_addresses[1].into(),
                            region_addresses[2].into(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false,
                    },
                }],
                read_only_parent: None,
            },
        );

        // Now undo the replacement. Note volume ID is not swapped.
        let volume_replace_region_result = datastore
            .volume_replace_region(
                /* target */
                db::datastore::VolumeReplacementParams {
                    volume_id,
                    region_id: replacement_region.id(),
                    region_addr: replacement_region_addr,
                },
                /* replacement */
                db::datastore::VolumeReplacementParams {
                    volume_id: volume_to_delete_id,
                    region_id: datasets_and_regions[0].1.id(),
                    region_addr: region_addresses[0],
                },
            )
            .await
            .unwrap();

        assert_eq!(volume_replace_region_result, VolumeReplaceResult::Done);

        let vcr: VolumeConstructionRequest = serde_json::from_str(
            datastore.volume_get(volume_id).await.unwrap().unwrap().data(),
        )
        .unwrap();

        // Ensure the shape of the resulting VCR
        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: *volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    r#gen: 3, // generation number bumped
                    opts: CrucibleOpts {
                        id: *volume_id.as_untyped_uuid(),
                        target: vec![
                            region_addresses[0].into(), // back to what it was
                            region_addresses[1].into(),
                            region_addresses[2].into(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false,
                    },
                }],
                read_only_parent: None,
            },
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_volume_replace_snapshot() {
        let logctx = dev::test_setup_log("test_volume_replace_snapshot");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let opctx = db.opctx();
        let datastore = db.datastore();
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        let _test_datasets = TestDatasets::create(
            &opctx,
            datastore.clone(),
            REGION_REDUNDANCY_THRESHOLD,
        )
        .await;

        let volume_id = VolumeUuid::new_v4();
        let volume_to_delete_id = VolumeUuid::new_v4();

        let datasets_and_regions = datastore
            .disk_region_allocate(
                &opctx,
                volume_id,
                &DiskSource::Blank { block_size: 512.try_into().unwrap() },
                ByteCount::from_gibibytes_u32(1),
                &&RegionAllocationStrategy::RandomWithDistinctSleds {
                    seed: None,
                },
            )
            .await
            .unwrap();

        let mut region_addresses: Vec<SocketAddrV6> =
            Vec::with_capacity(datasets_and_regions.len());

        for (i, (_, region)) in datasets_and_regions.iter().enumerate() {
            // `disk_region_allocate` won't put any ports in, so add fake ones
            // here
            use nexus_db_schema::schema::region::dsl;
            diesel::update(dsl::region)
                .filter(dsl::id.eq(region.id()))
                .set(dsl::port.eq(Some::<SqlU16>((100 + i as u16).into())))
                .execute_async(&*conn)
                .await
                .unwrap();

            let address: SocketAddrV6 =
                datastore.region_addr(region.id()).await.unwrap().unwrap();

            region_addresses.push(address);
        }

        // Manually create a replacement region at the first dataset
        let replacement_region = {
            let (dataset, region) = &datasets_and_regions[0];
            let region = Region::new(
                dataset.id(),
                volume_to_delete_id,
                region.block_size().try_into().unwrap(),
                region.blocks_per_extent(),
                region.extent_count(),
                111,
                true, // read-only
            );

            use nexus_db_schema::schema::region::dsl;
            diesel::insert_into(dsl::region)
                .values(region.clone())
                .execute_async(&*conn)
                .await
                .unwrap();

            region
        };

        let replacement_region_addr: SocketAddrV6 = datastore
            .region_addr(replacement_region.id())
            .await
            .unwrap()
            .unwrap();

        // need to add region snapshot objects to satisfy volume create
        // transaction's search for resources

        let address_1: SocketAddrV6 =
            "[fd00:1122:3344:104::1]:400".parse().unwrap();
        let address_2: SocketAddrV6 =
            "[fd00:1122:3344:105::1]:401".parse().unwrap();
        let address_3: SocketAddrV6 =
            "[fd00:1122:3344:106::1]:402".parse().unwrap();

        let region_snapshots = [
            RegionSnapshot::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_1.to_string(),
            ),
            RegionSnapshot::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_2.to_string(),
            ),
            RegionSnapshot::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_3.to_string(),
            ),
        ];

        datastore
            .region_snapshot_create(region_snapshots[0].clone())
            .await
            .unwrap();
        datastore
            .region_snapshot_create(region_snapshots[1].clone())
            .await
            .unwrap();
        datastore
            .region_snapshot_create(region_snapshots[2].clone())
            .await
            .unwrap();

        // Insert two volumes: one with the target to replace, and one temporary
        // "volume to delete" that's blank. Validate the pre-replacement volume
        // resource usage records.

        let rop_id = Uuid::new_v4();

        datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: *volume_id.as_untyped_uuid(),
                    block_size: 512,
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        r#gen: 1,
                        opts: CrucibleOpts {
                            id: *volume_id.as_untyped_uuid(),
                            target: vec![
                                region_addresses[0].into(),
                                region_addresses[1].into(),
                                region_addresses[2].into(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: false,
                        },
                    }],
                    read_only_parent: Some(Box::new(
                        VolumeConstructionRequest::Region {
                            block_size: 512,
                            blocks_per_extent: 10,
                            extent_count: 10,
                            r#gen: 1,
                            opts: CrucibleOpts {
                                id: rop_id,
                                target: vec![
                                    // target to replace
                                    address_1.into(),
                                    address_2.into(),
                                    address_3.into(),
                                ],
                                lossy: false,
                                flush_timeout: None,
                                key: None,
                                cert_pem: None,
                                key_pem: None,
                                root_cert_pem: None,
                                control: None,
                                read_only: true,
                            },
                        },
                    )),
                },
            )
            .await
            .unwrap();

        for region_snapshot in &region_snapshots {
            let usage = datastore
                .volume_usage_records_for_resource(
                    VolumeResourceUsage::RegionSnapshot {
                        dataset_id: region_snapshot.dataset_id(),
                        region_id: region_snapshot.region_id,
                        snapshot_id: region_snapshot.snapshot_id,
                    },
                )
                .await
                .unwrap();

            assert_eq!(usage.len(), 1);
            assert_eq!(usage[0].volume_id(), volume_id);
        }

        datastore
            .volume_create(
                volume_to_delete_id,
                VolumeConstructionRequest::Volume {
                    id: *volume_to_delete_id.as_untyped_uuid(),
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: None,
                },
            )
            .await
            .unwrap();

        // `volume_create` above was called with a blank volume, so no usage
        // record will have been created for the read-only region

        let usage = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::ReadOnlyRegion {
                    region_id: replacement_region.id(),
                },
            )
            .await
            .unwrap();

        assert!(usage.is_empty());

        // Do the replacement

        let volume_replace_snapshot_result = datastore
            .volume_replace_snapshot(
                VolumeWithTarget(volume_id),
                ExistingTarget(address_1),
                ReplacementTarget(replacement_region_addr),
                VolumeToDelete(volume_to_delete_id),
            )
            .await
            .unwrap();

        assert_eq!(volume_replace_snapshot_result, VolumeReplaceResult::Done);

        // Ensure the shape of the resulting VCRs

        let vcr: VolumeConstructionRequest = serde_json::from_str(
            datastore.volume_get(volume_id).await.unwrap().unwrap().data(),
        )
        .unwrap();

        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: *volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    r#gen: 1,
                    opts: CrucibleOpts {
                        id: *volume_id.as_untyped_uuid(),
                        target: vec![
                            region_addresses[0].into(),
                            region_addresses[1].into(),
                            region_addresses[2].into(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false,
                    },
                }],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        r#gen: 1,
                        opts: CrucibleOpts {
                            id: rop_id,
                            target: vec![
                                // target replaced
                                replacement_region_addr.into(),
                                address_2.into(),
                                address_3.into(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        },
                    }
                )),
            },
        );

        let vcr: VolumeConstructionRequest = serde_json::from_str(
            datastore
                .volume_get(volume_to_delete_id)
                .await
                .unwrap()
                .unwrap()
                .data(),
        )
        .unwrap();

        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: *volume_to_delete_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 1,
                    extent_count: 1,
                    r#gen: 1,
                    opts: CrucibleOpts {
                        id: *volume_to_delete_id.as_untyped_uuid(),
                        target: vec![
                            // replaced target stashed here
                            address_1.into(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: true,
                    },
                }],
                read_only_parent: None,
            },
        );

        // Validate the post-replacement volume resource usage records

        for (i, region_snapshot) in region_snapshots.iter().enumerate() {
            let usage = datastore
                .volume_usage_records_for_resource(
                    VolumeResourceUsage::RegionSnapshot {
                        dataset_id: region_snapshot.dataset_id(),
                        region_id: region_snapshot.region_id,
                        snapshot_id: region_snapshot.snapshot_id,
                    },
                )
                .await
                .unwrap();

            assert_eq!(usage.len(), 1);

            match i {
                0 => {
                    assert_eq!(usage[0].volume_id(), volume_to_delete_id);
                }

                1 | 2 => {
                    assert_eq!(usage[0].volume_id(), volume_id);
                }

                _ => panic!("out of range"),
            }
        }

        let usage = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::ReadOnlyRegion {
                    region_id: replacement_region.id(),
                },
            )
            .await
            .unwrap();

        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].volume_id(), volume_id);

        // Now undo the replacement. Note volume ID is not swapped.

        let volume_replace_snapshot_result = datastore
            .volume_replace_snapshot(
                VolumeWithTarget(volume_id),
                ExistingTarget(replacement_region_addr),
                ReplacementTarget(address_1),
                VolumeToDelete(volume_to_delete_id),
            )
            .await
            .unwrap();

        assert_eq!(volume_replace_snapshot_result, VolumeReplaceResult::Done,);

        let vcr: VolumeConstructionRequest = serde_json::from_str(
            datastore.volume_get(volume_id).await.unwrap().unwrap().data(),
        )
        .unwrap();

        // Ensure the shape of the resulting VCR
        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: *volume_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    r#gen: 1,
                    opts: CrucibleOpts {
                        id: *volume_id.as_untyped_uuid(),
                        target: vec![
                            region_addresses[0].into(),
                            region_addresses[1].into(),
                            region_addresses[2].into(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false,
                    },
                }],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        r#gen: 1,
                        opts: CrucibleOpts {
                            id: *rop_id.as_untyped_uuid(),
                            target: vec![
                                // back to what it was
                                address_1.into(),
                                address_2.into(),
                                address_3.into(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        },
                    }
                )),
            },
        );

        let vcr: VolumeConstructionRequest = serde_json::from_str(
            datastore
                .volume_get(volume_to_delete_id)
                .await
                .unwrap()
                .unwrap()
                .data(),
        )
        .unwrap();

        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: *volume_to_delete_id.as_untyped_uuid(),
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 1,
                    extent_count: 1,
                    r#gen: 1,
                    opts: CrucibleOpts {
                        id: *volume_to_delete_id.as_untyped_uuid(),
                        target: vec![
                            // replacement stashed here
                            replacement_region_addr.into(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: true,
                    },
                }],
                read_only_parent: None,
            },
        );

        // Validate the post-post-replacement volume resource usage records

        for region_snapshot in &region_snapshots {
            let usage = datastore
                .volume_usage_records_for_resource(
                    VolumeResourceUsage::RegionSnapshot {
                        dataset_id: region_snapshot.dataset_id(),
                        region_id: region_snapshot.region_id,
                        snapshot_id: region_snapshot.snapshot_id,
                    },
                )
                .await
                .unwrap();

            assert_eq!(usage.len(), 1);
            assert_eq!(usage[0].volume_id(), volume_id);
        }

        let usage = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::ReadOnlyRegion {
                    region_id: replacement_region.id(),
                },
            )
            .await
            .unwrap();

        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].volume_id(), volume_to_delete_id);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_find_volumes_referencing_socket_addr() {
        let logctx =
            dev::test_setup_log("test_find_volumes_referencing_socket_addr");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let volume_id = VolumeUuid::new_v4();

        // need to add region snapshot objects to satisfy volume create
        // transaction's search for resources

        let address_1: SocketAddrV6 =
            "[fd00:1122:3344:104::1]:400".parse().unwrap();
        let address_2: SocketAddrV6 =
            "[fd00:1122:3344:105::1]:401".parse().unwrap();
        let address_3: SocketAddrV6 =
            "[fd00:1122:3344:106::1]:402".parse().unwrap();

        datastore
            .region_snapshot_create(RegionSnapshot::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_1.to_string(),
            ))
            .await
            .unwrap();
        datastore
            .region_snapshot_create(RegionSnapshot::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_2.to_string(),
            ))
            .await
            .unwrap();
        datastore
            .region_snapshot_create(RegionSnapshot::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_3.to_string(),
            ))
            .await
            .unwrap();

        // case where the needle is found

        datastore
            .volume_create(
                volume_id,
                VolumeConstructionRequest::Volume {
                    id: *volume_id.as_untyped_uuid(),
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: Some(Box::new(
                        VolumeConstructionRequest::Region {
                            block_size: 512,
                            blocks_per_extent: 10,
                            extent_count: 10,
                            r#gen: 1,
                            opts: CrucibleOpts {
                                id: Uuid::new_v4(),
                                target: vec![
                                    address_1.into(),
                                    address_2.into(),
                                    address_3.into(),
                                ],
                                lossy: false,
                                flush_timeout: None,
                                key: None,
                                cert_pem: None,
                                key_pem: None,
                                root_cert_pem: None,
                                control: None,
                                read_only: true,
                            },
                        },
                    )),
                },
            )
            .await
            .unwrap();

        let volumes = datastore
            .find_volumes_referencing_socket_addr(&opctx, address_1.into())
            .await
            .unwrap();

        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].id(), volume_id);

        // case where the needle is missing

        let volumes = datastore
            .find_volumes_referencing_socket_addr(
                &opctx,
                "[fd55:1122:3344:104::1]:400".parse().unwrap(),
            )
            .await
            .unwrap();

        assert!(volumes.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[test]
    fn test_read_only_target_in_vcr() {
        // read_only_target_in_vcr should find read-only targets

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: 512,
            sub_volumes: vec![],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    r#gen: 1,
                    opts: CrucibleOpts {
                        id: Uuid::new_v4(),
                        target: vec![
                            "[fd00:1122:3344:104::1]:400".parse().unwrap(),
                            "[fd00:1122:3344:105::1]:401".parse().unwrap(),
                            "[fd00:1122:3344:106::1]:402".parse().unwrap(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: true,
                    },
                },
            )),
        };

        assert!(
            read_only_target_in_vcr(
                &vcr,
                &"[fd00:1122:3344:104::1]:400".parse().unwrap(),
            )
            .unwrap()
        );

        // read_only_target_in_vcr should _not_ find read-write targets

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: 512,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 512,
                blocks_per_extent: 10,
                extent_count: 10,
                r#gen: 1,
                opts: CrucibleOpts {
                    id: Uuid::new_v4(),
                    target: vec![
                        "[fd00:1122:3344:104::1]:400".parse().unwrap(),
                        "[fd00:1122:3344:105::1]:401".parse().unwrap(),
                        "[fd00:1122:3344:106::1]:402".parse().unwrap(),
                    ],
                    lossy: false,
                    flush_timeout: None,
                    key: None,
                    cert_pem: None,
                    key_pem: None,
                    root_cert_pem: None,
                    control: None,
                    read_only: false,
                },
            }],
            read_only_parent: None,
        };

        assert!(
            !read_only_target_in_vcr(
                &vcr,
                &"[fd00:1122:3344:104::1]:400".parse().unwrap(),
            )
            .unwrap()
        );

        // read_only_target_in_vcr should bail on incorrect VCRs (currently it
        // only detects a read/write region under a read-only parent)

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: 512,
            sub_volumes: vec![],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    r#gen: 1,
                    opts: CrucibleOpts {
                        id: Uuid::new_v4(),
                        target: vec![
                            "[fd00:1122:3344:104::1]:400".parse().unwrap(),
                            "[fd00:1122:3344:105::1]:401".parse().unwrap(),
                            "[fd00:1122:3344:106::1]:402".parse().unwrap(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false, // invalid!
                    },
                },
            )),
        };

        read_only_target_in_vcr(
            &vcr,
            &"[fd00:1122:3344:104::1]:400".parse().unwrap(),
        )
        .unwrap_err();
    }

    #[test]
    fn test_replace_read_only_target_in_vcr() {
        // replace_read_only_target_in_vcr should perform a replacement in a
        // read-only parent

        let volume_id = Uuid::new_v4();

        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: 512,
            sub_volumes: vec![],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    r#gen: 1,
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            "[fd00:1122:3344:104::1]:400".parse().unwrap(),
                            "[fd00:1122:3344:105::1]:401".parse().unwrap(),
                            "[fd00:1122:3344:106::1]:402".parse().unwrap(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: true,
                    },
                },
            )),
        };

        let old_target =
            ExistingTarget("[fd00:1122:3344:105::1]:401".parse().unwrap());
        let new_target =
            ReplacementTarget("[fd99:1122:3344:105::1]:12345".parse().unwrap());

        let (new_vcr, replacements) =
            replace_read_only_target_in_vcr(&vcr, old_target, new_target)
                .unwrap();

        assert_eq!(replacements, 1);
        assert_eq!(
            &new_vcr,
            &VolumeConstructionRequest::Volume {
                id: volume_id,
                block_size: 512,
                sub_volumes: vec![],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        r#gen: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                "[fd00:1122:3344:104::1]:400".parse().unwrap(),
                                new_target.0.into(),
                                "[fd00:1122:3344:106::1]:402".parse().unwrap(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        }
                    }
                ))
            }
        );

        // replace_read_only_target_in_vcr should perform a replacement in a
        // read-only parent in a sub-volume

        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: 512,
            sub_volumes: vec![VolumeConstructionRequest::Volume {
                id: volume_id,
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    r#gen: 1,
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            "[fd55:1122:3344:204::1]:1000".parse().unwrap(),
                            "[fd55:1122:3344:205::1]:1001".parse().unwrap(),
                            "[fd55:1122:3344:206::1]:1002".parse().unwrap(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false,
                    },
                }],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        r#gen: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                "[fd33:1122:3344:304::1]:2000".parse().unwrap(),
                                "[fd33:1122:3344:305::1]:2001".parse().unwrap(),
                                "[fd33:1122:3344:306::1]:2002".parse().unwrap(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        },
                    },
                )),
            }],
            read_only_parent: Some(Box::new(
                VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    r#gen: 1,
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            "[fd00:1122:3344:104::1]:400".parse().unwrap(),
                            "[fd00:1122:3344:105::1]:401".parse().unwrap(),
                            "[fd00:1122:3344:106::1]:402".parse().unwrap(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: true,
                    },
                },
            )),
        };

        let old_target =
            ExistingTarget("[fd33:1122:3344:306::1]:2002".parse().unwrap());
        let new_target =
            ReplacementTarget("[fd99:1122:3344:105::1]:12345".parse().unwrap());

        let (new_vcr, replacements) =
            replace_read_only_target_in_vcr(&vcr, old_target, new_target)
                .unwrap();

        assert_eq!(replacements, 1);
        assert_eq!(
            &new_vcr,
            &VolumeConstructionRequest::Volume {
                id: volume_id,
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Volume {
                    id: volume_id,
                    block_size: 512,
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        r#gen: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                "[fd55:1122:3344:204::1]:1000".parse().unwrap(),
                                "[fd55:1122:3344:205::1]:1001".parse().unwrap(),
                                "[fd55:1122:3344:206::1]:1002".parse().unwrap(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: false,
                        }
                    }],
                    read_only_parent: Some(Box::new(
                        VolumeConstructionRequest::Region {
                            block_size: 512,
                            blocks_per_extent: 10,
                            extent_count: 10,
                            r#gen: 1,
                            opts: CrucibleOpts {
                                id: volume_id,
                                target: vec![
                                    "[fd33:1122:3344:304::1]:2000"
                                        .parse()
                                        .unwrap(),
                                    "[fd33:1122:3344:305::1]:2001"
                                        .parse()
                                        .unwrap(),
                                    new_target.0.into(),
                                ],
                                lossy: false,
                                flush_timeout: None,
                                key: None,
                                cert_pem: None,
                                key_pem: None,
                                root_cert_pem: None,
                                control: None,
                                read_only: true,
                            }
                        }
                    )),
                }],
                read_only_parent: Some(Box::new(
                    VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        r#gen: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                "[fd00:1122:3344:104::1]:400".parse().unwrap(),
                                "[fd00:1122:3344:105::1]:401".parse().unwrap(),
                                "[fd00:1122:3344:106::1]:402".parse().unwrap(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: true,
                        }
                    }
                ))
            }
        );

        // replace_read_only_target_in_vcr should perform multiple replacements
        // if necessary (even if this is dubious!) - the caller will decide if
        // this should be legal or not

        let rop = VolumeConstructionRequest::Region {
            block_size: 512,
            blocks_per_extent: 10,
            extent_count: 10,
            r#gen: 1,
            opts: CrucibleOpts {
                id: volume_id,
                target: vec![
                    "[fd33:1122:3344:304::1]:2000".parse().unwrap(),
                    "[fd33:1122:3344:305::1]:2001".parse().unwrap(),
                    "[fd33:1122:3344:306::1]:2002".parse().unwrap(),
                ],
                lossy: false,
                flush_timeout: None,
                key: None,
                cert_pem: None,
                key_pem: None,
                root_cert_pem: None,
                control: None,
                read_only: true,
            },
        };

        let vcr = VolumeConstructionRequest::Volume {
            id: volume_id,
            block_size: 512,
            sub_volumes: vec![VolumeConstructionRequest::Volume {
                id: volume_id,
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    r#gen: 1,
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            "[fd55:1122:3344:204::1]:1000".parse().unwrap(),
                            "[fd55:1122:3344:205::1]:1001".parse().unwrap(),
                            "[fd55:1122:3344:206::1]:1002".parse().unwrap(),
                        ],
                        lossy: false,
                        flush_timeout: None,
                        key: None,
                        cert_pem: None,
                        key_pem: None,
                        root_cert_pem: None,
                        control: None,
                        read_only: false,
                    },
                }],
                read_only_parent: Some(Box::new(rop.clone())),
            }],
            read_only_parent: Some(Box::new(rop)),
        };

        let old_target =
            ExistingTarget("[fd33:1122:3344:304::1]:2000".parse().unwrap());
        let new_target =
            ReplacementTarget("[fd99:1122:3344:105::1]:12345".parse().unwrap());

        let (new_vcr, replacements) =
            replace_read_only_target_in_vcr(&vcr, old_target, new_target)
                .unwrap();

        assert_eq!(replacements, 2);

        let rop = VolumeConstructionRequest::Region {
            block_size: 512,
            blocks_per_extent: 10,
            extent_count: 10,
            r#gen: 1,
            opts: CrucibleOpts {
                id: volume_id,
                target: vec![
                    new_target.0.into(),
                    "[fd33:1122:3344:305::1]:2001".parse().unwrap(),
                    "[fd33:1122:3344:306::1]:2002".parse().unwrap(),
                ],
                lossy: false,
                flush_timeout: None,
                key: None,
                cert_pem: None,
                key_pem: None,
                root_cert_pem: None,
                control: None,
                read_only: true,
            },
        };

        assert_eq!(
            &new_vcr,
            &VolumeConstructionRequest::Volume {
                id: volume_id,
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Volume {
                    id: volume_id,
                    block_size: 512,
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        r#gen: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                "[fd55:1122:3344:204::1]:1000".parse().unwrap(),
                                "[fd55:1122:3344:205::1]:1001".parse().unwrap(),
                                "[fd55:1122:3344:206::1]:1002".parse().unwrap(),
                            ],
                            lossy: false,
                            flush_timeout: None,
                            key: None,
                            cert_pem: None,
                            key_pem: None,
                            root_cert_pem: None,
                            control: None,
                            read_only: false,
                        }
                    }],
                    read_only_parent: Some(Box::new(rop.clone())),
                }],
                read_only_parent: Some(Box::new(rop)),
            }
        );
    }

    /// Assert that there are no "deleted" r/w regions found when the associated
    /// volume hasn't been created yet.
    #[tokio::test]
    async fn test_no_find_deleted_region_for_no_volume() {
        let logctx =
            dev::test_setup_log("test_no_find_deleted_region_for_no_volume");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let _test_datasets = TestDatasets::create(
            &opctx,
            datastore.clone(),
            REGION_REDUNDANCY_THRESHOLD,
        )
        .await;

        let volume_id = VolumeUuid::new_v4();

        // Assert that allocating regions without creating the volume does not
        // cause them to be returned as "deleted" regions, as this can cause
        // sagas that allocate regions to race with the volume delete saga and
        // cause premature region deletion.

        let _datasets_and_regions = datastore
            .disk_region_allocate(
                &opctx,
                volume_id,
                &DiskSource::Blank { block_size: 512.try_into().unwrap() },
                ByteCount::from_gibibytes_u32(1),
                &&RegionAllocationStrategy::RandomWithDistinctSleds {
                    seed: None,
                },
            )
            .await
            .unwrap();

        let deleted_regions = datastore
            .find_deleted_volume_regions()
            .await
            .expect("find_deleted_volume_regions");

        assert!(deleted_regions.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
