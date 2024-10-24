// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Volume`]s.

use super::DataStore;
use crate::db;
use crate::db::datastore::OpContext;
use crate::db::datastore::RunnableQuery;
use crate::db::datastore::REGION_REDUNDANCY_THRESHOLD;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::identity::Asset;
use crate::db::model::Dataset;
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
use crate::db::pagination::paginated;
use crate::db::pagination::Paginator;
use crate::db::DbConnection;
use crate::transaction_retry::OptionalError;
use anyhow::bail;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::OptionalExtension;
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
use omicron_uuid_kinds::DownstairsKind;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::TypedUuid;
use omicron_uuid_kinds::UpstairsKind;
use omicron_uuid_kinds::UpstairsRepairKind;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use sled_agent_client::types::VolumeConstructionRequest;
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
    CouldNotFindResource(String),
}

impl DataStore {
    async fn volume_create_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<VolumeCreationError>,
        volume: Volume,
        crucible_targets: CrucibleTargets,
    ) -> Result<Volume, diesel::result::Error> {
        use db::schema::volume::dsl;

        let maybe_volume: Option<Volume> = dsl::volume
            .filter(dsl::id.eq(volume.id()))
            .select(Volume::as_select())
            .first_async(conn)
            .await
            .optional()?;

        // If the volume existed already, return it and do not increase usage
        // counts.
        if let Some(volume) = maybe_volume {
            return Ok(volume);
        }

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
        use db::schema::volume_resource_usage::dsl as ru_dsl;

        for read_only_target in crucible_targets.read_only_targets {
            let sub_err = OptionalError::new();

            let maybe_usage = Self::read_only_target_to_volume_resource_usage(
                conn,
                &sub_err,
                &read_only_target,
            )
            .await
            .map_err(|e| {
                if let Some(sub_err) = sub_err.take() {
                    err.bail(VolumeCreationError::AddressParseError(sub_err))
                } else {
                    e
                }
            })?;

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
        err: &OptionalError<AddrParseError>,
        target: &str,
        read_only: bool,
    ) -> Result<Option<Region>, diesel::result::Error> {
        let address: SocketAddrV6 = target.parse().map_err(|e| err.bail(e))?;
        let ip: db::model::Ipv6Addr = address.ip().into();

        use db::schema::dataset::dsl as dataset_dsl;
        use db::schema::region::dsl as region_dsl;

        dataset_dsl::dataset
            .inner_join(
                region_dsl::region
                    .on(region_dsl::dataset_id.eq(dataset_dsl::id)),
            )
            .filter(dataset_dsl::ip.eq(ip))
            .filter(
                region_dsl::port
                    .eq(Some::<db::model::SqlU16>(address.port().into())),
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
        err: &OptionalError<AddrParseError>,
        read_only_target: &str,
    ) -> Result<Option<VolumeResourceUsage>, diesel::result::Error> {
        // Easy case: it's a region snapshot, and we can match by the snapshot
        // address directly

        let maybe_region_snapshot = {
            use db::schema::region_snapshot::dsl;
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
                dataset_id: region_snapshot.dataset_id,
                region_id: region_snapshot.region_id,
                snapshot_id: region_snapshot.snapshot_id,
            }));
        }

        // Less easy case: it's a read-only region, and we have to match by
        // dataset ip and region port

        let maybe_region = Self::target_to_region(
            conn,
            err,
            read_only_target,
            true, // read-only
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

    pub async fn volume_create(&self, volume: Volume) -> CreateResult<Volume> {
        // Grab all the targets that the volume construction request references.
        // Do this outside the transaction, as the data inside volume doesn't
        // change and this would simply add to the transaction time.
        let crucible_targets = {
            let vcr: VolumeConstructionRequest =
                serde_json::from_str(&volume.data()).map_err(|e| {
                    Error::internal_error(&format!(
                        "serde_json::from_str error in volume_create: {}",
                        e
                    ))
                })?;

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
                let volume = volume.clone();
                async move {
                    Self::volume_create_txn(
                        &conn,
                        err,
                        volume,
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

    async fn volume_get_impl(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        volume_id: Uuid,
    ) -> Result<Option<Volume>, diesel::result::Error> {
        use db::schema::volume::dsl;
        dsl::volume
            .filter(dsl::id.eq(volume_id))
            .select(Volume::as_select())
            .first_async::<Volume>(conn)
            .await
            .optional()
    }

    /// Return a `Option<Volume>` based on id, even if it's soft deleted.
    pub async fn volume_get(
        &self,
        volume_id: Uuid,
    ) -> LookupResult<Option<Volume>> {
        let conn = self.pool_connection_unauthorized().await?;
        Self::volume_get_impl(&conn, volume_id)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Delete the volume if it exists. If it was already deleted, this is a
    /// no-op.
    pub async fn volume_hard_delete(&self, volume_id: Uuid) -> DeleteResult {
        use db::schema::volume::dsl;

        diesel::delete(dsl::volume)
            .filter(dsl::id.eq(volume_id))
            .execute_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map(|_| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    fn volume_usage_records_for_resource_query(
        resource: VolumeResourceUsage,
    ) -> impl RunnableQuery<VolumeResourceUsageRecord> {
        use db::schema::volume_resource_usage::dsl;

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
                .filter(dsl::region_snapshot_dataset_id.eq(dataset_id))
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
        from_volume_id: Uuid,
        to_volume_id: Uuid,
    ) -> Result<(), diesel::result::Error> {
        use db::schema::volume_resource_usage::dsl;

        match resource {
            VolumeResourceUsage::ReadOnlyRegion { region_id } => {
                let updated_rows = diesel::update(dsl::volume_resource_usage)
                    .filter(
                        dsl::usage_type
                            .eq(VolumeResourceUsageType::ReadOnlyRegion),
                    )
                    .filter(dsl::region_id.eq(region_id))
                    .filter(dsl::volume_id.eq(from_volume_id))
                    .set(dsl::volume_id.eq(to_volume_id))
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
                    .filter(dsl::region_snapshot_dataset_id.eq(dataset_id))
                    .filter(dsl::region_snapshot_region_id.eq(region_id))
                    .filter(dsl::region_snapshot_snapshot_id.eq(snapshot_id))
                    .filter(dsl::volume_id.eq(from_volume_id))
                    .set(dsl::volume_id.eq(to_volume_id))
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
                            return Err(VolumeGetError::CheckoutConditionFailed(
                                String::from("Non-read-only Volume Checkout for use Copy!")
                            ));
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
                        Err(VolumeGetError::CheckoutConditionFailed(
                            format!(
                                "InstanceStart {}: instance {} is undergoing migration",
                                vmm_id,
                                instance.id(),
                            )
                        ))
                    }

                    (None, None) => {
                        Err(VolumeGetError::CheckoutConditionFailed(
                            format!(
                                "InstanceStart {}: instance {} has no propolis ids",
                                vmm_id,
                                instance.id(),
                            )
                        ))
                    }

                    (Some(propolis_id), None) => {
                        if propolis_id != vmm_id.into_untyped_uuid() {
                            return Err(VolumeGetError::CheckoutConditionFailed(
                                format!(
                                    "InstanceStart {}: instance {} propolis id {} mismatch",
                                    vmm_id,
                                    instance.id(),
                                    propolis_id,
                                )
                            ));
                        }

                        Ok(())
                    }

                    (None, Some(dst_propolis_id)) => {
                        Err(VolumeGetError::CheckoutConditionFailed(
                            format!(
                                "InstanceStart {}: instance {} has no propolis id but dst propolis id {}",
                                vmm_id,
                                instance.id(),
                                dst_propolis_id,
                            )
                        ))
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
                        if propolis_id != vmm_id.into_untyped_uuid() || dst_propolis_id != target_vmm_id.into_untyped_uuid() {
                            return Err(VolumeGetError::CheckoutConditionFailed(
                                format!(
                                    "InstanceMigrate {} {}: instance {} propolis id mismatches {} {}",
                                    vmm_id,
                                    target_vmm_id,
                                    instance.id(),
                                    propolis_id,
                                    dst_propolis_id,
                                )
                            ));
                        }

                        Ok(())
                    }

                    (None, None) => {
                        Err(VolumeGetError::CheckoutConditionFailed(
                            format!(
                                "InstanceMigrate {} {}: instance {} has no propolis ids",
                                vmm_id,
                                target_vmm_id,
                                instance.id(),
                            )
                        ))
                    }

                    (Some(propolis_id), None) => {
                        // XXX is this right?
                        if propolis_id != vmm_id.into_untyped_uuid() {
                            return Err(VolumeGetError::CheckoutConditionFailed(
                                format!(
                                    "InstanceMigrate {} {}: instance {} propolis id {} mismatch",
                                    vmm_id,
                                    target_vmm_id,
                                    instance.id(),
                                    propolis_id,
                                )
                            ));
                        }

                        Ok(())
                    }

                    (None, Some(dst_propolis_id)) => {
                        Err(VolumeGetError::CheckoutConditionFailed(
                            format!(
                                "InstanceMigrate {} {}: instance {} has no propolis id but dst propolis id {}",
                                vmm_id,
                                target_vmm_id,
                                instance.id(),
                                dst_propolis_id,
                            )
                        ))
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
                            "Pantry: instance {} backing disk {} does not exist?",
                            attach_instance_id,
                            disk.id(),
                        )
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

    /// Checkout a copy of the Volume from the database.
    /// This action (getting a copy) will increase the generation number
    /// of Volumes of the VolumeConstructionRequest::Volume type that have
    /// sub_volumes of the VolumeConstructionRequest::Region type.
    /// This generation number increase is required for Crucible to support
    /// crash consistency.
    pub async fn volume_checkout(
        &self,
        volume_id: Uuid,
        reason: VolumeCheckoutReason,
    ) -> LookupResult<Volume> {
        use db::schema::volume::dsl;

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
                    // Grab the volume in question.
                    let volume = dsl::volume
                        .filter(dsl::id.eq(volume_id))
                        .select(Volume::as_select())
                        .get_result_async(&conn)
                        .await?;

                    // Turn the volume.data into the VolumeConstructionRequest
                    let vcr: VolumeConstructionRequest =
                        serde_json::from_str(volume.data()).map_err(|e| {
                            err.bail(VolumeGetError::SerdeError(e))
                        })?;

                    // The VolumeConstructionRequest resulting from this checkout will have its
                    // generation numbers bumped, and as result will (if it has non-read-only
                    // sub-volumes) take over from previous read/write activations when sent to a
                    // place that will `construct` a new Volume. Depending on the checkout reason,
                    // prevent creating multiple read/write Upstairs acting on the same Volume,
                    // except where the take over is intended.

                    let (maybe_disk, maybe_instance) = {
                        use db::schema::instance::dsl as instance_dsl;
                        use db::schema::disk::dsl as disk_dsl;

                        let maybe_disk: Option<Disk> = disk_dsl::disk
                            .filter(disk_dsl::time_deleted.is_null())
                            .filter(disk_dsl::volume_id.eq(volume_id))
                            .select(Disk::as_select())
                            .get_result_async(&conn)
                            .await
                            .optional()?;

                        let maybe_instance: Option<Instance> = if let Some(disk) = &maybe_disk {
                            if let Some(attach_instance_id) = disk.runtime().attach_instance_id {
                                instance_dsl::instance
                                    .filter(instance_dsl::time_deleted.is_null())
                                    .filter(instance_dsl::id.eq(attach_instance_id))
                                    .select(Instance::as_select())
                                    .get_result_async(&conn)
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
                    .await {
                        return Err(err.bail(e));
                    }

                    // Look to see if the VCR is a Volume type, and if so, look at
                    // its sub_volumes. If they are of type Region, then we need
                    // to update their generation numbers and record that update
                    // back to the database. We return to the caller whatever the
                    // original volume data was we pulled from the database.
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
                                        gen,
                                    } => {
                                        update_needed = true;
                                        new_sv.push(
                                            VolumeConstructionRequest::Region {
                                                block_size,
                                                blocks_per_extent,
                                                extent_count,
                                                opts,
                                                gen: gen + 1,
                                            },
                                        );
                                    }
                                    _ => {
                                        new_sv.push(sv);
                                    }
                                }
                            }

                            // Only update the volume data if we found the type
                            // of volume that needed it.
                            if update_needed {
                                // Create a new VCR and fill in the contents
                                // from what the original volume had, but with our
                                // updated sub_volume records.
                                let new_vcr = VolumeConstructionRequest::Volume {
                                    id,
                                    block_size,
                                    sub_volumes: new_sv,
                                    read_only_parent,
                                };

                                let new_volume_data = serde_json::to_string(
                                    &new_vcr,
                                )
                                .map_err(|e| {
                                    err.bail(VolumeGetError::SerdeError(e))
                                })?;

                                // Update the original volume_id with the new
                                // volume.data.
                                use db::schema::volume::dsl as volume_dsl;
                                let num_updated =
                                    diesel::update(volume_dsl::volume)
                                        .filter(volume_dsl::id.eq(volume_id))
                                        .set(volume_dsl::data.eq(new_volume_data))
                                        .execute_async(&conn)
                                        .await?;

                                // This should update just one row.  If it does
                                // not, then something is terribly wrong in the
                                // database.
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
                            gen: _,
                        } => {
                            // We don't support a pure Region VCR at the volume
                            // level in the database, so this choice should
                            // never be encountered, but I want to know if it is.
                            panic!("Region not supported as a top level volume");
                        }
                        VolumeConstructionRequest::File {
                            id: _,
                            block_size: _,
                            path: _,
                        }
                        | VolumeConstructionRequest::Url {
                            id: _,
                            block_size: _,
                            url: _,
                        } => {}
                    }
                    Ok(volume)
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
                            return Error::internal_error(&format!("Transaction error: {}", err));
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
                        // UUID is recorded in the region table accordingly. It is
                        // an error to make a copy of a volume construction request
                        // that references non-read-only Regions.
                        bail!(
                            "only one Volume can reference a Region non-read-only!"
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
        volume_id: Uuid,
        reason: VolumeCheckoutReason,
    ) -> CreateResult<Volume> {
        let volume = self.volume_checkout(volume_id, reason).await?;

        let vcr: sled_agent_client::types::VolumeConstructionRequest =
            serde_json::from_str(volume.data())?;

        let randomized_vcr = serde_json::to_string(
            &Self::randomize_ids(&vcr)
                .map_err(|e| Error::internal_error(&e.to_string()))?,
        )?;

        self.volume_create(db::model::Volume::new(
            Uuid::new_v4(),
            randomized_vcr,
        ))
        .await
    }

    /// Find regions for deleted volumes that do not have associated region
    /// snapshots and are not being used by any other non-deleted volumes, and
    /// return them for garbage collection
    pub async fn find_deleted_volume_regions(
        &self,
    ) -> ListResultVec<(Dataset, Region, Option<Volume>)> {
        let conn = self.pool_connection_unauthorized().await?;
        self.transaction_retry_wrapper("find_deleted_volume_regions")
            .transaction(&conn, |conn| async move {
                Self::find_deleted_volume_regions_txn(&conn).await
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    async fn find_deleted_volume_regions_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Vec<(Dataset, Region, Option<Volume>)>, diesel::result::Error>
    {
        use db::schema::dataset::dsl as dataset_dsl;
        use db::schema::region::dsl as region_dsl;
        use db::schema::region_snapshot::dsl;
        use db::schema::volume::dsl as volume_dsl;

        // Find all read-write regions (read-only region cleanup is taken care
        // of in soft_delete_volume_txn!) and their associated datasets
        let unfiltered_deleted_regions = region_dsl::region
            .filter(region_dsl::read_only.eq(false))
            // the volume may be hard deleted, so use a left join here
            .left_join(
                volume_dsl::volume.on(region_dsl::volume_id.eq(volume_dsl::id)),
            )
            .inner_join(
                dataset_dsl::dataset
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
                Dataset::as_select(),
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

        for (dataset, region, region_snapshot, volume) in
            unfiltered_deleted_regions
        {
            // only operate on soft or hard deleted volumes
            let deleted = match &volume {
                Some(volume) => volume.time_deleted.is_some(),
                None => true,
            };

            if !deleted {
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

            deleted_regions.push((dataset, region, volume));
        }

        Ok(deleted_regions)
    }

    pub async fn read_only_resources_associated_with_volume(
        &self,
        volume_id: Uuid,
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
enum SoftDeleteTransactionError {
    #[error("Serde error decreasing Crucible resources: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Updated {0} database rows in {1}, expected 1")]
    UnexpectedDatabaseUpdate(usize, String),

    // XXX is this an error? delete volume anyway, else we're stuck?
    #[error("Could not match resource to {0}")]
    CouldNotFindResource(String),

    #[error("Address parsing error during Volume soft-delete: {0}")]
    AddressParseError(#[from] AddrParseError),
}

impl DataStore {
    // See comment for `soft_delete_volume`
    async fn soft_delete_volume_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        volume_id: Uuid,
        err: OptionalError<SoftDeleteTransactionError>,
    ) -> Result<CrucibleResources, diesel::result::Error> {
        // Grab the volume, and check if the volume was already soft-deleted.
        // We have to guard against the case where this function is called
        // multiple times, and that is done by soft-deleting the volume during
        // the transaction, and returning the previously serialized list of
        // resources to clean up.
        let volume = {
            use db::schema::volume::dsl;

            let volume = dsl::volume
                .filter(dsl::id.eq(volume_id))
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

        let num_read_write_subvolumes = count_read_write_sub_volumes(&vcr);

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
            let sub_err = OptionalError::new();

            let maybe_region = Self::target_to_region(
                conn, &sub_err, &target, false, // read-write
            )
            .await
            .map_err(|e| {
                if let Some(sub_err) = sub_err.take() {
                    err.bail(SoftDeleteTransactionError::AddressParseError(
                        sub_err,
                    ))
                } else {
                    e
                }
            })?;

            let Some(region) = maybe_region else {
                return Err(err.bail(
                    SoftDeleteTransactionError::CouldNotFindResource(format!(
                        "could not find resource for {target}"
                    )),
                ));
            };

            // Filter out regions that have any region-snapshots
            let region_snapshot_count: i64 = {
                use db::schema::region_snapshot::dsl;
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
            use db::schema::volume_resource_usage::dsl as ru_dsl;

            let sub_err = OptionalError::new();

            let maybe_usage = Self::read_only_target_to_volume_resource_usage(
                conn,
                &sub_err,
                read_only_target,
            )
            .await
            .map_err(|e| {
                if let Some(sub_err) = sub_err.take() {
                    err.bail(SoftDeleteTransactionError::AddressParseError(
                        sub_err,
                    ))
                } else {
                    e
                }
            })?;

            let Some(usage) = maybe_usage else {
                return Err(err.bail(
                    SoftDeleteTransactionError::CouldNotFindResource(format!(
                        "could not find resource for {read_only_target}"
                    )),
                ));
            };

            // For each read-only resource, remove the associated volume
            // resource usage record for this volume. Only return a resource for
            // deletion if no more associated volume usage records are found.
            match usage {
                VolumeResourceUsage::ReadOnlyRegion { region_id } => {
                    let updated_rows =
                        diesel::delete(ru_dsl::volume_resource_usage)
                            .filter(ru_dsl::volume_id.eq(volume_id))
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
                            SoftDeleteTransactionError::UnexpectedDatabaseUpdate(
                                updated_rows,
                                "volume_resource_usage (region)".into(),
                            )
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
                        use db::schema::region::dsl;
                        let updated_rows = diesel::update(dsl::region)
                            .filter(dsl::id.eq(region_id))
                            .filter(dsl::read_only.eq(true))
                            .filter(dsl::deleting.eq(false))
                            .set(dsl::deleting.eq(true))
                            .execute_async(conn)
                            .await?;

                        if updated_rows != 1 {
                            return Err(err.bail(
                                SoftDeleteTransactionError::UnexpectedDatabaseUpdate(
                                    updated_rows,
                                    "setting deleting (region)".into(),
                                )
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
                            .filter(ru_dsl::volume_id.eq(volume_id))
                            .filter(
                                ru_dsl::usage_type.eq(
                                    VolumeResourceUsageType::RegionSnapshot,
                                ),
                            )
                            .filter(
                                ru_dsl::region_snapshot_dataset_id
                                    .eq(Some(dataset_id)),
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
                            SoftDeleteTransactionError::UnexpectedDatabaseUpdate(
                                updated_rows,
                                "volume_resource_usage \
                                (region_snapshot)".into(),
                            )
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
                                    .eq(Some(dataset_id)),
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
                        {
                            use db::schema::region_snapshot::dsl;
                            let updated_rows =
                                diesel::update(dsl::region_snapshot)
                                    .filter(dsl::dataset_id.eq(dataset_id))
                                    .filter(dsl::region_id.eq(region_id))
                                    .filter(dsl::snapshot_id.eq(snapshot_id))
                                    .filter(
                                        dsl::snapshot_addr
                                            .eq(read_only_target.clone()),
                                    )
                                    .filter(dsl::deleting.eq(false))
                                    .set(dsl::deleting.eq(true))
                                    .execute_async(conn)
                                    .await?;

                            if updated_rows != 1 {
                                return Err(err.bail(
                                    SoftDeleteTransactionError::UnexpectedDatabaseUpdate(
                                        updated_rows,
                                        "setting deleting (region snapshot)".into(),
                                    )
                                ));
                            }
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
            use db::schema::volume::dsl;
            let updated_rows = diesel::update(dsl::volume)
                .filter(dsl::id.eq(volume_id))
                .set((
                    dsl::time_deleted.eq(Utc::now()),
                    dsl::resources_to_clean_up.eq(Some(serialized_resources)),
                ))
                .execute_async(conn)
                .await?;

            if updated_rows != 1 {
                return Err(err.bail(
                    SoftDeleteTransactionError::UnexpectedDatabaseUpdate(
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
        volume_id: Uuid,
    ) -> Result<CrucibleResources, Error> {
        let err = OptionalError::new();
        let conn = self.pool_connection_unauthorized().await?;
        self.transaction_retry_wrapper("soft_delete_volume")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    Self::soft_delete_volume_txn(&conn, volume_id, err).await
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

    // Here we remove the read only parent from volume_id, and attach it
    // to temp_volume_id.
    //
    // As this is part of a saga, it will be able to handle being replayed
    // If we call this twice, any work done the first time through should
    // not happen again, or be undone.
    pub async fn volume_remove_rop(
        &self,
        volume_id: Uuid,
        temp_volume_id: Uuid,
    ) -> Result<bool, Error> {
        #[derive(Debug, thiserror::Error)]
        enum RemoveReadOnlyParentError {
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
                    // Grab the volume in question. If the volume record was
                    // already deleted then we can just return.
                    let volume = {
                        use db::schema::volume::dsl;

                        let volume = dsl::volume
                            .filter(dsl::id.eq(volume_id))
                            .select(Volume::as_select())
                            .get_result_async(&conn)
                            .await
                            .optional()?;

                        let volume = if let Some(v) = volume {
                            v
                        } else {
                            // the volume does not exist, nothing to do.
                            return Ok(false);
                        };

                        if volume.time_deleted.is_some() {
                            // this volume is deleted, so let whatever is
                            // deleting it clean it up.
                            return Ok(false);
                        } else {
                            // A volume record exists, and was not deleted, we
                            // can attempt to remove its read_only_parent.
                            volume
                        }
                    };

                    // If a read_only_parent exists, remove it from volume_id,
                    // and attach it to temp_volume_id.
                    let vcr: VolumeConstructionRequest =
                        serde_json::from_str(
                            volume.data()
                        )
                        .map_err(|e| {
                            err.bail(
                                RemoveReadOnlyParentError::SerdeError(
                                    e,
                                )
                            )
                        })?;

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
                                // Create a new VCR and fill in the contents
                                // from what the original volume had.
                                let new_vcr = VolumeConstructionRequest::Volume {
                                    id,
                                    block_size,
                                    sub_volumes,
                                    read_only_parent: None,
                                };

                                let new_volume_data =
                                    serde_json::to_string(
                                        &new_vcr
                                    )
                                    .map_err(|e| {
                                        err.bail(RemoveReadOnlyParentError::SerdeError(
                                            e,
                                        ))
                                    })?;

                                // Update the original volume_id with the new
                                // volume.data.
                                use db::schema::volume::dsl as volume_dsl;
                                let num_updated = diesel::update(volume_dsl::volume)
                                    .filter(volume_dsl::id.eq(volume_id))
                                    .set(volume_dsl::data.eq(new_volume_data))
                                    .execute_async(&conn)
                                    .await?;

                                // This should update just one row.  If it does
                                // not, then something is terribly wrong in the
                                // database.
                                if num_updated != 1 {
                                    return Err(err.bail(RemoveReadOnlyParentError::UnexpectedDatabaseUpdate(num_updated, 1)));
                                }

                                // Make a new VCR, with the information from
                                // our temp_volume_id, but the read_only_parent
                                // from the original volume.
                                let rop_vcr = VolumeConstructionRequest::Volume {
                                    id: temp_volume_id,
                                    block_size,
                                    sub_volumes: vec![],
                                    read_only_parent,
                                };

                                let rop_volume_data =
                                    serde_json::to_string(
                                        &rop_vcr
                                    )
                                    .map_err(|e| {
                                        err.bail(RemoveReadOnlyParentError::SerdeError(
                                            e,
                                        ))
                                    })?;

                                // Update the temp_volume_id with the volume
                                // data that contains the read_only_parent.
                                let num_updated =
                                    diesel::update(volume_dsl::volume)
                                        .filter(volume_dsl::id.eq(temp_volume_id))
                                        .filter(volume_dsl::time_deleted.is_null())
                                        .set(volume_dsl::data.eq(rop_volume_data))
                                        .execute_async(&conn)
                                        .await?;

                                if num_updated != 1 {
                                    return Err(err.bail(RemoveReadOnlyParentError::UnexpectedDatabaseUpdate(num_updated, 1)));
                                }

                                // Update the volume resource usage record for
                                // every read-only resource in the ROP
                                let crucible_targets = {
                                    let mut crucible_targets = CrucibleTargets::default();
                                    read_only_resources_associated_with_volume(
                                        &rop_vcr,
                                        &mut crucible_targets,
                                    );
                                    crucible_targets
                                };

                                for read_only_target in crucible_targets.read_only_targets {
                                    let sub_err = OptionalError::new();

                                    let maybe_usage = Self::read_only_target_to_volume_resource_usage(
                                        &conn,
                                        &sub_err,
                                        &read_only_target,
                                    )
                                    .await
                                    .map_err(|e| {
                                        if let Some(sub_err) = sub_err.take() {
                                            err.bail(RemoveReadOnlyParentError::AddressParseError(sub_err))
                                        } else {
                                            e
                                        }
                                    })?;

                                    let Some(usage) = maybe_usage else {
                                        return Err(err.bail(
                                            RemoveReadOnlyParentError::CouldNotFindResource(format!(
                                                "could not find resource for {read_only_target}"
                                            )),
                                        ));
                                    };

                                    Self::swap_volume_usage_records_for_resources(
                                        &conn,
                                        usage,
                                        volume_id,
                                        temp_volume_id,
                                    )
                                    .await
                                    .map_err(|e| {
                                        err.bail_retryable_or_else(e, |e| {
                                            RemoveReadOnlyParentError::Public(
                                                public_error_from_diesel(
                                                    e,
                                                    ErrorHandler::Server,
                                                )
                                            )
                                        })
                                    })?;
                                }

                                // After read-only parent removal, validate
                                // invariants for all volumes
                                #[cfg(any(test, feature = "testing"))]
                                Self::validate_volume_invariants(&conn).await?;

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
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return Error::internal_error(&format!("Transaction error: {}", err));
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    /// Return all the read-write regions in a volume whose target address
    /// matches the argument dataset's.
    pub async fn get_dataset_rw_regions_in_volume(
        &self,
        opctx: &OpContext,
        dataset_id: Uuid,
        volume_id: Uuid,
    ) -> LookupResult<Vec<SocketAddrV6>> {
        let conn = self.pool_connection_authorized(opctx).await?;

        let dataset = {
            use db::schema::dataset::dsl;

            dsl::dataset
                .filter(dsl::id.eq(dataset_id))
                .select(Dataset::as_select())
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

        let Some(address) = dataset.address() else {
            return Err(Error::internal_error(
                "Crucible Dataset missing IP address",
            ));
        };

        find_matching_rw_regions_in_volume(&vcr, address.ip(), &mut targets)
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
        use db::schema::upstairs_repair_notification::dsl;

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
                            "existing repair type for id {} does not match {:?}!",
                            record.repair_id,
                            record.repair_type,
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

    /// Record Upstairs repair progress
    pub async fn upstairs_repair_progress(
        &self,
        opctx: &OpContext,
        upstairs_id: TypedUuid<UpstairsKind>,
        repair_id: TypedUuid<UpstairsRepairKind>,
        repair_progress: RepairProgress,
    ) -> Result<(), Error> {
        use db::schema::upstairs_repair_notification::dsl as notification_dsl;
        use db::schema::upstairs_repair_progress::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper("upstairs_repair_progress")
            .transaction(&conn, |conn| {
                let repair_progress = repair_progress.clone();
                let err = err.clone();

                async move {
                    // Check that there is a repair id for the upstairs id
                    let matching_repair: Option<UpstairsRepairNotification> =
                        notification_dsl::upstairs_repair_notification
                            .filter(notification_dsl::repair_id.eq(nexus_db_model::to_db_typed_uuid(repair_id)))
                            .filter(notification_dsl::upstairs_id.eq(nexus_db_model::to_db_typed_uuid(upstairs_id)))
                            .filter(notification_dsl::notification_type.eq(UpstairsRepairNotificationType::Started))
                            .get_result_async(&conn)
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

    /// Record when a Downstairs client is requested to stop, and why
    pub async fn downstairs_client_stop_request_notification(
        &self,
        opctx: &OpContext,
        upstairs_id: TypedUuid<UpstairsKind>,
        downstairs_id: TypedUuid<DownstairsKind>,
        downstairs_client_stop_request: DownstairsClientStopRequest,
    ) -> Result<(), Error> {
        use db::schema::downstairs_client_stop_request_notification::dsl;

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
        use db::schema::downstairs_client_stopped_notification::dsl;

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

        use db::schema::upstairs_repair_notification::dsl;

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

        use db::schema::upstairs_repair_notification::dsl;

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

        use db::schema::upstairs_repair_progress::dsl;

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
    pub async fn volume_deleted(&self, volume_id: Uuid) -> Result<bool, Error> {
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
    pub datasets_and_regions: Vec<(Dataset, Region)>,
    pub datasets_and_snapshots: Vec<(Dataset, RegionSnapshot)>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CrucibleResourcesV2 {
    pub datasets_and_regions: Vec<(Dataset, Region)>,
    pub snapshots_to_delete: Vec<RegionSnapshot>,
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RegionSnapshotV3 {
    dataset: Uuid,
    region: Uuid,
    snapshot: Uuid,
}

#[derive(Debug, Default, Serialize, Deserialize)]
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
    /// the Dataset they belong to.
    pub async fn regions_to_delete(
        &self,
        crucible_resources: &CrucibleResources,
    ) -> LookupResult<Vec<(Dataset, Region)>> {
        let conn = self.pool_connection_unauthorized().await?;

        match crucible_resources {
            CrucibleResources::V1(crucible_resources) => {
                Ok(crucible_resources.datasets_and_regions.clone())
            }

            CrucibleResources::V2(crucible_resources) => {
                Ok(crucible_resources.datasets_and_regions.clone())
            }

            CrucibleResources::V3(crucible_resources) => {
                use db::schema::dataset::dsl as dataset_dsl;
                use db::schema::region::dsl as region_dsl;

                region_dsl::region
                    .filter(
                        region_dsl::id
                            .eq_any(crucible_resources.regions.clone()),
                    )
                    .inner_join(
                        dataset_dsl::dataset
                            .on(region_dsl::dataset_id.eq(dataset_dsl::id)),
                    )
                    .select((Dataset::as_select(), Region::as_select()))
                    .get_results_async::<(Dataset, Region)>(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })
            }
        }
    }

    /// For a CrucibleResources object, return the RegionSnapshots to delete, as
    /// well as the Dataset they belong to.
    pub async fn snapshots_to_delete(
        &self,
        crucible_resources: &CrucibleResources,
    ) -> LookupResult<Vec<(Dataset, RegionSnapshot)>> {
        let conn = self.pool_connection_unauthorized().await?;

        match crucible_resources {
            CrucibleResources::V1(crucible_resources) => {
                Ok(crucible_resources.datasets_and_snapshots.clone())
            }

            CrucibleResources::V2(crucible_resources) => {
                use db::schema::dataset::dsl;

                let mut result: Vec<_> = Vec::with_capacity(
                    crucible_resources.snapshots_to_delete.len(),
                );

                for snapshots_to_delete in
                    &crucible_resources.snapshots_to_delete
                {
                    let maybe_dataset = dsl::dataset
                        .filter(dsl::id.eq(snapshots_to_delete.dataset_id))
                        .select(Dataset::as_select())
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
                use db::schema::dataset::dsl as dataset_dsl;
                use db::schema::region_snapshot::dsl;

                let mut datasets_and_snapshots = Vec::with_capacity(
                    crucible_resources.region_snapshots.len(),
                );

                for region_snapshots in &crucible_resources.region_snapshots {
                    let maybe_tuple = dsl::region_snapshot
                        .filter(dsl::dataset_id.eq(region_snapshots.dataset))
                        .filter(dsl::region_id.eq(region_snapshots.region))
                        .filter(dsl::snapshot_id.eq(region_snapshots.snapshot))
                        .inner_join(
                            dataset_dsl::dataset
                                .on(dsl::dataset_id.eq(dataset_dsl::id)),
                        )
                        .select((
                            Dataset::as_select(),
                            RegionSnapshot::as_select(),
                        ))
                        .first_async::<(Dataset, RegionSnapshot)>(&*conn)
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
                    let parsed_target: SocketAddrV6 = target.parse()?;
                    if parsed_target == *region {
                        region_found = true;
                        break;
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
                    let parsed_target: SocketAddrV6 = target.parse()?;
                    if parsed_target == *read_only_target && opts.read_only {
                        return Ok(true);
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

pub struct VolumeReplacementParams {
    pub volume_id: Uuid,
    pub region_id: Uuid,
    pub region_addr: SocketAddrV6,
}

// types for volume_replace_snapshot and replace_read_only_target_in_vcr
// parameters

#[derive(Debug, Clone, Copy)]
pub struct VolumeWithTarget(pub Uuid);

// Note: it would be easier to pass around strings, but comparison could fail
// due to formatting issues, so pass around SocketAddrV6 here
#[derive(Debug, Clone, Copy)]
pub struct ExistingTarget(pub SocketAddrV6);

#[derive(Debug, Clone, Copy)]
pub struct ReplacementTarget(pub SocketAddrV6);

#[derive(Debug, Clone, Copy)]
pub struct VolumeToDelete(pub Uuid);

// The result type returned from both `volume_replace_region` and
// `volume_replace_snapshot`
#[must_use]
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum VolumeReplaceResult {
    // based on the VCRs, seems like the replacement already happened
    AlreadyHappened,

    // this call performed the replacement
    Done,

    // the "existing" volume was deleted
    ExistingVolumeDeleted,
}

impl DataStore {
    /// Replace a read-write region in a Volume with a new region.
    pub async fn volume_replace_region(
        &self,
        existing: VolumeReplacementParams,
        replacement: VolumeReplacementParams,
    ) -> Result<VolumeReplaceResult, Error> {
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

        #[derive(Debug, thiserror::Error)]
        enum VolumeReplaceRegionError {
            #[error("Error from Volume region replacement: {0}")]
            Public(Error),

            #[error("Serde error during Volume region replacement: {0}")]
            SerdeError(#[from] serde_json::Error),

            #[error("Region replacement error: {0}")]
            RegionReplacementError(#[from] anyhow::Error),
        }
        let err = OptionalError::new();

        let conn = self.pool_connection_unauthorized().await?;
        self.transaction_retry_wrapper("volume_replace_region")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    // Grab the old volume first
                    let maybe_old_volume = {
                        volume_dsl::volume
                            .filter(volume_dsl::id.eq(existing.volume_id))
                            .select(Volume::as_select())
                            .first_async::<Volume>(&conn)
                            .await
                            .optional()
                            .map_err(|e| {
                                err.bail_retryable_or_else(e, |e| {
                                    VolumeReplaceRegionError::Public(
                                        public_error_from_diesel(
                                            e,
                                            ErrorHandler::Server,
                                        )
                                    )
                                })
                            })?
                    };

                    let old_volume = if let Some(old_volume) = maybe_old_volume {
                        old_volume
                    } else {
                        // Existing volume was hard-deleted, so return here. We
                        // can't perform the region replacement now, and this
                        // will short-circuit the rest of the process.

                        return Ok(VolumeReplaceResult::ExistingVolumeDeleted);
                    };

                    if old_volume.time_deleted.is_some() {
                        // Existing volume was soft-deleted, so return here for
                        // the same reason: the region replacement process
                        // should be short-circuited now.
                        return Ok(VolumeReplaceResult::ExistingVolumeDeleted);
                    }

                    let old_vcr: VolumeConstructionRequest =
                        match serde_json::from_str(&old_volume.data()) {
                            Ok(vcr) => vcr,
                            Err(e) => {
                                return Err(err.bail(VolumeReplaceRegionError::SerdeError(e)));
                            },
                        };

                    // Does it look like this replacement already happened?
                    let old_region_in_vcr = match region_in_vcr(&old_vcr, &existing.region_addr) {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(err.bail(VolumeReplaceRegionError::RegionReplacementError(e)));
                        },
                    };
                    let new_region_in_vcr = match region_in_vcr(&old_vcr, &replacement.region_addr) {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(err.bail(VolumeReplaceRegionError::RegionReplacementError(e)));
                        },
                    };

                    if !old_region_in_vcr && new_region_in_vcr {
                        // It does seem like the replacement happened
                        return Ok(VolumeReplaceResult::AlreadyHappened);
                    }

                    use db::schema::region::dsl as region_dsl;
                    use db::schema::volume::dsl as volume_dsl;

                    // Set the existing region's volume id to the replacement's
                    // volume id
                    diesel::update(region_dsl::region)
                        .filter(region_dsl::id.eq(existing.region_id))
                        .set(region_dsl::volume_id.eq(replacement.volume_id))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                VolumeReplaceRegionError::Public(
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::Server,
                                    )
                                )
                            })
                        })?;

                    // Set the replacement region's volume id to the existing's
                    // volume id
                    diesel::update(region_dsl::region)
                        .filter(region_dsl::id.eq(replacement.region_id))
                        .set(region_dsl::volume_id.eq(existing.volume_id))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                VolumeReplaceRegionError::Public(
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::Server,
                                    )
                                )
                            })
                        })?;

                    // Update the existing volume's construction request to
                    // replace the existing region's SocketAddrV6 with the
                    // replacement region's

                    // Copy the old volume's VCR, changing out the old region
                    // for the new.
                    let new_vcr = match replace_region_in_vcr(
                        &old_vcr,
                        existing.region_addr,
                        replacement.region_addr,
                    ) {
                        Ok(new_vcr) => new_vcr,
                        Err(e) => {
                            return Err(err.bail(
                                VolumeReplaceRegionError::RegionReplacementError(e)
                            ));
                        }
                    };

                    let new_volume_data = serde_json::to_string(
                        &new_vcr,
                    )
                    .map_err(|e| {
                        err.bail(VolumeReplaceRegionError::SerdeError(e))
                    })?;

                    // Update the existing volume's data
                    diesel::update(volume_dsl::volume)
                        .filter(volume_dsl::id.eq(existing.volume_id))
                        .set(volume_dsl::data.eq(new_volume_data))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                VolumeReplaceRegionError::Public(
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::Server,
                                    )
                                )
                            })
                        })?;

                    // After region replacement, validate invariants for all
                    // volumes
                    #[cfg(any(test, feature = "testing"))]
                    Self::validate_volume_invariants(&conn).await?;

                    Ok(VolumeReplaceResult::Done)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        VolumeReplaceRegionError::Public(e) => e,

                        VolumeReplaceRegionError::SerdeError(_) => {
                            Error::internal_error(&err.to_string())
                        }

                        VolumeReplaceRegionError::RegionReplacementError(_) => {
                            Error::internal_error(&err.to_string())
                        }
                    }
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
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
        #[derive(Debug, thiserror::Error)]
        enum VolumeReplaceSnapshotError {
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
        let err = OptionalError::new();

        let conn = self.pool_connection_unauthorized().await?;
        self.transaction_retry_wrapper("volume_replace_snapshot")
            .transaction(&conn, |conn| {
                let err = err.clone();

                async move {
                    use db::schema::volume::dsl as volume_dsl;
                    use db::schema::volume_resource_usage::dsl as ru_dsl;

                    // Grab the old volume first
                    let maybe_old_volume = {
                        volume_dsl::volume
                            .filter(volume_dsl::id.eq(volume_id.0))
                            .select(Volume::as_select())
                            .first_async::<Volume>(&conn)
                            .await
                            .optional()
                            .map_err(|e| {
                                err.bail_retryable_or_else(e, |e| {
                                    VolumeReplaceSnapshotError::Public(
                                        public_error_from_diesel(
                                            e,
                                            ErrorHandler::Server,
                                        )
                                    )
                                })
                            })?
                    };

                    let old_volume = if let Some(old_volume) = maybe_old_volume {
                        old_volume
                    } else {
                        // Existing volume was hard-deleted, so return here. We
                        // can't perform the region replacement now, and this
                        // will short-circuit the rest of the process.

                        return Ok(VolumeReplaceResult::ExistingVolumeDeleted);
                    };

                    if old_volume.time_deleted.is_some() {
                        // Existing volume was soft-deleted, so return here for
                        // the same reason: the region replacement process
                        // should be short-circuited now.
                        return Ok(VolumeReplaceResult::ExistingVolumeDeleted);
                    }

                    let old_vcr: VolumeConstructionRequest =
                        match serde_json::from_str(&old_volume.data()) {
                            Ok(vcr) => vcr,
                            Err(e) => {
                                return Err(err.bail(
                                    VolumeReplaceSnapshotError::SerdeError(e)
                                ));
                            },
                        };

                    // Does it look like this replacement already happened?
                    let old_target_in_vcr = match read_only_target_in_vcr(&old_vcr, &existing.0) {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(err.bail(
                                VolumeReplaceSnapshotError::SnapshotReplacementError(e)
                            ));
                        },
                    };

                    let new_target_in_vcr = match read_only_target_in_vcr(&old_vcr, &replacement.0) {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(err.bail(
                                VolumeReplaceSnapshotError::SnapshotReplacementError(e)
                            ));
                        },
                    };

                    if !old_target_in_vcr && new_target_in_vcr {
                        // It does seem like the replacement happened
                        return Ok(VolumeReplaceResult::AlreadyHappened);
                    }

                    // Update the existing volume's construction request to
                    // replace the existing target's SocketAddrV6 with the
                    // replacement target's

                    // Copy the old volume's VCR, changing out the old target
                    // for the new.
                    let (new_vcr, replacements) = match replace_read_only_target_in_vcr(
                        &old_vcr,
                        existing,
                        replacement,
                    ) {
                        Ok(new_vcr) => new_vcr,
                        Err(e) => {
                            return Err(err.bail(
                                VolumeReplaceSnapshotError::SnapshotReplacementError(e)
                            ));
                        }
                    };

                    // Expect that this only happened once. If it happened
                    // multiple times, question everything: how would a snapshot
                    // be used twice?!

                    if replacements != 1 {
                        return Err(err.bail(
                            VolumeReplaceSnapshotError::UnexpectedReplacedTargets(
                                replacements, 1,
                            )
                        ));
                    }

                    let new_volume_data = serde_json::to_string(
                        &new_vcr,
                    )
                    .map_err(|e| {
                        err.bail(VolumeReplaceSnapshotError::SerdeError(e))
                    })?;

                    // Update the existing volume's data
                    diesel::update(volume_dsl::volume)
                        .filter(volume_dsl::id.eq(volume_id.0))
                        .set(volume_dsl::data.eq(new_volume_data))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                VolumeReplaceSnapshotError::Public(
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::Server,
                                    )
                                )
                            })
                        })?;

                    // Make a new VCR that will stash the target to delete. The
                    // values here don't matter, just that it gets fed into the
                    // volume_delete machinery later.
                    let vcr = VolumeConstructionRequest::Volume {
                        id: volume_to_delete_id.0,
                        block_size: 512,
                        sub_volumes: vec![
                            VolumeConstructionRequest::Region {
                                block_size: 512,
                                blocks_per_extent: 1,
                                extent_count: 1,
                                gen: 1,
                                opts: sled_agent_client::types::CrucibleOpts {
                                    id: volume_to_delete_id.0,
                                    target: vec![
                                        existing.0.to_string(),
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
                        ],
                        read_only_parent: None,
                    };

                    let volume_data = serde_json::to_string(&vcr)
                        .map_err(|e| {
                            err.bail(VolumeReplaceSnapshotError::SerdeError(e))
                        })?;

                    // Update the volume to delete data
                    let num_updated =
                        diesel::update(volume_dsl::volume)
                            .filter(volume_dsl::id.eq(volume_to_delete_id.0))
                            .filter(volume_dsl::time_deleted.is_null())
                            .set(volume_dsl::data.eq(volume_data))
                            .execute_async(&conn)
                            .await?;

                    if num_updated != 1 {
                        return Err(err.bail(
                            VolumeReplaceSnapshotError::UnexpectedDatabaseUpdate(
                                num_updated, 1,
                            )
                        ));
                    }

                    // Update the appropriate volume resource usage records - it
                    // could either be a read-only region or a region snapshot,
                    // so determine what it is first

                    let sub_err = OptionalError::new();
                    let maybe_existing_usage = Self::read_only_target_to_volume_resource_usage(
                        &conn,
                        &sub_err,
                        &existing.0.to_string(),
                    )
                    .await
                    .map_err(|e| if let Some(sub_err) = sub_err.take() {
                            err.bail(VolumeReplaceSnapshotError::AddressParseError(
                                sub_err
                            ))
                        } else {
                            e
                        }
                    )?;

                    let Some(existing_usage) = maybe_existing_usage else {
                        return Err(err.bail(
                            VolumeReplaceSnapshotError::CouldNotFindResource(
                                format!(
                                    "could not find resource for {}",
                                    existing.0,
                                )
                            ))
                        );
                    };

                    // The "existing" target moved into the volume to delete

                    Self::swap_volume_usage_records_for_resources(
                        &conn,
                        existing_usage,
                        volume_id.0,
                        volume_to_delete_id.0,
                    )
                    .await
                    .map_err(|e| {
                        err.bail_retryable_or_else(e, |e| {
                            VolumeReplaceSnapshotError::Public(
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Server,
                                )
                            )
                        })
                    })?;

                    let sub_err = OptionalError::new();
                    let maybe_replacement_usage =
                        Self::read_only_target_to_volume_resource_usage(
                            &conn,
                            &sub_err,
                            &replacement.0.to_string(),
                        )
                        .await
                        .map_err(|e| if let Some(sub_err) = sub_err.take() {
                                err.bail(VolumeReplaceSnapshotError::AddressParseError(
                                    sub_err
                                ))
                            } else {
                                e
                            }
                        )?;

                    let Some(replacement_usage) = maybe_replacement_usage else {
                        return Err(err.bail(
                            VolumeReplaceSnapshotError::CouldNotFindResource(
                                format!(
                                    "could not find resource for {}",
                                    existing.0,
                                )
                            ))
                        );
                    };

                    // This function may be called with a replacement volume
                    // that is completely blank, to be filled in later by this
                    // function. `volume_create` will have been called but will
                    // not have added any volume resource usage records, because
                    // it was blank!
                    //
                    // The indention leaving this transaction is that the
                    // correct volume resource usage records exist, so if this
                    // is the case, create a new record.
                    //
                    // If the replacement volume usage records exist, then
                    // perform a swap instead.

                    let existing_replacement_volume_usage_records =
                        Self::volume_usage_records_for_resource_query(
                            replacement_usage.clone(),
                        )
                        .load_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                VolumeReplaceSnapshotError::Public(
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::Server,
                                    )
                                )
                            })
                        })?
                        // TODO be smart enough to .filter the above query
                        .into_iter()
                        .filter(|record| record.volume_id == volume_to_delete_id.0)
                        .count();

                    // The "replacement" target moved into the volume

                    if existing_replacement_volume_usage_records == 0 {
                        // No matching record
                        let new_record = VolumeResourceUsageRecord::new(
                            volume_id.0,
                            replacement_usage,
                        );

                        diesel::insert_into(ru_dsl::volume_resource_usage)
                            .values(new_record)
                            .execute_async(&conn)
                            .await
                            .map_err(|e| {
                                err.bail_retryable_or_else(e, |e| {
                                    VolumeReplaceSnapshotError::Public(
                                        public_error_from_diesel(
                                            e,
                                            ErrorHandler::Server,
                                        )
                                    )
                                })
                            })?;
                    } else if existing_replacement_volume_usage_records == 1 {
                        // One matching record: perform swap
                        Self::swap_volume_usage_records_for_resources(
                            &conn,
                            replacement_usage,
                            volume_to_delete_id.0,
                            volume_id.0,
                        )
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                VolumeReplaceSnapshotError::Public(
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::Server,
                                    )
                                )
                            })
                        })?;
                    } else {
                        // More than one matching record!
                        return Err(err.bail(
                            VolumeReplaceSnapshotError::MultipleResourceUsageRecords(
                                format!("{replacement_usage:?}")
                            )
                        ));
                    }

                    // After region snapshot replacement, validate invariants
                    // for all volumes
                    #[cfg(any(test, feature = "testing"))]
                    Self::validate_volume_invariants(&conn).await?;

                    Ok(VolumeReplaceResult::Done)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        VolumeReplaceSnapshotError::Public(e) => e,

                        VolumeReplaceSnapshotError::SerdeError(_) |
                        VolumeReplaceSnapshotError::SnapshotReplacementError(_) |
                        VolumeReplaceSnapshotError::UnexpectedReplacedTargets(_, _) |
                        VolumeReplaceSnapshotError::UnexpectedDatabaseUpdate(_, _) |
                        VolumeReplaceSnapshotError::AddressParseError(_) |
                        VolumeReplaceSnapshotError::CouldNotFindResource(_) |
                        VolumeReplaceSnapshotError::MultipleResourceUsageRecords(_) => {
                            Error::internal_error(&err.to_string())
                        }
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
                        crucible_targets.read_only_targets.push(target.clone());
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
                        targets.push(target.clone());
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
pub fn count_read_write_sub_volumes(vcr: &VolumeConstructionRequest) -> usize {
    match vcr {
        VolumeConstructionRequest::Volume { sub_volumes, .. } => {
            sub_volumes.len()
        }

        VolumeConstructionRequest::Url { .. } => 0,

        VolumeConstructionRequest::Region { .. } => {
            // We don't support a pure Region VCR at the volume
            // level in the database, so this choice should
            // never be encountered.
            panic!("Region not supported as a top level volume");
        }

        VolumeConstructionRequest::File { .. } => 0,
    }
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
            panic!("Region not supported as a top level volume");
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

            VolumeConstructionRequest::Region { opts, gen, .. } => {
                for target in &mut opts.target {
                    let parsed_target: SocketAddrV6 = target.parse()?;
                    if parsed_target == old_region {
                        *target = new_region.to_string();
                        old_region_found = true;
                    }
                }

                // Bump generation number, otherwise update will be rejected
                *gen = *gen + 1;
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
                    let parsed_target: SocketAddrV6 = target.parse()?;
                    if parsed_target == old_target.0 && opts.read_only {
                        *target = new_target.0.to_string();
                        replacements += 1;
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
                        let parsed_target: SocketAddrV6 = target.parse()?;
                        if parsed_target.ip() == ip {
                            matched_targets.push(parsed_target);
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

impl DataStore {
    pub async fn find_volumes_referencing_socket_addr(
        &self,
        opctx: &OpContext,
        address: SocketAddr,
    ) -> ListResultVec<Volume> {
        opctx.check_complex_operations_allowed()?;

        let mut volumes = Vec::new();
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        let conn = self.pool_connection_authorized(opctx).await?;

        let needle = address.to_string();

        while let Some(p) = paginator.next() {
            use db::schema::volume::dsl;

            let haystack =
                paginated(dsl::volume, dsl::id, &p.current_pagparams())
                    .select(Volume::as_select())
                    .get_results_async::<Volume>(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;

            paginator = p.found_batch(&haystack, &|r| r.id());

            for volume in haystack {
                if volume.data().contains(&needle) {
                    volumes.push(volume);
                }
            }
        }

        Ok(volumes)
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
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);

        while let Some(p) = paginator.next() {
            use db::schema::volume::dsl;
            let haystack =
                paginated(dsl::volume, dsl::id, &p.current_pagparams())
                    .select(Volume::as_select())
                    .get_results_async::<Volume>(conn)
                    .await?;

            paginator = p.found_batch(&haystack, &|v| v.id());

            for volume in haystack {
                Self::validate_volume_has_all_resources(&conn, volume).await?;
            }
        }

        let mut paginator = Paginator::new(SQL_BATCH_SIZE);

        while let Some(p) = paginator.next() {
            use db::schema::region::dsl;
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
        volume: Volume,
    ) -> Result<(), diesel::result::Error> {
        if volume.time_deleted.is_some() {
            // Do not need to validate resources for soft-deleted volumes
            return Ok(());
        }

        let vcr: VolumeConstructionRequest =
            serde_json::from_str(&volume.data()).unwrap();

        // validate all read/write resources still exist

        let num_read_write_subvolumes = count_read_write_sub_volumes(&vcr);

        let mut read_write_targets = Vec::with_capacity(
            REGION_REDUNDANCY_THRESHOLD * num_read_write_subvolumes,
        );

        read_write_resources_associated_with_volume(
            &vcr,
            &mut read_write_targets,
        );

        for target in read_write_targets {
            let sub_err = OptionalError::new();

            let maybe_region = DataStore::target_to_region(
                conn, &sub_err, &target, false, // read-write
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
            let sub_err = OptionalError::new();

            let maybe_usage =
                DataStore::read_only_target_to_volume_resource_usage(
                    conn,
                    &sub_err,
                    read_only_target,
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

    /// Assert that read-only regions do not have any associated region
    /// snapshots (see associated comment in `soft_delete_volume_txn`)
    async fn validate_read_only_region_has_no_snapshots(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        region: Region,
    ) -> Result<(), diesel::result::Error> {
        if !region.read_only() {
            return Ok(());
        }

        use db::schema::volume_resource_usage::dsl;

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

    use crate::db::datastore::test::TestDatasets;
    use crate::db::datastore::test_utils::datastore_test;
    use crate::db::datastore::REGION_REDUNDANCY_THRESHOLD;
    use nexus_config::RegionAllocationStrategy;
    use nexus_db_model::SqlU16;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::external_api::params::DiskSource;
    use omicron_common::api::external::ByteCount;
    use omicron_test_utils::dev;
    use sled_agent_client::types::CrucibleOpts;

    // Assert that Nexus will not fail to deserialize an old version of
    // CrucibleResources that was serialized before schema update 6.0.0.
    #[tokio::test]
    async fn test_deserialize_old_crucible_resources() {
        let logctx =
            dev::test_setup_log("test_deserialize_old_crucible_resources");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let (_opctx, db_datastore) = datastore_test(&logctx, &db).await;

        // Start with a fake volume, doesn't matter if it's empty

        let volume_id = Uuid::new_v4();
        let _volume = db_datastore
            .volume_create(nexus_db_model::Volume::new(
                volume_id,
                serde_json::to_string(&VolumeConstructionRequest::Volume {
                    id: volume_id,
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: None,
                })
                .unwrap(),
            ))
            .await
            .unwrap();

        // Add old CrucibleResources json in the `resources_to_clean_up` column -
        // this was before the `deleting` column / field was added to
        // ResourceSnapshot.

        {
            use db::schema::volume::dsl;

            let conn =
                db_datastore.pool_connection_unauthorized().await.unwrap();

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
                .filter(dsl::id.eq(volume_id))
                .set((
                    dsl::resources_to_clean_up.eq(resources_to_clean_up),
                    dsl::time_deleted.eq(Utc::now()),
                ))
                .execute_async(&*conn)
                .await
                .unwrap();
        }

        // Soft delete the volume

        let cr = db_datastore.soft_delete_volume(volume_id).await.unwrap();

        // Assert the contents of the returned CrucibleResources

        let datasets_and_regions =
            db_datastore.regions_to_delete(&cr).await.unwrap();
        let datasets_and_snapshots =
            db_datastore.snapshots_to_delete(&cr).await.unwrap();

        assert!(datasets_and_regions.is_empty());
        assert_eq!(datasets_and_snapshots.len(), 1);

        let region_snapshot = &datasets_and_snapshots[0].1;

        assert_eq!(
            region_snapshot.snapshot_id,
            "f548332c-6026-4eff-8c1c-ba202cd5c834".parse().unwrap()
        );
        assert_eq!(region_snapshot.deleting, false);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_volume_replace_region() {
        let logctx = dev::test_setup_log("test_volume_replace_region");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let (opctx, db_datastore) = datastore_test(&logctx, &db).await;
        let conn = db_datastore.pool_connection_unauthorized().await.unwrap();

        let _test_datasets = TestDatasets::create(
            &opctx,
            db_datastore.clone(),
            REGION_REDUNDANCY_THRESHOLD,
        )
        .await;

        let volume_id = Uuid::new_v4();
        let volume_to_delete_id = Uuid::new_v4();

        let datasets_and_regions = db_datastore
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

        let mut region_addresses: Vec<String> =
            Vec::with_capacity(datasets_and_regions.len());

        for (i, (_, region)) in datasets_and_regions.iter().enumerate() {
            // `disk_region_allocate` won't put any ports in, so add fake ones
            // here
            use nexus_db_model::schema::region::dsl;
            diesel::update(dsl::region)
                .filter(dsl::id.eq(region.id()))
                .set(dsl::port.eq(Some::<SqlU16>((100 + i as u16).into())))
                .execute_async(&*conn)
                .await
                .unwrap();

            let address: SocketAddrV6 =
                db_datastore.region_addr(region.id()).await.unwrap().unwrap();

            region_addresses.push(address.to_string());
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

            use nexus_db_model::schema::region::dsl;
            diesel::insert_into(dsl::region)
                .values(region.clone())
                .execute_async(&*conn)
                .await
                .unwrap();

            region
        };

        let replacement_region_addr: SocketAddrV6 = db_datastore
            .region_addr(replacement_region.id())
            .await
            .unwrap()
            .unwrap();

        let _volume = db_datastore
            .volume_create(nexus_db_model::Volume::new(
                volume_id,
                serde_json::to_string(&VolumeConstructionRequest::Volume {
                    id: volume_id,
                    block_size: 512,
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        gen: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                region_addresses[0].clone(), // target to replace
                                region_addresses[1].clone(),
                                region_addresses[2].clone(),
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
                })
                .unwrap(),
            ))
            .await
            .unwrap();

        // Replace one

        let volume_replace_region_result = db_datastore
            .volume_replace_region(
                /* target */
                db::datastore::VolumeReplacementParams {
                    volume_id,
                    region_id: datasets_and_regions[0].1.id(),
                    region_addr: region_addresses[0].parse().unwrap(),
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
            db_datastore.volume_get(volume_id).await.unwrap().unwrap().data(),
        )
        .unwrap();

        // Ensure the shape of the resulting VCR
        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: volume_id,
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    gen: 2, // generation number bumped
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            replacement_region_addr.to_string(), // replaced
                            region_addresses[1].clone(),
                            region_addresses[2].clone(),
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
        let volume_replace_region_result = db_datastore
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
                    region_addr: region_addresses[0].parse().unwrap(),
                },
            )
            .await
            .unwrap();

        assert_eq!(volume_replace_region_result, VolumeReplaceResult::Done);

        let vcr: VolumeConstructionRequest = serde_json::from_str(
            db_datastore.volume_get(volume_id).await.unwrap().unwrap().data(),
        )
        .unwrap();

        // Ensure the shape of the resulting VCR
        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: volume_id,
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    gen: 3, // generation number bumped
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            region_addresses[0].clone(), // back to what it was
                            region_addresses[1].clone(),
                            region_addresses[2].clone(),
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

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_volume_replace_snapshot() {
        let logctx = dev::test_setup_log("test_volume_replace_snapshot");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let (opctx, db_datastore) = datastore_test(&logctx, &db).await;
        let conn = db_datastore.pool_connection_for_tests().await.unwrap();

        let _test_datasets = TestDatasets::create(
            &opctx,
            db_datastore.clone(),
            REGION_REDUNDANCY_THRESHOLD,
        )
        .await;

        let volume_id = Uuid::new_v4();
        let volume_to_delete_id = Uuid::new_v4();

        let datasets_and_regions = db_datastore
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

        let mut region_addresses: Vec<String> =
            Vec::with_capacity(datasets_and_regions.len());

        for (i, (_, region)) in datasets_and_regions.iter().enumerate() {
            // `disk_region_allocate` won't put any ports in, so add fake ones
            // here
            use nexus_db_model::schema::region::dsl;
            diesel::update(dsl::region)
                .filter(dsl::id.eq(region.id()))
                .set(dsl::port.eq(Some::<SqlU16>((100 + i as u16).into())))
                .execute_async(&*conn)
                .await
                .unwrap();

            let address: SocketAddrV6 =
                db_datastore.region_addr(region.id()).await.unwrap().unwrap();

            region_addresses.push(address.to_string());
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

            use nexus_db_model::schema::region::dsl;
            diesel::insert_into(dsl::region)
                .values(region.clone())
                .execute_async(&*conn)
                .await
                .unwrap();

            region
        };

        let replacement_region_addr: SocketAddrV6 = db_datastore
            .region_addr(replacement_region.id())
            .await
            .unwrap()
            .unwrap();

        // need to add region snapshot objects to satisfy volume create
        // transaction's search for resources

        let address_1 = String::from("[fd00:1122:3344:104::1]:400");
        let address_2 = String::from("[fd00:1122:3344:105::1]:401");
        let address_3 = String::from("[fd00:1122:3344:106::1]:402");

        let region_snapshots = [
            RegionSnapshot::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_1.clone(),
            ),
            RegionSnapshot::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_2.clone(),
            ),
            RegionSnapshot::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_3.clone(),
            ),
        ];

        db_datastore
            .region_snapshot_create(region_snapshots[0].clone())
            .await
            .unwrap();
        db_datastore
            .region_snapshot_create(region_snapshots[1].clone())
            .await
            .unwrap();
        db_datastore
            .region_snapshot_create(region_snapshots[2].clone())
            .await
            .unwrap();

        // Insert two volumes: one with the target to replace, and one temporary
        // "volume to delete" that's blank. Validate the pre-replacement volume
        // resource usage records.

        let rop_id = Uuid::new_v4();

        db_datastore
            .volume_create(nexus_db_model::Volume::new(
                volume_id,
                serde_json::to_string(&VolumeConstructionRequest::Volume {
                    id: volume_id,
                    block_size: 512,
                    sub_volumes: vec![VolumeConstructionRequest::Region {
                        block_size: 512,
                        blocks_per_extent: 10,
                        extent_count: 10,
                        gen: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                region_addresses[0].clone(),
                                region_addresses[1].clone(),
                                region_addresses[2].clone(),
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
                            gen: 1,
                            opts: CrucibleOpts {
                                id: rop_id,
                                target: vec![
                                    // target to replace
                                    address_1.clone(),
                                    address_2.clone(),
                                    address_3.clone(),
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
                })
                .unwrap(),
            ))
            .await
            .unwrap();

        for region_snapshot in &region_snapshots {
            let usage = db_datastore
                .volume_usage_records_for_resource(
                    VolumeResourceUsage::RegionSnapshot {
                        dataset_id: region_snapshot.dataset_id,
                        region_id: region_snapshot.region_id,
                        snapshot_id: region_snapshot.snapshot_id,
                    },
                )
                .await
                .unwrap();

            assert_eq!(usage.len(), 1);
            assert_eq!(usage[0].volume_id, volume_id);
        }

        db_datastore
            .volume_create(nexus_db_model::Volume::new(
                volume_to_delete_id,
                serde_json::to_string(&VolumeConstructionRequest::Volume {
                    id: volume_to_delete_id,
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: None,
                })
                .unwrap(),
            ))
            .await
            .unwrap();

        // `volume_create` above was called with a blank volume, so no usage
        // record will have been created for the read-only region

        let usage = db_datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::ReadOnlyRegion {
                    region_id: replacement_region.id(),
                },
            )
            .await
            .unwrap();

        assert!(usage.is_empty());

        // Do the replacement

        let volume_replace_snapshot_result = db_datastore
            .volume_replace_snapshot(
                VolumeWithTarget(volume_id),
                ExistingTarget(address_1.parse().unwrap()),
                ReplacementTarget(replacement_region_addr),
                VolumeToDelete(volume_to_delete_id),
            )
            .await
            .unwrap();

        assert_eq!(volume_replace_snapshot_result, VolumeReplaceResult::Done);

        // Ensure the shape of the resulting VCRs

        let vcr: VolumeConstructionRequest = serde_json::from_str(
            db_datastore.volume_get(volume_id).await.unwrap().unwrap().data(),
        )
        .unwrap();

        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: volume_id,
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    gen: 1,
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            region_addresses[0].clone(),
                            region_addresses[1].clone(),
                            region_addresses[2].clone(),
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
                        gen: 1,
                        opts: CrucibleOpts {
                            id: rop_id,
                            target: vec![
                                // target replaced
                                replacement_region_addr.to_string(),
                                address_2.clone(),
                                address_3.clone(),
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
            db_datastore
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
                id: volume_to_delete_id,
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 1,
                    extent_count: 1,
                    gen: 1,
                    opts: CrucibleOpts {
                        id: volume_to_delete_id,
                        target: vec![
                            // replaced target stashed here
                            address_1.clone(),
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
            let usage = db_datastore
                .volume_usage_records_for_resource(
                    VolumeResourceUsage::RegionSnapshot {
                        dataset_id: region_snapshot.dataset_id,
                        region_id: region_snapshot.region_id,
                        snapshot_id: region_snapshot.snapshot_id,
                    },
                )
                .await
                .unwrap();

            assert_eq!(usage.len(), 1);

            match i {
                0 => {
                    assert_eq!(usage[0].volume_id, volume_to_delete_id);
                }

                1 | 2 => {
                    assert_eq!(usage[0].volume_id, volume_id);
                }

                _ => panic!("out of range"),
            }
        }

        let usage = db_datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::ReadOnlyRegion {
                    region_id: replacement_region.id(),
                },
            )
            .await
            .unwrap();

        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].volume_id, volume_id);

        // Now undo the replacement. Note volume ID is not swapped.

        let volume_replace_snapshot_result = db_datastore
            .volume_replace_snapshot(
                VolumeWithTarget(volume_id),
                ExistingTarget(replacement_region_addr),
                ReplacementTarget(address_1.parse().unwrap()),
                VolumeToDelete(volume_to_delete_id),
            )
            .await
            .unwrap();

        assert_eq!(volume_replace_snapshot_result, VolumeReplaceResult::Done,);

        let vcr: VolumeConstructionRequest = serde_json::from_str(
            db_datastore.volume_get(volume_id).await.unwrap().unwrap().data(),
        )
        .unwrap();

        // Ensure the shape of the resulting VCR
        assert_eq!(
            &vcr,
            &VolumeConstructionRequest::Volume {
                id: volume_id,
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 10,
                    extent_count: 10,
                    gen: 1,
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            region_addresses[0].clone(),
                            region_addresses[1].clone(),
                            region_addresses[2].clone(),
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
                        gen: 1,
                        opts: CrucibleOpts {
                            id: rop_id,
                            target: vec![
                                // back to what it was
                                address_1, address_2, address_3,
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
            db_datastore
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
                id: volume_to_delete_id,
                block_size: 512,
                sub_volumes: vec![VolumeConstructionRequest::Region {
                    block_size: 512,
                    blocks_per_extent: 1,
                    extent_count: 1,
                    gen: 1,
                    opts: CrucibleOpts {
                        id: volume_to_delete_id,
                        target: vec![
                            // replacement stashed here
                            replacement_region_addr.to_string(),
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
            let usage = db_datastore
                .volume_usage_records_for_resource(
                    VolumeResourceUsage::RegionSnapshot {
                        dataset_id: region_snapshot.dataset_id,
                        region_id: region_snapshot.region_id,
                        snapshot_id: region_snapshot.snapshot_id,
                    },
                )
                .await
                .unwrap();

            assert_eq!(usage.len(), 1);
            assert_eq!(usage[0].volume_id, volume_id);
        }

        let usage = db_datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::ReadOnlyRegion {
                    region_id: replacement_region.id(),
                },
            )
            .await
            .unwrap();

        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].volume_id, volume_to_delete_id);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_find_volumes_referencing_socket_addr() {
        let logctx =
            dev::test_setup_log("test_find_volumes_referencing_socket_addr");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let (opctx, db_datastore) = datastore_test(&logctx, &db).await;

        let volume_id = Uuid::new_v4();

        // need to add region snapshot objects to satisfy volume create
        // transaction's search for resources

        let address_1 = String::from("[fd00:1122:3344:104::1]:400");
        let address_2 = String::from("[fd00:1122:3344:105::1]:401");
        let address_3 = String::from("[fd00:1122:3344:106::1]:402");

        db_datastore
            .region_snapshot_create(RegionSnapshot::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_1.clone(),
            ))
            .await
            .unwrap();
        db_datastore
            .region_snapshot_create(RegionSnapshot::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_2.clone(),
            ))
            .await
            .unwrap();
        db_datastore
            .region_snapshot_create(RegionSnapshot::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                address_3.clone(),
            ))
            .await
            .unwrap();

        // case where the needle is found

        db_datastore
            .volume_create(nexus_db_model::Volume::new(
                volume_id,
                serde_json::to_string(&VolumeConstructionRequest::Volume {
                    id: volume_id,
                    block_size: 512,
                    sub_volumes: vec![],
                    read_only_parent: Some(Box::new(
                        VolumeConstructionRequest::Region {
                            block_size: 512,
                            blocks_per_extent: 10,
                            extent_count: 10,
                            gen: 1,
                            opts: CrucibleOpts {
                                id: Uuid::new_v4(),
                                target: vec![
                                    address_1.clone(),
                                    address_2,
                                    address_3,
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
                })
                .unwrap(),
            ))
            .await
            .unwrap();

        let volumes = db_datastore
            .find_volumes_referencing_socket_addr(
                &opctx,
                address_1.parse().unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].id(), volume_id);

        // case where the needle is missing

        let volumes = db_datastore
            .find_volumes_referencing_socket_addr(
                &opctx,
                "[fd55:1122:3344:104::1]:400".parse().unwrap(),
            )
            .await
            .unwrap();

        assert!(volumes.is_empty());

        db.cleanup().await.unwrap();
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
                    gen: 1,
                    opts: CrucibleOpts {
                        id: Uuid::new_v4(),
                        target: vec![
                            String::from("[fd00:1122:3344:104::1]:400"),
                            String::from("[fd00:1122:3344:105::1]:401"),
                            String::from("[fd00:1122:3344:106::1]:402"),
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

        assert!(read_only_target_in_vcr(
            &vcr,
            &"[fd00:1122:3344:104::1]:400".parse().unwrap(),
        )
        .unwrap());

        // read_only_target_in_vcr should _not_ find read-write targets

        let vcr = VolumeConstructionRequest::Volume {
            id: Uuid::new_v4(),
            block_size: 512,
            sub_volumes: vec![VolumeConstructionRequest::Region {
                block_size: 512,
                blocks_per_extent: 10,
                extent_count: 10,
                gen: 1,
                opts: CrucibleOpts {
                    id: Uuid::new_v4(),
                    target: vec![
                        String::from("[fd00:1122:3344:104::1]:400"),
                        String::from("[fd00:1122:3344:105::1]:401"),
                        String::from("[fd00:1122:3344:106::1]:402"),
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

        assert!(!read_only_target_in_vcr(
            &vcr,
            &"[fd00:1122:3344:104::1]:400".parse().unwrap(),
        )
        .unwrap());

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
                    gen: 1,
                    opts: CrucibleOpts {
                        id: Uuid::new_v4(),
                        target: vec![
                            String::from("[fd00:1122:3344:104::1]:400"),
                            String::from("[fd00:1122:3344:105::1]:401"),
                            String::from("[fd00:1122:3344:106::1]:402"),
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
                    gen: 1,
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            String::from("[fd00:1122:3344:104::1]:400"),
                            String::from("[fd00:1122:3344:105::1]:401"),
                            String::from("[fd00:1122:3344:106::1]:402"),
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
                        gen: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                String::from("[fd00:1122:3344:104::1]:400"),
                                new_target.0.to_string(),
                                String::from("[fd00:1122:3344:106::1]:402"),
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
                    gen: 1,
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            String::from("[fd55:1122:3344:204::1]:1000"),
                            String::from("[fd55:1122:3344:205::1]:1001"),
                            String::from("[fd55:1122:3344:206::1]:1002"),
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
                        gen: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                String::from("[fd33:1122:3344:304::1]:2000"),
                                String::from("[fd33:1122:3344:305::1]:2001"),
                                String::from("[fd33:1122:3344:306::1]:2002"),
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
                    gen: 1,
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            String::from("[fd00:1122:3344:104::1]:400"),
                            String::from("[fd00:1122:3344:105::1]:401"),
                            String::from("[fd00:1122:3344:106::1]:402"),
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
                        gen: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                String::from("[fd55:1122:3344:204::1]:1000"),
                                String::from("[fd55:1122:3344:205::1]:1001"),
                                String::from("[fd55:1122:3344:206::1]:1002"),
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
                            gen: 1,
                            opts: CrucibleOpts {
                                id: volume_id,
                                target: vec![
                                    String::from(
                                        "[fd33:1122:3344:304::1]:2000"
                                    ),
                                    String::from(
                                        "[fd33:1122:3344:305::1]:2001"
                                    ),
                                    new_target.0.to_string(),
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
                        gen: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                String::from("[fd00:1122:3344:104::1]:400"),
                                String::from("[fd00:1122:3344:105::1]:401"),
                                String::from("[fd00:1122:3344:106::1]:402"),
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
            gen: 1,
            opts: CrucibleOpts {
                id: volume_id,
                target: vec![
                    String::from("[fd33:1122:3344:304::1]:2000"),
                    String::from("[fd33:1122:3344:305::1]:2001"),
                    String::from("[fd33:1122:3344:306::1]:2002"),
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
                    gen: 1,
                    opts: CrucibleOpts {
                        id: volume_id,
                        target: vec![
                            String::from("[fd55:1122:3344:204::1]:1000"),
                            String::from("[fd55:1122:3344:205::1]:1001"),
                            String::from("[fd55:1122:3344:206::1]:1002"),
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
            gen: 1,
            opts: CrucibleOpts {
                id: volume_id,
                target: vec![
                    new_target.0.to_string(),
                    String::from("[fd33:1122:3344:305::1]:2001"),
                    String::from("[fd33:1122:3344:306::1]:2002"),
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
                        gen: 1,
                        opts: CrucibleOpts {
                            id: volume_id,
                            target: vec![
                                String::from("[fd55:1122:3344:204::1]:1000"),
                                String::from("[fd55:1122:3344:205::1]:1001"),
                                String::from("[fd55:1122:3344:206::1]:1002"),
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
}
