// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Volume`]s.

use super::DataStore;
use crate::db;
use crate::db::datastore::OpContext;
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
use crate::db::queries::volume::DecreaseCrucibleResourceCountAndSoftDeleteVolume;
use crate::transaction_retry::OptionalError;
use anyhow::bail;
use async_bb8_diesel::AsyncRunQueryDsl;
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

impl DataStore {
    pub async fn volume_create(&self, volume: Volume) -> CreateResult<Volume> {
        use db::schema::volume::dsl;

        #[derive(Debug, thiserror::Error)]
        enum VolumeCreationError {
            #[error("Error from Volume creation: {0}")]
            Public(Error),

            #[error("Serde error during Volume creation: {0}")]
            SerdeError(#[from] serde_json::Error),
        }

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
                    let maybe_volume: Option<Volume> = dsl::volume
                        .filter(dsl::id.eq(volume.id()))
                        .select(Volume::as_select())
                        .first_async(&conn)
                        .await
                        .optional()?;

                    // If the volume existed already, return it and do not increase
                    // usage counts.
                    if let Some(volume) = maybe_volume {
                        return Ok(volume);
                    }

                    // TODO do we need on_conflict do_nothing here? if the transaction
                    // model is read-committed, the SELECT above could return nothing,
                    // and the INSERT here could still result in a conflict.
                    //
                    // See also https://github.com/oxidecomputer/omicron/issues/1168
                    let volume: Volume = diesel::insert_into(dsl::volume)
                        .values(volume.clone())
                        .on_conflict(dsl::id)
                        .do_nothing()
                        .returning(Volume::as_returning())
                        .get_result_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                VolumeCreationError::Public(
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::Conflict(
                                            ResourceType::Volume,
                                            volume.id().to_string().as_str(),
                                        ),
                                    ),
                                )
                            })
                        })?;

                    // Increase the usage count for Crucible resources according to the
                    // contents of the volume.

                    // Increase the number of uses for each referenced region snapshot.
                    use db::schema::region_snapshot::dsl as rs_dsl;
                    for read_only_target in &crucible_targets.read_only_targets
                    {
                        diesel::update(rs_dsl::region_snapshot)
                            .filter(
                                rs_dsl::snapshot_addr
                                    .eq(read_only_target.clone()),
                            )
                            .filter(rs_dsl::deleting.eq(false))
                            .set(
                                rs_dsl::volume_references
                                    .eq(rs_dsl::volume_references + 1),
                            )
                            .execute_async(&conn)
                            .await?;
                    }

                    Ok(volume)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        VolumeCreationError::Public(err) => err,
                        VolumeCreationError::SerdeError(err) => {
                            Error::internal_error(&format!(
                                "Transaction error: {}",
                                err
                            ))
                        }
                    }
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    /// Return a `Option<Volume>` based on id, even if it's soft deleted.
    pub async fn volume_get(
        &self,
        volume_id: Uuid,
    ) -> LookupResult<Option<Volume>> {
        use db::schema::volume::dsl;
        dsl::volume
            .filter(dsl::id.eq(volume_id))
            .select(Volume::as_select())
            .first_async::<Volume>(&*self.pool_connection_unauthorized().await?)
            .await
            .optional()
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
    /// snapshots.
    pub async fn find_deleted_volume_regions(
        &self,
    ) -> ListResultVec<(Dataset, Region, Option<RegionSnapshot>, Volume)> {
        use db::schema::dataset::dsl as dataset_dsl;
        use db::schema::region::dsl as region_dsl;
        use db::schema::region_snapshot::dsl;
        use db::schema::volume::dsl as volume_dsl;

        // Find all regions and datasets
        region_dsl::region
            .inner_join(
                volume_dsl::volume.on(region_dsl::volume_id.eq(volume_dsl::id)),
            )
            .inner_join(
                dataset_dsl::dataset
                    .on(region_dsl::dataset_id.eq(dataset_dsl::id.nullable())),
            )
            // where there either are no region snapshots, or the region
            // snapshot volume references have gone to zero
            .left_join(
                dsl::region_snapshot.on(dsl::region_id
                    .eq(region_dsl::id)
                    .and(dsl::dataset_id.eq(dataset_dsl::id))),
            )
            .filter(
                dsl::volume_references
                    .eq(0)
                    // Despite the SQL specifying that this column is NOT NULL,
                    // this null check is required for this function to work!
                    // It's possible that the left join of region_snapshot above
                    // could join zero rows, making this null.
                    .or(dsl::volume_references.is_null()),
            )
            // where the volume has already been soft-deleted
            .filter(volume_dsl::time_deleted.is_not_null())
            // and return them (along with the volume so it can be hard deleted)
            .select((
                Dataset::as_select(),
                Region::as_select(),
                Option::<RegionSnapshot>::as_select(),
                Volume::as_select(),
            ))
            .load_async(&*self.pool_connection_unauthorized().await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
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

    /// Decrease the usage count for Crucible resources according to the
    /// contents of the volume. Call this when deleting a volume (but before the
    /// volume record has been hard deleted).
    ///
    /// Returns a list of Crucible resources to clean up, and soft-deletes the
    /// volume. Note this function must be idempotent, it is called from a saga
    /// node.
    pub async fn decrease_crucible_resource_count_and_soft_delete_volume(
        &self,
        volume_id: Uuid,
    ) -> Result<CrucibleResources, Error> {
        // Grab all the targets that the volume construction request references.
        // Do this outside the transaction, as the data inside volume doesn't
        // change and this would simply add to the transaction time.
        let crucible_targets = {
            let volume =
                if let Some(volume) = self.volume_get(volume_id).await? {
                    volume
                } else {
                    // The volume was hard-deleted, return an empty
                    // CrucibleResources
                    return Ok(CrucibleResources::V1(
                        CrucibleResourcesV1::default(),
                    ));
                };

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

        // Call a CTE that will:
        //
        // 1. decrease the number of references for each region snapshot that
        //    this Volume references
        // 2. soft-delete the volume
        // 3. record the resources to clean up as a serialized CrucibleResources
        //    struct in volume's `resources_to_clean_up` column.
        //
        // Step 3 is important because this function is called from a saga node.
        // If saga execution crashes after steps 1 and 2, but before serializing
        // the resources to be cleaned up as part of the saga node context, then
        // that list of resources will be lost.
        //
        // We also have to guard against the case where this function is called
        // multiple times, and that is done by soft-deleting the volume during
        // the CTE, and returning the previously serialized list of resources to
        // clean up if a soft-delete has already occurred.

        let _old_volume: Vec<Volume> =
            DecreaseCrucibleResourceCountAndSoftDeleteVolume::new(
                volume_id,
                crucible_targets.read_only_targets.clone(),
            )
            .get_results_async::<Volume>(
                &*self.pool_connection_unauthorized().await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // Get the updated Volume to get the resources to clean up
        let resources_to_clean_up: CrucibleResources = match self
            .volume_get(volume_id)
            .await?
        {
            Some(volume) => {
                match volume.resources_to_clean_up.as_ref() {
                    Some(v) => serde_json::from_str(v)?,

                    None => {
                        // Even volumes with nothing to clean up should have
                        // a serialized CrucibleResources that contains
                        // empty vectors instead of None. Instead of
                        // panicing here though, just return the default
                        // (nothing to clean up).
                        CrucibleResources::V1(CrucibleResourcesV1::default())
                    }
                }
            }

            None => {
                // If the volume was hard-deleted already, return the
                // default (nothing to clean up).
                CrucibleResources::V1(CrucibleResourcesV1::default())
            }
        };

        Ok(resources_to_clean_up)
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
            #[error("Serde error removing read only parent: {0}")]
            SerdeError(#[from] serde_json::Error),

            #[error("Updated {0} database rows, expected {1}")]
            UnexpectedDatabaseUpdate(usize, usize),
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
                    // Grab the volume in question. If the volume record was already
                    // deleted then we can just return.
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
                            // this volume is deleted, so let whatever is deleting
                            // it clean it up.
                            return Ok(false);
                        } else {
                            // A volume record exists, and was not deleted, we
                            // can attempt to remove its read_only_parent.
                            volume
                        }
                    };

                    // If a read_only_parent exists, remove it from volume_id, and
                    // attach it to temp_volume_id.
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
                                Ok(true)
                            }
                        }
                        VolumeConstructionRequest::File { id: _, block_size: _, path: _ }
                        | VolumeConstructionRequest::Region {
                            block_size: _,
                            blocks_per_extent: _,
                            extent_count: _,
                            opts: _,
                            gen: _ }
                        | VolumeConstructionRequest::Url { id: _, block_size: _, url: _ } => {
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

#[derive(Debug, Default, Serialize, Deserialize)]
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
    Ok(match Option::<Vec<T>>::deserialize(de)? {
        Some(v) => v,
        None => vec![],
    })
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
                    .inner_join(dataset_dsl::dataset.on(
                        region_dsl::dataset_id.eq(dataset_dsl::id.nullable()),
                    ))
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

pub struct VolumeReplacementParams {
    pub volume_id: Uuid,
    pub region_id: Uuid,
    pub region_addr: SocketAddrV6,
}

impl DataStore {
    /// Replace a read-write region in a Volume with a new region.
    pub async fn volume_replace_region(
        &self,
        existing: VolumeReplacementParams,
        replacement: VolumeReplacementParams,
    ) -> Result<(), Error> {
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

            #[error("Target Volume deleted")]
            TargetVolumeDeleted,

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
                        // Existing volume was deleted, so return an error. We
                        // can't perform the region replacement now!
                        return Err(err.bail(VolumeReplaceRegionError::TargetVolumeDeleted));
                    };

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
                        return Ok(());
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

                    Ok(())
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

                        VolumeReplaceRegionError::TargetVolumeDeleted => {
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
}

/// Return the targets from a VolumeConstructionRequest.
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::db::datastore::test_utils::datastore_test;
    use nexus_test_utils::db::test_setup_database;
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
                .set(dsl::resources_to_clean_up.eq(resources_to_clean_up))
                .execute_async(&*conn)
                .await
                .unwrap();
        }

        // Soft delete the volume, which runs the CTE

        let cr = db_datastore
            .decrease_crucible_resource_count_and_soft_delete_volume(volume_id)
            .await
            .unwrap();

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
        let (_opctx, db_datastore) = datastore_test(&logctx, &db).await;

        // Insert four Region records (three, plus one additionally allocated)

        let volume_id = Uuid::new_v4();
        let new_volume_id = Uuid::new_v4();

        let mut region_and_volume_ids = [
            (Uuid::new_v4(), volume_id),
            (Uuid::new_v4(), volume_id),
            (Uuid::new_v4(), volume_id),
            (Uuid::new_v4(), new_volume_id),
        ];

        {
            let conn = db_datastore.pool_connection_for_tests().await.unwrap();

            for i in 0..4 {
                let (_, volume_id) = region_and_volume_ids[i];

                let region = Region::new(
                    Uuid::new_v4(), // dataset id
                    volume_id,
                    512_i64.try_into().unwrap(),
                    10,
                    10,
                    10001,
                );

                region_and_volume_ids[i].0 = region.id();

                use nexus_db_model::schema::region::dsl;
                diesel::insert_into(dsl::region)
                    .values(region.clone())
                    .execute_async(&*conn)
                    .await
                    .unwrap();
            }
        }

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
                                String::from("[fd00:1122:3344:101::1]:11111"), // target to replace
                                String::from("[fd00:1122:3344:102::1]:22222"),
                                String::from("[fd00:1122:3344:103::1]:33333"),
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

        let target = region_and_volume_ids[0];
        let replacement = region_and_volume_ids[3];

        db_datastore
            .volume_replace_region(
                /* target */
                db::datastore::VolumeReplacementParams {
                    volume_id: target.1,
                    region_id: target.0,
                    region_addr: "[fd00:1122:3344:101::1]:11111"
                        .parse()
                        .unwrap(),
                },
                /* replacement */
                db::datastore::VolumeReplacementParams {
                    volume_id: replacement.1,
                    region_id: replacement.0,
                    region_addr: "[fd55:1122:3344:101::1]:11111"
                        .parse()
                        .unwrap(),
                },
            )
            .await
            .unwrap();

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
                            String::from("[fd55:1122:3344:101::1]:11111"), // replaced
                            String::from("[fd00:1122:3344:102::1]:22222"),
                            String::from("[fd00:1122:3344:103::1]:33333"),
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
        db_datastore
            .volume_replace_region(
                /* target */
                db::datastore::VolumeReplacementParams {
                    volume_id: target.1,
                    region_id: replacement.0,
                    region_addr: "[fd55:1122:3344:101::1]:11111"
                        .parse()
                        .unwrap(),
                },
                /* replacement */
                db::datastore::VolumeReplacementParams {
                    volume_id: replacement.1,
                    region_id: target.0,
                    region_addr: "[fd00:1122:3344:101::1]:11111"
                        .parse()
                        .unwrap(),
                },
            )
            .await
            .unwrap();

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
                            String::from("[fd00:1122:3344:101::1]:11111"), // back to what it was
                            String::from("[fd00:1122:3344:102::1]:22222"),
                            String::from("[fd00:1122:3344:103::1]:33333"),
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
}
