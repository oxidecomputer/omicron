// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus is responsible for telling Crucible Agent(s) when to clean up
//! resources - those Agents do not have any idea of what volumes are
//! constructed, currently active, etc. Plus, volumes can (and will) change
//! during their lifetime. Operations like growing a disk, removing a read-only
//! parent after a scrub has completed, or re-encrypting a disk will all change
//! the volume that backs a disk.
//!
//! Nexus has to account for all the Crucible resources it is using, and count
//! how many volumes are using those resources. Only when that count drops to
//! zero is it valid to clean up the appropriate Crucible resource.
//!
//! Complicating things is the fact that ZFS datasets cannot be deleted if there
//! are snapshots of that dataset. Nexus' resource accounting must take this
//! dependency into account. Note that ZFS snapshots can layer, but any snapshot
//! can be deleted without the requirement of (for example) deleting the
//! snapshots in a certain order.
//!
//! One problem to solve is doing this idempotently. Volumes reference Crucible
//! resources, and when they are inserted or deleted the accounting needs to
//! change. Saga nodes must be idempotent in order to work correctly.

use super::common_storage::delete_crucible_regions;
use super::common_storage::delete_crucible_snapshots;
use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas::NexusAction;
use crate::authn;
use crate::context::OpContext;
use crate::db::datastore::CrucibleResources;
use lazy_static::lazy_static;
use nexus_types::identity::Asset;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// volume delete saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub project_id: Uuid,
    pub volume_id: Uuid,
}
// volume delete saga: actions

lazy_static! {
    // TODO(https://github.com/oxidecomputer/omicron/issues/612):
    //
    // We need a way to deal with this operation failing, aside from
    // propagating the error to the user.
    //
    // What if the Sled goes offline? Nexus must ultimately be
    // responsible for reconciling this scenario.

    static ref DECREASE_CRUCIBLE_RESOURCE_COUNT: NexusAction = new_action_noop_undo(
        "volume-delete.decrease-resource-count",
        svd_decrease_crucible_resource_count,
    );

    static ref DELETE_CRUCIBLE_REGIONS: NexusAction = new_action_noop_undo(
        "volume-delete.delete-crucible-regions",
        svd_delete_crucible_regions,
    );

    static ref DELETE_CRUCIBLE_SNAPSHOTS: NexusAction = new_action_noop_undo(
        "volume-delete.delete-crucible-snapshots",
        svd_delete_crucible_snapshots,
    );

    static ref DELETE_FREED_CRUCIBLE_REGIONS: NexusAction = new_action_noop_undo(
        "volume-delete.delete-freed-crucible-regions",
        svd_delete_freed_crucible_regions,
    );

    static ref HARD_DELETE_VOLUME_RECORD: NexusAction = new_action_noop_undo(
        "volume-delete.hard-delete-volume-record",
        svd_hard_delete_volume_record,
    );
}

// volume delete saga: definition

#[derive(Debug)]
pub struct SagaVolumeDelete;
impl NexusSaga for SagaVolumeDelete {
    const NAME: &'static str = "volume-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        registry.register(Arc::clone(&*DECREASE_CRUCIBLE_RESOURCE_COUNT));
        registry.register(Arc::clone(&*DELETE_CRUCIBLE_REGIONS));
        registry.register(Arc::clone(&*DELETE_CRUCIBLE_SNAPSHOTS));
        registry.register(Arc::clone(&*DELETE_FREED_CRUCIBLE_REGIONS));
        registry.register(Arc::clone(&*HARD_DELETE_VOLUME_RECORD));
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        builder.append(Node::action(
            "crucible_resources_to_delete",
            "DecreaseCrucibleResources",
            DECREASE_CRUCIBLE_RESOURCE_COUNT.as_ref(),
        ));

        builder.append_parallel(vec![
            // clean up top level regions for volume
            Node::action(
                "no_result_1",
                "DeleteCrucibleRegions",
                DELETE_CRUCIBLE_REGIONS.as_ref(),
            ),
            // clean up snapshots no longer referenced by any volume
            Node::action(
                "no_result_2",
                "DeleteCrucibleSnapshots",
                DELETE_CRUCIBLE_SNAPSHOTS.as_ref(),
            ),
        ]);

        // clean up regions that were freed by deleting snapshots
        builder.append(Node::action(
            "no_result_3",
            "DeleteFreedCrucibleRegions",
            DELETE_FREED_CRUCIBLE_REGIONS.as_ref(),
        ));

        builder.append(Node::action(
            "final_no_result",
            "HardDeleteVolumeRecord",
            HARD_DELETE_VOLUME_RECORD.as_ref(),
        ));

        Ok(builder.build()?)
    }
}

// volume delete saga: action implementations

/// Decrease Crucible resource accounting for this volume, and return Crucible
/// resources to delete.
async fn svd_decrease_crucible_resource_count(
    sagactx: NexusActionContext,
) -> Result<CrucibleResources, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let crucible_resources = osagactx
        .datastore()
        .decrease_crucible_resource_count_and_soft_delete_volume(
            params.volume_id,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(crucible_resources)
}

/// Clean up regions associated with this volume.
async fn svd_delete_crucible_regions(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let crucible_resources_to_delete =
        sagactx.lookup::<CrucibleResources>("crucible_resources_to_delete")?;

    // Send DELETE calls to the corresponding Crucible agents
    match crucible_resources_to_delete {
        CrucibleResources::V1(crucible_resources_to_delete) => {
            delete_crucible_regions(
                crucible_resources_to_delete.datasets_and_regions.clone(),
            )
            .await
            .map_err(ActionError::action_failed)?;

            // Remove DB records
            let region_ids_to_delete = crucible_resources_to_delete
                .datasets_and_regions
                .iter()
                .map(|(_, r)| r.id())
                .collect();

            // TODO: This accounting is not yet idempotent
            let space_used = crucible_resources_to_delete
                .datasets_and_regions
                .iter()
                .fold(0, |acc, (_, r)| acc + r.size_used());
            let opctx =
                OpContext::for_saga_action(&sagactx, &params.serialized_authn);
            osagactx
                .datastore()
                .resource_usage_update_disk(
                    &opctx,
                    params.project_id,
                    -space_used,
                )
                .await
                .map_err(ActionError::action_failed)?;

            osagactx
                .datastore()
                .regions_hard_delete(region_ids_to_delete)
                .await
                .map_err(ActionError::action_failed)?;
        }
    }

    Ok(())
}

/// Clean up snapshots freed up for deletion by deleting this volume.
///
/// This Volume may have referenced read-only downstairs (and their snapshots),
/// and deleting it will remove the references - this may free up those
/// resources for deletion, which this Saga node does.
async fn svd_delete_crucible_snapshots(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();

    let crucible_resources_to_delete =
        sagactx.lookup::<CrucibleResources>("crucible_resources_to_delete")?;

    // Send DELETE calls to the corresponding Crucible agents
    match crucible_resources_to_delete {
        CrucibleResources::V1(crucible_resources_to_delete) => {
            delete_crucible_snapshots(
                crucible_resources_to_delete.datasets_and_snapshots.clone(),
            )
            .await
            .map_err(ActionError::action_failed)?;

            // Remove DB records
            for (_, region_snapshot) in
                &crucible_resources_to_delete.datasets_and_snapshots
            {
                osagactx
                    .datastore()
                    .region_snapshot_remove(
                        region_snapshot.dataset_id,
                        region_snapshot.region_id,
                        region_snapshot.snapshot_id,
                    )
                    .await
                    .map_err(ActionError::action_failed)?;
            }
        }
    }

    Ok(())
}

/// Deleting region snapshots in a previous saga node may have freed up regions
/// that were deleted in the DB but couldn't be deleted by the Crucible Agent
/// because a snapshot existed. Look for those here, and delete them. These will
/// be a different volume id (i.e. for a previously deleted disk) than the one
/// in this saga's params struct.
///
/// Note: each delete of a snapshot could trigger another delete of a region, if
/// that region's use has gone to zero. A snapshot delete will never trigger
/// another snapshot delete.
async fn svd_delete_freed_crucible_regions(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();

    // Find regions freed up for deletion by a previous saga node deleting the
    // region snapshots.
    let freed_datasets_regions_and_volumes = osagactx
        .datastore()
        .find_deleted_volume_regions()
        .await
        .map_err(ActionError::action_failed)?;

    // Send DELETE calls to the corresponding Crucible agents
    delete_crucible_regions(
        freed_datasets_regions_and_volumes
            .iter()
            .map(|(d, r, _)| (d.clone(), r.clone()))
            .collect(),
    )
    .await
    .map_err(ActionError::action_failed)?;

    // Remove region DB records
    osagactx
        .datastore()
        .regions_hard_delete(
            freed_datasets_regions_and_volumes
                .iter()
                .map(|(_, r, _)| r.id())
                .collect(),
        )
        .await
        .map_err(ActionError::action_failed)?;

    // Remove volume DB records
    for (_, _, volume) in &freed_datasets_regions_and_volumes {
        osagactx
            .datastore()
            .volume_hard_delete(volume.id())
            .await
            .map_err(ActionError::action_failed)?;
    }

    Ok(())
}

/// Hard delete the volume record
async fn svd_hard_delete_volume_record(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    // Do not hard delete the volume record if there are lingering regions
    // associated with them. This occurs when a region snapshot hasn't been
    // deleted, which means we can't delete the region. Later on, deleting the
    // region snapshot will free up the region(s) to be deleted (this occurs in
    // svd_delete_freed_crucible_regions).
    let allocated_regions = osagactx
        .datastore()
        .get_allocated_regions(params.volume_id)
        .await
        .map_err(ActionError::action_failed)?;

    if !allocated_regions.is_empty() {
        return Ok(());
    }

    osagactx
        .datastore()
        .volume_hard_delete(params.volume_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}
