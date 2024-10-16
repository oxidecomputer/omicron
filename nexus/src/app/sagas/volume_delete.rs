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

use super::ActionRegistry;
use super::NexusActionContext;
use super::NexusSaga;
use crate::app::sagas::declare_saga_actions;
use nexus_db_model::Dataset;
use nexus_db_model::Region;
use nexus_db_model::Volume;
use nexus_db_queries::authn;
use nexus_db_queries::db::datastore::CrucibleResources;
use nexus_types::identity::Asset;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use uuid::Uuid;

// volume delete saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub volume_id: Uuid,
}

// volume delete saga: actions

declare_saga_actions! {
    volume_delete;
    DECREASE_CRUCIBLE_RESOURCE_COUNT -> "crucible_resources_to_delete" {
        + svd_decrease_crucible_resource_count
    }
    DELETE_CRUCIBLE_REGIONS -> "no_result_1" {
        + svd_delete_crucible_regions
    }
    DELETE_CRUCIBLE_RUNNING_SNAPSHOTS -> "no_result_2" {
        + svd_delete_crucible_running_snapshots
    }
    DELETE_CRUCIBLE_SNAPSHOTS -> "no_result_3" {
        + svd_delete_crucible_snapshots
    }
    DELETE_CRUCIBLE_SNAPSHOT_RECORDS -> "no_result_4" {
        + svd_delete_crucible_snapshot_records
    }
    FIND_FREED_CRUCIBLE_REGIONS -> "freed_crucible_regions" {
        + svd_find_freed_crucible_regions
    }
    DELETE_FREED_CRUCIBLE_REGIONS -> "no_result_5" {
        + svd_delete_freed_crucible_regions
    }
    HARD_DELETE_VOLUME_RECORD -> "volume_hard_deleted" {
        + svd_hard_delete_volume_record
    }
}

// volume delete saga: definition

pub fn create_dag(
    mut builder: steno::DagBuilder,
) -> Result<steno::Dag, super::SagaInitError> {
    builder.append(decrease_crucible_resource_count_action());
    builder.append_parallel(vec![
        // clean up top level regions for volume
        delete_crucible_regions_action(),
        // clean up running snapshots no longer referenced by any volume
        delete_crucible_running_snapshots_action(),
    ]);
    // clean up snapshots no longer referenced by any volume
    builder.append(delete_crucible_snapshots_action());
    // remove snapshot db records
    builder.append(delete_crucible_snapshot_records_action());
    // clean up regions that were freed by deleting snapshots
    builder.append(find_freed_crucible_regions_action());
    builder.append(delete_freed_crucible_regions_action());
    builder.append(hard_delete_volume_record_action());

    Ok(builder.build()?)
}

#[derive(Debug)]
pub(crate) struct SagaVolumeDelete;
impl NexusSaga for SagaVolumeDelete {
    const NAME: &'static str = "volume-delete";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        volume_delete_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        builder: steno::DagBuilder,
    ) -> Result<steno::Dag, super::SagaInitError> {
        create_dag(builder)
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
        .soft_delete_volume(params.volume_id)
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "failed to soft_delete_volume: {:?}",
                e,
            ))
        })?;

    Ok(crucible_resources)
}

/// Clean up regions associated with this volume.
async fn svd_delete_crucible_regions(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();

    let crucible_resources_to_delete =
        sagactx.lookup::<CrucibleResources>("crucible_resources_to_delete")?;

    // Send DELETE calls to the corresponding Crucible agents
    let datasets_and_regions = osagactx
        .datastore()
        .regions_to_delete(
            &crucible_resources_to_delete,
        )
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "failed to get datasets_and_regions from crucible resources ({:?}): {:?}",
                crucible_resources_to_delete,
                e,
            ))
        })?;

    osagactx
        .nexus()
        .delete_crucible_regions(log, datasets_and_regions.clone())
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "failed to delete_crucible_regions: {:?}",
                e,
            ))
        })?;

    // Remove DB records
    let region_ids_to_delete =
        datasets_and_regions.iter().map(|(_, r)| r.id()).collect();

    osagactx
        .datastore()
        .regions_hard_delete(log, region_ids_to_delete)
        .await
        .map_err(|e| {
        ActionError::action_failed(format!(
            "failed to regions_hard_delete: {:?}",
            e,
        ))
    })?;

    Ok(())
}

/// Clean up running read-only downstairs corresponding to snapshots freed up
/// for deletion by deleting this volume.
///
/// This Volume may have referenced read-only downstairs (and their snapshots),
/// and deleting it will remove the references - this may free up those
/// resources for deletion, which this Saga node does.
async fn svd_delete_crucible_running_snapshots(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();

    let crucible_resources_to_delete =
        sagactx.lookup::<CrucibleResources>("crucible_resources_to_delete")?;

    // Send DELETE calls to the corresponding Crucible agents
    let datasets_and_snapshots = osagactx
        .datastore()
        .snapshots_to_delete(
            &crucible_resources_to_delete,
        )
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "failed to get datasets_and_snapshots from crucible resources ({:?}): {:?}",
                crucible_resources_to_delete,
                e,
            ))
        })?;

    osagactx
        .nexus()
        .delete_crucible_running_snapshots(log, datasets_and_snapshots.clone())
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "failed to delete_crucible_running_snapshots: {:?}",
                e,
            ))
        })?;

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
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();

    let crucible_resources_to_delete =
        sagactx.lookup::<CrucibleResources>("crucible_resources_to_delete")?;

    // Send DELETE calls to the corresponding Crucible agents
    let datasets_and_snapshots = osagactx
        .datastore()
        .snapshots_to_delete(
            &crucible_resources_to_delete,
        )
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "failed to get datasets_and_snapshots from crucible resources ({:?}): {:?}",
                crucible_resources_to_delete,
                e,
            ))
        })?;

    osagactx
        .nexus()
        .delete_crucible_snapshots(log, datasets_and_snapshots.clone())
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "failed to delete_crucible_snapshots: {:?}",
                e,
            ))
        })?;

    Ok(())
}

/// Remove records for deleted snapshots
async fn svd_delete_crucible_snapshot_records(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();

    let crucible_resources_to_delete =
        sagactx.lookup::<CrucibleResources>("crucible_resources_to_delete")?;

    // Remove DB records
    let datasets_and_snapshots = osagactx
        .datastore()
        .snapshots_to_delete(
            &crucible_resources_to_delete,
        )
        .await
        .map_err(|e| {
            ActionError::action_failed(format!(
                "failed to get datasets_and_snapshots from crucible resources ({:?}): {:?}",
                crucible_resources_to_delete,
                e,
            ))
        })?;

    for (_, region_snapshot) in datasets_and_snapshots {
        osagactx
            .datastore()
            .region_snapshot_remove(
                region_snapshot.dataset_id,
                region_snapshot.region_id,
                region_snapshot.snapshot_id,
            )
            .await
            .map_err(|e| {
                ActionError::action_failed(format!(
                    "failed to region_snapshot_remove {} {} {}: {:?}",
                    region_snapshot.dataset_id,
                    region_snapshot.region_id,
                    region_snapshot.snapshot_id,
                    e,
                ))
            })?;
    }

    Ok(())
}

type FreedCrucibleRegions = Vec<(Dataset, Region, Option<Uuid>)>;

/// Deleting region snapshots in a previous saga node may have freed up regions
/// that were deleted in the DB but couldn't be deleted by the Crucible Agent
/// because a snapshot existed. Look for those here. These will be a different
/// volume id (i.e. for a previously deleted disk) than the one in this saga's
/// params struct.
///
/// It's insufficient to rely on the struct of CrucibleResources to clean up
/// that is returned as part of svd_decrease_crucible_resource_count. Imagine a
/// disk that is composed of three regions (a subset of
/// [`sled_agent_client::types::VolumeConstructionRequest`] is shown here):
///
/// ```json
/// {
///   "type": "volume",
///   "id": "6b353c87-afac-4ee2-b71a-6fe35fcf9e46",
///   "sub_volumes": [
///     {
///       "type": "region",
///       "opts": {
///         "targets": [
///           "[fd00:1122:3344:101::5]:1000",
///           "[fd00:1122:3344:102::9]:1000",
///           "[fd00:1122:3344:103::2]:1000"
///         ],
///         "read_only": false
///       }
///     }
///   ],
///   "read_only_parent": null,
/// }
/// ```
///
/// Taking a snapshot of this will produce the following volume:
///
/// ```json
/// {
///   "type": "volume",
///   "id": "1ef7282e-a3fb-4222-85a8-b16d3fbfd738",   <-- new UUID
///   "sub_volumes": [
///     {
///       "type": "region",
///       "opts": {
///         "targets": [
///           "[fd00:1122:3344:101::5]:1001",         <-- port changed
///           "[fd00:1122:3344:102::9]:1001",         <-- port changed
///           "[fd00:1122:3344:103::2]:1001"          <-- port changed
///         ],
///         "read_only": true                         <-- read_only now true
///       }
///     }
///   ],
///   "read_only_parent": null,
/// }
/// ```
///
/// The snapshot targets will use the same IP but different port: snapshots are
/// initially located on the same filesystem as their region.
///
/// The disk's volume has no read only resources, while the snapshot's volume
/// does. The disk volume's targets are all regions (backed by downstairs that
/// are read/write) while the snapshot volume's targets are all snapshots
/// (backed by downstairs that are read-only). The two volumes are linked in the
/// sense that the snapshots from the second are contained *within* the regions
/// of the first, reflecting the resource nesting from ZFS. This is also
/// reflected in the REST endpoint that the Crucible agent uses:
///
///   /crucible/0/regions/{id}/snapshots/{name}
///
/// If the disk is then deleted, the volume delete saga will run for the first
/// volume shown here. The CrucibleResources struct returned as part of
/// [`svd_decrease_crucible_resource_count`] will contain *nothing* to clean up:
/// the regions contain snapshots that are part of other volumes and cannot be
/// deleted, and the disk's volume doesn't reference any read-only resources.
///
/// This is expected and normal: regions are "leaked" all the time due to
/// snapshots preventing their deletion. This part of the saga detects when
/// those regions can be cleaned up - it must be stored in the output of this
/// saga node as deleting volume records will affect what is returned by
/// `find_deleted_volume_regions`.
///
/// Note: each delete of a snapshot could trigger another delete of a region, if
/// that region's use has gone to zero. A snapshot delete will never trigger
/// another snapshot delete.
async fn svd_find_freed_crucible_regions(
    sagactx: NexusActionContext,
) -> Result<FreedCrucibleRegions, ActionError> {
    let osagactx = sagactx.user_data();

    // Find regions freed up for deletion by a previous saga node deleting the
    // region snapshots.
    let freed_datasets_regions_and_volumes =
        osagactx.datastore().find_deleted_volume_regions().await.map_err(
            |e| {
                ActionError::action_failed(format!(
                    "failed to find_deleted_volume_regions: {:?}",
                    e,
                ))
            },
        )?;

    // Don't serialize the whole Volume, as the data field contains key material!
    Ok(freed_datasets_regions_and_volumes
        .into_iter()
        .map(|x| (x.0, x.1, x.2.map(|v: Volume| v.id())))
        .collect())
}

async fn svd_delete_freed_crucible_regions(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();

    // Find regions freed up for deletion by a previous saga node deleting the
    // region snapshots.
    let freed_datasets_regions_and_volumes =
        sagactx.lookup::<FreedCrucibleRegions>("freed_crucible_regions")?;

    for (dataset, region, volume_id) in freed_datasets_regions_and_volumes {
        // Send DELETE calls to the corresponding Crucible agents
        osagactx
            .nexus()
            .delete_crucible_regions(
                log,
                vec![(dataset.clone(), region.clone())],
            )
            .await
            .map_err(|e| {
                ActionError::action_failed(format!(
                    "failed to delete_crucible_regions: {:?}",
                    e,
                ))
            })?;

        // Remove region DB record
        osagactx
            .datastore()
            .regions_hard_delete(log, vec![region.id()])
            .await
            .map_err(|e| {
                ActionError::action_failed(format!(
                    "failed to regions_hard_delete: {:?}",
                    e,
                ))
            })?;

        // Remove volume DB record
        if let Some(volume_id) = volume_id {
            osagactx.datastore().volume_hard_delete(volume_id).await.map_err(
                |e| {
                    ActionError::action_failed(format!(
                        "failed to volume_hard_delete {}: {:?}",
                        volume_id, e,
                    ))
                },
            )?;
        }
    }

    Ok(())
}

/// Hard delete the volume record
async fn svd_hard_delete_volume_record(
    sagactx: NexusActionContext,
) -> Result<bool, ActionError> {
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
        .map_err(|e| {
            ActionError::action_failed(format!(
                "failed to get_allocated_regions for {}: {:?}",
                params.volume_id, e,
            ))
        })?;

    let log = sagactx.user_data().log();

    if !allocated_regions.is_empty() {
        info!(
            &log,
            "allocated regions for {} is not-empty, skipping volume_hard_delete",
            params.volume_id,
        );
        return Ok(false);
    }

    info!(
        &log,
        "allocated regions for {} is empty, calling volume_hard_delete",
        params.volume_id,
    );

    osagactx.datastore().volume_hard_delete(params.volume_id).await.map_err(
        |e| {
            ActionError::action_failed(format!(
                "failed to volume_hard_delete {}: {:?}",
                params.volume_id, e,
            ))
        },
    )?;

    Ok(true)
}
