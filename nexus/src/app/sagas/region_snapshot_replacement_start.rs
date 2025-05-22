// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! In the same way that read/write regions need to be replaced when a physical
//! disk is expunged, read-only regions need to be replaced too: Volumes are in
//! a similarly degraded state when the read-only Downstairs have gone away, and
//! remain in this degraded state until a new Region replaces the one that is
//! gone.
//!
//! It's this saga's responsibility to start that replacement process. This saga
//! handles the following region snapshot replacement request state transitions:
//!
//! ```text
//!         Requested   <--
//!                       |
//!             |         |
//!             v         |
//!                       |
//!         Allocating  --
//!
//!             |
//!             v
//!
//!       ReplacementDone
//! ```
//!
//! The first thing this saga does is set itself as the "operating saga" for the
//! request, and change the state to "Allocating". Then, it performs the
//! following steps:
//!
//! 1. Allocate a new region
//!
//! 2. Create a blank volume that can be later deleted to stash the snapshot
//!    being replaced. This is populated in the `volume_replace_snapshot`
//!    transaction so that `volume_references` for the corresponding region
//!    snapshot remains accurate.
//!
//! 3. For the affected Volume, swap the snapshot being replaced with the new
//!    region.
//!
//! 4. Update the region snapshot replacement request by clearing the operating
//!    saga id and changing the state to "ReplacementDone".
//!
//! Any unwind will place the state back into Requested.
//!
//! See the documentation for the "region snapshot replacement garbage collect"
//! saga for the next step in the process.

use super::{
    ACTION_GENERATE_ID, ActionRegistry, NexusActionContext, NexusSaga,
    SagaInitError,
};
use crate::app::RegionAllocationStrategy;
use crate::app::db::datastore::ExistingTarget;
use crate::app::db::datastore::RegionAllocationFor;
use crate::app::db::datastore::RegionAllocationParameters;
use crate::app::db::datastore::ReplacementTarget;
use crate::app::db::datastore::VolumeReplaceResult;
use crate::app::db::datastore::VolumeToDelete;
use crate::app::db::datastore::VolumeWithTarget;
use crate::app::sagas::common_storage::find_only_new_region;
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, db};
use nexus_db_model::ReadOnlyTargetReplacement;
use nexus_db_queries::db::datastore::NewRegionVolumeId;
use nexus_db_queries::db::datastore::OldSnapshotVolumeId;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::VolumeUuid;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::CrucibleOpts;
use sled_agent_client::VolumeConstructionRequest;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// region snapshot replacement start saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub request: db::model::RegionSnapshotReplacement,
    pub allocation_strategy: RegionAllocationStrategy,
}

// region snapshot replacement start saga: actions

declare_saga_actions! {
    region_snapshot_replacement_start;
    SET_SAGA_ID -> "unused_1" {
        + rsrss_set_saga_id
        - rsrss_set_saga_id_undo
    }
    GET_REQUEST_SNAPSHOT_AND_REGION_ID -> "snapshot_and_region_id" {
        + rsrss_get_snapshot_and_region_id
    }
    GET_CLONE_SOURCE -> "clone_source" {
        + rsrss_get_clone_source
    }
    GET_ALLOC_REGION_PARAMS -> "alloc_region_params" {
        + rsrss_get_alloc_region_params
    }
    ALLOC_NEW_REGION -> "new_datasets_and_regions" {
        + rsrss_alloc_new_region
        - rsrss_alloc_new_region_undo
    }
    FIND_NEW_REGION -> "new_dataset_and_region" {
        + rsrss_find_new_region
    }
    // One of the common sharp edges of sagas is that the compensating action of
    // a node does _not_ run if the forward action fails. Said another way, for
    // this node:
    //
    // EXAMPLE -> "output" {
    //   + forward_action
    //   - forward_action_undo
    // }
    //
    // If `forward_action` fails, `forward_action_undo` is never executed.
    // Forward actions are therefore required to be atomic, in that they either
    // fully apply or don't apply at all.
    //
    // Sagas with nodes that ensure multiple regions exist cannot be atomic
    // because they can partially fail (for example: what if only 2 out of 3
    // ensures succeed?). In order for the compensating action to be run, it
    // must exist as a separate node that has a no-op forward action:
    //
    // EXAMPLE_UNDO -> "not_used" {
    //   + noop
    //   - forward_action_undo
    // }
    // EXAMPLE -> "output" {
    //   + forward_action
    // }
    //
    // This saga will only ever ensure that a single region exists, so you might
    // think you could get away with a single node that combines the forward and
    // compensating action - you'd be mistaken! The Crucible agent's region
    // ensure is not atomic in all cases: if the region fails to create, it
    // enters the `failed` state, but is not deleted. Nexus must clean these up.
    NEW_REGION_ENSURE_UNDO -> "not_used" {
        + rsrss_noop
        - rsrss_new_region_ensure_undo
    }
    NEW_REGION_ENSURE -> "ensured_dataset_and_region" {
        + rsrss_new_region_ensure
    }
    NEW_REGION_VOLUME_CREATE -> "new_region_volume" {
        + rsrss_new_region_volume_create
        - rsrss_new_region_volume_create_undo
    }
    GET_OLD_SNAPSHOT_VOLUME_ID -> "old_snapshot_volume_id" {
        + rsrss_get_old_snapshot_volume_id
    }
    CREATE_FAKE_VOLUME -> "unused_2" {
        + rsrss_create_fake_volume
        - rsrss_create_fake_volume_undo
    }
    REPLACE_SNAPSHOT_IN_VOLUME -> "unused_3" {
        + rsrss_replace_snapshot_in_volume
        - rsrss_replace_snapshot_in_volume_undo
    }
    UPDATE_REQUEST_RECORD -> "unused_4" {
        + rsrss_update_request_record
    }
}

// region snapshot replacement start saga: definition

#[derive(Debug)]
pub(crate) struct SagaRegionSnapshotReplacementStart;
impl NexusSaga for SagaRegionSnapshotReplacementStart {
    const NAME: &'static str = "region-snapshot-replacement-start";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        region_snapshot_replacement_start_register_actions(registry);
    }

    fn make_saga_dag(
        _params: &Self::Params,
        mut builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError> {
        builder.append(Node::action(
            "saga_id",
            "GenerateSagaId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(Node::action(
            "new_volume_id",
            "GenerateNewVolumeId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(Node::action(
            "new_region_volume_id",
            "GenerateNewRegionVolumeId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(set_saga_id_action());
        builder.append(get_request_snapshot_and_region_id_action());
        builder.append(get_clone_source_action());
        builder.append(get_alloc_region_params_action());
        builder.append(alloc_new_region_action());
        builder.append(find_new_region_action());
        builder.append(new_region_ensure_undo_action());
        builder.append(new_region_ensure_action());
        builder.append(new_region_volume_create_action());
        builder.append(get_old_snapshot_volume_id_action());
        builder.append(create_fake_volume_action());
        builder.append(replace_snapshot_in_volume_action());
        builder.append(update_request_record_action());

        Ok(builder.build()?)
    }
}

// region snapshot replacement start saga: action implementations

async fn rsrss_set_saga_id(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let saga_id = sagactx.lookup::<Uuid>("saga_id")?;

    // Change the request record here to an intermediate "allocating" state to
    // block out other sagas that will be triggered for the same request. This
    // avoids Nexus allocating a bunch of replacement read-only regions only to
    // unwind all but one.
    osagactx
        .datastore()
        .set_region_snapshot_replacement_allocating(
            &opctx,
            params.request.id,
            saga_id,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn rsrss_set_saga_id_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let saga_id = sagactx.lookup::<Uuid>("saga_id")?;

    osagactx
        .datastore()
        .undo_set_region_snapshot_replacement_allocating(
            &opctx,
            params.request.id,
            saga_id,
        )
        .await?;

    Ok(())
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
enum CloneSource {
    RegionSnapshot { dataset_id: DatasetUuid, region_id: Uuid },
    Region { region_id: Uuid },
}

async fn rsrss_get_snapshot_and_region_id(
    sagactx: NexusActionContext,
) -> Result<(Uuid, Uuid), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (snapshot_id, region_id) = match params.request.replacement_type() {
        ReadOnlyTargetReplacement::RegionSnapshot {
            region_id,
            snapshot_id,
            ..
        } => (snapshot_id, region_id),

        ReadOnlyTargetReplacement::ReadOnlyRegion { region_id } => {
            let Some(region) = osagactx
                .datastore()
                .get_region_optional(region_id)
                .await
                .map_err(ActionError::action_failed)?
            else {
                return Err(ActionError::action_failed(Error::internal_error(
                    &format!("region {region_id} deleted"),
                )));
            };

            let maybe_snapshot = osagactx
                .datastore()
                .find_snapshot_by_volume_id(&opctx, region.volume_id())
                .await
                .map_err(ActionError::action_failed)?;

            match maybe_snapshot {
                Some(snapshot) => (snapshot.id(), region.id()),

                None => {
                    return Err(ActionError::action_failed(
                        Error::internal_error(&format!(
                            "region {} volume {} deleted",
                            region.id(),
                            region.volume_id(),
                        )),
                    ));
                }
            }
        }
    };

    Ok((snapshot_id, region_id))
}

async fn rsrss_get_clone_source(
    sagactx: NexusActionContext,
) -> Result<CloneSource, ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let log = osagactx.log();

    // Find either a region snapshot or a read-only region that is associated
    // with the request snapshot that has not been expunged, and return that as
    // the source to be used to populate the read-only region that will replace
    // the request's region snapshot.
    //
    // Importantly, determine the clone source before new region alloc step in
    // this saga, otherwise the query that searches for read-only region
    // candidates will match the newly allocated region (that is not created
    // yet!).
    //
    // Choose a clone source based on the following policy:
    //
    // - choose a region snapshot associated with the one being replaced
    //
    // - choose a read-only region from the associated snapshot volume
    //
    // - choose the region snapshot being replaced (only if it is not expunged!
    //   if the downstairs being cloned from is on an expunged dataset, we have
    //   to assume that the clone will never succeed, even if the expunged
    //   thing is still there)
    //
    // The policy here prefers to choose a clone source that isn't the region
    // snapshot in the request: if it's flaky, it shouldn't be used as a clone
    // source! This function does not know _why_ the replacement request was
    // created for that region snapshot, and assumes that there may be a problem
    // with it and will choose it as a last resort (if no other candidate clone
    // source is found and the request's region snapshot is not on an expunged
    // dataset, then it has to be chosen as a clone source, as the alternative
    // is lost data). The request's region snapshot may also be completely fine,
    // for example if a scrub is being requested.
    //
    // Also, the policy also prefers to choose to clone from a region snapshot
    // instead of a read-only region: this is an arbitrary order, there is no
    // reason behind this. The region snapshots and read-only regions will have
    // identical contents.

    let (snapshot_id, _) =
        sagactx.lookup::<(Uuid, Uuid)>("snapshot_and_region_id")?;

    // First, try to select another region snapshot that's part of this
    // snapshot.

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let mut non_expunged_region_snapshots = osagactx
        .datastore()
        .find_non_expunged_region_snapshots(&opctx, snapshot_id)
        .await
        .map_err(ActionError::action_failed)?;

    // Filter out the request's region snapshot, if appropriate - if there are
    // no other candidates, this could be chosen later in this function, but it
    // may be experiencing problems and shouldn't be the first choice for a
    // clone source.

    match params.request.replacement_type() {
        ReadOnlyTargetReplacement::RegionSnapshot {
            dataset_id,
            region_id,
            snapshot_id,
        } => {
            non_expunged_region_snapshots.retain(|rs| {
                !(rs.dataset_id == dataset_id
                    && rs.region_id == region_id
                    && rs.snapshot_id == snapshot_id)
            });
        }

        ReadOnlyTargetReplacement::ReadOnlyRegion { .. } => {
            // no-op
        }
    }

    if let Some(candidate) = non_expunged_region_snapshots.pop() {
        info!(
            log,
            "found another non-expunged region snapshot";
            "snapshot_id" => %snapshot_id,
            "dataset_id" => %candidate.dataset_id,
            "region_id" => %candidate.region_id,
        );

        return Ok(CloneSource::RegionSnapshot {
            dataset_id: candidate.dataset_id.into(),
            region_id: candidate.region_id,
        });
    }

    // Next, try to select a read-only region that's associated with the
    // snapshot volume

    info!(
        log,
        "no region snapshot clone source candidates";
        "snapshot_id" => %snapshot_id,
    );

    // Look up the existing snapshot
    let maybe_db_snapshot = osagactx
        .datastore()
        .snapshot_get(&opctx, snapshot_id)
        .await
        .map_err(ActionError::action_failed)?;

    let Some(db_snapshot) = maybe_db_snapshot else {
        return Err(ActionError::action_failed(Error::internal_error(
            &format!("snapshot {} was hard deleted!", snapshot_id),
        )));
    };

    let mut non_expunged_read_only_regions = osagactx
        .datastore()
        .find_non_expunged_regions(&opctx, db_snapshot.volume_id())
        .await
        .map_err(ActionError::action_failed)?;

    // Filter out the request's region, if appropriate.

    match params.request.replacement_type() {
        ReadOnlyTargetReplacement::RegionSnapshot { .. } => {
            // no-op
        }

        ReadOnlyTargetReplacement::ReadOnlyRegion { region_id } => {
            non_expunged_read_only_regions.retain(|r| r.id() != region_id);
        }
    }

    if let Some(candidate) = non_expunged_read_only_regions.pop() {
        info!(
            log,
            "found region clone source candidate";
            "snapshot_id" => %snapshot_id,
            "dataset_id" => %candidate.dataset_id(),
            "region_id" => %candidate.id(),
        );

        return Ok(CloneSource::Region { region_id: candidate.id() });
    }

    // If no other non-expunged region snapshot or read-only region exists, then
    // check if the request's read-only target is non-expunged. This will use
    // the region snapshot or read-only region that is being replaced as a clone
    // source, which may not work if there's a problem with it that this
    // replacement request is meant to fix!

    match params.request.replacement_type() {
        ReadOnlyTargetReplacement::RegionSnapshot {
            dataset_id,
            region_id,
            ..
        } => {
            let request_dataset_on_in_service_physical_disk = osagactx
                .datastore()
                .crucible_dataset_physical_disk_in_service(dataset_id.into())
                .await
                .map_err(ActionError::action_failed)?;

            if request_dataset_on_in_service_physical_disk {
                // If the request region snapshot's dataset has not been
                // expunged, it can be used

                info!(
                    log,
                    "using request region snapshot as clone source candidate"
                );

                return Ok(CloneSource::RegionSnapshot {
                    dataset_id: dataset_id.into(),
                    region_id,
                });
            }
        }

        ReadOnlyTargetReplacement::ReadOnlyRegion { region_id } => {
            let Some(region) = osagactx
                .datastore()
                .get_region_optional(region_id)
                .await
                .map_err(ActionError::action_failed)?
            else {
                return Err(ActionError::action_failed(Error::internal_error(
                    &format!("region {region_id} deleted"),
                )));
            };

            let request_dataset_on_in_service_physical_disk = osagactx
                .datastore()
                .crucible_dataset_physical_disk_in_service(region.dataset_id())
                .await
                .map_err(ActionError::action_failed)?;

            if request_dataset_on_in_service_physical_disk {
                // If the request read-only region's dataset has not been
                // expunged, it can be used.

                info!(
                    log,
                    "using request read-only region as clone source candidate"
                );

                return Ok(CloneSource::Region { region_id });
            }
        }
    }

    // If all targets of a Volume::Region are on expunged datasets, then the
    // user's data is gone, and this code will fail to select a clone source.

    return Err(ActionError::action_failed(format!(
        "no clone source candidate for {}!",
        snapshot_id,
    )));
}

#[derive(Debug, Deserialize, Serialize)]
struct AllocRegionParams {
    block_size: u64,
    blocks_per_extent: u64,
    extent_count: u64,
    current_allocated_regions:
        Vec<(db::model::CrucibleDataset, db::model::Region)>,
    snapshot_id: Uuid,
    snapshot_volume_id: VolumeUuid,
}

async fn rsrss_get_alloc_region_params(
    sagactx: NexusActionContext,
) -> Result<AllocRegionParams, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // Look up the existing snapshot

    let (snapshot_id, region_id) =
        sagactx.lookup::<(Uuid, Uuid)>("snapshot_and_region_id")?;

    let maybe_db_snapshot = osagactx
        .datastore()
        .snapshot_get(&opctx, snapshot_id)
        .await
        .map_err(ActionError::action_failed)?;

    let Some(db_snapshot) = maybe_db_snapshot else {
        return Err(ActionError::action_failed(Error::internal_error(
            &format!("snapshot {} was hard deleted!", snapshot_id),
        )));
    };

    // Find the region to replace
    let db_region = osagactx
        .datastore()
        .get_region(region_id)
        .await
        .map_err(ActionError::action_failed)?;

    let current_allocated_regions = osagactx
        .datastore()
        .get_allocated_regions(db_snapshot.volume_id())
        .await
        .map_err(ActionError::action_failed)?;

    Ok(AllocRegionParams {
        block_size: db_region.block_size().to_bytes(),
        blocks_per_extent: db_region.blocks_per_extent(),
        extent_count: db_region.extent_count(),
        current_allocated_regions,
        snapshot_id: db_snapshot.id(),
        snapshot_volume_id: db_snapshot.volume_id(),
    })
}

async fn rsrss_alloc_new_region(
    sagactx: NexusActionContext,
) -> Result<Vec<(db::model::CrucibleDataset, db::model::Region)>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let alloc_region_params =
        sagactx.lookup::<AllocRegionParams>("alloc_region_params")?;

    // Request an additional region for this snapshot volume. It's important
    // _not_ to delete the existing snapshot first, as (if it's still there)
    // then the Crucible agent could reuse the allocated port and cause trouble.
    let datasets_and_regions = osagactx
        .datastore()
        .arbitrary_region_allocate(
            &opctx,
            RegionAllocationFor::SnapshotVolume {
                volume_id: alloc_region_params.snapshot_volume_id,
                snapshot_id: alloc_region_params.snapshot_id,
            },
            RegionAllocationParameters::FromRaw {
                block_size: alloc_region_params.block_size,
                blocks_per_extent: alloc_region_params.blocks_per_extent,
                extent_count: alloc_region_params.extent_count,
            },
            &params.allocation_strategy,
            alloc_region_params.current_allocated_regions.len() + 1,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(datasets_and_regions)
}

async fn rsrss_alloc_new_region_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();

    let alloc_region_params =
        sagactx.lookup::<AllocRegionParams>("alloc_region_params")?;

    let maybe_dataset_and_region = find_only_new_region(
        log,
        alloc_region_params.current_allocated_regions,
        sagactx
            .lookup::<Vec<(db::model::CrucibleDataset, db::model::Region)>>(
                "new_datasets_and_regions",
            )?,
    );

    // It should be guaranteed that if rsrss_alloc_new_region succeeded then it
    // would have bumped the region redundancy, so we should see something here.
    // Guard against the case anyway.
    if let Some(dataset_and_region) = maybe_dataset_and_region {
        let (_, region) = dataset_and_region;
        osagactx
            .datastore()
            .regions_hard_delete(log, vec![region.id()])
            .await?;
    } else {
        warn!(&log, "maybe_dataset_and_region is None!");
    }

    Ok(())
}

async fn rsrss_find_new_region(
    sagactx: NexusActionContext,
) -> Result<(db::model::CrucibleDataset, db::model::Region), ActionError> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();

    let alloc_region_params =
        sagactx.lookup::<AllocRegionParams>("alloc_region_params")?;

    let maybe_dataset_and_region = find_only_new_region(
        log,
        alloc_region_params.current_allocated_regions,
        sagactx
            .lookup::<Vec<(db::model::CrucibleDataset, db::model::Region)>>(
                "new_datasets_and_regions",
            )?,
    );

    let Some(dataset_and_region) = maybe_dataset_and_region else {
        return Err(ActionError::action_failed(Error::internal_error(
            &format!(
                "expected dataset and region, saw {:?}!",
                maybe_dataset_and_region,
            ),
        )));
    };

    Ok(dataset_and_region)
}

async fn rsrss_noop(_sagactx: NexusActionContext) -> Result<(), ActionError> {
    Ok(())
}

async fn rsrss_new_region_ensure(
    sagactx: NexusActionContext,
) -> Result<
    (nexus_db_model::CrucibleDataset, crucible_agent_client::types::Region),
    ActionError,
> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();

    let new_dataset_and_region = sagactx
        .lookup::<(db::model::CrucibleDataset, db::model::Region)>(
            "new_dataset_and_region",
        )?;

    let (snapshot_id, _) =
        sagactx.lookup::<(Uuid, Uuid)>("snapshot_and_region_id")?;
    let clone_source = sagactx.lookup::<CloneSource>("clone_source")?;

    let mut source_repair_addr: SocketAddrV6 = match clone_source {
        CloneSource::RegionSnapshot { dataset_id, region_id } => {
            let region_snapshot = osagactx
                .datastore()
                .region_snapshot_get(dataset_id, region_id, snapshot_id)
                .await
                .map_err(ActionError::action_failed)?;

            let Some(region_snapshot) = region_snapshot else {
                return Err(ActionError::action_failed(format!(
                    "region snapshot {} {} {} deleted!",
                    dataset_id, region_id, snapshot_id,
                )));
            };

            match region_snapshot.snapshot_addr.parse() {
                Ok(addr) => addr,

                Err(e) => {
                    return Err(ActionError::action_failed(format!(
                        "error parsing region_snapshot.snapshot_addr: {e}"
                    )));
                }
            }
        }

        CloneSource::Region { region_id } => {
            let maybe_addr = osagactx
                .datastore()
                .region_addr(region_id)
                .await
                .map_err(ActionError::action_failed)?;

            match maybe_addr {
                Some(addr) => addr,

                None => {
                    return Err(ActionError::action_failed(format!(
                        "region clone source {region_id} has no port!"
                    )));
                }
            }
        }
    };

    // Currently, the repair port is set using a fixed offset above the
    // downstairs port. Once this goes away, Nexus will require a way to query
    // for the repair port!

    source_repair_addr.set_port(
        source_repair_addr.port() + crucible_common::REPAIR_PORT_OFFSET,
    );

    let (new_dataset, new_region) = new_dataset_and_region;

    let ensured_region = osagactx
        .nexus()
        .ensure_region_in_dataset(
            log,
            &new_dataset,
            &new_region,
            Some(source_repair_addr.to_string()),
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok((new_dataset, ensured_region))
}

async fn rsrss_new_region_ensure_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();

    warn!(log, "rsrss_new_region_ensure_undo: Deleting crucible regions");

    let new_dataset_and_region = sagactx
        .lookup::<(db::model::CrucibleDataset, db::model::Region)>(
            "new_dataset_and_region",
        )?;

    osagactx
        .nexus()
        .delete_crucible_regions(log, vec![new_dataset_and_region])
        .await?;

    Ok(())
}

async fn rsrss_new_region_volume_create(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();

    let new_region_volume_id =
        sagactx.lookup::<VolumeUuid>("new_region_volume_id")?;

    let (new_dataset, ensured_region) =
        sagactx.lookup::<(
            db::model::CrucibleDataset,
            crucible_agent_client::types::Region,
        )>("ensured_dataset_and_region")?;

    let new_dataset_address = new_dataset.address();
    let new_region_address = SocketAddr::V6(SocketAddrV6::new(
        *new_dataset_address.ip(),
        ensured_region.port_number,
        0,
        0,
    ));

    // Create a volume to inflate the reference count of the newly created
    // read-only region. If this is not done it's possible that a user could
    // delete the snapshot volume _after_ the new read-only region was swapped
    // in, removing the last reference to it and causing garbage collection.

    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: new_region_volume_id.into_untyped_uuid(),
        block_size: 0,
        sub_volumes: vec![VolumeConstructionRequest::Region {
            block_size: 0,
            blocks_per_extent: 0,
            extent_count: 0,
            gen: 0,
            opts: CrucibleOpts {
                id: new_region_volume_id.into_untyped_uuid(),
                target: vec![new_region_address],
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

    osagactx
        .datastore()
        .volume_create(new_region_volume_id, volume_construction_request)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn rsrss_new_region_volume_create_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();

    // Delete the volume.

    let new_region_volume_id =
        sagactx.lookup::<VolumeUuid>("new_region_volume_id")?;
    osagactx.datastore().volume_hard_delete(new_region_volume_id).await?;

    Ok(())
}

async fn rsrss_get_old_snapshot_volume_id(
    sagactx: NexusActionContext,
) -> Result<VolumeUuid, ActionError> {
    // Save the snapshot's original volume ID, because we'll be altering it and
    // need the original

    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (snapshot_id, _) =
        sagactx.lookup::<(Uuid, Uuid)>("snapshot_and_region_id")?;

    // Look up the existing snapshot
    let maybe_db_snapshot = osagactx
        .datastore()
        .snapshot_get(&opctx, snapshot_id)
        .await
        .map_err(ActionError::action_failed)?;

    let Some(db_snapshot) = maybe_db_snapshot else {
        return Err(ActionError::action_failed(Error::internal_error(
            &format!("snapshot {} was hard deleted!", snapshot_id),
        )));
    };

    Ok(db_snapshot.volume_id())
}

async fn rsrss_create_fake_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();

    let new_volume_id = sagactx.lookup::<VolumeUuid>("new_volume_id")?;

    // Create a fake volume record for the old snapshot target. This will be
    // deleted after snapshot replacement has finished. It can be completely
    // blank here, it will be replaced by `volume_replace_snapshot`.

    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: *new_volume_id.as_untyped_uuid(),
        block_size: 0,
        sub_volumes: vec![VolumeConstructionRequest::Region {
            block_size: 0,
            blocks_per_extent: 0,
            extent_count: 0,
            gen: 0,
            opts: CrucibleOpts {
                id: *new_volume_id.as_untyped_uuid(),
                // Do not put the new region ID here: it will be deleted during
                // the associated garbage collection saga if
                // `volume_replace_snapshot` does not perform the swap (which
                // happens when the snapshot volume is deleted) and we still
                // want it to exist when performing other replacement steps. If
                // the replacement does occur, then the old address will swapped
                // in here.
                target: vec![],
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

    osagactx
        .datastore()
        .volume_create(new_volume_id, volume_construction_request)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn rsrss_create_fake_volume_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();

    // Delete the fake volume.

    let new_volume_id = sagactx.lookup::<VolumeUuid>("new_volume_id")?;
    osagactx.datastore().volume_hard_delete(new_volume_id).await?;

    Ok(())
}

#[derive(Debug)]
struct ReplaceParams {
    old_volume_id: VolumeUuid,
    old_target_address: SocketAddrV6,
    new_region_address: SocketAddrV6,
    new_volume_id: VolumeUuid,
}

async fn get_replace_params(
    sagactx: &NexusActionContext,
) -> Result<ReplaceParams, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let new_volume_id = sagactx.lookup::<VolumeUuid>("new_volume_id")?;

    let Some(old_target_address) = osagactx
        .datastore()
        .read_only_target_addr(&params.request)
        .await
        .map_err(ActionError::action_failed)?
    else {
        // This is ok - the next background task invocation will move the
        // request state forward appropriately.
        return Err(ActionError::action_failed(format!(
            "request {} target deleted!",
            params.request.id,
        )));
    };

    let (new_dataset, ensured_region) =
        sagactx.lookup::<(
            db::model::CrucibleDataset,
            crucible_agent_client::types::Region,
        )>("ensured_dataset_and_region")?;

    let new_dataset_address = new_dataset.address();
    let new_region_address = SocketAddrV6::new(
        *new_dataset_address.ip(),
        ensured_region.port_number,
        0,
        0,
    );

    let old_volume_id =
        sagactx.lookup::<VolumeUuid>("old_snapshot_volume_id")?;

    // Return the replacement parameters for the forward action case - the undo
    // will swap the existing and replacement target
    Ok(ReplaceParams {
        old_volume_id,
        old_target_address,
        new_region_address,
        new_volume_id,
    })
}

async fn rsrss_replace_snapshot_in_volume(
    sagactx: NexusActionContext,
) -> Result<VolumeReplaceResult, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();

    let replacement_params = get_replace_params(&sagactx).await?;

    info!(
        log,
        "replacing {} with {} in volume {}",
        replacement_params.old_target_address,
        replacement_params.new_region_address,
        replacement_params.old_volume_id,
    );

    // `volume_replace_snapshot` will swap the old snapshot for the new region.
    // No repair or reconcilation needs to occur after this.
    let volume_replace_snapshot_result = osagactx
        .datastore()
        .volume_replace_snapshot(
            VolumeWithTarget(replacement_params.old_volume_id),
            ExistingTarget(replacement_params.old_target_address),
            ReplacementTarget(replacement_params.new_region_address),
            VolumeToDelete(replacement_params.new_volume_id),
        )
        .await
        .map_err(ActionError::action_failed)?;

    match volume_replace_snapshot_result {
        VolumeReplaceResult::AlreadyHappened | VolumeReplaceResult::Done => {
            // The replacement was done either by this run of this saga node, or
            // a previous one (and this is a rerun). This can only be returned
            // if the transaction occurred on the non-deleted volume so proceed
            // with the rest of the saga.

            Ok(volume_replace_snapshot_result)
        }

        VolumeReplaceResult::ExistingVolumeSoftDeleted
        | VolumeReplaceResult::ExistingVolumeHardDeleted => {
            // If the snapshot volume was deleted, we still want to proceed with
            // replacing the rest of the uses of the region snapshot. Note this
            // also covers the case where this saga node runs (performing the
            // replacement), the executor crashes before it can record that
            // success, and then before this node is rerun the snapshot is
            // deleted. If this saga unwound here, that would violate the
            // property of idempotency.

            Ok(volume_replace_snapshot_result)
        }
    }
}

async fn rsrss_replace_snapshot_in_volume_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    // Undo the forward action's volume_replace_snapshot call by swapping the
    // existing target and replacement target parameters.

    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();

    let replacement_params = get_replace_params(&sagactx).await?;

    // Note the old and new are _not_ swapped in this log message! The intention
    // is that someone reviewing the logs could search for "replacing UUID with
    // UUID in volume UUID" and get (in the case of no re-execution) two
    // results.
    info!(
        log,
        "undo: replacing {} with {} in volume {}",
        replacement_params.old_target_address,
        replacement_params.new_region_address,
        replacement_params.old_volume_id,
    );

    // Note only the ExistingTarget and ReplacementTarget arguments are swapped
    // here!
    //
    // It's ok if this function returns ExistingVolumeDeleted here: we don't
    // want to throw an error and cause the saga to be stuck unwinding, as this
    // would hold the lock on the replacement request.
    let volume_replace_snapshot_result = osagactx
        .datastore()
        .volume_replace_snapshot(
            VolumeWithTarget(replacement_params.old_volume_id),
            ExistingTarget(replacement_params.new_region_address),
            ReplacementTarget(replacement_params.old_target_address),
            VolumeToDelete(replacement_params.new_volume_id),
        )
        .await?;

    info!(
        log,
        "undo: volume_replace_snapshot returned {:?}",
        volume_replace_snapshot_result,
    );

    Ok(())
}

async fn rsrss_update_request_record(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let datastore = osagactx.datastore();
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let saga_id = sagactx.lookup::<Uuid>("saga_id")?;
    let new_dataset_and_region = sagactx
        .lookup::<(db::model::CrucibleDataset, db::model::Region)>(
            "new_dataset_and_region",
        )?;

    let new_region_id = new_dataset_and_region.1.id();

    let old_region_volume_id = sagactx.lookup::<VolumeUuid>("new_volume_id")?;

    let new_region_volume_id =
        sagactx.lookup::<VolumeUuid>("new_region_volume_id")?;

    // Now that the region has been ensured and the construction request has
    // been updated, update the replacement request record to 'ReplacementDone'
    // and clear the operating saga id. There is no undo step for this, it
    // should succeed idempotently.
    datastore
        .set_region_snapshot_replacement_replacement_done(
            &opctx,
            params.request.id,
            saga_id,
            new_region_id,
            NewRegionVolumeId(new_region_volume_id),
            OldSnapshotVolumeId(old_region_volume_id),
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

#[cfg(test)]
pub(crate) mod test {
    use crate::{
        app::RegionAllocationStrategy, app::db::DataStore,
        app::saga::create_saga_dag,
        app::sagas::region_snapshot_replacement_start::*,
        app::sagas::test_helpers::test_opctx,
    };
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::PhysicalDiskPolicy;
    use nexus_db_model::RegionSnapshotReplacement;
    use nexus_db_model::RegionSnapshotReplacementState;
    use nexus_db_model::Volume;
    use nexus_db_queries::authn::saga::Serialized;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils::resource_helpers::DiskTestBuilder;
    use nexus_test_utils::resource_helpers::create_disk;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::create_snapshot;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::external_api::views;
    use nexus_types::identity::Asset;
    use omicron_uuid_kinds::GenericUuid;
    use sled_agent_client::VolumeConstructionRequest;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const DISK_NAME: &str = "my-disk";
    const SNAPSHOT_NAME: &str = "my-snap";
    const PROJECT_NAME: &str = "springfield-squidport";

    /// Create four zpools, a disk, and a snapshot of that disk
    async fn prepare_for_test(
        cptestctx: &ControlPlaneTestContext,
    ) -> PrepareResult<'_> {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = test_opctx(cptestctx);

        assert_eq!(region_allocations(&datastore).await, 0);

        let disk_test =
            DiskTestBuilder::new(cptestctx).with_zpool_count(4).build().await;

        assert_eq!(region_allocations(&datastore).await, 0);

        let _project_id =
            create_project(&client, PROJECT_NAME).await.identity.id;

        assert_eq!(region_allocations(&datastore).await, 0);

        // Create a disk
        let disk = create_disk(&client, PROJECT_NAME, DISK_NAME).await;

        assert_eq!(region_allocations(&datastore).await, 3);

        let disk_id = disk.identity.id;
        let (.., db_disk) = LookupPath::new(&opctx, datastore)
            .disk_id(disk_id)
            .fetch()
            .await
            .unwrap_or_else(|_| panic!("test disk {:?} should exist", disk_id));

        // Create a snapshot
        let snapshot =
            create_snapshot(&client, PROJECT_NAME, DISK_NAME, SNAPSHOT_NAME)
                .await;

        assert_eq!(region_allocations(&datastore).await, 6);

        let snapshot_id = snapshot.identity.id;
        let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
            .snapshot_id(snapshot_id)
            .fetch()
            .await
            .unwrap_or_else(|_| {
                panic!("test snapshot {:?} should exist", snapshot_id)
            });

        PrepareResult { db_disk, snapshot, db_snapshot, disk_test }
    }

    struct PrepareResult<'a> {
        db_disk: nexus_db_model::Disk,
        snapshot: views::Snapshot,
        db_snapshot: nexus_db_model::Snapshot,
        disk_test: DiskTest<'a, crate::Server>,
    }

    #[nexus_test(server = crate::Server)]
    async fn test_region_snapshot_replacement_start_saga(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let PrepareResult { db_disk, snapshot, db_snapshot, .. } =
            prepare_for_test(cptestctx).await;

        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = test_opctx(cptestctx);

        // Assert disk has three allocated regions
        let disk_allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
        assert_eq!(disk_allocated_regions.len(), 3);

        // Assert the snapshot has zero allocated regions
        let snapshot_id = snapshot.identity.id;

        let snapshot_allocated_regions = datastore
            .get_allocated_regions(db_snapshot.volume_id())
            .await
            .unwrap();
        assert_eq!(snapshot_allocated_regions.len(), 0);

        // Replace one of the snapshot's targets
        let region: &nexus_db_model::Region = &disk_allocated_regions[0].1;

        let region_snapshot = datastore
            .region_snapshot_get(region.dataset_id(), region.id(), snapshot_id)
            .await
            .unwrap()
            .unwrap();

        // Manually insert the region snapshot replacement request
        let request =
            RegionSnapshotReplacement::for_region_snapshot(&region_snapshot);

        datastore
            .insert_region_snapshot_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        // Run the region snapshot replacement start saga
        let dag =
            create_saga_dag::<SagaRegionSnapshotReplacementStart>(Params {
                serialized_authn: Serialized::for_opctx(&opctx),
                request: request.clone(),
                allocation_strategy: RegionAllocationStrategy::Random {
                    seed: None,
                },
            })
            .unwrap();

        let runnable_saga = nexus.sagas.saga_prepare(dag).await.unwrap();

        // Actually run the saga
        runnable_saga.run_to_completion().await.unwrap();

        // Validate the state transition
        let result = datastore
            .get_region_snapshot_replacement_request_by_id(&opctx, request.id)
            .await
            .unwrap();

        assert_eq!(
            result.replacement_state,
            RegionSnapshotReplacementState::ReplacementDone
        );
        assert!(result.new_region_id.is_some());
        assert!(result.operating_saga_id.is_none());

        // Validate number of regions for disk didn't change
        let disk_allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
        assert_eq!(disk_allocated_regions.len(), 3);

        // Validate that the snapshot now has one allocated region
        let snapshot_allocated_datasets_and_regions = datastore
            .get_allocated_regions(db_snapshot.volume_id())
            .await
            .unwrap();

        assert_eq!(snapshot_allocated_datasets_and_regions.len(), 1);

        let (_, snapshot_allocated_region) =
            &snapshot_allocated_datasets_and_regions[0];

        // Validate that the snapshot's volume contains this newly allocated
        // region

        let new_region_addr = datastore
            .region_addr(snapshot_allocated_region.id())
            .await
            .unwrap()
            .unwrap();

        let volumes = datastore
            .find_volumes_referencing_socket_addr(
                &opctx,
                new_region_addr.into(),
            )
            .await
            .unwrap();

        assert!(
            volumes
                .iter()
                .map(|v| v.id())
                .any(|vid| vid == db_snapshot.volume_id())
        );
    }

    fn new_test_params(
        opctx: &OpContext,
        request: &RegionSnapshotReplacement,
    ) -> Params {
        Params {
            serialized_authn: Serialized::for_opctx(opctx),
            request: request.clone(),
            allocation_strategy: RegionAllocationStrategy::Random {
                seed: None,
            },
        }
    }

    pub(crate) async fn verify_clean_slate(
        cptestctx: &ControlPlaneTestContext,
        test: &DiskTest<'_, crate::Server>,
        request: &RegionSnapshotReplacement,
        affected_volume_original: &Volume,
    ) {
        let sled_agent = cptestctx.first_sled_agent();
        let datastore = cptestctx.server.server_context().nexus.datastore();

        crate::app::sagas::test_helpers::assert_no_failed_undo_steps(
            &cptestctx.logctx.log,
            datastore,
        )
        .await;

        // For these tests, six provisioned regions exist: three for the
        // original disk, and three for the (currently unused) snapshot
        // destination volume
        assert_eq!(region_allocations(&datastore).await, 6);

        // Assert that only those six provisioned regions are non-destroyed
        assert_no_other_ensured_regions(sled_agent, test, &datastore).await;

        assert_region_snapshot_replacement_request_untouched(
            cptestctx, &datastore, &request,
        )
        .await;
        assert_volume_untouched(&datastore, &affected_volume_original).await;
    }

    async fn regions(datastore: &DataStore) -> Vec<db::model::Region> {
        use async_bb8_diesel::AsyncConnection;
        use async_bb8_diesel::AsyncRunQueryDsl;
        use async_bb8_diesel::AsyncSimpleConnection;
        use diesel::QueryDsl;
        use diesel::SelectableHelper;
        use nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
        use nexus_db_schema::schema::region::dsl;

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        #[allow(clippy::disallowed_methods)]
        conn.transaction_async(|conn| async move {
            // Selecting all regions requires a full table scan
            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await.unwrap();

            dsl::region
                .select(db::model::Region::as_select())
                .get_results_async(&conn)
                .await
        })
        .await
        .unwrap()
    }

    async fn region_allocations(datastore: &DataStore) -> usize {
        regions(datastore).await.len()
    }

    async fn assert_no_other_ensured_regions(
        sled_agent: &omicron_sled_agent::sim::SledAgent,
        test: &DiskTest<'_, crate::Server>,
        datastore: &DataStore,
    ) {
        let mut non_destroyed_regions_from_agent = vec![];

        for zpool in test.zpools() {
            let dataset = zpool.crucible_dataset();
            let crucible_dataset =
                sled_agent.get_crucible_dataset(zpool.id, dataset.id);
            for region in crucible_dataset.list() {
                match region.state {
                    crucible_agent_client::types::State::Tombstoned
                    | crucible_agent_client::types::State::Destroyed => {
                        // ok
                    }

                    _ => {
                        non_destroyed_regions_from_agent.push(region.clone());
                    }
                }
            }
        }

        let db_regions = regions(datastore).await;
        let db_region_ids: Vec<Uuid> =
            db_regions.iter().map(|x| x.id()).collect();

        for region in non_destroyed_regions_from_agent {
            let region_id = region.id.0.parse().unwrap();
            let contains = db_region_ids.contains(&region_id);
            assert!(contains, "db does not have {:?}", region_id);
        }
    }

    async fn assert_region_snapshot_replacement_request_untouched(
        cptestctx: &ControlPlaneTestContext,
        datastore: &DataStore,
        request: &RegionSnapshotReplacement,
    ) {
        let opctx = test_opctx(cptestctx);
        let db_request = datastore
            .get_region_snapshot_replacement_request_by_id(&opctx, request.id)
            .await
            .unwrap();

        assert_eq!(db_request.new_region_id, None);
        assert_eq!(
            db_request.replacement_state,
            RegionSnapshotReplacementState::Requested
        );
        assert_eq!(db_request.operating_saga_id, None);
    }

    async fn assert_volume_untouched(
        datastore: &DataStore,
        affected_volume_original: &Volume,
    ) {
        let affected_volume = datastore
            .volume_get(affected_volume_original.id())
            .await
            .unwrap()
            .unwrap();

        let actual: VolumeConstructionRequest =
            serde_json::from_str(&affected_volume.data()).unwrap();

        let expected: VolumeConstructionRequest =
            serde_json::from_str(&affected_volume_original.data()).unwrap();

        assert_eq!(actual, expected);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let PrepareResult { db_disk, snapshot, db_snapshot, disk_test } =
            prepare_for_test(cptestctx).await;

        let log = &cptestctx.logctx.log;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = test_opctx(cptestctx);

        let disk_allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
        assert_eq!(disk_allocated_regions.len(), 3);

        let region: &nexus_db_model::Region = &disk_allocated_regions[0].1;
        let snapshot_id = snapshot.identity.id;

        let region_snapshot = datastore
            .region_snapshot_get(region.dataset_id(), region.id(), snapshot_id)
            .await
            .unwrap()
            .unwrap();

        let request =
            RegionSnapshotReplacement::for_region_snapshot(&region_snapshot);

        datastore
            .insert_region_snapshot_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        let affected_volume_original = datastore
            .volume_get(db_snapshot.volume_id())
            .await
            .unwrap()
            .unwrap();

        verify_clean_slate(
            &cptestctx,
            &disk_test,
            &request,
            &affected_volume_original,
        )
        .await;

        crate::app::sagas::test_helpers::action_failure_can_unwind::<
            SagaRegionSnapshotReplacementStart,
            _,
            _,
        >(
            nexus,
            || Box::pin(async { new_test_params(&opctx, &request) }),
            || {
                Box::pin(async {
                    verify_clean_slate(
                        &cptestctx,
                        &disk_test,
                        &request,
                        &affected_volume_original,
                    )
                    .await;
                })
            },
            log,
        )
        .await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let PrepareResult { db_disk, snapshot, db_snapshot, disk_test } =
            prepare_for_test(cptestctx).await;

        let log = &cptestctx.logctx.log;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = test_opctx(cptestctx);

        let disk_allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
        assert_eq!(disk_allocated_regions.len(), 3);

        let region: &nexus_db_model::Region = &disk_allocated_regions[0].1;
        let snapshot_id = snapshot.identity.id;

        let region_snapshot = datastore
            .region_snapshot_get(region.dataset_id(), region.id(), snapshot_id)
            .await
            .unwrap()
            .unwrap();

        let request =
            RegionSnapshotReplacement::for_region_snapshot(&region_snapshot);

        datastore
            .insert_region_snapshot_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        let affected_volume_original = datastore
            .volume_get(db_snapshot.volume_id())
            .await
            .unwrap()
            .unwrap();

        verify_clean_slate(
            &cptestctx,
            &disk_test,
            &request,
            &affected_volume_original,
        )
        .await;

        crate::app::sagas::test_helpers::action_failure_can_unwind_idempotently::<
            SagaRegionSnapshotReplacementStart,
            _,
            _
        >(
            nexus,
            || Box::pin(async { new_test_params(&opctx, &request) }),
            || Box::pin(async {
                verify_clean_slate(
                    &cptestctx,
                    &disk_test,
                    &request,
                    &affected_volume_original,
                ).await;
            }),
            log
        ).await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let PrepareResult { db_disk, snapshot, .. } =
            prepare_for_test(cptestctx).await;

        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = test_opctx(cptestctx);

        let disk_allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
        assert_eq!(disk_allocated_regions.len(), 3);

        let region: &nexus_db_model::Region = &disk_allocated_regions[0].1;
        let snapshot_id = snapshot.identity.id;

        let region_snapshot = datastore
            .region_snapshot_get(region.dataset_id(), region.id(), snapshot_id)
            .await
            .unwrap()
            .unwrap();

        let request =
            RegionSnapshotReplacement::for_region_snapshot(&region_snapshot);

        datastore
            .insert_region_snapshot_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        // Build the saga DAG with the provided test parameters
        let params = new_test_params(&opctx, &request);
        let dag = create_saga_dag::<SagaRegionSnapshotReplacementStart>(params)
            .unwrap();
        crate::app::sagas::test_helpers::actions_succeed_idempotently(
            nexus, dag,
        )
        .await;
    }

    /// Assert this saga does not leak regions if the replacement read-only
    /// region cannot be created.
    #[nexus_test(server = crate::Server)]
    async fn test_no_leak_region(cptestctx: &ControlPlaneTestContext) {
        let PrepareResult { db_disk, snapshot, db_snapshot, disk_test } =
            prepare_for_test(cptestctx).await;

        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = test_opctx(cptestctx);

        let disk_allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
        assert_eq!(disk_allocated_regions.len(), 3);

        let region: &nexus_db_model::Region = &disk_allocated_regions[0].1;
        let snapshot_id = snapshot.identity.id;

        let region_snapshot = datastore
            .region_snapshot_get(region.dataset_id(), region.id(), snapshot_id)
            .await
            .unwrap()
            .unwrap();

        let request =
            RegionSnapshotReplacement::for_region_snapshot(&region_snapshot);

        datastore
            .insert_region_snapshot_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        let affected_volume_original = datastore
            .volume_get(db_snapshot.volume_id())
            .await
            .unwrap()
            .unwrap();

        disk_test.set_always_fail_callback().await;

        // Run the region snapshot replacement start saga
        let dag =
            create_saga_dag::<SagaRegionSnapshotReplacementStart>(Params {
                serialized_authn: Serialized::for_opctx(&opctx),
                request: request.clone(),
                allocation_strategy: RegionAllocationStrategy::Random {
                    seed: None,
                },
            })
            .unwrap();

        let runnable_saga = nexus.sagas.saga_prepare(dag).await.unwrap();

        // Actually run the saga
        runnable_saga.run_to_completion().await.unwrap();

        verify_clean_slate(
            &cptestctx,
            &disk_test,
            &request,
            &affected_volume_original,
        )
        .await;
    }

    /// Tests that the region snapshot replacement start saga will not choose
    /// the request's region snapshot, but instead will choose the other
    /// non-expunged one.
    #[nexus_test(server = crate::Server)]
    async fn test_region_snapshot_replacement_start_prefer_not_self(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            datastore.clone(),
        );

        // Create four zpools, each with one dataset. This is required for
        // region and region snapshot replacement to have somewhere to move the
        // data, and for this test we're doing one expungements.
        let sled_id = cptestctx.first_sled_id();

        let disk_test = DiskTestBuilder::new(&cptestctx)
            .on_specific_sled(sled_id)
            .with_zpool_count(4)
            .build()
            .await;

        // Any volumes sent to the Pantry for reconciliation should return
        // active for this test

        cptestctx
            .first_sim_server()
            .pantry_server
            .as_ref()
            .unwrap()
            .pantry
            .set_auto_activate_volumes();

        // Create a disk and a snapshot
        let client = &cptestctx.external_client;
        let _project_id =
            create_project(&client, PROJECT_NAME).await.identity.id;

        let disk = create_disk(&client, PROJECT_NAME, "disk").await;
        let snapshot =
            create_snapshot(&client, PROJECT_NAME, "disk", "snap").await;

        // Before expunging any physical disk, save some DB models
        let (.., db_disk) = LookupPath::new(&opctx, datastore)
            .disk_id(disk.identity.id)
            .fetch()
            .await
            .unwrap();

        let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
            .snapshot_id(snapshot.identity.id)
            .fetch()
            .await
            .unwrap();

        let disk_allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
        let snapshot_allocated_regions = datastore
            .get_allocated_regions(db_snapshot.volume_id())
            .await
            .unwrap();

        assert_eq!(disk_allocated_regions.len(), 3);
        assert_eq!(snapshot_allocated_regions.len(), 0);

        // Expunge one physical disk
        {
            let (dataset, _) = &disk_allocated_regions[0];

            let zpool = disk_test
                .zpools()
                .find(|x| *x.id.as_untyped_uuid() == dataset.pool_id)
                .expect("Expected at least one zpool");

            let (_, db_zpool) = LookupPath::new(&opctx, datastore)
                .zpool_id(zpool.id.into_untyped_uuid())
                .fetch()
                .await
                .unwrap();

            datastore
                .physical_disk_update_policy(
                    &opctx,
                    db_zpool.physical_disk_id.into(),
                    PhysicalDiskPolicy::Expunged,
                )
                .await
                .unwrap();
        }

        // Request that the second region snapshot be replaced

        let region_snapshot = datastore
            .region_snapshot_get(
                disk_allocated_regions[1].0.id(), // dataset id
                disk_allocated_regions[1].1.id(), // region id
                snapshot.identity.id,
            )
            .await
            .unwrap()
            .unwrap();

        let request_id = datastore
            .create_region_snapshot_replacement_request(
                &opctx,
                &region_snapshot,
            )
            .await
            .unwrap();

        // Manually invoke the region snapshot replacement start saga

        let saga_outputs = nexus
            .sagas
            .saga_execute::<SagaRegionSnapshotReplacementStart>(Params {
                serialized_authn: Serialized::for_opctx(&opctx),

                request: datastore
                    .get_region_snapshot_replacement_request_by_id(
                        &opctx, request_id,
                    )
                    .await
                    .unwrap(),

                allocation_strategy: RegionAllocationStrategy::Random {
                    seed: None,
                },
            })
            .await
            .unwrap();

        // The third region snapshot should have been selected as the clone
        // source

        let selected_clone_source = saga_outputs
            .lookup_node_output::<CloneSource>("clone_source")
            .unwrap();

        assert_eq!(
            selected_clone_source,
            CloneSource::RegionSnapshot {
                dataset_id: disk_allocated_regions[2].0.id(),
                region_id: disk_allocated_regions[2].1.id(),
            },
        );

        let snapshot_allocated_regions = datastore
            .get_allocated_regions(db_snapshot.volume_id())
            .await
            .unwrap();

        assert_eq!(snapshot_allocated_regions.len(), 1);
        assert!(snapshot_allocated_regions.iter().all(|(_, r)| r.read_only()));
    }

    /// Tests that a region snapshot replacement request can select the region
    /// snapshot being replaced as a clone source (but only if it is not
    /// expunged!)
    #[nexus_test(server = crate::Server)]
    async fn test_region_snapshot_replacement_start_hail_mary(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            datastore.clone(),
        );

        // Create five zpools, each with one dataset. This is required for
        // region and region snapshot replacement to have somewhere to move the
        // data, and for this test we're doing two expungements.
        let sled_id = cptestctx.first_sled_id();

        let disk_test = DiskTestBuilder::new(&cptestctx)
            .on_specific_sled(sled_id)
            .with_zpool_count(5)
            .build()
            .await;

        // Any volumes sent to the Pantry for reconciliation should return
        // active for this test

        cptestctx
            .first_sim_server()
            .pantry_server
            .as_ref()
            .unwrap()
            .pantry
            .set_auto_activate_volumes();

        // Create a disk and a snapshot
        let client = &cptestctx.external_client;
        let _project_id =
            create_project(&client, PROJECT_NAME).await.identity.id;

        let disk = create_disk(&client, PROJECT_NAME, "disk").await;
        let snapshot =
            create_snapshot(&client, PROJECT_NAME, "disk", "snap").await;

        // Before expunging any physical disk, save some DB models
        let (.., db_disk) = LookupPath::new(&opctx, datastore)
            .disk_id(disk.identity.id)
            .fetch()
            .await
            .unwrap();

        let (.., db_snapshot) = LookupPath::new(&opctx, datastore)
            .snapshot_id(snapshot.identity.id)
            .fetch()
            .await
            .unwrap();

        let disk_allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
        let snapshot_allocated_regions = datastore
            .get_allocated_regions(db_snapshot.volume_id())
            .await
            .unwrap();

        assert_eq!(disk_allocated_regions.len(), 3);
        assert_eq!(snapshot_allocated_regions.len(), 0);

        // Expunge two physical disks
        for i in [0, 1] {
            let (dataset, _) = &disk_allocated_regions[i];

            let zpool = disk_test
                .zpools()
                .find(|x| *x.id.as_untyped_uuid() == dataset.pool_id)
                .expect("Expected at least one zpool");

            let (_, db_zpool) = LookupPath::new(&opctx, datastore)
                .zpool_id(zpool.id.into_untyped_uuid())
                .fetch()
                .await
                .unwrap();

            datastore
                .physical_disk_update_policy(
                    &opctx,
                    db_zpool.physical_disk_id.into(),
                    PhysicalDiskPolicy::Expunged,
                )
                .await
                .unwrap();
        }

        // Request that the third region snapshot be replaced

        let region_snapshot = datastore
            .region_snapshot_get(
                disk_allocated_regions[2].0.id(), // dataset id
                disk_allocated_regions[2].1.id(), // region id
                snapshot.identity.id,
            )
            .await
            .unwrap()
            .unwrap();

        let request_id = datastore
            .create_region_snapshot_replacement_request(
                &opctx,
                &region_snapshot,
            )
            .await
            .unwrap();

        // Manually invoke the region snapshot replacement start saga

        let saga_outputs = nexus
            .sagas
            .saga_execute::<SagaRegionSnapshotReplacementStart>(Params {
                serialized_authn: Serialized::for_opctx(&opctx),

                request: datastore
                    .get_region_snapshot_replacement_request_by_id(
                        &opctx, request_id,
                    )
                    .await
                    .unwrap(),

                allocation_strategy: RegionAllocationStrategy::Random {
                    seed: None,
                },
            })
            .await
            .unwrap();

        // This should have chosen the request's region snapshot as a clone
        // source, and replaced it with a read-only region

        let selected_clone_source = saga_outputs
            .lookup_node_output::<CloneSource>("clone_source")
            .unwrap();

        assert_eq!(
            selected_clone_source,
            CloneSource::RegionSnapshot {
                dataset_id: disk_allocated_regions[2].0.id(),
                region_id: disk_allocated_regions[2].1.id(),
            },
        );

        let snapshot_allocated_regions = datastore
            .get_allocated_regions(db_snapshot.volume_id())
            .await
            .unwrap();

        assert_eq!(snapshot_allocated_regions.len(), 1);
        assert!(snapshot_allocated_regions.iter().all(|(_, r)| r.read_only()));
    }
}
