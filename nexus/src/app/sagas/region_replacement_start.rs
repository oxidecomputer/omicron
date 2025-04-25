// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Region replacements are required when a physical disk is expunged: Volumes
//! are now pointing to Regions whose Downstairs have gone away, and are in a
//! degraded state. The Upstairs can handle a single Downstairs going away at
//! any time, but the Volume remains degraded until a new Region replaces the
//! one that is gone.
//!
//! It's this saga's responsibility to start that replacement process. This saga
//! handles the following region replacement request state transitions:
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
//!          Running
//! ```
//!
//! The first thing this saga does is set itself as the "operating saga" for the
//! request, and change the state to "Allocating". Then, it performs the
//! following steps:
//!
//! 1. Allocate a new region
//!
//! 2. For the affected Volume, swap the region being replaced with the new
//!    region.
//!
//! 3. Create a fake volume that can be later deleted with the region being
//!    replaced.
//!
//! 4. Update the region replacement request by clearing the operating saga id
//!    and changing the state to "Running".
//!
//! Any unwind will place the state back into Requested.
//!
//! See the documentation for the "region replacement drive" saga for the next
//! steps in the process.

use super::{
    ACTION_GENERATE_ID, ActionRegistry, NexusActionContext, NexusSaga,
    SagaInitError,
};
use crate::app::RegionAllocationStrategy;
use crate::app::db::datastore::VolumeReplaceResult;
use crate::app::sagas::common_storage::find_only_new_region;
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, db};
use nexus_db_queries::db::datastore::REGION_REDUNDANCY_THRESHOLD;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::VolumeUuid;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::CrucibleOpts;
use sled_agent_client::VolumeConstructionRequest;
use std::net::SocketAddrV6;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// region replacement start saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub request: db::model::RegionReplacement,
    pub allocation_strategy: RegionAllocationStrategy,
}

// region replacement start saga: actions

declare_saga_actions! {
    region_replacement_start;
    SET_SAGA_ID -> "unused_1" {
        + srrs_set_saga_id
        - srrs_set_saga_id_undo
    }
    GET_EXISTING_DATASETS_AND_REGIONS -> "existing_datasets_and_regions" {
        + srrs_get_existing_datasets_and_regions
    }
    ALLOC_NEW_REGION -> "new_datasets_and_regions" {
        + srrs_alloc_new_region
        - srrs_alloc_new_region_undo
    }
    FIND_NEW_REGION -> "new_dataset_and_region" {
        + srrs_find_new_region
    }
    NEW_REGION_ENSURE -> "ensured_dataset_and_region" {
        + srrs_new_region_ensure
        - srrs_new_region_ensure_undo
    }
    GET_OLD_REGION_ADDRESS -> "old_region_address" {
        + srrs_get_old_region_address
    }
    GET_OLD_REGION_VOLUME_ID -> "old_region_volume_id" {
        + srrs_get_old_region_volume_id
    }
    REPLACE_REGION_IN_VOLUME -> "unused_2" {
        + srrs_replace_region_in_volume
        - srrs_replace_region_in_volume_undo
    }
    CREATE_FAKE_VOLUME -> "unused_3" {
        + srrs_create_fake_volume
        - srrs_create_fake_volume_undo
    }
    UPDATE_REQUEST_RECORD -> "unused_4" {
        + srrs_update_request_record
    }
}

// region replacement start saga: definition

#[derive(Debug)]
pub(crate) struct SagaRegionReplacementStart;
impl NexusSaga for SagaRegionReplacementStart {
    const NAME: &'static str = "region-replacement-start";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        region_replacement_start_register_actions(registry);
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

        builder.append(set_saga_id_action());
        builder.append(get_existing_datasets_and_regions_action());
        builder.append(alloc_new_region_action());
        builder.append(find_new_region_action());
        builder.append(new_region_ensure_action());
        builder.append(get_old_region_address_action());
        builder.append(get_old_region_volume_id_action());
        builder.append(replace_region_in_volume_action());
        builder.append(create_fake_volume_action());
        builder.append(update_request_record_action());

        Ok(builder.build()?)
    }
}

// region replacement start saga: action implementations

async fn srrs_set_saga_id(
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
    // avoids Nexus allocating a bunch of replacement regions only to unwind all
    // but one.
    osagactx
        .datastore()
        .set_region_replacement_allocating(&opctx, params.request.id, saga_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn srrs_set_saga_id_undo(
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
        .undo_set_region_replacement_allocating(
            &opctx,
            params.request.id,
            saga_id,
        )
        .await?;

    Ok(())
}

async fn srrs_get_existing_datasets_and_regions(
    sagactx: NexusActionContext,
) -> Result<Vec<(db::model::CrucibleDataset, db::model::Region)>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    // Look up the existing region
    let db_region = osagactx
        .datastore()
        .get_region(params.request.old_region_id)
        .await
        .map_err(ActionError::action_failed)?;

    // XXX for now, bail out if requesting the replacement of a read-only region
    if db_region.read_only() {
        return Err(ActionError::action_failed(String::from(
            "replacing read-only region currently unsupported",
        )));
    }

    // Find out the existing datasets and regions that back the volume
    let datasets_and_regions = osagactx
        .datastore()
        .get_allocated_regions(db_region.volume_id())
        .await
        .map_err(ActionError::action_failed)?;

    Ok(datasets_and_regions)
}

async fn srrs_alloc_new_region(
    sagactx: NexusActionContext,
) -> Result<Vec<(db::model::CrucibleDataset, db::model::Region)>, ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // Look up the existing region: we want to duplicate it but at another
    // physical disk
    let db_region = osagactx
        .datastore()
        .get_region(params.request.old_region_id)
        .await
        .map_err(ActionError::action_failed)?;

    // Request an additional region for this volume: THRESHOLD + 1 is required
    // in order to have the proper redundancy. It's important _not_ to delete
    // the existing region first, as (if it's still there) then the Crucible
    // agent could reuse the allocated port and cause trouble.
    let datasets_and_regions = osagactx
        .datastore()
        .arbitrary_region_allocate(
            &opctx,
            db::datastore::RegionAllocationFor::DiskVolume {
                volume_id: db_region.volume_id(),
            },
            db::datastore::RegionAllocationParameters::FromRaw {
                block_size: db_region.block_size().to_bytes(),
                blocks_per_extent: db_region.blocks_per_extent(),
                extent_count: db_region.extent_count(),
            },
            &params.allocation_strategy,
            // Note: this assumes that previous redundancy is
            // REGION_REDUNDANCY_THRESHOLD, and that region replacement will
            // only be run for volumes that start at this redundancy level.
            REGION_REDUNDANCY_THRESHOLD + 1,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(datasets_and_regions)
}

async fn srrs_alloc_new_region_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();

    let maybe_dataset_and_region = find_only_new_region(
        log,
        sagactx
            .lookup::<Vec<(db::model::CrucibleDataset, db::model::Region)>>(
                "existing_datasets_and_regions",
            )?,
        sagactx
            .lookup::<Vec<(db::model::CrucibleDataset, db::model::Region)>>(
                "new_datasets_and_regions",
            )?,
    );

    // It should be guaranteed that if srrs_alloc_new_region succeeded then it
    // would have bumped the region redundancy to 4, and the existing region
    // redundancy should be 3, so we should see something here. Guard against
    // the case anyway.
    if let Some(dataset_and_region) = maybe_dataset_and_region {
        let (_, region) = dataset_and_region;
        osagactx
            .datastore()
            .regions_hard_delete(log, vec![region.id()])
            .await?;
    }

    Ok(())
}

async fn srrs_find_new_region(
    sagactx: NexusActionContext,
) -> Result<(db::model::CrucibleDataset, db::model::Region), ActionError> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();

    let maybe_dataset_and_region = find_only_new_region(
        log,
        sagactx
            .lookup::<Vec<(db::model::CrucibleDataset, db::model::Region)>>(
                "existing_datasets_and_regions",
            )?,
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

async fn srrs_new_region_ensure(
    sagactx: NexusActionContext,
) -> Result<
    (nexus_db_model::CrucibleDataset, crucible_agent_client::types::Region),
    ActionError,
> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();

    // With a list of datasets and regions to ensure, other sagas need to have a
    // separate no-op forward step for the undo action to ensure that the undo
    // step occurs in the case that the ensure partially fails. Here this not
    // required, there's only one dataset and region.
    let new_dataset_and_region = sagactx
        .lookup::<(db::model::CrucibleDataset, db::model::Region)>(
            "new_dataset_and_region",
        )?;

    let mut ensured_dataset_and_region = osagactx
        .nexus()
        .ensure_all_datasets_and_regions(&log, vec![new_dataset_and_region])
        .await
        .map_err(ActionError::action_failed)?;

    if ensured_dataset_and_region.len() != 1 {
        return Err(ActionError::action_failed(Error::internal_error(
            &format!(
                "expected 1 dataset and region, saw {}!",
                ensured_dataset_and_region.len()
            ),
        )));
    }

    Ok(ensured_dataset_and_region.pop().unwrap())
}

async fn srrs_new_region_ensure_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();

    warn!(log, "srrs_new_region_ensure_undo: Deleting crucible regions");

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

async fn srrs_get_old_region_volume_id(
    sagactx: NexusActionContext,
) -> Result<VolumeUuid, ActionError> {
    // Save the region's original volume ID, because we'll be altering it and
    // need the original

    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let db_region = osagactx
        .datastore()
        .get_region(params.request.old_region_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(db_region.volume_id())
}

async fn srrs_get_old_region_address(
    sagactx: NexusActionContext,
) -> Result<SocketAddrV6, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    // Either retrieve the address from the database (because the port was
    // previously recorded), or attempt grabbing the port from the corresponding
    // Crucible agent: the sled or disk may not be physically gone, or we may be
    // running in a test where the allocation strategy does not mandate distinct
    // sleds.

    let maybe_addr =
        osagactx.nexus().region_addr(log, params.request.old_region_id).await;

    match maybe_addr {
        Ok(addr) => Ok(addr),

        Err(Error::Gone) => {
            // It was a mistake not to record the port of a region in the Region
            // record.  However, we haven't needed it until now! If the Crucible
            // agent is gone (which it will be if the disk is expunged), assume
            // that the region in the read/write portion of the volume with the
            // same dataset address (there should only be one due to the
            // allocation strategy!) is the old region.

            let opctx = crate::context::op_context_for_saga_action(
                &sagactx,
                &params.serialized_authn,
            );

            let db_region = osagactx
                .datastore()
                .get_region(params.request.old_region_id)
                .await
                .map_err(ActionError::action_failed)?;

            let targets = osagactx
                .datastore()
                .get_dataset_rw_regions_in_volume(
                    &opctx,
                    db_region.dataset_id(),
                    db_region.volume_id(),
                )
                .await
                .map_err(ActionError::action_failed)?;

            if targets.len() == 1 {
                // If there's a single RW region in the volume that matches this
                // region's dataset, then it must match. Return the target.
                Ok(targets[0])
            } else {
                // Otherwise, Nexus cannot know the region's port. Return an
                // error.
                Err(ActionError::action_failed(format!(
                    "{} regions match dataset {} in volume {}",
                    targets.len(),
                    db_region.dataset_id(),
                    db_region.volume_id(),
                )))
            }
        }

        Err(e) => Err(ActionError::action_failed(e)),
    }
}

async fn srrs_replace_region_in_volume(
    sagactx: NexusActionContext,
) -> Result<VolumeReplaceResult, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let db_region = osagactx
        .datastore()
        .get_region(params.request.old_region_id)
        .await
        .map_err(ActionError::action_failed)?;

    let new_volume_id = sagactx.lookup::<VolumeUuid>("new_volume_id")?;
    let old_region_address =
        sagactx.lookup::<SocketAddrV6>("old_region_address")?;

    let (new_dataset, ensured_region) =
        sagactx.lookup::<(
            db::model::CrucibleDataset,
            crucible_agent_client::types::Region,
        )>("ensured_dataset_and_region")?;

    let new_address = new_dataset.address();
    let new_region_address =
        SocketAddrV6::new(*new_address.ip(), ensured_region.port_number, 0, 0);

    // If this node is rerun, the forward action will have overwritten
    // db_region's volume id, so get the cached copy.
    let old_volume_id = sagactx.lookup::<VolumeUuid>("old_region_volume_id")?;

    info!(
        log,
        "replacing {} with {} in volume {}",
        old_region_address,
        new_region_address,
        old_volume_id,
    );

    // `volume_replace_region` will swap the old region for the new region,
    // assigning the old region to the new volume id for later (attempted)
    // deletion. After this is done, repair or reconciliation needs to occur.
    let volume_replace_region_result = osagactx
        .datastore()
        .volume_replace_region(
            /* target */
            db::datastore::VolumeReplacementParams {
                volume_id: old_volume_id,
                region_id: db_region.id(),
                region_addr: old_region_address,
            },
            /* replacement */
            db::datastore::VolumeReplacementParams {
                volume_id: new_volume_id,
                region_id: ensured_region.id.0.parse().unwrap(),
                region_addr: new_region_address,
            },
        )
        .await
        .map_err(ActionError::action_failed)?;

    match volume_replace_region_result {
        VolumeReplaceResult::AlreadyHappened | VolumeReplaceResult::Done => {
            // The replacement was done either by this run of this saga node, or
            // a previous one (and this is a rerun). This can only be returned
            // if the transaction occurred on the non-deleted volume so proceed
            // with the rest of the saga (to properly clean up allocated
            // resources).

            Ok(volume_replace_region_result)
        }

        VolumeReplaceResult::ExistingVolumeSoftDeleted
        | VolumeReplaceResult::ExistingVolumeHardDeleted => {
            // Unwind the saga here to clean up the resources allocated during
            // this saga. The associated background task will transition this
            // request's state to Completed.

            Err(ActionError::action_failed(Error::conflict(format!(
                "existing volume {} deleted",
                old_volume_id
            ))))
        }
    }
}

async fn srrs_replace_region_in_volume_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    // Undo the forward action's volume_replace_region call by swapping the
    // region id and address fields in the parameters. Note: ROP removal may
    // have occurred but this does not affect what volume_replace_region does.
    //
    // IMPORTANT: it is _not_ valid to undo this step if a repair or
    // reconciliation has started! However that _should_ only start if this saga
    // is successful due to the last step of this saga changing the request's
    // state to "Running".

    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let db_region = osagactx
        .datastore()
        .get_region(params.request.old_region_id)
        .await
        .map_err(ActionError::action_failed)?;

    let new_volume_id = sagactx.lookup::<VolumeUuid>("new_volume_id")?;
    let old_region_address =
        sagactx.lookup::<SocketAddrV6>("old_region_address")?;

    let (new_dataset, ensured_region) =
        sagactx.lookup::<(
            db::model::CrucibleDataset,
            crucible_agent_client::types::Region,
        )>("ensured_dataset_and_region")?;

    let new_address = new_dataset.address();
    let new_region_address =
        SocketAddrV6::new(*new_address.ip(), ensured_region.port_number, 0, 0);

    // The forward action will have overwritten db_region's volume id, so get
    // the cached copy.
    let old_volume_id = sagactx.lookup::<VolumeUuid>("old_region_volume_id")?;

    info!(
        log,
        "undo: replacing {} with {} in volume {}",
        old_region_address,
        new_region_address,
        old_volume_id,
    );

    // Note: volume ID is not swapped! The fake volume hasn't been created yet,
    // and we have to target the original volume id.
    //
    // It's ok if this function returns ExistingVolumeDeleted here: we don't
    // want to throw an error and cause the saga to be stuck unwinding, as this
    // would hold the lock on the replacement request.

    let volume_replace_region_result = osagactx
        .datastore()
        .volume_replace_region(
            /* target */
            db::datastore::VolumeReplacementParams {
                volume_id: old_volume_id,
                region_id: ensured_region.id.0.parse().unwrap(),
                region_addr: new_region_address,
            },
            /* replacement */
            db::datastore::VolumeReplacementParams {
                volume_id: new_volume_id,
                region_id: db_region.id(),
                region_addr: old_region_address,
            },
        )
        .await?;

    info!(
        log,
        "undo: volume_replace_region returned {:?}",
        volume_replace_region_result,
    );

    Ok(())
}

async fn srrs_create_fake_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();

    let new_volume_id = sagactx.lookup::<VolumeUuid>("new_volume_id")?;
    let old_region_address =
        sagactx.lookup::<SocketAddrV6>("old_region_address")?;

    // One the new region was swapped in, create a fake volume record for the
    // old region record. This will be deleted after region replacement has
    // finished.

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
                target: vec![old_region_address.into()],
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

    osagactx
        .datastore()
        .volume_create(new_volume_id, volume_construction_request)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn srrs_create_fake_volume_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();

    // Delete the fake volume.

    let new_volume_id = sagactx.lookup::<VolumeUuid>("new_volume_id")?;
    osagactx.datastore().volume_hard_delete(new_volume_id).await?;

    Ok(())
}

async fn srrs_update_request_record(
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

    // Now that the region has been ensured and the construction request has
    // been updated, update the replacement request record to 'Running' and
    // clear the operating saga id. There is no undo step for this, it should
    // succeed idempotently.
    datastore
        .set_region_replacement_running(
            &opctx,
            params.request.id,
            saga_id,
            new_region_id,
            old_region_volume_id,
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
        app::sagas::region_replacement_start::Params,
        app::sagas::region_replacement_start::SagaRegionReplacementStart,
        app::sagas::region_replacement_start::find_only_new_region,
        app::sagas::test_helpers::test_opctx,
    };
    use chrono::Utc;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::CrucibleDataset;
    use nexus_db_model::Region;
    use nexus_db_model::RegionReplacement;
    use nexus_db_model::RegionReplacementState;
    use nexus_db_model::Volume;
    use nexus_db_queries::authn::saga::Serialized;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::resource_helpers::create_disk;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::identity::Asset;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::VolumeUuid;
    use sled_agent_client::VolumeConstructionRequest;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;
    type DiskTest<'a> =
        nexus_test_utils::resource_helpers::DiskTest<'a, crate::Server>;
    type DiskTestBuilder<'a> =
        nexus_test_utils::resource_helpers::DiskTestBuilder<'a, crate::Server>;

    const DISK_NAME: &str = "my-disk";
    const PROJECT_NAME: &str = "springfield-squidport";

    #[nexus_test(server = crate::Server)]
    async fn test_region_replacement_start_saga(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _disk_test =
            DiskTestBuilder::new(cptestctx).with_zpool_count(4).build().await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = test_opctx(cptestctx);

        let _project_id =
            create_project(&client, PROJECT_NAME).await.identity.id;

        // Create a disk
        let client = &cptestctx.external_client;
        let disk = create_disk(&client, PROJECT_NAME, DISK_NAME).await;

        // Assert disk has three allocated regions
        let disk_id = disk.identity.id;
        let (.., db_disk) = LookupPath::new(&opctx, datastore)
            .disk_id(disk_id)
            .fetch()
            .await
            .unwrap_or_else(|_| panic!("test disk {:?} should exist", disk_id));

        let allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
        assert_eq!(allocated_regions.len(), 3);

        // Replace one of the disk's regions
        let region_to_replace: &nexus_db_model::Region =
            &allocated_regions[0].1;

        // Manually insert the replacement request
        let request = RegionReplacement {
            id: Uuid::new_v4(),
            request_time: Utc::now(),
            old_region_id: region_to_replace.id(),
            volume_id: region_to_replace.volume_id().into(),
            old_region_volume_id: None,
            new_region_id: None,
            replacement_state: RegionReplacementState::Requested,
            operating_saga_id: None,
        };

        datastore
            .insert_region_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        // Run the region replacement start saga
        let params = Params {
            serialized_authn: Serialized::for_opctx(&opctx),
            request: request.clone(),
            allocation_strategy: RegionAllocationStrategy::Random {
                seed: None,
            },
        };
        let output = nexus
            .sagas
            .saga_execute::<SagaRegionReplacementStart>(params)
            .await
            .unwrap();

        // Validate the state transition
        let result = datastore
            .get_region_replacement_request_by_id(&opctx, request.id)
            .await
            .unwrap();
        assert_eq!(result.replacement_state, RegionReplacementState::Running);
        assert!(result.new_region_id.is_some());
        assert!(result.operating_saga_id.is_none());

        // Validate number of regions for disk didn't change
        let allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
        assert_eq!(allocated_regions.len(), 3);

        // Validate that one of the regions for the disk is the new one
        let new_region =
            datastore.get_region(result.new_region_id.unwrap()).await.unwrap();
        assert!(
            allocated_regions.iter().any(|(_, region)| *region == new_region)
        );

        // Validate the old region has the new volume id
        let old_region =
            datastore.get_region(region_to_replace.id()).await.unwrap();
        let new_volume_id =
            output.lookup_node_output::<VolumeUuid>("new_volume_id").unwrap();
        assert_eq!(old_region.volume_id(), new_volume_id);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_find_only_new_region(cptestctx: &ControlPlaneTestContext) {
        let log = &cptestctx.logctx.log;

        let datasets = vec![
            CrucibleDataset::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                "[fd00:1122:3344:101::1]:12345".parse().unwrap(),
            ),
            CrucibleDataset::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                "[fd00:1122:3344:102::1]:12345".parse().unwrap(),
            ),
            CrucibleDataset::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                "[fd00:1122:3344:103::1]:12345".parse().unwrap(),
            ),
            CrucibleDataset::new(
                DatasetUuid::new_v4(),
                Uuid::new_v4(),
                "[fd00:1122:3344:104::1]:12345".parse().unwrap(),
            ),
        ];

        let regions = vec![
            Region::new(
                datasets[0].id(),
                VolumeUuid::new_v4(),
                512_i64.try_into().unwrap(),
                10,
                10,
                1001,
                false,
            ),
            Region::new(
                datasets[1].id(),
                VolumeUuid::new_v4(),
                512_i64.try_into().unwrap(),
                10,
                10,
                1002,
                false,
            ),
            Region::new(
                datasets[2].id(),
                VolumeUuid::new_v4(),
                512_i64.try_into().unwrap(),
                10,
                10,
                1003,
                false,
            ),
            Region::new(
                datasets[3].id(),
                VolumeUuid::new_v4(),
                512_i64.try_into().unwrap(),
                10,
                10,
                1004,
                false,
            ),
        ];

        let existing_datasets_and_regions = vec![
            (datasets[0].clone(), regions[0].clone()),
            (datasets[1].clone(), regions[1].clone()),
            (datasets[2].clone(), regions[2].clone()),
        ];

        let new_datasets_and_regions = vec![
            (datasets[0].clone(), regions[0].clone()),
            (datasets[1].clone(), regions[1].clone()),
            (datasets[2].clone(), regions[2].clone()),
            (datasets[3].clone(), regions[3].clone()),
        ];

        let only_new_region = find_only_new_region(
            &log,
            existing_datasets_and_regions,
            new_datasets_and_regions,
        );

        assert_eq!(
            only_new_region,
            Some((datasets[3].clone(), regions[3].clone()))
        );
    }

    fn new_test_params(
        opctx: &OpContext,
        request: &RegionReplacement,
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
        test: &DiskTest<'_>,
        request: &RegionReplacement,
        affected_volume_original: &Volume,
        affected_region_original: &Region,
    ) {
        let datastore = cptestctx.server.server_context().nexus.datastore();

        crate::app::sagas::test_helpers::assert_no_failed_undo_steps(
            &cptestctx.logctx.log,
            datastore,
        )
        .await;

        assert!(three_region_allocations_exist(&datastore, &test).await);
        assert_region_replacement_request_untouched(
            cptestctx, &datastore, &request,
        )
        .await;
        assert_volume_untouched(&datastore, &affected_volume_original).await;
        assert_region_untouched(&datastore, &affected_region_original).await;
    }

    async fn three_region_allocations_exist(
        datastore: &DataStore,
        test: &DiskTest<'_>,
    ) -> bool {
        let mut count = 0;

        for zpool in test.zpools() {
            let dataset = zpool.crucible_dataset();
            if datastore.regions_total_reserved_size(dataset.id).await.unwrap()
                != 0
            {
                count += 1;
            }
        }

        count == 3
    }

    async fn assert_region_replacement_request_untouched(
        cptestctx: &ControlPlaneTestContext,
        datastore: &DataStore,
        request: &RegionReplacement,
    ) {
        let opctx = test_opctx(cptestctx);
        let db_request = datastore
            .get_region_replacement_request_by_id(&opctx, request.id)
            .await
            .unwrap();

        assert_eq!(db_request.new_region_id, None);
        assert_eq!(
            db_request.replacement_state,
            RegionReplacementState::Requested
        );
        assert_eq!(db_request.operating_saga_id, None);
    }

    fn zero_out_gen_number(vcr: &mut VolumeConstructionRequest) {
        match vcr {
            VolumeConstructionRequest::Volume {
                sub_volumes,
                read_only_parent,
                ..
            } => {
                for sv in sub_volumes {
                    zero_out_gen_number(sv);
                }

                if let Some(rop) = read_only_parent {
                    zero_out_gen_number(rop);
                }
            }

            VolumeConstructionRequest::Region { gen, .. } => {
                *gen = 0;
            }

            _ => {}
        }
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

        let mut actual: VolumeConstructionRequest =
            serde_json::from_str(&affected_volume.data()).unwrap();

        let mut expected: VolumeConstructionRequest =
            serde_json::from_str(&affected_volume_original.data()).unwrap();

        zero_out_gen_number(&mut actual);
        zero_out_gen_number(&mut expected);

        assert_eq!(actual, expected);
    }

    async fn assert_region_untouched(
        datastore: &DataStore,
        affected_region_original: &Region,
    ) {
        let affected_region =
            datastore.get_region(affected_region_original.id()).await.unwrap();
        assert_eq!(&affected_region, affected_region_original);
    }

    #[nexus_test(server = crate::Server)]
    async fn test_action_failure_can_unwind_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let disk_test =
            DiskTestBuilder::new(cptestctx).with_zpool_count(4).build().await;

        let log = &cptestctx.logctx.log;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();

        let _ = create_project(&client, PROJECT_NAME).await.identity.id;
        let opctx = test_opctx(&cptestctx);

        // Create a disk, and use the first region as input to the replacement
        // start saga

        let client = &cptestctx.external_client;
        let disk = create_disk(&client, PROJECT_NAME, DISK_NAME).await;

        let disk_id = disk.identity.id;
        let (.., db_disk) = LookupPath::new(&opctx, datastore)
            .disk_id(disk_id)
            .fetch()
            .await
            .unwrap_or_else(|_| panic!("test disk {:?} should exist", disk_id));

        let allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
        assert_eq!(allocated_regions.len(), 3);

        let region_to_replace: &Region = &allocated_regions[0].1;

        let request = RegionReplacement {
            id: Uuid::new_v4(),
            request_time: Utc::now(),
            old_region_id: region_to_replace.id(),
            volume_id: region_to_replace.volume_id().into(),
            old_region_volume_id: None,
            new_region_id: None,
            replacement_state: RegionReplacementState::Requested,
            operating_saga_id: None,
        };

        datastore
            .insert_region_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        let affected_volume_original = datastore
            .volume_get(region_to_replace.volume_id())
            .await
            .unwrap()
            .unwrap();

        crate::app::sagas::test_helpers::action_failure_can_unwind_idempotently::<
            SagaRegionReplacementStart,
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
                    &region_to_replace,
                ).await;
            }),
            log
        ).await;
    }

    #[nexus_test(server = crate::Server)]
    async fn test_actions_succeed_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let _disk_test =
            DiskTestBuilder::new(cptestctx).with_zpool_count(4).build().await;

        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();

        let _ = create_project(&client, PROJECT_NAME).await.identity.id;
        let opctx = test_opctx(&cptestctx);

        // Create a disk, and use the first region as input to the replacement
        // start saga

        let client = &cptestctx.external_client;
        let disk = create_disk(&client, PROJECT_NAME, DISK_NAME).await;

        let disk_id = disk.identity.id;
        let (.., db_disk) = LookupPath::new(&opctx, datastore)
            .disk_id(disk_id)
            .fetch()
            .await
            .unwrap_or_else(|_| panic!("test disk {:?} should exist", disk_id));

        let allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id()).await.unwrap();
        assert_eq!(allocated_regions.len(), 3);

        let region_to_replace: &Region = &allocated_regions[0].1;

        let request = RegionReplacement {
            id: Uuid::new_v4(),
            request_time: Utc::now(),
            old_region_id: region_to_replace.id(),
            volume_id: region_to_replace.volume_id().into(),
            old_region_volume_id: None,
            new_region_id: None,
            replacement_state: RegionReplacementState::Requested,
            operating_saga_id: None,
        };

        datastore
            .insert_region_replacement_request(&opctx, request.clone())
            .await
            .unwrap();

        // Build the saga DAG with the provided test parameters
        let params = new_test_params(&opctx, &request);
        let dag =
            create_saga_dag::<SagaRegionReplacementStart>(params).unwrap();
        crate::app::sagas::test_helpers::actions_succeed_idempotently(
            nexus, dag,
        )
        .await;
    }
}
