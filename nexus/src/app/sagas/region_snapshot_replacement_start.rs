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
//!    being replaced. This is filled in the `volume_replace_snapshot`
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
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::db::datastore::ExistingTarget;
use crate::app::db::datastore::RegionAllocationFor;
use crate::app::db::datastore::RegionAllocationParameters;
use crate::app::db::datastore::ReplacementTarget;
use crate::app::db::datastore::VolumeToDelete;
use crate::app::db::datastore::VolumeWithTarget;
use crate::app::db::lookup::LookupPath;
use crate::app::sagas::common_storage::find_only_new_region;
use crate::app::sagas::declare_saga_actions;
use crate::app::RegionAllocationStrategy;
use crate::app::{authn, db};
use anyhow::bail;
use nexus_types::identity::Asset;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::CrucibleOpts;
use sled_agent_client::types::VolumeConstructionRequest;
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
    NEW_REGION_ENSURE -> "ensured_dataset_and_region" {
        + rsrss_new_region_ensure
        - rsrss_new_region_ensure_undo
    }
    GET_OLD_SNAPSHOT_VOLUME_ID -> "old_snapshot_volume_id" {
        + rsrss_get_old_snapshot_volume_id
    }
    CREATE_FAKE_VOLUME -> "unused_3" {
        + rsrss_create_fake_volume
        - rsrss_create_fake_volume_undo
    }
    REPLACE_SNAPSHOT_IN_VOLUME -> "unused_2" {
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

        builder.append(set_saga_id_action());
        builder.append(get_alloc_region_params_action());
        builder.append(alloc_new_region_action());
        builder.append(find_new_region_action());
        builder.append(new_region_ensure_action());
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

#[derive(Debug, Deserialize, Serialize)]
struct AllocRegionParams {
    block_size: u64,
    blocks_per_extent: u64,
    extent_count: u64,
    current_allocated_regions: Vec<(db::model::Dataset, db::model::Region)>,
    snapshot_id: Uuid,
    snapshot_volume_id: Uuid,
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
    let (.., db_snapshot) = LookupPath::new(&opctx, &osagactx.datastore())
        .snapshot_id(params.request.old_snapshot_id)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    // Find the region to replace
    let db_region = osagactx
        .datastore()
        .get_region(params.request.old_region_id)
        .await
        .map_err(ActionError::action_failed)?;

    let current_allocated_regions = osagactx
        .datastore()
        .get_allocated_regions(db_snapshot.volume_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(AllocRegionParams {
        block_size: db_region.block_size().to_bytes(),
        blocks_per_extent: db_region.blocks_per_extent(),
        extent_count: db_region.extent_count(),
        current_allocated_regions,
        snapshot_id: db_snapshot.id(),
        snapshot_volume_id: db_snapshot.volume_id,
    })
}

async fn rsrss_alloc_new_region(
    sagactx: NexusActionContext,
) -> Result<Vec<(db::model::Dataset, db::model::Region)>, ActionError> {
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
        sagactx.lookup::<Vec<(db::model::Dataset, db::model::Region)>>(
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
    }

    Ok(())
}

async fn rsrss_find_new_region(
    sagactx: NexusActionContext,
) -> Result<(db::model::Dataset, db::model::Region), ActionError> {
    let osagactx = sagactx.user_data();
    let log = osagactx.log();

    let alloc_region_params =
        sagactx.lookup::<AllocRegionParams>("alloc_region_params")?;

    let maybe_dataset_and_region = find_only_new_region(
        log,
        alloc_region_params.current_allocated_regions,
        sagactx.lookup::<Vec<(db::model::Dataset, db::model::Region)>>(
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

async fn rsrss_new_region_ensure(
    sagactx: NexusActionContext,
) -> Result<
    (nexus_db_model::Dataset, crucible_agent_client::types::Region),
    ActionError,
> {
    let params = sagactx.saga_params::<Params>()?;
    let osagactx = sagactx.user_data();
    let log = osagactx.log();

    // With a list of datasets and regions to ensure, other sagas need to have a
    // separate no-op forward step for the undo action to ensure that the undo
    // step occurs in the case that the ensure partially fails. Here this not
    // required, there's only one dataset and region.
    let new_dataset_and_region = sagactx
        .lookup::<(db::model::Dataset, db::model::Region)>(
            "new_dataset_and_region",
        )?;

    let region_snapshot = osagactx
        .datastore()
        .region_snapshot_get(
            params.request.old_dataset_id,
            params.request.old_region_id,
            params.request.old_snapshot_id,
        )
        .await
        .map_err(ActionError::action_failed)?;

    let Some(region_snapshot) = region_snapshot else {
        return Err(ActionError::action_failed(format!(
            "region snapshot {} {} {} deleted!",
            params.request.old_dataset_id,
            params.request.old_region_id,
            params.request.old_snapshot_id,
        )));
    };

    let (new_dataset, new_region) = new_dataset_and_region;

    // Currently, the repair port is set using a fixed offset above the
    // downstairs port. Once this goes away, Nexus will require a way to query
    // for the repair port!

    let mut repair_addr: SocketAddrV6 =
        match region_snapshot.snapshot_addr.parse() {
            Ok(addr) => addr,

            Err(e) => {
                return Err(ActionError::action_failed(format!(
                    "error parsing region_snapshot.snapshot_addr: {e}"
                )));
            }
        };

    repair_addr
        .set_port(repair_addr.port() + crucible_common::REPAIR_PORT_OFFSET);

    let ensured_region = osagactx
        .nexus()
        .ensure_region_in_dataset(
            log,
            &new_dataset,
            &new_region,
            Some(repair_addr.to_string()),
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
        .lookup::<(db::model::Dataset, db::model::Region)>(
            "new_dataset_and_region",
        )?;

    osagactx
        .nexus()
        .delete_crucible_regions(log, vec![new_dataset_and_region])
        .await?;

    Ok(())
}

async fn rsrss_get_old_snapshot_volume_id(
    sagactx: NexusActionContext,
) -> Result<Uuid, ActionError> {
    // Save the snapshot's original volume ID, because we'll be altering it and
    // need the original

    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., db_snapshot) = LookupPath::new(&opctx, &osagactx.datastore())
        .snapshot_id(params.request.old_snapshot_id)
        .fetch()
        .await
        .map_err(ActionError::action_failed)?;

    Ok(db_snapshot.volume_id)
}

async fn rsrss_create_fake_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();

    let new_volume_id = sagactx.lookup::<Uuid>("new_volume_id")?;

    // Create a fake volume record for the old snapshot target. This will be
    // deleted after snapshot replacement has finished. It can be completely
    // blank here, it will be replaced by `volume_replace_snapshot`.

    let volume_construction_request = VolumeConstructionRequest::Volume {
        id: new_volume_id,
        block_size: 0,
        sub_volumes: vec![VolumeConstructionRequest::Region {
            block_size: 0,
            blocks_per_extent: 0,
            extent_count: 0,
            gen: 0,
            opts: CrucibleOpts {
                id: new_volume_id,
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

    let volume_data = serde_json::to_string(&volume_construction_request)
        .map_err(|e| {
            ActionError::action_failed(Error::internal_error(&e.to_string()))
        })?;

    let volume = db::model::Volume::new(new_volume_id, volume_data);

    osagactx
        .datastore()
        .volume_create(volume)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn rsrss_create_fake_volume_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();

    // Delete the fake volume.

    let new_volume_id = sagactx.lookup::<Uuid>("new_volume_id")?;
    osagactx.datastore().volume_hard_delete(new_volume_id).await?;

    Ok(())
}

async fn rsrss_replace_snapshot_in_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let new_volume_id = sagactx.lookup::<Uuid>("new_volume_id")?;

    let region_snapshot = osagactx
        .datastore()
        .region_snapshot_get(
            params.request.old_dataset_id,
            params.request.old_region_id,
            params.request.old_snapshot_id,
        )
        .await
        .map_err(ActionError::action_failed)?;

    let Some(region_snapshot) = region_snapshot else {
        return Err(ActionError::action_failed(format!(
            "region snapshot {} {} {} deleted!",
            params.request.old_dataset_id,
            params.request.old_region_id,
            params.request.old_snapshot_id,
        )));
    };

    let old_snapshot_address: SocketAddrV6 =
        match region_snapshot.snapshot_addr.parse() {
            Ok(addr) => addr,

            Err(e) => {
                return Err(ActionError::action_failed(format!(
                    "parsing {} as SocketAddrV6 failed: {e}",
                    region_snapshot.snapshot_addr,
                )));
            }
        };

    let (new_dataset, ensured_region) = sagactx.lookup::<(
        db::model::Dataset,
        crucible_agent_client::types::Region,
    )>(
        "ensured_dataset_and_region",
    )?;

    let Some(new_dataset_address) = new_dataset.address() else {
        return Err(ActionError::action_failed(format!(
            "dataset {} does not have an address!",
            new_dataset.id(),
        )));
    };

    let new_region_address = SocketAddrV6::new(
        *new_dataset_address.ip(),
        ensured_region.port_number,
        0,
        0,
    );

    // If this node is rerun, the forward action will have overwritten
    // db_region's volume id, so get the cached copy.
    let old_volume_id = sagactx.lookup::<Uuid>("old_snapshot_volume_id")?;

    info!(
        log,
        "replacing {} with {} in volume {}",
        old_snapshot_address,
        new_region_address,
        old_volume_id,
    );

    // `volume_replace_snapshot` will swap the old snapshot for the new region.
    // No repair or reconcilation needs to occur after this.
    osagactx
        .datastore()
        .volume_replace_snapshot(
            VolumeWithTarget(old_volume_id),
            ExistingTarget(old_snapshot_address),
            ReplacementTarget(new_region_address),
            VolumeToDelete(new_volume_id),
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn rsrss_replace_snapshot_in_volume_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    // Undo the forward action's volume_replace_snapshot call by swapping the
    // target_addr fields in the parameters.

    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let new_volume_id = sagactx.lookup::<Uuid>("new_volume_id")?;

    let region_snapshot = osagactx
        .datastore()
        .region_snapshot_get(
            params.request.old_dataset_id,
            params.request.old_region_id,
            params.request.old_snapshot_id,
        )
        .await
        .map_err(ActionError::action_failed)?;

    let Some(region_snapshot) = region_snapshot else {
        bail!(
            "region snapshot {} {} {} deleted!",
            params.request.old_dataset_id,
            params.request.old_region_id,
            params.request.old_snapshot_id,
        );
    };

    let old_snapshot_address: SocketAddrV6 =
        match region_snapshot.snapshot_addr.parse() {
            Ok(addr) => addr,

            Err(e) => {
                bail!(
                    "parsing {} as SocketAddrV6 failed: {e}",
                    region_snapshot.snapshot_addr,
                );
            }
        };

    let (new_dataset, ensured_region) = sagactx.lookup::<(
        db::model::Dataset,
        crucible_agent_client::types::Region,
    )>(
        "ensured_dataset_and_region",
    )?;

    let Some(new_dataset_address) = new_dataset.address() else {
        bail!("dataset {} does not have an address!", new_dataset.id());
    };

    let new_region_address = SocketAddrV6::new(
        *new_dataset_address.ip(),
        ensured_region.port_number,
        0,
        0,
    );

    // The forward action will have overwritten db_region's volume id, so get
    // the cached copy.
    let old_volume_id = sagactx.lookup::<Uuid>("old_snapshot_volume_id")?;

    info!(
        log,
        "undo: replacing {} with {} in volume {}",
        old_snapshot_address,
        new_region_address,
        old_volume_id,
    );

    osagactx
        .datastore()
        .volume_replace_snapshot(
            VolumeWithTarget(old_volume_id),
            ExistingTarget(new_region_address),
            ReplacementTarget(old_snapshot_address),
            VolumeToDelete(new_volume_id),
        )
        .await?;

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
        .lookup::<(db::model::Dataset, db::model::Region)>(
            "new_dataset_and_region",
        )?;

    let new_region_id = new_dataset_and_region.1.id();

    let old_region_volume_id = sagactx.lookup::<Uuid>("new_volume_id")?;

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
            old_region_volume_id,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

#[cfg(test)]
pub(crate) mod test {
    use crate::{
        app::db::lookup::LookupPath, app::db::DataStore,
        app::saga::create_saga_dag,
        app::sagas::region_snapshot_replacement_start::*,
        app::sagas::test_helpers::test_opctx, app::RegionAllocationStrategy,
    };
    use nexus_db_model::RegionSnapshotReplacement;
    use nexus_db_model::RegionSnapshotReplacementState;
    use nexus_db_model::Volume;
    use nexus_db_queries::authn::saga::Serialized;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils::resource_helpers::create_disk;
    use nexus_test_utils::resource_helpers::create_project;
    use nexus_test_utils::resource_helpers::create_snapshot;
    use nexus_test_utils::resource_helpers::DiskTest;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::external_api::views;
    use nexus_types::identity::Asset;
    use sled_agent_client::types::VolumeConstructionRequest;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    const DISK_NAME: &str = "my-disk";
    const SNAPSHOT_NAME: &str = "my-snap";
    const PROJECT_NAME: &str = "springfield-squidport";

    async fn prepare_for_test(
        cptestctx: &ControlPlaneTestContext,
    ) -> PrepareResult {
        let client = &cptestctx.external_client;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = test_opctx(cptestctx);

        assert_eq!(region_allocations(&datastore).await, 0);

        let mut disk_test = DiskTest::new(cptestctx).await;
        disk_test.add_zpool_with_dataset(cptestctx.first_sled()).await;

        assert_eq!(region_allocations(&datastore).await, 0);

        let _project_id =
            create_project(&client, PROJECT_NAME).await.identity.id;

        assert_eq!(region_allocations(&datastore).await, 0);

        // Create a disk
        let disk = create_disk(&client, PROJECT_NAME, DISK_NAME).await;

        assert_eq!(region_allocations(&datastore).await, 3);

        let disk_id = disk.identity.id;
        let (.., db_disk) = LookupPath::new(&opctx, &datastore)
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
        let (.., db_snapshot) = LookupPath::new(&opctx, &datastore)
            .snapshot_id(snapshot_id)
            .fetch()
            .await
            .unwrap_or_else(|_| {
                panic!("test snapshot {:?} should exist", snapshot_id)
            });

        PrepareResult { db_disk, snapshot, db_snapshot }
    }

    struct PrepareResult {
        db_disk: nexus_db_model::Disk,
        snapshot: views::Snapshot,
        db_snapshot: nexus_db_model::Snapshot,
    }

    #[nexus_test(server = crate::Server)]
    async fn test_region_snapshot_replacement_start_saga(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let PrepareResult { db_disk, snapshot, db_snapshot } =
            prepare_for_test(cptestctx).await;

        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = test_opctx(cptestctx);

        // Assert disk has three allocated regions
        let disk_allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id).await.unwrap();
        assert_eq!(disk_allocated_regions.len(), 3);

        // Assert the snapshot has zero allocated regions
        let snapshot_id = snapshot.identity.id;

        let snapshot_allocated_regions = datastore
            .get_allocated_regions(db_snapshot.volume_id)
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
            datastore.get_allocated_regions(db_disk.volume_id).await.unwrap();
        assert_eq!(disk_allocated_regions.len(), 3);

        // Validate that the snapshot now has one allocated region
        let snapshot_allocated_datasets_and_regions = datastore
            .get_allocated_regions(db_snapshot.volume_id)
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

        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].id(), db_snapshot.volume_id);
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
        request: &RegionSnapshotReplacement,
        affected_volume_original: &Volume,
    ) {
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
        assert_region_snapshot_replacement_request_untouched(
            cptestctx, &datastore, &request,
        )
        .await;
        assert_volume_untouched(&datastore, &affected_volume_original).await;
    }

    async fn region_allocations(datastore: &DataStore) -> usize {
        use async_bb8_diesel::AsyncConnection;
        use async_bb8_diesel::AsyncRunQueryDsl;
        use async_bb8_diesel::AsyncSimpleConnection;
        use diesel::QueryDsl;
        use nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
        use nexus_db_queries::db::schema::region::dsl;

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        conn.transaction_async(|conn| async move {
            // Selecting all regions requires a full table scan
            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await.unwrap();

            dsl::region
                .count()
                .get_result_async(&conn)
                .await
                .map(|x: i64| x as usize)
        })
        .await
        .unwrap()
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
    async fn test_action_failure_can_unwind_idempotently(
        cptestctx: &ControlPlaneTestContext,
    ) {
        let PrepareResult { db_disk, snapshot, db_snapshot } =
            prepare_for_test(cptestctx).await;

        let log = &cptestctx.logctx.log;
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = test_opctx(cptestctx);

        let disk_allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id).await.unwrap();
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

        let affected_volume_original =
            datastore.volume_get(db_snapshot.volume_id).await.unwrap().unwrap();

        verify_clean_slate(&cptestctx, &request, &affected_volume_original)
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
        let PrepareResult { db_disk, snapshot, db_snapshot: _ } =
            prepare_for_test(cptestctx).await;

        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = test_opctx(cptestctx);

        let disk_allocated_regions =
            datastore.get_allocated_regions(db_disk.volume_id).await.unwrap();
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
}
