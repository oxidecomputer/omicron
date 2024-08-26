// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Region snapshot replacement is distinct from region replacement: replacing
//! parts of a volume's read-only parent (and all the layers under it) is easier
//! because this does _not_ incur a live repair or reconciliation. Each part of
//! a read-only region set contains the same data that will never be modified.
//!
//! A region snapshot replacement request starts off in the "Requested" state,
//! just like a region replacement request. A background task will search for
//! region snapshot replacement requests in this state and trigger the "region
//! snapshot replacement start" saga. This will allocate a new region to replace
//! the requested one, and modify the snapshot VCR accordingly. If any disks are
//! then created using that snapshot as a source, they will have the replacement
//! and will not need a replace request.
//!
//! However, any past use of that snapshot as a source means that the Volume
//! created from that will have a copy of the unmodified snapshot Volume as a
//! read-only parent. Any construction of the Volume will be referencing the
//! replaced region snapshot (which could be gone if it is expunged). It is this
//! saga's responsibility to update all Volumes that reference the region
//! snapshot being replaced, and send a replacement request to any Upstairs that
//! were constructed.
//!
//! Some difficulty comes from the requirement to notify existing Upstairs that
//! reference the replaced read-only part, but even this is not as difficult as
//! region replacement: Nexus does not have to continually monitor and drive
//! either live repair or reconciliation, just ensure that the read-only
//! replacement occurs. Read-only replacements should be basically
//! instantaneous.
//!
//! A replace request only needs to be done once per Upstairs that has the old
//! reference. This is done as a "region snapshot replacement step", and once
//! all those are done, the region snapshot replacement request can be
//! "completed".
//!
//! Region snapshot replacement steps need to be written into the database and
//! have an associated state and operating saga id for the same reason that
//! region snapshot replacement requests do: multiple background tasks will
//! invoke multiple sagas, and there needs to be some exclusive access.
//!
//! See the documentation for the "region snapshot replacement step garbage
//! collect" saga for the next step in the process.

use super::{
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::db::datastore::ExistingTarget;
use crate::app::db::datastore::ReplacementTarget;
use crate::app::db::datastore::VolumeToDelete;
use crate::app::db::datastore::VolumeWithTarget;
use crate::app::db::lookup::LookupPath;
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, authz, db};
use nexus_db_model::VmmState;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use propolis_client::types::ReplaceResult;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::CrucibleOpts;
use sled_agent_client::types::VolumeConstructionRequest;
use std::net::SocketAddrV6;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// region snapshot replacement step saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub request: db::model::RegionSnapshotReplacementStep,
}

// region snapshot replacement step saga: actions

declare_saga_actions! {
    region_snapshot_replacement_step;
    SET_SAGA_ID -> "unused_1" {
        + rsrss_set_saga_id
        - rsrss_set_saga_id_undo
    }
    CREATE_REPLACE_PARAMS -> "replace_params" {
        + rsrss_create_replace_params
    }
    CREATE_FAKE_VOLUME -> "unused_2" {
        + rssrs_create_fake_volume
        - rssrs_create_fake_volume_undo
    }
    REPLACE_SNAPSHOT_IN_VOLUME -> "unused_3" {
        + rsrss_replace_snapshot_in_volume
        - rsrss_replace_snapshot_in_volume_undo
    }
    NOTIFY_UPSTAIRS -> "unused_4" {
        + rsrss_notify_upstairs
    }
    UPDATE_REQUEST_RECORD -> "unused_5" {
        + rsrss_update_request_record
    }
}

// region snapshot replacement step saga: definition

#[derive(Debug)]
pub(crate) struct SagaRegionSnapshotReplacementStep;
impl NexusSaga for SagaRegionSnapshotReplacementStep {
    const NAME: &'static str = "region-snapshot-replacement-step";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        region_snapshot_replacement_step_register_actions(registry);
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
        builder.append(create_replace_params_action());
        builder.append(create_fake_volume_action());
        builder.append(replace_snapshot_in_volume_action());
        builder.append(notify_upstairs_action());
        builder.append(update_request_record_action());

        Ok(builder.build()?)
    }
}

// region snapshot replacement step saga: action implementations

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

    // Change the request record here to an intermediate "running" state to
    // block out other sagas that will be triggered for the same request.

    osagactx
        .datastore()
        .set_region_snapshot_replacement_step_running(
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
        .undo_set_region_snapshot_replacement_step_running(
            &opctx,
            params.request.id,
            saga_id,
        )
        .await?;

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplaceParams {
    old_snapshot_address: SocketAddrV6,
    new_region_address: SocketAddrV6,
}

async fn rsrss_create_replace_params(
    sagactx: NexusActionContext,
) -> Result<ReplaceParams, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // look up region snapshot replace request by id

    let region_snapshot_replace_request = osagactx
        .datastore()
        .get_region_snapshot_replacement_request_by_id(
            &opctx,
            params.request.request_id,
        )
        .await
        .map_err(ActionError::action_failed)?;

    let region_snapshot = osagactx
        .datastore()
        .region_snapshot_get(
            region_snapshot_replace_request.old_dataset_id,
            region_snapshot_replace_request.old_region_id,
            region_snapshot_replace_request.old_snapshot_id,
        )
        .await
        .map_err(ActionError::action_failed)?;

    let Some(region_snapshot) = region_snapshot else {
        return Err(ActionError::action_failed(format!(
            "region snapshot {} {} {} deleted!",
            region_snapshot_replace_request.old_dataset_id,
            region_snapshot_replace_request.old_region_id,
            region_snapshot_replace_request.old_snapshot_id,
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

    let Some(new_region_id) = region_snapshot_replace_request.new_region_id
    else {
        return Err(ActionError::action_failed(format!(
            "request {} does not have a new_region_id!",
            region_snapshot_replace_request.id,
        )));
    };

    let new_region_address = osagactx
        .nexus()
        .region_addr(&log, new_region_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(ReplaceParams { old_snapshot_address, new_region_address })
}

async fn rssrs_create_fake_volume(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();

    let new_volume_id = sagactx.lookup::<Uuid>("new_volume_id")?;

    // Create a fake volume record for the old snapshot target. This will be
    // deleted after region snapshot replacement step saga has finished, and the
    // region replacement snapshot gc step has run. It can be completely blank
    // here, it will be replaced by `volume_replace_snapshot`.

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

async fn rssrs_create_fake_volume_undo(
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
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let replace_params = sagactx.lookup::<ReplaceParams>("replace_params")?;

    let new_volume_id = sagactx.lookup::<Uuid>("new_volume_id")?;

    // `volume_replace_snapshot` will swap the old snapshot for the new region.
    // No repair or reconcilation needs to occur after this.
    osagactx
        .datastore()
        .volume_replace_snapshot(
            VolumeWithTarget(params.request.volume_id),
            ExistingTarget(replace_params.old_snapshot_address),
            ReplacementTarget(replace_params.new_region_address),
            VolumeToDelete(new_volume_id),
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn rsrss_replace_snapshot_in_volume_undo(
    sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let replace_params = sagactx.lookup::<ReplaceParams>("replace_params")?;

    let new_volume_id = sagactx.lookup::<Uuid>("new_volume_id")?;

    osagactx
        .datastore()
        .volume_replace_snapshot(
            VolumeWithTarget(params.request.volume_id),
            ExistingTarget(replace_params.new_region_address),
            ReplacementTarget(replace_params.old_snapshot_address),
            VolumeToDelete(new_volume_id),
        )
        .await?;

    Ok(())
}

async fn rsrss_notify_upstairs(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let log = sagactx.user_data().log();

    // Make an effort to notify a Propolis if one was booted for this volume.
    // This is best effort: if there is a failure, this saga will unwind and be
    // triggered again for the same request. If there is no Propolis booted for
    // this volume, then there's nothing to be done: any future Propolis will
    // receive the updated Volume.
    //
    // Unlike for region replacement, there's no step required here if there
    // isn't an active Propolis: any Upstairs created after the snapshot_addr
    // is replaced will reference the cloned data.

    let Some(disk) = osagactx
        .datastore()
        .disk_for_volume_id(params.request.volume_id)
        .await
        .map_err(ActionError::action_failed)?
    else {
        return Ok(());
    };

    let Some(instance_id) = disk.runtime().attach_instance_id else {
        return Ok(());
    };

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let (.., authz_instance) = LookupPath::new(&opctx, &osagactx.datastore())
        .instance_id(instance_id)
        .lookup_for(authz::Action::Read)
        .await
        .map_err(ActionError::action_failed)?;

    let instance_and_vmm = osagactx
        .datastore()
        .instance_fetch_with_vmm(&opctx, &authz_instance)
        .await
        .map_err(ActionError::action_failed)?;

    let Some(vmm) = instance_and_vmm.vmm() else {
        return Ok(());
    };

    let state = vmm.runtime.state;

    info!(
        log,
        "volume associated with disk attached to instance with vmm in \
        state {state}";
        "request id" => %params.request.id,
        "volume id" => %params.request.volume_id,
        "disk id" => ?disk.id(),
        "instance id" => ?instance_id,
        "vmm id" => ?vmm.id,
    );

    match &state {
        VmmState::Running | VmmState::Rebooting => {
            // Propolis server is ok to receive the volume replacement request.
        }

        VmmState::Starting
        | VmmState::Stopping
        | VmmState::Stopped
        | VmmState::Migrating
        | VmmState::Failed
        | VmmState::Destroyed
        | VmmState::SagaUnwound => {
            // Propolis server is not ok to receive volume replacement requests
            // - unwind so that this saga can run again.
            return Err(ActionError::action_failed(format!(
                "vmm {} propolis not in a state to receive request",
                vmm.id,
            )));
        }
    }

    let new_volume_vcr = match osagactx
        .datastore()
        .volume_get(params.request.volume_id)
        .await
        .map_err(ActionError::action_failed)?
    {
        Some(volume) => volume.data().to_string(),

        None => {
            return Err(ActionError::action_failed(Error::internal_error(
                "new volume is gone!",
            )));
        }
    };

    let instance_lookup =
        LookupPath::new(&opctx, &osagactx.datastore()).instance_id(instance_id);

    let (vmm, client) = osagactx
        .nexus()
        .propolis_client_for_instance(
            &opctx,
            &instance_lookup,
            authz::Action::Modify,
        )
        .await
        .map_err(ActionError::action_failed)?;

    info!(
        log,
        "sending replacement request for disk volume to propolis";
        "request id" => %params.request.id,
        "volume id" => %params.request.volume_id,
        "disk id" => ?disk.id(),
        "instance id" => ?instance_id,
        "vmm id" => ?vmm.id,
    );

    let result = client
        .instance_issue_crucible_vcr_request()
        .id(disk.id())
        .body(propolis_client::types::InstanceVcrReplace {
            name: disk.name().to_string(),
            vcr_json: new_volume_vcr,
        })
        .send()
        .await
        .map_err(|e| match e {
            propolis_client::Error::ErrorResponse(rv) => {
                ActionError::action_failed(rv.message.clone())
            }

            _ => ActionError::action_failed(format!(
                "unexpected failure during \
                        `instance_issue_crucible_vcr_request`: {e}",
            )),
        })?;

    let replace_result = result.into_inner();

    info!(
        log,
        "saw replace result {replace_result:?}";
        "request id" => %params.request.id,
        "volume id" => %params.request.volume_id,
        "disk id" => ?disk.id(),
        "instance id" => ?instance_id,
        "vmm id" => ?vmm.id,
    );

    match &replace_result {
        ReplaceResult::Started => {
            // This saga's call just started the replacement
        }

        ReplaceResult::StartedAlready => {
            // A previous run of this saga (or saga node) started the
            // replacement
        }

        ReplaceResult::CompletedAlready => {
            // It's done! We see this if the same propolis that received the
            // original replace request started and finished the replacement.
        }

        ReplaceResult::VcrMatches => {
            // This propolis booted with the updated VCR
        }

        ReplaceResult::Missing => {
            // The volume does not contain the region to be replaced. This is an
            // error!
            return Err(ActionError::action_failed(String::from(
                "saw ReplaceResult::Missing",
            )));
        }
    }

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
    let new_volume_id = sagactx.lookup::<Uuid>("new_volume_id")?;

    // Update the request record to 'Completed' and clear the operating saga id.
    // There is no undo step for this, it should succeed idempotently.
    datastore
        .set_region_snapshot_replacement_step_complete(
            &opctx,
            params.request.id,
            saga_id,
            new_volume_id,
        )
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}
