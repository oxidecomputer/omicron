// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # first, some Crucible background #
//!
//! Crucible's Upstairs has two methods of swapping in a new downstairs to a
//! region set:
//!
//! - A running Upstairs that is currently activated can be sent a request to
//!   replace a downstairs with a new one - this can be done while accepting all
//!   the usual IO requests. This is called _Live Repair_.
//!
//! - Prior to activation, an Upstairs will perform _Reconciliation_ to ensure
//!   that all the downstairs are consistent. Activation is held back until this
//!   is true.
//!
//! Each of these operations will ensure that each member of the three-way
//! mirror that is a region set is the same.
//!
//! Usually, each running Volume will have been constructed from a Volume
//! Construction Request (VCR) that Nexus created as part of a
//! `volume_checkout`.  This VCR is sent to a service (for example, a Propolis
//! or Pantry) and ultimately passed to `Volume::construct` to create a running
//! Volume. This is then activated, and then IO can proceed.
//!
//! # how did we get here? #
//!
//! The process of region replacement begins with a region replacement request.
//! Today this is created either manually with omdb, or as a result of a
//! physical disk being expunged. Affected VCRs are modified first by the region
//! replacement start saga, which includes allocating a new replacement region.
//! This then places the region replacement request into the state "Running".
//! See that saga's documentation for more information.
//!
//! # why does the drive saga exist? #
//!
//! Region replacement is similar to instance migration in that it is initiated
//! by Nexus but not directly controlled by it. Instance migration requires a
//! source and destination Propolis to exist, and then Nexus waits for a
//! callback to occur. For region replacement, it's Nexus' job to trigger
//! either the Live Repair or Reconciliation operations via some Upstairs. Nexus
//! then either receives a notification of success, or sees that the Volume is
//! no longer in a degraded state as the result of some polling operation.
//!
//! Note: _it's very important that only_ the Upstairs can make the
//! determination that a Volume is no longer degraded. Nexus should not be
//! assuming anything. This is the _golden rule_ that this saga must follow.
//!
//! Volumes are in this degraded state the moment one or more targets in a
//! region set is no longer functional. An Upstairs can still accept reads,
//! writes, and flushes with only two out of three present in the set, but it's
//! operating with a reduced redundancy.
//!
//! Through disk expungement, an operator has told Nexus that failure is not
//! transient. The region replacement start saga then modifies them: a blank
//! region is swapped in to replace one of the regions that are gone. Then this
//! saga triggers either Live Repair or Reconciliation, and that's it right?
//!
//! Volumes back higher level objects that users interact with: disks,
//! snapshots, images, etc. Users can start and stop Upstairs by starting and
//! stopping Instances. This interrupts any current operation on the Volume!
//! This is ok: both operations were designed so that interruptions are not a
//! problem, but it does stop progress.
//!
//! Say an Instance is running, and that Instance's propolis is performing a
//! Live Repair. If a user stops that Instance, the propolis is torn down, and
//! the Volume remains degraded. The next time that Volume is constructed and
//! activated, the Upstairs will check each downstairs in the region set, see
//! that there's a difference, and perform Reconciliation. If the user stops an
//! Instance and does not start it again, that difference will remain.
//!
//! Nexus can at that point send the Volume to a Pantry and activate it, causing
//! Reconciliation. At any time, the user can come along and start the Instance
//! in question, which would take over the activation from the Pantry - this
//! would cause that Reconciliation to fail, and the new propolis server would
//! start its own Reconciliation. Again, the user may then stop the Instance,
//! halting progress.
//!
//! This saga is responsible for driving forward the Volume repair process, by
//! initiating repair operations. One invocation of this saga is not enough to
//! repair a Volume: Nexus must continuously monitor the degraded Volumes and
//! initiate the necessary operation (LR or Reconciliation) until those Volumes
//! are no longer degraded. Those operations can fail or be interrupted at any
//! time due to user actions.
//!
//! # what does the saga do? #
//!
//! A background task will look at all region replacement requests in the
//! "Running" state, and call this saga for each one. This saga then does what's
//! required to fix these degraded Volumes.
//!
//! This saga handles the following region replacement request state
//! transitions:
//!
//! ```text
//!         Running  <--
//!                    |
//!            |       |
//!            v       |
//!                    |
//!         Driving  --
//!
//!            |
//!            v
//!
//!     ReplacementDone
//! ```
//!
//! The first thing this saga does is set itself as the "operating saga" for the
//! request, and change the state to "Driving". Then, it performs the following
//! (generic) steps:
//!
//! 1. If there was a previous repair step, check what the status of the
//!    Volume's repair is. Determine if there is action required by Nexus, if
//!    Nexus should wait, or if Nexus saw that some response that indicated the
//!    repair was done (don't forget the golden rule!).
//!
//!    If there was no previous repair step, then some action is required.
//!
//! 2. If there is action required, prepare an action that will initiate either
//!    Live Repair or Reconciliation, based on the current state of the world
//!    (noting that it's entirely possible that state will change before
//!    executing that action, and invalidate the action!).
//!
//! 3. If there is one, execute the action.
//!
//! 4. If an action was executed without error, then commit it to CRDB as a
//!    repair step.
//!
//! Recording the steps that were taken as part of repairing this Volume helps
//! this saga determine what to do, and can be helpful for Oxide support staff
//! if there's a problem.
//!
//! TODO: Cases not handled yet:
//! - a disk attached to a pantry for bulk imports
//!

use super::{
    ActionRegistry, NexusActionContext, NexusSaga, SagaInitError,
    ACTION_GENERATE_ID,
};
use crate::app::db::lookup::LookupPath;
use crate::app::sagas::common_storage::get_pantry_address;
use crate::app::sagas::declare_saga_actions;
use crate::app::{authn, authz, db};
use chrono::Utc;
use nexus_db_model::VmmState;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use propolis_client::types::ReplaceResult;
use serde::Deserialize;
use serde::Serialize;
use steno::ActionError;
use steno::Node;
use uuid::Uuid;

// region replacement drive saga: input parameters

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Params {
    pub serialized_authn: authn::saga::Serialized,
    pub request: db::model::RegionReplacement,
}

// region replacement drive saga: actions

declare_saga_actions! {
    region_replacement_drive;
    SET_SAGA_ID -> "unused_1" {
        + srrd_set_saga_id
        - srrd_set_saga_id_undo
    }
    DRIVE_REGION_REPLACEMENT_CHECK -> "check" {
        + srrd_drive_region_replacement_check
    }
    DRIVE_REGION_REPLACEMENT_PREPARE -> "prepare" {
        + srrd_drive_region_replacement_prepare
    }
    DRIVE_REGION_REPLACEMENT_EXECUTE -> "execute" {
        + srrd_drive_region_replacement_execute
    }
    DRIVE_REGION_REPLACEMENT_COMMIT -> "commit" {
        + srrd_drive_region_replacement_commit
        - srrd_drive_region_replacement_commit_undo
    }
    FINISH_SAGA -> "unused_2" {
        + srrd_finish_saga
    }
}

// region replacement drive saga: definition

#[derive(Debug)]
pub(crate) struct SagaRegionReplacementDrive;
impl NexusSaga for SagaRegionReplacementDrive {
    const NAME: &'static str = "region-replacement-drive";
    type Params = Params;

    fn register_actions(registry: &mut ActionRegistry) {
        region_replacement_drive_register_actions(registry);
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
            "job_id",
            "GenerateJobId",
            ACTION_GENERATE_ID.as_ref(),
        ));

        builder.append(set_saga_id_action());

        builder.append(drive_region_replacement_check_action());
        builder.append(drive_region_replacement_prepare_action());
        builder.append(drive_region_replacement_execute_action());
        builder.append(drive_region_replacement_commit_action());

        builder.append(finish_saga_action());

        Ok(builder.build()?)
    }
}

// region replacement drive saga: action implementations

async fn srrd_set_saga_id(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let saga_id = sagactx.lookup::<Uuid>("saga_id")?;

    // Change the request record here to an intermediate "driving" state to
    // block out other sagas that will be triggered for the same request.
    osagactx
        .datastore()
        .set_region_replacement_driving(&opctx, params.request.id, saga_id)
        .await
        .map_err(ActionError::action_failed)?;

    Ok(())
}

async fn srrd_set_saga_id_undo(
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
        .undo_set_region_replacement_driving(&opctx, params.request.id, saga_id)
        .await?;

    Ok(())
}

/// What is the status of the repair?
#[derive(Debug, Serialize, Deserialize)]
enum DriveCheck {
    /// The last step is still running, so don't do anything
    LastStepStillRunning,

    /// The last step is not still running, but all we can do is wait.
    Wait,

    /// We got some status that indicates that the region has been replaced!
    Done,

    /// Some action is required. Either the last step is no longer running, or
    /// the repair needs to be unstuck.
    ActionRequired,
}

async fn srrd_drive_region_replacement_check(
    sagactx: NexusActionContext,
) -> Result<DriveCheck, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let last_request_step = osagactx
        .datastore()
        .current_region_replacement_request_step(&opctx, params.request.id)
        .await
        .map_err(ActionError::action_failed)?;

    let Some(last_request_step) = last_request_step else {
        // This is the first time this saga was invoked for this particular
        // replacement request, so some action is required
        info!(
            log,
            "no steps taken yet";
            "region replacement id" => %params.request.id,
        );

        return Ok(DriveCheck::ActionRequired);
    };

    // If the last request step is still "running", then check on it, and
    // determine if any action is required.

    match last_request_step.step_type {
        db::model::RegionReplacementStepType::Propolis => {
            let Some((step_instance_id, step_vmm_id)) =
                last_request_step.instance_and_vmm_ids()
            else {
                // This record is invalid, but we can still attempt to drive the
                // repair forward.
                error!(
                    log,
                    "step at {} has no associated ids", last_request_step.step_time;
                    "region replacement id" => ?params.request.id,
                    "last replacement drive time" => ?last_request_step.step_time,
                    "last replacement drive step" => "propolis",
                );

                return Ok(DriveCheck::ActionRequired);
            };

            let (.., authz_instance) =
                LookupPath::new(&opctx, &osagactx.datastore())
                    .instance_id(step_instance_id)
                    .lookup_for(authz::Action::Read)
                    .await
                    .map_err(ActionError::action_failed)?;

            let instance_and_vmm = osagactx
                .datastore()
                .instance_fetch_with_vmm(&opctx, &authz_instance)
                .await
                .map_err(ActionError::action_failed)?;

            // When this saga recorded a Propolis replacement step, an instance
            // existed and had a running vmm. Is this true now?

            let Some(current_vmm) = instance_and_vmm.vmm() else {
                // There is no current VMM, but if the current repair step was
                // `Propolis` then there was previously one. Some action is
                // required: namely, attach disk to the pantry and let it
                // perform reconcilation.

                info!(
                    log,
                    "instance from last step no longer has vmm";
                    "region replacement id" => ?params.request.id,
                    "last replacement drive time" => ?last_request_step.step_time,
                    "last replacement drive step" => "propolis",
                    "instance id" => ?step_instance_id,
                );

                return Ok(DriveCheck::ActionRequired);
            };

            // Check if the VMM has changed.

            if current_vmm.id != step_vmm_id {
                // The VMM has changed! This can be due to a stop and start of
                // the instance, or a migration. If this is the case, then the
                // new VMM (propolis server) will be performing reconcilation as
                // part of the Volume activation. Nexus should be receiving
                // notifications from the Upstairs there.
                //
                // If the new vmm is in the right state, this drive saga can
                // re-send the target replacement request to poll if the
                // replacement is done yet.

                info!(
                    log,
                    "vmm has changed from last step";
                    "region replacement id" => ?params.request.id,
                    "last replacement drive time" => ?last_request_step.step_time,
                    "last replacement drive step" => "propolis",
                    "instance id" => ?step_instance_id,
                    "old vmm id" => ?step_vmm_id,
                    "new vmm id" => ?current_vmm.id,
                );

                Ok(DriveCheck::ActionRequired)
            } else {
                // The VMM has not changed: check if the VMM is still active.

                let state = current_vmm.runtime.state;

                info!(
                    log,
                    "vmm from last step in state {}", state;
                    "region replacement id" => ?params.request.id,
                    "last replacement drive time" => ?last_request_step.step_time,
                    "last replacement drive step" => "propolis",
                    "instance id" => ?step_instance_id,
                    "vmm id" => ?step_vmm_id,
                );

                match &state {
                    // If propolis is running, or rebooting, then it is likely
                    // that the Upstairs that was previously sent the volume
                    // replacement request is still running the live repair
                    // (note: rebooting does not affect the running volume).
                    VmmState::Running | VmmState::Rebooting => {
                        // Until crucible#1277 is merged, choose to _not_ poll
                        // Propolis (which would happen if ActionRequired was
                        // returned here).
                        //
                        // TODO Nexus needs to poll, as it could miss receiving
                        // the "Finished" notification that would complete this
                        // region replacement. Most of the time it will receive
                        // that ok though.

                        Ok(DriveCheck::LastStepStillRunning)
                    }

                    VmmState::Starting => {
                        // This state is unexpected, considering Nexus
                        // previously sent a target replacement request to this
                        // propolis!

                        return Err(ActionError::action_failed(format!(
                            "vmm {} propolis is Starting",
                            step_vmm_id,
                        )));
                    }

                    VmmState::Stopping
                    | VmmState::Stopped
                    | VmmState::Migrating
                    | VmmState::Failed
                    | VmmState::Destroyed
                    | VmmState::SagaUnwound => {
                        // The VMM we sent the replacement request to is
                        // probably not operating on the request anymore. Wait
                        // to see where to send the next action: if the instance
                        // is migrating, eventually that will be a new propolis.
                        // If the instance is stopping, then that will be a
                        // Pantry. Otherwise, the saga will wait: propolis
                        // should only receive target replacement requests when
                        // in a good state.

                        Ok(DriveCheck::Wait)
                    }
                }
            }
        }

        db::model::RegionReplacementStepType::Pantry => {
            // Check if the Pantry is still trying to activate the Volume

            let Some(pantry_address) = last_request_step.pantry_address()
            else {
                // This record is invalid, but we can still attempt to drive the
                // repair forward.

                error!(
                    log,
                    "step has no associated pantry address";
                    "region replacement id" => %params.request.id,
                    "last replacement drive time" => ?last_request_step.step_time,
                    "last replacement drive step" => "pantry",
                );

                return Ok(DriveCheck::ActionRequired);
            };

            let endpoint = format!("http://{}", pantry_address);
            let client = crucible_pantry_client::Client::new(&endpoint);

            let Some(job_id) = last_request_step.step_associated_pantry_job_id
            else {
                // This record is invalid, but we can still attempt to drive the
                // repair forward.

                error!(
                    log,
                    "step has no associated pantry job id";
                    "region replacement id" => %params.request.id,
                    "last replacement drive time" => ?last_request_step.step_time,
                    "last replacement drive step" => "pantry",
                    "pantry address" => ?pantry_address,
                );

                return Ok(DriveCheck::ActionRequired);
            };

            // If there is a committed step, Nexus attached this Volume to a
            // Pantry, and requested activation in a background job. Is it
            // finished?

            match client.is_job_finished(&job_id.to_string()).await {
                Ok(status) => {
                    if status.job_is_finished {
                        // The job could be done because it failed: check the
                        // volume status to query if it is active or gone.

                        let Some(new_region_id) = params.request.new_region_id
                        else {
                            return Err(ActionError::action_failed(format!(
                                "region replacement request {} has new_region_id = None",
                                params.request.id,
                            )));
                        };

                        let new_region: db::model::Region = osagactx
                            .datastore()
                            .get_region(new_region_id)
                            .await
                            .map_err(ActionError::action_failed)?;

                        let volume_id = new_region.volume_id().to_string();

                        match client.volume_status(&volume_id).await {
                            Ok(volume_status) => {
                                info!(
                                    log,
                                    "pantry job finished, saw status {volume_status:?}";
                                    "region replacement id" => %params.request.id,
                                    "last replacement drive time" => ?last_request_step.step_time,
                                    "last replacement drive step" => "pantry",
                                    "pantry address" => ?pantry_address,
                                );

                                if volume_status.seen_active {
                                    // It may not be active now if a Propolis
                                    // activated the volume, but if the Pantry's
                                    // ever seen this Volume active before, then
                                    // the reconciliation completed ok.

                                    Ok(DriveCheck::Done)
                                } else {
                                    // The Pantry has never seen this active
                                    // before, and the job finished - some
                                    // action is required, the job failed.

                                    Ok(DriveCheck::ActionRequired)
                                }
                            }

                            Err(e) => {
                                // Seeing 410 Gone here may mean that the pantry
                                // performed reconciliation successfully, but
                                // had a propolis activation take over from the
                                // pantry's. If this occurred before a
                                // "reconciliation successful" notification
                                // occurred, and the propolis activation does
                                // not require a reconcilation (because the
                                // pantry did it already), then another
                                // notification will not be resent by propolis.
                                //
                                // Return ActionRequired here so that this saga
                                // will re-send the target replacement request
                                // to the propolis the did the take over: if the
                                // above race occurred, that request will return
                                // ReplaceResult::VcrMatches.

                                error!(
                                    log,
                                    "pantry job finished, saw error {e}";
                                    "region replacement id" => %params.request.id,
                                    "last replacement drive time" => ?last_request_step.step_time,
                                    "last replacement drive step" => "pantry",
                                    "pantry address" => ?pantry_address,
                                );

                                Ok(DriveCheck::ActionRequired)
                            }
                        }
                    } else {
                        info!(
                            log,
                            "pantry is still performing reconcilation";
                            "region replacement id" => %params.request.id,
                            "last replacement drive time" => ?last_request_step.step_time,
                            "last replacement drive step" => "pantry",
                            "pantry address" => ?pantry_address,
                        );

                        Ok(DriveCheck::LastStepStillRunning)
                    }
                }

                Err(e) => {
                    // If there was some problem accessing the Pantry. It may be
                    // because that Pantry is now gone, so check on it.

                    error!(
                        log,
                        "pantry returned an error checking job {job_id}: {e}";
                        "region replacement id" => %params.request.id,
                        "last replacement drive time" => ?last_request_step.step_time,
                        "last replacement drive step" => "pantry",
                        "pantry address" => ?pantry_address,
                    );

                    match client.pantry_status().await {
                        Ok(_) => {
                            // The pantry responded, so it's still there. It may
                            // be that the volume is no longer attached because
                            // a Propolis activation took over from the Pantry.

                            let Some(new_region_id) =
                                params.request.new_region_id
                            else {
                                return Err(ActionError::action_failed(format!(
                                    "region replacement request {} has new_region_id = None",
                                    params.request.id,
                                )));
                            };

                            let new_region: db::model::Region = osagactx
                                .datastore()
                                .get_region(new_region_id)
                                .await
                                .map_err(ActionError::action_failed)?;

                            let volume_id = new_region.volume_id().to_string();

                            match client.volume_status(&volume_id).await {
                                Ok(_) => {
                                    // The volume is still there as an entry,
                                    // but the job isn't? Action is required:
                                    // this saga should delete the attached
                                    // volume, then re-attach it.

                                    info!(
                                        log,
                                        "pantry still has active volume";
                                        "region replacement id" => %params.request.id,
                                        "last replacement drive time" => ?last_request_step.step_time,
                                        "last replacement drive step" => "pantry",
                                        "pantry address" => ?pantry_address,
                                        "volume id" => volume_id,
                                    );

                                    Ok(DriveCheck::ActionRequired)
                                }

                                Err(e) => {
                                    // The volume is gone: it's likely been
                                    // activated by a Propolis, but this could
                                    // also be because the Pantry bounced. Some
                                    // further action is required: either poll
                                    // the propolis that stole the activation or
                                    // send the volume to a new Pantry.

                                    error!(
                                        log,
                                        "pantry returned an error checking on volume: {e}";
                                        "region replacement id" => %params.request.id,
                                        "last replacement drive time" => ?last_request_step.step_time,
                                        "last replacement drive step" => "pantry",
                                        "pantry address" => ?pantry_address,
                                        "volume id" => volume_id,
                                    );

                                    Ok(DriveCheck::ActionRequired)
                                }
                            }
                        }

                        Err(e) => {
                            // The pantry is not responding on its status
                            // endpoint.  Further action is required to drive
                            // the repair, which may be attaching to another
                            // Pantry.

                            error!(
                                log,
                                "pantry returned an error checking on status: {e}";
                                "region replacement id" => %params.request.id,
                                "last replacement drive time" => ?last_request_step.step_time,
                                "last replacement drive step" => "pantry",
                                "pantry address" => ?pantry_address,
                            );

                            Ok(DriveCheck::ActionRequired)
                        }
                    }
                }
            }
        }
    }
}

/// What action does this saga invocation need to take?
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize)]
enum DriveAction {
    /// Do nothing - the repair is proceeding from the last drive step, or is
    /// done.
    Noop { replacement_done: bool },

    /// If there is no active Propolis that is running the Volume, attach the
    /// associated Volume to a Pantry.
    Pantry { step: db::model::RegionReplacementStep, volume_id: Uuid },

    /// If the Volume is currently running in a Propolis server, then send the
    /// volume replacement request there.
    Propolis { step: db::model::RegionReplacementStep, disk: db::model::Disk },
}

async fn srrd_drive_region_replacement_prepare(
    sagactx: NexusActionContext,
) -> Result<DriveAction, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    // If the previous saga step did _not_ require an action, then return Noop
    // here.

    let check_result = sagactx.lookup::<DriveCheck>("check")?;

    if !matches!(check_result, DriveCheck::ActionRequired) {
        return Ok(DriveAction::Noop {
            replacement_done: matches!(check_result, DriveCheck::Done),
        });
    }

    // Otherwise, take a look at the state of the world, and prepare an action
    // to execute.

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let nexus = osagactx.nexus();

    let Some(new_region_id) = params.request.new_region_id else {
        return Err(ActionError::action_failed(format!(
            "region replacement request {} has new_region_id = None",
            params.request.id,
        )));
    };

    let new_region: db::model::Region = osagactx
        .datastore()
        .get_region(new_region_id)
        .await
        .map_err(ActionError::action_failed)?;

    let maybe_disk = osagactx
        .datastore()
        .disk_for_volume_id(new_region.volume_id())
        .await
        .map_err(ActionError::action_failed)?;

    // Does this volume back a disk?
    let drive_action = if let Some(disk) = maybe_disk {
        match &disk.runtime().attach_instance_id {
            Some(instance_id) => {
                // The region's volume is attached to an instance
                let (.., authz_instance) =
                    LookupPath::new(&opctx, &osagactx.datastore())
                        .instance_id(*instance_id)
                        .lookup_for(authz::Action::Read)
                        .await
                        .map_err(ActionError::action_failed)?;

                let instance_and_vmm = osagactx
                    .datastore()
                    .instance_fetch_with_vmm(&opctx, &authz_instance)
                    .await
                    .map_err(ActionError::action_failed)?;

                match instance_and_vmm.vmm() {
                    Some(vmm) => {
                        // The disk is attached to an instance and there's an
                        // active propolis server. Send the volume replacement
                        // request to the running Volume there if the runtime
                        // state is either running or rebooting.

                        let state = vmm.runtime.state;

                        info!(
                            log,
                            "disk attached to instance with vmm in state {state}";
                            "region replacement id" => %params.request.id,
                            "disk id" => ?disk.id(),
                            "instance id" => ?instance_id,
                            "vmm id" => ?vmm.id,
                        );

                        match &state {
                            VmmState::Running | VmmState::Rebooting => {
                                // Propolis server is ok to receive the volume
                                // replacement request.
                            }

                            VmmState::Starting
                            | VmmState::Stopping
                            | VmmState::Stopped
                            | VmmState::Migrating
                            | VmmState::Failed
                            | VmmState::Destroyed
                            | VmmState::SagaUnwound => {
                                // Propolis server is not ok to receive volume
                                // replacement requests, bail out
                                return Err(ActionError::action_failed(format!(
                                    "vmm {} propolis not in a state to receive request",
                                    vmm.id,
                                )));
                            }
                        }

                        DriveAction::Propolis {
                            step: db::model::RegionReplacementStep {
                                replacement_id: params.request.id,
                                step_time: Utc::now(),
                                step_type: db::model::RegionReplacementStepType::Propolis,

                                step_associated_instance_id: Some(*instance_id),
                                step_associated_vmm_id: Some(vmm.id),

                                step_associated_pantry_ip: None,
                                step_associated_pantry_port: None,
                                step_associated_pantry_job_id: None,
                            },

                            disk,
                        }
                    }

                    None => {
                        // The disk is attached to an instance but there's no
                        // active propolis server. Attach to a pantry.

                        let state =
                            &instance_and_vmm.instance().runtime().nexus_state;

                        info!(
                            log,
                            "disk attached to instance in state {state} with no vmm";
                            "region replacement id" => %params.request.id,
                            "disk id" => ?disk.id(),
                            "instance id" => ?instance_id,
                        );

                        let pantry_address =
                            get_pantry_address(osagactx.nexus()).await?;

                        DriveAction::Pantry {
                            step: db::model::RegionReplacementStep {
                                replacement_id: params.request.id,
                                step_time: Utc::now(),
                                step_type:
                                    db::model::RegionReplacementStepType::Pantry,

                                step_associated_instance_id: None,
                                step_associated_vmm_id: None,

                                step_associated_pantry_ip: Some(
                                    pantry_address.ip().into(),
                                ),
                                step_associated_pantry_port: Some(
                                    pantry_address.port().into(),
                                ),
                                step_associated_pantry_job_id: Some(
                                    sagactx.lookup::<Uuid>("job_id")?,
                                ),
                            },

                            volume_id: new_region.volume_id(),
                        }
                    }
                }
            }

            None => {
                // The disk is not attached to an instance. Is it attached to a
                // Pantry right now (aka performing bulk import)?

                if let Some(address) = &disk.pantry_address {
                    // TODO currently unsupported
                    return Err(ActionError::action_failed(format!(
                        "disk {} attached to {address}, not supported",
                        disk.id(),
                    )));
                }

                // Attach to a pantry.

                info!(
                    log,
                    "disk not attached to instance";
                    "region replacement id" => %params.request.id,
                    "disk id" => ?disk.id(),
                );

                // XXX: internal-dns does not randomize the order of addresses
                // in its responses: if the first Pantry in the list of
                // addresses returned by DNS isn't responding, the drive saga
                // will still continually try to use it.

                let pantry_address = get_pantry_address(nexus).await?;

                DriveAction::Pantry {
                    step: db::model::RegionReplacementStep {
                        replacement_id: params.request.id,
                        step_time: Utc::now(),
                        step_type: db::model::RegionReplacementStepType::Pantry,

                        step_associated_instance_id: None,
                        step_associated_vmm_id: None,

                        step_associated_pantry_ip: Some(
                            pantry_address.ip().into(),
                        ),
                        step_associated_pantry_port: Some(
                            pantry_address.port().into(),
                        ),
                        step_associated_pantry_job_id: Some(
                            sagactx.lookup::<Uuid>("job_id")?,
                        ),
                    },

                    volume_id: new_region.volume_id(),
                }
            }
        }
    } else {
        // Is this volume the destination volume for a snapshot?

        let maybe_snapshot = osagactx
            .datastore()
            .find_snapshot_by_destination_volume_id(
                &opctx,
                new_region.volume_id(),
            )
            .await
            .map_err(ActionError::action_failed)?;

        if maybe_snapshot.is_some() {
            // Volume is the destination that snapshot blocks should be scrubbed
            // into. The scrubber is not written yet, so nothing should be using
            // this volume yet. We can attach it to the Pantry.

            info!(
                log,
                "volume is for a snapshot destination";
                "region replacement id" => %params.request.id,
            );

            let pantry_address = get_pantry_address(nexus).await?;

            DriveAction::Pantry {
                step: db::model::RegionReplacementStep {
                    replacement_id: params.request.id,
                    step_time: Utc::now(),
                    step_type: db::model::RegionReplacementStepType::Pantry,

                    step_associated_instance_id: None,
                    step_associated_vmm_id: None,

                    step_associated_pantry_ip: Some(pantry_address.ip().into()),
                    step_associated_pantry_port: Some(
                        pantry_address.port().into(),
                    ),
                    step_associated_pantry_job_id: Some(
                        sagactx.lookup::<Uuid>("job_id")?,
                    ),
                },

                volume_id: new_region.volume_id(),
            }
        } else {
            // XXX what other volumes are created?
            return Err(ActionError::action_failed(format!(
                "don't know what to do with volume {}",
                new_region.volume_id(),
            )));
        }
    };

    Ok(drive_action)
}

#[derive(Debug, Serialize, Deserialize)]
struct ExecuteResult {
    step_to_commit: Option<db::model::RegionReplacementStep>,
    replacement_done: bool,
}

/// Attempt to execute the prepared step. If it was successful, return the step
/// to commit to the database.
async fn srrd_drive_region_replacement_execute(
    sagactx: NexusActionContext,
) -> Result<ExecuteResult, ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let nexus = osagactx.nexus();

    // Look up the prepared action, and execute it. If something has changed
    // between when the action was determined and now, then bail out - the next
    // drive saga invocation will pick up the new state of the world and act
    // accordingly.

    let action = sagactx.lookup::<DriveAction>("prepare")?;

    let result = match action {
        DriveAction::Noop { replacement_done } => {
            // *slaps knees and stands up* welp
            ExecuteResult { step_to_commit: None, replacement_done }
        }

        DriveAction::Pantry { step, volume_id } => {
            // Importantly, _do not use `call_pantry_attach_for_disk`_! That
            // uses `retry_until_known_result`, which we _do not want here_. The
            // Pantry attach can fail if there's a racing Volume checkout to be
            // sent to Propolis. Additionally, it uses `attach` instead of
            // `attach_activate_background`, which means it will hang on the
            // activation.

            let Some(pantry_address) = step.pantry_address() else {
                return Err(ActionError::action_failed(String::from(
                    "pantry step does not have an address",
                )));
            };

            let endpoint = format!("http://{}", pantry_address);
            let client = crucible_pantry_client::Client::new(&endpoint);

            // Check pantry first, to see if this volume is attached already. This can
            // occur if:
            //
            // - the volume is attached to the target pantry, but it can't be reliably
            //   determined if reconcilation finished.
            //
            // - a previous repair operated on another region in the same Volume, and that
            //   attachment was not garbage collected.
            //
            // Try to get the volume's status in order to check.

            let detach_required = match client
                .volume_status(&volume_id.to_string())
                .await
            {
                Ok(volume_status) => {
                    info!(
                        log,
                        "volume is already attached with status {volume_status:?}";
                        "region replacement id" => %params.request.id,
                        "volume id" => ?volume_id,
                        "endpoint" => endpoint.clone(),
                    );

                    // Detach this volume so we can reattach with this saga's job id.
                    true
                }

                Err(e) => {
                    match e {
                        crucible_pantry_client::Error::ErrorResponse(
                            ref rv,
                        ) => {
                            match rv.status() {
                                http::StatusCode::NOT_FOUND => {
                                    // No detach required, this Volume isn't attached to
                                    // this Pantry.
                                    false
                                }

                                http::StatusCode::GONE => {
                                    // 410 Gone means detach is required - it was
                                    // previously attached and may have been activated
                                    true
                                }

                                _ => {
                                    error!(
                                        log,
                                        "error checking volume status: {e}";
                                        "region replacement id" => %params.request.id,
                                        "volume id" => ?volume_id,
                                        "endpoint" => endpoint.clone(),
                                    );

                                    return Err(ActionError::action_failed(Error::internal_error(
                                        &format!("unexpected error from volume_status: {e}")
                                    )));
                                }
                            }
                        }

                        _ => {
                            error!(
                                log,
                                "error checking volume status: {e}";
                                "region replacement id" => %params.request.id,
                                "volume id" => ?volume_id,
                                "endpoint" => endpoint.clone(),
                            );

                            return Err(ActionError::action_failed(
                                Error::internal_error(&format!(
                                    "unexpected error from volume_status: {e}"
                                )),
                            ));
                        }
                    }
                }
            };

            if detach_required {
                info!(
                    log,
                    "detach required";
                    "region replacement id" => %params.request.id,
                    "volume id" => ?volume_id,
                    "endpoint" => endpoint.clone(),
                );

                match client.detach(&volume_id.to_string()).await {
                    Ok(_) => {
                        info!(
                            log,
                            "detached volume";
                            "region replacement id" => %params.request.id,
                            "volume id" => ?volume_id,
                            "endpoint" => endpoint.clone(),
                        );
                    }

                    Err(e) => {
                        error!(
                            log,
                            "error detaching volume: {e}";
                            "region replacement id" => %params.request.id,
                            "volume id" => ?volume_id,
                            "endpoint" => endpoint.clone(),
                        );

                        // Cannot continue: the Pantry will return an error unless the
                        // volume construction request matches what was originally
                        // attached, and the job id matches what was originally sent. Even
                        // if the VCR is the same, this saga does not have the same job
                        // id. Bail out here: hopefully the next time this saga runs, it
                        // will select a different Pantry.

                        return Err(ActionError::action_failed(Error::invalid_request(
                            String::from("cannot proceed, pantry will reject our request")
                        )));
                    }
                }
            } else {
                info!(
                    log,
                    "no detach required";
                    "region replacement id" => %params.request.id,
                    "volume id" => ?volume_id,
                    "endpoint" => endpoint.clone(),
                );
            }

            // Attach the volume to the pantry, and let reconciliation occur.

            info!(
                log,
                "sending attach for volume";
                "region replacement id" => %params.request.id,
                "volume id" => ?volume_id,
                "endpoint" => endpoint.clone(),
            );

            let disk_volume = nexus
                .datastore()
                .volume_checkout(
                    volume_id,
                    db::datastore::VolumeCheckoutReason::Pantry,
                )
                .await
                .map_err(ActionError::action_failed)?;

            let volume_construction_request: crucible_pantry_client::types::VolumeConstructionRequest =
                serde_json::from_str(&disk_volume.data()).map_err(|e| {
                    ActionError::action_failed(Error::internal_error(&format!(
                        "failed to deserialize volume {volume_id} data: {e}",
                    )))
                })?;

            let job_id = sagactx.lookup::<Uuid>("job_id")?;

            let attach_request =
                crucible_pantry_client::types::AttachBackgroundRequest {
                    volume_construction_request,
                    job_id: job_id.to_string(),
                };

            client
                .attach_activate_background(
                    &volume_id.to_string(),
                    &attach_request,
                )
                .await
                .map_err(|e| {
                    ActionError::action_failed(format!(
                        "pantry attach failed with {:?}",
                        e,
                    ))
                })?;

            ExecuteResult {
                step_to_commit: Some(step),
                replacement_done: false,
            }
        }

        DriveAction::Propolis { step, disk } => {
            // The disk is attached to an instance and there's an active
            // propolis server. Send a volume replacement request to the running
            // Volume there - either it will start a live repair, or be ignored
            // because there is no difference in the volume construction
            // request.

            let disk_new_volume_vcr = match osagactx
                .datastore()
                .volume_get(disk.volume_id)
                .await
                .map_err(ActionError::action_failed)?
            {
                Some(volume) => volume.data().to_string(),

                None => {
                    return Err(ActionError::action_failed(
                        Error::internal_error("new volume is gone!"),
                    ));
                }
            };

            let Some((instance_id, vmm_id)) = step.instance_and_vmm_ids()
            else {
                return Err(ActionError::action_failed(Error::internal_error(
                    "propolis step does not have instance and vmm ids",
                )));
            };

            let instance_lookup =
                LookupPath::new(&opctx, &osagactx.datastore())
                    .instance_id(instance_id);

            let (vmm, client) = nexus
                .propolis_client_for_instance(
                    &opctx,
                    &instance_lookup,
                    authz::Action::Modify,
                )
                .await
                .map_err(ActionError::action_failed)?;

            // This client could be for a different VMM than the step was
            // prepared for. Bail out if this is true
            if vmm.id != vmm_id {
                return Err(ActionError::action_failed(format!(
                    "propolis client vmm {} does not match step vmm {}",
                    vmm.id, vmm_id,
                )));
            }

            info!(
                log,
                "sending replacement request for disk volume to propolis {vmm_id}";
                "region replacement id" => %params.request.id,
                "disk id" => ?disk.id(),
                "volume id" => ?disk.volume_id,
            );

            // Start (or poll) the replacement
            let result = client
                .instance_issue_crucible_vcr_request()
                .id(disk.id())
                .body(
                    propolis_client::types::InstanceVcrReplace {
                        name: disk.name().to_string(),
                        vcr_json: disk_new_volume_vcr.to_string(),
                    }
                )
                .send()
                .await
                .map_err(|e| match e {
                    propolis_client::Error::ErrorResponse(
                        rv,
                    ) => {
                        ActionError::action_failed(rv.message.clone())
                    }

                    _ => {
                        ActionError::action_failed(
                            format!(
                                "unexpected failure during `instance_issue_crucible_vcr_request`: {e}",
                            )
                        )
                    }
                })?;

            let replace_result = result.into_inner();

            info!(
                log,
                "saw replace result {replace_result:?}";
                "region replacement id" => %params.request.id,
                "disk id" => ?disk.id(),
                "volume id" => ?disk.volume_id,
            );

            let replacement_done = match &replace_result {
                ReplaceResult::Started => {
                    // This drive saga's call just started the replacement
                    false
                }

                ReplaceResult::StartedAlready => {
                    // A previous drive saga's call started the replacement, but
                    // it's not done yet.
                    false
                }

                ReplaceResult::CompletedAlready => {
                    // It's done! We see this if the same propolis that received
                    // the original replace request started and finished the
                    // live repair.
                    true
                }

                ReplaceResult::VcrMatches => {
                    // If this propolis booted after the volume construction
                    // request was modified but before all the regions were
                    // reconciled, then `VcrMatches` will be seen as a result of
                    // `target_replace`: the new propolis will have received the
                    // updated VCR when it was created.
                    //
                    // The upstairs will be performing reconciliation (or have
                    // previously performed it), not live repair, and will have
                    // no record of a previous replace request (sent to a
                    // different propolis!) starting a live repair.
                    //
                    // If the Volume is active, that means reconcilation
                    // completed ok, and therefore Nexus can consider this
                    // repair complete. This is only true if one repair occurs
                    // at a time per volume (which is true due to the presense
                    // of volume_repair records), and if this saga locks the
                    // region replacement request record as part of it executing
                    // (which it does through the SET_SAGA_ID forward action).
                    // If either of those conditions are not held, then multiple
                    // replacement calls and activation checks can interleave
                    // and confuse this saga.
                    //
                    // Check if the Volume activated.

                    let result = client
                        .disk_volume_status()
                        .id(disk.id())
                        .send()
                        .await
                        .map_err(|e| match e {
                            propolis_client::Error::ErrorResponse(
                                rv,
                            ) => {
                                ActionError::action_failed(rv.message.clone())
                            }

                            _ => {
                                ActionError::action_failed(
                                    format!(
                                        "unexpected failure during `disk_volume_status`: {e}",
                                    )
                                )
                            }
                        })?;

                    // If the Volume is active, then reconciliation finished
                    // successfully.
                    //
                    // There's a few reasons it may not be active yet:
                    //
                    // - Propolis could be shutting down, and tearing down the
                    //   Upstairs in the process (which deactivates the Volume)
                    //
                    // - reconciliation could still be going on
                    //
                    // - reconciliation could have failed
                    //
                    // If it's not active, wait until the next invocation of
                    // this saga to decide what to do next.

                    result.into_inner().active
                }

                ReplaceResult::Missing => {
                    // The disk's volume does not contain the region to be
                    // replaced. This is an error!

                    return Err(ActionError::action_failed(String::from(
                        "saw ReplaceResult::Missing",
                    )));
                }
            };

            ExecuteResult { step_to_commit: Some(step), replacement_done }
        }
    };

    Ok(result)
}

async fn srrd_drive_region_replacement_commit(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let log = sagactx.user_data().log();
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;

    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    // If there was an executed step, record it!

    let execute_result = sagactx.lookup::<ExecuteResult>("execute")?;

    if let Some(step) = execute_result.step_to_commit {
        info!(
            log,
            "committing step {}", step.step_time;
            "region replacement id" => %params.request.id,
        );

        osagactx
            .datastore()
            .add_region_replacement_request_step(&opctx, step)
            .await
            .map_err(ActionError::action_failed)?;
    } else {
        info!(
            log,
            "no step to commit";
            "region replacement id" => %params.request.id,
        );
    }

    Ok(())
}

async fn srrd_drive_region_replacement_commit_undo(
    _sagactx: NexusActionContext,
) -> Result<(), anyhow::Error> {
    // If this saga unwinds at the last step, do we have to remove the committed
    // step from db? The problem is that we did execute the step, and it's not
    // something we can roll back. Leave the last step in the DB so it can be
    // referenced during the check step the next time this saga is invoked.

    Ok(())
}

async fn srrd_finish_saga(
    sagactx: NexusActionContext,
) -> Result<(), ActionError> {
    let osagactx = sagactx.user_data();
    let params = sagactx.saga_params::<Params>()?;
    let opctx = crate::context::op_context_for_saga_action(
        &sagactx,
        &params.serialized_authn,
    );

    let saga_id = sagactx.lookup::<Uuid>("saga_id")?;

    let execute_result = sagactx.lookup::<ExecuteResult>("execute")?;

    // Use the same undo function to exit the saga. If it was determined that
    // the region replacement is done, transition to ReplacementDone, else
    // transition back to Running.
    if execute_result.replacement_done {
        osagactx
            .datastore()
            .set_region_replacement_from_driving_to_done(
                &opctx,
                params.request.id,
                saga_id,
            )
            .await
            .map_err(ActionError::action_failed)?;
    } else {
        osagactx
            .datastore()
            .undo_set_region_replacement_driving(
                &opctx,
                params.request.id,
                saga_id,
            )
            .await
            .map_err(ActionError::action_failed)?;
    }

    Ok(())
}
