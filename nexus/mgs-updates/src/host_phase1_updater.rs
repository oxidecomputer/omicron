// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating host OS phase1 images via MGS.

use super::MgsClients;
use super::SpComponentUpdateError;
use super::UpdateProgress;
use super::common_sp_update::SpComponentUpdater;
use super::common_sp_update::deliver_update;
use crate::SpComponentUpdateHelper;
use crate::common_sp_update::FoundArtifact;
use crate::common_sp_update::PostUpdateError;
use crate::common_sp_update::PrecheckError;
use crate::common_sp_update::PrecheckStatus;
use futures::FutureExt as _;
use futures::future::BoxFuture;
use gateway_client::HostPhase1HashError;
use gateway_client::SpComponent;
use gateway_client::types::PowerState;
use gateway_client::types::SpComponentFirmwareSlot;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpType;
use nexus_types::deployment::ExpectedArtifact;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateHostPhase1Details;
use omicron_common::disk::M2Slot;
use sled_agent_client::Client as SledAgentClient;
use slog::Logger;
use slog::debug;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::time::Duration;
use tokio::sync::watch;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactKind;
use uuid::Uuid;

// Hashing the current phase 1 contents on the SP is an asynchronous operation:
// we request a hash and then poll until the hashing completes. We have to pick
// some timeout to give up on polling. In practice we expect this hashing to
// take a few seconds, so set something very generous here that would indicate
// something very wrong.
const PHASE_1_HASHING_TIMEOUT: Duration = Duration::from_secs(60);

// To reset the host, we have to tell the SP to go to PowerState::A2 then back
// to PowerState::A0. We sleep briefly in between those transitions. (It's not
// clear to me whether we _need_ to do this; presumably the SP should enforce
// that if so? But this is copied from how `pilot sp cycle` is implemented.) We
// could also consider implementing a more idempotent "reset" operation on the
// SP side.
const POWER_CYCLE_SLEEP: Duration = Duration::from_secs(1);

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;

pub struct HostPhase1Updater {
    log: Logger,
    progress: watch::Sender<Option<UpdateProgress>>,
    sp_type: SpType,
    sp_slot: u16,
    target_host_slot: u16,
    update_id: Uuid,
    // TODO-clarity maybe a newtype for this? TBD how we get this from
    // wherever it's stored, which might give us a stronger type already.
    phase1_data: Vec<u8>,
}

impl HostPhase1Updater {
    pub fn new(
        sp_type: SpType,
        sp_slot: u16,
        target_host_slot: u16,
        update_id: Uuid,
        phase1_data: Vec<u8>,
        log: &Logger,
    ) -> Self {
        let log = log.new(slog::o!(
            "component" => "HostPhase1Updater",
            "sp_type" => format!("{sp_type:?}"),
            "sp_slot" => sp_slot,
            "target_host_slot" => target_host_slot,
            "update_id" => format!("{update_id}"),
        ));
        let progress = watch::Sender::new(None);
        Self {
            log,
            progress,
            sp_type,
            sp_slot,
            target_host_slot,
            update_id,
            phase1_data,
        }
    }

    pub fn progress_watcher(&self) -> watch::Receiver<Option<UpdateProgress>> {
        self.progress.subscribe()
    }

    /// Drive this host phase 1 update to completion (or failure).
    ///
    /// Only one MGS instance is required to drive an update; however, if
    /// multiple MGS instances are available and passed to this method and an
    /// error occurs communicating with one instance, `HostPhase1Updater` will
    /// try the remaining instances before failing.
    pub async fn update(
        mut self,
        mgs_clients: &mut MgsClients,
    ) -> Result<(), SpComponentUpdateError> {
        // The async block below wants a `&self` reference, but we take `self`
        // for API clarity (to start a new update, the caller should construct a
        // new instance of the updater). Create a `&self` ref that we use
        // through the remainder of this method.
        let me = &self;

        // Prior to delivering the update, ensure the correct target slot is
        // activated.
        //
        // TODO-correctness Should we be doing this, or should a higher level
        // executor set this up before calling us?
        mgs_clients
            .try_all_serially(&self.log, |client| async move {
                me.mark_target_slot_active(&client).await
            })
            .await?;

        // Deliver and drive the update to completion
        deliver_update(&mut self, mgs_clients).await?;

        // Unlike SP and RoT updates, we have nothing to do after delivery of
        // the update completes; signal to any watchers that we're done.
        self.progress.send_replace(Some(UpdateProgress::Complete));

        // wait for any progress watchers to be dropped before we return;
        // otherwise, they'll get `RecvError`s when trying to check the current
        // status
        self.progress.closed().await;

        Ok(())
    }

    async fn mark_target_slot_active(
        &self,
        client: &gateway_client::Client,
    ) -> Result<(), GatewayClientError> {
        // TODO-correctness Should we always persist this choice?
        let persist = true;

        let slot = self.firmware_slot();

        // TODO-correctness Until
        // https://github.com/oxidecomputer/hubris/issues/1172 is fixed, the
        // host must be in A2 for this operation to succeed. After it is fixed,
        // there will still be a window while a host is booting where this
        // operation can fail. How do we handle this?
        client
            .sp_component_active_slot_set(
                self.sp_type,
                self.sp_slot,
                self.component(),
                persist,
                &SpComponentFirmwareSlot { slot },
            )
            .await?;

        // TODO-correctness Should we send some kind of update to
        // `self.progress`? We haven't actually started delivering an update
        // yet, but it seems weird to give no indication that we have
        // successfully (potentially) modified the state of the target sled.

        info!(
            self.log, "host phase1 target slot marked active";
            "mgs_addr" => client.baseurl(),
        );

        Ok(())
    }
}

impl SpComponentUpdater for HostPhase1Updater {
    fn component(&self) -> &'static str {
        SpComponent::HOST_CPU_BOOT_FLASH.const_as_str()
    }

    fn target_sp_type(&self) -> SpType {
        self.sp_type
    }

    fn target_sp_slot(&self) -> u16 {
        self.sp_slot
    }

    fn firmware_slot(&self) -> u16 {
        self.target_host_slot
    }

    fn update_id(&self) -> Uuid {
        self.update_id
    }

    fn update_data(&self) -> Vec<u8> {
        self.phase1_data.clone()
    }

    fn progress(&self) -> &watch::Sender<Option<UpdateProgress>> {
        &self.progress
    }

    fn logger(&self) -> &Logger {
        &self.log
    }
}

pub struct ReconfiguratorHostPhase1Updater {
    details: PendingMgsUpdateHostPhase1Details,
}

impl ReconfiguratorHostPhase1Updater {
    pub fn new(details: PendingMgsUpdateHostPhase1Details) -> Self {
        Self { details }
    }

    async fn precheck_impl(
        &self,
        log: &slog::Logger,
        mgs_clients: &mut MgsClients,
        update: &PendingMgsUpdate,
    ) -> Result<PrecheckStatus, PrecheckError> {
        // Verify that the device is the one we think it is.
        let target_sp =
            SpIdentifier { type_: update.sp_type, slot: update.slot_id };
        let state = mgs_clients
            .try_all_serially(log, move |mgs_client| async move {
                mgs_client.sp_get(target_sp.type_, target_sp.slot).await
            })
            .await?
            .into_inner();
        debug!(log, "found SP state"; "state" => ?state);
        if state.model != update.baseboard_id.part_number
            || state.serial_number != update.baseboard_id.serial_number
        {
            return Err(PrecheckError::WrongDevice {
                sp_type: update.sp_type,
                slot_id: update.slot_id,
                expected_part: update.baseboard_id.part_number.clone(),
                expected_serial: update.baseboard_id.serial_number.clone(),
                found_part: state.model,
                found_serial: state.serial_number,
            });
        }

        // Verify expected phase 2 contents against sled-agent.
        self.precheck_phase_2(log).await?;

        // Fetch the active slot's current phase 1.
        let PendingMgsUpdateHostPhase1Details {
            expected_active_slot,
            expected_inactive_artifact,
            sled_agent_address: _,
        } = &self.details;
        let active_slot = expected_active_slot.slot.to_mgs_firmware_slot();
        let inactive_slot =
            expected_active_slot.slot.toggled().to_mgs_firmware_slot();
        let expected_active_artifact = expected_active_slot.phase_1;
        let expected_inactive_artifact = expected_inactive_artifact.phase_1;
        let found_active_artifact = self
            .precheck_fetch_phase_1(
                mgs_clients,
                target_sp,
                active_slot,
                expected_active_artifact,
                log,
            )
            .await?;
        debug!(
            log, "found active slot phase 1 artifact";
            "hash" => %found_active_artifact,
        );

        // If the version in the currently-active slot matches the one we're
        // trying to set, then there's nothing to do.
        if found_active_artifact == update.artifact_hash {
            return Ok(PrecheckStatus::UpdateComplete);
        }
        // Otherwise, if the version in the currently active slot does not
        // match what we expect to find, bail out.  It may be that somebody
        // else has come along and completed a subsequent update and we
        // don't want to roll that back.  (If for some reason we *do* want
        // to do this update, the planner will have to notice that what's
        // here is wrong and update the blueprint.)
        if found_active_artifact != expected_active_artifact {
            return Err(PrecheckError::WrongActiveArtifact {
                kind: ArtifactKind::HOST_PHASE_1,
                expected: expected_active_artifact,
                found: found_active_artifact,
            });
        }

        // For the same reason, check that the version in the inactive slot
        // matches what we expect to find.
        let found_inactive_artifact = self
            .precheck_fetch_phase_1(
                mgs_clients,
                target_sp,
                inactive_slot,
                expected_inactive_artifact,
                log,
            )
            .await?;
        debug!(
            log, "found inactive slot phase 1 artifact";
            "hash" => %found_inactive_artifact,
        );

        if found_inactive_artifact == expected_inactive_artifact {
            Ok(PrecheckStatus::ReadyForUpdate)
        } else {
            Err(PrecheckError::WrongInactiveArtifact {
                kind: ArtifactKind::HOST_PHASE_1,
                expected: ExpectedArtifact::Artifact(
                    expected_inactive_artifact,
                ),
                found: FoundArtifact::Artifact(found_inactive_artifact),
            })
        }
    }

    async fn precheck_fetch_phase_1(
        &self,
        mgs_clients: &mut MgsClients,
        target_sp: SpIdentifier,
        target_slot: u16,
        expected_artifact: ArtifactHash,
        log: &Logger,
    ) -> Result<ArtifactHash, PrecheckError> {
        match mgs_clients
            .try_all_serially(log, move |mgs_client| async move {
                match mgs_client
                    .host_phase_1_flash_hash_calculate_with_timeout(
                        target_sp,
                        target_slot,
                        PHASE_1_HASHING_TIMEOUT,
                    )
                    .await
                {
                    // The return types here are a little weird;
                    // `try_all_serially()` requires us to return a `Result<T,
                    // GatewayClientError>`, but
                    // `host_phase_1_flash_hash_calculate_with_timeout()`
                    // returns a `HostPhase1HashError`; its `RequestError`
                    // variant _contains_ a `GatewayClientError`. We convert
                    // that specific variant into its `GatewayClientError`, and
                    // return a `Result<Result<_, HostPhase1HashError>,
                    // GatewayClientError>. We unpack the inner result in a
                    // `match` after `try_all_serially()`.
                    Ok(hash) => Ok(Ok(hash)),
                    Err(HostPhase1HashError::RequestError { err, .. }) => {
                        Err(err)
                    }
                    Err(err) => Ok(Err(err)),
                }
            })
            .await?
        {
            Ok(hash) => Ok(ArtifactHash(hash)),
            Err(HostPhase1HashError::RequestError { err, .. }) => {
                Err(err.into())
            }
            Err(err) => Err(PrecheckError::DeterminingActiveArtifact {
                kind: ArtifactKind::HOST_PHASE_1,
                expected: expected_artifact,
                err: InlineErrorChain::new(&err).to_string(),
            }),
        }
    }

    async fn precheck_phase_2(
        &self,
        log: &Logger,
    ) -> Result<(), PrecheckError> {
        let PendingMgsUpdateHostPhase1Details {
            expected_active_slot,
            expected_inactive_artifact,
            sled_agent_address,
        } = &self.details;

        // Fetch the current inventory from sled-agent.
        let sled_agent = SledAgentClient::new(
            &format!("http://{sled_agent_address}"),
            log.clone(),
        );
        let sled_inventory = sled_agent
            .inventory()
            .await
            .map_err(|err| PrecheckError::SledAgentInventory {
                address: *sled_agent_address,
                err,
            })?
            .into_inner()
            .last_reconciliation
            .ok_or(PrecheckError::SledAgentInventoryMissingLastReconciliation)?
            .boot_partitions;
        debug!(
            log, "got phase 2 inventory details from sled-agent";
            "inventory" => ?sled_inventory,
        );

        // Confirm the expected active slot (i.e., the slot we booted from) and
        match (sled_inventory.boot_disk, expected_active_slot.slot) {
            (Ok(found), expected) if found == expected => (),
            (Ok(found), expected) => {
                return Err(PrecheckError::WrongActiveHostOsSlot {
                    expected,
                    found,
                });
            }
            (Err(err), expected) => {
                return Err(PrecheckError::DeterminingActiveHostOsSlot {
                    expected,
                    err,
                });
            }
        }

        // Confirm the two slots' phase 2 contents. If these don't match, we
        // can't proceed: either our update has become impossible due to other
        // changes (requires replanning), or we're waiting for sled-agent to
        // write the phase 2 we expect.
        let (active, inactive) = match expected_active_slot.slot {
            M2Slot::A => (sled_inventory.slot_a, sled_inventory.slot_b),
            M2Slot::B => (sled_inventory.slot_b, sled_inventory.slot_a),
        };
        let active = active.map(|s| s.artifact_hash);

        match (active, expected_active_slot.phase_2) {
            (Ok(found), expected) if found == expected => (),
            (Ok(found), expected) => {
                return Err(PrecheckError::WrongActiveArtifact {
                    kind: ArtifactKind::HOST_PHASE_2,
                    expected,
                    found,
                });
            }
            (Err(err), expected) => {
                return Err(PrecheckError::DeterminingActiveArtifact {
                    kind: ArtifactKind::HOST_PHASE_2,
                    expected,
                    err,
                });
            }
        }

        let found_inactive = match inactive {
            Ok(details) => FoundArtifact::Artifact(details.artifact_hash),
            // TODO-correctness There are many reasons sled-agent could report
            // an error in a phase 2 slot, including a couple cases where we
            // definitely want to convert the error to
            // `FoundArtifact::MissingArtifact`:
            //
            // 1. it couldn't parse the image header
            // 2. it parsed the image header, but the contents of the rest of
            //    the slot didn't match the image header's description (it
            //    contains a hash of the rest of the data in the slot)
            //
            // There are a variety of other errors possible, though, from
            // garden variety I/O errors to "there is no physical disk present
            // in this slot". Do we need to distinguish these from the
            // "indicative of a missing artifact" cases above? At the moment all
            // we get from inventory is a string...
            Err(_) => FoundArtifact::MissingArtifact,
        };
        found_inactive.matches(
            &expected_inactive_artifact.phase_2,
            ArtifactKind::HOST_PHASE_2,
        )
    }

    async fn post_update_impl(
        &self,
        log: &slog::Logger,
        mgs_clients: &mut MgsClients,
        update: &PendingMgsUpdate,
    ) -> Result<(), PostUpdateError> {
        debug!(log, "attempting to set active slot");
        let new_active_slot = self
            .details
            .expected_active_slot
            .slot
            .toggled()
            .to_mgs_firmware_slot();
        mgs_clients
            .try_all_serially(log, |mgs_client| async move {
                let persist = true;
                mgs_client
                    .sp_component_active_slot_set(
                        update.sp_type,
                        update.slot_id,
                        SpComponent::HOST_CPU_BOOT_FLASH.const_as_str(),
                        persist,
                        &SpComponentFirmwareSlot { slot: new_active_slot },
                    )
                    .await
            })
            .await?;

        debug!(log, "attempting to put sled in A2");
        mgs_clients
            .try_all_serially(log, |mgs_client| async move {
                mgs_client
                    .sp_power_state_set(
                        update.sp_type,
                        update.slot_id,
                        PowerState::A2,
                    )
                    .await
            })
            .await?;

        debug!(log, "sleeping briefly before powering sled back on");
        tokio::time::sleep(POWER_CYCLE_SLEEP).await;

        debug!(log, "attempting to put sled in A2");
        mgs_clients
            .try_all_serially(log, |mgs_client| async move {
                mgs_client
                    .sp_power_state_set(
                        update.sp_type,
                        update.slot_id,
                        PowerState::A0,
                    )
                    .await
            })
            .await?;

        Ok(())
    }
}

impl SpComponentUpdateHelper for ReconfiguratorHostPhase1Updater {
    fn precheck<'a>(
        &'a self,
        log: &'a slog::Logger,
        mgs_clients: &'a mut MgsClients,
        update: &'a PendingMgsUpdate,
    ) -> BoxFuture<'a, Result<PrecheckStatus, PrecheckError>> {
        self.precheck_impl(log, mgs_clients, update).boxed()
    }

    fn post_update<'a>(
        &'a self,
        log: &'a slog::Logger,
        mgs_clients: &'a mut MgsClients,
        update: &'a PendingMgsUpdate,
    ) -> BoxFuture<'a, Result<(), PostUpdateError>> {
        self.post_update_impl(log, mgs_clients, update).boxed()
    }
}
