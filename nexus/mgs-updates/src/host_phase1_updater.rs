// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating host OS phase1 images via MGS.
//!
//! Reconfigurator-driven OS updates are complicated because it has to
//! coordinate sled-agent writing the paired phase 2 _before_ we reboot the sled
//! with a new phase 1. To walk through a typical update, we'll describe three
//! OS versions:
//!
//! X1, X2 (phase 1 and phase2 of the version we want to upgrade to)
//! Y1, Y2 (current phase 1 and phase 2 of the active slot)
//! Z1, Z2 (current phase 1 and phase 2 of the inactive slot)
//!
//! We have two different meanings of "the active slot":
//!
//! 1. The active phase 1 slot as controlled by the SP; the value of this
//!    determines which slot will be used the next time the sled boots.
//! 2. The boot disk as reported by sled-agent; the value of this is a record of
//!    which slot was used the last time the sled booted.
//!
//! Assume without loss of generality that the current active slot (in both
//! meanings) is A. These are the steps we go through:
//!
//! 1. Initial sled state:
//!    - active phase 1 slot: A
//!    - boot disk: A
//!    - slot A contains Y1+Y2
//!    - slot B contains Z1+Z2
//!
//! 2. The planner decides to upgrade this sled. It generates a new blueprint
//!    with two changes:
//!
//!    * The sled config sets the desired phase 2 for slot B to X2 (i.e., we
//!      want to write the new phase 2 to the currently-inactive slot)
//!
//!    * Insert a `PendingMgsUpdate` for phase 1, written by
//!      `ReconfiguratorHostPhase1Updater` in this module, with these values:
//!      - expected phase 1 active slot: A
//!      - expected boot disk: A
//!      - expected active slot phase 1: Y1
//!      - expected active slot phase 2: Y2
//!      - expected inactive slot phase 1: Z1
//!      - expected inactive slot phase 2: X2
//!
//! 3. If we run our precheck now, we will return a
//!    `WrongInactiveArtifact(Phase2)`, because the inactive slot's phase 2 is
//!    currently Z2, but we don't begin the phase 1 update until it becomes X2.
//!
//! 4. sled-agent writes X2 to the inactive slot phase 2; the state of the sled
//!    is now:
//!
//!    - active phase 1 slot: A
//!    - boot disk: A
//!    - slot A contains Y1+Y2
//!    - slot B contains Z1+X2 (note: mismatched! this is expected, and our
//!      active slo tis still set to A, so if we reboot in this state everything
//!      is fine and we'll come up on version Y)
//!
//! 5. Our precheck will now return `ReadyForUpdate`, and we'll start writing
//!    X1 to the inactive slot phase 1. This behaves similarly to updating an
//!    SP: if we try to precheck while writing phase 1, we'll get a
//!    `WrongInactiveArtifact(Phase1)` (because we'll see a hash of a
//!    partially-written artifact).
//!
//! 6. Our phase 1 write completes; the state of the sled is now:
//!
//!    - active phase 1 slot: A
//!    - boot disk: A
//!    - slot A contains Y1+Y2
//!    - slot B contains X1+X2
//!
//!    A precheck at this point will return `WrongInactiveArtifact`, as X1 no
//!    longer matches the expected value Z1.
//!
//! 7. We run our post_update. This has three substeps; it's important to
//!    separate these out to ensure we handle takeovers correctly if the Nexus
//!    driving the update dies partway through post_update.
//!
//!    * We set the active phase 1 slot to B; the state of the sled is now:
//!
//!      - active phase 1 slot: B
//!      - boot disk: A
//!      - slot A contains Y1+Y2
//!      - slot B contains X1+X2
//!
//!      At this point, technically the phase 1 update alone is done, in the way
//!      that other MGS-driven updates are done: the currently-active slot (B)
//!      now contains the new version (X1). However, we must not return
//!      `UpdateComplete` from precheck yet, so that we can handle Nexus dying
//!      before we finish each of the next two steps. To handle this, once the
//!      phase 1 update looks complete, we also query the sled-agent to check
//!      that its boot disk matches the currently-active slot. A precheck at
//!      this point will fail with `MismatchedHostOsActiveSlot`, because the
//!      sled's boot disk (A) doesn't match the active phase 1 slot (B).
//!
//!    * We power off the host (i.e., go to PowerState::A2). This does not
//!      change the state of the sled, except that we can no longer talk to
//!      sled-agent and therefore no longer have a report of the boot disk.
//!
//!      A precheck at this point will fail with `SledAgentInventory`, because
//!      we won't be able to fetch inventory from sled-agent on a powered-off
//!      sled.
//!
//!    * We power on the host (i.e., go to PowerState::A0). Once it comes back,
//!      the state of the sled will be:
//!
//!      - active phase 1 slot: B
//!      - boot disk: B
//!      - slot A contains Y1+Y2
//!      - slot B contains X1+X2
//!
//!      and the upgrade will be complete.

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
use gateway_client::types::SpType;
use nexus_sled_agent_shared::inventory::BootPartitionContents;
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
        let state = mgs_clients
            .try_all_serially(log, move |mgs_client| async move {
                mgs_client.sp_get(update.sp_type, update.slot_id).await
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

        let PendingMgsUpdateHostPhase1Details {
            expected_active_slot,
            expected_inactive_artifact,
            sled_agent_address: _,
        } = &self.details;

        // Fetch the current phase 1 active slot.
        let current_active_slot = mgs_clients
            .try_all_serially(log, |mgs_client| async move {
                mgs_client
                    .sp_component_active_slot_get(
                        update.sp_type,
                        update.slot_id,
                        SpComponent::HOST_CPU_BOOT_FLASH.const_as_str(),
                    )
                    .await
            })
            .await?
            .slot;
        debug!(
            log, "found currently-active phase 1 slot";
            "slot" => %current_active_slot,
        );

        // Fetch the hash of the current phase 1 active slot.
        let current_active_slot_hash = self
            .precheck_fetch_phase_1(
                mgs_clients,
                update.sp_type,
                update.slot_id,
                current_active_slot,
                log,
            )
            .await?;
        debug!(
            log, "found currently-active phase 1 artifact";
            "hash" => %current_active_slot_hash,
        );

        // If the version in the currently-active slot matches the one we're
        // trying to set, the the phase 1 update is complete. We need to confirm
        // that we've finished `post_update()` though (i.e., we've rebooted the
        // sled); contact sled agent and confirm it booted from this same active
        // slot.
        if current_active_slot_hash == update.artifact_hash {
            self.confirm_sled_agent_boot_disk_matches(current_active_slot, log)
                .await?;
            return Ok(PrecheckStatus::UpdateComplete);
        }

        // Otherwise, confirm the currently-active slot matches what we
        // expect...
        {
            let expected_active_mgs_slot =
                expected_active_slot.phase_1_slot.to_mgs_firmware_slot();
            if current_active_slot != expected_active_mgs_slot {
                return Err(PrecheckError::WrongActiveHostPhase1Slot {
                    expected: expected_active_mgs_slot,
                    found: current_active_slot,
                });
            }
        }

        // ... and that the version in the currently active slot matches what we
        // expect to find.  It may be that somebody else has come along and
        // completed a subsequent update and we don't want to roll that back.
        // (If for some reason we *do* want to do this update, the planner will
        // have to notice that what's here is wrong and update the blueprint.)
        if current_active_slot_hash != expected_active_slot.phase_1 {
            return Err(PrecheckError::WrongActiveArtifact {
                kind: ArtifactKind::HOST_PHASE_1,
                expected: expected_active_slot.phase_1,
                found: current_active_slot_hash,
            });
        }

        // For the same reason, check that the version in the inactive slot
        // matches what we expect to find.
        let expected_inactive_slot =
            expected_active_slot.phase_1_slot.toggled();
        let found_inactive_artifact = self
            .precheck_fetch_phase_1(
                mgs_clients,
                update.sp_type,
                update.slot_id,
                expected_inactive_slot.to_mgs_firmware_slot(),
                log,
            )
            .await?;
        debug!(
            log, "found inactive slot phase 1 artifact";
            "hash" => %found_inactive_artifact,
        );

        if found_inactive_artifact == expected_inactive_artifact.phase_1 {
            // Everything looks good as far as phase 1 preconditions are
            // concerned; also confirm that our phase 2 preconditions are
            // satisfied by contacting sled-agent.
            self.precheck_phase_2_via_sled_agent(log).await?;

            Ok(PrecheckStatus::ReadyForUpdate)
        } else {
            Err(PrecheckError::WrongInactiveArtifact {
                kind: ArtifactKind::HOST_PHASE_1,
                expected: ExpectedArtifact::Artifact(
                    expected_inactive_artifact.phase_1,
                ),
                found: FoundArtifact::Artifact(found_inactive_artifact),
            })
        }
    }

    async fn precheck_fetch_phase_1(
        &self,
        mgs_clients: &mut MgsClients,
        sp_type: SpType,
        sp_slot: u16,
        target_slot: u16,
        log: &Logger,
    ) -> Result<ArtifactHash, PrecheckError> {
        match mgs_clients
            .try_all_serially(log, move |mgs_client| async move {
                match mgs_client
                    .host_phase_1_flash_hash_calculate_with_timeout(
                        sp_type,
                        sp_slot,
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
                err: InlineErrorChain::new(&err).to_string(),
            }),
        }
    }

    async fn get_boot_partition_inventory_from_sled_agent(
        &self,
        log: &Logger,
    ) -> Result<BootPartitionContents, PrecheckError> {
        let address = self.details.sled_agent_address;
        let sled_agent =
            SledAgentClient::new(&format!("http://{address}"), log.clone());
        let sled_inventory = sled_agent
            .inventory()
            .await
            .map_err(|err| PrecheckError::SledAgentInventory { address, err })?
            .into_inner()
            .last_reconciliation
            .ok_or(PrecheckError::SledAgentInventoryMissingLastReconciliation)?
            .boot_partitions;
        debug!(
            log, "got phase 2 inventory details from sled-agent";
            "inventory" => ?sled_inventory,
        );
        Ok(sled_inventory)
    }

    async fn confirm_sled_agent_boot_disk_matches(
        &self,
        active_phase1_slot: u16,
        log: &Logger,
    ) -> Result<(), PrecheckError> {
        let sled_inventory =
            self.get_boot_partition_inventory_from_sled_agent(log).await?;

        match sled_inventory.boot_disk {
            Ok(slot) if slot.to_mgs_firmware_slot() == active_phase1_slot => {
                Ok(())
            }
            Ok(slot) => Err(PrecheckError::MismatchedHostOsActiveSlot {
                phase1: active_phase1_slot,
                boot_disk: slot,
            }),
            Err(err) => Err(PrecheckError::DeterminingHostOsBootDisk { err }),
        }
    }

    async fn precheck_phase_2_via_sled_agent(
        &self,
        log: &Logger,
    ) -> Result<(), PrecheckError> {
        let PendingMgsUpdateHostPhase1Details {
            expected_active_slot,
            expected_inactive_artifact,
            ..
        } = &self.details;

        // Fetch the current inventory from sled-agent.
        let sled_inventory =
            self.get_boot_partition_inventory_from_sled_agent(log).await?;

        // Confirm the expected boot disk.
        match (sled_inventory.boot_disk, expected_active_slot.boot_disk) {
            (Ok(found), expected) if found == expected => (),
            (Ok(found), expected) => {
                return Err(PrecheckError::WrongHostOsBootDisk {
                    expected,
                    found,
                });
            }
            (Err(err), _) => {
                return Err(PrecheckError::DeterminingHostOsBootDisk { err });
            }
        }

        // Confirm the two slots' phase 2 contents. If these don't match, we
        // can't proceed: either our update has become impossible due to other
        // changes (requires replanning), or we're waiting for sled-agent to
        // write the phase 2 we expect.
        let (active, inactive) = match expected_active_slot.boot_disk {
            M2Slot::A => (sled_inventory.slot_a, sled_inventory.slot_b),
            M2Slot::B => (sled_inventory.slot_b, sled_inventory.slot_a),
        };
        let active = match active {
            Ok(details) => details.artifact_hash,
            Err(err) => {
                return Err(PrecheckError::DeterminingActiveArtifact {
                    kind: ArtifactKind::HOST_PHASE_2,
                    err,
                });
            }
        };
        if active != expected_active_slot.phase_2 {
            return Err(PrecheckError::WrongActiveArtifact {
                kind: ArtifactKind::HOST_PHASE_2,
                expected: expected_active_slot.phase_2,
                found: active,
            });
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
            .phase_1_slot
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
