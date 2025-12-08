// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating host OS phase1 images via MGS.
//!
//! OS images are divided into phase 1 and phase 2. Phase 1 is small (~32 MiB)
//! and is written to SPI flash by the SP. Updates to phase 1 therefore must be
//! delivered via MGS. Phase 2 is large and is written to a slice of an internal
//! disk. Updates to phase 2 are delivered via sled-agent.
//!
//! These images must be paired together; a phase 1 image contains a hash
//! of the expected phase 2 that completes it.
//!
//! There are two phase 1 slots (A and B, or 0 and 1), and there are two
//! internal disks (A and B). When a sled boots, the OS will ask the SP which
//! phase 1 slot is active, and it will look for the paired phase 2 image in the
//! corresponding A or B disk's boot slice. If the paired phase 2 is not present
//! on the matching disk, the OS will fail to boot.
//!
//! Reconfigurator-driven OS updates are complicated because they have to
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
//!    * Insert a `PendingMgsUpdate` for phase 1, which will be written to the
//!      actual slot by `ReconfiguratorHostPhase1Updater` in this module, with
//!      these values:
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
//!      active slot is still set to A, so if we reboot in this state everything
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
//!    longer matches the expected value Z1. Rebooting in this state still
//!    results in coming up on version Y.
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
//!      Rebooting in this state will result in the sled coming up on version X
//!      (i.e., a reboot now will finish the update).
//!
//!    * We want to restart the host at this point, but currently the SP doesn't
//!      expose a single "restart the host" operation. As a stopgap, we instead
//!      reset the _SP_, specifically for the side effect that this also
//!      restarts the host. While the host is booting, a precheck will fail with
//!      `SledAgentInventory`, because we won't be able to fetch inventory from
//!      sled-agent on a still-booting host. Once the OS finishes booting and
//!      sled-agent has started, the state of the sled will be:
//!
//!      - active phase 1 slot: B
//!      - boot disk: B
//!      - slot A contains Y1+Y2
//!      - slot B contains X1+X2
//!
//!      and the upgrade will be complete.

use super::MgsClients;
use crate::SpComponentUpdateHelperImpl;
use crate::common_sp_update::PostUpdateError;
use crate::common_sp_update::PrecheckError;
use crate::common_sp_update::PrecheckStatus;
use futures::FutureExt as _;
use futures::future::BoxFuture;
use gateway_client::HostPhase1HashError;
use gateway_client::SpComponent;
use gateway_client::types::SpComponentFirmwareSlot;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateHostPhase1Details;
use nexus_types::inventory::SpType;
use omicron_common::disk::M2Slot;
use sled_agent_client::Client as SledAgentClient;
use sled_agent_types_migrations::latest::inventory::BootPartitionContents;
use slog::Logger;
use slog::debug;
use slog_error_chain::InlineErrorChain;
use std::time::Duration;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactKind;

// Hashing the current phase 1 contents on the SP is an asynchronous operation:
// we request a hash and then poll until the hashing completes. We have to pick
// some timeout to give up on polling. In practice we expect this hashing to
// take a few seconds, so set something very generous here that would indicate
// something very wrong.
const PHASE_1_HASHING_TIMEOUT: Duration = Duration::from_secs(60);

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
                mgs_client.sp_get(&update.sp_type, update.slot_id).await
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
            expected_active_phase_1_slot,
            expected_active_phase_1_hash,
            expected_inactive_phase_1_hash,
            ..
        } = &self.details;

        // Fetch the current phase 1 active slot.
        let current_active_slot = {
            let slot = mgs_clients
                .try_all_serially(log, |mgs_client| async move {
                    mgs_client
                        .sp_component_active_slot_get(
                            &update.sp_type,
                            update.slot_id,
                            SpComponent::HOST_CPU_BOOT_FLASH.const_as_str(),
                        )
                        .await
                })
                .await?
                .slot;
            M2Slot::from_mgs_firmware_slot(slot)
                .ok_or(PrecheckError::InvalidHostPhase1Slot { slot })?
        };
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

        // If the artifact hash in the currently-active slot matches the one
        // we're trying to set, then the phase 1 update is complete. We need to
        // confirm that we've finished `post_update()` though (i.e., we've
        // rebooted the sled); contact sled agent and confirm it booted from
        // this same active slot.
        if current_active_slot_hash == update.artifact_hash {
            self.confirm_sled_agent_boot_disk_matches(current_active_slot, log)
                .await?;
            return Ok(PrecheckStatus::UpdateComplete);
        }

        // Otherwise, confirm the currently-active slot matches what we
        // expect...
        if current_active_slot != *expected_active_phase_1_slot {
            return Err(PrecheckError::WrongActiveHostPhase1Slot {
                expected: *expected_active_phase_1_slot,
                found: current_active_slot,
            });
        }

        // ... and that the artifact hash in the currently active slot matches
        // what we expect to find.  It may be that somebody else has come along
        // and completed a subsequent update and we don't want to roll that
        // back. (If for some reason we *do* want to do this update, the planner
        // will have to notice that what's here is wrong and update the
        // blueprint.)
        if current_active_slot_hash != *expected_active_phase_1_hash {
            return Err(PrecheckError::WrongActiveArtifact {
                kind: ArtifactKind::GIMLET_HOST_PHASE_1,
                expected: *expected_active_phase_1_hash,
                found: current_active_slot_hash,
            });
        }

        // For the same reason, check that the artifact hash in the inactive
        // slot matches what we expect to find.
        let expected_inactive_slot = expected_active_phase_1_slot.toggled();
        let found_inactive_artifact = self
            .precheck_fetch_phase_1(
                mgs_clients,
                update.sp_type,
                update.slot_id,
                expected_inactive_slot,
                log,
            )
            .await?;
        debug!(
            log, "found inactive slot phase 1 artifact";
            "hash" => %found_inactive_artifact,
        );

        if found_inactive_artifact == *expected_inactive_phase_1_hash {
            // Everything looks good as far as phase 1 preconditions are
            // concerned; also confirm that our phase 2 preconditions are
            // satisfied by contacting sled-agent.
            self.precheck_phase_2_via_sled_agent(log).await?;

            Ok(PrecheckStatus::ReadyForUpdate)
        } else {
            Err(PrecheckError::WrongInactiveArtifact {
                kind: ArtifactKind::GIMLET_HOST_PHASE_1,
                expected: *expected_inactive_phase_1_hash,
                found: found_inactive_artifact,
            })
        }
    }

    async fn precheck_fetch_phase_1(
        &self,
        mgs_clients: &mut MgsClients,
        sp_type: SpType,
        sp_slot: u16,
        target_slot: M2Slot,
        log: &Logger,
    ) -> Result<ArtifactHash, PrecheckError> {
        match mgs_clients
            .try_all_serially(log, move |mgs_client| async move {
                mgs_client
                    .host_phase_1_flash_hash_calculate_with_timeout(
                        sp_type,
                        sp_slot,
                        target_slot.to_mgs_firmware_slot(),
                        PHASE_1_HASHING_TIMEOUT,
                    )
                    .await
            })
            .await
        {
            Ok(hash) => Ok(ArtifactHash(hash)),
            Err(HostPhase1HashError::RequestError { err, .. }) => {
                Err(err.into())
            }
            Err(
                err @ (HostPhase1HashError::Timeout(_)
                | HostPhase1HashError::ContentsModifiedWhileHashing),
            ) => Err(PrecheckError::DeterminingActiveArtifact {
                kind: ArtifactKind::GIMLET_HOST_PHASE_1,
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
        active_phase1_slot: M2Slot,
        log: &Logger,
    ) -> Result<(), PrecheckError> {
        let sled_inventory =
            self.get_boot_partition_inventory_from_sled_agent(log).await?;

        match sled_inventory.boot_disk {
            Ok(slot) if slot == active_phase1_slot => Ok(()),
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
            expected_boot_disk,
            expected_active_phase_2_hash,
            expected_inactive_phase_2_hash,
            ..
        } = &self.details;

        // Fetch the current inventory from sled-agent.
        let sled_inventory =
            self.get_boot_partition_inventory_from_sled_agent(log).await?;

        // Confirm the expected boot disk.
        match (sled_inventory.boot_disk, *expected_boot_disk) {
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
        let (active, inactive) = match expected_boot_disk {
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
        if active != *expected_active_phase_2_hash {
            return Err(PrecheckError::WrongActiveArtifact {
                kind: ArtifactKind::HOST_PHASE_2,
                expected: *expected_active_phase_2_hash,
                found: active,
            });
        }

        let inactive = match inactive {
            Ok(details) => details.artifact_hash,
            Err(err) => {
                return Err(PrecheckError::DeterminingInactiveHostPhase2 {
                    err,
                });
            }
        };
        if inactive != *expected_inactive_phase_2_hash {
            return Err(PrecheckError::WrongInactiveArtifact {
                kind: ArtifactKind::HOST_PHASE_2,
                expected: *expected_inactive_phase_2_hash,
                found: inactive,
            });
        }

        // Both active and inactive match; we can proceed.
        Ok(())
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
            .expected_active_phase_1_slot
            .toggled()
            .to_mgs_firmware_slot();
        mgs_clients
            .try_all_serially(log, |mgs_client| async move {
                let persist = true;
                mgs_client
                    .sp_component_active_slot_set(
                        &update.sp_type,
                        update.slot_id,
                        SpComponent::HOST_CPU_BOOT_FLASH.const_as_str(),
                        persist,
                        &SpComponentFirmwareSlot { slot: new_active_slot },
                    )
                    .await
            })
            .await?;

        // We now want to reboot the host. Ideally we would send a "reset the
        // host" command here, but that's not currently provided by the SP
        // (included in https://github.com/oxidecomputer/hubris/issues/2178). We
        // could do what `pilot sp cycle` does and send the sled to A2, sleep
        // briefly, then send the sled to A0, but this causes problems if we're
        // trying to reset the sled on which we're currently running! We'll send
        // our own sled to A2, but then we won't be around to send us back to
        // A0, and there's currently no other mechanism where some other Nexus
        // could recover this and power our sled back on. As a stopgap, we'll
        // ask the _SP_ to reset: this is a single request and has the side
        // effect of also rebooting the host.
        debug!(log, "resetting sled (via SP reset)");
        mgs_clients
            .try_all_serially(log, |mgs_client| async move {
                mgs_client
                    .sp_component_reset(
                        &update.sp_type,
                        update.slot_id,
                        SpComponent::SP_ITSELF.const_as_str(),
                    )
                    .await
            })
            .await?;

        Ok(())
    }
}

impl SpComponentUpdateHelperImpl for ReconfiguratorHostPhase1Updater {
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
