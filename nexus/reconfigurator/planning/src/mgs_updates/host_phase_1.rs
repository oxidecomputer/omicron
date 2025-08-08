// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Facilities for making choices about MGS-managed host phase 1 updates

use super::MgsUpdateStatus;
use super::MgsUpdateStatusError;
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::deployment::BlueprintHostPhase2DesiredContents;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use nexus_types::deployment::PendingMgsUpdateHostPhase1Details;
use nexus_types::inventory::BaseboardId;
use nexus_types::inventory::Collection;
use omicron_common::api::external::TufArtifactMeta;
use omicron_common::api::external::TufRepoDescription;
use omicron_common::disk::M2Slot;
use omicron_uuid_kinds::SledUuid;
use slog::Logger;
use slog::error;
use slog::warn;
use std::collections::BTreeMap;
use std::sync::Arc;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactKind;

#[derive(Debug, Default)]
pub(crate) struct PendingHostPhase2Changes {
    by_sled: BTreeMap<SledUuid, (M2Slot, BlueprintHostPhase2DesiredContents)>,
}

impl PendingHostPhase2Changes {
    fn insert(
        &mut self,
        sled_id: SledUuid,
        slot: M2Slot,
        artifact: &TufArtifactMeta,
    ) {
        let contents = BlueprintHostPhase2DesiredContents::Artifact {
            version: BlueprintArtifactVersion::Available {
                version: artifact.id.version.clone(),
            },
            hash: artifact.hash,
        };
        let previous = self.by_sled.insert(sled_id, (slot, contents));
        assert!(
            previous.is_none(),
            "recorded multiple changes for sled {sled_id}"
        );
    }

    pub(super) fn append(&mut self, other: &mut Self) {
        let expected_count = self.by_sled.len() + other.by_sled.len();
        self.by_sled.append(&mut other.by_sled);
        assert_eq!(
            self.by_sled.len(),
            expected_count,
            "appended PendingHostPhase2Changes with duplicate sled IDs"
        );
    }

    pub(crate) fn into_iter(
        self,
    ) -> impl Iterator<Item = (SledUuid, M2Slot, BlueprintHostPhase2DesiredContents)>
    {
        self.by_sled
            .into_iter()
            .map(|(sled_id, (slot, contents))| (sled_id, slot, contents))
    }
}

pub(super) fn update_status(
    baseboard_id: &Arc<BaseboardId>,
    desired_artifact: ArtifactHash,
    inventory: &Collection,
    details: &PendingMgsUpdateHostPhase1Details,
    log: &Logger,
) -> Result<MgsUpdateStatus, MgsUpdateStatusError> {
    let active_phase_1_slot = inventory
        .host_phase_1_active_slot_for(baseboard_id)
        .ok_or_else(|| MgsUpdateStatusError::MissingHostPhase1ActiveSlot)?
        .slot;

    let active_phase_1_hash = inventory
        .host_phase_1_flash_hash_for(active_phase_1_slot, baseboard_id)
        .ok_or_else(|| {
            MgsUpdateStatusError::MissingHostPhase1FlashHash(
                active_phase_1_slot,
            )
        })?
        .hash;

    // Get the latest inventory report from sled-agent; we need this to confirm
    // that it's actually booted the OS image we're trying to update to. If it's
    // not present in inventory at all, we'll assume it's in the process of
    // rebooting.
    let Some(sled_agent) = inventory.sled_agents.iter().find(|sled_agent| {
        sled_agent.baseboard_id.as_ref() == Some(baseboard_id)
    }) else {
        return Ok(MgsUpdateStatus::NotDone);
    };

    let last_reconciliation =
        sled_agent.last_reconciliation.as_ref().ok_or_else(|| {
            MgsUpdateStatusError::MissingSledAgentLastReconciliation
        })?;
    let boot_disk = *last_reconciliation
        .boot_partitions
        .boot_disk
        .as_ref()
        .map_err(|err| {
            MgsUpdateStatusError::SledAgentErrorDeterminingBootDisk(err.clone())
        })?;

    // If we find the desired artifact in the active slot _and_ we see that
    // sled-agent has successfully booted from that same slot, we're done.
    if active_phase_1_hash == desired_artifact
        && boot_disk == active_phase_1_slot
    {
        return Ok(MgsUpdateStatus::Done);
    }

    // The update hasn't completed. We need to compare the inventory contents
    // (for both the SP and sled-agent) against the expectations we recorded in
    // `details` to check whether the update is still in progress or has become
    // impossible.
    let PendingMgsUpdateHostPhase1Details {
        expected_active_phase_1_slot,
        expected_boot_disk,
        expected_active_phase_1_hash,
        expected_active_phase_2_hash,
        expected_inactive_phase_1_hash,
        expected_inactive_phase_2_hash,
        sled_agent_address,
    } = details;

    // It should be impossible for the sled-agent address to change, unless this
    // sled isn't the one we think it is.
    if sled_agent.sled_agent_address != *sled_agent_address {
        error!(
            log,
            "sled-agent with in-progress MGS-driven update has moved";
            "inventory_sled_agent_address" => %sled_agent.sled_agent_address,
        );
        return Ok(MgsUpdateStatus::Impossible);
    }

    // If the active slot or its contents do not match what we expect, we've
    // changed the active slot _without_ completing the update. It's impossible
    // to proceed.
    if active_phase_1_slot != *expected_active_phase_1_slot
        || active_phase_1_hash != *expected_active_phase_1_hash
    {
        return Ok(MgsUpdateStatus::Impossible);
    }

    // Similarly, if the boot disk or active phase 2 hash does not match what we
    // expect, we've changed boot disks _without_ completing the update.
    if boot_disk != *expected_boot_disk {
        return Ok(MgsUpdateStatus::Impossible);
    }
    let active_phase_2_hash = last_reconciliation
        .boot_partitions
        .slot_details(boot_disk)
        .as_ref()
        .map_err(|err| {
            MgsUpdateStatusError::SledAgentErrorDeterminingBootPartitionDetails {
                slot: boot_disk,
                err: err.clone(),
            }
        })?
        .artifact_hash;
    if active_phase_2_hash != *expected_active_phase_2_hash {
        return Ok(MgsUpdateStatus::Impossible);
    }

    // Checking the expected inactive phase 2 hash is a little weird. When we
    // plan this update, we set this field to the new OS phase 2 (i.e., the one
    // that is paired with `desired_artifact`), because we need sled-agent to
    // write that part of the image before we start trying to update the phase
    // 1. Therefore, if the actual inactive phase 2 hash doesn't match the
    // expected one, the update is `NotDone`, because we're still waiting on
    // sled-agent to write it.
    let inactive_phase_2_hash = last_reconciliation
        .boot_partitions
        .slot_details(boot_disk.toggled())
        .as_ref()
        .map_err(|err| {
            MgsUpdateStatusError::SledAgentErrorDeterminingBootPartitionDetails {
                slot: boot_disk.toggled(),
                err: err.clone(),
            }
        })?
        .artifact_hash;
    if inactive_phase_2_hash != *expected_inactive_phase_2_hash {
        return Ok(MgsUpdateStatus::NotDone);
    }

    // If the inactive phase 1 hash doesn't match what we expect, we won't be
    // able to pass our preconditions. This one is tricky because we could be in
    // the process of writing to the inactive phase 1 slot, which could cause
    // inventory to fail to collect the hash entirely (fine - we'll return an
    // error and wait to make a decision until inventory didn't fail) or give us
    // a hash that matches a partially-written artifact (which could mean the
    // update is `NotDone` we're actively writing it). However, we have to treat
    // the latter case as `Impossible`:
    //
    // 1. It's possible we partially wrote the inactive slot and then the MGS
    //    instance sending that update died
    // 2. We can't tell whether we have a hash of a partially-written
    //    `desired_artifact` or a hash of some completely unrelated thing.
    //
    // Returning `Impossible` could cause some unnecessary churn in planning
    // steps, but it should eventually converge.
    let inactive_phase_1_hash = inventory
        .host_phase_1_flash_hash_for(
            active_phase_1_slot.toggled(),
            baseboard_id,
        )
        .ok_or_else(|| {
            MgsUpdateStatusError::MissingHostPhase1FlashHash(
                active_phase_1_slot.toggled(),
            )
        })?
        .hash;
    if inactive_phase_1_hash == *expected_inactive_phase_1_hash {
        Ok(MgsUpdateStatus::NotDone)
    } else {
        Ok(MgsUpdateStatus::Impossible)
    }
}

pub(super) fn try_make_update(
    log: &slog::Logger,
    baseboard_id: &Arc<BaseboardId>,
    inventory: &Collection,
    current_artifacts: &TufRepoDescription,
) -> Option<(PendingMgsUpdate, PendingHostPhase2Changes)> {
    let Some(sp_info) = inventory.sps.get(baseboard_id) else {
        warn!(
            log,
            "cannot configure host OS update for board \
             (missing SP info from inventory)";
            baseboard_id,
        );
        return None;
    };
    let Some(sled_agent) = inventory.sled_agents.iter().find(|sled_agent| {
        sled_agent.baseboard_id.as_ref() == Some(baseboard_id)
    }) else {
        warn!(
            log,
            "cannot configure host OS update for board \
             (missing sled-agent info from inventory)";
            baseboard_id,
        );
        return None;
    };
    let Some(last_reconciliation) = sled_agent.last_reconciliation.as_ref()
    else {
        warn!(
            log,
            "cannot configure host OS update for board \
             (missing last reconciliation details from inventory)";
            baseboard_id,
        );
        return None;
    };
    let boot_disk = match &last_reconciliation.boot_partitions.boot_disk {
        Ok(boot_disk) => *boot_disk,
        Err(err) => {
            // This error is a `String`; we can't use `InlineErrorChain`.
            let err: &str = &err;
            warn!(
                log,
                "cannot configure host OS update for board \
                 (sled-agent reported an error determining boot disk)";
                baseboard_id,
                "err" => err,
            );
            return None;
        }
    };
    let active_phase_2_hash =
        match &last_reconciliation.boot_partitions.slot_details(boot_disk) {
            Ok(details) => details.artifact_hash,
            Err(err) => {
                // This error is a `String`; we can't use `InlineErrorChain`.
                let err: &str = &err;
                warn!(
                    log,
                    "cannot configure host OS update for board \
                     (sled-agent reported an error boot disk phase 2 image)";
                    baseboard_id,
                    "boot_disk" => ?boot_disk,
                    "err" => err,
                );
                return None;
            }
        };

    let Some(active_phase_1_slot) =
        inventory.host_phase_1_active_slot_for(baseboard_id).map(|s| s.slot)
    else {
        warn!(
            log,
            "cannot configure host OS update for board \
             (inventory missing current active host phase 1 slot)";
            baseboard_id,
        );
        return None;
    };

    // TODO-correctness What should we do if the active phase 1 slot doesn't
    // match the boot disk? That means the active phase 1 slot has been changed
    // since the last time the sled booted, which should only happen at the very
    // end of a host OS update just before the sled is rebooted. It's possible
    // (albeit unlikely) we collected inventory in that window; we don't want to
    // plan a new update for this sled if it's about to reboot into some other
    // update.
    //
    // If there are other ways we could get a mismatch between the active phase
    // 1 slot and the boot disk, they'll induce a support case to recover, given
    // this current implementation. As far as we know they shouldn't happen.
    if active_phase_1_slot != boot_disk {
        warn!(
            log,
            "cannot configure host OS update for board (active phase 1 slot \
             doesn't match boot disk; is the sled already being updated?)";
            baseboard_id,
            "active_phase_1_slot" => ?active_phase_1_slot,
            "boot_disk" => ?boot_disk,
        );
        return None;
    }

    let Some(active_phase_1_hash) = inventory
        .host_phase_1_flash_hash_for(active_phase_1_slot, baseboard_id)
        .map(|h| h.hash)
    else {
        warn!(
            log,
            "cannot configure host OS update for board \
             (missing active phase 1 hash from inventory)";
            baseboard_id,
            "slot" => ?active_phase_1_slot,
        );
        return None;
    };

    let Some(inactive_phase_1_hash) = inventory
        .host_phase_1_flash_hash_for(
            active_phase_1_slot.toggled(),
            baseboard_id,
        )
        .map(|h| h.hash)
    else {
        warn!(
            log,
            "cannot configure host OS update for board \
             (missing inactive phase 1 hash from inventory)";
            baseboard_id,
            "slot" => ?active_phase_1_slot.toggled(),
        );
        return None;
    };

    let mut phase_1_artifacts = Vec::with_capacity(1);
    let mut phase_2_artifacts = Vec::with_capacity(1);
    for artifact in &current_artifacts.artifacts {
        // TODO Need to choose gimlet vs cosmo here! Need help from tufaceous to
        // tell us which is which.
        if artifact.id.kind == ArtifactKind::HOST_PHASE_1 {
            phase_1_artifacts.push(artifact);
        } else if artifact.id.kind == ArtifactKind::HOST_PHASE_2 {
            phase_2_artifacts.push(artifact);
        }
    }
    let (phase_1_artifact, phase_2_artifact) =
        match (phase_1_artifacts.as_slice(), phase_2_artifacts.as_slice()) {
            // Common case: Exactly 1 of each artifact.
            ([p1], [p2]) => (p1, p2),
            // "TUF is broken" cases: missing one or the other.
            ([], _) => {
                warn!(
                    log,
                    "cannot configure host OS update for board \
                     (no phase 1 artifact)";
                    baseboard_id,
                );
                return None;
            }
            (_, []) => {
                warn!(
                    log,
                    "cannot configure host OS update for board \
                     (no phase 1 artifact)";
                    baseboard_id,
                );
                return None;
            }
            // "TUF is broken" cases: have multiple of one or the other. This
            // should be impossible unless we shipped a TUF repo with multiple
            // host OS images. We can't proceed, because we don't know how to
            // pair up which phase 1 matches which phase 2.
            (_, _) => {
                warn!(
                    log,
                    "cannot configure host OS update for board \
                     (multiple OS images in TUF repo)";
                    baseboard_id,
                    "num-phase-1-images" => phase_1_artifacts.len(),
                    "num-phase-2-images" => phase_2_artifacts.len(),
                );
                return None;
            }
        };

    // Before we can proceed with the phase 1 update, we need sled-agent to
    // write the corresponding phase 2 artifact to its inactive disk. This
    // requires us updating its `OmicronSledConfig`. We don't thread the
    // blueprint editor all the way down to this point, so instead we'll return
    // the set of host phase 2 changes we want the planner to make on our
    // behalf.
    let mut pending_host_phase_2_changes = PendingHostPhase2Changes::default();
    pending_host_phase_2_changes.insert(
        sled_agent.sled_id,
        boot_disk.toggled(),
        phase_2_artifact,
    );

    Some((
        PendingMgsUpdate {
            baseboard_id: baseboard_id.clone(),
            sp_type: sp_info.sp_type,
            slot_id: sp_info.sp_slot,
            details: PendingMgsUpdateDetails::HostPhase1(
                PendingMgsUpdateHostPhase1Details {
                    expected_active_phase_1_slot: active_phase_1_slot,
                    expected_boot_disk: boot_disk,
                    expected_active_phase_1_hash: active_phase_1_hash,
                    expected_active_phase_2_hash: active_phase_2_hash,
                    expected_inactive_phase_1_hash: inactive_phase_1_hash,
                    expected_inactive_phase_2_hash: phase_2_artifact.hash,
                    sled_agent_address: sled_agent.sled_agent_address,
                },
            ),
            artifact_hash: phase_1_artifact.hash,
            artifact_version: phase_1_artifact.id.version.clone(),
        },
        pending_host_phase_2_changes,
    ))
}
