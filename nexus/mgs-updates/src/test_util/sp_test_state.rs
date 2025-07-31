// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::common_sp_update::error_means_caboose_is_invalid;
use gateway_client::HostPhase1HashError;
use gateway_client::SpComponent;
use gateway_client::types::GetRotBootInfoParams;
use gateway_client::types::RotState;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpState;
use gateway_client::types::SpType;
use gateway_messages::RotBootInfo;
use gateway_types::rot::RotSlot;
use nexus_types::deployment::ExpectedActiveRotSlot;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::inventory::BaseboardId;
use omicron_common::disk::M2Slot;
use slog_error_chain::InlineErrorChain;
use std::time::Duration;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactVersion;

pub type GatewayClientError =
    gateway_client::Error<gateway_client::types::Error>;

/// Combines all the state we'd like to fetch from the SP to verify its behavior
/// across an update
#[derive(Debug, Eq, PartialEq)]
pub struct SpTestState {
    /// caboose read from the SP active slot
    ///
    /// This is not an `Option` because we never expect to fail to read this.
    pub caboose_sp_active: SpComponentCaboose,

    /// caboose read from the SP inactive slot
    ///
    /// This can be None if the caboose contents were not valid.
    pub caboose_sp_inactive: Option<SpComponentCaboose>,

    /// caboose read from RoT slot A
    ///
    /// This can be None if the caboose contents were not valid.
    pub caboose_rot_a: Option<SpComponentCaboose>,

    /// caboose read from RoT slot B
    ///
    /// This can be None if the caboose contents were not valid.
    pub caboose_rot_b: Option<SpComponentCaboose>,

    /// caboose read from stage 0 (active slot for the RoT bootloader)
    ///
    /// This is not an `Option` because we never expect to fail to read this.
    pub caboose_stage0: SpComponentCaboose,

    /// caboose read from stage 0 next (inactive slot for the RoT bootloader)
    ///
    /// This can be None if the caboose contents were not valid.
    pub caboose_stage0_next: Option<SpComponentCaboose>,

    /// Overall SP state
    pub sp_state: SpState,

    /// RoT boot information
    pub sp_boot_info: RotState,

    /// Host phase 1 information; these are optional because they only exist for
    /// `SpType::Sled` SPs.
    pub host_phase_1_active_slot: Option<M2Slot>,
    pub host_phase_1_slot_a_hash: Option<ArtifactHash>,
    pub host_phase_1_slot_b_hash: Option<ArtifactHash>,
}

impl SpTestState {
    /// Load all the state we care about from the given SP
    pub async fn load(
        mgs_client: &gateway_client::Client,
        sp_type: SpType,
        sp_slot: u16,
    ) -> Result<SpTestState, GatewayClientError> {
        let caboose_sp_active = mgs_client
            .sp_component_caboose_get(
                sp_type,
                sp_slot,
                SpComponent::SP_ITSELF.const_as_str(),
                0,
            )
            .await?
            .into_inner();
        let caboose_sp_inactive = mgs_client
            .sp_component_caboose_get(
                sp_type,
                sp_slot,
                SpComponent::SP_ITSELF.const_as_str(),
                1,
            )
            .await
            .map(|c| c.into_inner());
        let caboose_rot_a = mgs_client
            .sp_component_caboose_get(
                sp_type,
                sp_slot,
                SpComponent::ROT.const_as_str(),
                0,
            )
            .await
            .map(|c| c.into_inner());
        let caboose_rot_b = mgs_client
            .sp_component_caboose_get(
                sp_type,
                sp_slot,
                SpComponent::ROT.const_as_str(),
                1,
            )
            .await
            .map(|c| c.into_inner());
        let caboose_stage0 = mgs_client
            .sp_component_caboose_get(
                sp_type,
                sp_slot,
                SpComponent::STAGE0.const_as_str(),
                0,
            )
            .await?
            .into_inner();
        let caboose_stage0_next = mgs_client
            .sp_component_caboose_get(
                sp_type,
                sp_slot,
                SpComponent::STAGE0.const_as_str(),
                1,
            )
            .await
            .map(|c| c.into_inner());
        let sp_info = mgs_client.sp_get(sp_type, sp_slot).await?.into_inner();
        let sp_boot_info = mgs_client
            .sp_rot_boot_info(
                sp_type,
                sp_slot,
                SpComponent::ROT.const_as_str(),
                &GetRotBootInfoParams {
                    version: RotBootInfo::HIGHEST_KNOWN_VERSION,
                },
            )
            .await?
            .into_inner();

        // Read these values only for sleds.
        let host_phase_1_active_slot;
        let host_phase_1_slot_a_hash;
        let host_phase_1_slot_b_hash;
        if matches!(sp_type, SpType::Sled) {
            host_phase_1_active_slot = Some(
                M2Slot::from_mgs_firmware_slot(
                    mgs_client
                        .sp_component_active_slot_get(
                            sp_type,
                            sp_slot,
                            SpComponent::HOST_CPU_BOOT_FLASH.const_as_str(),
                        )
                        .await?
                        .into_inner()
                        .slot,
                )
                .expect("simulated SP always returns valid slot"),
            );
            host_phase_1_slot_a_hash = Some(
                match mgs_client
                    .host_phase_1_flash_hash_calculate_with_timeout(
                        sp_type,
                        sp_slot,
                        0,
                        Duration::from_secs(30),
                    )
                    .await
                {
                    Ok(hash) => ArtifactHash(hash),
                    Err(HostPhase1HashError::RequestError { err, .. }) => {
                        return Err(err);
                    }
                    Err(err) => {
                        panic!("unexpected error from simulator: {err}")
                    }
                },
            );
            host_phase_1_slot_b_hash = Some(
                match mgs_client
                    .host_phase_1_flash_hash_calculate_with_timeout(
                        sp_type,
                        sp_slot,
                        1,
                        Duration::from_secs(30),
                    )
                    .await
                {
                    Ok(hash) => ArtifactHash(hash),
                    Err(HostPhase1HashError::RequestError { err, .. }) => {
                        return Err(err);
                    }
                    Err(err) => {
                        panic!("unexpected error from simulator: {err}")
                    }
                },
            );
        } else {
            host_phase_1_active_slot = None;
            host_phase_1_slot_a_hash = None;
            host_phase_1_slot_b_hash = None;
        }

        Ok(SpTestState {
            caboose_sp_active,
            caboose_sp_inactive: ignore_invalid_caboose_error(
                caboose_sp_inactive,
            ),
            caboose_rot_a: ignore_invalid_caboose_error(caboose_rot_a),
            caboose_rot_b: ignore_invalid_caboose_error(caboose_rot_b),
            caboose_stage0,
            caboose_stage0_next: ignore_invalid_caboose_error(
                caboose_stage0_next,
            ),
            sp_state: sp_info,
            sp_boot_info,
            host_phase_1_active_slot,
            host_phase_1_slot_a_hash,
            host_phase_1_slot_b_hash,
        })
    }

    pub fn expect_caboose_sp_active(&self) -> &SpComponentCaboose {
        &self.caboose_sp_active
    }

    pub fn expect_caboose_sp_inactive(&self) -> &SpComponentCaboose {
        self.caboose_sp_inactive.as_ref().expect("inactive SP caboose")
    }

    pub fn expect_caboose_rot_a(&self) -> &SpComponentCaboose {
        self.caboose_rot_a.as_ref().expect("ROT slot A caboose")
    }

    pub fn expect_caboose_rot_b(&self) -> &SpComponentCaboose {
        self.caboose_rot_b.as_ref().expect("ROT slot B caboose")
    }

    pub fn expect_caboose_rot_active(&self) -> &SpComponentCaboose {
        match self.expect_rot_active_slot() {
            RotSlot::A => self.expect_caboose_rot_a(),
            RotSlot::B => self.expect_caboose_rot_b(),
        }
    }

    pub fn expect_caboose_stage0(&self) -> &SpComponentCaboose {
        &self.caboose_stage0
    }

    pub fn expect_caboose_stage0_next(&self) -> &SpComponentCaboose {
        self.caboose_stage0_next.as_ref().expect("stage0 next caboose")
    }

    pub fn expect_caboose_rot_inactive(&self) -> &SpComponentCaboose {
        let slot = self.expect_rot_active_slot().toggled();
        match slot {
            RotSlot::A => self.expect_caboose_rot_a(),
            RotSlot::B => self.expect_caboose_rot_b(),
        }
    }

    pub fn baseboard_id(&self) -> BaseboardId {
        BaseboardId {
            part_number: self.sp_state.model.clone(),
            serial_number: self.sp_state.serial_number.clone(),
        }
    }

    pub fn expect_sp_active_version(&self) -> ArtifactVersion {
        self.expect_caboose_sp_active()
            .version
            .parse()
            .expect("valid artifact version")
    }

    pub fn expect_sp_inactive_version(&self) -> ExpectedVersion {
        match &self.caboose_sp_inactive {
            Some(v) => ExpectedVersion::Version(
                v.version.parse().expect("valid SP inactive slot version"),
            ),
            None => ExpectedVersion::NoValidVersion,
        }
    }

    pub fn expect_rot_state(&self) -> RotState {
        self.sp_boot_info.clone()
    }

    pub fn expect_rot_active_slot(&self) -> RotSlot {
        match self.expect_rot_state() {
            RotState::V2 { active, .. } | RotState::V3 { active, .. } => active,
            RotState::CommunicationFailed { .. } => panic!("ROT active slot"),
        }
    }

    pub fn expect_rot_persistent_boot_preference(&self) -> RotSlot {
        match self.expect_rot_state() {
            RotState::V2 { persistent_boot_preference, .. }
            | RotState::V3 { persistent_boot_preference, .. } => {
                persistent_boot_preference
            }
            RotState::CommunicationFailed { .. } => {
                panic!("ROT persistent boot preference")
            }
        }
    }

    pub fn expect_rot_pending_persistent_boot_preference(
        &self,
    ) -> Option<RotSlot> {
        match self.expect_rot_state() {
            RotState::V2 { pending_persistent_boot_preference, .. }
            | RotState::V3 { pending_persistent_boot_preference, .. } => {
                pending_persistent_boot_preference
            }
            RotState::CommunicationFailed { .. } => {
                panic!("ROT pending persistent boot preference")
            }
        }
    }

    pub fn expect_rot_transient_boot_preference(&self) -> Option<RotSlot> {
        match self.expect_rot_state() {
            RotState::V2 { transient_boot_preference, .. }
            | RotState::V3 { transient_boot_preference, .. } => {
                transient_boot_preference
            }
            RotState::CommunicationFailed { .. } => {
                panic!("ROT pending persistent boot preference")
            }
        }
    }

    pub fn expected_active_rot_slot(&self) -> ExpectedActiveRotSlot {
        let slot = self.expect_rot_active_slot();
        let version = match slot {
            RotSlot::A => self
                .expect_caboose_rot_a()
                .version
                .parse()
                .expect("valid artifact version"),
            RotSlot::B => self
                .expect_caboose_rot_b()
                .version
                .parse()
                .expect("valid artifact version"),
        };
        ExpectedActiveRotSlot { slot, version }
    }

    pub fn expect_rot_inactive_version(&self) -> ExpectedVersion {
        let slot = self.expect_rot_active_slot().toggled();
        match slot {
            RotSlot::A => match &self.caboose_rot_a {
                Some(v) => ExpectedVersion::Version(
                    v.version.parse().expect("valid SP inactive slot version"),
                ),
                None => ExpectedVersion::NoValidVersion,
            },
            RotSlot::B => match &self.caboose_rot_b {
                Some(v) => ExpectedVersion::Version(
                    v.version.parse().expect("valid SP inactive slot version"),
                ),
                None => ExpectedVersion::NoValidVersion,
            },
        }
    }

    pub fn expect_stage0_version(&self) -> ArtifactVersion {
        self.expect_caboose_stage0()
            .version
            .parse()
            .expect("valid artifact version")
    }

    pub fn expect_stage0_next_version(&self) -> ExpectedVersion {
        match &self.caboose_stage0_next {
            Some(v) => ExpectedVersion::Version(
                v.version.parse().expect("valid stage0 next version"),
            ),
            None => ExpectedVersion::NoValidVersion,
        }
    }

    pub fn expect_host_phase_1_active_slot(&self) -> M2Slot {
        self.host_phase_1_active_slot
            .expect("should be called only for sled SPs")
    }

    pub fn expect_host_phase_1_active_hash(&self) -> ArtifactHash {
        let (active, _inactive) = self.expect_host_phase_1_hashes();
        active
    }

    pub fn expect_host_phase_1_inactive_hash(&self) -> ArtifactHash {
        let (_active, inactive) = self.expect_host_phase_1_hashes();
        inactive
    }

    // Returns (active, inactive); helper to avoid needing to unpack the
    // active/inactive slot in both of this method's callers.
    fn expect_host_phase_1_hashes(&self) -> (ArtifactHash, ArtifactHash) {
        let a = self
            .host_phase_1_slot_a_hash
            .expect("should be called only for sled SPs");
        let b = self
            .host_phase_1_slot_b_hash
            .expect("should be called only for sled SPs");
        match self.expect_host_phase_1_active_slot() {
            M2Slot::A => (a, b),
            M2Slot::B => (b, a),
        }
    }
}

fn ignore_invalid_caboose_error(
    result: Result<SpComponentCaboose, GatewayClientError>,
) -> Option<SpComponentCaboose> {
    match result {
        Ok(caboose) => Some(caboose),
        Err(error) if error_means_caboose_is_invalid(&error) => None,
        Err(error) => {
            panic!(
                "unexpected error reading caboose: {}",
                InlineErrorChain::new(&error)
            );
        }
    }
}
