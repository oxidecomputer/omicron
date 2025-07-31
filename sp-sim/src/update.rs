// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::mem;
use std::time::Duration;
use std::time::Instant;

use crate::SIM_GIMLET_BOARD;
use crate::SIM_ROT_BOARD;
use crate::SIM_ROT_STAGE0_BOARD;
use crate::SIM_SIDECAR_BOARD;
use crate::helpers::rot_slot_id_from_u16;
use crate::helpers::rot_slot_id_to_u16;
use gateway_messages::Fwid;
use gateway_messages::HfError;
use gateway_messages::RotSlotId;
use gateway_messages::RotStateV3;
use gateway_messages::SpComponent;
use gateway_messages::SpError;
use gateway_messages::UpdateChunk;
use gateway_messages::UpdateId;
use gateway_messages::UpdateInProgressStatus;
use hubtools::RawHubrisImage;
use sha2::Sha256;
use sha3::Digest;
use sha3::Sha3_256;
use tokio::sync::mpsc;

pub(crate) struct SimSpUpdate {
    /// tracks the state of any ongoing simulated update
    ///
    /// Both the hardware and this simulator enforce that only one update to any
    /// SP-managed component can be in flight at a time.
    state: UpdateState,

    /// data from the last completed SP update (exposed for testing)
    last_sp_update_data: Option<Box<[u8]>>,
    /// data from the last completed RoT update (exposed for testing)
    last_rot_update_data: Option<Box<[u8]>>,
    /// data from the last completed phase1 update for each slot (exposed for
    /// testing)
    last_host_phase1_update_data: BTreeMap<u16, Box<[u8]>>,
    /// state of hashing each of the host phase1 slots
    phase1_hash_state: BTreeMap<u16, HostFlashHashState>,
    /// how do we decide when we're done hashing host phase1 slots? this allows
    /// us to default to "instant" (e.g., for running sp-sim as a part of
    /// `omicron-dev`) while giving tests that want explicit control the ability
    /// to precisely trigger completion of hashing.
    phase1_hash_policy: HostFlashHashPolicyInner,

    /// records whether a change to the stage0 "active slot" has been requested
    pending_stage0_update: bool,

    /// current caboose for the SP active slot
    caboose_sp_active: CabooseValue,
    /// current caboose for the SP inactive slot
    caboose_sp_inactive: CabooseValue,
    /// current caboose for the RoT slot A
    caboose_rot_a: CabooseValue,
    /// current caboose for the RoT slot B
    caboose_rot_b: CabooseValue,
    /// current caboose for stage0
    caboose_stage0: CabooseValue,
    /// current caboose for stage0next
    caboose_stage0next: CabooseValue,
    /// current RoT boot and version information
    rot_state: RotStateV3,
}

impl SimSpUpdate {
    pub(crate) fn new(
        baseboard_kind: BaseboardKind,
        no_stage0_caboose: bool,
        phase1_hash_policy: HostFlashHashPolicy,
        sp_board_name: Option<String>,
    ) -> Self {
        const SP_GITC0: &str = "ffffffff";
        const SP_GITC1: &str = "fefefefe";
        const SP_VERS0: &str = "0.0.2";
        const SP_VERS1: &str = "0.0.1";

        const ROT_GITC0: &str = "eeeeeeee";
        const ROT_GITC1: &str = "edededed";
        const ROT_VERS0: &str = "0.0.4";
        const ROT_VERS1: &str = "0.0.3";
        // staging/devel key signature
        const ROT_STAGING_DEVEL_SIGN: &str =
            "11594bb5548a757e918e6fe056e2ad9e084297c9555417a025d8788eacf55daf";

        const STAGE0_GITC0: &str = "ddddddddd";
        const STAGE0_GITC1: &str = "dadadadad";
        const STAGE0_VERS0: &str = "0.0.200";
        const STAGE0_VERS1: &str = "0.0.200";

        // TODO-K: This is where the boards are set?
        let sp_board = if let Some(b) = sp_board_name  {
            b
        } else {baseboard_kind.sp_board().to_string()};
        let sp_name = baseboard_kind.sp_name();
        let rot_name = baseboard_kind.rot_name();

        let caboose_sp_active = CabooseValue::Caboose(
            hubtools::CabooseBuilder::default()
                .git_commit(SP_GITC0)
                .board(&sp_board)
                .name(sp_name)
                .version(SP_VERS0)
                .build(),
        );
        let caboose_sp_inactive = CabooseValue::Caboose(
            hubtools::CabooseBuilder::default()
                .git_commit(SP_GITC1)
                .board(&sp_board)
                .name(sp_name)
                .version(SP_VERS1)
                .build(),
        );

        let caboose_rot_a = CabooseValue::Caboose(
            hubtools::CabooseBuilder::default()
                .git_commit(ROT_GITC0)
                .board(SIM_ROT_BOARD)
                .name(rot_name)
                .version(ROT_VERS0)
                .sign(ROT_STAGING_DEVEL_SIGN)
                .build(),
        );

        let caboose_rot_b = CabooseValue::Caboose(
            hubtools::CabooseBuilder::default()
                .git_commit(ROT_GITC1)
                .board(SIM_ROT_BOARD)
                .name(rot_name)
                .version(ROT_VERS1)
                .sign(ROT_STAGING_DEVEL_SIGN)
                .build(),
        );

        let (caboose_stage0, caboose_stage0next) = if no_stage0_caboose {
            (
                CabooseValue::InvalidMissingAllKeys,
                CabooseValue::InvalidMissingAllKeys,
            )
        } else {
            (
                CabooseValue::Caboose(
                    hubtools::CabooseBuilder::default()
                        .git_commit(STAGE0_GITC0)
                        .board(SIM_ROT_STAGE0_BOARD)
                        .name(rot_name)
                        .version(STAGE0_VERS0)
                        .sign(ROT_STAGING_DEVEL_SIGN)
                        .build(),
                ),
                CabooseValue::Caboose(
                    hubtools::CabooseBuilder::default()
                        .git_commit(STAGE0_GITC1)
                        .board(SIM_ROT_STAGE0_BOARD)
                        .name(rot_name)
                        .version(STAGE0_VERS1)
                        .sign(ROT_STAGING_DEVEL_SIGN)
                        .build(),
                ),
            )
        };

        const SLOT_A_DIGEST: [u8; 32] = [0xaa; 32];
        const SLOT_B_DIGEST: [u8; 32] = [0xbb; 32];
        const STAGE0_DIGEST: [u8; 32] = [0xcc; 32];
        const STAGE0NEXT_DIGEST: [u8; 32] = [0xdd; 32];

        let rot_state = RotStateV3 {
            active: RotSlotId::A,
            persistent_boot_preference: RotSlotId::A,
            pending_persistent_boot_preference: None,
            transient_boot_preference: None,
            slot_a_fwid: gateway_messages::Fwid::Sha3_256(SLOT_A_DIGEST),
            slot_b_fwid: gateway_messages::Fwid::Sha3_256(SLOT_B_DIGEST),
            stage0_fwid: gateway_messages::Fwid::Sha3_256(STAGE0_DIGEST),
            stage0next_fwid: gateway_messages::Fwid::Sha3_256(
                STAGE0NEXT_DIGEST,
            ),
            slot_a_status: Ok(()),
            slot_b_status: Ok(()),
            stage0_status: Ok(()),
            stage0next_status: Ok(()),
        };

        Self {
            state: UpdateState::NotPrepared,
            last_sp_update_data: None,
            last_rot_update_data: None,
            last_host_phase1_update_data: BTreeMap::new(),
            phase1_hash_state: BTreeMap::new(),
            phase1_hash_policy: phase1_hash_policy.0,

            pending_stage0_update: false,

            caboose_sp_active,
            caboose_sp_inactive,
            caboose_rot_a,
            caboose_rot_b,
            caboose_stage0,
            caboose_stage0next,

            rot_state,
        }
    }

    pub(crate) fn set_phase1_hash_policy(
        &mut self,
        policy: HostFlashHashPolicy,
    ) {
        self.phase1_hash_policy = policy.0;
    }

    pub(crate) fn sp_update_prepare(
        &mut self,
        id: UpdateId,
        total_size: usize,
    ) -> Result<(), SpError> {
        // SP updates always target slot 0.
        let slot = 0;
        self.component_update_prepare(
            SpComponent::SP_ITSELF,
            id,
            total_size,
            slot,
        )
    }

    pub(crate) fn component_update_prepare(
        &mut self,
        component: SpComponent,
        id: UpdateId,
        total_size: usize,
        slot: u16,
    ) -> Result<(), SpError> {
        match &self.state {
            state @ UpdateState::Prepared { .. } => {
                Err(SpError::UpdateInProgress(state.to_message()))
            }
            state @ UpdateState::Completed {
                component: update_component,
                ..
            } if component == SpComponent::SP_ITSELF
                && component == *update_component =>
            {
                // The SP will not allow you to start another update of itself
                // while one is completed.
                Err(SpError::UpdateInProgress(state.to_message()))
            }
            UpdateState::NotPrepared
            | UpdateState::Aborted(_)
            | UpdateState::Completed { .. } => {
                self.state = UpdateState::Prepared {
                    component,
                    id,
                    slot,
                    data: Cursor::new(vec![0u8; total_size].into_boxed_slice()),
                };

                // When the SP or RoT begins preparing an update, we can start
                // seeing various errors when we try to read the caboose for the
                // slot we're updating.  Empirically, during the SP update we
                // tend to see InvalidMissing.  During the stage0next update, we
                // see InvalidFailedRead.  It'd be nice to get these exactly
                // right, but it's most important that consumers handle both
                // cases, which they will if they test updates to both of these
                // components.
                if let Some(caboose) = self.caboose_mut(component, slot) {
                    *caboose = if component == SpComponent::STAGE0 {
                        CabooseValue::InvalidFailedRead
                    } else {
                        CabooseValue::InvalidMissing
                    }
                }

                Ok(())
            }
        }
    }

    fn caboose_mut(
        &mut self,
        component: SpComponent,
        slot: u16,
    ) -> Option<&mut CabooseValue> {
        match (component, slot) {
            // SP always updates slot 0.
            (SpComponent::SP_ITSELF, 0) => Some(&mut self.caboose_sp_inactive),
            (SpComponent::ROT, 0) => Some(&mut self.caboose_rot_a),
            (SpComponent::ROT, 1) => Some(&mut self.caboose_rot_b),
            // RoT bootloader always updates slot 1.
            (SpComponent::STAGE0, 1) => Some(&mut self.caboose_stage0next),
            _ => None,
        }
    }

    pub(crate) fn status(&self) -> gateway_messages::UpdateStatus {
        self.state.to_message()
    }

    pub(crate) fn ingest_chunk(
        &mut self,
        chunk: UpdateChunk,
        chunk_data: &[u8],
    ) -> Result<(), SpError> {
        match &mut self.state {
            UpdateState::Prepared { component, id, slot, data } => {
                // Ensure that the update ID and target component are correct.
                let id = *id;
                if chunk.id != id || chunk.component != *component {
                    return Err(SpError::InvalidUpdateId { sp_update_id: id });
                };
                if data.position() != u64::from(chunk.offset) {
                    return Err(SpError::UpdateInProgress(
                        self.state.to_message(),
                    ));
                }

                // We're writing to an in-memory buffer; the only failure
                // possible is if there isn't enough space left (i.e., the
                // update is larger than what the preparation we received
                // claimed it would be).
                std::io::Write::write_all(data, chunk_data)
                    .map_err(|_| SpError::UpdateIsTooLarge)?;

                // If we're writing to the host flash, invalidate the cached
                // hash of this slot.
                if *component == SpComponent::HOST_CPU_BOOT_FLASH {
                    self.phase1_hash_state
                        .insert(*slot, HostFlashHashState::HashInvalidated);
                }

                if data.position() == data.get_ref().len() as u64 {
                    let mut stolen = Cursor::new(Box::default());
                    mem::swap(data, &mut stolen);
                    let data = stolen.into_inner();

                    if *component == SpComponent::HOST_CPU_BOOT_FLASH {
                        self.last_host_phase1_update_data
                            .insert(*slot, data.clone());
                    }

                    let component = *component;
                    let slot = *slot;
                    if let Some(caboose_value) =
                        self.caboose_mut(component, slot)
                    {
                        let caboose = RawHubrisImage::from_binary(
                            data.clone().to_vec(),
                            0,
                            0,
                        )
                        .and_then(|image| image.read_caboose());
                        match caboose {
                            Ok(caboose) => {
                                *caboose_value = CabooseValue::Caboose(caboose);
                            }
                            Err(_) => {
                                *caboose_value = CabooseValue::InvalidMissing;
                            }
                        };
                    }

                    self.state =
                        UpdateState::Completed { component, id, slot, data };
                }

                Ok(())
            }
            UpdateState::NotPrepared
            | UpdateState::Aborted(_)
            | UpdateState::Completed { .. } => Err(SpError::UpdateNotPrepared),
        }
    }

    pub(crate) fn abort(&mut self, update_id: UpdateId) -> Result<(), SpError> {
        match &self.state {
            UpdateState::NotPrepared => Err(SpError::UpdateNotPrepared),
            UpdateState::Prepared { id, .. } => {
                if *id == update_id {
                    self.state = UpdateState::Aborted(update_id);
                    Ok(())
                } else {
                    Err(SpError::UpdateInProgress(self.status()))
                }
            }
            UpdateState::Aborted(_) => Ok(()),
            UpdateState::Completed { .. } => {
                Err(SpError::UpdateInProgress(self.status()))
            }
        }
    }

    pub(crate) fn sp_reset(&mut self) {
        // Clear any update, whatever state it was in.
        let prev_state =
            std::mem::replace(&mut self.state, UpdateState::NotPrepared);

        // Check if an SP update completed since the last reset.
        if let UpdateState::Completed { data, component, .. } = prev_state {
            if component == SpComponent::SP_ITSELF {
                // Save the data that we received so that we can expose it
                // for tests.
                self.last_sp_update_data = Some(data.clone());

                // Swap the cabooses to simulate the real SP behavior of
                // swapping which slot is active after an update.
                std::mem::swap(
                    &mut self.caboose_sp_active,
                    &mut self.caboose_sp_inactive,
                );
            }
        }
    }

    pub(crate) fn rot_reset(&mut self) {
        // Clear any update, whatever state it was in.
        let prev_state =
            std::mem::replace(&mut self.state, UpdateState::NotPrepared);

        // Apply changes from a successfully-completed update.
        if let UpdateState::Completed { data, component, slot, .. } = prev_state
        {
            if component == SpComponent::ROT {
                // Compute a new FWID for the affected slot.
                // unwrap(): we've already validated this.
                let slot = rot_slot_id_from_u16(slot).unwrap();
                let fwidp = match slot {
                    RotSlotId::A => &mut self.rot_state.slot_a_fwid,
                    RotSlotId::B => &mut self.rot_state.slot_b_fwid,
                };
                *fwidp = fake_fwid_compute(&data);

                // For an RoT update, save the data that we received so that
                // we can expose it for tests.
                self.last_rot_update_data = Some(data);
            } else if component == SpComponent::STAGE0 {
                // Similarly, for a bootloader update, compute a new FWID
                // for stage0next.
                let fwid = fake_fwid_compute(&data);
                self.rot_state.stage0next_fwid = fwid;
            }
        }

        // If there was a pending persistent boot preference set, apply that now
        // (and unset it).
        if let Some(new_boot_pref) =
            self.rot_state.pending_persistent_boot_preference.take()
        {
            self.rot_state.persistent_boot_preference = new_boot_pref;
            self.rot_state.active = new_boot_pref;
        }

        // We do not currently simulate changes to the transient boot
        // preference.
        self.rot_state.transient_boot_preference = None;

        // If there was a pending stage0 update (i.e., a change to the stage0
        // "active slot"), apply that now.  All this means is changing the
        // stage0 FWID and caboose to match the stage0next FWID and caboose.
        if self.pending_stage0_update {
            self.rot_state.stage0_fwid = self.rot_state.stage0next_fwid;
            self.caboose_stage0 = self.caboose_stage0next.clone();
        }
    }

    pub(crate) fn last_sp_update_data(&self) -> Option<Box<[u8]>> {
        self.last_sp_update_data.clone()
    }

    pub(crate) fn last_rot_update_data(&self) -> Option<Box<[u8]>> {
        self.last_rot_update_data.clone()
    }

    pub(crate) fn last_host_phase1_update_data(
        &self,
        slot: u16,
    ) -> Option<Box<[u8]>> {
        self.last_host_phase1_update_data.get(&slot).cloned()
    }

    pub(crate) fn start_host_flash_hash(
        &mut self,
        slot: u16,
    ) -> Result<(), SpError> {
        self.check_host_flash_state_and_policy(slot);
        match self
            .phase1_hash_state
            .get_mut(&slot)
            .expect("check_host_flash_state_and_policy always inserts")
        {
            // Already hashed; this is a no-op.
            HostFlashHashState::Hashed(_) => Ok(()),
            // No current hash; record our start time.
            state @ (HostFlashHashState::NeverHashed
            | HostFlashHashState::HashInvalidated) => {
                *state = HostFlashHashState::HashStarted(Instant::now());
                Ok(())
            }
            // Still hashing; this is an error.
            HostFlashHashState::HashStarted(_) => {
                Err(SpError::Hf(HfError::HashInProgress))
            }
        }
    }

    pub(crate) fn get_host_flash_hash(
        &mut self,
        slot: u16,
    ) -> Result<[u8; 32], SpError> {
        self.check_host_flash_state_and_policy(slot);
        match self
            .phase1_hash_state
            .get_mut(&slot)
            .expect("check_host_flash_state_and_policy always inserts")
        {
            HostFlashHashState::Hashed(hash) => Ok(*hash),
            HostFlashHashState::NeverHashed => {
                Err(SpError::Hf(HfError::HashUncalculated))
            }
            HostFlashHashState::HashStarted(_) => {
                Err(SpError::Hf(HfError::HashInProgress))
            }
            HostFlashHashState::HashInvalidated => {
                Err(SpError::Hf(HfError::RecalculateHash))
            }
        }
    }

    fn check_host_flash_state_and_policy(&mut self, slot: u16) {
        let state = self
            .phase1_hash_state
            .entry(slot)
            .or_insert(HostFlashHashState::NeverHashed);

        // If we've already hashed this slot, we're done.
        if matches!(state, HostFlashHashState::Hashed(_)) {
            return;
        }

        // Should we hash the flash now? It depends on our state + policy.
        let should_hash = match (&mut self.phase1_hash_policy, state) {
            // If we want to always assume contents are hashed, compute that
            // hash _unless_ the contents have changed (in which case a client
            // would need to send us an explicit "start hashing" request).
            (
                HostFlashHashPolicyInner::AssumeAlreadyHashed,
                HostFlashHashState::HashInvalidated,
            ) => false,
            (HostFlashHashPolicyInner::AssumeAlreadyHashed, _) => true,
            // If we're timer based, only hash if the timer has elapsed.
            (
                HostFlashHashPolicyInner::Timer(timeout),
                HostFlashHashState::HashStarted(started),
            ) => *timeout >= started.elapsed(),
            (HostFlashHashPolicyInner::Timer(_), _) => false,
            // If we're channel based, only hash if we've gotten a request to
            // start hashing and there's a message in the channel.
            (
                HostFlashHashPolicyInner::Channel(rx),
                HostFlashHashState::HashStarted(_),
            ) => rx.try_recv().is_ok(),
            (HostFlashHashPolicyInner::Channel(_), _) => false,
        };

        if should_hash {
            let data = self.last_host_phase1_update_data(slot);
            let data = data.as_deref().unwrap_or(&[]);
            let hash = Sha256::digest(&data).into();
            self.phase1_hash_state
                .insert(slot, HostFlashHashState::Hashed(hash));
        }
    }

    pub(crate) fn get_component_caboose_value(
        &mut self,
        component: SpComponent,
        slot: u16,
        key: [u8; 4],
        buf: &mut [u8],
    ) -> Result<usize, SpError> {
        let which_caboose = match (component, slot) {
            (SpComponent::SP_ITSELF, 0) => &self.caboose_sp_active,
            (SpComponent::SP_ITSELF, 1) => &self.caboose_sp_inactive,
            (SpComponent::ROT, 0) => &self.caboose_rot_a,
            (SpComponent::ROT, 1) => &self.caboose_rot_b,
            (SpComponent::STAGE0, 0) => &self.caboose_stage0,
            (SpComponent::STAGE0, 1) => &self.caboose_stage0next,
            _ => {
                return Err(SpError::NoSuchCabooseKey(key));
            }
        };

        which_caboose.value(key, buf)
    }

    pub(crate) fn rot_state(&self) -> RotStateV3 {
        self.rot_state
    }

    pub(crate) fn component_set_active_slot(
        &mut self,
        component: SpComponent,
        slot: u16,
        persist: bool,
    ) -> Result<(), SpError> {
        match component {
            SpComponent::ROT => {
                if persist {
                    self.rot_state.pending_persistent_boot_preference =
                        Some(rot_slot_id_from_u16(slot)?);
                } else {
                    self.rot_state.transient_boot_preference =
                        Some(rot_slot_id_from_u16(slot)?);
                }
                Ok(())
            }
            SpComponent::STAGE0 => {
                if slot == 1 {
                    self.pending_stage0_update = true;
                    return Ok(());
                } else {
                    Err(SpError::RequestUnsupportedForComponent)
                }
            }
            SpComponent::HOST_CPU_BOOT_FLASH => Ok(()),
            _ => {
                // The real SP returns `RequestUnsupportedForComponent` for
                // anything other than the RoT and host boot flash, including
                // SP_ITSELF.
                Err(SpError::RequestUnsupportedForComponent)
            }
        }
    }

    pub(crate) fn component_get_active_slot(
        &mut self,
        component: SpComponent,
    ) -> Result<u16, SpError> {
        match component {
            // The only active component for SP is slot 0.
            SpComponent::SP_ITSELF => Ok(0),
            SpComponent::ROT => Ok(rot_slot_id_to_u16(
                self.rot_state.persistent_boot_preference,
            )),
            // The only active component is stage0
            SpComponent::STAGE0 => Ok(0),
            _ => Err(SpError::RequestUnsupportedForComponent),
        }
    }
}

/// Specifies what kind of device we're constructing caboose metadata for
pub enum BaseboardKind {
    Gimlet,
    Sidecar,
}

impl BaseboardKind {
    // TODO-K: This is where the boards are set?
    fn sp_board(&self) -> &str {
        match self {
            BaseboardKind::Gimlet => &SIM_GIMLET_BOARD,
            BaseboardKind::Sidecar => &SIM_SIDECAR_BOARD,
        }
    }

    fn sp_name(&self) -> &str {
        match self {
            BaseboardKind::Gimlet => "SimGimlet",
            BaseboardKind::Sidecar => "SimSidecar",
        }
    }

    fn rot_name(&self) -> &str {
        match self {
            BaseboardKind::Gimlet => "SimGimletRot",
            BaseboardKind::Sidecar => "SimSidecarRot",
        }
    }
}

/// Represents a simulated caboose
#[derive(Clone)]
enum CabooseValue {
    /// emulate an actual caboose
    Caboose(hubtools::Caboose),
    /// emulate "the image does not include a caboose"
    InvalidMissing,
    /// emulate "the image caboose does not contain 'KEY'"
    InvalidMissingAllKeys,
    /// emulate "failed to read data from the caboose" (erased)
    InvalidFailedRead,
}

impl CabooseValue {
    fn value(&self, key: [u8; 4], buf: &mut [u8]) -> Result<usize, SpError> {
        match self {
            CabooseValue::Caboose(caboose) => {
                let value = match &key {
                    b"GITC" => caboose.git_commit(),
                    b"BORD" => caboose.board(),
                    b"NAME" => caboose.name(),
                    b"VERS" => caboose.version(),
                    b"SIGN" => caboose.sign(),
                    _ => return Err(SpError::NoSuchCabooseKey(key)),
                };

                match value {
                    Ok(value) => {
                        buf[..value.len()].copy_from_slice(value);
                        Ok(value.len())
                    }
                    Err(hubtools::CabooseError::MissingTag { .. }) => {
                        Err(SpError::NoSuchCabooseKey(key))
                    }
                    Err(hubtools::CabooseError::TlvcReadError(_)) => {
                        Err(SpError::CabooseReadError)
                    }
                }
            }
            CabooseValue::InvalidMissing => Err(SpError::NoCaboose),
            CabooseValue::InvalidMissingAllKeys => {
                Err(SpError::NoSuchCabooseKey(key))
            }
            CabooseValue::InvalidFailedRead => Err(SpError::CabooseReadError),
        }
    }
}

enum UpdateState {
    NotPrepared,
    Prepared {
        component: SpComponent,
        id: UpdateId,
        slot: u16,
        // data would ordinarily be a Cursor<Vec<u8>>, but that can grow and
        // reallocate. We want to ensure that we don't receive any more data
        // than originally promised, so use a Cursor<Box<[u8]>> to ensure that
        // it never grows.
        data: Cursor<Box<[u8]>>,
    },
    Aborted(UpdateId),
    Completed {
        component: SpComponent,
        id: UpdateId,
        slot: u16,
        data: Box<[u8]>,
    },
}

impl UpdateState {
    fn to_message(&self) -> gateway_messages::UpdateStatus {
        use gateway_messages::UpdateStatus;

        match self {
            Self::NotPrepared => UpdateStatus::None,
            Self::Prepared { id, data, .. } => {
                // If all the data has written, mark it as completed.
                let bytes_received = data.position() as u32;
                let total_size = data.get_ref().len() as u32;
                if bytes_received == total_size {
                    UpdateStatus::Complete(*id)
                } else {
                    UpdateStatus::InProgress(UpdateInProgressStatus {
                        id: *id,
                        bytes_received,
                        total_size,
                    })
                }
            }
            Self::Aborted(id) => UpdateStatus::Aborted(*id),
            Self::Completed { id, .. } => UpdateStatus::Complete(*id),
        }
    }
}

/// Computes a fake FWID for the Hubris image contained in `data`
fn fake_fwid_compute(data: &[u8]) -> Fwid {
    // This is NOT the way real FWIDs are computed.  For our purposes, all we
    // really care about is that the value changes when the image changes, and
    // maybe that the value doesn't change when the image doesn't change.
    let mut digest = Sha3_256::default();
    digest.update(data);
    Fwid::Sha3_256(digest.finalize().into())
}

#[derive(Debug, Clone, Copy)]
enum HostFlashHashState {
    Hashed([u8; 32]),
    NeverHashed,
    HashStarted(Instant),
    HashInvalidated,
}

/// Policy controlling how `sp-sim` behaves when asked to flash its host phase 1
/// contents.
#[derive(Debug)]
pub struct HostFlashHashPolicy(HostFlashHashPolicyInner);

impl HostFlashHashPolicy {
    /// Always return computed hashes when asked.
    ///
    /// This emulates an SP that has previously computed its phase 1 flash hash
    /// and whose contents haven't changed. Most Nexus tests should use this
    /// policy by default to allow inventory collections to complete promptly.
    pub fn assume_already_hashed() -> Self {
        Self(HostFlashHashPolicyInner::AssumeAlreadyHashed)
    }

    /// Return `HashInProgress` for `timeout` after hashing has started before
    /// completing it successfully.
    pub fn timer(timeout: Duration) -> Self {
        Self(HostFlashHashPolicyInner::Timer(timeout))
    }

    /// Returns a channel that allows the caller to control when hashing
    /// completes.
    pub fn channel() -> (Self, HostFlashHashCompletionSender) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self(HostFlashHashPolicyInner::Channel(rx)),
            HostFlashHashCompletionSender(tx),
        )
    }
}

#[derive(Debug)]
enum HostFlashHashPolicyInner {
    /// always assume hashing has already been computed
    AssumeAlreadyHashed,
    /// complete hashing after `Duration` has elapsed
    Timer(Duration),
    /// complete hashing if there's a message in this channel
    Channel(mpsc::UnboundedReceiver<()>),
}

pub struct HostFlashHashCompletionSender(mpsc::UnboundedSender<()>);

impl HostFlashHashCompletionSender {
    /// Allow the next request to get the hash result to succeed.
    ///
    /// Multiple calls to this function will queue multiple hash result
    /// successes.
    pub fn complete_next_hashing_attempt(&self) {
        self.0.send(()).expect("receiving sp-sim instance is gone");
    }
}
