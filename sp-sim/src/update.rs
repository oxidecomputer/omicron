// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::mem;

use crate::SIM_GIMLET_BOARD;
use crate::SIM_ROT_BOARD;
use crate::SIM_ROT_STAGE0_BOARD;
use crate::SIM_SIDECAR_BOARD;
use gateway_messages::RotSlotId;
use gateway_messages::RotStateV3;
use gateway_messages::SpComponent;
use gateway_messages::SpError;
use gateway_messages::UpdateChunk;
use gateway_messages::UpdateId;
use gateway_messages::UpdateInProgressStatus;
use hubtools::RawHubrisImage;

pub(crate) struct SimSpUpdate {
    state: UpdateState,
    last_sp_update_data: Option<Box<[u8]>>,
    last_rot_update_data: Option<Box<[u8]>>,
    last_host_phase1_update_data: BTreeMap<u16, Box<[u8]>>,
    active_host_slot: Option<u16>,

    caboose_sp_active: CabooseValue,
    caboose_sp_inactive: CabooseValue,
    caboose_rot_a: CabooseValue,
    caboose_rot_b: CabooseValue,
    caboose_stage0: CabooseValue,
    caboose_stage0next: CabooseValue,
    rot_state: RotStateV3,
}

impl SimSpUpdate {
    pub(crate) fn new(
        baseboard_kind: BaseboardKind,
        no_stage0_caboose: bool,
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

        let sp_board = baseboard_kind.sp_board();
        let sp_name = baseboard_kind.sp_name();
        let rot_name = baseboard_kind.rot_name();

        let caboose_sp_active = CabooseValue::Caboose(
            hubtools::CabooseBuilder::default()
                .git_commit(SP_GITC0)
                .board(sp_board)
                .name(sp_name)
                .version(SP_VERS0)
                .build(),
        );
        let caboose_sp_inactive = CabooseValue::Caboose(
            hubtools::CabooseBuilder::default()
                .git_commit(SP_GITC1)
                .board(sp_board)
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

            // In the real SP, there is always _some_ active host slot. We could
            // emulate that by always defaulting to slot 0, but instead we'll
            // ensure any tests that expect to read or write a particular slot
            // set that slot as active first.
            active_host_slot: None,

            caboose_sp_active,
            caboose_sp_inactive,
            caboose_rot_a,
            caboose_rot_b,
            caboose_stage0,
            caboose_stage0next,

            rot_state,
        }
    }

    // TODO-completeness Split into `sp_prepare` and `component_prepare` when we
    // need to simulate aux flash-related (SP update only) things.
    pub(crate) fn prepare(
        &mut self,
        component: SpComponent,
        id: UpdateId,
        total_size: usize,
    ) -> Result<(), SpError> {
        match &self.state {
            state @ UpdateState::Prepared { .. } => {
                Err(SpError::UpdateInProgress(state.to_message()))
            }
            UpdateState::NotPrepared
            | UpdateState::Aborted(_)
            | UpdateState::Completed { .. } => {
                // XXX-dap shouldn't we fail in some of these cases?
                let slot = if component == SpComponent::HOST_CPU_BOOT_FLASH {
                    match self.active_host_slot {
                        Some(slot) => slot,
                        None => return Err(SpError::InvalidSlotForComponent),
                    }
                } else {
                    // We don't manage SP or RoT slots, so just use 0
                    0
                };

                self.state = UpdateState::Prepared {
                    component,
                    id,
                    slot,
                    data: Cursor::new(vec![0u8; total_size].into_boxed_slice()),
                };

                if let Some(caboose) = self.caboose_mut(component, slot) {
                    *caboose = CabooseValue::InvalidMissing;
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
                                // XXX-dap log error
                                *caboose_value = CabooseValue::InvalidMissing;
                            }
                        };
                    }

                    self.state = UpdateState::Completed { component, id, data };
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
        // XXX-dap copy state fields
        match &self.state {
            UpdateState::Completed { data, component, .. } => {
                if *component == SpComponent::SP_ITSELF {
                    self.last_sp_update_data = Some(data.clone());
                }
            }
            UpdateState::NotPrepared
            | UpdateState::Prepared { .. }
            | UpdateState::Aborted(_) => (),
        }
    }

    pub(crate) fn rot_reset(&mut self) {
        // XXX-dap copy state fields
        match &self.state {
            UpdateState::Completed { data, component, .. } => {
                if *component == SpComponent::ROT {
                    self.last_rot_update_data = Some(data.clone());
                }
            }
            UpdateState::NotPrepared
            | UpdateState::Prepared { .. }
            | UpdateState::Aborted(_) => (),
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
                // XXX-dap
                Ok(())
            }
            SpComponent::STAGE0 => {
                if slot == 1 {
                    // XXX-dap
                    return Ok(());
                } else {
                    Err(SpError::RequestUnsupportedForComponent)
                }
            }
            SpComponent::HOST_CPU_BOOT_FLASH => {
                // XXX-dap
                self.active_host_slot = Some(slot);
                Ok(())
            }
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
            SpComponent::ROT => todo!(), // XXX-dap
            // The only active component is stage0
            SpComponent::STAGE0 => Ok(0),
            // The real SP returns `RequestUnsupportedForComponent` for anything
            // other than the RoT, including SP_ITSELF.
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
enum CabooseValue {
    /// emulate an actual caboose
    Caboose(hubtools::Caboose),
    /// emulate "the image does not include a caboose"
    // This will be used shortly.
    #[allow(dead_code)]
    InvalidMissing,
    /// emulate "the image caboose does not contain 'KEY'"
    InvalidMissingAllKeys,
    /// emulate "failed to read data from the caboose" (erased)
    // This will be used shortly.
    #[allow(dead_code)]
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
