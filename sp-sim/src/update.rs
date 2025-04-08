// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::mem;

use gateway_messages::SpComponent;
use gateway_messages::SpError;
use gateway_messages::UpdateChunk;
use gateway_messages::UpdateId;
use gateway_messages::UpdateInProgressStatus;

pub(crate) struct SimSpUpdate {
    state: UpdateState,
    last_sp_update_data: Option<Box<[u8]>>,
    last_rot_update_data: Option<Box<[u8]>>,
    last_host_phase1_update_data: BTreeMap<u16, Box<[u8]>>,
    active_host_slot: Option<u16>,
}

impl Default for SimSpUpdate {
    fn default() -> Self {
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
        }
    }
}

impl SimSpUpdate {
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
                Ok(())
            }
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
                if chunk.id != *id || chunk.component != *component {
                    return Err(SpError::InvalidUpdateId { sp_update_id: *id });
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

                    self.state = UpdateState::Completed {
                        component: *component,
                        id: *id,
                        data,
                    };
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

    pub(crate) fn set_active_host_slot(&mut self, slot: u16) {
        self.active_host_slot = Some(slot);
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
