// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::io::Cursor;
use std::mem;

use gateway_messages::SpComponent;
use gateway_messages::SpError;
use gateway_messages::UpdateChunk;
use gateway_messages::UpdateId;
use gateway_messages::UpdateInProgressStatus;

pub(crate) struct SimSpUpdate {
    state: UpdateState,
    last_update_data: Option<Box<[u8]>>,
}

impl Default for SimSpUpdate {
    fn default() -> Self {
        Self { state: UpdateState::NotPrepared, last_update_data: None }
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
                self.state = UpdateState::Prepared {
                    component,
                    id,
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
            UpdateState::Prepared { component, id, data } => {
                // Ensure that the update ID and target component are correct.
                if chunk.id != *id || chunk.component != *component {
                    return Err(SpError::InvalidUpdateId { sp_update_id: *id });
                };
                if data.position() != chunk.offset as u64 {
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
                    self.state = UpdateState::Completed {
                        id: *id,
                        data: stolen.into_inner(),
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
        self.last_update_data = match &self.state {
            UpdateState::Completed { data, .. } => Some(data.clone()),
            UpdateState::NotPrepared
            | UpdateState::Prepared { .. }
            | UpdateState::Aborted(_) => None,
        };
    }

    pub(crate) fn last_update_data(&self) -> Option<Box<[u8]>> {
        self.last_update_data.clone()
    }
}

enum UpdateState {
    NotPrepared,
    Prepared {
        component: SpComponent,
        id: UpdateId,
        // data would ordinarily be a Cursor<Vec<u8>>, but that can grow and
        // reallocate. We want to ensure that we don't receive any more data
        // than originally promised, so use a Cursor<Box<[u8]>> to ensure that
        // it never grows.
        data: Cursor<Box<[u8]>>,
    },
    Aborted(UpdateId),
    Completed {
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
