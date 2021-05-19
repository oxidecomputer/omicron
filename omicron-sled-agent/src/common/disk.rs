use chrono::Utc;
use omicron_common::error::ApiError;
use omicron_common::model::ApiDiskRuntimeState;
use omicron_common::model::ApiDiskState;
use omicron_common::model::ApiDiskStateRequested;
use propolis_client::api::DiskAttachmentState as PropolisDiskState;
use uuid::Uuid;

/// Action to be taken on behalf of state transition.
#[derive(Clone, Debug)]
pub enum Action {
    Attach(Uuid),
    Detach(Uuid),
    Destroy,
}

/// The disk state is a combination of the last-known state, as well as an
/// "objective" state which the sled agent will work towards achieving.
#[derive(Clone, Debug)]
pub struct DiskState {
    current: ApiDiskRuntimeState,
    pending: Option<ApiDiskStateRequested>,
}

impl DiskState {
    pub fn new(current: ApiDiskRuntimeState) -> Self {
        DiskState { current, pending: None }
    }

    pub fn current(&self) -> &ApiDiskRuntimeState {
        &self.current
    }
    pub fn pending(&self) -> &Option<ApiDiskStateRequested> {
        &self.pending
    }

    /// Update the known state of a disk based on a response from Propolis.
    pub fn observe_transition(
        &mut self,
        observed: &PropolisDiskState,
    ) -> Option<Action> {
        let next = match observed {
            PropolisDiskState::Attached(uuid) => ApiDiskState::Attached(*uuid),
            PropolisDiskState::Detached => ApiDiskState::Detached,
            PropolisDiskState::Destroyed => ApiDiskState::Destroyed,
            PropolisDiskState::Faulted => ApiDiskState::Faulted,
        };
        self.transition(next, None);
        None
    }

    /// Attempts to move from the current state to the requested "target" state.
    ///
    /// On success, returns the action, if any, which is necessary to carry
    /// out this state transition.
    pub fn request_transition(
        &mut self,
        target: &ApiDiskStateRequested,
    ) -> Result<Option<Action>, ApiError> {
        match target {
            ApiDiskStateRequested::Detached => self.request_detach(),
            ApiDiskStateRequested::Attached(uuid) => self.request_attach(*uuid),
            ApiDiskStateRequested::Destroyed => self.request_destroy(),
            ApiDiskStateRequested::Faulted => self.request_fault(),
        }
    }

    // Transitions to a new DiskState value, updating the timestamp and
    // generation number.
    //
    // This transition always succeeds.
    fn transition(
        &mut self,
        next: ApiDiskState,
        pending: Option<ApiDiskStateRequested>,
    ) {
        // TODO: Deal with no-op transition?
        self.current = ApiDiskRuntimeState {
            disk_state: next,
            gen: self.current.gen.next(),
            time_updated: Utc::now(),
        };
        self.pending = pending;
    }

    fn request_detach(&mut self) -> Result<Option<Action>, ApiError> {
        match self.current.disk_state {
            // Already detached or can detach immediately.
            ApiDiskState::Creating | ApiDiskState::Detached => {
                self.transition(ApiDiskState::Detached, None);
                return Ok(None);
            }
            // Currently attached - enter detached through detaching.
            ApiDiskState::Attaching(uuid)
            | ApiDiskState::Attached(uuid)
            | ApiDiskState::Detaching(uuid) => {
                self.transition(
                    ApiDiskState::Detaching(uuid),
                    Some(ApiDiskStateRequested::Detached),
                );
                return Ok(Some(Action::Detach(uuid)));
            }
            // Cannot detach.
            ApiDiskState::Destroyed | ApiDiskState::Faulted => {
                return Err(ApiError::InvalidRequest {
                    message: format!(
                        "cannot detach from {}",
                        self.current.disk_state
                    ),
                });
            }
        };
    }

    fn request_attach(
        &mut self,
        uuid: Uuid,
    ) -> Result<Option<Action>, ApiError> {
        match self.current.disk_state {
            // Currently attached - only legal to attach to current ID
            // (which is a no-op anyway).
            ApiDiskState::Attaching(id) | ApiDiskState::Attached(id) => {
                if uuid != id {
                    return Err(ApiError::InvalidRequest {
                        message: "disk is already attached".to_string(),
                    });
                }
                return Ok(None);
            }
            // Not attached - enter attached through attaching.
            ApiDiskState::Creating | ApiDiskState::Detached => {
                self.transition(
                    ApiDiskState::Attaching(uuid),
                    Some(ApiDiskStateRequested::Attached(uuid)),
                );
                return Ok(Some(Action::Attach(uuid)));
            }
            // Cannot attach.
            ApiDiskState::Detaching(_)
            | ApiDiskState::Destroyed
            | ApiDiskState::Faulted => {
                return Err(ApiError::InvalidRequest {
                    message: format!(
                        "cannot attach from {}",
                        self.current.disk_state
                    ),
                });
            }
        }
    }

    fn request_destroy(&mut self) -> Result<Option<Action>, ApiError> {
        if self.current.disk_state.is_attached() {
            let id = *self.current.disk_state.attached_instance_id().unwrap();
            self.transition(
                ApiDiskState::Detaching(id),
                Some(ApiDiskStateRequested::Destroyed),
            );
            return Ok(Some(Action::Detach(id)));
        } else {
            self.transition(ApiDiskState::Destroyed, None);
            return Ok(Some(Action::Destroy));
        }
    }

    fn request_fault(&mut self) -> Result<Option<Action>, ApiError> {
        if self.current.disk_state.is_attached() {
            let id = *self.current.disk_state.attached_instance_id().unwrap();
            self.transition(
                ApiDiskState::Detaching(id),
                Some(ApiDiskStateRequested::Faulted),
            );
            return Ok(Some(Action::Detach(id)));
        } else {
            self.transition(ApiDiskState::Faulted, None);
            // Unlike "destroy", no action necessary to identify that
            // the attached disk is now faulted.
            return Ok(None);
        }
    }
}
