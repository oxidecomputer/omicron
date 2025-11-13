// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the states of network-attached storage.

use chrono::Utc;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use sled_agent_types::disk::DiskStateRequested;
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
pub struct DiskStates {
    current: DiskRuntimeState,
    desired: Option<DiskStateRequested>,
}

impl DiskStates {
    pub fn new(current: DiskRuntimeState) -> Self {
        DiskStates { current, desired: None }
    }

    /// Returns the current disk state.
    pub fn current(&self) -> &DiskRuntimeState {
        &self.current
    }

    /// Returns the desired disk state, if any exists.
    pub fn desired(&self) -> &Option<DiskStateRequested> {
        &self.desired
    }

    /// Update the known state of a disk based on an observed state from
    /// Propolis.
    pub fn observe_transition(
        &mut self,
        observed: &DiskState,
    ) -> Option<Action> {
        self.transition(observed.clone(), None);
        None
    }

    /// Attempts to move from the current state to the requested "target" state.
    ///
    /// On success, returns the action, if any, which is necessary to carry
    /// out this state transition.
    pub fn request_transition(
        &mut self,
        target: &DiskStateRequested,
    ) -> Result<Option<Action>, Error> {
        match target {
            DiskStateRequested::Detached => self.request_detach(),
            DiskStateRequested::Attached(uuid) => self.request_attach(*uuid),
            DiskStateRequested::Destroyed => self.request_destroy(),
            DiskStateRequested::Faulted => self.request_fault(),
        }
    }

    // Transitions to a new DiskState value, updating the timestamp and
    // generation number.
    //
    // This transition always succeeds.
    fn transition(
        &mut self,
        next: DiskState,
        desired: Option<DiskStateRequested>,
    ) {
        // TODO: Deal with no-op transition?
        self.current = DiskRuntimeState {
            disk_state: next,
            generation: self.current.generation.next(),
            time_updated: Utc::now(),
        };
        self.desired = desired;
    }

    fn request_detach(&mut self) -> Result<Option<Action>, Error> {
        match self.current.disk_state {
            // Already detached or can detach immediately.
            DiskState::Creating | DiskState::Detached => {
                self.transition(DiskState::Detached, None);
                return Ok(None);
            }
            // Currently attached - enter detached through detaching.
            DiskState::Attaching(uuid)
            | DiskState::Attached(uuid)
            | DiskState::Detaching(uuid) => {
                self.transition(
                    DiskState::Detaching(uuid),
                    Some(DiskStateRequested::Detached),
                );
                return Ok(Some(Action::Detach(uuid)));
            }
            // Cannot detach.
            DiskState::Finalizing
            | DiskState::Maintenance
            | DiskState::ImportReady
            | DiskState::ImportingFromUrl
            | DiskState::ImportingFromBulkWrites
            | DiskState::Destroyed
            | DiskState::Faulted => {
                return Err(Error::invalid_request(format!(
                    "cannot detach from {}",
                    self.current.disk_state
                )));
            }
        };
    }

    fn request_attach(&mut self, uuid: Uuid) -> Result<Option<Action>, Error> {
        match self.current.disk_state {
            // Currently attached - only legal to attach to current ID
            // (which is a no-op anyway).
            DiskState::Attaching(id) | DiskState::Attached(id) => {
                if uuid != id {
                    return Err(Error::invalid_request(
                        "disk is already attached",
                    ));
                }
                return Ok(None);
            }
            // Not attached - enter attached through attaching.
            DiskState::Creating | DiskState::Detached => {
                self.transition(
                    DiskState::Attaching(uuid),
                    Some(DiskStateRequested::Attached(uuid)),
                );
                return Ok(Some(Action::Attach(uuid)));
            }
            // Cannot attach.
            DiskState::Finalizing
            | DiskState::Maintenance
            | DiskState::ImportReady
            | DiskState::ImportingFromUrl
            | DiskState::ImportingFromBulkWrites
            | DiskState::Detaching(_)
            | DiskState::Destroyed
            | DiskState::Faulted => {
                return Err(Error::invalid_request(format!(
                    "cannot attach from {}",
                    self.current.disk_state
                )));
            }
        }
    }

    fn request_destroy(&mut self) -> Result<Option<Action>, Error> {
        if self.current.disk_state.is_attached() {
            let id = *self.current.disk_state.attached_instance_id().unwrap();
            self.transition(
                DiskState::Detaching(id),
                Some(DiskStateRequested::Destroyed),
            );
            return Ok(Some(Action::Detach(id)));
        } else {
            self.transition(DiskState::Destroyed, None);
            return Ok(Some(Action::Destroy));
        }
    }

    fn request_fault(&mut self) -> Result<Option<Action>, Error> {
        if self.current.disk_state.is_attached() {
            let id = *self.current.disk_state.attached_instance_id().unwrap();
            self.transition(
                DiskState::Detaching(id),
                Some(DiskStateRequested::Faulted),
            );
            return Ok(Some(Action::Detach(id)));
        } else {
            self.transition(DiskState::Faulted, None);
            // Unlike "destroy", no action necessary to identify that
            // the attached disk is now faulted.
            return Ok(None);
        }
    }
}
