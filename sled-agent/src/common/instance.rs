// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the states of VM instances.

use crate::params::{
    InstanceRuntimeStateMigrateParams, InstanceRuntimeStateRequested,
    InstanceStateRequested,
};
use chrono::Utc;
use omicron_common::api::external::Error;
use omicron_common::api::external::InstanceState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use propolis_client::api::InstanceState as PropolisInstanceState;

/// Action to be taken on behalf of state transition.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// Update the VM state to cause it to run.
    Run,
    /// Update the VM state to cause it to stop.
    Stop,
    /// Invoke a reboot of the VM.
    Reboot,
    /// Terminate the VM and associated service.
    Destroy,
}

fn get_next_desired_state(
    observed: &PropolisInstanceState,
    requested: InstanceStateRequested,
) -> Option<InstanceStateRequested> {
    use InstanceStateRequested as Requested;
    use PropolisInstanceState as Observed;

    match (requested, observed) {
        (Requested::Running, Observed::Running) => None,
        (Requested::Stopped, Observed::Stopped) => None,
        (Requested::Destroyed, Observed::Destroyed) => None,

        // Reboot requests.
        // - "Stopping" is an expected state on the way to rebooting.
        // - "Starting" expects "Running" to occur next.
        // - Other observed states stop the expected transitions.
        (Requested::Reboot, Observed::Stopping) => Some(Requested::Reboot),
        (Requested::Reboot, Observed::Starting) => Some(Requested::Running),
        (Requested::Reboot, _) => None,

        (_, _) => Some(requested),
    }
}

/// The instance state is a combination of the last-known state, as well as an
/// "objective" state which the sled agent will work towards achieving.
#[derive(Clone, Debug)]
pub struct InstanceStates {
    // Last known state reported from Propolis.
    current: InstanceRuntimeState,

    // Desired state, which we will attempt to poke the VM towards.
    desired: Option<InstanceRuntimeStateRequested>,
}

impl InstanceStates {
    pub fn new(current: InstanceRuntimeState) -> Self {
        InstanceStates { current, desired: None }
    }

    /// Returns the current instance state.
    pub fn current(&self) -> &InstanceRuntimeState {
        &self.current
    }

    /// Returns the current instance state.
    pub fn current_mut(&mut self) -> &mut InstanceRuntimeState {
        &mut self.current
    }

    /// Returns the desired instance state, if any exists.
    pub fn desired(&self) -> &Option<InstanceRuntimeStateRequested> {
        &self.desired
    }

    /// Update the known state of an instance based on an observed state from
    /// Propolis.
    pub fn observe_transition(
        &mut self,
        observed: &PropolisInstanceState,
    ) -> Option<Action> {
        use InstanceState as State;
        use PropolisInstanceState as Observed;

        let current = match observed {
            // TODO(luqman): update propolis naming to match
            Observed::Creating => State::Provisioned,
            Observed::Starting => State::Starting,
            Observed::Running => State::Running,
            Observed::Stopping => State::Stopping,
            Observed::Stopped => State::Stopped,
            Observed::Rebooting => State::Rebooting,
            Observed::Migrating => State::Migrating,
            Observed::Repairing => State::Repairing,
            Observed::Failed => State::Failed,
            // NOTE: This is a bit of an odd one - we intentionally do *not*
            // translate the "destroyed" propolis state to the destroyed instance
            // API state.
            //
            // When a propolis instance reports that it has been destroyed,
            // this does not necessarily mean the customer-visible instance
            // should be torn down. Instead, it implies that the Propolis service
            // should be stopped, but the VM could be allocated to a different
            // machine.
            Observed::Destroyed => State::Stopped,
        };

        let desired = self
            .desired
            .as_ref()
            .and_then(|s| get_next_desired_state(&observed, s.run_state));

        self.transition(current, desired);

        // Most commands to update Propolis are triggered via requests (from
        // Nexus), but if the instance reports that it has been destroyed,
        // we should clean it up.
        if matches!(observed, Observed::Destroyed) {
            Some(Action::Destroy)
        } else {
            None
        }
    }

    /// Attempts to move from the current state to the requested "target" state.
    ///
    /// On success, returns the action, if any, which is necessary to carry
    /// out this state transition.
    pub fn request_transition(
        &mut self,
        target: &InstanceRuntimeStateRequested,
    ) -> Result<Option<Action>, Error> {
        match target.run_state {
            InstanceStateRequested::Provisioned => self.request_provisioned(),
            InstanceStateRequested::Running => self.request_running(),
            InstanceStateRequested::Stopped => self.request_stopped(),
            InstanceStateRequested::Reboot => self.request_reboot(),
            InstanceStateRequested::Migrating => {
                self.request_migrating(target.migration_params)
            }
            InstanceStateRequested::Destroyed => self.request_destroyed(),
        }
    }

    // Transitions to a new InstanceState value, updating the timestamp and
    // generation number.
    //
    // This transition always succeeds.
    pub(crate) fn transition(
        &mut self,
        next: InstanceState,
        desired: Option<InstanceStateRequested>,
    ) {
        self.current.run_state = next;
        self.current.gen = self.current.gen.next();
        self.current.time_updated = Utc::now();
        self.desired = desired.map(|run_state| InstanceRuntimeStateRequested {
            run_state,
            migration_params: None,
        });
    }

    fn request_provisioned(&mut self) -> Result<Option<Action>, Error> {
        match self.current.run_state {
            // Valid states we can transition through and stop right before starting the Instance.
            InstanceState::Creating
            | InstanceState::Provisioning
            | InstanceState::Stopping
            | InstanceState::Stopped
            | InstanceState::Rebooting => {
                self.transition(
                    InstanceState::Provisioned,
                    Some(InstanceStateRequested::Provisioned),
                );
                // No further action needed to provision the instance.
                return Ok(None);
            }

            // No-op
            InstanceState::Provisioned => return Ok(None),

            // Can't go backwards if the Instance is already running (or will be shortly).
            InstanceState::Starting
            | InstanceState::Running
            | InstanceState::Migrating => {
                return Err(Error::InvalidRequest {
                    message: format!(
                        "instance already provisioned and in state \"{}\"",
                        self.current.run_state,
                    ),
                });
            }

            InstanceState::Repairing
            | InstanceState::Failed
            | InstanceState::Destroyed => {
                return Err(Error::InvalidRequest {
                    message: format!(
                        "instance in invalid state \"{}\"",
                        self.current.run_state,
                    ),
                });
            }
        }
    }

    fn request_running(&mut self) -> Result<Option<Action>, Error> {
        match self.current.run_state {
            // Early exit: Running request is no-op
            InstanceState::Running
            | InstanceState::Starting
            | InstanceState::Rebooting
            | InstanceState::Migrating => return Ok(None),
            // Valid states for a running request
            InstanceState::Creating
            | InstanceState::Provisioning
            | InstanceState::Provisioned
            | InstanceState::Stopping
            | InstanceState::Stopped => {
                self.transition(
                    InstanceState::Starting,
                    Some(InstanceStateRequested::Running),
                );
                return Ok(Some(Action::Run));
            }
            // Invalid states for a running request
            InstanceState::Repairing
            | InstanceState::Failed
            | InstanceState::Destroyed => {
                return Err(Error::InvalidRequest {
                    message: format!(
                        "cannot run instance in state \"{}\"",
                        self.current.run_state,
                    ),
                });
            }
        }
    }

    fn request_stopped(&mut self) -> Result<Option<Action>, Error> {
        match self.current.run_state {
            // Early exit: Stop request is a no-op
            InstanceState::Stopped | InstanceState::Stopping => {
                return Ok(None)
            }
            // Valid states for a stop request
            InstanceState::Creating | InstanceState::Provisioning | InstanceState::Provisioned => {
                // Instance hasn't been started yet, no action necessary.
                self.transition(InstanceState::Stopped, None);
                return Ok(None);
            }
            InstanceState::Starting
            | InstanceState::Running
            | InstanceState::Rebooting => {
                // The VM is running, explicitly tell it to stop.
                self.transition(
                    InstanceState::Stopping,
                    Some(InstanceStateRequested::Stopped),
                );
                return Ok(Some(Action::Stop));
            }
            // Invalid states for a stop request
            InstanceState::Migrating
            | InstanceState::Repairing
            | InstanceState::Failed
            | InstanceState::Destroyed => {
                return Err(Error::InvalidRequest {
                    message: format!(
                        "cannot stop instance in state \"{}\"",
                        self.current.run_state,
                    ),
                });
            }
        }
    }

    fn request_reboot(&mut self) -> Result<Option<Action>, Error> {
        match self.current.run_state {
            // Early exit: Reboot request is a no-op
            InstanceState::Rebooting => return Ok(None),
            // Valid states for a reboot request
            InstanceState::Starting | InstanceState::Running => {
                self.transition(
                    InstanceState::Rebooting,
                    Some(InstanceStateRequested::Reboot),
                );
                return Ok(Some(Action::Reboot));
            }
            // Invalid states for a reboot request
            _ => {
                return Err(Error::InvalidRequest {
                    message: format!(
                        "cannot reboot instance in state \"{}\"",
                        self.current.run_state,
                    ),
                });
            }
        }
    }

    fn request_migrating(
        &mut self,
        migration_params: Option<InstanceRuntimeStateMigrateParams>,
    ) -> Result<Option<Action>, Error> {
        let InstanceRuntimeStateMigrateParams { migration_id, dst_propolis_id } =
            migration_params.ok_or_else(|| {
                Error::invalid_request(
                "expected migration IDs to transition to \"migrating\" state",
            )
            })?;
        match self.current.run_state {
            // Valid states for a migration request
            InstanceState::Running => {
                self.current.dst_propolis_id = Some(dst_propolis_id);
                self.current.migration_id = Some(migration_id);
                self.transition(
                    InstanceState::Migrating,
                    Some(InstanceStateRequested::Running),
                );
                // We don't return any action here for propolis to take
                // because the actual migration process will be driven
                // via a followup request to sled agent.
                Ok(None)
            }
            InstanceState::Migrating => match self.current.migration_id {
                Some(id) if id == migration_id => {
                    match self.current.dst_propolis_id {
                        // We're already performing the given migration to the
                        // given propolis instance: no-op
                        Some(id) if id == dst_propolis_id => Ok(None),
                        // The migration ID matched but not the propolis ID: bail out
                        Some(id) => Err(Error::InvalidRequest {
                                message: format!(
                                    "already perfoming given migration to different propolis: {}",
                                    id
                                )
                            }),
                        // If we're marked as 'Migrating' but have no dst propolis
                        // ID bail because that shouldn't be possible
                        None => Err(Error::internal_error(
                            "migrating but no dst propolis id present"
                        )),
                    }
                }
                // A different migration is already underway
                Some(id) => Err(Error::InvalidRequest {
                    message: format!("migration already in progress: {}", id),
                }),
                // If we're marked as 'Migrating' but have no migration
                // id bail because that shouldn't be possible
                None => Err(Error::internal_error(
                    "migrating but no migration id present",
                )),
            },

            // Invalid states for a migration request
            InstanceState::Creating
            | InstanceState::Provisioning
            | InstanceState::Provisioned // TODO(luqman): should be able to migrate this?
            | InstanceState::Starting
            | InstanceState::Stopping
            | InstanceState::Stopped
            | InstanceState::Rebooting
            | InstanceState::Repairing
            | InstanceState::Failed
            | InstanceState::Destroyed => {
                return Err(Error::InvalidRequest {
                    message: format!(
                        "cannot migrate instance in state \"{}\"",
                        self.current.run_state,
                    ),
                });
            }
        }
    }

    fn request_destroyed(&mut self) -> Result<Option<Action>, Error> {
        if self.current.run_state.is_stopped() {
            self.transition(InstanceState::Destroyed, None);
            return Ok(Some(Action::Destroy));
        } else {
            self.transition(
                InstanceState::Stopping,
                Some(InstanceStateRequested::Destroyed),
            );
            return Ok(Some(Action::Stop));
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Action, InstanceStates};

    use crate::params::{
        InstanceRuntimeStateMigrateParams, InstanceRuntimeStateRequested,
        InstanceStateRequested as Requested,
    };
    use chrono::Utc;
    use omicron_common::api::external::{
        ByteCount, Error, Generation, InstanceCpuCount, InstanceState as State,
    };
    use omicron_common::api::internal::nexus::InstanceRuntimeState;
    use propolis_client::api::InstanceState as Observed;
    use std::assert_matches::assert_matches;
    use uuid::Uuid;

    fn make_instance() -> InstanceStates {
        InstanceStates::new(InstanceRuntimeState {
            run_state: State::Creating,
            sled_id: Uuid::new_v4(),
            propolis_id: Uuid::new_v4(),
            dst_propolis_id: None,
            propolis_addr: None,
            migration_id: None,
            ncpus: InstanceCpuCount(2),
            memory: ByteCount::from_mebibytes_u32(512),
            hostname: "myvm".to_string(),
            gen: Generation::new(),
            time_updated: Utc::now(),
        })
    }

    fn verify_state(
        instance: &InstanceStates,
        expected_current: State,
        expected_desired: Option<Requested>,
    ) {
        assert_eq!(expected_current, instance.current().run_state);
        match expected_desired {
            Some(desired) => {
                assert_eq!(
                    desired,
                    instance.desired().as_ref().unwrap().run_state
                );
            }
            None => assert!(instance.desired().is_none()),
        }
    }

    fn runtime_state(run_state: Requested) -> InstanceRuntimeStateRequested {
        InstanceRuntimeStateRequested { run_state, migration_params: None }
    }

    #[test]
    fn test_running_from_creating() {
        let mut instance = make_instance();

        verify_state(&instance, State::Creating, None);
        assert_eq!(
            Action::Run,
            instance
                .request_transition(&runtime_state(Requested::Running))
                .unwrap()
                .unwrap()
        );
        verify_state(&instance, State::Starting, Some(Requested::Running));
    }

    #[test]
    fn test_reboot() {
        let mut instance = make_instance();

        assert_eq!(
            Action::Run,
            instance
                .request_transition(&runtime_state(Requested::Running))
                .unwrap()
                .unwrap()
        );
        verify_state(&instance, State::Starting, Some(Requested::Running));
        assert_eq!(None, instance.observe_transition(&Observed::Running));

        // Normal reboot behavior:
        // - Request Reboot
        // - Observe Stopping, Starting, Running
        assert_eq!(
            Action::Reboot,
            instance
                .request_transition(&runtime_state(Requested::Reboot))
                .unwrap()
                .unwrap()
        );
        verify_state(&instance, State::Rebooting, Some(Requested::Reboot));

        assert_eq!(None, instance.observe_transition(&Observed::Stopping));
        verify_state(&instance, State::Stopping, Some(Requested::Reboot));

        assert_eq!(None, instance.observe_transition(&Observed::Starting));
        verify_state(&instance, State::Starting, Some(Requested::Running));

        assert_eq!(None, instance.observe_transition(&Observed::Running));
        verify_state(&instance, State::Running, None);
    }

    #[test]
    fn test_reboot_skip_starting_converges_to_running() {
        let mut instance = make_instance();

        assert_eq!(
            Action::Run,
            instance
                .request_transition(&runtime_state(Requested::Running))
                .unwrap()
                .unwrap()
        );
        verify_state(&instance, State::Starting, Some(Requested::Running));
        assert_eq!(None, instance.observe_transition(&Observed::Running));

        // Unexpected reboot behavior:
        // - Request Reboot
        // - Observe Stopping, jump immediately to Running.
        // - Ultimately, we should still end up "running".
        assert_eq!(
            Action::Reboot,
            instance
                .request_transition(&runtime_state(Requested::Reboot))
                .unwrap()
                .unwrap()
        );
        verify_state(&instance, State::Rebooting, Some(Requested::Reboot));

        assert_eq!(None, instance.observe_transition(&Observed::Stopping));
        verify_state(&instance, State::Stopping, Some(Requested::Reboot));

        assert_eq!(None, instance.observe_transition(&Observed::Running));
        verify_state(&instance, State::Running, None);
    }

    #[test]
    fn test_reboot_skip_stopping_converges_to_running() {
        let mut instance = make_instance();

        assert_eq!(
            Action::Run,
            instance
                .request_transition(&runtime_state(Requested::Running))
                .unwrap()
                .unwrap()
        );
        verify_state(&instance, State::Starting, Some(Requested::Running));
        assert_eq!(None, instance.observe_transition(&Observed::Running));

        // Unexpected reboot behavior:
        // - Request Reboot
        // - Observe Starting then Running.
        // - Ultimately, we should still end up "running".
        assert_eq!(
            Action::Reboot,
            instance
                .request_transition(&runtime_state(Requested::Reboot))
                .unwrap()
                .unwrap()
        );
        verify_state(&instance, State::Rebooting, Some(Requested::Reboot));

        assert_eq!(None, instance.observe_transition(&Observed::Starting));
        verify_state(&instance, State::Starting, Some(Requested::Running));

        assert_eq!(None, instance.observe_transition(&Observed::Running));
        verify_state(&instance, State::Running, None);
    }

    #[test]
    fn test_destroy_from_running_stops_first() {
        let mut instance = make_instance();
        assert_eq!(None, instance.observe_transition(&Observed::Running));
        assert_eq!(
            Action::Stop,
            instance
                .request_transition(&runtime_state(Requested::Destroyed))
                .unwrap()
                .unwrap()
        );
        verify_state(&instance, State::Stopping, Some(Requested::Destroyed));
        assert_eq!(None, instance.observe_transition(&Observed::Stopped));
        verify_state(&instance, State::Stopped, Some(Requested::Destroyed));
        assert_eq!(
            Action::Destroy,
            instance.observe_transition(&Observed::Destroyed).unwrap()
        );
        verify_state(&instance, State::Stopped, None);
    }

    #[test]
    fn test_destroy_from_stopped_destroys_immediately() {
        let mut instance = make_instance();
        assert_eq!(None, instance.observe_transition(&Observed::Stopped));
        assert_eq!(
            Action::Destroy,
            instance
                .request_transition(&runtime_state(Requested::Destroyed))
                .unwrap()
                .unwrap()
        );
        verify_state(&instance, State::Destroyed, None);
    }

    fn migrating_req() -> InstanceRuntimeStateRequested {
        InstanceRuntimeStateRequested {
            run_state: Requested::Migrating,
            migration_params: Some(InstanceRuntimeStateMigrateParams {
                migration_id: Uuid::new_v4(),
                dst_propolis_id: Uuid::new_v4(),
            }),
        }
    }

    #[test]
    fn test_migrating_from_invalid_states_fails() {
        let mut instance = make_instance();

        let invalid_migrate_states = [
            State::Creating,
            State::Starting,
            State::Stopping,
            State::Stopped,
            State::Rebooting,
            State::Repairing,
            State::Failed,
            State::Destroyed,
        ];

        for state in invalid_migrate_states {
            instance.current_mut().run_state = state;
            assert_matches!(
                // Match state here as well so failure shows which
                // state we failed on.
                (state, instance.request_transition(&migrating_req())),
                (_, Err(Error::InvalidRequest { message }))
                    if message.contains("cannot migrate instance"),
            );
        }
    }

    #[test]
    fn test_migrating_from_running() {
        let mut instance = make_instance();

        verify_state(&instance, State::Creating, None);
        assert_eq!(
            Action::Run,
            instance
                .request_transition(&runtime_state(Requested::Running))
                .unwrap()
                .unwrap()
        );
        verify_state(&instance, State::Starting, Some(Requested::Running));
        assert_eq!(None, instance.observe_transition(&Observed::Running));
        verify_state(&instance, State::Running, None);

        let migrating_req = migrating_req();
        assert_matches!(instance.request_transition(&migrating_req), Ok(None),);
        verify_state(&instance, State::Migrating, Some(Requested::Running));
        assert_eq!(
            migrating_req.migration_params.map(|m| m.migration_id),
            instance.current().migration_id,
        );
        assert_eq!(
            migrating_req.migration_params.map(|m| m.dst_propolis_id),
            instance.current().dst_propolis_id,
        );
    }

    #[test]
    fn test_migrating_with_same_migration() {
        let mut instance = make_instance();

        verify_state(&instance, State::Creating, None);
        assert_eq!(
            Action::Run,
            instance
                .request_transition(&runtime_state(Requested::Running))
                .unwrap()
                .unwrap()
        );
        verify_state(&instance, State::Starting, Some(Requested::Running));
        assert_eq!(None, instance.observe_transition(&Observed::Running));
        verify_state(&instance, State::Running, None);

        let migrating_req = migrating_req();
        assert_matches!(instance.request_transition(&migrating_req), Ok(None),);
        verify_state(&instance, State::Migrating, Some(Requested::Running));

        // A subsequent request for the same migration is a no-op
        assert_matches!(instance.request_transition(&migrating_req), Ok(None),);
        verify_state(&instance, State::Migrating, Some(Requested::Running));
    }

    #[test]
    fn test_migrating_missing_params_fails() {
        let mut instance = make_instance();

        verify_state(&instance, State::Creating, None);
        assert_eq!(
            Action::Run,
            instance
                .request_transition(&runtime_state(Requested::Running))
                .unwrap()
                .unwrap()
        );
        verify_state(&instance, State::Starting, Some(Requested::Running));
        assert_eq!(None, instance.observe_transition(&Observed::Running));
        verify_state(&instance, State::Running, None);

        assert_matches!(
            instance.request_transition(&runtime_state(Requested::Migrating)),
            Err(Error::InvalidRequest { message })
                if message.contains("expected migration IDs to transition"),
        );
    }

    #[test]
    fn test_migrating_with_same_migration_different_propolis_fails() {
        let mut instance = make_instance();

        verify_state(&instance, State::Creating, None);
        assert_eq!(
            Action::Run,
            instance
                .request_transition(&runtime_state(Requested::Running))
                .unwrap()
                .unwrap()
        );
        verify_state(&instance, State::Starting, Some(Requested::Running));
        assert_eq!(None, instance.observe_transition(&Observed::Running));
        verify_state(&instance, State::Running, None);

        let mut migrating_req = migrating_req();
        assert_matches!(instance.request_transition(&migrating_req), Ok(None),);
        verify_state(&instance, State::Migrating, Some(Requested::Running));

        // We keep the Migration ID the same but pass a different
        // propolis ID to migrate too.
        migrating_req.migration_params.as_mut().unwrap().dst_propolis_id =
            Uuid::new_v4();
        assert_matches!(
            instance.request_transition(&migrating_req),
            Err(Error::InvalidRequest { message })
                if message.contains("already perfoming given migration to different propolis"),
        );
    }

    #[test]
    fn test_migrating_with_new_migration_fails() {
        let mut instance = make_instance();

        verify_state(&instance, State::Creating, None);
        assert_eq!(
            Action::Run,
            instance
                .request_transition(&runtime_state(Requested::Running))
                .unwrap()
                .unwrap()
        );
        verify_state(&instance, State::Starting, Some(Requested::Running));
        assert_eq!(None, instance.observe_transition(&Observed::Running));
        verify_state(&instance, State::Running, None);

        assert_matches!(
            instance.request_transition(&migrating_req()),
            Ok(None),
        );
        verify_state(&instance, State::Migrating, Some(Requested::Running));

        assert_matches!(
            // NOTE: different migration parameters given in subsequent request
            instance.request_transition(&migrating_req()),
            Err(Error::InvalidRequest { message })
                if message.contains("migration already in progress"),
        );
    }

    #[test]
    fn test_migrating_inconsistent_internal_state() {
        let mut instance = make_instance();

        verify_state(&instance, State::Creating, None);
        assert_eq!(
            Action::Run,
            instance
                .request_transition(&runtime_state(Requested::Running))
                .unwrap()
                .unwrap()
        );
        verify_state(&instance, State::Starting, Some(Requested::Running));
        assert_eq!(None, instance.observe_transition(&Observed::Running));
        verify_state(&instance, State::Running, None);

        let migrating_req = migrating_req();
        assert_matches!(instance.request_transition(&migrating_req), Ok(None),);
        verify_state(&instance, State::Migrating, Some(Requested::Running));

        // This shouldn't happen during the normal course of operation
        // but we also don't want things to proceed if such a state is encountered.

        // Instance is currently marked as 'Migrating' but we'll
        // remove the destination propolis ID
        instance.current_mut().dst_propolis_id = None;

        // A subsequent migrate request that would've been a no-op
        // otherwise should fail in this case
        assert_matches!(
            instance.request_transition(&migrating_req),
            Err(Error::InternalError { internal_message })
                if internal_message.contains("migrating but no dst propolis id present"),
        );

        // Instance is still marked as 'Migrating' but we'll
        // remove the Migration ID as well
        instance.current_mut().migration_id = None;

        // A subsequent migrate request that would've been a no-op
        // otherwise should fail in this case
        assert_matches!(
            instance.request_transition(&migrating_req),
            Err(Error::InternalError { internal_message })
                if internal_message.contains("migrating but no migration id present"),
        );
    }
}
