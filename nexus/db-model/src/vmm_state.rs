// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::Instance;
use crate::InstanceState;
use crate::Vmm;
use nexus_types::external_api::instance;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use uuid::Uuid;

impl_enum_type!(
    VmmStateEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
        strum::VariantArray,
    )]
    pub enum VmmState;

    Creating => b"creating"
    Starting => b"starting"
    Running => b"running"
    Stopping => b"stopping"
    Stopped => b"stopped"
    Rebooting => b"rebooting"
    Migrating => b"migrating"
    Failed => b"failed"
    Destroyed => b"destroyed"
    SagaUnwound => b"saga_unwound"
);

impl VmmState {
    /// Converts this DB VMM state to the corresponding
    /// `nexus_types::instance::VmmState`, to pass through methods defined in
    /// `nexus_types`.
    ///
    /// This is an internal conversion that always emits the `Prehistoric`
    /// failure reason, since we don't know the actual `nexus_types` failure
    /// reason. Doing this is fine *here*, as the methods we intend to call
    /// don't care about the failure reason.
    fn to_nexus_state(self) -> nexus_types::instance::VmmState {
        use nexus_types::instance::VmmFailureReason;
        use nexus_types::instance::VmmState as NexusVmmState;
        match self {
            Self::Creating => NexusVmmState::Creating,
            Self::Starting => NexusVmmState::Starting,
            Self::Running => NexusVmmState::Running,
            Self::Stopping => NexusVmmState::Stopping,
            Self::Stopped => NexusVmmState::Stopped,
            Self::Rebooting => NexusVmmState::Rebooting,
            Self::Migrating => NexusVmmState::Migrating,
            Self::Failed => {
                NexusVmmState::Failed(VmmFailureReason::Prehistoric)
            }
            Self::Destroyed => NexusVmmState::Destroyed,
            Self::SagaUnwound => NexusVmmState::SagaUnwound,
        }
    }

    pub fn label(&self) -> &'static str {
        self.to_nexus_state().label()
    }

    /// All VMM states.
    pub const ALL_STATES: &'static [Self] =
        <Self as strum::VariantArray>::VARIANTS;

    /// States in which it is safe to deallocate a VMM's sled resources and mark
    /// it as deleted.
    pub const DESTROYABLE_STATES: &'static [Self] =
        &[Self::Destroyed, Self::Failed, Self::SagaUnwound];

    pub const TERMINAL_STATES: &'static [Self] =
        &[Self::Destroyed, Self::Failed];

    /// States in which a VMM record is present in the database but is not
    /// resident on a sled, either because it does not yet exist, was produced
    /// by an unwound update saga and will never exist, or has already been
    /// destroyed.
    pub const NONEXISTENT_STATES: &'static [Self] =
        &[Self::Creating, Self::SagaUnwound, Self::Destroyed];

    pub fn is_terminal(&self) -> bool {
        self.to_nexus_state().is_terminal()
    }

    /// Returns `true` if the VMM is in a state where it is safe to
    /// deallocate its sled resources and mark it as deleted.
    pub fn is_destroyable(&self) -> bool {
        self.to_nexus_state().is_destroyable()
    }

    /// Returns `true` if the VMM is in a state in which it exists on a
    /// sled.
    pub fn exists_on_sled(&self) -> bool {
        self.to_nexus_state().exists_on_sled()
    }
}

impl fmt::Display for VmmState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.to_nexus_state(), f)
    }
}

impl From<nexus_types::instance::VmmState> for VmmState {
    fn from(value: nexus_types::instance::VmmState) -> Self {
        use nexus_types::instance::VmmState as Input;
        match value {
            Input::Creating => Self::Creating,
            Input::Starting => Self::Starting,
            Input::Running => Self::Running,
            Input::Stopping => Self::Stopping,
            Input::Stopped => Self::Stopped,
            Input::Rebooting => Self::Rebooting,
            Input::Migrating => Self::Migrating,
            Input::Failed(_) => Self::Failed,
            Input::Destroyed => Self::Destroyed,
            Input::SagaUnwound => Self::SagaUnwound,
        }
    }
}

impl From<sled_agent_types::instance::VmmState> for VmmState {
    fn from(value: sled_agent_types::instance::VmmState) -> Self {
        use sled_agent_types::instance::VmmState as Input;
        match value {
            Input::Starting => Self::Starting,
            Input::Running => Self::Running,
            Input::Stopping => Self::Stopping,
            Input::Stopped => Self::Stopped,
            Input::Rebooting => Self::Rebooting,
            Input::Migrating => Self::Migrating,
            Input::Failed => Self::Failed,
            Input::Destroyed => Self::Destroyed,
        }
    }
}

impl std::str::FromStr for VmmState {
    type Err = VmmStateParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        for &state in Self::ALL_STATES {
            if s.eq_ignore_ascii_case(state.label()) {
                return Ok(state);
            }
        }

        Err(VmmStateParseError(()))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct VmmStateParseError(());

impl fmt::Display for VmmStateParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "expected one of [")?;
        let mut variants = VmmState::ALL_STATES.iter();
        if let Some(v) = variants.next() {
            write!(f, "{v}")?;
            for v in variants {
                write!(f, ", {v}")?;
            }
        }
        f.write_str("]")
    }
}

impl std::error::Error for VmmStateParseError {}

/// Returns the operator-visible [external API
/// `InstanceState`](instance::InstanceState) for the provided [`Instance`]
/// and its active [`Vmm`], if one exists.
pub struct InstanceStateComputer<'s> {
    instance_state: InstanceState,
    migration_id: Option<&'s Uuid>,
    vmm_state: Option<&'s VmmState>,
}

impl<'s> InstanceStateComputer<'s> {
    pub fn new(instance: &'s Instance, vmm: Option<&'s Vmm>) -> Self {
        Self {
            instance_state: instance.nexus_state,
            migration_id: instance.migration_id.as_ref(),
            vmm_state: vmm.as_ref().map(|vmm| &vmm.state),
        }
    }

    pub(crate) fn from_sled_instance(
        sled_instance: &'s crate::SledInstance,
    ) -> Self {
        Self {
            // A `SledInstance` is a view which contains only instances which
            // are in the `Vmm` state, so we can assume the `InstanceState`
            // here, even though it isn't part of the `SledInstance` model.
            instance_state: InstanceState::Vmm,
            migration_id: sled_instance.migration_id.as_ref(),
            vmm_state: Some(&sled_instance.state),
        }
    }

    /// Determine the [`instance::InstanceState`] to report for the instance,
    /// based on the provided `instance_state`, `vmm_state`, and `migration_id`.
    ///
    /// Note that these fields *must* all come from the the same instance record
    /// in the database; otherwise, an incorrect state may be synthesized.
    ///
    /// # Panics
    ///
    /// In debug mode only, this function panics if it encounters a combination
    /// of instance and VMM states that should not occur: namely, if the
    /// instance is in [`InstanceState::Vmm`] but has no active VMM, or if it is
    /// in any other [`InstanceState`] and *does* have an active VMM.
    ///
    /// These cases should be unrepresentable in the database due to CHECK
    /// constraints on the `instance` table. If they are encountered here, it is
    /// due to a programmer error; either the check constraint is not working
    /// correctly or the instance and VMM records did not come from the same
    /// database query. Therefore, we panic when running tests if these
    /// conditions are encountered in order to loudly alert the programmer that
    /// they've made a mistake. If we are not running in debug mode, we instead
    /// produce a state based on the `InstanceState` so that Nexus does not
    /// crash upon encountering an invalid situation.
    pub fn compute_state_from(
        instance_state: &'s InstanceState,
        migration_id: Option<&'s Uuid>,
        vmm_state: Option<&'s VmmState>,
    ) -> instance::InstanceState {
        Self { instance_state: *instance_state, migration_id, vmm_state }
            .compute_state()
    }

    /// Determine the [`instance::InstanceState`] to report for the instance,
    /// based on the state of the instance record and its active VMM record (if
    /// one exists).
    ///
    /// # Panics
    ///
    /// In debug mode only, this function panics if it encounters a combination
    /// of instance and VMM states that should not occur: namely, if the
    /// instance is in [`InstanceState::Vmm`] but has no active VMM, or if it is
    /// in any other [`InstanceState`] and *does* have an active VMM.
    ///
    /// These cases should be unrepresentable in the database due to CHECK
    /// constraints on the `instance` table. If they are encountered here, it is
    /// due to a programmer error; either the check constraint is not working
    /// correctly or the instance and VMM records did not come from the same
    /// database query. Therefore, we panic when running tests if these
    /// conditions are encountered in order to loudly alert the programmer that
    /// they've made a mistake. If we are not running in debug mode, we instead
    /// produce a state based on the `InstanceState` so that Nexus does not
    /// crash upon encountering an invalid situation.
    pub fn compute_state(&self) -> instance::InstanceState {
        // We want to only report that an instance is `Stopped` when a new
        // `instance-start` saga is able to proceed. That means that:
        match (self.instance_state, self.vmm_state) {
            // - If there's an active migration ID for the instance, *always*
            //   treat its state as "migration" regardless of the VMM's state.
            //
            //   This avoids an issue where an instance whose previous active
            //   VMM has been destroyed as a result of a successful migration
            //   out will appear to be "stopping" for the time between when that
            //   VMM was reported destroyed and when the instance record was
            //   updated to reflect the migration's completion.
            //
            //   Instead, we'll continue to report the instance's state as
            //   "migrating" until an instance-update saga has resolved the
            //   outcome of the migration, since only the instance-update saga
            //   can complete the migration and update the instance record to
            //   point at its new active VMM. No new instance-migrate,
            //   instance-stop, or instance-delete saga can be started
            //   until this occurs.
            //
            //   If the instance actually *has* stopped or failed before a
            //   successful migration out, this is fine, because an
            //   instance-update saga will come along and remove the active VMM
            //   and migration IDs.
            //
            (InstanceState::Vmm, Some(_)) if self.migration_id.is_some() => {
                instance::InstanceState::Migrating
            }
            // - An instance with a "stopped" or "destroyed" VMM needs to be
            //   recast as a "stopping" instance, as the virtual provisioning
            //   resources for that instance have not been deallocated until the
            //   active VMM ID has been unlinked by an update saga.
            (
                InstanceState::Vmm,
                Some(VmmState::Stopped | VmmState::Destroyed),
            ) => instance::InstanceState::Stopping,
            // - An instance with a "failed" VMM should *not* be counted as
            //   failed until the VMM is unlinked, because a start saga must be
            //   able to run for a "failed" instance. Until then, it will
            //   continue to appear "stopping".
            (InstanceState::Vmm, Some(VmmState::Failed)) => {
                instance::InstanceState::Stopping
            }
            // - An instance with a "saga unwound" VMM, on the other hand, can
            //   be treated as "failed", since --- unlike an instance with a
            //   "failed" active VMM --- a new start saga can run at any time by
            //   just clearing out the old VMM ID.
            (InstanceState::Vmm, Some(VmmState::SagaUnwound)) => {
                instance::InstanceState::Failed
            }
            // - If the instance has a VMM but the VMM is "creating", return
            //   `InstanceState::Starting`, rather than
            //   `InstanceState::Creating`.
            //   If we are still creating the VMM, this is because we are
            //   attempting to *start* the instance; instances may be created
            //   without creating a VMM to run them, and then started later.
            (
                InstanceState::Vmm,
                Some(VmmState::Creating | VmmState::Starting),
            ) => instance::InstanceState::Starting,
            (InstanceState::Vmm, Some(VmmState::Running)) => {
                instance::InstanceState::Running
            }
            (InstanceState::Vmm, Some(VmmState::Stopping)) => {
                instance::InstanceState::Stopping
            }
            (InstanceState::Vmm, Some(VmmState::Rebooting)) => {
                instance::InstanceState::Rebooting
            }
            (InstanceState::Vmm, Some(VmmState::Migrating)) => {
                instance::InstanceState::Migrating
            }
            // - An instance with no VMM is always "stopped" (as long as it's
            //   not "starting" etc.)
            (InstanceState::NoVmm, None) => instance::InstanceState::Stopped,
            // - The instance should not be in `InstanceState::Vmm` if there is
            //   no active VMM record, this is probably a bug, but return
            //   `InstanceState::Stopped`, because that's basically true
            //   regardless.
            (InstanceState::Vmm, None) => {
                debug_assert!(
                    false,
                    "if the instance state is `InstanceState::Vmm`, there \
                     should be a VMM state"
                );
                instance::InstanceState::Stopped
            }
            (InstanceState::NoVmm, _vmm_state) => {
                debug_assert_eq!(
                    _vmm_state, None,
                    "if the instance is in `InstanceState::NoVmm`, there \
                     should be no VMM state"
                );
                instance::InstanceState::Stopped
            }
            // - If there's no VMM state, use the instance's state.
            (instance_state, None) => instance_state.into(),
            (instance_state, _vmm_state) => {
                debug_assert_eq!(
                    _vmm_state, None,
                    "if the instance state is not `InstanceState::Vmm`, \
                     there should be no VMM state"
                );
                instance_state.into()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_terminal_api_states_are_terminal_db_states() {
        for &api_state in sled_agent_types::instance::VmmState::TERMINAL_STATES
        {
            let db_state = VmmState::from(api_state);
            assert!(
                db_state.is_terminal(),
                "API VmmState::{api_state:?} is considered terminal, but its \
                 corresponding DB state ({db_state:?}) is not!"
            );
        }
    }

    #[test]
    fn test_from_str_roundtrips() {
        for &variant in VmmState::ALL_STATES {
            assert_eq!(Ok(dbg!(variant)), dbg!(variant.to_string().parse()));
        }
    }

    #[test]
    fn test_terminal_states_consistent() {
        for &state in VmmState::ALL_STATES {
            assert_eq!(
                VmmState::TERMINAL_STATES.contains(&state),
                state.is_terminal(),
                "inconsistency between nexus_db_model::VmmState::{state:?} \
                and nexus_types::instance::VmmState::{state:?}: if the \
                is_terminal() method in nexus_types returns true, the state \
                should be in TERMINAL_STATES, and vice versa",
            );
        }
    }

    #[test]
    fn test_destroyable_states_consistent() {
        for &state in VmmState::ALL_STATES {
            assert_eq!(
                VmmState::DESTROYABLE_STATES.contains(&state),
                state.is_destroyable(),
                "inconsistency between nexus_db_model::VmmState::{state:?} \
                and nexus_types::instance::VmmState::{state:?}: if the \
                is_destroyable() method in nexus_types returns true, the state \
                should be in DESTROYABLE_STATES, and vice versa",
            );
        }
    }

    #[test]
    fn test_nonexistent_states_consistent() {
        for &state in VmmState::ALL_STATES {
            assert_eq!(
                VmmState::NONEXISTENT_STATES.contains(&state),
                !state.exists_on_sled(),
                "inconsistency between nexus_db_model::VmmState::{state:?} \
                and nexus_types::instance::VmmState::{state:?}: if the \
                exists_on_sled() method in nexus_types returns false, the \
                state should be in NONEXISTENT_STATES, and vice versa",
            );
        }
    }
}
