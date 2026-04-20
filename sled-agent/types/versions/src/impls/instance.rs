// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest::instance::{
    MigrationState, Migrations, SledVmmState, VmmSpec, VmmState,
    VmmStateRequested,
};
use propolis_api_types::instance_spec::{
    Component, SpecKey,
    components::backends::{
        CrucibleStorageBackend, FileStorageBackend, VirtioNetworkBackend,
    },
};

impl VmmState {
    /// States in which the VMM no longer exists and must be cleaned up.
    pub const TERMINAL_STATES: &'static [Self] =
        &[Self::Failed, Self::Destroyed];

    /// Returns `true` if this VMM is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        Self::TERMINAL_STATES.contains(self)
    }
}

impl std::fmt::Display for VmmStateRequested {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

impl VmmStateRequested {
    fn label(&self) -> &str {
        match self {
            VmmStateRequested::MigrationTarget(_) => "migrating in",
            VmmStateRequested::Running => "running",
            VmmStateRequested::Stopped => "stopped",
            VmmStateRequested::Reboot => "reboot",
        }
    }
}

impl VmmSpec {
    pub fn crucible_backends(
        &self,
    ) -> impl Iterator<Item = (&SpecKey, &CrucibleStorageBackend)> {
        self.0.components.iter().filter_map(
            |(key, component)| match component {
                Component::CrucibleStorageBackend(be) => Some((key, be)),
                _ => None,
            },
        )
    }

    pub fn viona_backends(
        &self,
    ) -> impl Iterator<Item = (&SpecKey, &VirtioNetworkBackend)> {
        self.0.components.iter().filter_map(
            |(key, component)| match component {
                Component::VirtioNetworkBackend(be) => Some((key, be)),
                _ => None,
            },
        )
    }

    pub fn file_backends(
        &self,
    ) -> impl Iterator<Item = (&SpecKey, &FileStorageBackend)> {
        self.0.components.iter().filter_map(
            |(key, component)| match component {
                Component::FileStorageBackend(be) => Some((key, be)),
                _ => None,
            },
        )
    }
}

impl VmmStateRequested {
    /// Returns true if the state represents a stopped Instance.
    pub fn is_stopped(&self) -> bool {
        match self {
            VmmStateRequested::MigrationTarget(_) => false,
            VmmStateRequested::Running => false,
            VmmStateRequested::Stopped => true,
            VmmStateRequested::Reboot => false,
        }
    }
}

impl Migrations<'_> {
    pub fn empty() -> Self {
        Self { migration_in: None, migration_out: None }
    }
}

impl SledVmmState {
    pub fn migrations(&self) -> Migrations<'_> {
        Migrations {
            migration_in: self.migration_in.as_ref(),
            migration_out: self.migration_out.as_ref(),
        }
    }
}

impl MigrationState {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
    /// Returns `true` if this migration state means that the migration is no
    /// longer in progress (it has either succeeded or failed).
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, MigrationState::Completed | MigrationState::Failed)
    }
}

impl std::fmt::Display for MigrationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}
