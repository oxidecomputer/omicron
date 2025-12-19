// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest::instance::{VmmSpec, VmmStateRequested};
use propolis_api_types::instance_spec::{
    SpecKey,
    components::backends::{
        CrucibleStorageBackend, FileStorageBackend, VirtioNetworkBackend,
    },
    v0::ComponentV0,
};

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
                ComponentV0::CrucibleStorageBackend(be) => Some((key, be)),
                _ => None,
            },
        )
    }

    pub fn viona_backends(
        &self,
    ) -> impl Iterator<Item = (&SpecKey, &VirtioNetworkBackend)> {
        self.0.components.iter().filter_map(
            |(key, component)| match component {
                ComponentV0::VirtioNetworkBackend(be) => Some((key, be)),
                _ => None,
            },
        )
    }

    pub fn file_backends(
        &self,
    ) -> impl Iterator<Item = (&SpecKey, &FileStorageBackend)> {
        self.0.components.iter().filter_map(
            |(key, component)| match component {
                ComponentV0::FileStorageBackend(be) => Some((key, be)),
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
