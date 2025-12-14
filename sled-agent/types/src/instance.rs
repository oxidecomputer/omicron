// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common instance-related types.

use propolis_api_types::instance_spec::{
    SpecKey,
    components::backends::{
        CrucibleStorageBackend, FileStorageBackend, VirtioNetworkBackend,
    },
    v0::ComponentV0,
};

pub use sled_agent_types_versions::latest::instance::*;

/// Extension trait for [`VmmSpec`] to provide helper methods.
pub trait VmmSpecExt {
    fn crucible_backends(
        &self,
    ) -> impl Iterator<Item = (&SpecKey, &CrucibleStorageBackend)>;

    fn viona_backends(
        &self,
    ) -> impl Iterator<Item = (&SpecKey, &VirtioNetworkBackend)>;

    fn file_backends(
        &self,
    ) -> impl Iterator<Item = (&SpecKey, &FileStorageBackend)>;
}

impl VmmSpecExt for VmmSpec {
    fn crucible_backends(
        &self,
    ) -> impl Iterator<Item = (&SpecKey, &CrucibleStorageBackend)> {
        self.0.components.iter().filter_map(
            |(key, component)| match component {
                ComponentV0::CrucibleStorageBackend(be) => Some((key, be)),
                _ => None,
            },
        )
    }

    fn viona_backends(
        &self,
    ) -> impl Iterator<Item = (&SpecKey, &VirtioNetworkBackend)> {
        self.0.components.iter().filter_map(
            |(key, component)| match component {
                ComponentV0::VirtioNetworkBackend(be) => Some((key, be)),
                _ => None,
            },
        )
    }

    fn file_backends(
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

/// Extension trait for [`VmmStateRequested`].
pub trait VmmStateRequestedExt {
    /// Returns true if the state represents a stopped Instance.
    fn is_stopped(&self) -> bool;
}

impl VmmStateRequestedExt for VmmStateRequested {
    fn is_stopped(&self) -> bool {
        match self {
            VmmStateRequested::MigrationTarget(_) => false,
            VmmStateRequested::Running => false,
            VmmStateRequested::Stopped => true,
            VmmStateRequested::Reboot => false,
        }
    }
}
