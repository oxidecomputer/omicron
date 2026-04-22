// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest::instance::ExternalIpConfig;
use crate::latest::instance::ExternalIpv4Config;
use crate::latest::instance::ExternalIpv6Config;
use crate::latest::instance::MigrationState;
use crate::latest::instance::Migrations;
use crate::latest::instance::SledVmmState;
use crate::latest::instance::VmmSpec;
use crate::latest::instance::VmmState;
use crate::latest::instance::VmmStateRequested;
use crate::latest::inventory::SourceNatConfig;
use propolis_api_types::instance_spec::{
    Component, SpecKey,
    components::backends::{
        CrucibleStorageBackend, FileStorageBackend, VirtioNetworkBackend,
    },
};
use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;

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

impl From<VmmState> for omicron_common::api::external::InstanceState {
    fn from(state: VmmState) -> Self {
        match state {
            VmmState::Starting => Self::Starting,
            VmmState::Running => Self::Running,
            VmmState::Stopping => Self::Stopping,
            VmmState::Stopped => Self::Stopped,
            VmmState::Rebooting => Self::Rebooting,
            VmmState::Migrating => Self::Migrating,
            VmmState::Failed => Self::Failed,
            VmmState::Destroyed => Self::Destroyed,
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

impl ExternalIpConfig {
    /// Create a new configuration with only a Floating IPv4 address.
    pub fn new_floating_ipv4(ipv4: Ipv4Addr) -> Self {
        Self {
            v4: Some(ExternalIpv4Config {
                floating_ips: BTreeSet::from([ipv4]),
                ..Default::default()
            }),
            v6: None,
        }
    }

    /// Create a new configuration with only an IPv4 SNAT address.
    pub fn new_ipv4_source_nat(snat: SourceNatConfig<Ipv4Addr>) -> Self {
        Self {
            v4: Some(ExternalIpv4Config {
                source_nat: Some(snat),
                ..Default::default()
            }),
            v6: None,
        }
    }

    /// Create a new configuration with only a Floating IPv6 address.
    pub fn new_floating_ipv6(ipv6: Ipv6Addr) -> Self {
        Self {
            v6: Some(ExternalIpv6Config {
                floating_ips: BTreeSet::from([ipv6]),
                ..Default::default()
            }),
            v4: None,
        }
    }

    /// Create a new configuration with only an IPv6 SNAT address.
    pub fn new_ipv6_source_nat(snat: SourceNatConfig<Ipv6Addr>) -> Self {
        Self {
            v6: Some(ExternalIpv6Config {
                source_nat: Some(snat),
                ..Default::default()
            }),
            v4: None,
        }
    }
}
