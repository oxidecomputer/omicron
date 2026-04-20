// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest::instance::ExternalIpConfig;
use crate::latest::instance::ExternalIpv4Config;
use crate::latest::instance::ExternalIpv6Config;
use crate::latest::instance::VmmSpec;
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
