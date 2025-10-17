// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Blueprint planner resource allocation

use std::net::IpAddr;

use super::SledEditor;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::ExternalIpPolicy;

mod external_networking;

pub use self::external_networking::ExternalNetworkingError;

pub(crate) use self::external_networking::ExternalNetworkingChoice;
pub(crate) use self::external_networking::ExternalSnatNetworkingChoice;

use self::external_networking::ExternalNetworkingAllocator;

#[derive(Debug, thiserror::Error)]
pub enum BlueprintResourceAllocatorInputError {
    #[error("failed to create external networking allocator")]
    ExternalNetworking(#[source] anyhow::Error),
}

#[derive(Debug)]
pub(crate) struct BlueprintResourceAllocator {
    external_networking: ExternalNetworkingAllocator,
}

impl BlueprintResourceAllocator {
    pub fn new<'a, I>(
        all_sleds: I,
        external_ip_policy: &ExternalIpPolicy,
    ) -> Result<Self, BlueprintResourceAllocatorInputError>
    where
        I: Iterator<Item = &'a SledEditor> + Clone,
    {
        let external_networking = ExternalNetworkingAllocator::new(
            all_sleds.clone().flat_map(|editor| {
                editor.zones(BlueprintZoneDisposition::is_in_service)
            }),
            all_sleds.flat_map(|editor| {
                editor.zones(BlueprintZoneDisposition::is_expunged)
            }),
            external_ip_policy.service_ip_pool_ranges().to_vec(),
        )
        .map_err(BlueprintResourceAllocatorInputError::ExternalNetworking)?;

        Ok(Self { external_networking })
    }

    pub(crate) fn next_external_ip_nexus(
        &mut self,
    ) -> Result<ExternalNetworkingChoice, ExternalNetworkingError> {
        self.external_networking.for_new_nexus()
    }

    pub(crate) fn next_external_ip_external_dns(
        &mut self,
    ) -> Result<ExternalNetworkingChoice, ExternalNetworkingError> {
        self.external_networking.for_new_external_dns()
    }

    pub(crate) fn next_external_ip_boundary_ntp(
        &mut self,
    ) -> Result<ExternalSnatNetworkingChoice, ExternalNetworkingError> {
        self.external_networking.for_new_boundary_ntp()
    }

    /// Allow a test to manually add an external DNS address, which could
    /// ordinarily only come from RSS.
    ///
    /// TODO-cleanup: Remove when external DNS addresses are in the policy.
    // This can't be `#[cfg(test)]` because it's used by the `ExampleSystem`
    // helper (which itself is used by reconfigurator-cli and friends). We give
    // it a scary name instead.
    pub(crate) fn inject_untracked_external_dns_ip(
        &mut self,
        ip: IpAddr,
    ) -> Result<(), ExternalNetworkingError> {
        self.external_networking.add_external_dns_ip(ip)
    }
}
