// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Blueprint planner resource allocation

use super::SledEditor;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::ExternalIpPolicy;

mod external_networking;

pub use self::external_networking::ExternalNetworkingAllocator;
pub use self::external_networking::ExternalNetworkingChoice;
pub use self::external_networking::ExternalNetworkingError;
pub use self::external_networking::ExternalSnatNetworkingChoice;

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
        I: Iterator<Item = &'a SledEditor>,
    {
        let external_networking = ExternalNetworkingAllocator::new(
            all_sleds.flat_map(|editor| {
                editor.zones(BlueprintZoneDisposition::is_in_service)
            }),
            external_ip_policy,
        )
        .map_err(BlueprintResourceAllocatorInputError::ExternalNetworking)?;

        Ok(Self { external_networking })
    }

    pub(crate) fn next_external_ip_nexus(
        &mut self,
    ) -> Result<ExternalNetworkingChoice, ExternalNetworkingError> {
        self.external_networking.for_new_nexus()
    }
}
