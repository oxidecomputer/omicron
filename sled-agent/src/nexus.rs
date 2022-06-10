// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#[cfg(test)]
pub use crate::mocks::MockNexusClient as NexusClient;
#[cfg(not(test))]
pub use nexus_client::Client as NexusClient;

use internal_dns_client::names::SRV;
use omicron_common::address::{
    AZ_PREFIX, NEXUS_INTERNAL_PORT, Ipv6Subnet,
};
use slog::Logger;
use std::net::Ipv6Addr;
use std::sync::Arc;

struct Inner {
    log: Logger,
    addr: Ipv6Addr,

    // TODO: We could also totally cache the resolver / observed IP here?
}

#[derive(Clone)]
pub struct LazyNexusClient {
    inner: Arc<Inner>,
}

impl LazyNexusClient {
    pub fn new(log: Logger, addr: Ipv6Addr) -> Self {
        Self {
            inner: Arc::new(
                Inner {
                    log,
                    addr,
                }
            )
        }
    }

    pub async fn get(&self) -> Result<NexusClient, String> {
        // TODO: Consider refactoring this:
        // - Address as input
        // - Lookup "nexus" DNS record
        // - Result<Address> as output
        let az_subnet = Ipv6Subnet::<AZ_PREFIX>::new(self.inner.addr);
        let resolver =
            internal_dns_client::multiclient::create_resolver(az_subnet)
                .map_err(|e| format!("Failed to create DNS resolver: {}", e))?;
        let response = resolver
            .lookup_ip(&SRV::Service("nexus".to_string()).to_string())
            .await
            .map_err(|e| format!("Failed to lookup Nexus IP: {}", e))?;
        let address = response.iter().next().ok_or_else(|| {
            "no addresses returned from DNS resolver".to_string()
        })?;

        Ok(NexusClient::new(
            &format!("http://[{}]:{}", address, NEXUS_INTERNAL_PORT),
            self.inner.log.clone(),
        ))
    }
}
