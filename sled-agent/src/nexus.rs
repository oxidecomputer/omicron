// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#[cfg(test)]
pub use crate::mocks::MockNexusClient as NexusClient;
#[cfg(not(test))]
pub use nexus_client::Client as NexusClient;

use internal_dns_client::names::{ServiceName, SRV};
use omicron_common::address::{Ipv6Subnet, AZ_PREFIX, NEXUS_INTERNAL_PORT};
use slog::Logger;
use std::net::Ipv6Addr;
use std::sync::Arc;

struct Inner {
    log: Logger,
    addr: Ipv6Addr,
    // TODO: We could also totally cache the resolver / observed IP here?
}

/// Wrapper around a [`NexusClient`] object, which allows deferring
/// the DNS lookup until accessed.
///
/// Without the assistance of OS-level DNS lookups, the [`NexusClient`]
/// interface requires knowledge of the target service IP address.
/// For some services, like Nexus, this can be painful, as the IP address
/// may not have even been allocated when the Sled Agent starts.
///
/// This structure allows clients to access the client on-demand, performing
/// the DNS lookup only once it is actually needed.
#[derive(Clone)]
pub struct LazyNexusClient {
    inner: Arc<Inner>,
}

impl LazyNexusClient {
    pub fn new(log: Logger, addr: Ipv6Addr) -> Self {
        Self { inner: Arc::new(Inner { log, addr }) }
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
            .lookup_ip(&SRV::Service(ServiceName::Nexus).to_string())
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

// Provides a mock implementation of the [`LazyNexusClient`].
//
// This allows tests to use the structure without actually performing
// any DNS lookups.
#[cfg(test)]
mockall::mock! {
    pub LazyNexusClient {
        pub fn new(log: Logger, addr: Ipv6Addr) -> Self;
        pub async fn get(&self) -> Result<NexusClient, String>;
    }
    impl Clone for LazyNexusClient {
        fn clone(&self) -> Self;
    }
}
