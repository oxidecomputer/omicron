// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#[cfg(test)]
pub use crate::mocks::MockNexusClient as NexusClient;
#[cfg(not(test))]
pub use nexus_client::Client as NexusClient;

use internal_dns_client::{
    multiclient::{ResolveError, Resolver},
    names::{ServiceName, SRV},
};
use omicron_common::address::NEXUS_INTERNAL_PORT;
use slog::Logger;
use std::net::Ipv6Addr;
use std::sync::Arc;

struct Inner {
    log: Logger,
    resolver: Resolver,
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
    pub fn new(log: Logger, addr: Ipv6Addr) -> Result<Self, ResolveError> {
        Ok(Self {
            inner: Arc::new(Inner {
                log,
                resolver: Resolver::new_from_ip(addr)?,
            }),
        })
    }

    pub async fn get(&self) -> Result<NexusClient, ResolveError> {
        let address = self
            .inner
            .resolver
            .lookup_ipv6(SRV::Service(ServiceName::Nexus))
            .await?;

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
        pub fn new(log: Logger, addr: Ipv6Addr) -> Result<Self, ResolveError>;
        pub async fn get(&self) -> Result<NexusClient, String>;
    }
    impl Clone for LazyNexusClient {
        fn clone(&self) -> Self;
    }
}
