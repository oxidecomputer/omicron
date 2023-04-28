// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

#![allow(clippy::redundant_closure_call)]
#![allow(clippy::needless_lifetimes)]
#![allow(clippy::match_single_binding)]
#![allow(clippy::clone_on_copy)]

#[allow(dead_code)]
mod inner {
    include!(concat!(env!("OUT_DIR"), "/ddm-admin-client.rs"));

    impl Copy for types::Ipv6Prefix {}
}

pub use inner::types;
pub use inner::Error;

use either::Either;
use inner::types::Ipv6Prefix;
use inner::Client as InnerClient;
use omicron_common::address::get_switch_zone_address;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::backoff::retry_notify;
use omicron_common::backoff::retry_policy_internal_service_aggressive;
use sled_hardware::underlay::BootstrapInterface;
use sled_hardware::underlay::BOOTSTRAP_MASK;
use sled_hardware::underlay::BOOTSTRAP_PREFIX;
use slog::info;
use slog::Logger;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use thiserror::Error;

// TODO-cleanup Is it okay to hardcode this port number here?
const DDMD_PORT: u16 = 8000;

#[derive(Debug, Error)]
pub enum DdmError {
    #[error("Failed to construct an HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("Failed making HTTP request to ddmd: {0}")]
    DdmdApi(#[from] Error<types::Error>),
}

#[derive(Debug, Clone)]
pub struct Client {
    inner: InnerClient,
    log: Logger,
}

impl Client {
    /// Creates a new [`Client`] that points to localhost
    pub fn localhost(log: &Logger) -> Result<Self, DdmError> {
        Self::new(log, SocketAddrV6::new(Ipv6Addr::LOCALHOST, DDMD_PORT, 0, 0))
    }

    /// Creates a new [`Client`] that points to the switch zone in a
    /// sled subnet.
    pub fn switch_zone(
        log: &Logger,
        sled_subnet: Ipv6Subnet<SLED_PREFIX>,
    ) -> Result<Self, DdmError> {
        Self::new(
            log,
            SocketAddrV6::new(
                get_switch_zone_address(sled_subnet),
                DDMD_PORT,
                0,
                0,
            ),
        )
    }

    /// Creates a new [`Client`] that points to an IPv6 address
    pub fn address(log: &Logger, address: Ipv6Addr) -> Result<Self, DdmError> {
        Self::new(log, SocketAddrV6::new(address, DDMD_PORT, 0, 0))
    }

    fn new(log: &Logger, ddmd_addr: SocketAddrV6) -> Result<Self, DdmError> {
        let dur = std::time::Duration::from_secs(60);
        let log =
            log.new(slog::o!("DdmAdminClient" => SocketAddr::V6(ddmd_addr)));

        let inner = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()?;
        let inner = InnerClient::new_with_client(
            &format!("http://{ddmd_addr}"),
            inner,
            log.clone(),
        );
        Ok(Self { inner, log })
    }

    /// Spawns a background task to instruct ddmd to advertise the given prefix
    /// to peer sleds.
    pub fn advertise_prefix(&self, address: Ipv6Subnet<SLED_PREFIX>) {
        let me = self.clone();
        tokio::spawn(async move {
            let prefix =
                Ipv6Prefix { addr: address.net().network(), len: SLED_PREFIX };
            retry_notify(retry_policy_internal_service_aggressive(), || async {
                info!(
                    me.log, "Sending prefix to ddmd for advertisement";
                    "prefix" => ?prefix,
                );

                // TODO-cleanup Why does the generated openapi client require a
                // `&Vec` instead of a `&[]`?
                let prefixes = vec![prefix];
                me.inner.advertise_prefixes(&prefixes).await?;
                Ok(())
            }, |err, duration| {
                info!(
                    me.log,
                    "Failed to notify ddmd of our address (will retry after {duration:?}";
                    "err" => %err,
                );
            }).await.unwrap();
        });
    }

    /// Returns the addresses of connected sleds.
    ///
    /// Note: These sleds have not yet been verified.
    pub async fn peer_addrs<'a>(
        &self,
        interfaces: &'a [BootstrapInterface],
    ) -> Result<impl Iterator<Item = Ipv6Addr> + 'a, DdmError> {
        let prefixes = self.inner.get_prefixes().await?.into_inner();
        info!(self.log, "Received prefixes from ddmd"; "prefixes" => ?prefixes);
        Ok(prefixes.into_iter().flat_map(|(_, prefixes)| {
            prefixes.into_iter().flat_map(|prefix| {
                let mut segments = prefix.destination.addr.segments();
                if prefix.destination.len == BOOTSTRAP_MASK
                    && segments[0] == BOOTSTRAP_PREFIX
                {
                    Either::Left(interfaces.iter().map(move |interface| {
                        let id = interface.interface_id();
                        segments[4] = ((id >> 48) & 0xffff) as u16;
                        segments[5] = ((id >> 32) & 0xffff) as u16;
                        segments[6] = ((id >> 16) & 0xffff) as u16;
                        segments[7] = (id & 0xffff) as u16;
                        Ipv6Addr::from(segments)
                    }))
                } else {
                    Either::Right(std::iter::empty())
                }
            })
        }))
    }
}
