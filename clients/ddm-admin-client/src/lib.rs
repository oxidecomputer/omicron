// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

#![allow(clippy::redundant_closure_call)]
#![allow(clippy::needless_lifetimes)]
#![allow(clippy::match_single_binding)]
#![allow(clippy::clone_on_copy)]
pub use ddm_admin_client::Error;
pub use ddm_admin_client::types;

use ddm_admin_client::Client as InnerClient;
use either::Either;
use oxnet::Ipv6Net;
use sled_hardware_types::underlay::BOOTSTRAP_MASK;
use sled_hardware_types::underlay::BOOTSTRAP_PREFIX;
use sled_hardware_types::underlay::BootstrapInterface;
use slog::Logger;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use thiserror::Error;

use crate::types::EnableStatsRequest;

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

    pub fn new(
        log: &Logger,
        ddmd_addr: SocketAddrV6,
    ) -> Result<Self, DdmError> {
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

    pub fn log(&self) -> &Logger {
        &self.log
    }

    pub async fn advertise_prefixes(
        &self,
        prefixes: &Vec<Ipv6Net>,
    ) -> Result<(), Error<types::Error>> {
        self.inner
            .advertise_prefixes(prefixes)
            .await
            .map(|resp| resp.into_inner())
    }

    pub async fn withdraw_prefixes(
        &self,
        prefixes: &Vec<Ipv6Net>,
    ) -> Result<(), Error<types::Error>> {
        self.inner
            .withdraw_prefixes(prefixes)
            .await
            .map(|resp| resp.into_inner())
    }

    pub async fn get_originated(
        &self,
    ) -> Result<Vec<Ipv6Net>, Error<types::Error>> {
        self.inner.get_originated().await.map(|resp| resp.into_inner())
    }

    pub async fn enable_stats(
        &self,
        request: &EnableStatsRequest,
    ) -> Result<(), Error<types::Error>> {
        self.inner.enable_stats(request).await.map(|resp| resp.into_inner())
    }

    /// Returns the addresses of connected sleds.
    ///
    /// Note: These sleds have not yet been verified.
    pub async fn derive_bootstrap_addrs_from_prefixes<'a>(
        &self,
        interfaces: &'a [BootstrapInterface],
    ) -> Result<impl Iterator<Item = Ipv6Addr> + 'a + use<'a>, DdmError> {
        let prefixes = self.inner.get_prefixes().await?.into_inner();
        Ok(prefixes.into_iter().flat_map(|(_, prefixes)| {
            prefixes.into_iter().flat_map(|prefix| {
                let mut segments = prefix.destination.addr().segments();
                if prefix.destination.width() == BOOTSTRAP_MASK
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
