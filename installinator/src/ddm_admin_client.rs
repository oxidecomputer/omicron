// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This file is mostly copied from sled-agent. It's a TODO to unify them.

use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

use ddm_admin_client::Client;
use thiserror::Error;

/// Initial octet of IPv6 for bootstrap addresses.
pub(crate) const BOOTSTRAP_PREFIX: u16 = 0xfdb0;

/// IPv6 prefix mask for bootstrap addresses.
pub(crate) const BOOTSTRAP_MASK: u8 = 64;

const DDMD_PORT: u16 = 8000;

#[derive(Debug, Error)]
pub enum DdmError {
    #[error("Failed to construct an HTTP client")]
    HttpClient(#[from] reqwest::Error),

    #[error("Failed making HTTP request to ddmd")]
    DdmdApi(#[from] ddm_admin_client::Error<ddm_admin_client::types::Error>),
}

/// Manages Sled Discovery.
#[derive(Clone, Debug)]
pub struct DdmAdminClient {
    client: Client,
    log: slog::Logger,
}

impl DdmAdminClient {
    /// Creates a new [`DdmAdminClient`].
    pub fn new(log: &slog::Logger) -> Result<Self, DdmError> {
        let dur = std::time::Duration::from_secs(60);
        let ddmd_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, DDMD_PORT, 0, 0);

        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()?;
        let client = Client::new_with_client(
            &format!("http://{ddmd_addr}"),
            client,
            log.new(slog::o!("DdmAdminClient" => SocketAddr::V6(ddmd_addr))),
        );
        let log = log.new(slog::o!("component" => "DdmAdminClient"));
        Ok(DdmAdminClient { client, log })
    }

    /// Returns the addresses of connected sleds.
    ///
    /// Note: These sleds have not yet been verified.
    pub async fn peer_addrs(
        &self,
    ) -> Result<impl Iterator<Item = Ipv6Addr> + '_, DdmError> {
        let prefixes = self.client.get_prefixes().await?.into_inner();
        slog::info!(self.log, "Received prefixes from ddmd"; "prefixes" => ?prefixes);

        Ok(prefixes.into_iter().filter_map(|(_, prefixes)| {
            // If we receive multiple bootstrap prefixes from one peer, trim it
            // down to just one. Connections on the bootstrap network are always
            // authenticated via sprockets, which only needs one address.
            prefixes.into_iter().find_map(|prefix| {
                let mut segments = prefix.addr.segments();
                if prefix.len == BOOTSTRAP_MASK
                    && segments[0] == BOOTSTRAP_PREFIX
                {
                    // Bootstrap agent IPs always end in ::1; convert the
                    // `BOOTSTRAP_PREFIX::*/BOOTSTRAP_PREFIX` address we
                    // received into that specific address.
                    segments[7] = 1;
                    Some(Ipv6Addr::from(segments))
                } else {
                    None
                }
            })
        }))
    }
}
