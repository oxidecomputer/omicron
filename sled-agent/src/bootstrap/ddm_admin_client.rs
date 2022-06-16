// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Client to ddmd (the maghemite service running on localhost).

use ddm_admin_client::Client;
use slog::Logger;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::net::SocketAddr;
use thiserror::Error;

// TODO-cleanup Is it okay to hardcode this port number and assume ddmd is bound
// to `::1`, or should we move that into our config?
const DDMD_PORT: u16 = 8000;

#[derive(Debug, Error)]
pub enum DdmError {
    #[error("Failed to construct an HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("Failed making HTTP request to ddmd: {0}")]
    DdmdApi(#[from] ddm_admin_client::Error<ddm_admin_client::types::Error>),
}

/// Manages Sled Discovery - both our announcement to other Sleds,
/// as well as our discovery of those sleds.
#[derive(Clone)]
pub struct DdmAdminClient {
    client: Client,
    log: Logger,
}

impl DdmAdminClient {
    /// Creates a new [`PeerMonitor`].
    pub fn new(log: Logger) -> Result<Self, DdmError> {
        let dur = std::time::Duration::from_secs(60);
        let ddmd_addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, DDMD_PORT, 0, 0);

        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()?;
        let client = Client::new_with_client(
            &format!("http://{ddmd_addr}"),
            client,
            log.new(o!("DdmAdminClient" => SocketAddr::V6(ddmd_addr))),
        );
        Ok(DdmAdminClient { client, log })
    }

    /// Returns the addresses of connected sleds.
    ///
    /// Note: These sleds have not yet been verified.
    pub async fn peer_addrs(
        &self,
    ) -> Result<impl Iterator<Item = Ipv6Addr> + '_, DdmError> {
        let prefixes = self.client.get_prefixes().await?.into_inner();
        info!(self.log, "Received prefixes from ddmd"; "prefixes" => ?prefixes);
        Ok(prefixes.into_iter().filter_map(|(_, prefixes)| {
            // TODO-correctness What if a single peer is advertising multiple
            // bootstrap network prefixes? This will only grab the first. We
            // could use `flat_map` instead of `filter_map`, but then our caller
            // wouldn't be able to tell "one peer with 3 prefixes" apart from
            // "three peers with 1 prefix each".
            prefixes.into_iter().find_map(|prefix| {
                let mut segments = prefix.addr.segments();
                // TODO GROSS
                if segments[0] == 0xfdb0 {
                    segments[7] = 1;
                    Some(Ipv6Addr::from(segments))
                } else {
                    None
                }
            })
        }))
    }
}
