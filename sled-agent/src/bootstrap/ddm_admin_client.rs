// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Client to ddmd (the maghemite service running on localhost).

use ddm_admin_client::types::Ipv6Prefix;
use ddm_admin_client::Client;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::backoff::internal_service_policy;
use omicron_common::backoff::retry_notify;
use slog::Logger;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use thiserror::Error;

use crate::bootstrap::agent::BOOTSTRAP_MASK;
use crate::bootstrap::agent::BOOTSTRAP_PREFIX;

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
    /// Creates a new [`DdmAdminClient`].
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

    /// Spawns a background task to instruct ddmd to advertise the given prefix
    /// to peer sleds.
    pub fn advertise_prefix(&self, address: Ipv6Subnet<SLED_PREFIX>) {
        let me = self.clone();
        tokio::spawn(async move {
            let prefix =
                Ipv6Prefix { addr: address.net().network(), mask: SLED_PREFIX };
            retry_notify(internal_service_policy(), || async {
                info!(
                    me.log, "Sending prefix to ddmd for advertisement";
                    "prefix" => ?prefix,
                );

                // TODO-cleanup Why does the generated openapi client require a
                // `&Vec` instead of a `&[]`?
                let prefixes = vec![prefix];
                me.client.advertise_prefixes(&prefixes).await?;
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
    pub async fn peer_addrs(
        &self,
    ) -> Result<impl Iterator<Item = Ipv6Addr> + '_, DdmError> {
        let prefixes = self.client.get_prefixes().await?.into_inner();
        info!(self.log, "Received prefixes from ddmd"; "prefixes" => ?prefixes);
        Ok(prefixes.into_iter().filter_map(|(_, prefixes)| {
            // If we receive multiple bootstrap prefixes from one peer, trim it
            // down to just one. Connections on the bootstrap network are always
            // authenticated via sprockets, which only needs one address.
            prefixes.into_iter().find_map(|prefix| {
                let mut segments = prefix.addr.segments();
                if prefix.mask == BOOTSTRAP_MASK
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
