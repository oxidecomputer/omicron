// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! User provided dropshot server context

use crate::bootstrap_addrs::BootstrapPeers;
use crate::rss_config::CurrentRssConfig;
use crate::update_tracker::UpdateTracker;
use crate::MgsHandle;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use sled_hardware::Baseboard;
use slog::info;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::Mutex;

/// Shared state used by API handlers
pub struct ServerContext {
    pub mgs_handle: MgsHandle,
    pub mgs_client: gateway_client::Client,
    pub(crate) log: slog::Logger,
    pub(crate) bootstrap_peers: BootstrapPeers,
    pub(crate) update_tracker: Arc<UpdateTracker>,
    pub(crate) baseboard: Option<Baseboard>,
    pub(crate) rss_config: Mutex<CurrentRssConfig>,
}

impl ServerContext {
    pub(crate) fn bootstrap_agent_addr(&self) -> Result<SocketAddrV6> {
        // Port on which the bootstrap agent dropshot server within sled-agent
        // is listening.
        const BOOTSTRAP_AGENT_HTTP_PORT: u16 = 80;

        let ip = self.bootstrap_agent_ip()?;
        Ok(SocketAddrV6::new(ip, BOOTSTRAP_AGENT_HTTP_PORT, 0, 0))
    }

    fn bootstrap_agent_ip(&self) -> Result<Ipv6Addr> {
        let mut any_bootstrap_peer = None;
        for (baseboard, ip) in self.bootstrap_peers.sleds() {
            if self.baseboard.as_ref() == Some(&baseboard) {
                return Ok(ip);
            }
            any_bootstrap_peer = Some((baseboard, ip));
        }

        // If we get past the loop above, we did not find a match for our
        // baseboard in our list of peers. If we know our own baseboard, this is
        // an error: we didn't find ourself. If we don't know our own
        // baseboard, we can pick any IP.
        if let Some(baseboard) = self.baseboard.as_ref() {
            bail!("IP address not known for our own sled ({baseboard:?})");
        } else {
            let (baseboard, ip) = any_bootstrap_peer
                .ok_or_else(|| anyhow!("no bootstrap agent peers found"))?;
            info!(
                self.log,
                "Baseboard unknown; choosing arbitrary bootstrap peer as 'our' sled-agent";
                "peer_baseboard" => ?baseboard,
                "peer_ip" => %ip,
            );
            Ok(ip)
        }
    }
}
