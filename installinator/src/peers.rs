// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    collections::BTreeSet,
    fmt,
    net::{AddrParseError, IpAddr, SocketAddr},
    str::FromStr,
};

use anyhow::{Result, bail};
use itertools::Itertools;
use omicron_common::address::BOOTSTRAP_ARTIFACT_PORT;
use omicron_ddm_admin_client::Client as DdmAdminClient;
use sled_hardware_types::underlay::BootstrapInterface;

use crate::errors::DiscoverPeersError;

/// A chosen discovery mechanism for peers, passed in over the command line.
#[derive(Clone, Debug)]
pub(crate) enum DiscoveryMechanism {
    /// The default discovery mechanism: hit the bootstrap network.
    Bootstrap,

    /// A list of peers is manually specified.
    List(Vec<PeerAddress>),
}

impl DiscoveryMechanism {
    /// Discover peers.
    pub(crate) async fn discover_peers(
        &self,
        log: &slog::Logger,
    ) -> Result<PeerAddresses, DiscoverPeersError> {
        let peers = match self {
            Self::Bootstrap => {
                // Note: we do not abort this process and instead keep retrying
                // forever. This attempts to ensure that we'll eventually find
                // peers.
                let ddm_admin_client =
                    DdmAdminClient::localhost(log).map_err(|err| {
                        DiscoverPeersError::Retry(anyhow::anyhow!(err))
                    })?;
                // We want to find both sled-agent (global zone) and wicketd
                // (switch zone) peers.
                let addrs = ddm_admin_client
                    .derive_bootstrap_addrs_from_prefixes(&[
                        BootstrapInterface::GlobalZone,
                        BootstrapInterface::SwitchZone,
                    ])
                    .await
                    .map_err(|err| {
                        DiscoverPeersError::Retry(anyhow::anyhow!(err))
                    })?;
                addrs
                    .map(|addr| {
                        PeerAddress::new(SocketAddr::new(
                            IpAddr::V6(addr),
                            BOOTSTRAP_ARTIFACT_PORT,
                        ))
                    })
                    .collect()
            }
            Self::List(peers) => peers.iter().copied().collect(),
        };

        Ok(peers)
    }
}

impl fmt::Display for DiscoveryMechanism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bootstrap => write!(f, "bootstrap"),
            Self::List(peers) => {
                write!(f, "list:{}", peers.iter().join(","))
            }
        }
    }
}

impl FromStr for DiscoveryMechanism {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "bootstrap" {
            Ok(Self::Bootstrap)
        } else if let Some(peers) = s.strip_prefix("list:") {
            let peers = peers
                .split(',')
                .map(|s| s.parse())
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Self::List(peers))
        } else {
            bail!(
                "invalid discovery mechanism (expected \"bootstrap\" or \"list:[::1]:8000\"): {}",
                s
            );
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PeerAddresses {
    peers: BTreeSet<PeerAddress>,
}

impl PeerAddresses {
    pub(crate) fn peers(&self) -> &BTreeSet<PeerAddress> {
        &self.peers
    }

    pub(crate) fn len(&self) -> usize {
        self.peers.len()
    }

    pub(crate) fn display(&self) -> impl fmt::Display + use<> {
        self.peers().iter().join(", ")
    }
}

impl FromIterator<PeerAddress> for PeerAddresses {
    fn from_iter<I: IntoIterator<Item = PeerAddress>>(iter: I) -> Self {
        let peers = iter.into_iter().collect::<BTreeSet<_>>();
        Self { peers }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
pub(crate) struct PeerAddress {
    address: SocketAddr,
}

impl PeerAddress {
    pub(crate) const fn new(address: SocketAddr) -> Self {
        Self { address }
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }
}

impl fmt::Display for PeerAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.address.fmt(f)
    }
}

impl FromStr for PeerAddress {
    type Err = AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let address = s.parse()?;
        Ok(Self { address })
    }
}
