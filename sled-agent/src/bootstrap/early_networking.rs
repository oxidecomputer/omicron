// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Network setup required to bring up the control plane

use bootstore::schemes::v0 as bootstore;
use ddm_admin_client::{Client as DdmAdminClient, DdmError};
use dpd_client::types::Ipv6Entry;
use dpd_client::types::{
    LinkCreate, LinkId, LinkSettings, PortId, PortSettings, RouteSettingsV4,
};
use dpd_client::Client as DpdClient;
use dpd_client::Ipv4Cidr;
use gateway_client::Client as MgsClient;
use internal_dns::resolver::{ResolveError, Resolver as DnsResolver};
use internal_dns::ServiceName;
use omicron_common::address::{Ipv6Subnet, AZ_PREFIX, MGS_PORT};
use omicron_common::address::{DDMD_PORT, DENDRITE_PORT};
use omicron_common::api::internal::shared::{
    PortFec, PortSpeed, RackNetworkConfig, SwitchLocation, UplinkConfig,
};
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, BackoffError,
};
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv6Addr, SocketAddrV6};
use thiserror::Error;

static BOUNDARY_SERVICES_ADDR: &str = "fd00:99::1";

/// Errors that can occur during early network setup
#[derive(Error, Debug)]
pub enum EarlyNetworkSetupError {
    #[error("Bad configuration for setting up rack: {0}")]
    BadConfig(String),

    #[error("Error contacting ddmd: {0}")]
    DdmError(#[from] DdmError),

    #[error("Error during request to Dendrite: {0}")]
    Dendrite(String),

    #[error("Error during DNS lookup: {0}")]
    DnsResolver(#[from] ResolveError),
}

/// Code for configuring the necessary network bits to bring up the control
/// plane
pub struct EarlyNetworkSetup<'a> {
    log: &'a Logger,
}

impl<'a> EarlyNetworkSetup<'a> {
    pub fn new(log: &'a Logger) -> Self {
        EarlyNetworkSetup { log }
    }

    pub async fn lookup_boundary_switch_addrs(
        &self,
        resolver: &DnsResolver,
        config: &RackNetworkConfig,
    ) -> HashSet<Ipv6Addr> {
        let switch_zone_addrs = self.lookup_switch_zone_addrs(resolver).await;
        let mut boundary_switch_addrs = HashSet::new();

        for uplink_config in &config.uplinks {
            if let Some(&zone_addr) =
                switch_zone_addrs.get(&uplink_config.switch)
            {
                boundary_switch_addrs.insert(zone_addr);
            } else {
                // TODO-correctness is it okay to just return here if the config
                // specifies a switch that we don't have in DNS? This shouldn't
                // happen...
                warn!(
                    self.log,
                    "No switch zone address found for {:?}",
                    uplink_config.switch,
                );
            }
        }

        boundary_switch_addrs
    }

    pub async fn lookup_switch_zone_addrs(
        &self,
        resolver: &DnsResolver,
    ) -> HashMap<SwitchLocation, Ipv6Addr> {
        let lookup = || async {
            info!(self.log, "Finding switch zone addresses in DNS");
            resolver
                .lookup_all_ipv6(ServiceName::Dendrite)
                .await
                .map_err(BackoffError::transient)
        };
        let log_failure = |error, delay| {
            warn!(
                self.log,
                "failed to look up switch zone addresses in DNS";
                "error" => ?error,
                "retry_after" => ?delay,
            );
        };
        let switch_zone_addresses = retry_notify(
            retry_policy_internal_service_aggressive(),
            lookup,
            log_failure,
        )
        .await
        .expect(
            "Expected an infinite retry loop looking up switch zone addresses",
        );

        info!(
            self.log, "Detected switch zone addresses";
            "addresses" => #?switch_zone_addresses,
        );

        self.map_switch_zone_addrs(switch_zone_addresses).await
    }

    // TODO: #3601 Audit switch location discovery logic for robustness
    // in multi-rack deployments. Query MGS servers in each switch zone to
    // determine which switch slot they are managing. This logic does not handle
    // an event where there are multiple racks. Is that ok?
    async fn map_switch_zone_addrs(
        &self,
        switch_zone_addresses: Vec<Ipv6Addr>,
    ) -> HashMap<SwitchLocation, Ipv6Addr> {
        info!(self.log, "Determining switch slots managed by switch zones");
        let mut switch_zone_addrs = HashMap::new();
        for addr in switch_zone_addresses {
            let mgs_client = MgsClient::new(
                &format!("http://[{}]:{}", addr, MGS_PORT),
                self.log.new(o!("component" => "MgsClient")),
            );

            info!(
                self.log,
                "determining switch slot managed by dendrite zone";
                "zone_address" => #?addr
            );
            // TODO: #3599 Use retry function instead of looping on a fixed timer
            let switch_slot = loop {
                match mgs_client.sp_local_switch_id().await {
                    Ok(switch) => {
                        info!(
                            self.log,
                            "identified switch slot for dendrite zone";
                            "slot" => #?switch,
                            "zone_address" => #?addr
                        );
                        break switch.slot;
                    }
                    Err(e) => {
                        warn!(
                            self.log,
                            concat!(
                                "failed to identify switch slot for dendrite, ",
                                "will retry in 2 seconds"
                            );
                            "zone_address" => #?addr,
                            "reason" => #?e
                        );
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            };

            match switch_slot {
                0 => {
                    switch_zone_addrs.insert(SwitchLocation::Switch0, addr);
                }
                1 => {
                    switch_zone_addrs.insert(SwitchLocation::Switch1, addr);
                }
                _ => {
                    warn!(
                        self.log,
                        concat!(
                            "Expected a slot number of 0 or 1, ",
                            "found {:#?} when querying {:#?}"
                        ),
                        switch_slot,
                        addr
                    );
                }
            };
        }
        switch_zone_addrs
    }

    // Initialize a single switch via DPD.
    pub async fn init_switch_config(
        &mut self,
        rack_network_config: &RackNetworkConfig,
        switch_ip: Ipv6Addr,
        switch_location: SwitchLocation,
    ) -> Result<(), EarlyNetworkSetupError> {
        // Initialize rack network before NTP comes online, otherwise boundary
        // services will not be available and NTP will fail to sync
        info!(self.log, "Initializing Rack Network");
        let dpd = DpdClient::new(
            &format!("http://[{}]:{}", switch_ip, DENDRITE_PORT),
            dpd_client::ClientState {
                tag: "early_networking".to_string(),
                log: self.log.new(o!("component" => "DpdClient")),
            },
        );

        // configure uplink for each requested uplink in configuration that
        // matches our switch_location
        for uplink_config in rack_network_config
            .uplinks
            .iter()
            .filter(|uplink| uplink.switch == switch_location)
        {
            let (ipv6_entry, dpd_port_settings, port_id) =
                self.build_uplink_config(uplink_config)?;

            self.wait_for_dendrite(&dpd).await;

            info!(
                self.log,
                "Configuring boundary services loopback address on switch";
                "config" => #?ipv6_entry
            );
            dpd.loopback_ipv6_create(&ipv6_entry).await.map_err(|e| {
                EarlyNetworkSetupError::Dendrite(format!(
                    "unable to create inital switch loopback address: {e}"
                ))
            })?;

            info!(
                self.log,
                "Configuring default uplink on switch";
                "config" => #?dpd_port_settings
            );
            dpd.port_settings_apply(&port_id, &dpd_port_settings)
                .await
                .map_err(|e| {
                    EarlyNetworkSetupError::Dendrite(format!(
                        "unable to apply uplink port configuration: {e}"
                    ))
                })?;

            info!(self.log, "advertising boundary services loopback address");

            let ddmd_addr = SocketAddrV6::new(switch_ip, DDMD_PORT, 0, 0);
            let ddmd_client = DdmAdminClient::new(&self.log, ddmd_addr)?;
            ddmd_client.advertise_prefix(Ipv6Subnet::new(ipv6_entry.addr));
        }
        Ok(())
    }

    fn build_uplink_config(
        &self,
        uplink_config: &UplinkConfig,
    ) -> Result<(Ipv6Entry, PortSettings, PortId), EarlyNetworkSetupError> {
        info!(self.log, "Building Uplink Configuration");
        let ipv6_entry = Ipv6Entry {
            addr: BOUNDARY_SERVICES_ADDR.parse().map_err(|e| {
                EarlyNetworkSetupError::BadConfig(format!(
                "failed to parse `BOUNDARY_SERVICES_ADDR` as `Ipv6Addr`: {e}"
            ))
            })?,
            tag: "rss".into(),
        };
        let mut dpd_port_settings = PortSettings {
            tag: "rss".into(),
            links: HashMap::new(),
            v4_routes: HashMap::new(),
            v6_routes: HashMap::new(),
        };
        let link_id = LinkId(0);
        // TODO We're discarding the `uplink_cidr.prefix()` here and only using
        // the IP address; at some point we probably need to give the full CIDR
        // to dendrite?
        let addr = IpAddr::V4(uplink_config.uplink_cidr.ip());
        let link_settings = LinkSettings {
            // TODO Allow user to configure link properties
            // https://github.com/oxidecomputer/omicron/issues/3061
            params: LinkCreate {
                autoneg: false,
                kr: false,
                fec: convert_fec(&uplink_config.uplink_port_fec),
                speed: convert_speed(&uplink_config.uplink_port_speed),
            },
            addrs: vec![addr],
        };
        dpd_port_settings.links.insert(link_id.to_string(), link_settings);
        let port_id: PortId =
            uplink_config.uplink_port.parse().map_err(|e| {
                EarlyNetworkSetupError::BadConfig(format!(
                    concat!(
                        "could not use value provided to",
                        "rack_network_config.uplink_port as PortID: {}"
                    ),
                    e
                ))
            })?;
        let nexthop = Some(uplink_config.gateway_ip);
        dpd_port_settings.v4_routes.insert(
            Ipv4Cidr { prefix: "0.0.0.0".parse().unwrap(), prefix_len: 0 }
                .to_string(),
            RouteSettingsV4 {
                link_id: link_id.0,
                vid: uplink_config.uplink_vid,
                nexthop,
            },
        );
        Ok((ipv6_entry, dpd_port_settings, port_id))
    }

    async fn wait_for_dendrite(&self, dpd: &DpdClient) {
        loop {
            info!(self.log, "Checking dendrite uptime");
            match dpd.dpd_uptime().await {
                Ok(uptime) => {
                    info!(
                        self.log,
                        "Dendrite online";
                        "uptime" => uptime.to_string()
                    );
                    break;
                }
                Err(e) => {
                    info!(
                        self.log,
                        "Unable to check Dendrite uptime";
                        "reason" => #?e
                    );
                }
            }
            info!(self.log, "Waiting for dendrite to come online");
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }
}

/// Network configuration required to bring up the control plane
///
/// The fields in this structure are those from
/// [`super::params::RackInitializeRequest`] necessary for use beyond RSS. This
/// is just for the initial rack configuration and cold boot purposes. Updates
/// will come from Nexus in the future.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EarlyNetworkConfig {
    // The version of data. Always `1` when created from RSS.
    pub generation: u64,

    pub rack_subnet: Ipv6Addr,

    /// The external NTP server addresses.
    pub ntp_servers: Vec<String>,

    /// A copy of the initial rack network configuration when we are in
    /// generation `1`.
    pub rack_network_config: RackNetworkConfig,
}

impl EarlyNetworkConfig {
    pub fn az_subnet(&self) -> Ipv6Subnet<AZ_PREFIX> {
        Ipv6Subnet::<AZ_PREFIX>::new(self.rack_subnet)
    }
}

impl From<EarlyNetworkConfig> for bootstore::NetworkConfig {
    fn from(value: EarlyNetworkConfig) -> Self {
        // Can this ever actually fail?
        // We literally just deserialized the same data in RSS
        let blob = serde_json::to_vec(&value).unwrap();

        // Yes this is duplicated, but that seems fine.
        let generation = value.generation;

        bootstore::NetworkConfig { generation, blob }
    }
}

impl TryFrom<bootstore::NetworkConfig> for EarlyNetworkConfig {
    type Error = serde_json::Error;

    fn try_from(
        value: bootstore::NetworkConfig,
    ) -> std::result::Result<Self, Self::Error> {
        serde_json::from_slice(&value.blob)
    }
}

// The following two conversion functions translate the speed and fec types used
// in the internal API to the types used in the dpd-client API.  The conversion
// is done here, rather than with "impl From" at the definition, to avoid a
// circular dependency between omicron-common and dpd.
fn convert_speed(speed: &PortSpeed) -> dpd_client::types::PortSpeed {
    match speed {
        PortSpeed::Speed0G => dpd_client::types::PortSpeed::Speed0G,
        PortSpeed::Speed1G => dpd_client::types::PortSpeed::Speed1G,
        PortSpeed::Speed10G => dpd_client::types::PortSpeed::Speed10G,
        PortSpeed::Speed25G => dpd_client::types::PortSpeed::Speed25G,
        PortSpeed::Speed40G => dpd_client::types::PortSpeed::Speed40G,
        PortSpeed::Speed50G => dpd_client::types::PortSpeed::Speed50G,
        PortSpeed::Speed100G => dpd_client::types::PortSpeed::Speed100G,
        PortSpeed::Speed200G => dpd_client::types::PortSpeed::Speed200G,
        PortSpeed::Speed400G => dpd_client::types::PortSpeed::Speed400G,
    }
}

fn convert_fec(fec: &PortFec) -> dpd_client::types::PortFec {
    match fec {
        PortFec::Firecode => dpd_client::types::PortFec::Firecode,
        PortFec::None => dpd_client::types::PortFec::None,
        PortFec::Rs => dpd_client::types::PortFec::Rs,
    }
}
