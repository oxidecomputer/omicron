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
use internal_dns::resolver::{DnsError, Resolver as DnsResolver};
use internal_dns::ServiceName;
use omicron_common::address::{
    get_64_subnet, Ipv6Subnet, AZ_PREFIX, RACK_PREFIX, SLED_PREFIX,
};
use omicron_common::address::{DDMD_PORT, DENDRITE_PORT, MGS_PORT};
use omicron_common::api::internal::shared::{
    RackNetworkConfig, SwitchLocation, UplinkConfig,
};
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv6Addr, SocketAddrV6};
use thiserror::Error;

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
    DnsResolver(#[from] internal_dns::resolver::ResolveError),
}

/// Code for configuring the necessary network bits to bring up the control
/// plane
pub struct EarlyNetworkSetup {
    log: Logger,

    /// Handle for interacting with the local `bootstore::Node`
    bootstore: bootstore::NodeHandle,
}

impl EarlyNetworkSetup {
    pub fn new(log: &Logger, bootstore: bootstore::NodeHandle) -> Self {
        EarlyNetworkSetup { log: log.clone(), bootstore }
    }

    pub async fn init_switch_zone(
        &mut self,
        config: EarlyNetworkConfig,
    ) -> Result<(), EarlyNetworkSetupError> {
        let resolver = DnsResolver::new_from_subnet(
            self.log.new(o!("component" => "DnsResolver")),
            config.az_subnet(),
        )?;

        info!(self.log, "Finding switch zone addresses in DNS");
        let switch_zone_addresses =
            resolver.lookup_all_ipv6(ServiceName::Dendrite).await?;
        info!(self.log, "Detected switch zone addresses"; "addresses" => #?switch_zone_addresses);

        let switch_mgmt_addrs =
            self.map_switch_zone_addrs(switch_zone_addresses).await;

        // Initialize rack network before NTP comes online, otherwise boundary
        // services will not be available and NTP will fail to sync
        let rack_network_config = &config.rack_network_config;
        info!(self.log, "Initializing Rack Network");
        let dpd_clients = self.initialize_dpd_clients(&switch_mgmt_addrs);

        // set of switches from uplinks, these are our targets for initial NAT configurations
        let mut boundary_switch_addrs: HashSet<Ipv6Addr> = HashSet::new();

        // configure uplink for each requested uplink in configuration
        for uplink_config in &rack_network_config.uplinks {
            // Configure the switch requested by the user
            // Raise error if requested switch is not found
            let dpd = dpd_clients
                .get(&uplink_config.switch)
                .ok_or_else(|| {
                    EarlyNetworkSetupError::BadConfig(format!(
                        "Switch requested by rack network config not found: {:#?}",
                        uplink_config.switch
                    ))
                })?;

            let zone_addr =
                switch_mgmt_addrs.get(&uplink_config.switch).unwrap();

            // This switch will have an uplink configured, so lets add it to our boundary_switch_addrs
            boundary_switch_addrs.insert(*zone_addr);

            let (ipv6_entry, dpd_port_settings, port_id) =
                self.build_uplink_config(uplink_config)?;

            self.wait_for_dendrite(dpd).await;

            info!(self.log, "Configuring boundary services loopback address on switch"; "config" => #?ipv6_entry);
            dpd.loopback_ipv6_create(&ipv6_entry).await.map_err(|e| {
                EarlyNetworkSetupError::Dendrite(format!(
                    "unable to create inital switch loopback address: {e}"
                ))
            })?;

            info!(self.log, "Configuring default uplink on switch"; "config" => #?dpd_port_settings);
            dpd.port_settings_apply(&port_id, &dpd_port_settings)
                    .await
                    .map_err(|e| {
                        EarlyNetworkSetupError::Dendrite(format!("unable to apply initial uplink port configuration: {e}"))
                    })?;

            info!(self.log, "advertising boundary services loopback address");

            let ddmd_addr = SocketAddrV6::new(*zone_addr, DDMD_PORT, 0, 0);
            let ddmd_client = DdmAdminClient::new(&self.log, ddmd_addr)?;
            ddmd_client.advertise_prefix(Ipv6Subnet::new(ipv6_entry.addr));
        }
        // Inject boundary_switch_addrs into ServiceZoneRequests
        // When the opte interface is created for the service,
        // nat entries will be created using the switches present here
        let switch_addrs: Vec<Ipv6Addr> = Vec::from_iter(boundary_switch_addrs);
        for (_, request) in &mut service_plan.services {
            for zone_request in &mut request.services {
                // Do not modify any services that have already been deployed
                if zone_types.contains(&zone_request.zone_type) {
                    continue;
                }

                zone_request.boundary_switches.extend_from_slice(&switch_addrs);
            }
        }
    }

    fn initialize_dpd_clients(
        &self,
        switch_mgmt_addrs: &HashMap<SwitchLocation, Ipv6Addr>,
    ) -> HashMap<SwitchLocation, DpdClient> {
        switch_mgmt_addrs
            .iter()
            .map(|(location, addr)| {
                (
                    location.clone(),
                    DpdClient::new(
                        &format!("http://[{}]:{}", addr, DENDRITE_PORT),
                        dpd_client::ClientState {
                            tag: "rss".to_string(),
                            log: self.log.new(o!("component" => "DpdClient")),
                        },
                    ),
                )
            })
            .collect()
    }

    // TODO: #3601 Audit switch location discovery logic for robustness in multi-rack deployments.
    // Query MGS servers in each switch zone to determine which switch slot they are managing.
    // This logic does not handle an event where there are multiple racks. Is that ok?
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

            info!(self.log, "determining switch slot managed by dendrite zone"; "zone_address" => #?addr);
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
                            "failed to identify switch slot for dendrite, will retry in 2 seconds";
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
                    warn!(self.log, "Expected a slot number of 0 or 1, found {switch_slot:#?} when querying {addr:#?}");
                }
            };
        }
        switch_zone_addrs
    }

    fn build_uplink_config(
        &self,
        uplink_config: &UplinkConfig,
    ) -> Result<(Ipv6Entry, PortSettings, PortId), EarlyNetworkSetupError> {
        info!(self.log, "Building Uplink Configuration");
        let ipv6_entry = Ipv6Entry {
            addr: BOUNDARY_SERVICES_ADDR.parse().map_err(|e| {
                SetupServiceError::BadConfig(format!(
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
        let addr = IpAddr::V4(uplink_config.uplink_ip);
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
        let port_id: PortId = uplink_config
            .uplink_port
            .parse()
            .map_err(|e| SetupServiceError::BadConfig(
            format!("could not use value provided to rack_network_config.uplink_port as PortID: {e}")))?;
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
                    info!(self.log, "Dendrite online"; "uptime" => uptime.to_string());
                    break;
                }
                Err(e) => {
                    info!(self.log, "Unable to check Dendrite uptime"; "reason" => #?e);
                }
            }
            info!(self.log, "Waiting for dendrite to come online");
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }
}

/// Network configuration required to bring up the control plane
///
/// The fields in this structure are those from [`RackInitializeRequest`]
/// necessary for use beyond RSS. This is just for the initial rack configuration
/// and cold boot purposes. Updates will come from Nexus in the future.
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
    type Error = anyhow::Error;

    fn try_from(
        value: bootstore::NetworkConfig,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(serde_json::from_slice(&value.blob)?)
    }
}
