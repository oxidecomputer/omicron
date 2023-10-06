// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Network setup required to bring up the control plane

use anyhow::{anyhow, Context};
use bootstore::schemes::v0 as bootstore;
use ddm_admin_client::{Client as DdmAdminClient, DdmError};
use dpd_client::types::Ipv6Entry;
use dpd_client::types::{
    LinkCreate, LinkId, LinkSettings, PortId, PortSettings, RouteSettingsV4,
};
use dpd_client::Client as DpdClient;
use dpd_client::Ipv4Cidr;
use futures::future;
use gateway_client::Client as MgsClient;
use internal_dns::resolver::{ResolveError, Resolver as DnsResolver};
use internal_dns::ServiceName;
use omicron_common::address::{Ipv6Subnet, AZ_PREFIX, MGS_PORT};
use omicron_common::address::{DDMD_PORT, DENDRITE_PORT};
use omicron_common::api::internal::shared::{
    PortFec, PortSpeed, RackNetworkConfig, SwitchLocation, UplinkConfig,
};
use omicron_common::backoff::{
    retry_notify, retry_policy_local, BackoffError, ExponentialBackoff,
    ExponentialBackoffBuilder,
};
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv6Addr, SocketAddrV6};
use std::time::{Duration, Instant};
use thiserror::Error;

static BOUNDARY_SERVICES_ADDR: &str = "fd00:99::1";

/// Errors that can occur during early network setup
#[derive(Error, Debug)]
pub enum EarlyNetworkSetupError {
    #[error("Bad configuration for setting up rack: {0}")]
    BadConfig(String),

    #[error("Error contacting ddmd: {0}")]
    DdmError(#[from] DdmError),

    #[error("Error during request to MGS: {0}")]
    Mgs(String),

    #[error("Error during request to Dendrite: {0}")]
    Dendrite(String),

    #[error("Error during DNS lookup: {0}")]
    DnsResolver(#[from] ResolveError),
}

enum LookupSwitchZoneAddrsResult {
    // We found every switch zone reported by internal DNS.
    TotalSuccess(HashMap<SwitchLocation, Ipv6Addr>),
    // We found some (but not all) switch zones reported by internal DNS.
    PartialSuccess(HashMap<SwitchLocation, Ipv6Addr>),
}

/// Code for configuring the necessary network bits to bring up the control
/// plane
pub struct EarlyNetworkSetup<'a> {
    log: &'a Logger,
}

/// `EarlyNetworkSetup` provides three helper methods primarily used for cold
/// boot and RSS networking configuration (i.e., prior to the control plane
/// being up). The first two are related to finding the switch zones:
///
/// * `lookup_switch_zone_underlay_addrs` attempts to find the underlay
///   addresses and locations of all switch zones reported by internal DNS,
///   which must already be up and populated with the switch zone services.
/// * `lookup_uplinked_switch_zone_underlay_addrs` attempts to find the underlay
///   addresses of all switch zones that are configured with an uplink. Internal
///   DNS must already be up and populated with the switch zone services.
///
/// There are additional details about how these fail behave if one or both
/// switch zones are unresponse; see their respective documentation.
///
/// The third function provided is `init_switch_config`. It is intended to be
/// called by a scrimlet to configure _its own_ switch by talking to dendrite on
/// its own switch zone's underlay address.
impl<'a> EarlyNetworkSetup<'a> {
    pub fn new(log: &'a Logger) -> Self {
        EarlyNetworkSetup { log }
    }

    /// Dynamically looks up (via internal DNS and queries to MGS) the underlay
    /// addresses of the switch zone(s) that have uplinks configured.
    ///
    /// If `rack_network_config` does not contain any uplinks, returns an empty
    /// set. Otherwise:
    ///
    /// * If `rack_network_config` specifies only one switch with an uplink,
    ///   blocks until we can find that switch zone's underlay address.
    /// * If `rack_network_config` specifies two switches with uplinks, we will
    ///   block for a while (~5 minutes, currently) trying to find both
    ///   corresponding switch zones. If we pass our deadline without having
    ///   found both, we will return as soon after that as we can find one of
    ///   the switch zone's addresses.
    pub async fn lookup_uplinked_switch_zone_underlay_addrs(
        &self,
        resolver: &DnsResolver,
        config: &RackNetworkConfig,
    ) -> HashSet<Ipv6Addr> {
        // Which switches have uplinks?
        let uplinked_switches = config
            .uplinks
            .iter()
            .map(|uplink_config| uplink_config.switch)
            .collect::<HashSet<SwitchLocation>>();

        // If we have no uplinks, we have nothing to look up.
        if uplinked_switches.is_empty() {
            return HashSet::new();
        }

        let uplinked_switch_zone_addrs = self
            .lookup_switch_zone_underlay_addrs_impl(
                resolver,
                Some(uplinked_switches),
            )
            .await;

        // lookup_switch_zone_underlay_addrs_impl should not return until it
        // finds at least one of the uplinked_switches
        assert!(!uplinked_switch_zone_addrs.is_empty());

        uplinked_switch_zone_addrs.into_values().collect()
    }

    /// Dynamically looks up (via internal DNS and queries to MGS) the underlay
    /// addresses of the switch zones.
    ///
    /// Blocks until either:
    ///
    /// * We found the location and underlay addresses of all switch zones
    ///   reported by internal DNS
    /// * We've passed a substantial deadline (~5 minutes, currently) and have
    ///   found the location and underlay address of one switch zone reported by
    ///   internal DNS
    pub async fn lookup_switch_zone_underlay_addrs(
        &self,
        resolver: &DnsResolver,
    ) -> HashMap<SwitchLocation, Ipv6Addr> {
        self.lookup_switch_zone_underlay_addrs_impl(resolver, None).await
    }

    async fn lookup_switch_zone_underlay_addrs_impl(
        &self,
        resolver: &DnsResolver,
        switches_to_find: Option<HashSet<SwitchLocation>>,
    ) -> HashMap<SwitchLocation, Ipv6Addr> {
        // We will wait up to 5 minutes to try to find all switches; if we pass
        // the 5 minute mark, we will return as soon as we find at least one
        // switch.
        const MAX_SWITCH_ZONE_WAIT_TIME: Duration = Duration::from_secs(60 * 5);

        let query_start = Instant::now();
        retry_notify(
            retry_policy_switch_mapping(),
            || async {
                match self
                    .lookup_switch_zone_underlay_addrs_one_attempt(
                        resolver,
                        switches_to_find.as_ref(),
                    )
                    .await?
                {
                    LookupSwitchZoneAddrsResult::TotalSuccess(map) => {
                        info!(
                            self.log,
                            "Successfully looked up all expected switch zone \
                             underlay addresses";
                            "addrs" => ?map,
                        );
                        Ok(map)
                    }
                    LookupSwitchZoneAddrsResult::PartialSuccess(map) => {
                        let elapsed = query_start.elapsed();
                        if elapsed >= MAX_SWITCH_ZONE_WAIT_TIME {
                            // We only found one switch when we are expecting
                            // two, but we've been waiting for too long: go with
                            // just one.
                            warn!(
                                self.log,
                                "Only found one switch (expected two), \
                                 but passed wait time of \
                                 {MAX_SWITCH_ZONE_WAIT_TIME:?}: returning";
                                "switch_found" => ?map,
                                "total_elapsed" => ?elapsed,
                            );
                            Ok(map)
                        } else {
                            // We only found one switch when we are expecting
                            // two; retry after a backoff. Our logging closure
                            // below will `warn!` with this error.
                            Err(BackoffError::transient(format!(
                                "Only found one switch (expected two): {map:?}"
                            )))
                        }
                    }
                }
            },
            |error, delay| {
                warn!(
                    self.log,
                    "Failed to look up switch zone locations";
                    "error" => #%error,
                    "retry_after" => ?delay,
                );
            },
        )
        .await
        .expect("Expected an infinite retry loop finding switch zones")
    }

    // TODO: #3601 Audit switch location discovery logic for robustness
    // in multi-rack deployments. Query MGS servers in each switch zone to
    // determine which switch slot they are managing. This logic does not handle
    // an event where there are multiple racks. Is that ok?
    async fn lookup_switch_zone_underlay_addrs_one_attempt(
        &self,
        resolver: &DnsResolver,
        switches_to_find: Option<&HashSet<SwitchLocation>>,
    ) -> Result<LookupSwitchZoneAddrsResult, BackoffError<String>> {
        if let Some(switches_to_find) = switches_to_find {
            // We should only be called with a nonempty `switches_to_find`;
            // otherwise we'll never return: we always want to find at least one
            // of these switches.
            assert!(!switches_to_find.is_empty());
        }

        // We might have stale DNS results; clear our resolver's cache.
        resolver.clear_cache();

        info!(self.log, "Resolving switch zone addresses in DNS");
        let switch_zone_addrs = resolver
            .lookup_all_ipv6(ServiceName::Dendrite)
            .await
            .map_err(|err| {
                BackoffError::transient(format!(
                    "Error resolving dendrite services in internal DNS: {err}",
                ))
            })?;

        let mgs_query_futures =
            switch_zone_addrs.iter().copied().map(|addr| async move {
                let mgs_client = MgsClient::new(
                    &format!("http://[{}]:{}", addr, MGS_PORT),
                    self.log.new(o!("component" => "MgsClient")),
                );

                info!(
                    self.log, "Querying MGS to determine switch location";
                    "addr" => %addr,
                );
                let switch_slot = mgs_client
                    .sp_local_switch_id()
                    .await
                    .with_context(|| format!("Failed to query MGS at {addr}"))?
                    .into_inner()
                    .slot;

                match switch_slot {
                    0 => Ok((SwitchLocation::Switch0, addr)),
                    1 => Ok((SwitchLocation::Switch1, addr)),
                    _ => Err(anyhow!(
                        "Nonsense switch slot returned by MGS at \
                         {addr}: {switch_slot}"
                    )),
                }
            });

        let mut switch_location_map = HashMap::new();
        for mgs_query_result in future::join_all(mgs_query_futures).await {
            match mgs_query_result {
                Ok((location, addr)) => {
                    info!(self.log, "Found {location:?} at {addr}");
                    switch_location_map.insert(location, addr);
                }
                Err(err) => {
                    warn!(self.log, "{err:#}");
                }
            }
        }

        if let Some(switches_to_find) = switches_to_find {
            // Were we tasked with finding _specific_ switches? If so, filter
            // `switch_location_map` down to just the ones we care about, and
            // then return total/partial/no success based on what's left.
            switch_location_map
                .retain(|location, _addr| switches_to_find.contains(location));

            if switch_location_map.is_empty() {
                Err(BackoffError::transient(
                    "No switch locations found".to_string(),
                ))
            } else if switch_location_map.len() == switches_to_find.len() {
                Ok(LookupSwitchZoneAddrsResult::TotalSuccess(
                    switch_location_map,
                ))
            } else {
                Ok(LookupSwitchZoneAddrsResult::PartialSuccess(
                    switch_location_map,
                ))
            }
        } else {
            // We were not tasked with finding specific switches: we're done if
            // we found both, or if we found one and internal DNS only gave us
            // one IP address (e.g., in test environments with just one switch).
            if switch_location_map.is_empty() {
                Err(BackoffError::transient(
                    "No switch locations found".to_string(),
                ))
            } else if switch_location_map.len() == 1
                && switch_zone_addrs.len() > 1
            {
                Ok(LookupSwitchZoneAddrsResult::PartialSuccess(
                    switch_location_map,
                ))
            } else {
                Ok(LookupSwitchZoneAddrsResult::TotalSuccess(
                    switch_location_map,
                ))
            }
        }
    }

    /// Initialize a single switch via DPD.
    ///
    /// This should be called by a scrimlet after it brings up its own switch
    /// zone. `switch_zone_underlay_ip` should be the IP address of the switch
    /// zone it brought up.
    ///
    /// Returns the list of uplinks configured via DPD.
    pub async fn init_switch_config(
        &mut self,
        rack_network_config: &RackNetworkConfig,
        switch_zone_underlay_ip: Ipv6Addr,
    ) -> Result<Vec<UplinkConfig>, EarlyNetworkSetupError> {
        // First, we have to know which switch we are: ask MGS.
        info!(
            self.log,
            "Determining physical location of our switch zone at \
             {switch_zone_underlay_ip}",
        );
        let mgs_client = MgsClient::new(
            &format!("http://[{}]:{}", switch_zone_underlay_ip, MGS_PORT),
            self.log.new(o!("component" => "MgsClient")),
        );
        let switch_slot = retry_notify(
            retry_policy_local(),
            || async {
                mgs_client
                    .sp_local_switch_id()
                    .await
                    .map_err(BackoffError::transient)
                    .map(|response| response.into_inner().slot)
            },
            |error, delay| {
                warn!(
                    self.log,
                    "Failed to get switch ID from MGS (retrying in {delay:?})";
                    "error" => ?error,
                );
            },
        )
        .await
        .expect("Expected an infinite retry loop getting our switch ID");

        let switch_location = match switch_slot {
            0 => SwitchLocation::Switch0,
            1 => SwitchLocation::Switch1,
            _ => {
                return Err(EarlyNetworkSetupError::Mgs(format!(
                    "Local switch zone returned nonsense switch \
                     slot {switch_slot}"
                )));
            }
        };

        // We now know which switch we are: filter the uplinks to just ours.
        let our_uplinks = rack_network_config
            .uplinks
            .iter()
            .filter(|uplink| uplink.switch == switch_location)
            .cloned()
            .collect::<Vec<_>>();

        info!(
            self.log,
            "Initializing {} Uplinks on {switch_location:?} at \
             {switch_zone_underlay_ip}",
            our_uplinks.len(),
        );
        let dpd = DpdClient::new(
            &format!("http://[{}]:{}", switch_zone_underlay_ip, DENDRITE_PORT),
            dpd_client::ClientState {
                tag: "early_networking".to_string(),
                log: self.log.new(o!("component" => "DpdClient")),
            },
        );

        // configure uplink for each requested uplink in configuration that
        // matches our switch_location
        for uplink_config in &our_uplinks {
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

            let ddmd_addr =
                SocketAddrV6::new(switch_zone_underlay_ip, DDMD_PORT, 0, 0);
            let ddmd_client = DdmAdminClient::new(&self.log, ddmd_addr)?;
            ddmd_client.advertise_prefix(Ipv6Subnet::new(ipv6_entry.addr));
        }

        Ok(our_uplinks)
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
        dpd_port_settings.v4_routes.insert(
            Ipv4Cidr { prefix: "0.0.0.0".parse().unwrap(), prefix_len: 0 }
                .to_string(),
            RouteSettingsV4 {
                link_id: link_id.0,
                vid: uplink_config.uplink_vid,
                nexthop: uplink_config.gateway_ip,
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

// This is derived from `retry_policy_internal_service_aggressive` with a
// much lower `max_interval`, because we are not going to retry forever: once we
// pass `MAX_SWITCH_ZONE_WAIT_TIME` we will stop as soon as we can talk to _any_
// switch zone (whereas we stop earlier if we can talk to _all_ switch zones).
fn retry_policy_switch_mapping() -> ExponentialBackoff {
    ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(100))
        .with_multiplier(1.2)
        .with_max_interval(Duration::from_secs(15))
        .with_max_elapsed_time(None)
        .build()
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
    pub rack_network_config: Option<RackNetworkConfig>,
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
