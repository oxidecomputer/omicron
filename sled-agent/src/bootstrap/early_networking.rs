// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Network setup required to bring up the control plane

use anyhow::{anyhow, Context};
use bootstore::schemes::v0 as bootstore;
use dpd_client::types::{
    LinkCreate, LinkId, LinkSettings, PortId, PortSettings,
};
use dpd_client::Client as DpdClient;
use futures::future;
use gateway_client::Client as MgsClient;
use internal_dns::resolver::{ResolveError, Resolver as DnsResolver};
use internal_dns::ServiceName;
use mg_admin_client::types::BfdPeerConfig as MgBfdPeerConfig;
use mg_admin_client::types::BgpPeerConfig as MgBgpPeerConfig;
use mg_admin_client::types::ImportExportPolicy as MgImportExportPolicy;
use mg_admin_client::types::{
    AddStaticRoute4Request, ApplyRequest, CheckerSource, Prefix, Prefix4,
    Prefix6, ShaperSource, StaticRoute4, StaticRoute4List,
};
use mg_admin_client::Client as MgdClient;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::address::{MGD_PORT, MGS_PORT};
use omicron_common::api::external::{BfdMode, ImportExportPolicy};
use omicron_common::api::internal::shared::{
    BfdPeerConfig, BgpConfig, BgpPeerConfig, PortConfig, PortConfigV2, PortFec,
    PortSpeed, RackNetworkConfig, RackNetworkConfigV2, RouteConfig,
    SwitchLocation, UplinkAddressConfig,
};
use omicron_common::backoff::{
    retry_notify, retry_policy_local, BackoffError, ExponentialBackoff,
    ExponentialBackoffBuilder,
};
use omicron_common::OMICRON_DPD_TAG;
use omicron_ddm_admin_client::DdmError;
use oxnet::{IpNet, Ipv4Net, Ipv6Net};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV6};
use std::time::{Duration, Instant};
use thiserror::Error;

const BGP_SESSION_RESOLUTION: u64 = 100;

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

    #[error("BGP configuration error: {0}")]
    BgpConfigurationError(String),

    #[error("BFD configuration error: {0}")]
    BfdConfigurationError(String),

    #[error("MGD error: {0}")]
    MgdError(String),
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
        // Which switches have configured ports?
        let uplinked_switches = config
            .ports
            .iter()
            .map(|port_config| port_config.switch)
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
    ) -> Result<Vec<PortConfig>, EarlyNetworkSetupError> {
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
        let our_ports = rack_network_config
            .ports
            .iter()
            .filter(|port| port.switch == switch_location)
            .cloned()
            .collect::<Vec<_>>();

        info!(
            self.log,
            "Initializing {} Uplinks on {switch_location:?} at \
             {switch_zone_underlay_ip}",
            our_ports.len(),
        );
        let dpd = DpdClient::new(
            &format!("http://[{}]:{}", switch_zone_underlay_ip, DENDRITE_PORT),
            dpd_client::ClientState {
                tag: OMICRON_DPD_TAG.into(),
                log: self.log.new(o!("component" => "DpdClient")),
            },
        );

        // configure uplink for each requested uplink in configuration that
        // matches our switch_location
        for port_config in &our_ports {
            let (dpd_port_settings, port_id) =
                self.build_port_config(port_config)?;

            self.wait_for_dendrite(&dpd).await;

            info!(
                self.log,
                "Configuring default uplink on switch";
                "config" => #?dpd_port_settings
            );
            dpd.port_settings_apply(
                &port_id,
                Some(OMICRON_DPD_TAG),
                &dpd_port_settings,
            )
            .await
            .map_err(|e| {
                EarlyNetworkSetupError::Dendrite(format!(
                    "unable to apply uplink port configuration: {e}"
                ))
            })?;
        }

        let mgd = MgdClient::new(
            &format!(
                "http://{}",
                &SocketAddrV6::new(switch_zone_underlay_ip, MGD_PORT, 0, 0)
            ),
            self.log.clone(),
        );

        let mut config: Option<BgpConfig> = None;
        let mut bgp_peer_configs =
            HashMap::<String, Vec<MgBgpPeerConfig>>::new();

        // Iterate through ports and apply BGP config.
        for port in &our_ports {
            for peer in &port.bgp_peers {
                if let Some(config) = &config {
                    if peer.asn != config.asn {
                        return Err(EarlyNetworkSetupError::BadConfig(
                            "only one ASN per switch is supported".into(),
                        ));
                    }
                } else {
                    config = Some(
                        rack_network_config
                            .bgp
                            .iter()
                            .find(|x| x.asn == peer.asn)
                            .ok_or(
                                EarlyNetworkSetupError::BgpConfigurationError(
                                    format!(
                                        "asn {} referenced by peer undefined",
                                        peer.asn
                                    ),
                                ),
                            )?
                            .clone(),
                    );
                }

                let bpc = MgBgpPeerConfig {
                    name: format!("{}", peer.addr),
                    host: format!("{}:179", peer.addr),
                    hold_time: peer.hold_time.unwrap_or(6),
                    idle_hold_time: peer.idle_hold_time.unwrap_or(3),
                    delay_open: peer.delay_open.unwrap_or(0),
                    connect_retry: peer.connect_retry.unwrap_or(3),
                    keepalive: peer.keepalive.unwrap_or(2),
                    resolution: BGP_SESSION_RESOLUTION,
                    passive: false,
                    remote_asn: peer.remote_asn,
                    min_ttl: peer.min_ttl,
                    md5_auth_key: peer.md5_auth_key.clone(),
                    multi_exit_discriminator: peer.multi_exit_discriminator,
                    communities: peer.communities.clone(),
                    local_pref: peer.local_pref,
                    enforce_first_as: peer.enforce_first_as,
                    allow_export: match &peer.allowed_export {
                        ImportExportPolicy::NoFiltering => {
                            MgImportExportPolicy::NoFiltering
                        }
                        ImportExportPolicy::Allow(list) => {
                            MgImportExportPolicy::Allow(
                                list.clone()
                                    .iter()
                                    .map(|x| match x {
                                        IpNet::V4(p) => Prefix::V4(Prefix4 {
                                            length: p.width(),
                                            value: p.addr(),
                                        }),
                                        IpNet::V6(p) => Prefix::V6(Prefix6 {
                                            length: p.width(),
                                            value: p.addr(),
                                        }),
                                    })
                                    .collect(),
                            )
                        }
                    },
                    allow_import: match &peer.allowed_import {
                        ImportExportPolicy::NoFiltering => {
                            MgImportExportPolicy::NoFiltering
                        }
                        ImportExportPolicy::Allow(list) => {
                            MgImportExportPolicy::Allow(
                                list.clone()
                                    .iter()
                                    .map(|x| match x {
                                        IpNet::V4(p) => Prefix::V4(Prefix4 {
                                            length: p.width(),
                                            value: p.addr(),
                                        }),
                                        IpNet::V6(p) => Prefix::V6(Prefix6 {
                                            length: p.width(),
                                            value: p.addr(),
                                        }),
                                    })
                                    .collect(),
                            )
                        }
                    },
                    vlan_id: peer.vlan_id,
                };
                match bgp_peer_configs.get_mut(&port.port) {
                    Some(peers) => {
                        peers.push(bpc);
                    }
                    None => {
                        bgp_peer_configs.insert(port.port.clone(), vec![bpc]);
                    }
                }
            }
        }

        if !bgp_peer_configs.is_empty() {
            if let Some(config) = &config {
                mgd.bgp_apply(&ApplyRequest {
                    asn: config.asn,
                    peers: bgp_peer_configs,
                    shaper: config.shaper.as_ref().map(|x| ShaperSource {
                        code: x.clone(),
                        asn: config.asn,
                    }),
                    checker: config.checker.as_ref().map(|x| CheckerSource {
                        code: x.clone(),
                        asn: config.asn,
                    }),
                    originate: config
                        .originate
                        .iter()
                        .map(|x| Prefix4 { length: x.width(), value: x.addr() })
                        .collect(),
                })
                .await
                .map_err(|e| {
                    EarlyNetworkSetupError::BgpConfigurationError(format!(
                        "BGP peer configuration failed: {e}",
                    ))
                })?;
            }
        }

        // Iterate through ports and apply static routing config.
        let mut rq = AddStaticRoute4Request {
            routes: StaticRoute4List { list: Vec::new() },
        };
        for port in &our_ports {
            for r in &port.routes {
                let nexthop = match r.nexthop {
                    IpAddr::V4(v4) => v4,
                    IpAddr::V6(_) => continue,
                };
                let prefix = match r.destination.addr() {
                    IpAddr::V4(v4) => {
                        Prefix4 { value: v4, length: r.destination.width() }
                    }
                    IpAddr::V6(_) => continue,
                };
                let vlan_id = r.vlan_id;
                let sr = StaticRoute4 { nexthop, prefix, vlan_id };
                rq.routes.list.push(sr);
            }
        }
        mgd.static_add_v4_route(&rq).await.map_err(|e| {
            EarlyNetworkSetupError::BgpConfigurationError(format!(
                "static routing configuration failed: {e}",
            ))
        })?;

        // BFD config
        for spec in &rack_network_config.bfd {
            if spec.switch != switch_location {
                continue;
            }
            let cfg = MgBfdPeerConfig {
                detection_threshold: spec.detection_threshold,
                listen: spec.local.unwrap_or(Ipv4Addr::UNSPECIFIED.into()),
                mode: match spec.mode {
                    BfdMode::SingleHop => {
                        mg_admin_client::types::SessionMode::SingleHop
                    }
                    BfdMode::MultiHop => {
                        mg_admin_client::types::SessionMode::MultiHop
                    }
                },
                peer: spec.remote,
                required_rx: spec.required_rx,
            };
            mgd.add_bfd_peer(&cfg).await.map_err(|e| {
                EarlyNetworkSetupError::BfdConfigurationError(e.to_string())
            })?;
        }

        Ok(our_ports)
    }

    fn build_port_config(
        &self,
        port_config: &PortConfig,
    ) -> Result<(PortSettings, PortId), EarlyNetworkSetupError> {
        info!(self.log, "Building Port Configuration");
        let mut dpd_port_settings = PortSettings { links: HashMap::new() };
        let link_id = LinkId(0);

        let mut addrs = Vec::new();
        for a in &port_config.addresses {
            // TODO We're discarding the `uplink_cidr.prefix()` here and only using
            // the IP address; at some point we probably need to give the full CIDR
            // to dendrite?
            addrs.push(a.addr());
        }

        let link_settings = LinkSettings {
            params: LinkCreate {
                autoneg: port_config.autoneg,
                kr: false, //NOTE: kr does not apply to user configurable links.
                fec: convert_fec(&port_config.uplink_port_fec),
                speed: convert_speed(&port_config.uplink_port_speed),
                lane: Some(LinkId(0)),
            },
            addrs,
        };
        dpd_port_settings.links.insert(link_id.to_string(), link_settings);
        let port_id: PortId = port_config.port.parse().map_err(|e| {
            EarlyNetworkSetupError::BadConfig(format!(
                concat!(
                    "could not use value provided to",
                    "rack_network_config.uplink_port as PortID: {}"
                ),
                e
            ))
        })?;

        Ok((dpd_port_settings, port_id))
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

// The first production version of the `EarlyNetworkConfig`.
//
// If this version is in the bootstore than we need to convert it to
// `EarlyNetworkConfigV2`.
//
// Once we do this for all customers that have initialized racks with the
// old version we can go ahead and remove this type and its conversion code
// altogether.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct EarlyNetworkConfigV0 {
    // The current generation number of data as stored in CRDB.
    // The initial generation is set during RSS time and then only mutated
    // by Nexus.
    pub generation: u64,

    pub rack_subnet: Ipv6Addr,

    /// The external NTP server addresses.
    pub ntp_servers: Vec<String>,

    // Rack network configuration as delivered from RSS and only existing at
    // generation 1
    pub rack_network_config: Option<RackNetworkConfigV0>,
}

// The second production version of the `EarlyNetworkConfig`.
//
// If this version is in the bootstore than we need to convert it to
// `EarlyNetworkConfigV2`.
//
// Once we do this for all customers that have initialized racks with the
// old version we can go ahead and remove this type and its conversion code
// altogether.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct EarlyNetworkConfigV1 {
    // The current generation number of data as stored in CRDB.
    // The initial generation is set during RSS time and then only mutated
    // by Nexus.
    pub generation: u64,

    // Which version of the data structure do we have. This is to help with
    // deserialization and conversion in future updates.
    pub schema_version: u32,

    // The actual configuration details
    pub body: EarlyNetworkConfigBodyV1,
}

/// Network configuration required to bring up the control plane
///
/// The fields in this structure are those from
/// [`super::params::RackInitializeRequest`] necessary for use beyond RSS. This
/// is just for the initial rack configuration and cold boot purposes. Updates
/// come from Nexus.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct EarlyNetworkConfig {
    // The current generation number of data as stored in CRDB.
    // The initial generation is set during RSS time and then only mutated
    // by Nexus.
    pub generation: u64,

    // Which version of the data structure do we have. This is to help with
    // deserialization and conversion in future updates.
    pub schema_version: u32,

    // The actual configuration details
    pub body: EarlyNetworkConfigBody,
}

impl EarlyNetworkConfig {
    // Note: This currently only converts between v0 and v1 or deserializes v1 of
    // `EarlyNetworkConfig`.
    pub fn deserialize_bootstore_config(
        log: &Logger,
        config: &bootstore::NetworkConfig,
    ) -> Result<Self, serde_json::Error> {
        // Try to deserialize the latest version of the data structure (v2). If
        // that succeeds we are done.
        let v2_error =
            match serde_json::from_slice::<EarlyNetworkConfig>(&config.blob) {
                Ok(val) => return Ok(val),
                Err(error) => {
                    // Log this error and continue trying to deserialize older
                    // versions.
                    warn!(
                        log,
                        "Failed to deserialize EarlyNetworkConfig \
                         as v2, trying next as v1: {}",
                        error,
                    );
                    error
                }
            };

        match serde_json::from_slice::<EarlyNetworkConfigV1>(&config.blob) {
            Ok(val) => {
                // Convert from v1 to v2
                return Ok(EarlyNetworkConfig {
                    generation: val.generation,
                    schema_version: 2,
                    body: EarlyNetworkConfigBody {
                        ntp_servers: val.body.ntp_servers,
                        rack_network_config: val.body.rack_network_config.map(
                            |v1_config| RackNetworkConfigV1::to_v2(v1_config),
                        ),
                    },
                });
            }
            Err(error) => {
                // Log this error.
                warn!(
                    log,
                    "Failed to deserialize EarlyNetworkConfig \
                         as v1, trying next as v0: {}",
                    error
                );
            }
        };

        match serde_json::from_slice::<EarlyNetworkConfigV0>(&config.blob) {
            Ok(val) => {
                // Convert from v0 to v2
                return Ok(EarlyNetworkConfig {
                    generation: val.generation,
                    schema_version: 2,
                    body: EarlyNetworkConfigBody {
                        ntp_servers: val.ntp_servers,
                        rack_network_config: val.rack_network_config.map(
                            |v0_config| {
                                RackNetworkConfigV0::to_v2(
                                    val.rack_subnet,
                                    v0_config,
                                )
                            },
                        ),
                    },
                });
            }
            Err(error) => {
                // Log this error.
                warn!(
                    log,
                    "Failed to deserialize EarlyNetworkConfig as v0: {}", error,
                );
            }
        };

        // Return the v2 error preferentially over subsequent errors as it's
        // more likely to be useful.
        Err(v2_error)
    }
}

/// This is the actual configuration of EarlyNetworking.
///
/// We nest it below the "header" of `generation` and `schema_version` so that
/// we can perform partial deserialization of `EarlyNetworkConfig` to only read
/// the header and defer deserialization of the body once we know the schema
/// version. This is possible via the use of [`serde_json::value::RawValue`] in
/// future (post-v1) deserialization paths.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct EarlyNetworkConfigBody {
    /// The external NTP server addresses.
    pub ntp_servers: Vec<String>,

    // Rack network configuration as delivered from RSS or Nexus
    pub rack_network_config: Option<RackNetworkConfig>,
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

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct EarlyNetworkConfigBodyV1 {
    /// The external NTP server addresses.
    pub ntp_servers: Vec<String>,

    // Rack network configuration as delivered from RSS or Nexus
    pub rack_network_config: Option<RackNetworkConfigV1>,
}

/// Deprecated, use `RackNetworkConfig` instead. Cannot actually deprecate due to
/// <https://github.com/serde-rs/serde/issues/2195>
///
/// Our first version of `RackNetworkConfig`. If this exists in the bootstore, we
/// upgrade out of it into `RackNetworkConfigV1` or later versions if possible.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
struct RackNetworkConfigV0 {
    // TODO: #3591 Consider making infra-ip ranges implicit for uplinks
    /// First ip address to be used for configuring network infrastructure
    pub infra_ip_first: Ipv4Addr,
    /// Last ip address to be used for configuring network infrastructure
    pub infra_ip_last: Ipv4Addr,
    /// Uplinks for connecting the rack to external networks
    pub uplinks: Vec<UplinkConfig>,
}

impl RackNetworkConfigV0 {
    /// Convert from `RackNetworkConfigV0` to `RackNetworkConfigV1`
    ///
    /// We cannot use `From<RackNetworkConfigV0> for `RackNetworkConfigV2`
    /// because the `rack_subnet` field does not exist in `RackNetworkConfigV0`
    /// and must be passed in from the `EarlyNetworkConfigV0` struct which
    /// contains the `RackNetworkConfigV0` struct.
    pub fn to_v2(
        rack_subnet: Ipv6Addr,
        v0: RackNetworkConfigV0,
    ) -> RackNetworkConfigV2 {
        RackNetworkConfigV2 {
            rack_subnet: Ipv6Net::new(rack_subnet, 56).unwrap(),
            infra_ip_first: v0.infra_ip_first,
            infra_ip_last: v0.infra_ip_last,
            ports: v0
                .uplinks
                .into_iter()
                .map(|uplink| PortConfigV2::from(uplink))
                .collect(),
            bgp: vec![],
            bfd: vec![],
        }
    }
}

/// Deprecated, use PortConfigV2 instead. Cannot actually deprecate due to
/// <https://github.com/serde-rs/serde/issues/2195>
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct PortConfigV1 {
    /// The set of routes associated with this port.
    pub routes: Vec<RouteConfig>,
    /// This port's addresses and optional vlan IDs
    pub addresses: Vec<IpNet>,
    /// Switch the port belongs to.
    pub switch: SwitchLocation,
    /// Nmae of the port this config applies to.
    pub port: String,
    /// Port speed.
    pub uplink_port_speed: PortSpeed,
    /// Port forward error correction type.
    pub uplink_port_fec: PortFec,
    /// BGP peers on this port
    pub bgp_peers: Vec<BgpPeerConfig>,
    /// Whether or not to set autonegotiation
    #[serde(default)]
    pub autoneg: bool,
}

impl From<PortConfigV1> for PortConfigV2 {
    fn from(value: PortConfigV1) -> Self {
        PortConfigV2 {
            routes: value.routes.clone(),
            addresses: value
                .addresses
                .iter()
                .map(|a| UplinkAddressConfig { address: *a, vlan_id: None })
                .collect(),
            switch: value.switch,
            port: value.port,
            uplink_port_speed: value.uplink_port_speed,
            uplink_port_fec: value.uplink_port_fec,
            bgp_peers: vec![],
            autoneg: false,
        }
    }
}

/// Deprecated, use PortConfigV2 instead. Cannot actually deprecate due to
/// <https://github.com/serde-rs/serde/issues/2195>
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
struct UplinkConfig {
    /// Gateway address
    pub gateway_ip: Ipv4Addr,
    /// Switch to use for uplink
    pub switch: SwitchLocation,
    /// Switchport to use for external connectivity
    pub uplink_port: String,
    /// Speed for the Switchport
    pub uplink_port_speed: PortSpeed,
    /// Forward Error Correction setting for the uplink port
    pub uplink_port_fec: PortFec,
    /// IP Address and prefix (e.g., `192.168.0.1/16`) to apply to switchport
    /// (must be in infra_ip pool)
    pub uplink_cidr: Ipv4Net,
    /// VLAN id to use for uplink
    pub uplink_vid: Option<u16>,
}

impl From<UplinkConfig> for PortConfigV2 {
    fn from(value: UplinkConfig) -> Self {
        PortConfigV2 {
            routes: vec![RouteConfig {
                destination: "0.0.0.0/0".parse().unwrap(),
                nexthop: value.gateway_ip.into(),
                vlan_id: value.uplink_vid,
            }],
            addresses: vec![UplinkAddressConfig {
                address: value.uplink_cidr.into(),
                vlan_id: value.uplink_vid,
            }],
            switch: value.switch,
            port: value.uplink_port,
            uplink_port_speed: value.uplink_port_speed,
            uplink_port_fec: value.uplink_port_fec,
            bgp_peers: vec![],
            autoneg: false,
        }
    }
}

/// Deprecated, use `RackNetworkConfig` instead. Cannot actually deprecate due to
/// <https://github.com/serde-rs/serde/issues/2195>
///
/// Our second version of `RackNetworkConfig`. If this exists in the bootstore,
/// we upgrade out of it into `RackNetworkConfigV1` or later versions if
/// possible.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct RackNetworkConfigV1 {
    pub rack_subnet: Ipv6Net,
    // TODO: #3591 Consider making infra-ip ranges implicit for uplinks
    /// First ip address to be used for configuring network infrastructure
    pub infra_ip_first: Ipv4Addr,
    /// Last ip address to be used for configuring network infrastructure
    pub infra_ip_last: Ipv4Addr,
    /// Uplinks for connecting the rack to external networks
    pub ports: Vec<PortConfigV1>,
    /// BGP configurations for connecting the rack to external networks
    pub bgp: Vec<BgpConfig>,
    /// BFD configuration for connecting the rack to external networks
    #[serde(default)]
    pub bfd: Vec<BfdPeerConfig>,
}

impl RackNetworkConfigV1 {
    /// Convert from `RackNetworkConfigV1` to `RackNetworkConfigV2`
    ///
    /// We cannot use `From<RackNetworkConfigV0> for `RackNetworkConfigV1`
    /// because the `rack_subnet` field does not exist in `RackNetworkConfigV0`
    /// and must be passed in from the `EarlyNetworkConfigV0` struct which
    /// contains the `RackNetworkConfivV0` struct.
    pub fn to_v2(v1: RackNetworkConfigV1) -> RackNetworkConfigV2 {
        RackNetworkConfigV2 {
            rack_subnet: v1.rack_subnet,
            infra_ip_first: v1.infra_ip_first,
            infra_ip_last: v1.infra_ip_last,
            ports: v1
                .ports
                .into_iter()
                .map(|ports| PortConfigV2::from(ports))
                .collect(),
            bgp: v1.bgp.clone(),
            bfd: v1.bfd.clone(),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_common::api::internal::shared::RouteConfig;
    use omicron_common::api::internal::shared::UplinkAddressConfig;
    use omicron_test_utils::dev::test_setup_log;

    #[test]
    fn serialized_early_network_config_v0_to_v2_conversion() {
        let logctx = test_setup_log(
            "serialized_early_network_config_v0_to_v2_conversion",
        );
        let v0 = EarlyNetworkConfigV0 {
            generation: 1,
            rack_subnet: Ipv6Addr::UNSPECIFIED,
            ntp_servers: Vec::new(),
            rack_network_config: Some(RackNetworkConfigV0 {
                infra_ip_first: Ipv4Addr::UNSPECIFIED,
                infra_ip_last: Ipv4Addr::UNSPECIFIED,
                uplinks: vec![UplinkConfig {
                    gateway_ip: Ipv4Addr::UNSPECIFIED,
                    switch: SwitchLocation::Switch0,
                    uplink_port: "Port0".to_string(),
                    uplink_port_speed: PortSpeed::Speed100G,
                    uplink_port_fec: PortFec::None,
                    uplink_cidr: "192.168.0.1/16".parse().unwrap(),
                    uplink_vid: None,
                }],
            }),
        };

        let v0_serialized = serde_json::to_vec(&v0).unwrap();
        let bootstore_conf =
            bootstore::NetworkConfig { generation: 1, blob: v0_serialized };

        let v2 = EarlyNetworkConfig::deserialize_bootstore_config(
            &logctx.log,
            &bootstore_conf,
        )
        .unwrap();
        let v0_rack_network_config = v0.rack_network_config.unwrap();
        let uplink = v0_rack_network_config.uplinks[0].clone();
        let expected = EarlyNetworkConfig {
            generation: 1,
            schema_version: 2,
            body: EarlyNetworkConfigBody {
                ntp_servers: v0.ntp_servers.clone(),
                rack_network_config: Some(RackNetworkConfigV2 {
                    rack_subnet: Ipv6Net::new(v0.rack_subnet, 56).unwrap(),
                    infra_ip_first: v0_rack_network_config.infra_ip_first,
                    infra_ip_last: v0_rack_network_config.infra_ip_last,
                    ports: vec![PortConfigV2 {
                        routes: vec![RouteConfig {
                            destination: "0.0.0.0/0".parse().unwrap(),
                            nexthop: uplink.gateway_ip.into(),
                            vlan_id: None,
                        }],
                        addresses: vec![UplinkAddressConfig {
                            address: uplink.uplink_cidr.into(),
                            vlan_id: None,
                        }],
                        switch: uplink.switch,
                        port: uplink.uplink_port,
                        uplink_port_speed: uplink.uplink_port_speed,
                        uplink_port_fec: uplink.uplink_port_fec,
                        autoneg: false,
                        bgp_peers: vec![],
                    }],
                    bgp: vec![],
                    bfd: vec![],
                }),
            },
        };

        assert_eq!(expected, v2);

        logctx.cleanup_successful();
    }

    #[test]
    fn serialized_early_network_config_v1_to_v2_conversion() {
        let logctx = test_setup_log(
            "serialized_early_network_config_v1_to_v2_conversion",
        );

        let v1 = EarlyNetworkConfigV1 {
            generation: 1,
            schema_version: 1,
            body: EarlyNetworkConfigBodyV1 {
                ntp_servers: Vec::new(),
                rack_network_config: Some(RackNetworkConfigV1 {
                    rack_subnet: Ipv6Net::new(Ipv6Addr::UNSPECIFIED, 56)
                        .unwrap(),
                    infra_ip_first: Ipv4Addr::UNSPECIFIED,
                    infra_ip_last: Ipv4Addr::UNSPECIFIED,
                    ports: vec![PortConfigV1 {
                        routes: vec![RouteConfig {
                            destination: "0.0.0.0/0".parse().unwrap(),
                            nexthop: "192.168.0.2".parse().unwrap(),
                            vlan_id: None,
                        }],
                        addresses: vec!["192.168.0.1/16".parse().unwrap()],
                        switch: SwitchLocation::Switch0,
                        port: "Port0".to_string(),
                        uplink_port_speed: PortSpeed::Speed100G,
                        uplink_port_fec: PortFec::None,
                        bgp_peers: Vec::new(),
                        autoneg: false,
                    }],
                    bgp: Vec::new(),
                    bfd: Vec::new(),
                }),
            },
        };

        let v1_serialized = serde_json::to_vec(&v1).unwrap();
        let bootstore_conf =
            bootstore::NetworkConfig { generation: 1, blob: v1_serialized };

        let v2 = EarlyNetworkConfig::deserialize_bootstore_config(
            &logctx.log,
            &bootstore_conf,
        )
        .unwrap();
        let v1_rack_network_config = v1.body.rack_network_config.unwrap();
        let port = v1_rack_network_config.ports[0].clone();
        let expected = EarlyNetworkConfig {
            generation: 1,
            schema_version: 2,
            body: EarlyNetworkConfigBody {
                ntp_servers: v1.body.ntp_servers.clone(),
                rack_network_config: Some(RackNetworkConfigV2 {
                    rack_subnet: v1_rack_network_config.rack_subnet,
                    infra_ip_first: v1_rack_network_config.infra_ip_first,
                    infra_ip_last: v1_rack_network_config.infra_ip_last,
                    ports: vec![PortConfigV2 {
                        routes: port.routes.clone(),
                        addresses: vec![UplinkAddressConfig {
                            address: port.addresses[0],
                            vlan_id: None,
                        }],
                        switch: port.switch,
                        port: port.port,
                        uplink_port_speed: port.uplink_port_speed,
                        uplink_port_fec: port.uplink_port_fec,
                        autoneg: false,
                        bgp_peers: vec![],
                    }],
                    bgp: vec![],
                    bfd: vec![],
                }),
            },
        };

        assert_eq!(expected, v2);

        logctx.cleanup_successful();
    }
}
