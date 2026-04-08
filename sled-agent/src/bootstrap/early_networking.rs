// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Network setup required to bring up the control plane

use anyhow::{Context, anyhow};
use futures::future;
use gateway_client::Client as MgsClient;
use internal_dns_resolver::{ResolveError, Resolver as DnsResolver};
use internal_dns_types::names::ServiceName;
use omicron_common::address::MGS_PORT;
use omicron_common::backoff::{
    BackoffError, ExponentialBackoff, ExponentialBackoffBuilder, retry_notify,
};
use omicron_ddm_admin_client::DdmError;
use sled_agent_scrimlet_reconcilers::ThisSledSwitchZoneUnderlayIpAddr;
use sled_agent_types::early_networking::{PortConfig, SwitchSlot};
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::{HashMap, HashSet};
use std::net::Ipv6Addr;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::watch;

/// Errors that can occur during early network setup
#[derive(Error, Debug)]
pub enum EarlyNetworkSetupError {
    #[error("Bad configuration for setting up rack: {0}")]
    BadConfig(String),

    #[error("Error contacting ddmd")]
    DdmError(#[from] DdmError),

    #[error("Error during request to MGS: {0}")]
    Mgs(String),

    #[error("Error during DNS lookup")]
    DnsResolver(#[from] ResolveError),
}

enum LookupSwitchZoneAddrsResult {
    // We found every switch zone reported by internal DNS.
    TotalSuccess(HashMap<SwitchSlot, Ipv6Addr>),
    // We found some (but not all) switch zones reported by internal DNS.
    PartialSuccess(HashMap<SwitchSlot, Ipv6Addr>),
}

/// Code for configuring the necessary network bits to bring up the control
/// plane
pub struct EarlyNetworkSetup<'a> {
    log: &'a Logger,
}

/// `EarlyNetworkSetup` provides helper methods primarily used for cold boot and
/// RSS networking configuration (i.e., prior to the control plane being up).
/// The first two are related to finding the switch zones:
///
/// * `lookup_switch_zone_underlay_addrs` attempts to find the underlay
///   addresses and slots of all switch zones reported by internal DNS,
///   which must already be up and populated with the switch zone services.
/// * `lookup_uplinked_switch_zone_underlay_addrs` attempts to find the underlay
///   addresses of all switch zones that are configured with an uplink. Internal
///   DNS must already be up and populated with the switch zone services.
///
/// There are additional details about how these fail behave if one or both
/// switch zones are unresponse; see their respective documentation.
///
/// The third function provided is `determine_our_switch_port_configs()`. It's
/// called by a scrimlet to find the port configs relevant only to it so it can
/// configure `uplinkd`.
impl<'a> EarlyNetworkSetup<'a> {
    pub fn new(log: &'a Logger) -> Self {
        EarlyNetworkSetup { log }
    }

    /// Dynamically looks up (via internal DNS and queries to MGS) the underlay
    /// addresses of the switch zone(s) that have uplinks configured.
    ///
    /// If the `RackNetworkConfig` inside `config_rx` does not contain any
    /// uplinks, returns an empty set. Otherwise:
    ///
    /// * If the config specifies only one switch with an uplink, blocks until
    ///   we can find that switch zone's underlay address.
    /// * If the config specifies two switches with uplinks, we will block up
    ///   to the `wait_for_at_least_one` duration trying to find both
    ///   corresponding switch zones. If we pass the deadline without having
    ///   found both, we will return as soon after that as we can find one of
    ///   the switch zone's addresses.
    pub async fn lookup_uplinked_switch_zone_underlay_addrs(
        &self,
        resolver: &DnsResolver,
        config_rx: &watch::Receiver<SystemNetworkingConfig>,
        wait_for_at_least_one: Duration,
    ) -> HashMap<SwitchSlot, Ipv6Addr> {
        let query_start = Instant::now();
        let uplinked_switch_zone_addrs = retry_notify(
            retry_policy_switch_mapping(),
            || async {
                // Which switches have configured ports?
                let uplinked_switches = config_rx
                    .borrow()
                    .rack_network_config
                    .ports
                    .iter()
                    .map(|port_config| port_config.switch)
                    .collect::<HashSet<SwitchSlot>>();

                // If we have no uplinks, we have nothing to look up.
                if uplinked_switches.is_empty() {
                    return Ok(HashMap::new());
                }

                match self
                    .lookup_switch_zone_underlay_addrs_one_attempt(
                        resolver,
                        &uplinked_switches,
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
                        if elapsed >= wait_for_at_least_one {
                            // We only found one switch when we are expecting
                            // two, but we've been waiting for too long: go with
                            // just one.
                            warn!(
                                self.log,
                                "Only found one switch (expected two), \
                                 but passed requested wait time: returning";
                                "switch_found" => ?map,
                                "requested_wait_time" => ?wait_for_at_least_one,
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
                let elapsed = query_start.elapsed();
                warn!(
                    self.log,
                    "Failed to look up switch zone slots";
                    "error" => #%error,
                    "retry_after" => ?delay,
                    "requested_wait_time" => ?wait_for_at_least_one,
                    "total_elapsed" => ?elapsed,
                );
            },
        )
        .await
        .expect("Expected an infinite retry loop finding switch zones");

        // lookup_switch_zone_underlay_addrs_impl should not return until it
        // finds at least one of the uplinked_switches
        assert!(!uplinked_switch_zone_addrs.is_empty());

        uplinked_switch_zone_addrs
    }

    // TODO: #3601 Audit switch slot discovery logic for robustness
    // in multi-rack deployments. Query MGS servers in each switch zone to
    // determine which switch slot they are managing. This logic does not handle
    // an event where there are multiple racks. Is that ok?
    async fn lookup_switch_zone_underlay_addrs_one_attempt(
        &self,
        resolver: &DnsResolver,
        switches_to_find: &HashSet<SwitchSlot>,
    ) -> Result<LookupSwitchZoneAddrsResult, BackoffError<String>> {
        // We should only be called with a nonempty `switches_to_find`;
        // otherwise we'll never return: we always want to find at least one
        // of these switches.
        assert!(!switches_to_find.is_empty());

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
                    self.log, "Querying MGS to determine switch slot";
                    "addr" => %addr,
                );
                let switch_slot = mgs_client
                    .sp_local_switch_id()
                    .await
                    .with_context(|| format!("Failed to query MGS at {addr}"))?
                    .into_inner()
                    .slot;

                match switch_slot {
                    0 => Ok((SwitchSlot::Switch0, addr)),
                    1 => Ok((SwitchSlot::Switch1, addr)),
                    _ => Err(anyhow!(
                        "Nonsense switch slot returned by MGS at \
                         {addr}: {switch_slot}"
                    )),
                }
            });

        let mut switch_slot_map = HashMap::new();
        for mgs_query_result in future::join_all(mgs_query_futures).await {
            match mgs_query_result {
                Ok((switch_slot, addr)) => {
                    info!(self.log, "Found {switch_slot:?} at {addr}");
                    switch_slot_map.insert(switch_slot, addr);
                }
                Err(err) => {
                    warn!(self.log, "{err:#}");
                }
            }
        }

        // Filter `switch_slot_map` down to just the ones we care about,
        // and then return total/partial/no success based on what's left.
        switch_slot_map.retain(|switch_slot, _addr| {
            switches_to_find.contains(switch_slot)
        });

        if switch_slot_map.is_empty() {
            Err(BackoffError::transient("No switch slots found".to_string()))
        } else if switch_slot_map.len() == switches_to_find.len() {
            Ok(LookupSwitchZoneAddrsResult::TotalSuccess(switch_slot_map))
        } else {
            Ok(LookupSwitchZoneAddrsResult::PartialSuccess(switch_slot_map))
        }
    }

    /// Filter the `SystemNetworkingConfig` down to just the `PortConfig`s
    /// relevant to our switch zone.
    ///
    /// This requires contacting MGS to figure out which switch we are.
    /// Eventually this method will be removed as a part of
    /// <https://github.com/oxidecomputer/omicron/issues/10167>.
    pub(crate) async fn determine_our_switch_port_configs(
        &mut self,
        network_config_rx: &watch::Receiver<SystemNetworkingConfig>,
        switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
    ) -> Result<Vec<PortConfig>, EarlyNetworkSetupError> {
        // First, we have to know which switch we are: ask MGS.
        info!(
            self.log,
            "Determining physical slot of our switch zone at \
             {switch_zone_underlay_ip}",
        );
        let mgs_client = MgsClient::new(
            &format!("http://[{}]:{}", switch_zone_underlay_ip, MGS_PORT),
            self.log.new(o!("component" => "MgsClient")),
        );
        let switch_slot = mgs_client
            .sp_local_switch_id()
            .await
            .map_err(|err| {
                EarlyNetworkSetupError::Mgs(format!(
                    "failed to determine local switch ID via MGS: {}",
                    InlineErrorChain::new(&err)
                ))
            })?
            .slot;

        let switch_slot = match switch_slot {
            0 => SwitchSlot::Switch0,
            1 => SwitchSlot::Switch1,
            _ => {
                // bail here because MGS is not reporting what we expect
                // and we cannot proceed without trustworthy MGS
                return Err(EarlyNetworkSetupError::Mgs(format!(
                    "Local switch zone returned nonsense switch \
                     slot {switch_slot}"
                )));
            }
        };

        // Take a snapshot of the current `RackNetworkConfig` at this point so
        // we use one consistent config throughout the rest of this function.
        let rack_network_config =
            network_config_rx.borrow().rack_network_config.clone();

        // We now know which switch we are: filter the uplinks to just ours.
        let our_ports = rack_network_config
            .ports
            .iter()
            .filter(|port| port.switch == switch_slot)
            .cloned()
            .collect::<Vec<_>>();

        Ok(our_ports)
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
