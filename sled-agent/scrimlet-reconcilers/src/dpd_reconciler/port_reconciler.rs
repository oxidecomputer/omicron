// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciliation of QSFP port settings.

use crate::switch_zone_slot::ThisSledSwitchSlot;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::dpd::{
    DpdPortOperationFailure, DpdPortReconcilerStatus,
};
use daft::Diffable;
use dpd_client::Client;
use dpd_client::types::LinkCreate as DpdLinkCreate;
use dpd_client::types::LinkId as DpdLinkId;
use dpd_client::types::LinkSettings as DpdLinkSettings;
use dpd_client::types::PortFec as DpdPortFec;
use dpd_client::types::PortId as DpdPortId;
use dpd_client::types::PortSettings as DpdPortSettings;
use dpd_client::types::PortSpeed as DpdPortSpeed;
use dpd_client::types::Qsfp as DpdQsfp;
use dpd_client::types::TxEq as DpdTxEq;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_ord_map;
use omicron_common::OMICRON_DPD_TAG;
use sled_agent_types::early_networking::LinkFec;
use sled_agent_types::early_networking::LinkSpeed;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::TxEqConfig;
use sled_agent_types::early_networking::UplinkAddress;
use sled_agent_types::early_networking::UplinkAddressConfig;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::IpAddr;

type DpdClientError = dpd_client::Error<dpd_client::types::Error>;

const DPD_TAG: Option<&'static str> = Some(OMICRON_DPD_TAG);

#[derive(Default, Debug)]
pub(super) struct PortReconciler {
    // The set of QSFP ports is a physical property; it never changes for a
    // given switch, so we can cache this value once. The first time dpd
    // successfully returns the list of ports, we save that value here and reuse
    // it forever; it can never change.
    cached_qsfp_ports: Option<Vec<DpdQsfp>>,
}

impl PortReconciler {
    pub(super) async fn reconcile(
        &mut self,
        client: &Client,
        desired_config: &RackNetworkConfig,
        our_switch_slot: ThisSledSwitchSlot,
        log: &Logger,
    ) -> DpdPortReconcilerStatus {
        let dpd_current_settings = match self
            .dpd_get_current_settings(client, log)
            .await
        {
            Ok(settings) => settings,
            Err(err) => {
                return DpdPortReconcilerStatus::FailedReadingCurrentSettings(
                    format!(
                        "failed to read current port settings from dpd: {}",
                        InlineErrorChain::new(&err),
                    ),
                );
            }
        };

        let plan = match ReconciliationPlan::new(
            dpd_current_settings,
            desired_config,
            our_switch_slot,
            log,
        ) {
            Ok(plan) => plan,
            Err(err) => {
                // Ensure `err` is actually a string; if it changes to a proper
                // error type, we need to use `InlineErrorChain` here instead.
                let err: &str = &err;
                return DpdPortReconcilerStatus::FailedGeneratingPlan(format!(
                    "failed to generate plan to apply port settings: {err}",
                ));
            }
        };

        apply_plan(client, plan, log).await
    }

    async fn dpd_get_current_settings(
        &mut self,
        client: &Client,
        log: &Logger,
    ) -> Result<BTreeMap<DpdQsfp, DpdPortSettings>, DpdClientError> {
        let qsfp_ports = match self.cached_qsfp_ports.as_mut() {
            Some(cached) => cached.as_slice(),
            None => {
                let ports = client
                    .port_list()
                    .await?
                    .into_inner()
                    .into_iter()
                    .filter_map(|port| match port {
                        // We're only responsible for applying settings to QSFP
                        // ports; any other kind of port cannot have settings
                        // populated in `RackNetworkConfig` and is
                        // internal-to-dpd.
                        DpdPortId::Internal(_) | DpdPortId::Rear(_) => None,
                        DpdPortId::Qsfp(qsfp) => Some(qsfp),
                    })
                    .collect::<Vec<_>>();
                info!(
                    log, "cached set of qsfp port IDs";
                    "num-qsfp-ports" => ports.len(),
                );
                self.cached_qsfp_ports.insert(ports).as_slice()
            }
        };

        let mut config_by_port = BTreeMap::new();
        for port_id in qsfp_ports.iter().cloned() {
            let settings = client
                .port_settings_get(&DpdPortId::Qsfp(port_id.clone()), DPD_TAG)
                .await?
                .into_inner();
            // Check for empty ports and filter those out.
            //
            // TODO-performance We could consider caching "ports known to have
            // no links", as that _shouldn't_ change unless we apply settings to
            // a port. But "shouldn't" is doing a lot of work here, and caching
            // has all the usual problems! For now, we'll just fetch all the
            // port settings every time, and we can tune that down in the future
            // if needed.
            let are_port_settings_clear = {
                let DpdPortSettings { links } = &settings;
                links.is_empty()
            };
            if !are_port_settings_clear {
                config_by_port.insert(port_id, settings);
            }
        }

        Ok(config_by_port)
    }
}

/// Apply the contents of `plan` to dpd via `client`.
///
/// This requires `plan.to_clear.len() + plan.to_apply.len()` independent
/// calls to `dpd`. We do not short circuit on failure: we'll always attempt to
/// make every call required. This may not be the right choice, but some
/// arguments in favor:
///
/// * In practice we expect the number of calls here to be small. On startup we
///   expect 1-32 `to_apply` calls (one for each configured uplink), and for
///   every reconciliation attempt after that we expect 0-1 (either no changes,
///   or a single port has had its settings changed; it's possible we'll see
///   multiple ports change at once, but at most 32).
/// * We always want to report the status of every step described by `plan`, and
///   implementing stop-on-first-failure means we'd need to record a "didn't
///   attempt because of an earlier failure" status for some steps. That's
///   doable but annoying.
async fn apply_plan(
    client: &Client,
    plan: ReconciliationPlan,
    log: &Logger,
) -> DpdPortReconcilerStatus {
    let ReconciliationPlan { unchanged, to_clear, to_apply } = plan;

    let mut cleared = BTreeSet::new();
    let mut clear_failures = Vec::new();
    for port_id in to_clear {
        let port_id_string = port_id.to_string();
        match client
            .port_settings_clear(&DpdPortId::Qsfp(port_id), DPD_TAG)
            .await
        {
            Ok(_) => {
                info!(
                    log, "successfully cleared settings for port";
                    "port_id" => &port_id_string,
                );
                cleared.insert(port_id_string);
            }
            Err(err) => {
                let err = InlineErrorChain::new(&err);
                warn!(
                    log, "failed to clear port settings";
                    "port_id" => &port_id_string,
                    &err,
                );
                clear_failures.push(DpdPortOperationFailure {
                    port_id: port_id_string,
                    error: err.to_string(),
                });
            }
        }
    }

    let mut applied = BTreeSet::new();
    let mut apply_failures = Vec::new();
    for (port_id, settings) in to_apply {
        let port_id_string = port_id.to_string();
        match client
            .port_settings_apply(&DpdPortId::Qsfp(port_id), DPD_TAG, &settings)
            .await
        {
            Ok(_) => {
                info!(
                    log, "successfully applied settings for port";
                    "port_id" => &port_id_string,
                );
                applied.insert(port_id_string);
            }
            Err(err) => {
                let err = InlineErrorChain::new(&err);
                warn!(
                    log, "failed to apply port settings";
                    "port_id" => &port_id_string,
                    &err,
                );
                apply_failures.push(DpdPortOperationFailure {
                    port_id: port_id_string,
                    error: err.to_string(),
                });
            }
        }
    }

    if clear_failures.is_empty() && apply_failures.is_empty() {
        DpdPortReconcilerStatus::Success { unchanged, cleared, applied }
    } else {
        DpdPortReconcilerStatus::PartialSuccess {
            unchanged,
            cleared,
            clear_failures,
            applied,
            apply_failures,
        }
    }
}

#[derive(Debug, PartialEq)]
struct ReconciliationPlan {
    // Set of ports whose settings are already correct in `dpd`.
    unchanged: BTreeSet<String>,

    // Set of ports that have settings in `dpd` but not in our desired config;
    // these should be cleared.
    to_clear: BTreeSet<DpdQsfp>,

    // Set of ports whose settings in `dpd` don't match our desired config or
    // don't exist at all; these need to be applied.
    to_apply: BTreeMap<DpdQsfp, DpdPortSettings>,
}

impl ReconciliationPlan {
    fn new(
        dpd_current_settings: BTreeMap<DpdQsfp, DpdPortSettings>,
        config: &RackNetworkConfig,
        our_switch_slot: ThisSledSwitchSlot,
        log: &Logger,
    ) -> Result<Self, String> {
        // Helper for all the places in this method where we have to convert a
        // string back into a `DpdQsfp`. We never expect this to fail in
        // practice, but want to report the source of the bad port name if it
        // happens.
        fn parse_port_id(
            port_id: &str,
            source: &str,
        ) -> Result<DpdQsfp, String> {
            port_id.parse().map_err(|err| {
                format!(
                    "invalid port ID `{port_id}` in {source}: {}",
                    InlineErrorChain::new(&err)
                )
            })
        }

        // Convert dpd settings into a diffable form.
        let dpd_current_settings = dpd_current_settings
            .into_iter()
            .map(DiffablePortSettings::try_from)
            .collect::<Result<IdOrdMap<_>, _>>()
            .map_err(|err| InlineErrorChain::new(&err).to_string())?;

        // Convert desired config into a diffable form.
        let desired_settings = config
            .ports
            .iter()
            .filter(|p| p.switch == our_switch_slot)
            .map(DiffablePortSettings::from)
            .collect::<IdOrdMap<_>>();

        let id_ord_map::Diff { common, added, removed } =
            dpd_current_settings.diff(&desired_settings);

        // Any entries removed are ports that have settings in dpd but not
        // `config`; we need to clear them.
        let to_clear = removed
            .into_iter()
            .map(|item| parse_port_id(&item.port_id, "dpd"))
            .collect::<Result<BTreeSet<DpdQsfp>, _>>()?;

        // Any entries added are ports that have settings in `config` but not
        // dpd; we need to add them.
        let mut to_apply = added
            .into_iter()
            .map(|p| {
                let port_id = parse_port_id(&p.port_id, "rack network config")?;
                Ok::<_, String>((port_id, DpdPortSettings::from(p)))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let mut unchanged = BTreeSet::new();

        // For any entries in common (i.e., the key exists in both dpd and
        // `config`), we have to check whether any values changed. For every
        // leaf in common, we'll either add its port ID to `unchanged` (if the
        // values match) or we'll add the desired value to `to_apply`.
        for leaf in common {
            if leaf.is_unchanged() {
                unchanged.insert(
                    parse_port_id(leaf.key(), "rack network config AND dpd")?
                        .to_string(),
                );
            } else {
                let port_id =
                    parse_port_id(leaf.key(), "rack network config AND dpd")?;

                // `common` is a map of unique keys that must be distinct from
                // the `added` keys used to seed `to_apply`, so these inserts
                // are guaranteed to all be unique.
                to_apply.insert(port_id, (*leaf.after()).into());
            }
        }

        info!(
            log,
            "generated dpd port settings reconciliation plan";
            "ports_unchanged" => unchanged.len(),
            "ports_to_clear" => to_clear.len(),
            "ports_to_apply" => to_apply.len(),
        );

        Ok(Self { unchanged, to_clear, to_apply })
    }
}

// We convert both `RackNetworkConfig`'s port settings and the `DpdPortSettings`
// we read from `dpd` into this type to compute the diff.
#[derive(Debug, Clone, PartialEq, Eq, daft::Diffable)]
struct DiffablePortSettings {
    port_id: String,
    autoneg: bool,
    tx_eq: Option<TxEqConfig>,
    fec: Option<LinkFec>,
    speed: LinkSpeed,
    addrs: BTreeSet<IpAddr>,
}

impl IdOrdItem for DiffablePortSettings {
    type Key<'a> = &'a str;

    fn key(&self) -> Self::Key<'_> {
        &self.port_id
    }

    iddqd::id_upcast!();
}

impl From<&'_ PortConfig> for DiffablePortSettings {
    fn from(port: &'_ PortConfig) -> Self {
        Self {
            port_id: port.port.clone(),
            autoneg: port.autoneg,
            tx_eq: port.tx_eq,
            fec: port.uplink_port_fec,
            speed: port.uplink_port_speed,
            addrs: port
                .addresses
                .iter()
                .filter_map(|a| {
                    let UplinkAddressConfig {
                        address,
                        // Discard `vlan_id` - that's handled by `uplinkd`.
                        vlan_id: _,
                    } = a;

                    match address {
                        UplinkAddress::AddrConf => None,
                        UplinkAddress::Static { ip_net } => {
                            // Discard the `ip_net` prefix, which is also
                            // handled by `uplinkd`. We only need the IP.
                            Some(ip_net.addr())
                        }
                    }
                })
                .collect(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error(
    "expected exactly 1 link per port in dpd, but got {nlinks} on port {port}"
)]
struct UnexpectedLinkCount {
    nlinks: usize,
    port: String,
}

impl TryFrom<(DpdQsfp, DpdPortSettings)> for DiffablePortSettings {
    type Error = UnexpectedLinkCount;

    fn try_from(
        value: (DpdQsfp, DpdPortSettings),
    ) -> Result<Self, Self::Error> {
        let (port_id, DpdPortSettings { links }) = value;

        // We only expect to be constructed if there's exactly one link
        // configured:
        //
        // * 0 links is an empty port; those should be filtered out by
        //   `dpd_get_current_settings()`
        // * 2 or more links cannot be represented in `RackNetworkConfig` today,
        //   so it shouldn't be possible for `dpd` to report that on any port
        if links.len() != 1 {
            return Err(UnexpectedLinkCount {
                nlinks: links.len(),
                port: port_id.to_string(),
            });
        }
        // We just confirmed there's exactly one link; take ownership of it.
        let sole_link = links.into_values().next().unwrap();

        let tx_eq = sole_link.params.tx_eq.map(|t| TxEqConfig {
            main: t.main,
            post1: t.post1,
            post2: t.post2,
            pre1: t.pre1,
            pre2: t.pre2,
        });

        let fec = sole_link.params.fec.map(|f| match f {
            DpdPortFec::Firecode => LinkFec::Firecode,
            DpdPortFec::None => LinkFec::None,
            DpdPortFec::Rs => LinkFec::Rs,
        });

        let speed = match sole_link.params.speed {
            DpdPortSpeed::Speed0G => LinkSpeed::Speed0G,
            DpdPortSpeed::Speed1G => LinkSpeed::Speed1G,
            DpdPortSpeed::Speed10G => LinkSpeed::Speed10G,
            DpdPortSpeed::Speed25G => LinkSpeed::Speed25G,
            DpdPortSpeed::Speed40G => LinkSpeed::Speed40G,
            DpdPortSpeed::Speed50G => LinkSpeed::Speed50G,
            DpdPortSpeed::Speed100G => LinkSpeed::Speed100G,
            DpdPortSpeed::Speed200G => LinkSpeed::Speed200G,
            DpdPortSpeed::Speed400G => LinkSpeed::Speed400G,
        };

        // dont consider link local addresses in change computation
        let addrs = sole_link
            .addrs
            .into_iter()
            .filter(|ip| match ip {
                IpAddr::V6(ip) if ip.is_unicast_link_local() => false,
                _ => true,
            })
            .collect();

        Ok(Self {
            port_id: port_id.to_string(),
            autoneg: sole_link.params.autoneg,
            tx_eq,
            fec,
            speed,
            addrs,
        })
    }
}

impl From<&'_ DiffablePortSettings> for DpdPortSettings {
    fn from(port: &DiffablePortSettings) -> Self {
        let autoneg = port.autoneg;
        let addrs = port.addrs.iter().copied().collect();
        let kr = false; //NOTE: kr does not apply to user configurable links.

        let fec = port.fec.map(|f| match f {
            LinkFec::Firecode => DpdPortFec::Firecode,
            LinkFec::None => DpdPortFec::None,
            LinkFec::Rs => DpdPortFec::Rs,
        });

        let speed = match port.speed {
            LinkSpeed::Speed0G => DpdPortSpeed::Speed0G,
            LinkSpeed::Speed1G => DpdPortSpeed::Speed1G,
            LinkSpeed::Speed10G => DpdPortSpeed::Speed10G,
            LinkSpeed::Speed25G => DpdPortSpeed::Speed25G,
            LinkSpeed::Speed40G => DpdPortSpeed::Speed40G,
            LinkSpeed::Speed50G => DpdPortSpeed::Speed50G,
            LinkSpeed::Speed100G => DpdPortSpeed::Speed100G,
            LinkSpeed::Speed200G => DpdPortSpeed::Speed200G,
            LinkSpeed::Speed400G => DpdPortSpeed::Speed400G,
        };

        let tx_eq = port.tx_eq.map(|t| DpdTxEq {
            main: t.main,
            post1: t.post1,
            post2: t.post2,
            pre1: t.pre1,
            pre2: t.pre2,
        });

        // TODO breakouts?
        let mut links = HashMap::with_capacity(1);
        let link_id = DpdLinkId(0);
        links.insert(
            link_id.to_string(),
            DpdLinkSettings {
                addrs,
                params: DpdLinkCreate {
                    autoneg,
                    fec,
                    kr,
                    lane: Some(link_id),
                    speed,
                    tx_eq,
                },
            },
        );

        DpdPortSettings { links }
    }
}

#[cfg(test)]
mod tests;
