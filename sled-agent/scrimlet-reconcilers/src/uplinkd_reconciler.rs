// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler for configuration of `uplinkd` within a scrimlet's switch zone.
//!
//! Unlike most reconcilers in this crate, `uplinkd`'s configuration is managed
//! via SMF, not a dropshot server.

use crate::ThisSledSwitchZoneUnderlayIpAddr;
use crate::reconciler_task::Reconciler;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use anyhow::anyhow;
use anyhow::bail;
use scuffle::AddPropertyGroupFlags;
use scuffle::EditPropertyGroups;
use scuffle::HasComposedPropertyGroups;
use scuffle::Instance;
use scuffle::PropertyGroupType;
use scuffle::Scf;
use scuffle::Value;
use scuffle::ValueKind;
use scuffle::ValueRef;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::time::Duration;

// TODO-cleanup don't hardcode zone name?
const SWITCH_ZONE_NAME: &str = "oxz_switch";
const UPLINK_SERVICE_NAME: &str = "oxide/uplink";
const UPLINK_INSTANCE_NAME: &str = "default";
const UPLINKS_PG_NAME: &str = "uplinks";

#[derive(Debug, Clone)]
pub enum UplinkdReconcilerStatus {
    Failed(String),
    SkippedConfigUpToDate,
    Reconciled { ports: BTreeMap<String, Vec<String>> },
}

impl slog::KV for UplinkdReconcilerStatus {
    fn serialize(
        &self,
        _record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        match self {
            UplinkdReconcilerStatus::Failed(reason) => {
                serializer.emit_str("uplinkd".into(), &reason)
            }
            UplinkdReconcilerStatus::SkippedConfigUpToDate => serializer
                .emit_str("uplinkd".into(), "skipped: config up-to-date"),
            UplinkdReconcilerStatus::Reconciled { ports } => serializer
                .emit_usize("uplinkd-reconciled-ports".into(), ports.len()),
        }
    }
}

pub(crate) struct UplinkdReconciler {
    switch_slot: ThisSledSwitchSlot,
}

impl Reconciler for UplinkdReconciler {
    type Status = UplinkdReconcilerStatus;

    const LOGGER_COMPONENT_NAME: &'static str = "UplinkdReconciler";
    const RE_RECONCILE_INTERVAL: std::time::Duration = Duration::from_secs(30);

    fn new(
        _switch_zone_underlay_ip: ThisSledSwitchZoneUnderlayIpAddr,
        switch_slot: ThisSledSwitchSlot,
        _parent_log: &Logger,
    ) -> Self {
        Self { switch_slot }
    }

    async fn do_reconciliation(
        &mut self,
        system_networking_config: &SystemNetworkingConfig,
        log: &Logger,
    ) -> Self::Status {
        let result = {
            let config = system_networking_config.rack_network_config.clone();
            let log = log.clone();
            let switch_slot = self.switch_slot;
            tokio::task::spawn_blocking(move || {
                do_reconciliation_blocking(&config, switch_slot, &log)
            })
            .await
        };

        let status = match result {
            Ok(Ok(status)) => status,
            Ok(Err(err)) => UplinkdReconcilerStatus::Failed(format!(
                "reconciliation failed: {}",
                InlineErrorChain::new(&*err)
            )),
            Err(err) => {
                // We don't expect any of these cases, except possibly during
                // tokio runtime shutdown in unit tests, but it's easy to map
                // them to errors instead of panicking ourselves.
                if err.is_cancelled() {
                    UplinkdReconcilerStatus::Failed(
                        "do_reconciliation_blocking() \
                         was cancelled unexpectedly"
                            .to_string(),
                    )
                } else if err.is_panic() {
                    UplinkdReconcilerStatus::Failed(
                        "do_reconciliation_blocking() panicked".to_string(),
                    )
                } else {
                    UplinkdReconcilerStatus::Failed(
                        "tokio::task::spawn_blocking() failed without \
                         being cancelled or panicking ?!"
                            .to_string(),
                    )
                }
            }
        };

        info!(
            log, "uplinkd reconciliation completed";
            &status,
        );

        status
    }
}

fn do_reconciliation_blocking(
    config: &RackNetworkConfig,
    our_switch_slot: ThisSledSwitchSlot,
    log: &Logger,
) -> anyhow::Result<UplinkdReconcilerStatus> {
    // Connect to SMF inside the zone.
    let scf = Scf::connect_zone(SWITCH_ZONE_NAME)?;

    // Forward to the real function; it's separate so unit tests can connect to
    // an isolated `svc.configd` instead of requiring a real switch zone.
    do_reconciliation_blocking_impl(&scf, config, our_switch_slot, log)
}

fn do_reconciliation_blocking_impl(
    scf: &Scf<'_>,
    config: &RackNetworkConfig,
    our_switch_slot: ThisSledSwitchSlot,
    log: &Logger,
) -> anyhow::Result<UplinkdReconcilerStatus> {
    let scope = scf.scope_local()?;
    let Some(service) = scope.service(UPLINK_SERVICE_NAME)? else {
        bail!("no `{UPLINK_SERVICE_NAME}` service present in switch zone");
    };
    let Some(mut instance) = service.instance(UPLINK_INSTANCE_NAME)? else {
        bail!("no `{UPLINK_INSTANCE_NAME}` instance within {}", service.fmri());
    };

    info!(log, "reading properties from running snapshot");
    let current_properties = current_uplink_addresses(&instance)?;
    let desired_properties = desired_uplink_addresses(config, our_switch_slot);

    // We could compute exact diffs to know exactly what properties need to be
    // removed / added / changed, but we'd need to compute _two_ diffs:
    //
    // 1. Does the running snapshot match the desired config? If so, nothing to
    //    do.
    // 2. If not, we have to compute a diff against the current instance
    //    properties, not the running snapshot - it's possible a previous
    //    reconciliation attempt changed the instance properties but failed to
    //    refresh, so this could be in any previous state.
    //
    // Instead, we do something simpler: if the running snapshot matches the
    // desired config, we do nothing; otherwise, we apply the new config
    // wholesale without computing a precise diff. `apply_config()` deletes the
    // `UPLINKS_PG_NAME` property group entirely, then recreates it with only
    // our desired values.
    if current_properties == desired_properties {
        Ok(UplinkdReconcilerStatus::SkippedConfigUpToDate)
    } else {
        info!(log, "applying new configuration");
        apply_config(&mut instance, &desired_properties)?;
        Ok(UplinkdReconcilerStatus::Reconciled { ports: desired_properties })
    }
}

fn apply_config(
    instance: &mut Instance<'_>,
    ports: &BTreeMap<String, Vec<String>>,
) -> anyhow::Result<()> {
    instance.delete_property_group(UPLINKS_PG_NAME)?;

    {
        let mut pg = instance.add_property_group(
            UPLINKS_PG_NAME,
            PropertyGroupType::Application,
            AddPropertyGroupFlags::Persistent,
        )?;

        let mut tx = pg.transaction()?.start()?;
        for (port, addrs) in ports {
            tx.property_new_multiple(
                port,
                ValueKind::AString,
                addrs.iter().map(|addr| ValueRef::AString(addr)),
            )?;
        }

        tx.commit()?;
    }

    instance.refresh()?;

    Ok(())
}

fn desired_uplink_addresses(
    config: &RackNetworkConfig,
    our_switch_slot: ThisSledSwitchSlot,
) -> BTreeMap<String, Vec<String>> {
    config
        .ports
        .iter()
        .filter(|port| port.switch == our_switch_slot)
        .filter_map(|port_config| {
            let values = port_config
                .addresses
                .iter()
                .map(|addr| addr.to_uplinkd_smf_property())
                .collect::<Vec<_>>();

            if values.is_empty() {
                None
            } else {
                Some((format!("{}_0", port_config.port), values))
            }
        })
        .collect()
}

fn current_uplink_addresses(
    instance: &Instance<'_>,
) -> anyhow::Result<BTreeMap<String, Vec<String>>> {
    let snapshot = instance
        .snapshot("running")?
        .ok_or_else(|| anyhow!("no running snapshot for uplinkd"))?;

    let Some(pg) = snapshot.property_group_composed("uplinks")? else {
        return Ok(BTreeMap::new());
    };

    let properties = pg
        .properties()?
        .map(|prop| {
            let prop = prop?;
            let addrs = prop
                .values()?
                .map(|value| match value? {
                    Value::AString(addr) => Ok(addr),
                    other => {
                        bail!(
                            "unexpected address value kind `{}` \
                             (expected `astring`)",
                            other.kind()
                        );
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok::<_, anyhow::Error>((prop.name().to_string(), addrs))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;

    Ok(properties)
}

// Our tests require smf, which is only available on illumos.
#[cfg(all(test, target_os = "illumos"))]
mod tests;
