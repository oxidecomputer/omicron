// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler for configuration of `lldpd` within a scrimlet's switch zone.
//!
//! Unlike most reconcilers in this crate, `lldpd`'s configuration is managed
//! via SMF, not a dropshot server.

use crate::SWITCH_ZONE_NAME;
use crate::ScrimletReconcilersMode;
use crate::reconciler_task::Reconciler;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use anyhow::anyhow;
use anyhow::bail;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::lldpd::LldpdReconcilerStatus;
use scuffle::AddPropertyGroupFlags;
use scuffle::EditPropertyGroups;
use scuffle::HasComposedPropertyGroups;
use scuffle::HasDirectPropertyGroups;
use scuffle::Instance;
use scuffle::PropertyGroupType;
use scuffle::Scf;
use scuffle::Value;
use scuffle::ValueKind;
use scuffle::ValueRef;
use scuffle::error::SingleValueError;
use sled_agent_types::early_networking::LldpAdminStatus;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::system_networking::SystemNetworkingConfig;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

// SMF service/instance names
const LLDPD_SERVICE_NAME: &str = "oxide/lldpd";
const LLDPD_INSTANCE_NAME: &str = "default";

// All the properties we set live underneath property groups named
// `port_{port_name}`.
const LLDPD_PORT_PG_PREFIX: &str = "port_";

mod property_name {
    pub(super) const STATUS: &str = "status";
    pub(super) const CHASSIS_ID: &str = "chassis_id";
    pub(super) const PORT_ID: &str = "port_id";
    pub(super) const PORT_DESCRIPTION: &str = "port_description";
    pub(super) const SYSTEM_NAME: &str = "system_name";
    pub(super) const SYSTEM_DESCRIPTION: &str = "system_description";
    pub(super) const MANAGEMENT_ADDRS: &str = "management_addrs";
}

#[derive(Debug)]
pub(crate) struct LldpdReconciler {
    switch_slot: ThisSledSwitchSlot,
    is_running_in_test_mode: bool,
}

impl Reconciler for LldpdReconciler {
    type Status = LldpdReconcilerStatus;

    const LOGGER_COMPONENT_NAME: &'static str = "LldpdReconciler";
    const RE_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

    fn new(
        mode: ScrimletReconcilersMode,
        switch_slot: ThisSledSwitchSlot,
        _parent_log: &slog::Logger,
    ) -> Self {
        let is_running_in_test_mode = match mode {
            ScrimletReconcilersMode::SwitchZone(_) => false,
            #[cfg(any(test, feature = "testing"))]
            ScrimletReconcilersMode::Test { .. } => true,
        };
        Self { switch_slot, is_running_in_test_mode }
    }

    async fn do_reconciliation(
        &mut self,
        system_networking_config: &SystemNetworkingConfig,
        log: &slog::Logger,
    ) -> Self::Status {
        if self.is_running_in_test_mode {
            // We can't run SMF-based reconcilers in test mode. We could thread
            // through a separate status variant just for this, but that'd be a
            // little painful for upstream callers; it's probably okay to report
            // any status, so we pick `Failed` to give a description of why we
            // didn't run.
            return LldpdReconcilerStatus::Failed(
                "reconciliation disabled \
                 (SMF within the switch zone not available in test mode)"
                    .to_string(),
            );
        }

        let result = {
            let config = system_networking_config.rack_network_config.clone();
            let log = log.clone();
            let switch_slot = self.switch_slot;
            tokio::task::spawn_blocking(move || {
                do_reconciliation_blocking(&config, switch_slot, &log)
            })
            .await
        };

        match result {
            Ok(Ok(status)) => status,
            Ok(Err(err)) => LldpdReconcilerStatus::Failed(format!(
                "reconciliation failed: {}",
                InlineErrorChain::new(&*err)
            )),
            Err(err) => {
                // We don't expect any of these cases, except possibly during
                // tokio runtime shutdown in unit tests, but it's easy to map
                // them to errors instead of panicking ourselves.
                if err.is_cancelled() {
                    LldpdReconcilerStatus::Failed(
                        "do_reconciliation_blocking() \
                         was cancelled unexpectedly"
                            .to_string(),
                    )
                } else if err.is_panic() {
                    LldpdReconcilerStatus::Failed(
                        "do_reconciliation_blocking() panicked".to_string(),
                    )
                } else {
                    LldpdReconcilerStatus::Failed(
                        "tokio::task::spawn_blocking() failed without \
                         being cancelled or panicking ?!"
                            .to_string(),
                    )
                }
            }
        }
    }
}

fn do_reconciliation_blocking(
    config: &RackNetworkConfig,
    our_switch_slot: ThisSledSwitchSlot,
    log: &Logger,
) -> anyhow::Result<LldpdReconcilerStatus> {
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
) -> anyhow::Result<LldpdReconcilerStatus> {
    let scope = scf.scope_local()?;
    let Some(service) = scope.service(LLDPD_SERVICE_NAME)? else {
        bail!("no `{LLDPD_SERVICE_NAME}` service present in switch zone");
    };
    let Some(mut instance) = service.instance(LLDPD_INSTANCE_NAME)? else {
        bail!("no `{LLDPD_INSTANCE_NAME}` instance within {}", service.fmri());
    };

    info!(log, "reading properties from running snapshot");
    let current_properties = current_port_configs(&instance, log)?;
    let (desired_properties, ports_by_status) =
        desired_port_configs(config, our_switch_slot);

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
    // wholesale without computing a precise diff. `apply_config()` deletes all
    // port property groups entirely, then creates only the ones described by
    // `desired_properties`.
    if current_properties == desired_properties {
        Ok(LldpdReconcilerStatus::SkippedConfigUpToDate)
    } else {
        info!(log, "applying new configuration");
        apply_config(&mut instance, &desired_properties)?;
        Ok(LldpdReconcilerStatus::Reconciled { ports: ports_by_status })
    }
}

fn apply_config(
    instance: &mut Instance<'_>,
    ports: &BTreeMap<String, DiffablePortConfig>,
) -> anyhow::Result<()> {
    // Delete all preexisting `port_*` property groups.
    let mut pgs_to_delete = Vec::new();
    for pg in instance.property_groups_direct()? {
        let pg = pg?;
        if pg.name().starts_with(LLDPD_PORT_PG_PREFIX) {
            pgs_to_delete.push(pg.name().to_owned());
        }
    }
    for pg_name in pgs_to_delete {
        instance.delete_property_group(&pg_name)?;
    }

    // Add new property groups for all desired ports.
    for (pg_name, config) in ports {
        let mut pg = instance.add_property_group(
            pg_name,
            PropertyGroupType::Application,
            AddPropertyGroupFlags::Persistent,
        )?;

        let mut tx = pg.transaction()?.start()?;
        for (field_name, field_value) in config.string_fields() {
            if let Some(field_value) = field_value {
                tx.property_new(field_name, ValueRef::AString(field_value))?;
            }
        }
        if !config.management_addrs.is_empty() {
            tx.property_new_multiple(
                property_name::MANAGEMENT_ADDRS,
                ValueKind::AString,
                config.management_addrs.iter().map(|ip| ValueRef::AString(ip)),
            )?;
        }
        tx.commit()?;
    }

    instance.smf_refresh()?;

    Ok(())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DiffablePortConfig {
    status: Option<String>,
    chassis_id: Option<String>,
    port_id: Option<String>,
    port_description: Option<String>,
    system_name: Option<String>,
    system_description: Option<String>,
    management_addrs: BTreeSet<String>,
}

impl DiffablePortConfig {
    fn string_fields(
        &self,
    ) -> impl Iterator<Item = (&'static str, Option<&str>)> {
        [
            (property_name::STATUS, self.status.as_deref()),
            (property_name::CHASSIS_ID, self.chassis_id.as_deref()),
            (property_name::PORT_ID, self.port_id.as_deref()),
            (property_name::PORT_DESCRIPTION, self.port_description.as_deref()),
            (property_name::SYSTEM_NAME, self.system_name.as_deref()),
            (
                property_name::SYSTEM_DESCRIPTION,
                self.system_description.as_deref(),
            ),
        ]
        .into_iter()
    }

    fn string_fields_mut(
        &mut self,
    ) -> impl Iterator<Item = (&'static str, &mut Option<String>)> {
        [
            (property_name::STATUS, &mut self.status),
            (property_name::CHASSIS_ID, &mut self.chassis_id),
            (property_name::PORT_ID, &mut self.port_id),
            (property_name::PORT_DESCRIPTION, &mut self.port_description),
            (property_name::SYSTEM_NAME, &mut self.system_name),
            (property_name::SYSTEM_DESCRIPTION, &mut self.system_description),
        ]
        .into_iter()
    }
}

fn current_port_configs(
    instance: &Instance<'_>,
    log: &Logger,
) -> anyhow::Result<BTreeMap<String, DiffablePortConfig>> {
    let snapshot = instance
        .snapshot("running")?
        .ok_or_else(|| anyhow!("no running snapshot for lldpd"))?;

    let mut port_configs = BTreeMap::new();

    for pg in snapshot.property_groups_composed()? {
        let pg = pg?;

        // skip property groups that we're not responsible for reconciling
        if !pg.name().starts_with(LLDPD_PORT_PG_PREFIX) {
            continue;
        }

        let mut config = DiffablePortConfig::default();
        for (prop_name, destination) in config.string_fields_mut() {
            let Some(prop) = pg.property(prop_name)? else {
                continue;
            };
            match prop.single_value() {
                Ok(Value::AString(value)) => {
                    *destination = Some(value);
                }
                Ok(value) => {
                    warn!(
                        log,
                        "ignoring unexpected SMF property value type";
                        "property" => prop.fmri(),
                        "type" => %value.kind(),
                        "value" => %value.display_smf(),
                    );
                }
                Err(SingleValueError::NoValues { .. }) => {
                    warn!(
                        log, "SMF property exists but has no values";
                        "property" => prop.fmri(),
                    );
                }
                Err(SingleValueError::MultipleValues { .. }) => {
                    warn!(
                        log,
                        "ignoring SMF property with multiple values";
                        "property" => prop.fmri(),
                    );
                }
                Err(err @ SingleValueError::IterError(_)) => bail!(err),
            }
        }

        if let Some(prop) = pg.property(property_name::MANAGEMENT_ADDRS)? {
            for value in prop.values()? {
                match value? {
                    Value::AString(value) => {
                        config.management_addrs.insert(value);
                    }
                    other => {
                        warn!(
                            log,
                            "ignoring unexpected SMF property value type";
                            "property" => prop.fmri(),
                            "type" => %other.kind(),
                            "value" => %other.display_smf(),
                        );
                    }
                }
            }
        }

        port_configs.insert(pg.name().to_string(), config);
    }

    Ok(port_configs)
}

fn desired_port_configs(
    rack_config: &RackNetworkConfig,
    our_switch_slot: ThisSledSwitchSlot,
) -> (BTreeMap<String, DiffablePortConfig>, BTreeMap<String, LldpAdminStatus>) {
    let mut desired_ports = BTreeMap::new();
    let mut ports_by_status = BTreeMap::new();

    for port_config in
        rack_config.ports.iter().filter(|port| port.switch == our_switch_slot)
    {
        let Some(config) = port_config.lldp.as_ref() else {
            continue;
        };

        ports_by_status.insert(port_config.port.clone(), config.status);

        let name = format!("{LLDPD_PORT_PG_PREFIX}{}", port_config.port);
        let value = DiffablePortConfig {
            status: Some(config.status.to_lldpd_smf_property().to_owned()),
            chassis_id: config.chassis_id.clone(),
            port_id: config.port_id.clone(),
            port_description: config.port_description.clone(),
            system_name: config.system_name.clone(),
            system_description: config.system_description.clone(),
            management_addrs: config
                .management_addrs
                .iter()
                .flatten()
                .map(|ip| ip.to_string())
                .collect(),
        };
        desired_ports.insert(name, value);
    }

    (desired_ports, ports_by_status)
}

// Our tests require smf, which is only available on illumos.
#[cfg(all(test, target_os = "illumos"))]
mod tests;
