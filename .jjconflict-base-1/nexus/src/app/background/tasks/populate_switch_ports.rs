// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for populating the `switch_port` table.
//!
//! Unlike most Nexus background tasks, this task is a critical part of rack
//! setup, and does nothing after rack setup. This is a bit strange, so for
//! context we'll provide both history and "current shortcomings we eventually
//! need to address".
//!
//! There is a database table named `switch_port`. It's expected to contain the
//! names of the front-facing ports that can be configured by operators for each
//! switch. Today, this list is _completely static_ for a given rack, although
//! it differs between environments (e.g., real sidecars always have `qsfp0`
//! through `qsfp31`; some test environments, like the `helios/deploy` CI job,
//! only have `qsfp0`). A client can determine the list by querying dendrite
//! (`dpd`, via the `port_list()` endpoint).
//!
//! Inserting networking configuration to the database requires this table to be
//! populated. Various other tables take port names as part of their
//! configuration, and the inserts require that the port name actually exist on
//! the switch to be a valid configuration.
//!
//! The initial set of networking configuration provided at RSS time is inserted
//! by Nexus inside the handoff endpoint called by RSS (running in sled-agent).
//! This means for RSS to succeed, the `switch_port` table must be populated
//! with at least the names of the ports used in that initial networking
//! configuration.
//!
//! Prior to the existence of this background task, the responsibility for
//! populating `switch_port` was split between RSS and Nexus:
//!
//! 1. RSS would build a `HashMap<SwitchSlot, Ipv6Addr>` specifying the IP
//!    addresses of one or both switch zones.
//! 2. Nexus, inside the handoff endpoint, would query `dpd` for each of the
//!    entries in that hash map to populate `switch_port` prior to inserting the
//!    rest of the networking configuration.
//!
//! Historically, this caused some problems. RSS didn't know whether it needed
//! to find the IPs of both switch zones or whether it was sufficient to find
//! just one. Originally it would try to find both for some amount of time, then
//! give up and proceed as long as it had found one; this led to RSS handoff
//! failures if the initial networking configuration contained ports on the
//! switch it didn't find within the timeout (omicron#9678). This was fixed by
//! forcing RSS to wait until it discovered the IP of any switch that had a
//! configured port in the initial network config.
//!
//! That still left a gap: it was possible to provide an initial network config
//! that only used a port on switch0 (or switch1), RSS would only specify an IP
//! address for that switch, and therefore Nexus would only fill in the
//! `switch_port` tables from that one switch. Any future attempt to configure a
//! port on the other switch would never succeed, because nothing in Nexus ever
//! came back to fill in "the other switch" in the `switch_port` table.
//!
//! This background task fixes that problem. When Nexus starts, it will
//! periodically activate this task, and it will attempt to find both switch
//! zones, ask `dpd` for the list of ports, and populate `switch_port`. It will
//! go inert once it's successfully contacted `dpd` in both switch zones, but
//! will keep trying forever until it has done so. This means we can perform an
//! RSS handoff with only one switch present and configured, then later add a
//! second switch, and this task will fill in that switch's ports in
//! `switch_port`.
//!
//! There are still some things here that aren't ideal:
//!
//! 1. This task _must_ complete successfully for any switches that have ports
//!    configured in the RSS network config in order for the RSS handoff to
//!    complete. (This is consistent with the prior behavior of RSS blocking
//!    until it found the switch zone IPs and then Nexus having to contact `dpd`
//!    in those switch zone(s), but it would be nicer if we could proceed
//!    through rack setup even in a degraded state.)
//! 2. This task assumes the set of switch port names never changes throughout
//!    the lifetime of the rack. This is consistent with our usage of the
//!    `switch_port` table; e.g., we don't have `DataStore` methods to delete
//!    rows from it. But eventually we'll either have a non-sidecar switch that
//!    may have different names, or we may want to break out additional ports on
//!    sidecars, and we'll need to be able to handle the names changing at
//!    runtime. This task is not set up to do that and will need some rework if
//!    and when that world arrives.

use crate::app::background::BackgroundTask;
use crate::app::dpd_clients;
use anyhow::Context;
use anyhow::anyhow;
use dpd_client::Client as DpdClient;
use dpd_client::types::PortId as DpdPortId;
use futures::future::BoxFuture;
use internal_dns_resolver::Resolver;
use nexus_auth::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::internal_api::background::SwitchPortPopulatorStatus;
use nexus_types::internal_api::background::SwitchPortPopulatorStatusKind;
use omicron_common::api::external;
use omicron_common::api::external::Name;
use omicron_uuid_kinds::RackUuid;
use sled_agent_types::early_networking::SwitchSlot;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

pub struct SwitchPortPopulator {
    rack_id: RackUuid,
    datastore: Arc<DataStore>,
    resolver: Resolver,
    populated_switches: BTreeSet<SwitchSlot>,
}

impl SwitchPortPopulator {
    pub fn new(
        rack_id: RackUuid,
        datastore: Arc<DataStore>,
        resolver: Resolver,
    ) -> Self {
        Self {
            rack_id,
            datastore,
            resolver,
            populated_switches: BTreeSet::new(),
        }
    }

    async fn activate_impl(
        &mut self,
        opctx: &OpContext,
    ) -> SwitchPortPopulatorStatus {
        // Short circuit - if we've already successfully contacted both switch
        // zones and populated the switch ports for them, we have nothing to do
        // ever again.
        if self.populated_switches.contains(&SwitchSlot::Switch0)
            && self.populated_switches.contains(&SwitchSlot::Switch1)
        {
            return SwitchPortPopulatorStatus {
                switch0: Ok(SwitchPortPopulatorStatusKind::PreviouslyPopulated),
                switch1: Ok(SwitchPortPopulatorStatusKind::PreviouslyPopulated),
            };
        }

        // Look up dendrite clients, keyed by which switch they're managing.
        let clients_by_switch = match dpd_clients(&self.resolver, &opctx.log)
            .await
        {
            Ok(clients) => clients,
            Err(err) => {
                // Ensure we're forced to update this logging (i.e., to
                // `InlineErrorChain`) if `dpd_clients()` starts returning a
                // different error type.
                let err: &str = &err;
                warn!(
                    opctx.log, "failed to look up dendrite clients";
                    "error" => err,
                );
                let err = format!("failed to look up dendrite clients: {err}");
                return SwitchPortPopulatorStatus {
                    switch0: Err(err.clone()),
                    switch1: Err(err),
                };
            }
        };

        // Populate each switch, unless we've already done so successfully.
        let switch0 = self
            .populate_one_switch_if_needed(
                SwitchSlot::Switch0,
                &clients_by_switch,
                opctx,
            )
            .await;
        let switch1 = self
            .populate_one_switch_if_needed(
                SwitchSlot::Switch1,
                &clients_by_switch,
                opctx,
            )
            .await;

        SwitchPortPopulatorStatus { switch0, switch1 }
    }

    async fn populate_one_switch_if_needed(
        &mut self,
        which_switch: SwitchSlot,
        clients_by_switch: &HashMap<SwitchSlot, DpdClient>,
        opctx: &OpContext,
    ) -> Result<SwitchPortPopulatorStatusKind, String> {
        if self.populated_switches.contains(&which_switch) {
            return Ok(SwitchPortPopulatorStatusKind::PreviouslyPopulated);
        }

        let Some(client) = clients_by_switch.get(&which_switch) else {
            warn!(
                opctx.log,
                "skipping (needed!) switch port population - \
                 no dpd client found for this switch slot";
                "switch-slot" => ?which_switch,
            );
            return Err("no dpd client found".to_string());
        };

        match self
            .try_populate_one_switch_via_dpd(which_switch, client, opctx)
            .await
        {
            Ok(num_ports) => {
                self.populated_switches.insert(which_switch);
                Ok(SwitchPortPopulatorStatusKind::Populated { num_ports })
            }
            Err(err) => {
                warn!(
                    opctx.log, "failed to populate switch ports";
                    "switch-slot" => ?which_switch,
                    InlineErrorChain::new(&*err),
                );
                Err(format!(
                    "failed to populate switch ports: {}",
                    InlineErrorChain::new(&*err),
                ))
            }
        }
    }

    async fn try_populate_one_switch_via_dpd(
        &self,
        which_switch: SwitchSlot,
        client: &DpdClient,
        opctx: &OpContext,
    ) -> anyhow::Result<usize> {
        let all_ports =
            client.port_list().await.context("failed to list ports")?;

        info!(
            opctx.log, "discovered ports for switch";
            "switch-slot" => ?which_switch,
            "all-ports" => ?all_ports,
        );

        let qsfp_ports = all_ports
            .iter()
            .filter_map(|port| match port {
                DpdPortId::Internal(_) | DpdPortId::Rear(_) => None,
                DpdPortId::Qsfp(_) => match port.to_string().parse::<Name>() {
                    Ok(name) => Some(name),
                    Err(err) => {
                        // Ensure we're forced to update this logging (i.e., to
                        // `InlineErrorChain`) if `parse()` starts returning a
                        // different error type.
                        let err: &str = &err;
                        warn!(
                            opctx.log, "failed to parse port as `Name`";
                            "port" => %port,
                            "error" => err,
                        );
                        None
                    }
                },
            })
            .collect::<Vec<_>>();

        // Try to insert all the ports, tracking how many insertions were
        // successful, and keep only the last error (if any). If any insert
        // fails, we'll return an error; otherwise, we return the total number
        // of ports successfully inserted.
        let mut num_inserted = 0;
        let mut last_error = None;
        for port_name in qsfp_ports {
            match self
                .datastore
                .switch_port_create(
                    opctx,
                    self.rack_id,
                    which_switch,
                    port_name.clone().into(),
                )
                .await
            {
                Ok(_) | Err(external::Error::ObjectAlreadyExists { .. }) => {
                    num_inserted += 1;
                }
                Err(err) => {
                    last_error =
                        Some(anyhow!(err).context(format!(
                            "failed to insert port {port_name}"
                        )));
                }
            }
        }

        if let Some(err) = last_error { Err(err) } else { Ok(num_inserted) }
    }
}

impl BackgroundTask for SwitchPortPopulator {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(async move {
            let status = self.activate_impl(opctx).await;
            match serde_json::to_value(status) {
                Ok(val) => val,
                Err(err) => {
                    let error = format!(
                        "could not serialize task status: {}",
                        InlineErrorChain::new(&err)
                    );
                    serde_json::json!({ "error": error })
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils_macros::nexus_test;
    use slog::o;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    /// Exercises the example from our module's toplevel docs: switch0's
    /// dendrite is reachable from the start and switch1's comes online later.
    ///
    /// The Nexus process has its own internal `SwitchPortPopulator` that
    /// already ran against both switches at boot; this test instantiates a
    /// fresh populator and drives it directly so we can observe the
    /// only-switch0-then-both transition.
    #[nexus_test(server = crate::Server, extra_sled_agents = 1)]
    async fn test_switch1_comes_up_late(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.server_context().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.new(o!()),
            datastore.clone(),
        );

        // Stop switch1's dendrite. The internal-DNS SRV record for it
        // remains; `dpd_clients()` will still try to resolve and connect,
        // but `switch_identifiers()` will fail and the slot will be
        // dropped from the resulting map.
        cptestctx.stop_dendrite(SwitchSlot::Switch1).await;

        let mut task = SwitchPortPopulator::new(
            nexus.rack_id(),
            datastore.clone(),
            nexus.resolver().clone(),
        );

        // Phase 1: only switch0 reachable.
        let v1: SwitchPortPopulatorStatus =
            serde_json::from_value(task.activate(&opctx).await)
                .expect("status deserializes");
        assert_matches!(
            v1.switch0,
            Ok(SwitchPortPopulatorStatusKind::Populated { .. })
        );
        assert_matches!(
            v1.switch1,
            Err(e) if e == "no dpd client found"
        );

        // Phase 2: re-activate with switch1 still down. Switch0 should
        // short-circuit (no work), switch1 should still not be populated.
        let v2: SwitchPortPopulatorStatus =
            serde_json::from_value(task.activate(&opctx).await)
                .expect("status deserializes");
        assert_matches!(
            v2.switch0,
            Ok(SwitchPortPopulatorStatusKind::PreviouslyPopulated)
        );
        assert_matches!(
            v2.switch1,
            Err(e) if e == "no dpd client found"
        );

        // Phase 3: switch1 comes online. `restart_dendrite` waits for
        // `switch_identifiers()` to succeed before returning, so by the
        // time it returns, the next `dpd_clients()` call will pick up
        // switch1.
        cptestctx.restart_dendrite(SwitchSlot::Switch1).await;

        let v3: SwitchPortPopulatorStatus =
            serde_json::from_value(task.activate(&opctx).await)
                .expect("status deserializes");
        assert_matches!(
            v3.switch0,
            Ok(SwitchPortPopulatorStatusKind::PreviouslyPopulated)
        );
        assert_matches!(
            v3.switch1,
            Ok(SwitchPortPopulatorStatusKind::Populated { .. })
        );

        // Phase 4: short-circuit. The task should bail out early without
        // touching dendrite or the database.
        let v4: SwitchPortPopulatorStatus =
            serde_json::from_value(task.activate(&opctx).await)
                .expect("status deserializes");
        assert_matches!(
            v4.switch0,
            Ok(SwitchPortPopulatorStatusKind::PreviouslyPopulated)
        );
        assert_matches!(
            v4.switch1,
            Ok(SwitchPortPopulatorStatusKind::PreviouslyPopulated)
        );
    }
}
