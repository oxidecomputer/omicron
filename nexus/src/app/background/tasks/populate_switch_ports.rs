// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for populating the `switch_port` table.
//!
//! TODO-john EXPAND

use crate::app::background::BackgroundTask;
use crate::app::dpd_clients;
use anyhow::Context;
use anyhow::bail;
use dpd_client::Client as DpdClient;
use dpd_client::types::PortId as DpdPortId;
use futures::future::BoxFuture;
use internal_dns_resolver::Resolver;
use nexus_auth::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::Name;
use sled_agent_types::early_networking::SwitchSlot;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use uuid::Uuid;

const NUM_EXPECTED_QSFP_PORTS: usize = 32;

pub struct SwitchPortPopulator {
    rack_id: Uuid,
    datastore: Arc<DataStore>,
    resolver: Resolver,
    did_populate_switch0: AtomicBool,
    did_populate_switch1: AtomicBool,
}

impl SwitchPortPopulator {
    pub fn new(
        rack_id: Uuid,
        datastore: Arc<DataStore>,
        resolver: Resolver,
    ) -> Self {
        Self {
            rack_id,
            datastore,
            resolver,
            did_populate_switch0: AtomicBool::new(false),
            did_populate_switch1: AtomicBool::new(false),
        }
    }

    async fn activate_impl(&self, opctx: &OpContext) {
        let need_to_populate_switch0 =
            !self.did_populate_switch0.load(Ordering::Relaxed);
        let need_to_populate_switch1 =
            !self.did_populate_switch1.load(Ordering::Relaxed);

        // Short circuit - if we've already successfully contacted both switch
        // zones and populated the switch ports for them, we have nothing to do
        // ever again.
        if !need_to_populate_switch0 && !need_to_populate_switch1 {
            return;
        }

        // Look up dendrite clients, keyed by which switch they're managing.
        let clients_by_switch =
            match dpd_clients(&self.resolver, &opctx.log).await {
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
                    return;
                }
            };

        // Populate each switch, unless we've already done so successfully.
        for (is_needed, which_switch) in [
            (need_to_populate_switch0, SwitchSlot::Switch0),
            (need_to_populate_switch1, SwitchSlot::Switch1),
        ] {
            if is_needed
                && let Some(client) = clients_by_switch.get(&which_switch)
            {
                match self
                    .try_populate_via_dpd(which_switch, client, opctx)
                    .await
                {
                    Ok(()) => {
                        let success_bool = match which_switch {
                            SwitchSlot::Switch0 => &self.did_populate_switch0,
                            SwitchSlot::Switch1 => &self.did_populate_switch1,
                        };
                        success_bool.store(true, Ordering::Relaxed);
                    }
                    Err(err) => {
                        warn!(
                            opctx.log, "failed to populate switch ports";
                            "switch-slot" => ?which_switch,
                            InlineErrorChain::new(&*err),
                        );
                    }
                }
            }
        }
    }

    async fn try_populate_via_dpd(
        &self,
        which_switch: SwitchSlot,
        client: &DpdClient,
        opctx: &OpContext,
    ) -> anyhow::Result<()> {
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

        // See the module-level comments: This task assumes we have a one-time,
        // static list of QSFP ports on each switch, and only attempts to insert
        // them once. Confirm we get the expected number of ports from dpd; if
        // we don't, this task needs to be reworked!
        if qsfp_ports.len() != NUM_EXPECTED_QSFP_PORTS {
            bail!(
                "unexpected number of QSFP ports: got {} but expected \
                 {NUM_EXPECTED_QSFP_PORTS}",
                qsfp_ports.len(),
            );
        }

        for port_name in qsfp_ports {
            self.datastore
                .switch_port_create(
                    opctx,
                    self.rack_id,
                    which_switch,
                    port_name.clone().into(),
                )
                .await
                .with_context(|| {
                    format!("failed to insert port {port_name}")
                })?;
        }

        Ok(())
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
