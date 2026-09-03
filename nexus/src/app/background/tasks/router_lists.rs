// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task propagating per-port tunnel-router lists (RFD 662) to
//! sled-agents.
//!
//! Each OPTE port gets the prioritized router-configuration list of the silo
//! its instance belongs to. A silo with no assignment gets an EMPTY list —
//! no config means no tunnel routers, i.e. no external egress. Service
//! ports get the control-plane list if one was ever configured (an
//! explicitly-configured-empty list means no egress), otherwise the
//! default list `[(1000, default)]`. Ports we cannot account for (DB/sled
//! skew) fail closed with an empty list. A list entry referencing either
//! built-in per-switch configuration ("default-switch0"/"default-switch1",
//! fixed well-known IDs) is pushed to OPTE as the `None` (default) router —
//! at the list level the two are aliases, and resolved lists are deduped to
//! the highest-priority entry per router.

use std::collections::HashMap;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_queries::{context::OpContext, db::DataStore};
use nexus_networking::sled_client_from_address;
use nexus_types::deployment::SledFilter;
use nexus_types::identity::Asset;
use omicron_uuid_kinds::GenericUuid;
use serde_json::json;
use sled_agent_client::types::{PortRouterList, RouterListEntry};
use slog_error_chain::InlineErrorChain;
use uuid::Uuid;

use crate::app::background::BackgroundTask;
use crate::app::router_configuration::{
    default_router_list, router_list_from_links,
};

pub struct RouterListManager {
    datastore: Arc<DataStore>,
}

impl RouterListManager {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }
}

fn normalized(routers: &[RouterListEntry]) -> Vec<(u16, Option<Uuid>)> {
    let mut v: Vec<_> =
        routers.iter().map(|e| (e.priority, e.router_id)).collect();
    v.sort();
    v
}

impl BackgroundTask for RouterListManager {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        let log = opctx.log.clone();

        async move {
            let mut errors = Vec::new();

            // silo -> prioritized router-configuration list
            let links = match self
                .datastore
                .silo_router_configurations_list_all(opctx)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    let msg = format!(
                        "failed to list silo router configurations: {}",
                        InlineErrorChain::new(&e)
                    );
                    error!(&log, "{msg}");
                    return json!({ "error": msg });
                }
            };
            let mut silo_links: HashMap<Uuid, Vec<(u16, Uuid)>> =
                HashMap::new();
            for link in links {
                silo_links.entry(link.silo_id).or_default().push((
                    *link.priority,
                    link.router_configuration_id.into_untyped_uuid(),
                ));
            }
            let silo_lists: HashMap<Uuid, Vec<RouterListEntry>> = silo_links
                .into_iter()
                .map(|(silo, links)| (silo, router_list_from_links(links)))
                .collect();

            // The control-plane (service port) list: never configured →
            // built-in default; the NULL-id marker row → explicitly empty.
            let cp_list = match self
                .datastore
                .control_plane_router_configurations_list(opctx)
                .await
            {
                Ok(entries) if entries.is_empty() => default_router_list(),
                Ok(entries) => router_list_from_links(
                    entries.into_iter().filter_map(|e| {
                        e.router_configuration_id.map(|id| {
                            (*e.priority, id.into_untyped_uuid())
                        })
                    }),
                ),
                Err(e) => {
                    let msg = format!(
                        "failed to list control-plane router configurations: {}",
                        InlineErrorChain::new(&e)
                    );
                    error!(&log, "{msg}");
                    return json!({ "error": msg });
                }
            };

            // nic -> (silo, sled): every guest NIC backing an OPTE port
            let targets =
                match self.datastore.port_router_list_targets(opctx).await {
                    Ok(v) => v,
                    Err(e) => {
                        let msg = format!(
                            "failed to list router list targets: {}",
                            InlineErrorChain::new(&e)
                        );
                        error!(&log, "{msg}");
                        return json!({ "error": msg });
                    }
                };

            // Service NICs, so service ports get the control-plane list
            // explicitly rather than as a fallback.
            let service_nics =
                match self.datastore.service_nic_ids(opctx).await {
                    Ok(v) => v,
                    Err(e) => {
                        let msg = format!(
                            "failed to list service NICs: {}",
                            InlineErrorChain::new(&e)
                        );
                        error!(&log, "{msg}");
                        return json!({ "error": msg });
                    }
                };

            let mut desired: HashMap<Uuid, Vec<RouterListEntry>> =
                HashMap::new();
            for nic_id in &service_nics {
                desired.insert(*nic_id, cp_list.clone());
            }
            for (nic_id, silo_id, _sled) in &targets {
                // No assignment = no config = no tunnel routers (empty
                // list), NOT the default router.
                desired.insert(
                    *nic_id,
                    silo_lists.get(silo_id).cloned().unwrap_or_default(),
                );
            }

            // Visit every in-service sled, not just those hosting guest
            // ports: sleds with only service ports must converge on the
            // control-plane list too.
            let sleds = match self
                .datastore
                .sled_list_all_batched(opctx, SledFilter::InService)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    let msg = format!(
                        "failed to list in-service sleds: {}",
                        InlineErrorChain::new(&e)
                    );
                    error!(&log, "{msg}");
                    return json!({ "error": msg });
                }
            };

            let mut pushed = 0;
            for sled in &sleds {
                let client =
                    sled_client_from_address(sled.id(), sled.address(), &log);

                // The sled enumerates its ports; a port we have no desired
                // list for (DB/sled skew: NIC created or deleted between
                // our query and the sled's report) fails CLOSED — empty
                // list, no egress — never the control-plane list.
                let found = match client.list_router_lists().await {
                    Ok(v) => v.into_inner(),
                    Err(e) => {
                        errors.push(format!(
                            "failed to list router lists on sled {}: {}",
                            sled.serial_number(),
                            InlineErrorChain::new(&e)
                        ));
                        continue;
                    }
                };

                for port in found {
                    let want = desired
                        .get(&port.nic_id)
                        .cloned()
                        .unwrap_or_default();
                    if normalized(&port.routers) == normalized(&want) {
                        continue;
                    }
                    let update =
                        PortRouterList { nic_id: port.nic_id, routers: want };
                    info!(
                        &log, "updating port router list";
                        "sled" => sled.serial_number(),
                        "list" => ?update,
                    );
                    match client.set_router_list(&update).await {
                        Ok(_) => pushed += 1,
                        Err(e) => errors.push(format!(
                            "failed to set router list for nic {} on sled {}: {}",
                            update.nic_id,
                            sled.serial_number(),
                            InlineErrorChain::new(&e)
                        )),
                    }
                }
            }

            json!({
                "ports": desired.len(),
                "sleds": sleds.len(),
                "pushed": pushed,
                "errors": errors,
            })
        }
        .boxed()
    }
}
