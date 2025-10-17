// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{collections::HashSet, net::IpAddr, sync::Arc};

use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_db_model::Sled;
use nexus_db_queries::{context::OpContext, db::DataStore};
use nexus_networking::sled_client_from_address;
use nexus_types::{deployment::SledFilter, identity::Asset};
use omicron_common::api::external::Vni;
use serde_json::json;
use sled_agent_client::types::VirtualNetworkInterfaceHost;

use crate::app::background::BackgroundTask;

pub struct V2PManager {
    datastore: Arc<DataStore>,
}

impl V2PManager {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }
}

impl BackgroundTask for V2PManager {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        let log = opctx.log.clone();

        async move {
            // Get the v2p mappings
            let v2p_mappings = match self.datastore.v2p_mappings(opctx).await {
                Ok(v) => v,
                Err(e) => {
                    let msg = format!("failed to list v2p mappings: {:#}", e);
                    error!(&log, "{msg}");
                    return json!({"error": msg});
                }
            };

            // Get sleds
            // we only care about sleds that are active && inservice
            let sleds = match self.datastore.sled_list_all_batched(opctx, SledFilter::VpcRouting).await
            {
                Ok(v) => v,
                Err(e) => {
                    let msg = format!("failed to enumerate sleds: {:#}", e);
                    error!(&log, "{msg}");
                    return json!({"error": msg});
                }
            };

            // Map sled db records to sled-agent clients
            let sled_clients: Vec<(Sled, sled_agent_client::Client)> = sleds
                .into_iter()
                .map(|sled| {
                    let client = sled_client_from_address(
                        sled.id(),
                        sled.address(),
                        &log,
                    );
                    (sled, client)
                })
                .collect();

            // create a set of updates from the v2p mappings
            let desired_v2p: HashSet<_> = v2p_mappings
                .into_iter()
                .filter_map(|mapping| {
                    // TODO-completeness: Support dual-stack in the
                    // `VirtualNetworkInterfaceHost` type. See
                    // https://github.com/oxidecomputer/omicron/issues/9246.
                    let Some(virtual_ip) = mapping.ipv4.map(IpAddr::from) else {
                        error!(
                            &log,
                            "No IPv4 address in V2P mapping";
                            "nic_id" => %mapping.nic_id,
                            "sled_id" => %mapping.sled_id,
                        );
                        return None;
                    };
                    Some(VirtualNetworkInterfaceHost {
                        virtual_ip,
                        virtual_mac: *mapping.mac,
                        physical_host_ip: *mapping.sled_ip,
                        vni: mapping.vni.0,
                    })
                })
                .collect();

            for (sled, client) in sled_clients {
                //
                // Get the current mappings on each sled
                // Ignore vopte interfaces that are used for services. Service zones only need
                // an opte interface for external communication. For services zones, intra-sled
                // communication is facilitated via zone underlay interfaces / addresses,
                // not opte interfaces / v2p mappings.
                //
                let found_v2p: HashSet<VirtualNetworkInterfaceHost> = match client.list_v2p().await {
                    Ok(v) => v.into_inner(),
                    Err(e) => {
                        error!(
                            &log,
                            "unable to list opte v2p mappings for sled";
                            "sled" => sled.serial_number(),
                            "error" => ?e
                        );
                        continue;
                    }
                }.into_iter().filter(|vnic| vnic.vni != Vni::SERVICES_VNI).collect();

                info!(&log, "found opte v2p mappings"; "sled" => sled.serial_number(), "interfaces" => ?found_v2p);

                let v2p_to_add: Vec<_> = desired_v2p.difference(&found_v2p).collect();

                let v2p_to_del: Vec<_> = found_v2p.difference(&desired_v2p).collect();

                //
                // Generally, we delete stale entries before adding new entries in RPWs to prevent stale entries
                // from causing a conflict with an incoming entry. In the case of opte it doesn't matter which
                // order we perform the next two steps in, since conflicting stale entries are overwritten by the
                // incoming entries.
                //
                info!(&log, "v2p mappings to delete"; "sled" => sled.serial_number(), "mappings" => ?v2p_to_del);
                for mapping in v2p_to_del {
                    if let Err(e) = client.del_v2p(&mapping).await {
                        error!(
                            &log,
                            "failed to delete v2p mapping from sled";
                            "sled" => sled.serial_number(),
                            "mapping" => ?mapping,
                            "error" => ?e,
                        );
                    }
                }

                info!(&log, "v2p mappings to add"; "sled" => sled.serial_number(), "mappings" => ?v2p_to_add);
                for mapping in v2p_to_add {
                    if let Err(e) = client.set_v2p(mapping).await {
                        error!(
                            &log,
                            "failed to add v2p mapping to sled";
                            "sled" => sled.serial_number(),
                            "mapping" => ?mapping,
                            "error" => ?e,
                        );
                    }
                }
            }
            json!({})
        }
        .boxed()
    }
}
