use std::{collections::HashSet, sync::Arc};

use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::{Sled, SledState};
use nexus_db_queries::{context::OpContext, db::DataStore};
use nexus_networking::sled_client_from_address;
use nexus_types::{external_api::views::SledPolicy, identity::Asset};
use omicron_common::api::external::Vni;
use serde_json::json;
use sled_agent_client::types::{
    DeleteVirtualNetworkInterfaceHost, SetVirtualNetworkInterfaceHost,
};
use uuid::Uuid;

use super::common::BackgroundTask;

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
            let sleds = match self.datastore.sled_list_all_batched(opctx).await
            {
                Ok(v) => v,
                Err(e) => {
                    let msg = format!("failed to enumerate sleds: {:#}", e);
                    error!(&log, "{msg}");
                    return json!({"error": msg});
                }
            }
            .into_iter()
            .filter(|sled| {
                matches!(sled.state(), SledState::Active)
                    && matches!(sled.policy(), SledPolicy::InService { .. })
            });

            // Map sled db records to sled-agent clients
            let sled_clients: Vec<(Sled, sled_agent_client::Client)> = sleds
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
                    let physical_host_ip = match mapping.sled_ip.ip() {
                        std::net::IpAddr::V4(v) => {
                            // sled ip should never be ipv4
                            error!(
                                &log,
                                "sled ip should be ipv6 but is ipv4: {v}"
                            );
                            return None;
                        }
                        std::net::IpAddr::V6(v) => v,
                    };

                    let vni = match mapping.vni.0.try_into() {
                        Ok(v) => v,
                        Err(e) => {
                            // if we're here, that means a VNI stored in the DB as SqlU32 is
                            // an invalid VNI
                            error!(
                                &log,
                                "unable to parse Vni from SqlU32";
                                "error" => ?e
                            );
                            return None;
                        }
                    };

                    // TODO: after looking at the sled-agent side of things, we may not need nic_id?
                    // the nic_id path parameter is not used in sled_agent
                    let _nic_id = mapping.nic_id;

                    let mapping = SetVirtualNetworkInterfaceHost {
                        virtual_ip: mapping.ip.ip(),
                        virtual_mac: *mapping.mac,
                        physical_host_ip,
                        vni,
                    };
                    Some(mapping)
                })
                .collect();

            for (sled, client) in sled_clients {
                // Get the current mappings on each sled
                // Ignore vopte interfaces that are used for services
                let found_v2p: HashSet<SetVirtualNetworkInterfaceHost> = match client.list_v2p().await {
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

                let v2p_to_del: Vec<_> = found_v2p
                    .difference(&desired_v2p)
                    .map(|mapping| DeleteVirtualNetworkInterfaceHost{
                        virtual_ip: mapping.virtual_ip,
                        vni: mapping.vni,
                    }).collect();

                info!(&log, "v2p mappings to delete"; "sled" => sled.serial_number(), "mappings" => ?v2p_to_del);
                for mapping in v2p_to_del {
                    if let Err(e) = client.del_v2p(&Uuid::default(), &mapping).await {
                        error!(
                            &log,
                            "failed to delete v2p mapping from sled";
                            "sled" => sled.serial_number(),
                            "mappng" => ?mapping,
                            "error" => ?e,
                        );
                    }
                }

                info!(&log, "v2p mappings to add"; "sled" => sled.serial_number(), "mappings" => ?v2p_to_add);
                for mapping in v2p_to_add {
                    if let Err(e) = client.set_v2p(&Uuid::default(), mapping).await {
                        error!(
                            &log,
                            "failed to add v2p mapping to sled";
                            "sled" => sled.serial_number(),
                            "mappng" => ?mapping,
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
