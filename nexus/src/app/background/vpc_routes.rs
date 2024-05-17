// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for propagating VPC routes (system and custom) to sleds.

use super::common::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::{Sled, SledState, Vni};
use nexus_db_queries::{context::OpContext, db::DataStore};
use nexus_networking::sled_client_from_address;
use nexus_types::{
    deployment::SledFilter, external_api::views::SledPolicy, identity::Asset,
    identity::Resource,
};
use omicron_common::api::internal::shared::ReifiedVpcRoute;
use omicron_common::api::internal::shared::{ReifiedVpcRouteSet, RouterId};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use uuid::Uuid;

pub struct VpcRouteManager {
    datastore: Arc<DataStore>,
}

impl VpcRouteManager {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }
}

impl BackgroundTask for VpcRouteManager {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            let log = &opctx.log;
            // let mut paginator = Paginator::new(MAX_SLED_AGENTS);

            // XX: copied from omicron#5566
            let sleds = match self
                .datastore
                .sled_list_all_batched(opctx, SledFilter::InService)
                .await
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

            // XX: actually reify rules.
            let mut known_rules: HashMap<Uuid, HashSet<ReifiedVpcRoute>> =
                HashMap::new();
            let mut db_routers = HashMap::new();

            for (sled, client) in sled_clients {
                let Ok(route_sets) = client.list_vpc_routes().await else {
                    warn!(
                        log,
                        "failed to fetch current VPC route state from sled";
                        "sled" => sled.serial_number(),
                    );
                    continue;
                };

                let route_sets = route_sets.into_inner();

                // Lookup all missing (VNI, subnet) pairs we need from this sled.
                for set in &route_sets {
                    let system_route =
                        RouterId { vni: set.id.vni, subnet: None };

                    if db_routers.contains_key(&system_route) {
                        continue;
                    }

                    let db_vni = Vni(set.id.vni);
                    let Ok(vpc) =
                        self.datastore.resolve_vni_to_vpc(opctx, db_vni).await
                    else {
                        error!(
                            log,
                            "failed to fetch VPC from VNI";
                            "sled" => sled.serial_number(),
                            "vni" => format!("{db_vni:?}")
                        );
                        continue;
                    };

                    let vpc_id = vpc.identity().id;

                    let Ok(system_router) = self
                        .datastore
                        .vpc_get_system_router(opctx, vpc_id)
                        .await
                    else {
                        error!(
                            log,
                            "failed to fetch system router for VPC";
                            "vpc" => vpc_id.to_string()
                        );
                        continue;
                    };

                    let Ok(custom_routers) = self
                        .datastore
                        .vpc_get_active_custom_routers(opctx, vpc_id)
                        .await
                    else {
                        error!(
                            log,
                            "failed to fetch custom routers for VPC";
                            "vpc" => vpc_id.to_string()
                        );
                        continue;
                    };

                    db_routers.insert(system_route, system_router);
                    db_routers.extend(custom_routers.into_iter().map(
                        |(subnet, router)| {
                            (
                                RouterId {
                                    vni: set.id.vni,
                                    subnet: Some(subnet.ipv4_block.0.into()),
                                },
                                router,
                            )
                        },
                    ));
                    // XX: do this right / unify v4 and v6
                    // db_routers.extend(custom_routers.into_iter().map(
                    //     |(subnet, router)| {
                    //         (
                    //             RouterId {
                    //                 vni: set.id.vni,
                    //                 subnet: Some(subnet.ipv6_block.0.into()),
                    //             },
                    //             router,
                    //         )
                    //     },
                    // ));
                }

                let mut to_push = HashMap::new();

                // reify into known_rules on an as-needed basis.
                for set in &route_sets {
                    let Some(db_router) = db_routers.get(&set.id) else {
                        // The sled wants to know about rules for a VPC
                        // subnet with no custom router set. Send them
                        // the empty list.
                        to_push.insert(set.id, HashSet::new());
                        continue;
                    };

                    let router_id = db_router.id();

                    // We may have already resolved the rules for this
                    // router in a previous call.
                    if let Some(rules) = known_rules.get(&router_id) {
                        to_push.insert(set.id, rules.clone());
                        continue;
                    }

                    match self
                        .datastore
                        .vpc_resolve_router_rules(
                            opctx,
                            db_router.identity().id,
                        )
                        .await
                    {
                        Ok(rules) => {
                            to_push.insert(set.id, rules.clone());
                            known_rules.insert(router_id, rules);
                        }
                        Err(e) => {
                            error!(
                                &log,
                                "failed to compute subnet routes";
                                "router" => router_id.to_string(),
                                "err" => e.to_string()
                            );
                        }
                    }
                }

                let to_push = to_push
                    .into_iter()
                    .map(|(id, routes)| ReifiedVpcRouteSet { id, routes })
                    .collect();

                if let Err(e) = client.set_vpc_routes(&to_push).await {
                    error!(
                        log,
                        "failed to push new VPC route state from sled";
                        "sled" => sled.serial_number(),
                        "err" => format!("{e}")
                    );
                    continue;
                };
            }

            json!({})
        }
        .boxed()
    }
}
