// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciliation of static routes within mgd.

use crate::switch_zone_slot::ThisSledSwitchSlot;
use bootstrap_agent_lockstep_types::scrimlet_reconcilers::mgd::MgdStaticRouteReconcilerStatus;
use daft::BTreeSetDiff;
use daft::Diffable;
use either::Either;
use mg_admin_client::Client;
use mg_admin_client::types::Path as MgdPath;
use mg_api_types::static_routes::AddStaticRoute4Request as MgdAddStaticRoute4Request;
use mg_api_types::static_routes::AddStaticRoute6Request as MgdAddStaticRoute6Request;
use mg_api_types::static_routes::DeleteStaticRoute4Request as MgdDeleteStaticRoute4Request;
use mg_api_types::static_routes::DeleteStaticRoute6Request as MgdDeleteStaticRoute6Request;
use mg_api_types::static_routes::StaticRoute4 as MgdStaticRoute4;
use mg_api_types::static_routes::StaticRoute4List as MgdStaticRoute4List;
use mg_api_types::static_routes::StaticRoute6 as MgdStaticRoute6;
use mg_api_types::static_routes::StaticRoute6List as MgdStaticRoute6List;
use oxnet::IpNet;
use oxnet::IpNetParseError;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::RouteConfig;
use slog::Logger;
use slog::info;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::iter;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;

// This is the default RIB Priority used for static routes.  This mirrors
// the const defined in maghemite in rdb/src/lib.rs.
//
// TODO-cleanup Once https://github.com/oxidecomputer/maghemite/pull/757 lands
// and is available in omicron, we should remove this in favor of using the
// exported constant from maghemite.
const DEFAULT_RIB_PRIORITY_STATIC: u8 = 1;

type MgdClientError = mg_admin_client::Error<mg_admin_client::types::Error>;

pub(super) async fn reconcile(
    client: &Client,
    desired_config: &RackNetworkConfig,
    our_switch_slot: ThisSledSwitchSlot,
    log: &Logger,
) -> MgdStaticRouteReconcilerStatus {
    let current_routes = match MgdCurrentRoutes::fetch(client).await {
        Ok(routes) => routes,
        Err(err) => {
            return MgdStaticRouteReconcilerStatus::FailedReadingStaticRoutes(
                format!(
                    "failed to read current static routes from mgd: {}",
                    InlineErrorChain::new(&err)
                ),
            );
        }
    };

    let plan = match ReconciliationPlan::new(
        current_routes,
        desired_config,
        our_switch_slot,
        log,
    ) {
        Ok(plan) => plan,
        Err(err) => {
            // Ensure `err` is actually a string; if it changes to a proper
            // error type, we need to use `InlineErrorChain` here instead.
            let err: &str = &err;
            return MgdStaticRouteReconcilerStatus::FailedGeneratingPlan(
                format!(
                    "failed to generate plan to apply static routes: {err}",
                ),
            );
        }
    };

    apply_plan(client, plan, log).await
}

/// Apply the contents of `plan` to mgd via `client`.
async fn apply_plan(
    client: &Client,
    plan: ReconciliationPlan,
    log: &Logger,
) -> MgdStaticRouteReconcilerStatus {
    let ReconciliationPlan { unchanged_count, to_delete, to_add } = plan;

    // Delete before adding in case there are any conflicting routes for new
    // routes we want to add.
    //
    // We will always attempt to add whatever routes we need to add even if one
    // or both of these deletes fail. That may fail too, if there are
    // conflicting routes we failed to delete! But it may succeed (or partially
    // succeed) if there are also routes we need to add that don't conflict.
    let (delete_v4_result, delete_v6_result) = {
        // Assemble all the v4 and v6 route deletes into two requests.
        let mut delete_v4_req = MgdDeleteStaticRoute4Request {
            routes: MgdStaticRoute4List { list: Vec::new() },
        };
        let mut delete_v6_req = MgdDeleteStaticRoute6Request {
            routes: MgdStaticRoute6List { list: Vec::new() },
        };
        for route in to_delete {
            match route.into_mgd_static_route() {
                MgdStaticRoute::V4(r) => delete_v4_req.routes.list.push(r),
                MgdStaticRoute::V6(r) => delete_v6_req.routes.list.push(r),
            }
        }
        (
            apply_bulk_operation_if_needed(
                "deleting static v4 routes",
                delete_v4_req.routes.list.len(),
                || client.static_remove_v4_route(&delete_v4_req),
                log,
            )
            .await,
            apply_bulk_operation_if_needed(
                "deleting static v6 routes",
                delete_v6_req.routes.list.len(),
                || client.static_remove_v6_route(&delete_v6_req),
                log,
            )
            .await,
        )
    };

    let (add_v4_result, add_v6_result) = {
        // Do the same for all route additions.
        let mut add_v4_req = MgdAddStaticRoute4Request {
            routes: MgdStaticRoute4List { list: Vec::new() },
        };
        let mut add_v6_req = MgdAddStaticRoute6Request {
            routes: MgdStaticRoute6List { list: Vec::new() },
        };
        for route in to_add {
            match route.into_mgd_static_route() {
                MgdStaticRoute::V4(r) => add_v4_req.routes.list.push(r),
                MgdStaticRoute::V6(r) => add_v6_req.routes.list.push(r),
            }
        }
        (
            apply_bulk_operation_if_needed(
                "adding static v4 routes",
                add_v4_req.routes.list.len(),
                || client.static_add_v4_route(&add_v4_req),
                log,
            )
            .await,
            apply_bulk_operation_if_needed(
                "adding static v6 routes",
                add_v6_req.routes.list.len(),
                || client.static_add_v6_route(&add_v6_req),
                log,
            )
            .await,
        )
    };

    match (&add_v4_result, &add_v6_result, &delete_v4_result, &delete_v6_result)
    {
        (Ok(added_v4), Ok(added_v6), Ok(deleted_v4), Ok(deleted_v6)) => {
            MgdStaticRouteReconcilerStatus::Success {
                unchanged: unchanged_count,
                deleted_v4: *deleted_v4,
                deleted_v6: *deleted_v6,
                added_v4: *added_v4,
                added_v6: *added_v6,
            }
        }
        _ => MgdStaticRouteReconcilerStatus::PartialSuccess {
            unchanged: unchanged_count,
            delete_v4_result,
            delete_v6_result,
            add_v4_result,
            add_v6_result,
        },
    }
}

// Helper to optionally perform `op`.
//
// If `nitems` is 0, `op` is never called, and we return
// `MgdStaticRouteBulkOperationResult::SkippedNoItems`.
//
// If `nitems` is not 0, we await the future returned by `op` and return either
// success or failure according to its returned value.
async fn apply_bulk_operation_if_needed<F, Fut, T>(
    description: &str,
    nitems: usize,
    op: F,
    log: &Logger,
) -> Result<usize, String>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, MgdClientError>>,
{
    if nitems == 0 {
        return Ok(0);
    }

    match op().await {
        Ok(_) => {
            info!(log, "{description} succeeded"; "num-routes" => %nitems);
            Ok(nitems)
        }
        Err(err) => {
            let err = InlineErrorChain::new(&err);
            warn!(log, "{description} failed"; &err);
            Err(format!("{description} failed: {err}"))
        }
    }
}

#[derive(Debug, PartialEq)]
struct ReconciliationPlan {
    // Count of routes that remained unchanged in this reconciliation.
    unchanged_count: usize,

    // Routes to delete.
    to_delete: BTreeSet<DiffableStaticRoute>,

    // Routes to add.
    to_add: BTreeSet<DiffableStaticRoute>,
}

impl ReconciliationPlan {
    fn new(
        mgd_current_routes: MgdCurrentRoutes,
        config: &RackNetworkConfig,
        our_switch_slot: ThisSledSwitchSlot,
        log: &Logger,
    ) -> Result<Self, String> {
        // Convert current routes into diffable form.
        let mgd_current_routes = match mgd_current_routes.try_into_diffable() {
            Ok(routes) => routes,
            Err(err) => {
                return Err(format!(
                    "invalid route fetched from mgd: {}",
                    InlineErrorChain::new(&err)
                ));
            }
        };

        // Convert desired config into diffable form.
        let desired_routes = config
            .ports
            .iter()
            .filter(|port| port.switch == our_switch_slot)
            .flat_map(|port| port.routes.iter())
            .map(DiffableStaticRoute::try_from)
            .collect::<Result<BTreeSet<_>, _>>()?;

        let BTreeSetDiff { common, added, removed } =
            mgd_current_routes.diff(&desired_routes);

        let unchanged_count = common.len();
        let to_delete = removed.into_iter().copied().collect::<BTreeSet<_>>();
        let to_add = added.into_iter().copied().collect::<BTreeSet<_>>();

        info!(
            log,
            "generated mgd static route reconciliation plan";
            "routes-unchanged" => unchanged_count,
            "routes-to-delete" => to_delete.len(),
            "routes-to-add" => to_add.len(),
        );

        Ok(Self { unchanged_count, to_delete, to_add })
    }
}

#[derive(Debug, Default)]
struct MgdCurrentRoutes {
    v4: HashMap<String, Vec<MgdPath>>,
    v6: HashMap<String, Vec<MgdPath>>,
}

impl MgdCurrentRoutes {
    async fn fetch(client: &Client) -> Result<Self, MgdClientError> {
        let v4 = client.static_list_v4_routes().await?.into_inner();
        let v6 = client.static_list_v6_routes().await?.into_inner();

        Ok(Self { v4, v6 })
    }

    fn try_into_diffable(
        self,
    ) -> Result<BTreeSet<DiffableStaticRoute>, BadMgdRoute> {
        let v4_routes = self.v4.into_iter().flat_map(|(prefix, paths)| {
            DiffableStaticRoute::try_from_v4(prefix, paths)
        });
        let v6_routes = self.v6.into_iter().flat_map(|(prefix, paths)| {
            DiffableStaticRoute::try_from_v6(prefix, paths)
        });

        v4_routes.chain(v6_routes).collect()
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, daft::Diffable,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
struct DiffableStaticRoute {
    description: DiffableStaticRouteDescription,
    vlan_id: Option<u16>,
    priority: u8,
}

impl TryFrom<&'_ RouteConfig> for DiffableStaticRoute {
    type Error = String;

    fn try_from(route: &'_ RouteConfig) -> Result<Self, Self::Error> {
        let description = match (route.nexthop, route.destination) {
            (IpAddr::V4(nexthop), IpNet::V4(prefix)) => {
                DiffableStaticRouteDescription::V4 { nexthop, prefix }
            }
            (IpAddr::V6(nexthop), IpNet::V4(prefix)) => {
                DiffableStaticRouteDescription::V4OverV6 { nexthop, prefix }
            }
            (IpAddr::V6(nexthop), IpNet::V6(prefix)) => {
                DiffableStaticRouteDescription::V6 { nexthop, prefix }
            }
            (IpAddr::V4(nexthop), IpNet::V6(prefix)) => {
                return Err(format!(
                    "rack network config route has unsuppored mix: \
                     ipv4 nexthop {nexthop} for ipv6 prefix {prefix}"
                ));
            }
        };

        Ok(Self {
            description,
            vlan_id: route.vlan_id,
            // TODO The rack network config uses `None` as a sentinel for "the
            // default priority". This isn't what we want long-term; see
            // https://github.com/oxidecomputer/maghemite/issues/646#issuecomment-3948331208.
            priority: route.rib_priority.unwrap_or(DEFAULT_RIB_PRIORITY_STATIC),
        })
    }
}

#[derive(Debug, thiserror::Error)]
enum BadMgdRoute {
    #[error("could not parse {family} route prefix `{prefix}`")]
    ParsePrefix {
        family: &'static str,
        prefix: String,
        #[source]
        err: IpNetParseError,
    },
    #[error(
        "expected {family} nexthop in prefix `{prefix}`, but got {nexthop}"
    )]
    BadNexthopFamily { family: &'static str, prefix: String, nexthop: String },
}

impl DiffableStaticRoute {
    fn into_mgd_static_route(self) -> MgdStaticRoute {
        match self.description {
            DiffableStaticRouteDescription::V4 { nexthop, prefix } => {
                MgdStaticRoute::V4(MgdStaticRoute4 {
                    nexthop: nexthop.into(),
                    prefix,
                    rib_priority: self.priority,
                    vlan_id: self.vlan_id,
                })
            }
            DiffableStaticRouteDescription::V4OverV6 { nexthop, prefix } => {
                MgdStaticRoute::V4(MgdStaticRoute4 {
                    nexthop: nexthop.into(),
                    prefix,
                    rib_priority: self.priority,
                    vlan_id: self.vlan_id,
                })
            }
            DiffableStaticRouteDescription::V6 { nexthop, prefix } => {
                MgdStaticRoute::V6(MgdStaticRoute6 {
                    nexthop,
                    prefix,
                    rib_priority: self.priority,
                    vlan_id: self.vlan_id,
                })
            }
        }
    }

    fn try_from_v4(
        prefix: String,
        paths: Vec<MgdPath>,
    ) -> impl Iterator<Item = Result<Self, BadMgdRoute>> {
        let prefix = match prefix.parse::<Ipv4Net>() {
            Ok(prefix) => prefix,
            Err(err) => {
                return Either::Left(iter::once(Err(
                    BadMgdRoute::ParsePrefix { family: "ipv4", prefix, err },
                )));
            }
        };

        Either::Right(paths.into_iter().map(move |path| {
            let description = match path.nexthop {
                IpAddr::V4(nexthop) => {
                    DiffableStaticRouteDescription::V4 { nexthop, prefix }
                }
                IpAddr::V6(nexthop) => {
                    DiffableStaticRouteDescription::V4OverV6 { nexthop, prefix }
                }
            };

            Ok(Self {
                description,
                vlan_id: path.vlan_id,
                priority: path.rib_priority,
            })
        }))
    }

    fn try_from_v6(
        prefix: String,
        paths: Vec<MgdPath>,
    ) -> impl Iterator<Item = Result<Self, BadMgdRoute>> {
        let prefix = match prefix.parse::<Ipv6Net>() {
            Ok(prefix) => prefix,
            Err(err) => {
                return Either::Left(iter::once(Err(
                    BadMgdRoute::ParsePrefix { family: "ipv6", prefix, err },
                )));
            }
        };

        Either::Right(paths.into_iter().map(move |path| {
            let nexthop = match path.nexthop {
                IpAddr::V6(ip) => ip,
                IpAddr::V4(ip) => {
                    return Err(BadMgdRoute::BadNexthopFamily {
                        family: "ipv6",
                        prefix: prefix.to_string(),
                        nexthop: ip.to_string(),
                    });
                }
            };

            let description =
                DiffableStaticRouteDescription::V6 { nexthop, prefix };

            Ok(Self {
                description,
                vlan_id: path.vlan_id,
                priority: path.rib_priority,
            })
        }))
    }
}

enum MgdStaticRoute {
    V4(MgdStaticRoute4),
    V6(MgdStaticRoute6),
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, daft::Diffable,
)]
enum DiffableStaticRouteDescription {
    V4 { nexthop: Ipv4Addr, prefix: Ipv4Net },
    V4OverV6 { nexthop: Ipv6Addr, prefix: Ipv4Net },
    V6 { nexthop: Ipv6Addr, prefix: Ipv6Net },
}

#[cfg(test)]
mod tests;
