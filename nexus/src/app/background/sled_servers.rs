// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for keeping track of Sled Agents

use super::common::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Asset;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Generation;
use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

// A subset of information about sled agents that we can pass to downstream
// background tasks.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct SledAgent {
    pub address: SocketAddr,
    pub generation: Generation,
}

type SledAgents = HashMap<Uuid, SledAgent>;

/// Background task that keeps track of the latest list of Sleds, with
/// their corresponding generation numbers.
pub struct SledAgentsWatcher {
    datastore: Arc<DataStore>,
    last: Option<SledAgents>,
    tx: watch::Sender<Option<SledAgents>>,
    rx: watch::Receiver<Option<SledAgents>>,
}

impl SledAgentsWatcher {
    pub fn new(datastore: Arc<DataStore>) -> SledAgentsWatcher {
        let (tx, rx) = watch::channel(None);
        SledAgentsWatcher { datastore, last: None, tx, rx }
    }

    /// Exposes the latest list of servers.
    ///
    /// You can use the returned [`watch::Receiver`] to look at the latest
    /// list of servers or to be notified when it changes.
    pub fn watcher(&self) -> watch::Receiver<Option<SledAgents>> {
        self.rx.clone()
    }
}

impl BackgroundTask for SledAgentsWatcher {
    fn activate<'a, 'b, 'c>(
        &'a mut self,
        opctx: &'b OpContext,
    ) -> BoxFuture<'c, serde_json::Value>
    where
        'a: 'c,
        'b: 'c,
    {
        async {
            let log = opctx.log.clone();

            let mut all_agents = SledAgents::new();

            // Arbitrary, just bounded to a reasonable size.
            const MAX_SLEDS_PER_QUERY: u32 = 32;
            let mut marker: Option<Uuid> = None;
            let limit = NonZeroU32::try_from(MAX_SLEDS_PER_QUERY).unwrap();
            let direction = dropshot::PaginationOrder::Ascending;

            loop {
                let pagparams = DataPageParams {
                    marker: marker.as_ref(),
                    limit,
                    direction,
                };

                let result = self.datastore.sled_list(opctx, &pagparams).await;

                let sleds_page = match result {
                    Ok(sleds_page) => sleds_page,
                    Err(err) => {
                        warn!(
                            &log,
                            "failed to read list of sled servers";
                            "error" => format!("{:#}", err)
                        );
                        return json!({
                            "error":
                                format!(
                                    "failed to read list of sled servers: {:#}",
                                    err
                                )
                        });
                    }
                };

                all_agents.extend(sleds_page.iter().map(|sled| {
                    (
                        sled.id(),
                        SledAgent {
                            address: SocketAddr::V6(sled.address()),
                            generation: *sled.services_gen(),
                        },
                    )
                }));

                if sleds_page.len() < limit.get().try_into().unwrap() {
                    break;
                }
                marker = sleds_page.last().map(|sled| sled.id());
            }

            let rv =
                serde_json::to_value(&all_agents).unwrap_or_else(|error| {
                    json!({
                        "error":
                            format!(
                                "failed to serialize final value: {:#}",
                                error
                            )
                    })
                });

            match &self.last {
                None => {
                    info!(
                        &log,
                        "found sleds (initial)";
                    );
                    self.last = Some(all_agents.clone());
                    self.tx.send_replace(Some(all_agents));
                }

                Some(old) => {
                    // The datastore should be sorting the servers by id in
                    // order to paginate through them.  Thus, it should be valid
                    // to compare what we got directly to what we had before
                    // without worrying about the order being different.
                    if *old == all_agents {
                        debug!(
                            &log,
                            "found sleds (no change)";
                        );
                    } else {
                        info!(
                            &log,
                            "found sleds (changed)";
                        );
                        self.last = Some(all_agents.clone());
                        self.tx.send_replace(Some(all_agents));
                    }
                }
            };

            rv
        }
        .boxed()
    }
}

/*
#[cfg(test)]
mod test {
    use crate::app::background::common::BackgroundTask;
    use crate::app::background::dns_servers::SledAgents;
    use crate::app::background::dns_servers::SledAgentsWatcher;
    use crate::app::background::dns_servers::MAX_DNS_SERVERS;
    use crate::db::model::Service;
    use crate::db::model::ServiceKind;
    use assert_matches::assert_matches;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use nexus_db_model::DnsGroup;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils_macros::nexus_test;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_basic(cptestctx: &ControlPlaneTestContext) {
        let nexus = &cptestctx.server.apictx().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Verify the initial state.
        let mut task =
            SledAgentsWatcher::new(datastore.clone(), DnsGroup::Internal);
        let watcher = task.watcher();
        assert_matches!(*watcher.borrow(), None);

        // The datastore from the ControlPlaneTestContext is initialized with
        // one DNS server.
        let _ = task.activate(&opctx).await;
        assert_matches!(*watcher.borrow(), Some(SledAgents {
            ref addresses
        }) if addresses.len() == 1);

        // If we add another server, we should see it.
        {
            use crate::db::schema::service::dsl;
            diesel::insert_into(dsl::service)
                .values(Service::new(
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    Some(Uuid::new_v4()),
                    SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1, 0, 0),
                    ServiceKind::InternalDns,
                ))
                .execute_async(datastore.pool_for_tests().await.unwrap())
                .await
                .unwrap();
        }

        let _ = task.activate(&opctx).await;
        assert_matches!(*watcher.borrow(), Some(SledAgents {
            ref addresses
        }) if addresses.len() == 2);

        // If we add MAX_DNS_SERVERS more servers, we should see
        // MAX_DNS_SERVERS.
        {
            use crate::db::schema::service::dsl;
            let new_services = (0..u16::try_from(MAX_DNS_SERVERS).unwrap())
                .map(|i| {
                    Service::new(
                        Uuid::new_v4(),
                        Uuid::new_v4(),
                        Some(Uuid::new_v4()),
                        SocketAddrV6::new(Ipv6Addr::LOCALHOST, i + 2, 0, 0),
                        ServiceKind::InternalDns,
                    )
                })
                .collect::<Vec<_>>();

            diesel::insert_into(dsl::service)
                .values(new_services)
                .execute_async(datastore.pool_for_tests().await.unwrap())
                .await
                .unwrap();
        }

        let _ = task.activate(&opctx).await;
        assert_matches!(*watcher.borrow(), Some(SledAgents {
            ref addresses
        }) if addresses.len() == MAX_DNS_SERVERS);

        // Now delete all the servers and try again.
        {
            use crate::db::schema::service::dsl;
            diesel::delete(
                dsl::service.filter(dsl::kind.eq(ServiceKind::InternalDns)),
            )
            .execute_async(datastore.pool_for_tests().await.unwrap())
            .await
            .unwrap();
        }
        let _ = task.activate(&opctx).await;
        assert_matches!(*watcher.borrow(), Some(SledAgents {
            ref addresses
        }) if addresses.is_empty());
    }
}
*/
