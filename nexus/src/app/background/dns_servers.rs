// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for keeping track of DNS servers

use super::common::BackgroundTask;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::DnsGroup;
use nexus_db_model::ServiceKind;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::DataPageParams;
use serde::Serialize;
use serde_json::json;
use std::net::{SocketAddr, SocketAddrV6};
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::watch;

// This constraint could be relaxed by paginating through the list of servers,
// but we don't expect to have this many servers any time soon.
const MAX_DNS_SERVERS: usize = 10;

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct DnsServersList {
    pub addresses: Vec<SocketAddr>,
}

/// Background task that keeps track of the latest list of DNS servers for a DNS
/// group
pub struct DnsServersWatcher {
    datastore: Arc<DataStore>,
    dns_group: DnsGroup,
    last: Option<DnsServersList>,
    tx: watch::Sender<Option<DnsServersList>>,
    rx: watch::Receiver<Option<DnsServersList>>,
}

impl DnsServersWatcher {
    pub fn new(
        datastore: Arc<DataStore>,
        dns_group: DnsGroup,
    ) -> DnsServersWatcher {
        let (tx, rx) = watch::channel(None);
        DnsServersWatcher { datastore, dns_group, last: None, tx, rx }
    }

    /// Exposes the latest list of DNS servers for this DNS group
    ///
    /// You can use the returned [`watch::Receiver`] to look at the latest
    /// list of servers or to be notified when it changes.
    pub fn watcher(&self) -> watch::Receiver<Option<DnsServersList>> {
        self.rx.clone()
    }
}

impl BackgroundTask for DnsServersWatcher {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async {
            // Set up a logger for this activation that includes metadata about
            // the current generation.
            let log = match &self.last {
                None => opctx.log.clone(),
                Some(old) => {
                    let old_addrs_dbg = format!("{:?}", old);
                    opctx.log.new(o!(
                        "current_servers" => old_addrs_dbg,
                    ))
                }
            };

            // Read the latest service configuration for this DNS group.
            let service_kind = match self.dns_group {
                DnsGroup::Internal => ServiceKind::InternalDns,
                DnsGroup::External => ServiceKind::ExternalDns,
            };

            let pagparams = DataPageParams {
                marker: None,
                limit: NonZeroU32::try_from(
                    u32::try_from(MAX_DNS_SERVERS).unwrap(),
                )
                .unwrap(),
                direction: dropshot::PaginationOrder::Ascending,
            };

            let result = self
                .datastore
                .services_list_kind(opctx, service_kind, &pagparams)
                .await;

            if let Err(error) = result {
                warn!(
                    &log,
                    "failed to read list of DNS servers";
                    "error" => format!("{:#}", error)
                );
                return json!({
                    "error":
                        format!(
                            "failed to read list of DNS servers: {:#}",
                            error
                        )
                });
            }

            let services = result.unwrap();
            if services.len() >= MAX_DNS_SERVERS {
                warn!(
                    &log,
                    "found {} servers, which is more than MAX_DNS_SERVERS \
                    ({}).  There may be more that will not be used.",
                    services.len(),
                    MAX_DNS_SERVERS
                );
            }

            let new_config = DnsServersList {
                addresses: services
                    .into_iter()
                    .map(|s| SocketAddrV6::new(*s.ip, *s.port, 0, 0).into())
                    .collect(),
            };
            let new_addrs_dbg = format!("{:?}", new_config);
            let rv =
                serde_json::to_value(&new_config).unwrap_or_else(|error| {
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
                        "found DNS servers (initial)";
                        "addresses" => new_addrs_dbg,
                    );
                    self.last = Some(new_config.clone());
                    self.tx.send_replace(Some(new_config));
                }

                Some(old) => {
                    // The datastore should be sorting the DNS servers by id in
                    // order to paginate through them.  Thus, it should be valid
                    // to compare what we got directly to what we had before
                    // without worrying about the order being different.
                    if *old == new_config {
                        debug!(
                            &log,
                            "found DNS servers (no change)";
                            "addresses" => new_addrs_dbg,
                        );
                    } else {
                        info!(
                            &log,
                            "found DNS servers (changed)";
                            "addresses" => new_addrs_dbg,
                        );
                        self.last = Some(new_config.clone());
                        self.tx.send_replace(Some(new_config));
                    }
                }
            };

            rv
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use crate::app::background::common::BackgroundTask;
    use crate::app::background::dns_servers::DnsServersList;
    use crate::app::background::dns_servers::DnsServersWatcher;
    use crate::app::background::dns_servers::MAX_DNS_SERVERS;
    use assert_matches::assert_matches;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use nexus_db_model::DnsGroup;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::model::Service;
    use nexus_db_queries::db::model::ServiceKind;
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
            DnsServersWatcher::new(datastore.clone(), DnsGroup::Internal);
        let watcher = task.watcher();
        assert_matches!(*watcher.borrow(), None);

        // The datastore from the ControlPlaneTestContext is initialized with
        // one DNS server.
        let _ = task.activate(&opctx).await;
        assert_matches!(*watcher.borrow(), Some(DnsServersList {
            ref addresses
        }) if addresses.len() == 1);

        // If we add another server, we should see it.
        {
            use nexus_db_queries::db::schema::service::dsl;
            diesel::insert_into(dsl::service)
                .values(Service::new(
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    Some(Uuid::new_v4()),
                    SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1, 0, 0),
                    ServiceKind::InternalDns,
                ))
                .execute_async(
                    &*datastore.pool_connection_for_tests().await.unwrap(),
                )
                .await
                .unwrap();
        }

        let _ = task.activate(&opctx).await;
        assert_matches!(*watcher.borrow(), Some(DnsServersList {
            ref addresses
        }) if addresses.len() == 2);

        // If we add MAX_DNS_SERVERS more servers, we should see
        // MAX_DNS_SERVERS.
        {
            use nexus_db_queries::db::schema::service::dsl;
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
                .execute_async(
                    &*datastore.pool_connection_for_tests().await.unwrap(),
                )
                .await
                .unwrap();
        }

        let _ = task.activate(&opctx).await;
        assert_matches!(*watcher.borrow(), Some(DnsServersList {
            ref addresses
        }) if addresses.len() == MAX_DNS_SERVERS);

        // Now delete all the servers and try again.
        {
            use nexus_db_queries::db::schema::service::dsl;
            diesel::delete(
                dsl::service.filter(dsl::kind.eq(ServiceKind::InternalDns)),
            )
            .execute_async(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .unwrap();
        }
        let _ = task.activate(&opctx).await;
        assert_matches!(*watcher.borrow(), Some(DnsServersList {
            ref addresses
        }) if addresses.is_empty());
    }
}
