// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task for keeping track of service configuration

use super::common::BackgroundTask;
use super::sled_servers::SledAgent;
use futures::future::BoxFuture;
use futures::FutureExt;
use nexus_db_model::DatasetKind as DbDatasetKind;
use nexus_db_model::ServiceKind as DbServiceKind;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Asset;
use serde_json::json;
use sled_agent_client::types::{
    DatasetKind, DatasetName, DatasetRequest, ServiceEnsureBody, ServiceType,
    ServiceZoneRequest, ServiceZoneService, ZoneType, ZpoolName,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::watch;
use uuid::Uuid;

type SledAgents = HashMap<Uuid, SledAgent>;
type SledServices = HashMap<SocketAddr, ServiceEnsureBody>;

/// Background task that keeps track of the latest service configuration
pub struct ServiceConfigWatcher {
    datastore: Arc<DataStore>,

    // Input: What are the sleds to be monitored?
    rx_sleds: watch::Receiver<Option<SledAgents>>,

    // Output: What are the set of services to be propagated?
    last: Option<SledServices>,
    tx: watch::Sender<Option<SledServices>>,
    rx: watch::Receiver<Option<SledServices>>,
}

impl ServiceConfigWatcher {
    pub fn new(
        datastore: Arc<DataStore>,
        rx_sleds: watch::Receiver<Option<SledAgents>>,
    ) -> ServiceConfigWatcher {
        let (tx, rx) = watch::channel(None);
        ServiceConfigWatcher { datastore, rx_sleds, last: None, tx, rx }
    }

    /// Exposes the latest service configuration
    ///
    /// You can use the returned [`watch::Receiver`] to look at the latest
    /// configuration or to be notified when it changes.
    pub fn watcher(&self) -> watch::Receiver<Option<SledServices>> {
        self.rx.clone()
    }
}

impl BackgroundTask for ServiceConfigWatcher {
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

            // Copy the latest sled list to avoid taking a read lock.
            let sled_agents = self.rx_sleds.borrow().clone();

            let Some(sled_agents) = sled_agents else {
                warn!(
                    &log,
                    "Service Lookup: skipped";
                    "reason" => "no known sled agents"
                );
                return json!({ "error": "no known sled agents" });
            };

            let empty_map = HashMap::new();
            let old_services = match &self.last {
                Some(prior) => prior,
                None => &empty_map,
            };

            let mut new_services = HashMap::new();
            for (sled_id, agent) in sled_agents {
                // If nothing has changed, use the services we saw last time.
                if let Some(request) = old_services.get(&agent.address) {
                    if request.generation == agent.generation.into() {
                        new_services.insert(agent.address, request.clone());
                        continue;
                    }
                }

                // Otherwise, we're updating the set of services.
                // Read the latest set of services from this sled.
                let result = self.datastore.services_list_for_sled(opctx, sled_id).await;
                let services = match result {
                    Ok(services) => services,
                    Err(err) => {
                        let err = err.to_string();
                        warn!(
                            &log,
                            "Failed to read services for sled";
                            "error" => &err,
                        );
                        return json!({ "error": format!("failed to lookup services for sled: {err}") });
                    }
                };

                let services = services.into_iter().map(|(service, maybe_dataset)| {
                    let zone_type = match service.kind {
                        DbServiceKind::Clickhouse => ZoneType::Clickhouse,
                        DbServiceKind::ClickhouseKeeper => ZoneType::ClickhouseKeeper,
                        DbServiceKind::Cockroach => ZoneType::CockroachDb,
                        DbServiceKind::CruciblePantry => ZoneType::CruciblePantry,
                        DbServiceKind::Crucible => ZoneType::Crucible,
                        DbServiceKind::ExternalDns => ZoneType::ExternalDns,
                        DbServiceKind::InternalDns => ZoneType::InternalDns,
                        DbServiceKind::Nexus => ZoneType::Nexus,
                        DbServiceKind::Ntp => ZoneType::Ntp,
                        DbServiceKind::Oximeter => ZoneType::Oximeter,
                        // TODO: I don't think it's correct to actually ask for
                        // this zone type?
                        DbServiceKind::Dendrite | DbServiceKind::Tfport => ZoneType::Switch,
                    };

                    let dataset = maybe_dataset.map(|dataset| {
                        let dataset_kind = match dataset.kind {
                            DbDatasetKind::Crucible => DatasetKind::Crucible,
                            DbDatasetKind::Cockroach => DatasetKind::CockroachDb,
                            DbDatasetKind::Clickhouse => DatasetKind::Clickhouse,
                            DbDatasetKind::ClickhouseKeeper => DatasetKind::ClickhouseKeeper,
                            DbDatasetKind::ExternalDns => DatasetKind::ExternalDns,
                            DbDatasetKind::InternalDns => DatasetKind::InternalDns,
                        };

                        DatasetRequest {
                            id: dataset.id(),
                            name: DatasetName {
                                // TODO: We need an additional lookup of the
                                // zpool to determine if it's internal or
                                // external. For now, however, we hard-code
                                // "external" via the "oxp_" prefix.
                                pool_name: ZpoolName::from_str(&format!("oxp_{}", dataset.pool_id)).unwrap(),
                                kind: dataset_kind,
                            },
                            service_address: service.address().to_string(),
                        }
                    });

                    let service_type = match service.kind {
                        DbServiceKind::Clickhouse => {
                            ServiceType::Clickhouse { address: service.address().to_string() }
                        },
                        DbServiceKind::ClickhouseKeeper => {
                            ServiceType::ClickhouseKeeper { address: service.address().to_string() }
                        },
                        DbServiceKind::Cockroach => {
                            ServiceType::CockroachDb { address: service.address().to_string() }
                        },
                        DbServiceKind::CruciblePantry => {
                            ServiceType::CruciblePantry { address: service.address().to_string() }
                        },
                        DbServiceKind::Crucible => {
                            ServiceType::Crucible { address: service.address().to_string() }
                        },
                        DbServiceKind::ExternalDns => {
                            todo!()
                            /*
                            ServiceType::ExternalDns {
                                http_address: ...,
                                dns_address: ...,
                                nic: ...,
                            }
                            */
                        },
                        DbServiceKind::InternalDns => {
                            todo!()
                            /*
                            ServiceType::InternalDns {
                                http_address: ...,
                                dns_address: ...,
                                gz_address: ...,
                                gz_address_index: ...,
                            }
                            */
                        },
                        DbServiceKind::Nexus => {
                            todo!()
                            /*
                            ServiceType::Nexus {
                                internal_address: ...,
                                external_ip: ...,
                                nic: ...,
                                external_tls: ...,
                                external_dns_servers: ...,
                            }
                            */
                        },
                        DbServiceKind::Ntp => {
                            // TODO: How do we distinguish internal from
                            // external?
                            todo!();
                        },
                        DbServiceKind::Oximeter => {
                            ServiceType::Oximeter { address: service.address().to_string() }
                        },
                        _ => todo!(),
                    };

                    ServiceZoneRequest {
                        id: service.id(),
                        zone_type,
                        addresses: vec![service.ip()],
                        dataset,
                        // TODO?
                        services: vec![ServiceZoneService {
                            id: service.id(),
                            details: service_type,
                        }],

                    }
                }).collect();

                new_services.insert(
                    agent.address,
                    ServiceEnsureBody {
                        generation: agent.generation.into(),
                        services,
                    }
                );
            }

            // If anything changed, notify watchers.
            if *old_services != new_services {
                self.last = Some(new_services.clone());
                self.tx.send_replace(Some(new_services));
            }
            json!([])
        }
        .boxed()
    }
}

/*
#[cfg(test)]
mod test {
    use crate::app::background::common::BackgroundTask;
    use crate::app::background::dns_config::ServiceConfigWatcher;
    use crate::app::background::init::test::read_internal_dns_zone_id;
    use crate::app::background::init::test::write_test_dns_generation;
    use assert_matches::assert_matches;
    use async_bb8_diesel::AsyncConnection;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use async_bb8_diesel::AsyncSimpleConnection;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use nexus_db_model::DnsGroup;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils_macros::nexus_test;
    use serde_json::json;

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
            ServiceConfigWatcher::new(datastore.clone(), DnsGroup::Internal);
        let watcher = task.watcher();
        assert_matches!(*watcher.borrow(), None);

        // The datastore from the ControlPlaneTestContext is initialized with a
        // DNS config with generation 1.
        let value = task.activate(&opctx).await;
        assert_eq!(watcher.borrow().as_ref().unwrap().generation, 1);
        assert_eq!(value, json!({ "generation": 1 }));

        // Now write generation 2, activate again, and verify that the update
        // was sent to the watcher.
        let internal_dns_zone_id =
            read_internal_dns_zone_id(&opctx, &datastore).await;
        write_test_dns_generation(&datastore, internal_dns_zone_id).await;
        assert_eq!(watcher.borrow().as_ref().unwrap().generation, 1);
        let value = task.activate(&opctx).await;
        assert_eq!(watcher.borrow().as_ref().unwrap().generation, 2);
        assert_eq!(value, json!({ "generation": 2 }));

        // Activate again and make sure it does nothing.
        let value = task.activate(&opctx).await;
        assert_eq!(watcher.borrow().as_ref().unwrap().generation, 2);
        assert_eq!(value, json!({ "generation": 2 }));

        // Simulate the configuration going backwards.  This should not be
        // possible, but it's easy to check that we at least don't panic.
        {
            use crate::db::schema::dns_version::dsl;
            diesel::delete(dsl::dns_version.filter(dsl::version.eq(2)))
                .execute_async(datastore.pool_for_tests().await.unwrap())
                .await
                .unwrap();
        }
        let value = task.activate(&opctx).await;
        assert_eq!(watcher.borrow().as_ref().unwrap().generation, 2);
        assert_eq!(
            value,
            json!({
                "error": "found latest DNS generation (1) is older \
                    than the one we already know about (2)"
            })
        );

        // Similarly, wipe all of the state and verify that we handle that okay.
        datastore
            .pool_for_tests()
            .await
            .unwrap()
            .transaction_async(|conn| async move {
                conn.batch_execute_async(
                    nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL,
                )
                .await
                .unwrap();
                diesel::delete(
                    crate::db::schema::dns_version::dsl::dns_version,
                )
                .execute_async(&conn)
                .await
                .unwrap();
                diesel::delete(crate::db::schema::dns_name::dsl::dns_name)
                    .execute_async(&conn)
                    .await
                    .unwrap();
                diesel::delete(crate::db::schema::dns_zone::dsl::dns_zone)
                    .execute_async(&conn)
                    .await
                    .unwrap();
                Ok::<_, crate::db::TransactionError<()>>(())
            })
            .await
            .unwrap();

        let _ = task.activate(&opctx).await;
        assert_eq!(watcher.borrow().as_ref().unwrap().generation, 2);

        // Verify that a new watcher also handles this okay. (i.e., that we can
        // come up with no state in the database).
        let mut task =
            ServiceConfigWatcher::new(datastore.clone(), DnsGroup::Internal);
        let watcher = task.watcher();
        assert_matches!(*watcher.borrow(), None);
        let _ = task.activate(&opctx).await;
        assert_eq!(watcher.borrow().as_ref(), None);

        // TODO-coverage It would be nice to verify the behavior when the
        // database is offline.  As of this writing, it works correctly: we
        // maintain what we currently have.  However, it takes 30s for bb8 to
        // time out waiting for a connection.  That's a little much for an
        // automatied test.
    }
}
*/
