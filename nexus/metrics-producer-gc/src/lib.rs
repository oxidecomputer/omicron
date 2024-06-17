// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Garbage collection of expired metrics producers
//!
//! A metrics producer is expected to reregister itself periodically. This crate
//! provides a mechanism to clean up any producers that have stopped
//! reregistering, both removing their registration records from the database
//! and notifying their assigned collector. It is expected to be invoked from a
//! Nexus background task.

use chrono::DateTime;
use chrono::Utc;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::model::ProducerEndpoint;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::Error as DbError;
use oximeter_client::Client as OximeterClient;
use slog::info;
use slog::o;
use slog::warn;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PrunedProducers {
    pub successes: BTreeSet<Uuid>,
    pub failures: BTreeMap<Uuid, DbError>,
}

#[derive(Debug, thiserror::Error, slog_error_chain::SlogInlineError)]
pub enum Error {
    #[error("failed to list expired producers")]
    ListExpiredProducers(#[source] DbError),
    #[error("failed to get Oximeter info for {id}")]
    GetOximterInfo {
        id: Uuid,
        #[source]
        err: DbError,
    },
}

/// Make one garbage collection pass over the metrics producers.
pub async fn prune_expired_producers(
    opctx: &OpContext,
    datastore: &DataStore,
    expiration: DateTime<Utc>,
) -> Result<PrunedProducers, Error> {
    // Get the list of expired producers we need to prune.
    let expired_producers =
        ExpiredProducers::new(opctx, datastore, expiration).await?;

    // Build a FuturesUnordered to prune each expired producer.
    let mut all_prunes = expired_producers
        .producer_client_pairs()
        .map(|(producer, client)| async {
            let result = unregister_producer(
                opctx, datastore, producer, client, &opctx.log,
            )
            .await;
            (producer.id(), result)
        })
        .collect::<FuturesUnordered<_>>();

    // Collect all the results.
    let mut successes = BTreeSet::new();
    let mut failures = BTreeMap::new();
    while let Some((id, result)) = all_prunes.next().await {
        match result {
            Ok(()) => {
                successes.insert(id);
            }
            Err(err) => {
                failures.insert(id, err);
            }
        }
    }
    Ok(PrunedProducers { successes, failures })
}

async fn unregister_producer(
    opctx: &OpContext,
    datastore: &DataStore,
    producer: &ProducerEndpoint,
    client: &OximeterClient,
    log: &Logger,
) -> Result<(), DbError> {
    // Attempt to notify this producer's collector that the producer's lease has
    // expired. This is an optimistic notification: if it fails, we will still
    // prune the producer from the database, so that the next time this
    // collector asks Nexus for its list of producers, this expired producer is
    // gone.
    match client.producer_delete(&producer.id()).await {
        Ok(_) => {
            info!(
                log, "successfully notified Oximeter of expired producer";
                "collector-id" => %producer.oximeter_id,
                "producer-id" => %producer.id(),
            );
        }
        Err(err) => {
            warn!(
                log, "failed to notify Oximeter of expired producer";
                "collector-id" => %producer.oximeter_id,
                "producer-id" => %producer.id(),
                InlineErrorChain::new(&err),
            );
        }
    }

    datastore.producer_endpoint_delete(opctx, &producer.id()).await.map(|_| ())
}

// Internal combination of all expired producers and a set of OximeterClients
// for each producer.
struct ExpiredProducers {
    producers: Vec<ProducerEndpoint>,
    clients: BTreeMap<Uuid, OximeterClient>,
}

impl ExpiredProducers {
    async fn new(
        opctx: &OpContext,
        datastore: &DataStore,
        expiration: DateTime<Utc>,
    ) -> Result<Self, Error> {
        let producers = datastore
            .producers_list_expired_batched(opctx, expiration)
            .await
            .map_err(Error::ListExpiredProducers)?;

        let mut clients = BTreeMap::new();
        for producer in &producers {
            let entry = match clients.entry(producer.oximeter_id) {
                Entry::Vacant(entry) => entry,
                Entry::Occupied(_) => continue,
            };
            let info = datastore
                .oximeter_lookup(opctx, &producer.oximeter_id)
                .await
                .map_err(|err| Error::GetOximterInfo {
                    id: producer.oximeter_id,
                    err,
                })?;
            let client_log =
                opctx.log.new(o!("oximeter-collector" => info.id.to_string()));
            let address = SocketAddr::new(info.ip.ip(), *info.port);
            let client = OximeterClient::new_with_client(
                &format!("http://{address}"),
                shared_client::new(),
                client_log,
            );
            entry.insert(client);
        }

        Ok(Self { producers, clients })
    }

    fn producer_client_pairs(
        &self,
    ) -> impl Iterator<Item = (&ProducerEndpoint, &OximeterClient)> {
        self.producers.iter().map(|producer| {
            // In `new()` we add a client for every producer.oximeter_id, so we
            // can unwrap this lookup.
            let client = self.clients.get(&producer.oximeter_id).unwrap();
            (producer, client)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use httptest::matchers::request;
    use httptest::responders::status_code;
    use httptest::Expectation;
    use nexus_db_model::OximeterInfo;
    use nexus_db_queries::db::datastore::pub_test_utils::datastore_test;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::internal_api::params;
    use omicron_common::api::internal::nexus;
    use omicron_test_utils::dev;
    use std::time::Duration;

    async fn read_time_modified(
        datastore: &DataStore,
        producer_id: Uuid,
    ) -> DateTime<Utc> {
        use nexus_db_queries::db::schema::metric_producer::dsl;

        let conn = datastore.pool_connection_for_tests().await.unwrap();
        match dsl::metric_producer
            .filter(dsl::id.eq(producer_id))
            .select(dsl::time_modified)
            .first_async(&*conn)
            .await
        {
            Ok(time_modified) => time_modified,
            Err(err) => panic!(
                "failed to read time_modified for producer {producer_id}: \
                {err}"
            ),
        }
    }

    #[tokio::test]
    async fn test_prune_expired_producers() {
        // Setup
        let logctx = dev::test_setup_log("test_prune_expired_producers");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) =
            datastore_test(&logctx, &db, Uuid::new_v4()).await;

        // Insert an Oximeter collector
        let collector_info = OximeterInfo::new(&params::OximeterInfo {
            collector_id: Uuid::new_v4(),
            address: "[::1]:0".parse().unwrap(),
        });
        datastore
            .oximeter_create(&opctx, &collector_info)
            .await
            .expect("failed to insert collector");

        // GC'ing expired producers should succeed if there are no producers at
        // all.
        let pruned = prune_expired_producers(&opctx, &datastore, Utc::now())
            .await
            .expect("failed to prune expired producers");
        assert!(pruned.successes.is_empty());
        assert!(pruned.failures.is_empty());

        // Insert a producer.
        let producer = ProducerEndpoint::new(
            &nexus::ProducerEndpoint {
                id: Uuid::new_v4(),
                kind: nexus::ProducerKind::Service,
                address: "[::1]:0".parse().unwrap(), // unused
                interval: Duration::from_secs(0),    // unused
            },
            collector_info.id,
        );
        datastore
            .producer_endpoint_create(&opctx, &producer)
            .await
            .expect("failed to insert producer");

        let producer_time_modified =
            read_time_modified(&datastore, producer.id()).await;

        // GC'ing expired producers with an expiration time older than our
        // producer's `time_modified` should not prune anything.
        let pruned = prune_expired_producers(
            &opctx,
            &datastore,
            producer_time_modified - Duration::from_secs(1),
        )
        .await
        .expect("failed to prune expired producers");
        assert!(pruned.successes.is_empty());
        assert!(pruned.failures.is_empty());

        // GC'ing expired producers with an expiration time _newer_ than our
        // producer's `time_modified` should prune our one producer.
        let pruned = prune_expired_producers(
            &opctx,
            &datastore,
            producer_time_modified + Duration::from_secs(1),
        )
        .await
        .expect("failed to prune expired producers");
        let expected_success =
            [producer.id()].into_iter().collect::<BTreeSet<_>>();
        assert_eq!(pruned.successes, expected_success);
        assert!(pruned.failures.is_empty());

        // GC'ing again with the same expiration should do nothing, because we
        // already pruned the producer.
        let pruned = prune_expired_producers(
            &opctx,
            &datastore,
            producer_time_modified + Duration::from_secs(1),
        )
        .await
        .expect("failed to prune expired producers");
        assert!(pruned.successes.is_empty());
        assert!(pruned.failures.is_empty());

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_prune_expired_producers_notifies_collector() {
        // Setup
        let logctx = dev::test_setup_log(
            "test_prune_expired_producers_notifies_collector",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) =
            datastore_test(&logctx, &db, Uuid::new_v4()).await;

        let mut collector = httptest::Server::run();

        // Insert an Oximeter collector
        let collector_info = OximeterInfo::new(&params::OximeterInfo {
            collector_id: Uuid::new_v4(),
            address: collector.addr(),
        });
        datastore
            .oximeter_create(&opctx, &collector_info)
            .await
            .expect("failed to insert collector");

        // Insert a producer.
        let producer = ProducerEndpoint::new(
            &nexus::ProducerEndpoint {
                id: Uuid::new_v4(),
                kind: nexus::ProducerKind::Service,
                address: "[::1]:0".parse().unwrap(), // unused
                interval: Duration::from_secs(0),    // unused
            },
            collector_info.id,
        );
        datastore
            .producer_endpoint_create(&opctx, &producer)
            .await
            .expect("failed to insert producer");

        let producer_time_modified =
            read_time_modified(&datastore, producer.id()).await;

        // GC'ing expired producers with an expiration time _newer_ than our
        // producer's `time_modified` should prune our one producer and notify
        // the collector that it's doing so.
        collector.expect(
            Expectation::matching(request::method_path(
                "DELETE",
                format!("/producers/{}", producer.id()),
            ))
            .respond_with(status_code(204)),
        );

        let pruned = prune_expired_producers(
            &opctx,
            &datastore,
            producer_time_modified + Duration::from_secs(1),
        )
        .await
        .expect("failed to prune expired producers");
        let expected_success =
            [producer.id()].into_iter().collect::<BTreeSet<_>>();
        assert_eq!(pruned.successes, expected_success);
        assert!(pruned.failures.is_empty());

        collector.verify_and_clear();

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
