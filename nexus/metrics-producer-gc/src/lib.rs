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
    log: &Logger,
) -> Result<PrunedProducers, Error> {
    // Get the list of expired producers we need to prune.
    let expired_producers =
        ExpiredProducers::new(opctx, datastore, expiration, log).await?;

    // Build a FuturesUnordered to prune each expired producer.
    let mut all_prunes = expired_producers
        .producer_client_pairs()
        .map(|(producer, client)| async {
            let result =
                unregister_producer(datastore, producer, client, log).await;
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

    datastore.producer_endpoint_delete(&producer.id()).await.map(|_| ())
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
        log: &Logger,
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
                .oximeter_lookup(&producer.oximeter_id)
                .await
                .map_err(|err| Error::GetOximterInfo {
                id: producer.oximeter_id,
                err,
            })?;
            let client_log =
                log.new(o!("oximeter-collector" => info.id.to_string()));
            let address = SocketAddr::new(info.ip.ip(), *info.port);
            let client =
                OximeterClient::new(&format!("http://{address}"), client_log);
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
