// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Oximeter-related functionality

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::identity::Asset;
use crate::internal_api::params::OximeterInfo;
use internal_dns_client::{
    multiclient::{ResolveError, Resolver},
    names::{ServiceName, SRV},
};
use omicron_common::address::CLICKHOUSE_PORT;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::PaginationOrder;
use omicron_common::api::internal::nexus;
use omicron_common::backoff;
use oximeter_client::Client as OximeterClient;
use oximeter_db::TimeseriesSchema;
use oximeter_db::TimeseriesSchemaPaginationParams;
use oximeter_producer::register;
use slog::Logger;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::time::Duration;
use uuid::Uuid;

/// A client which knows how to connect to Clickhouse, but does so
/// only when a request is actually made.
///
/// This allows callers to set up the mechanism of connection (by address
/// or DNS) separately from actually making that connection. This
/// is particularly useful in situations where configurations are parsed
/// prior to Clickhouse existing.
pub struct LazyTimeseriesClient {
    log: Logger,
    source: ClientSource,
}

enum ClientSource {
    FromDns { resolver: Resolver },
    FromIp { address: SocketAddr },
}

impl LazyTimeseriesClient {
    pub fn new_from_dns(log: Logger, resolver: Resolver) -> Self {
        Self { log, source: ClientSource::FromDns { resolver } }
    }

    pub fn new_from_address(log: Logger, address: SocketAddr) -> Self {
        Self { log, source: ClientSource::FromIp { address } }
    }

    pub async fn get(&self) -> Result<oximeter_db::Client, ResolveError> {
        let address = match &self.source {
            ClientSource::FromIp { address } => *address,
            ClientSource::FromDns { resolver } => SocketAddr::new(
                resolver
                    .lookup_ip(SRV::Service(ServiceName::Clickhouse))
                    .await?,
                CLICKHOUSE_PORT,
            ),
        };

        Ok(oximeter_db::Client::new(address, &self.log))
    }
}

impl super::Nexus {
    /// Insert a new record of an Oximeter collector server.
    pub async fn upsert_oximeter_collector(
        &self,
        oximeter_info: &OximeterInfo,
    ) -> Result<(), Error> {
        // Insert the Oximeter instance into the DB. Note that this _updates_ the record,
        // specifically, the time_modified, ip, and port columns, if the instance has already been
        // registered.
        let db_info = db::model::OximeterInfo::new(&oximeter_info);
        self.db_datastore.oximeter_create(&db_info).await?;
        info!(
            self.log,
            "registered new oximeter metric collection server";
            "collector_id" => ?oximeter_info.collector_id,
            "address" => oximeter_info.address,
        );

        // Regardless, notify the collector of any assigned metric producers. This should be empty
        // if this Oximeter collector is registering for the first time, but may not be if the
        // service is re-registering after failure.
        let pagparams = DataPageParams {
            marker: None,
            direction: PaginationOrder::Ascending,
            limit: std::num::NonZeroU32::new(100).unwrap(),
        };
        let producers = self
            .db_datastore
            .producers_list_by_oximeter_id(
                oximeter_info.collector_id,
                &pagparams,
            )
            .await?;
        if !producers.is_empty() {
            debug!(
                self.log,
                "registered oximeter collector that is already assigned producers, re-assigning them to the collector";
                "n_producers" => producers.len(),
                "collector_id" => ?oximeter_info.collector_id,
            );
            let client = self.build_oximeter_client(
                &oximeter_info.collector_id,
                oximeter_info.address,
            );
            for producer in producers.into_iter() {
                let producer_info = oximeter_client::types::ProducerEndpoint {
                    id: producer.id(),
                    address: SocketAddr::new(
                        producer.ip.ip(),
                        producer.port.try_into().unwrap(),
                    )
                    .to_string(),
                    base_route: producer.base_route,
                    interval: oximeter_client::types::Duration::from(
                        Duration::from_secs_f64(producer.interval),
                    ),
                };
                client
                    .producers_post(&producer_info)
                    .await
                    .map_err(Error::from)?;
            }
        }
        Ok(())
    }

    /// List all registered Oximeter collector instances.
    pub async fn oximeter_list(
        &self,
        page_params: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::OximeterInfo> {
        self.db_datastore.oximeter_list(page_params).await
    }

    /// Register as a metric producer with the oximeter metric collection server.
    pub async fn register_as_producer(&self, address: SocketAddr) {
        let producer_endpoint = nexus::ProducerEndpoint {
            id: self.id,
            address,
            base_route: String::from("/metrics/collect"),
            interval: Duration::from_secs(10),
        };
        let register = || async {
            debug!(self.log, "registering nexus as metric producer");
            register(address, &self.log, &producer_endpoint)
                .await
                .map_err(backoff::BackoffError::transient)
        };
        let log_registration_failure = |error, delay| {
            warn!(
                self.log,
                "failed to register nexus as a metric producer, will retry in {:?}", delay;
                "error_message" => ?error,
            );
        };
        backoff::retry_notify(
            backoff::internal_service_policy(),
            register,
            log_registration_failure,
        ).await
        .expect("expected an infinite retry loop registering nexus as a metric producer");
    }

    /// Assign a newly-registered metric producer to an oximeter collector server.
    pub async fn assign_producer(
        &self,
        producer_info: nexus::ProducerEndpoint,
    ) -> Result<(), Error> {
        let (collector, id) = self.next_collector().await?;
        let db_info = db::model::ProducerEndpoint::new(&producer_info, id);
        self.db_datastore.producer_endpoint_create(&db_info).await?;
        collector
            .producers_post(&oximeter_client::types::ProducerEndpoint::from(
                &producer_info,
            ))
            .await
            .map_err(Error::from)?;
        info!(
            self.log,
            "assigned collector to new producer";
            "producer_id" => ?producer_info.id,
            "collector_id" => ?id,
        );
        Ok(())
    }

    /// List existing timeseries schema.
    pub async fn timeseries_schema_list(
        &self,
        opctx: &OpContext,
        pag_params: &TimeseriesSchemaPaginationParams,
        limit: NonZeroU32,
    ) -> Result<dropshot::ResultsPage<TimeseriesSchema>, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.timeseries_client
            .get()
            .await
            .map_err(|e| Error::internal_error(&e.to_string()))?
            .timeseries_schema_list(&pag_params.page, limit)
            .await
            .map_err(|e| match e {
                oximeter_db::Error::DatabaseUnavailable(_) => {
                    Error::ServiceUnavailable {
                        internal_message: e.to_string(),
                    }
                }
                _ => Error::InternalError { internal_message: e.to_string() },
            })
    }

    // Internal helper to build an Oximeter client from its ID and address (common data between
    // model type and the API type).
    fn build_oximeter_client(
        &self,
        id: &Uuid,
        address: SocketAddr,
    ) -> OximeterClient {
        let client_log =
            self.log.new(o!("oximeter-collector" => id.to_string()));
        let client =
            OximeterClient::new(&format!("http://{}", address), client_log);
        info!(
            self.log,
            "registered oximeter collector client";
            "id" => id.to_string(),
        );
        client
    }

    // Return an oximeter collector to assign a newly-registered producer
    async fn next_collector(&self) -> Result<(OximeterClient, Uuid), Error> {
        // TODO-robustness Replace with a real load-balancing strategy.
        let page_params = DataPageParams {
            marker: None,
            direction: dropshot::PaginationOrder::Ascending,
            limit: std::num::NonZeroU32::new(1).unwrap(),
        };
        let oxs = self.db_datastore.oximeter_list(&page_params).await?;
        let info = oxs.first().ok_or_else(|| Error::ServiceUnavailable {
            internal_message: String::from("no oximeter collectors available"),
        })?;
        let address =
            SocketAddr::from((info.ip.ip(), info.port.try_into().unwrap()));
        let id = info.id;
        Ok((self.build_oximeter_client(&id, address), id))
    }
}
