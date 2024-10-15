// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Oximeter-related functionality

use crate::external_api::params::ResourceMetrics;
use crate::internal_api::params::OximeterInfo;
use dropshot::PaginationParams;
use internal_dns_resolver::{ResolveError, Resolver};
use internal_dns_types::names::ServiceName;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use omicron_common::address::CLICKHOUSE_HTTP_PORT;
use omicron_common::api::external::{DataPageParams, Error, ListResultVec};
use omicron_common::api::internal::nexus::{self, ProducerEndpoint};
use oximeter_client::Client as OximeterClient;
use oximeter_db::query::Timestamp;
use oximeter_db::Measurement;
use slog::Logger;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::time::Duration;
use uuid::Uuid;

/// How long a metrics producer remains registered to a collector.
///
/// Producers are expected to renew their registration lease periodically, at
/// some interval of this overall duration.
pub const PRODUCER_LEASE_DURATION: Duration = Duration::from_secs(10 * 60);

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

    pub(crate) async fn get(
        &self,
    ) -> Result<oximeter_db::Client, ResolveError> {
        let address = match &self.source {
            ClientSource::FromIp { address } => *address,
            ClientSource::FromDns { resolver } => SocketAddr::new(
                resolver.lookup_ip(ServiceName::Clickhouse).await?,
                CLICKHOUSE_HTTP_PORT,
            ),
        };

        Ok(oximeter_db::Client::new(address, &self.log))
    }
}

impl super::Nexus {
    /// Insert a new record of an Oximeter collector server.
    pub(crate) async fn upsert_oximeter_collector(
        &self,
        opctx: &OpContext,
        oximeter_info: &OximeterInfo,
    ) -> Result<(), Error> {
        // Insert the Oximeter instance into the DB. Note that this _updates_ the record,
        // specifically, the time_modified, ip, and port columns, if the instance has already been
        // registered.
        let db_info = db::model::OximeterInfo::new(&oximeter_info);
        self.db_datastore.oximeter_create(opctx, &db_info).await?;
        info!(
            self.log,
            "registered new oximeter metric collection server";
            "collector_id" => ?oximeter_info.collector_id,
            "address" => oximeter_info.address,
        );
        Ok(())
    }

    /// List the producers assigned to an oximeter collector.
    pub(crate) async fn list_assigned_producers(
        &self,
        opctx: &OpContext,
        collector_id: Uuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<ProducerEndpoint> {
        self.db_datastore
            .producers_list_by_oximeter_id(opctx, collector_id, pagparams)
            .await
            .map(|list| list.into_iter().map(ProducerEndpoint::from).collect())
    }

    /// Assign a newly-registered metric producer to an oximeter collector server.
    pub(crate) async fn assign_producer(
        &self,
        opctx: &OpContext,
        producer_info: nexus::ProducerEndpoint,
    ) -> Result<(), Error> {
        let collector_info = self
            .db_datastore
            .producer_endpoint_upsert_and_assign(opctx, &producer_info)
            .await?;

        let address = SocketAddr::from((
            collector_info.ip.ip(),
            collector_info.port.try_into().unwrap(),
        ));
        let collector =
            build_oximeter_client(&self.log, &collector_info.id, address);

        collector
            .producers_post(&oximeter_client::types::ProducerEndpoint::from(
                &producer_info,
            ))
            .await
            .map_err(Error::from)?;
        info!(
            self.log,
            "assigned collector to new producer";
            "producer_id" => %producer_info.id,
            "collector_id" => %collector_info.id,
        );

        Ok(())
    }

    /// Returns a results from the timeseries DB based on the provided query
    /// parameters.
    ///
    /// * `timeseries_name`: The "target:metric" name identifying the metric to
    /// be queried.
    /// * `criteria`: Any additional parameters to help narrow down the query
    /// selection further. These parameters are passed directly to
    /// [oximeter-db::client::select_timeseries_with].
    /// * `query_params`: Pagination parameter, identifying which page of
    /// results to return.
    /// * `limit`: The maximum number of results to return in a paginated
    /// request.
    pub(crate) async fn select_timeseries(
        &self,
        timeseries_name: &str,
        criteria: &[&str],
        query_params: PaginationParams<ResourceMetrics, ResourceMetrics>,
        limit: NonZeroU32,
    ) -> Result<dropshot::ResultsPage<Measurement>, Error> {
        #[inline]
        fn no_results() -> dropshot::ResultsPage<Measurement> {
            dropshot::ResultsPage { next_page: None, items: Vec::new() }
        }

        let (start_time, end_time, order, query) = match query_params.page {
            // Generally, we want the time bounds to be inclusive for the
            // start time, and exclusive for the end time...
            dropshot::WhichPage::First(query) => (
                Timestamp::Inclusive(query.start_time),
                Timestamp::Exclusive(query.end_time),
                query.order,
                query,
            ),
            // ... but for subsequent pages, we use the "last observed"
            // timestamp as the start time. If we used an inclusive bound,
            // we'd duplicate the returned measurement. To return each
            // measurement exactly once, we make the start time "exclusive"
            // on all "next" pages.
            dropshot::WhichPage::Next(query) => (
                Timestamp::Exclusive(query.start_time),
                Timestamp::Exclusive(query.end_time),
                query.order,
                query,
            ),
        };
        if query.start_time >= query.end_time {
            return Ok(no_results());
        }

        let timeseries_list = self
            .timeseries_client
            .get()
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "Cannot access timeseries DB: {}",
                    e
                ))
            })?
            .select_timeseries_with(
                timeseries_name,
                criteria,
                Some(start_time),
                Some(end_time),
                Some(limit),
                order,
            )
            .await
            .or_else(|err| {
                // If the timeseries name exists in the API, but not in Clickhouse,
                // it might just not have been populated yet.
                match err {
                    oximeter_db::Error::TimeseriesNotFound(_) => Ok(vec![]),
                    _ => Err(err),
                }
            })
            .map_err(map_oximeter_err)?;

        if timeseries_list.len() > 1 {
            return Err(Error::internal_error(&format!(
                "expected 1 timeseries but got {} ({:?} {:?})",
                timeseries_list.len(),
                timeseries_name,
                criteria
            )));
        }

        // If we received no data, exit early.
        let timeseries =
            if let Some(timeseries) = timeseries_list.into_iter().next() {
                timeseries
            } else {
                return Ok(no_results());
            };

        Ok(dropshot::ResultsPage::new(
            timeseries.measurements,
            &query,
            |last_measurement: &Measurement, query: &ResourceMetrics| {
                ResourceMetrics {
                    start_time: last_measurement.timestamp(),
                    end_time: query.end_time,
                    order: None,
                }
            },
        )
        .unwrap())
    }
}

/// Idempotently un-assign a producer from an oximeter collector.
pub(crate) async fn unassign_producer(
    datastore: &DataStore,
    log: &slog::Logger,
    opctx: &OpContext,
    id: &Uuid,
) -> Result<(), Error> {
    if let Some(collector_id) =
        datastore.producer_endpoint_delete(opctx, id).await?
    {
        debug!(
            log,
            "deleted metric producer assignment";
            "producer_id" => %id,
            "collector_id" => %collector_id,
        );
        let oximeter_info =
            datastore.oximeter_lookup(opctx, &collector_id).await?;
        let address =
            SocketAddr::new(oximeter_info.ip.ip(), *oximeter_info.port);
        let client = build_oximeter_client(&log, &id, address);
        if let Err(e) = client.producer_delete(&id).await {
            error!(
                log,
                "failed to delete producer from collector";
                "producer_id" => %id,
                "collector_id" => %collector_id,
                "address" => %address,
                "error" => ?e,
            );
            return Err(Error::internal_error(
                format!("failed to delete producer from collector: {e:?}")
                    .as_str(),
            ));
        } else {
            debug!(
                log,
                "successfully deleted producer from collector";
                "producer_id" => %id,
                "collector_id" => %collector_id,
                "address" => %address,
            );
            Ok(())
        }
    } else {
        trace!(
            log,
            "un-assigned non-existent metric producer";
            "producer_id" => %id,
        );
        Ok(())
    }
}

fn map_oximeter_err(error: oximeter_db::Error) -> Error {
    match error {
        oximeter_db::Error::DatabaseUnavailable(_) => {
            Error::ServiceUnavailable { internal_message: error.to_string() }
        }
        _ => Error::InternalError { internal_message: error.to_string() },
    }
}

// Internal helper to build an Oximeter client from its ID and address (common data between
// model type and the API type).
fn build_oximeter_client(
    log: &slog::Logger,
    id: &Uuid,
    address: SocketAddr,
) -> OximeterClient {
    let client_log = log.new(o!("oximeter-collector" => id.to_string()));
    let client =
        OximeterClient::new(&format!("http://{}", address), client_log);
    info!(
        log,
        "registered oximeter collector client";
        "id" => id.to_string(),
    );
    client
}
