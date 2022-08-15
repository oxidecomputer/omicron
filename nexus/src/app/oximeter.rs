// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Oximeter-related functionality

use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::identity::Asset;
use crate::db::lookup::LookupPath;
use crate::external_api::params;
use crate::external_api::params::MetricName;
use crate::external_api::params::ResourceMetricQuery;
use crate::external_api::params::ResourceMetrics;
use crate::external_api::params::TimeseriesPageSelector;
use crate::internal_api::params::OximeterInfo;
use dropshot::PaginationParams;
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
use oximeter_db::query::Timestamp;
use oximeter_db::Measurement;
use oximeter_db::PaginationKey;
use oximeter_db::Timeseries;
use oximeter_db::TimeseriesSchema;
use oximeter_db::TimeseriesSchemaPaginationParams;
use oximeter_producer::register;
use slog::Logger;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
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

enum AuthorizeBy {
    Disk(Uuid),
    Project(Uuid),
}

impl AuthorizeBy {
    async fn authenticate(
        &self,
        opctx: &OpContext,
        datastore: &Arc<db::DataStore>,
    ) -> Result<(), Error> {
        match *self {
            AuthorizeBy::Disk(id) => {
                LookupPath::new(opctx, datastore).disk_id(id).fetch().await?;
            }
            AuthorizeBy::Project(id) => {
                LookupPath::new(opctx, datastore)
                    .project_id(id)
                    .fetch()
                    .await?;
            }
        };

        Ok(())
    }
}

// A query which has been parsed based on the user-provided filters.
//
// This query contains:
// - AuthZ information that be validated against the caller's opctx.
// - A "criteria" set that can be propagated to Clickhouse as a query.
struct ParsedQuery {
    timeseries_name: &'static str,
    criteria: Vec<String>,
    authz: AuthorizeBy,
}

impl ParsedQuery {
    fn new(
        timeseries_name: &MetricName,
        filters: &Vec<FilterArgs>,
    ) -> Result<Self, Error> {
        let (timeseries_name, (criteria, authz)) = match timeseries_name {
            MetricName::DiskRead => {
                ("crucible_upstairs:disk_read", parse_disk_filters(filters)?)
            }
            MetricName::DiskWrite => {
                ("crucible_upstairs:disk_write", parse_disk_filters(filters)?)
            }
        };
        Ok(Self { timeseries_name, criteria, authz })
    }

    fn timeseries_name(&self) -> &'static str {
        self.timeseries_name
    }

    fn criteria(&self) -> Vec<&str> {
        self.criteria.iter().map(|s| &**s).collect::<Vec<&str>>()
    }
}

type FilterArgs = (params::Key, params::Predicate, params::Value);

fn parse_filters(filters: &Vec<FilterArgs>) -> Result<Vec<String>, Error> {
    let mut criteria = vec![];

    for (key, predicate, value) in filters {
        let predicate_str = match predicate {
            params::Predicate::Eq => "==",
            params::Predicate::Ne => "!=",
            params::Predicate::Ge => ">=",
            params::Predicate::Gt => ">",
            params::Predicate::Le => "<=",
            params::Predicate::Lt => "<",
            params::Predicate::Like => "~=",
        };

        criteria.push(format!(
            "{}{predicate_str}{}",
            key.0.as_str(),
            value.0.as_str()
        ));
    }
    Ok(criteria)
}

#[derive(thiserror::Error, Debug)]
enum MetricError {
    #[error("Need a key for performing authorization")]
    MissingKey,

    #[error("Invalid metric-filtering key {}", .key.0)]
    BadKey { key: params::Key },

    #[error("Cannot parse metric-filtering value {}: {err}", .value.0)]
    BadValue { value: params::Value, err: String },
}

impl MetricError {
    fn key(key: &params::Key) -> Self {
        Self::BadKey { key: key.clone() }
    }

    fn value<E: ToString>(value: &params::Value, err: E) -> Self {
        Self::BadValue { value: value.clone(), err: err.to_string() }
    }
}

impl From<MetricError> for Error {
    fn from(err: MetricError) -> Self {
        Error::invalid_request(&err.to_string())
    }
}

fn parse_disk_filters(
    filters: &Vec<FilterArgs>,
) -> Result<(Vec<String>, AuthorizeBy), Error> {
    let authz = validate_disk_filters(filters)?;
    let criteria = parse_filters(filters)?;
    Ok((criteria, authz))
}

fn validate_disk_filters(
    filters: &Vec<FilterArgs>,
) -> Result<AuthorizeBy, Error> {
    let mut authz = None;
    for (key, _predicate, value) in filters {
        match key.0.as_str() {
            "disk_uuid" => {
                let id = value
                    .0
                    .parse::<Uuid>()
                    .map_err(|e| MetricError::value(value, e))?;

                // Requesting for a disk metric by disk UUID is as specific as
                // we can get. This replaces other filters, including by project
                // UUID.
                authz.replace(AuthorizeBy::Disk(id));
            }
            "project_uuid" => {
                let id = value
                    .0
                    .parse::<Uuid>()
                    .map_err(|e| MetricError::value(value, e))?;

                // If there was no authz info, use this. However,
                // authz-by-disk could be more specific.
                authz.get_or_insert(AuthorizeBy::Project(id));
            }
            _ => return Err(MetricError::key(key).into()),
        }
    }

    authz.ok_or_else(|| MetricError::MissingKey.into())
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
            .map_err(map_oximeter_err)
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
    pub async fn select_timeseries(
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

        let (start_time, end_time, query) = match query_params.page {
            // Generally, we want the time bounds to be inclusive for the
            // start time, and exclusive for the end time...
            dropshot::WhichPage::First(query) => (
                Timestamp::Inclusive(query.start_time),
                Timestamp::Exclusive(query.end_time),
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
                None,
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
                }
            },
        )
        .unwrap())
    }

    // NOTE: Below is some "dead code" we'd use to authorize the returned results if
    // we don't believe the pre-query authz was sufficient.
    //
    // Is this actually necessary? Kinda waiting on RFD 304.

    /*
        async fn authenticate_disk_metric(
            &self,
            opctx: &OpContext,
            timeseries: Vec<Timeseries>
        ) -> Result<Vec<Timeseries>, Error> {
            let mut result = vec![];
            for t in timeseries {
                let target = &t.target;
                let disk_id = target.fields.iter().find_map(|field| {
                    if field.name == "disk_uuid" {
                        match field.value {
                            oximeter::FieldValue::Uuid(id) => Some(id),
                            _ => None,
                        }
                    } else {
                        None
                    }
                }).ok_or_else(|| {
                    Error::internal_error("Requested metric could not be authorized")
                })?;

                // Authenticate the requested disk id.
                let _ = crate::db::lookup::LookupPath::new(opctx, &self.db_datastore)
                    .disk_id(disk_id)
                    .fetch()
                    .await?;
                result.push(t);
            }
            Ok(result)
        }

        async fn authenticate_metric(
            &self,
            opctx: &OpContext,
            timeseries_name: &MetricName,
            timeseries: Vec<Timeseries>
        ) -> Result<Vec<Timeseries>, Error> {
            Ok(match timeseries_name {
                MetricName::DiskRead => self.authenticate_disk_metric(opctx, timeseries).await?,
                MetricName::DiskWrite => self.authenticate_disk_metric(opctx, timeseries).await?,
            })
        }
    */

    /// Returns a results from the timeseries DB based on the provided query
    /// parameters.
    ///
    /// * `opctx`: The calling context, including authentication information.
    /// * `timeseries_name`: The "target:metric" name identifying the metric to
    /// be queried.
    /// * `query_params`: Pagination parameter, identifying which page of
    /// results to return, and filters which should be applied.
    /// * `limit`: The maximum number of results to return in a paginated
    /// request.
    pub async fn select_timeseries2(
        &self,
        opctx: &OpContext,
        timeseries_name: &MetricName,
        query_params: PaginationParams<
            ResourceMetricQuery,
            TimeseriesPageSelector,
        >,
        limit: NonZeroU32,
    ) -> Result<dropshot::ResultsPage<Timeseries>, Error> {
        #[inline]
        fn no_results() -> dropshot::ResultsPage<Timeseries> {
            dropshot::ResultsPage { next_page: None, items: Vec::new() }
        }

        let (query, last) = match query_params.page {
            dropshot::WhichPage::First(query) => (query, None),
            dropshot::WhichPage::Next(page_selector) => {
                (page_selector.query, Some(page_selector.last))
            }
        };
        if query.start_time >= query.end_time {
            return Ok(no_results());
        }

        // Validate the filter parameters to this particular metric.
        let parsed_query = ParsedQuery::new(timeseries_name, &query.filters)?;

        // Authorize access to the information we'll be viewing by issuing this
        // query.
        parsed_query.authz.authenticate(opctx, &self.datastore()).await?;

        // Actually issue the query.
        let client = self.timeseries_client.get().await.map_err(|e| {
            Error::internal_error(&format!(
                "Cannot access timeseries DB: {}",
                e
            ))
        })?;
        let timeseries_list = client
            .select_timeseries_with(
                parsed_query.timeseries_name(),
                &parsed_query.criteria(),
                Some(Timestamp::Inclusive(query.start_time)),
                Some(Timestamp::Exclusive(query.end_time)),
                Some(limit),
                last,
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

        if timeseries_list.is_empty() {
            return Ok(no_results());
        }

        // TODO: authenticate + filter the returned timeseries?

        Ok(dropshot::ResultsPage::new(
            timeseries_list,
            &query,
            |last_timeseries: &Timeseries, query: &ResourceMetricQuery| {
                let last: PaginationKey = (
                    last_timeseries.timeseries_key,
                    last_timeseries
                        .measurements
                        .last()
                        .as_ref()
                        .unwrap()
                        .timestamp(),
                );
                TimeseriesPageSelector { query: query.clone(), last }
            },
        )
        .unwrap())
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

fn map_oximeter_err(error: oximeter_db::Error) -> Error {
    match error {
        oximeter_db::Error::DatabaseUnavailable(_) => {
            Error::ServiceUnavailable { internal_message: error.to_string() }
        }
        _ => Error::InternalError { internal_message: error.to_string() },
    }
}
