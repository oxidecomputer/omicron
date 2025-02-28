// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::HttpErrorResponseBody;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_types::external_api::params;
use nexus_types::external_api::views::OxqlQueryResult;
use omicron_test_utils::dev::poll;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_test_utils::dev::poll::CondCheckError;
use oximeter::TimeseriesSchema;
use serde::de::DeserializeOwned;
use slog::Logger;
use std::borrow::Cow;
use std::time::Duration;

enum TimeseriesQueryResult {
    TimeseriesNotFound,
    Ok(Vec<oxql_types::Table>),
}

pub(super) struct MetricsNotYet {
    note: Cow<'static, str>,
}

impl MetricsNotYet {
    pub fn new<S>(note: S) -> Self
    where
        S: Into<Cow<'static, str>>,
    {
        Self { note: note.into() }
    }
}

pub(super) struct MetricsQuerier<'a, N> {
    ctx: &'a ControlPlaneTestContext<N>,
}

impl<'a, N> MetricsQuerier<'a, N> {
    const POLL_INTERVAL: Duration = Duration::from_secs(1);
    const POLL_MAX: Duration = Duration::from_secs(30);

    pub fn new(ctx: &'a ControlPlaneTestContext<N>) -> Self {
        Self { ctx }
    }

    pub async fn system_timeseries_query_until<F, T>(
        &self,
        query: &str,
        cond: F,
    ) -> T
    where
        F: Fn(Vec<oxql_types::Table>) -> Result<T, MetricsNotYet>,
    {
        self.timeseries_query_until("/v1/system/timeseries/query", query, cond)
            .await
    }

    pub async fn project_timeseries_query_until<F, T>(
        &self,
        project: &str,
        query: &str,
        cond: F,
    ) -> T
    where
        F: Fn(Vec<oxql_types::Table>) -> Result<T, MetricsNotYet>,
    {
        self.timeseries_query_until(
            &format!("/v1/timeseries/query?project={project}"),
            query,
            cond,
        )
        .await
    }

    pub async fn project_timeseries_query(
        &self,
        project: &str,
        query: &str,
    ) -> Vec<oxql_types::Table> {
        self.project_timeseries_query_until(project, query, |tables| Ok(tables))
            .await
    }

    pub async fn wait_for_timeseries_schema<F, T>(&self, cond: F) -> T
    where
        F: Fn(Vec<TimeseriesSchema>) -> Result<T, MetricsNotYet>,
    {
        self.wait_for_objects("/v1/system/timeseries/schemas", cond).await
    }

    async fn wait_for_objects<F, T, U>(&self, endpoint: &str, cond: F) -> T
    where
        F: Fn(Vec<U>) -> Result<T, MetricsNotYet>,
        U: DeserializeOwned,
    {
        let result = wait_for_condition(
            || async {
                self.ctx
                    .oximeter
                    .try_force_collect()
                    .await
                    .expect("sent trigger to force oximeter collection");

                let page = objects_list_page_authz::<U>(
                    &self.ctx.external_client,
                    endpoint,
                )
                .await;

                match cond(page.items) {
                    Ok(res) => Ok(res),
                    Err(MetricsNotYet { note }) => {
                        info!(
                            self.log(),
                            "Metrics condition not yet true (will retry)";
                            "note" => %note,
                        );
                        Err(CondCheckError::<()>::NotYet)
                    }
                }
            },
            &Self::POLL_INTERVAL,
            &Self::POLL_MAX,
        )
        .await;

        match result {
            Ok(r) => r,
            Err(poll::Error::TimedOut(duration)) => {
                panic!(
                    "Timed out after {duration:?} waiting for objects list \
                    success, endpoint: '{endpoint}'"
                );
            }
            Err(poll::Error::PermanentError(_)) => unreachable!(
                "wait_for_condition closure never returns permanent errors"
            ),
        }
    }

    async fn timeseries_query_until<F, T>(
        &self,
        endpoint: &str,
        query: &str,
        cond: F,
    ) -> T
    where
        F: Fn(Vec<oxql_types::Table>) -> Result<T, MetricsNotYet>,
    {
        let result = wait_for_condition(
            || async {
                self.ctx
                    .oximeter
                    .try_force_collect()
                    .await
                    .expect("sent trigger to force oximeter collection");

                let tables = match self
                    .execute_query_once(endpoint, query.to_string())
                    .await
                {
                    TimeseriesQueryResult::Ok(r) => r,
                    TimeseriesQueryResult::TimeseriesNotFound => {
                        info!(
                            self.log(),
                            "Timeseries not found (will retry)";
                            "query" => %query,
                        );
                        return Err(CondCheckError::<()>::NotYet);
                    }
                };

                match cond(tables) {
                    Ok(res) => Ok(res),
                    Err(MetricsNotYet { note }) => {
                        info!(
                            self.log(),
                            "Metrics condition not yet true (will retry)";
                            "note" => %note,
                        );
                        Err(CondCheckError::NotYet)
                    }
                }
            },
            &Self::POLL_INTERVAL,
            &Self::POLL_MAX,
        )
        .await;

        match result {
            Ok(r) => r,
            Err(poll::Error::TimedOut(duration)) => {
                panic!(
                    "Timed out after {duration:?} waiting for timeseries query \
                    success, endpoint: '{endpoint}', query: '{query}'");
            }
            Err(poll::Error::PermanentError(_)) => unreachable!(
                "wait_for_condition closure never returns permanent errors"
            ),
        }
    }

    fn log(&self) -> &Logger {
        &self.ctx.logctx.log
    }

    // Execute a single query one time.
    //
    // May return `TimeseriesNotFound` if the metrics we're trying to query
    // don't exist yet. Panics on any other error.
    async fn execute_query_once(
        &self,
        endpoint: &str,
        query: String,
    ) -> TimeseriesQueryResult {
        // Issue the query.
        let body = params::TimeseriesQuery { query };
        let query = &body.query;
        let rsp = NexusRequest::new(
            RequestBuilder::new(
                &self.ctx.external_client,
                http::Method::POST,
                endpoint,
            )
            .body(Some(&body)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap_or_else(|e| {
            panic!("timeseries query failed: {e:?}\nquery: {query}")
        });

        // Check for a timeseries-not-found error specifically.
        if rsp.status.is_client_error() {
            let err = rsp
                .parsed_body::<HttpErrorResponseBody>()
                .unwrap_or_else(|e| {
                    panic!(
                    "could not parse body as `HttpErrorResponseBody`: {e:?}\n\
                     query: {query}\nresponse: {rsp:#?}",
                )
                });

            if err.message.starts_with("Timeseries not found for: ") {
                return TimeseriesQueryResult::TimeseriesNotFound;
            }
        }

        // Try to parse the query as usual, which will fail on other kinds of
        // errors.
        TimeseriesQueryResult::Ok(
            rsp.parsed_body::<OxqlQueryResult>()
                .unwrap_or_else(|e| {
                    panic!(
                        "could not parse timeseries query response: {e:?}\n\
                        query: {query}\nresponse: {rsp:#?}"
                    );
                })
                .tables,
        )
    }
}
