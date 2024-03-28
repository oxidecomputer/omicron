// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Oximeter collector server HTTP API

// Copyright 2023 Oxide Computer Company

use crate::OximeterAgent;
use chrono::DateTime;
use chrono::Utc;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::EmptyScanParams;
use dropshot::HttpError;
use dropshot::HttpResponseDeleted;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::PaginationParams;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::ResultsPage;
use dropshot::TypedBody;
use dropshot::WhichPage;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use uuid::Uuid;

// Build the HTTP API internal to the control plane
pub fn oximeter_api() -> ApiDescription<Arc<OximeterAgent>> {
    let mut api = ApiDescription::new();
    api.register(producers_post)
        .expect("Could not register producers_post API handler");
    api.register(producers_list)
        .expect("Could not register producers_list API handler");
    api.register(producer_delete)
        .expect("Could not register producers_delete API handler");
    api.register(collector_info)
        .expect("Could not register collector_info API handler");
    api
}

// Handle a request from Nexus to register a new producer with this collector.
#[endpoint {
    method = POST,
    path = "/producers",
}]
async fn producers_post(
    request_context: RequestContext<Arc<OximeterAgent>>,
    body: TypedBody<ProducerEndpoint>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let agent = request_context.context();
    let producer_info = body.into_inner();
    agent
        .register_producer(producer_info)
        .await
        .map_err(HttpError::from)
        .map(|_| HttpResponseUpdatedNoContent())
}

// Parameters for paginating the list of producers.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
struct ProducerPage {
    id: Uuid,
}

// List all producers
#[endpoint {
    method = GET,
    path = "/producers",
}]
async fn producers_list(
    request_context: RequestContext<Arc<OximeterAgent>>,
    query: Query<PaginationParams<EmptyScanParams, ProducerPage>>,
) -> Result<HttpResponseOk<ResultsPage<ProducerEndpoint>>, HttpError> {
    let agent = request_context.context();
    let pagination = query.into_inner();
    let limit = request_context.page_limit(&pagination)?.get() as usize;
    let start = match &pagination.page {
        WhichPage::First(..) => None,
        WhichPage::Next(ProducerPage { id }) => Some(*id),
    };
    let producers = agent.list_producers(start, limit).await;
    ResultsPage::new(
        producers,
        &EmptyScanParams {},
        |info: &ProducerEndpoint, _| ProducerPage { id: info.id },
    )
    .map(HttpResponseOk)
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
struct ProducerIdPathParams {
    producer_id: Uuid,
}

// Delete a producer by ID.
#[endpoint {
    method = DELETE,
    path = "/producers/{producer_id}",
}]
async fn producer_delete(
    request_context: RequestContext<Arc<OximeterAgent>>,
    path: dropshot::Path<ProducerIdPathParams>,
) -> Result<HttpResponseDeleted, HttpError> {
    let agent = request_context.context();
    let producer_id = path.into_inner().producer_id;
    agent
        .delete_producer(producer_id)
        .await
        .map_err(HttpError::from)
        .map(|_| HttpResponseDeleted())
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct CollectorInfo {
    /// The collector's UUID.
    pub id: Uuid,
    /// Last time we refreshed our producer list with Nexus.
    pub last_refresh: Option<DateTime<Utc>>,
}

// Return identifying information about this collector
#[endpoint {
    method = GET,
    path = "/info",
}]
async fn collector_info(
    request_context: RequestContext<Arc<OximeterAgent>>,
) -> Result<HttpResponseOk<CollectorInfo>, HttpError> {
    let agent = request_context.context();
    let id = agent.id;
    let last_refresh = *agent.last_refresh_time.lock().unwrap();
    let info = CollectorInfo { id, last_refresh };
    Ok(HttpResponseOk(info))
}
