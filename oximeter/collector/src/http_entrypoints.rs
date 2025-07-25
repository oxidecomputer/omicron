// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Oximeter collector server HTTP API

// Copyright 2025 Oxide Computer Company

use crate::OximeterAgent;
use dropshot::ApiDescription;
use dropshot::EmptyScanParams;
use dropshot::HttpError;
use dropshot::HttpResponseDeleted;
use dropshot::HttpResponseOk;
use dropshot::PaginationParams;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::ResultsPage;
use dropshot::WhichPage;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use oximeter_api::*;
use std::sync::Arc;

// Build the HTTP API internal to the control plane
pub fn oximeter_api() -> ApiDescription<Arc<OximeterAgent>> {
    oximeter_api_mod::api_description::<OximeterApiImpl>()
        .expect("registered entrypoints")
}

enum OximeterApiImpl {}

impl OximeterApi for OximeterApiImpl {
    type Context = Arc<OximeterAgent>;

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
        let producers = agent.list_producers(start, limit);
        ResultsPage::new(
            producers,
            &EmptyScanParams {},
            |info: &ProducerEndpoint, _| ProducerPage { id: info.id },
        )
        .map(HttpResponseOk)
    }

    async fn producer_details(
        request_context: RequestContext<Self::Context>,
        path: dropshot::Path<ProducerIdPathParams>,
    ) -> Result<HttpResponseOk<ProducerDetails>, HttpError> {
        let agent = request_context.context();
        let producer_id = path.into_inner().producer_id;
        agent
            .producer_details(producer_id)
            .map_err(HttpError::from)
            .map(HttpResponseOk)
    }

    async fn producer_delete(
        request_context: RequestContext<Self::Context>,
        path: dropshot::Path<ProducerIdPathParams>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let agent = request_context.context();
        let producer_id = path.into_inner().producer_id;
        agent.delete_producer(producer_id);
        Ok(HttpResponseDeleted())
    }

    async fn collector_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<CollectorInfo>, HttpError> {
        let agent = request_context.context();
        let id = agent.id;
        let last_refresh = *agent.last_refresh_time.lock().unwrap();
        let info = CollectorInfo { id, last_refresh };
        Ok(HttpResponseOk(info))
    }
}
