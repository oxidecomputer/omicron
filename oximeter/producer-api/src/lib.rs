// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API description for the Oximeter producer server.

// Copyright 2026 Oxide Computer Company

use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::Path;
use dropshot::RequestContext;
use oximeter_types_versions::latest::producer::ProducerIdPathParams;
use oximeter_types_versions::latest::types::ProducerResults;

#[dropshot::api_description]
pub trait ProducerApi {
    type Context;

    /// Collect metric data from this producer.
    #[endpoint {
        method = GET,
        path = "/{producer_id}",
    }]
    async fn collect(
        request_context: RequestContext<Self::Context>,
        path_params: Path<ProducerIdPathParams>,
    ) -> Result<HttpResponseOk<ProducerResults>, HttpError>;
}
