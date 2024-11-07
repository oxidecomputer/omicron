// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::{FreeformBody, HttpError, HttpResponseOk, Path, RequestContext};
use omicron_common::update::ArtifactHash;
use schemars::JsonSchema;
use serde::Deserialize;

#[dropshot::api_description]
pub trait RepoDepotApi {
    type Context;

    /// Fetch an artifact from the depot.
    #[endpoint {
        method = GET,
        path = "/artifact/sha256/{sha256}",
    }]
    async fn artifact_get_by_sha256(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<ArtifactPathParams>,
    ) -> Result<HttpResponseOk<FreeformBody>, HttpError>;
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct ArtifactPathParams {
    pub sha256: ArtifactHash,
}
