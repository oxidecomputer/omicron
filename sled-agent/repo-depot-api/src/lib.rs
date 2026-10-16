// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::{FreeformBody, HttpError, HttpResponseOk, Path, RequestContext};
use dropshot_api_manager_types::api_versions;
use schemars::JsonSchema;
use serde::Deserialize;
use tufaceous_artifact::ArtifactHash;

api_versions!([
    // Do not create new versions of this client-side versioned API.
    // https://github.com/oxidecomputer/omicron/issues/9290
    //
    // Version 2 of this API is added so we can eventually remove
    // `ArtifactHash::v1_json_schema`. It is being rolled out over multiple
    // releases.
    (2, ARTIFACT_HASH_SCHEMA),
    (1, INITIAL),
]);

#[dropshot::api_description]
pub trait RepoDepotApi {
    type Context;

    /// Fetch an artifact from the depot.
    #[endpoint {
        method = GET,
        path = "/artifact/sha256/{sha256}",
        versions = VERSION_ARTIFACT_HASH_SCHEMA..,
    }]
    async fn artifact_get_by_sha256(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<ArtifactPathParams>,
    ) -> Result<HttpResponseOk<FreeformBody>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/artifact/sha256/{sha256}",
        versions = ..VERSION_ARTIFACT_HASH_SCHEMA,
    }]
    async fn artifact_get_by_sha256_v1(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<ArtifactPathParamsV1>,
    ) -> Result<HttpResponseOk<FreeformBody>, HttpError> {
        Self::artifact_get_by_sha256(rqctx, path_params.map(Into::into)).await
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct ArtifactPathParams {
    pub sha256: ArtifactHash,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct ArtifactPathParamsV1 {
    // Tufaceous v2 introduces a new JSON schema for `ArtifactHash` that is
    // wire-compatible but perceived as different by drift. Continue using the
    // old schema in this API version.
    #[schemars(schema_with = "ArtifactHash::v1_json_schema")]
    pub sha256: ArtifactHash,
}

impl From<ArtifactPathParamsV1> for ArtifactPathParams {
    fn from(value: ArtifactPathParamsV1) -> Self {
        Self { sha256: value.sha256 }
    }
}
