// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for simulating the crucible pantry API.

use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseDeleted, HttpResponseOk,
    HttpResponseUpdatedNoContent, Path as TypedPath, RequestContext, TypedBody,
};
use propolis_client::types::VolumeConstructionRequest;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::storage::Pantry;

type CruciblePantryApiDescription = ApiDescription<Arc<Pantry>>;

/// Returns a description of the crucible pantry API.
pub fn api() -> CruciblePantryApiDescription {
    fn register_endpoints(
        api: &mut CruciblePantryApiDescription,
    ) -> Result<(), String> {
        api.register(attach)?;
        api.register(is_job_finished)?;
        api.register(job_result_ok)?;
        api.register(import_from_url)?;
        api.register(snapshot)?;
        api.register(bulk_write)?;
        api.register(scrub)?;
        api.register(detach)?;

        Ok(())
    }

    let mut api = CruciblePantryApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

// TODO: We'd like to de-duplicate as much as possible with the real crucible
// pantry here, to avoid skew. However, this was wholesale copied from the
// crucible repo!

#[derive(Deserialize, JsonSchema)]
struct VolumePath {
    pub id: String,
}

#[derive(Deserialize, JsonSchema)]
struct AttachRequest {
    pub volume_construction_request: VolumeConstructionRequest,
}

#[derive(Serialize, JsonSchema)]
struct AttachResult {
    pub id: String,
}

/// Construct a volume from a VolumeConstructionRequest, storing the result in
/// the Pantry.
#[endpoint {
    method = POST,
    path = "/crucible/pantry/0/volume/{id}",
}]
async fn attach(
    rc: RequestContext<Arc<Pantry>>,
    path: TypedPath<VolumePath>,
    body: TypedBody<AttachRequest>,
) -> Result<HttpResponseOk<AttachResult>, HttpError> {
    let path = path.into_inner();
    let body = body.into_inner();
    let pantry = rc.context();

    pantry
        .attach(path.id.clone(), body.volume_construction_request)
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseOk(AttachResult { id: path.id }))
}

#[derive(Deserialize, JsonSchema)]
struct JobPath {
    pub id: String,
}

#[derive(Serialize, JsonSchema)]
struct JobPollResponse {
    pub job_is_finished: bool,
}

/// Poll to see if a Pantry background job is done
#[endpoint {
    method = GET,
    path = "/crucible/pantry/0/job/{id}/is-finished",
}]
async fn is_job_finished(
    rc: RequestContext<Arc<Pantry>>,
    path: TypedPath<JobPath>,
) -> Result<HttpResponseOk<JobPollResponse>, HttpError> {
    let path = path.into_inner();
    let pantry = rc.context();

    let job_is_finished = pantry.is_job_finished(path.id).await?;

    Ok(HttpResponseOk(JobPollResponse { job_is_finished }))
}

#[derive(Serialize, JsonSchema)]
pub struct JobResultOkResponse {
    pub job_result_ok: bool,
}

/// Block on returning a Pantry background job result, then return 200 OK if the
/// job executed OK, 500 otherwise.
#[endpoint {
    method = GET,
    path = "/crucible/pantry/0/job/{id}/ok",
}]
async fn job_result_ok(
    rc: RequestContext<Arc<Pantry>>,
    path: TypedPath<JobPath>,
) -> Result<HttpResponseOk<JobResultOkResponse>, HttpError> {
    let path = path.into_inner();
    let pantry = rc.context();

    let job_result = pantry.get_job_result(path.id).await?;

    match job_result {
        Ok(job_result_ok) => {
            Ok(HttpResponseOk(JobResultOkResponse { job_result_ok }))
        }
        Err(e) => Err(HttpError::for_internal_error(e.to_string())),
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedDigest {
    Sha256(String),
}

#[derive(Deserialize, JsonSchema)]
struct ImportFromUrlRequest {
    pub url: String,
    pub expected_digest: Option<ExpectedDigest>,
}

#[derive(Serialize, JsonSchema)]
struct ImportFromUrlResponse {
    pub job_id: String,
}

/// Import data from a URL into a volume
#[endpoint {
    method = POST,
    path = "/crucible/pantry/0/volume/{id}/import-from-url",
}]
async fn import_from_url(
    rc: RequestContext<Arc<Pantry>>,
    path: TypedPath<VolumePath>,
    body: TypedBody<ImportFromUrlRequest>,
) -> Result<HttpResponseOk<ImportFromUrlResponse>, HttpError> {
    let path = path.into_inner();
    let body = body.into_inner();
    let pantry = rc.context();

    let job_id = pantry
        .import_from_url(path.id.clone(), body.url, body.expected_digest)
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseOk(ImportFromUrlResponse { job_id }))
}

#[derive(Deserialize, JsonSchema)]
struct SnapshotRequest {
    pub snapshot_id: String,
}

/// Take a snapshot of a volume
#[endpoint {
    method = POST,
    path = "/crucible/pantry/0/volume/{id}/snapshot",
}]
async fn snapshot(
    rc: RequestContext<Arc<Pantry>>,
    path: TypedPath<VolumePath>,
    body: TypedBody<SnapshotRequest>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let body = body.into_inner();
    let pantry = rc.context();

    pantry
        .snapshot(path.id.clone(), body.snapshot_id)
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
struct BulkWriteRequest {
    pub offset: u64,

    pub base64_encoded_data: String,
}

/// Bulk write data into a volume at a specified offset
#[endpoint {
    method = POST,
    path = "/crucible/pantry/0/volume/{id}/bulk-write",
}]
async fn bulk_write(
    rc: RequestContext<Arc<Pantry>>,
    path: TypedPath<VolumePath>,
    body: TypedBody<BulkWriteRequest>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let body = body.into_inner();
    let pantry = rc.context();

    let data = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        body.base64_encoded_data,
    )
    .map_err(|e| HttpError::for_bad_request(None, e.to_string()))?;

    pantry.bulk_write(path.id.clone(), body.offset, data).await?;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Serialize, JsonSchema)]
struct ScrubResponse {
    pub job_id: String,
}

/// Scrub the volume (copy blocks from read-only parent to subvolumes)
#[endpoint {
    method = POST,
    path = "/crucible/pantry/0/volume/{id}/scrub",
}]
async fn scrub(
    rc: RequestContext<Arc<Pantry>>,
    path: TypedPath<VolumePath>,
) -> Result<HttpResponseOk<ScrubResponse>, HttpError> {
    let path = path.into_inner();
    let pantry = rc.context();

    let job_id = pantry
        .scrub(path.id.clone())
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseOk(ScrubResponse { job_id }))
}

/// Flush and close a volume, removing it from the Pantry
#[endpoint {
    method = DELETE,
    path = "/crucible/pantry/0/volume/{id}",
}]
async fn detach(
    rc: RequestContext<Arc<Pantry>>,
    path: TypedPath<VolumePath>,
) -> Result<HttpResponseDeleted, HttpError> {
    let path = path.into_inner();
    let pantry = rc.context();

    pantry
        .detach(path.id)
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseDeleted())
}
