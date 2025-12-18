// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for simulating the crucible pantry API.

use dropshot::{
    ApiDescription, ApiDescriptionRegisterError, HttpError,
    HttpResponseDeleted, HttpResponseOk, HttpResponseUpdatedNoContent,
    Path as TypedPath, RequestContext, TypedBody, endpoint,
};
use propolis_client::VolumeConstructionRequest;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::storage::Pantry;

type CruciblePantryApiDescription = ApiDescription<Arc<Pantry>>;

/// Returns a description of the crucible pantry API.
pub fn api() -> CruciblePantryApiDescription {
    fn register_endpoints(
        api: &mut CruciblePantryApiDescription,
    ) -> Result<(), ApiDescriptionRegisterError> {
        api.register(pantry_status)?;
        api.register(volume_status)?;
        api.register(attach)?;
        api.register(attach_activate_background)?;
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

#[derive(Serialize, JsonSchema)]
pub struct PantryStatus {
    /// Which volumes does this Pantry know about? Note this may include volumes
    /// that are no longer active, and haven't been garbage collected yet.
    pub volumes: Vec<String>,

    /// How many job handles?
    pub num_job_handles: usize,
}

/// Get the Pantry's status
#[endpoint {
    method = GET,
    path = "/crucible/pantry/0",
}]
async fn pantry_status(
    rc: RequestContext<Arc<Pantry>>,
) -> Result<HttpResponseOk<PantryStatus>, HttpError> {
    let pantry = rc.context();

    let status = pantry.status()?;

    Ok(HttpResponseOk(status))
}

#[derive(Deserialize, JsonSchema)]
struct VolumePath {
    pub id: String,
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
pub struct VolumeStatus {
    /// Is the Volume currently active?
    pub active: bool,

    /// Has the Pantry ever seen this Volume active?
    pub seen_active: bool,

    /// How many job handles are there for this Volume?
    pub num_job_handles: usize,
}

/// Get a current Volume's status
#[endpoint {
    method = GET,
    path = "/crucible/pantry/0/volume/{id}",
}]
async fn volume_status(
    rc: RequestContext<Arc<Pantry>>,
    path: TypedPath<VolumePath>,
) -> Result<HttpResponseOk<VolumeStatus>, HttpError> {
    let path = path.into_inner();
    let pantry = rc.context();

    let status = pantry.volume_status(path.id.clone())?;
    Ok(HttpResponseOk(status))
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
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseOk(AttachResult { id: path.id }))
}

#[derive(Deserialize, JsonSchema)]
struct AttachBackgroundRequest {
    pub volume_construction_request: VolumeConstructionRequest,
    pub job_id: String,
}

/// Construct a volume from a VolumeConstructionRequest, storing the result in
/// the Pantry. Activate in a separate job so as not to block the request.
#[endpoint {
    method = POST,
    path = "/crucible/pantry/0/volume/{id}/background",
}]
async fn attach_activate_background(
    rc: RequestContext<Arc<Pantry>>,
    path: TypedPath<VolumePath>,
    body: TypedBody<AttachBackgroundRequest>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let path = path.into_inner();
    let body = body.into_inner();
    let pantry = rc.context();

    pantry.attach_activate_background(
        path.id.clone(),
        body.job_id,
        body.volume_construction_request,
    )?;

    Ok(HttpResponseUpdatedNoContent())
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

    let job_is_finished = pantry.is_job_finished(path.id)?;

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

    let job_result = pantry.get_job_result(path.id)?;

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

    pantry.bulk_write(path.id.clone(), body.offset, data)?;

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
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseOk(ScrubResponse { job_id }))
}

/// Deactivate a volume, removing it from the Pantry
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
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseDeleted())
}

#[cfg(test)]
mod tests {
    use guppy::MetadataCommand;
    use guppy::graph::ExternalSource;
    use guppy::graph::GitReq;
    use guppy::graph::PackageGraph;
    use serde_json::Value;
    use std::path::Path;

    fn load_real_api_as_json() -> serde_json::Value {
        let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("Cargo.toml");
        let mut cmd = MetadataCommand::new();
        cmd.manifest_path(&manifest_path);
        let graph = PackageGraph::from_command(&mut cmd).unwrap();
        let package = graph
            .packages()
            .find(|pkg| pkg.name() == "crucible-pantry-client")
            .unwrap();
        let ExternalSource::Git { req, .. } =
            package.source().parse_external().unwrap()
        else {
            panic!("This should be a Git dependency");
        };
        let part = match req {
            GitReq::Branch(inner) => inner,
            GitReq::Rev(inner) => inner,
            GitReq::Tag(inner) => inner,
            GitReq::Default => "main",
            _ => unreachable!(),
        };
        // Construct the URL for the pointer to the current document.
        let latest_url_pointer = format!(
            "https://raw.githubusercontent.com/oxidecomputer/crucible/{part}/openapi/crucible-pantry/crucible-pantry-latest.json",
        );
        println!("latest url pointer: {:?}", latest_url_pointer);

        // The default timeout of 30 seconds was sometimes not enough
        // heavy load.
        let latest_name = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .build()
            .unwrap()
            .get(&latest_url_pointer)
            .send()
            .unwrap()
            .text()
            .unwrap();

        // From that pointer name, construct the URL for the actual API document
        println!("latest_name: {:?}", latest_name);
        let raw_url = format!(
            "https://raw.githubusercontent.com/oxidecomputer/crucible/{part}/openapi/crucible-pantry/{latest_name}",
        );
        println!("raw_url: {:?}", raw_url);
        let raw_json = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .build()
            .unwrap()
            .get(&raw_url)
            .send()
            .unwrap()
            .text()
            .unwrap();

        serde_json::from_str(&raw_json).unwrap()
    }

    // Regression test for https://github.com/oxidecomputer/omicron/issues/4599.
    #[test]
    fn test_simulated_api_matches_real() {
        let real_api = load_real_api_as_json();
        let Value::String(ref title) = real_api["info"]["title"] else {
            unreachable!();
        };
        let Value::String(ref version) = real_api["info"]["version"] else {
            unreachable!();
        };
        let sim_api = super::api()
            .openapi(title, version.parse().unwrap())
            .json()
            .unwrap();

        // We'll assert that anything which apppears in the simulated API must
        // appear exactly as-is in the real API. I.e., the simulated is a subset
        // (possibly non-strict) of the real API.
        compare_json_values(&sim_api, &real_api, String::new());
    }

    fn compare_json_values(lhs: &Value, rhs: &Value, path: String) {
        match lhs {
            Value::Array(values) => {
                let Value::Array(rhs_values) = &rhs else {
                    panic!(
                        "Expected an array in the real API JSON at \
                        path \"{path}\", found {rhs:?}",
                    );
                };
                assert_eq!(values.len(), rhs_values.len());
                for (i, (left, right)) in
                    values.iter().zip(rhs_values.iter()).enumerate()
                {
                    let new_path = format!("{path}[{i}]");
                    compare_json_values(left, right, new_path);
                }
            }
            Value::Object(map) => {
                let Value::Object(rhs_map) = &rhs else {
                    panic!(
                        "Expected a map in the real API JSON at \
                        path \"{path}\", found {rhs:?}",
                    );
                };
                for (key, value) in map.iter() {
                    // We intentionally skip the "description" key, provided
                    // that the value is also a true String. This is mostly a
                    // one-off for the udpate to Progenitor 0.5.0, which caused
                    // this key to be added. But it's also pretty harmless,
                    // since it's not possible to get this key-value combination
                    // in a real JSON schema.
                    if key == "description" && value.is_string() {
                        continue;
                    }
                    let new_path = format!("{path}/{key}");
                    let rhs_value = rhs_map.get(key).unwrap_or_else(|| {
                        panic!("Real API JSON missing key: \"{new_path}\"")
                    });
                    compare_json_values(value, rhs_value, new_path);
                }
            }
            _ => {
                assert_eq!(lhs, rhs, "Mismatched keys at JSON path \"{path}\"")
            }
        }
    }
}
