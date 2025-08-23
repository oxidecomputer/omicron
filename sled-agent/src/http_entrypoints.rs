// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the sled agent's exposed API

use super::sled_agent::SledAgent;
use crate::sled_agent::Error as SledAgentError;
use crate::support_bundle::storage::SupportBundleQueryType;
use crate::zone_bundle::BundleError;
use bootstore::schemes::v0::NetworkConfig;
use camino::Utf8PathBuf;
use dropshot::{
    ApiDescription, Body, ErrorStatusCode, FreeformBody, Header, HttpError,
    HttpResponseAccepted, HttpResponseCreated, HttpResponseDeleted,
    HttpResponseHeaders, HttpResponseOk, HttpResponseUpdatedNoContent, Path,
    Query, RequestContext, StreamingBody, TypedBody,
};
use nexus_sled_agent_shared::inventory::{
    Inventory, OmicronSledConfig, SledRole,
};
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus::{DiskRuntimeState, SledVmmState};
use omicron_common::api::internal::shared::{
    ExternalIpGatewayMap, ResolvedVpcRouteSet, ResolvedVpcRouteState,
    SledIdentifiers, SwitchPorts, VirtualNetworkInterfaceHost,
};
use range_requests::PotentialRange;
use sled_agent_api::*;
use sled_agent_types::bootstore::BootstoreStatus;
use sled_agent_types::disk::DiskEnsureBody;
use sled_agent_types::early_networking::EarlyNetworkConfig;
use sled_agent_types::firewall_rules::VpcFirewallRulesEnsureBody;
use sled_agent_types::instance::{
    InstanceEnsureBody, InstanceExternalIpBody, VmmPutStateBody,
    VmmPutStateResponse, VmmUnregisterResponse,
};
use sled_agent_types::sled::AddSledRequest;
use sled_agent_types::zone_bundle::{
    BundleUtilization, CleanupContext, CleanupCount, CleanupPeriod,
    StorageLimit, ZoneBundleId, ZoneBundleMetadata,
};
use sled_diagnostics::{
    SledDiagnosticsCommandHttpOutput, SledDiagnosticsQueryOutput,
};
use std::collections::BTreeMap;

type SledApiDescription = ApiDescription<SledAgent>;

/// Returns a description of the sled agent API
pub fn api() -> SledApiDescription {
    sled_agent_api_mod::api_description::<SledAgentImpl>()
        .expect("registered entrypoints")
}

enum SledAgentImpl {}

impl SledAgentApi for SledAgentImpl {
    type Context = SledAgent;

    async fn zone_bundle_list_all(
        rqctx: RequestContext<Self::Context>,
        query: Query<ZoneBundleFilter>,
    ) -> Result<HttpResponseOk<Vec<ZoneBundleMetadata>>, HttpError> {
        let sa = rqctx.context();
        let filter = query.into_inner().filter;
        sa.list_all_zone_bundles(filter.as_deref())
            .await
            .map(HttpResponseOk)
            .map_err(HttpError::from)
    }

    async fn zone_bundle_list(
        rqctx: RequestContext<Self::Context>,
        params: Path<ZonePathParam>,
    ) -> Result<HttpResponseOk<Vec<ZoneBundleMetadata>>, HttpError> {
        let params = params.into_inner();
        let zone_name = params.zone_name;
        let sa = rqctx.context();
        sa.list_zone_bundles(&zone_name)
            .await
            .map(HttpResponseOk)
            .map_err(HttpError::from)
    }

    async fn zone_bundle_get(
        rqctx: RequestContext<Self::Context>,
        params: Path<ZoneBundleId>,
    ) -> Result<HttpResponseHeaders<HttpResponseOk<FreeformBody>>, HttpError>
    {
        let params = params.into_inner();
        let zone_name = params.zone_name;
        let bundle_id = params.bundle_id;
        let sa = rqctx.context();
        let Some(path) = sa
            .get_zone_bundle_paths(&zone_name, &bundle_id)
            .await
            .map_err(HttpError::from)?
            .into_iter()
            .next()
        else {
            return Err(HttpError::for_not_found(
                None,
                format!(
                    "No zone bundle for zone '{}' with ID '{}'",
                    zone_name, bundle_id
                ),
            ));
        };
        let f = tokio::fs::File::open(&path).await.map_err(|e| {
            HttpError::for_internal_error(format!(
                "failed to open zone bundle file at {}: {:?}",
                path, e,
            ))
        })?;
        let file_access = hyper_staticfile::vfs::TokioFileAccess::new(f);
        let file_stream =
            hyper_staticfile::util::FileBytesStream::new(file_access);
        let body = Body::wrap(hyper_staticfile::Body::Full(file_stream));
        let body = FreeformBody(body);
        let mut response =
            HttpResponseHeaders::new_unnamed(HttpResponseOk(body));
        response.headers_mut().append(
            http::header::CONTENT_TYPE,
            "application/gzip".try_into().unwrap(),
        );
        Ok(response)
    }

    async fn zone_bundle_delete(
        rqctx: RequestContext<Self::Context>,
        params: Path<ZoneBundleId>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let params = params.into_inner();
        let zone_name = params.zone_name;
        let bundle_id = params.bundle_id;
        let sa = rqctx.context();
        let paths = sa
            .get_zone_bundle_paths(&zone_name, &bundle_id)
            .await
            .map_err(HttpError::from)?;
        if paths.is_empty() {
            return Err(HttpError::for_not_found(
                None,
                format!(
                    "No zone bundle for zone '{}' with ID '{}'",
                    zone_name, bundle_id
                ),
            ));
        };
        for path in paths.into_iter() {
            tokio::fs::remove_file(&path).await.map_err(|e| {
                HttpError::for_internal_error(format!(
                    "Failed to delete zone bundle: {e}"
                ))
            })?;
        }
        Ok(HttpResponseDeleted())
    }

    async fn zone_bundle_utilization(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<BTreeMap<Utf8PathBuf, BundleUtilization>>,
        HttpError,
    > {
        let sa = rqctx.context();
        sa.zone_bundle_utilization()
            .await
            .map(HttpResponseOk)
            .map_err(HttpError::from)
    }

    async fn zone_bundle_cleanup_context(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<CleanupContext>, HttpError> {
        let sa = rqctx.context();
        Ok(HttpResponseOk(sa.zone_bundle_cleanup_context().await))
    }

    async fn zone_bundle_cleanup_context_update(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<CleanupContextUpdate>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let params = body.into_inner();
        let new_period =
            params.period.map(CleanupPeriod::new).transpose().map_err(|e| {
                HttpError::from(SledAgentError::from(BundleError::from(e)))
            })?;
        let new_priority = params.priority;
        let new_limit =
            params.storage_limit.map(StorageLimit::new).transpose().map_err(
                |e| HttpError::from(SledAgentError::from(BundleError::from(e))),
            )?;
        sa.update_zone_bundle_cleanup_context(
            new_period,
            new_limit,
            new_priority,
        )
        .await
        .map(|_| HttpResponseUpdatedNoContent())
        .map_err(HttpError::from)
    }

    async fn support_bundle_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SupportBundleListPathParam>,
    ) -> Result<HttpResponseOk<Vec<SupportBundleMetadata>>, HttpError> {
        let sa = rqctx.context();

        let SupportBundleListPathParam { zpool_id, dataset_id } =
            path_params.into_inner();

        let bundles =
            sa.as_support_bundle_storage().list(zpool_id, dataset_id).await?;

        Ok(HttpResponseOk(bundles))
    }

    async fn support_bundle_start_creation(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SupportBundlePathParam>,
    ) -> Result<HttpResponseCreated<SupportBundleMetadata>, HttpError> {
        let sa = rqctx.context();

        let SupportBundlePathParam { zpool_id, dataset_id, support_bundle_id } =
            path_params.into_inner();

        let metadata = sa
            .as_support_bundle_storage()
            .start_creation(zpool_id, dataset_id, support_bundle_id)
            .await?;

        Ok(HttpResponseCreated(metadata))
    }

    async fn support_bundle_transfer(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SupportBundlePathParam>,
        query_params: Query<SupportBundleTransferQueryParams>,
        body: StreamingBody,
    ) -> Result<HttpResponseCreated<SupportBundleMetadata>, HttpError> {
        let sa = rqctx.context();

        let SupportBundlePathParam { zpool_id, dataset_id, support_bundle_id } =
            path_params.into_inner();
        let SupportBundleTransferQueryParams { offset } =
            query_params.into_inner();

        let metadata = sa
            .as_support_bundle_storage()
            .transfer(
                zpool_id,
                dataset_id,
                support_bundle_id,
                offset,
                body.into_stream(),
            )
            .await?;

        Ok(HttpResponseCreated(metadata))
    }

    async fn support_bundle_finalize(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SupportBundlePathParam>,
        query_params: Query<SupportBundleFinalizeQueryParams>,
    ) -> Result<HttpResponseCreated<SupportBundleMetadata>, HttpError> {
        let sa = rqctx.context();

        let SupportBundlePathParam { zpool_id, dataset_id, support_bundle_id } =
            path_params.into_inner();
        let SupportBundleFinalizeQueryParams { hash } =
            query_params.into_inner();

        let metadata = sa
            .as_support_bundle_storage()
            .finalize(zpool_id, dataset_id, support_bundle_id, hash)
            .await?;

        Ok(HttpResponseCreated(metadata))
    }

    async fn support_bundle_download(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequestHeaders>,
        path_params: Path<SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError> {
        let sa = rqctx.context();
        let SupportBundlePathParam { zpool_id, dataset_id, support_bundle_id } =
            path_params.into_inner();

        let range = headers
            .into_inner()
            .range
            .map(|r| PotentialRange::new(r.as_bytes()));
        Ok(sa
            .as_support_bundle_storage()
            .get(
                zpool_id,
                dataset_id,
                support_bundle_id,
                range,
                SupportBundleQueryType::Whole,
            )
            .await?)
    }

    async fn support_bundle_download_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequestHeaders>,
        path_params: Path<SupportBundleFilePathParam>,
    ) -> Result<http::Response<Body>, HttpError> {
        let sa = rqctx.context();
        let SupportBundleFilePathParam {
            parent:
                SupportBundlePathParam { zpool_id, dataset_id, support_bundle_id },
            file,
        } = path_params.into_inner();

        let range = headers
            .into_inner()
            .range
            .map(|r| PotentialRange::new(r.as_bytes()));
        Ok(sa
            .as_support_bundle_storage()
            .get(
                zpool_id,
                dataset_id,
                support_bundle_id,
                range,
                SupportBundleQueryType::Path { file_path: file },
            )
            .await?)
    }

    async fn support_bundle_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequestHeaders>,
        path_params: Path<SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError> {
        let sa = rqctx.context();
        let SupportBundlePathParam { zpool_id, dataset_id, support_bundle_id } =
            path_params.into_inner();

        let range = headers
            .into_inner()
            .range
            .map(|r| PotentialRange::new(r.as_bytes()));
        Ok(sa
            .as_support_bundle_storage()
            .get(
                zpool_id,
                dataset_id,
                support_bundle_id,
                range,
                SupportBundleQueryType::Index,
            )
            .await?)
    }

    async fn support_bundle_head(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequestHeaders>,
        path_params: Path<SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError> {
        let sa = rqctx.context();
        let SupportBundlePathParam { zpool_id, dataset_id, support_bundle_id } =
            path_params.into_inner();

        let range = headers
            .into_inner()
            .range
            .map(|r| PotentialRange::new(r.as_bytes()));
        Ok(sa
            .as_support_bundle_storage()
            .head(
                zpool_id,
                dataset_id,
                support_bundle_id,
                range,
                SupportBundleQueryType::Whole,
            )
            .await?)
    }

    async fn support_bundle_head_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequestHeaders>,
        path_params: Path<SupportBundleFilePathParam>,
    ) -> Result<http::Response<Body>, HttpError> {
        let sa = rqctx.context();
        let SupportBundleFilePathParam {
            parent:
                SupportBundlePathParam { zpool_id, dataset_id, support_bundle_id },
            file,
        } = path_params.into_inner();

        let range = headers
            .into_inner()
            .range
            .map(|r| PotentialRange::new(r.as_bytes()));
        Ok(sa
            .as_support_bundle_storage()
            .head(
                zpool_id,
                dataset_id,
                support_bundle_id,
                range,
                SupportBundleQueryType::Path { file_path: file },
            )
            .await?)
    }

    async fn support_bundle_head_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequestHeaders>,
        path_params: Path<SupportBundlePathParam>,
    ) -> Result<http::Response<Body>, HttpError> {
        let sa = rqctx.context();
        let SupportBundlePathParam { zpool_id, dataset_id, support_bundle_id } =
            path_params.into_inner();

        let range = headers
            .into_inner()
            .range
            .map(|r| PotentialRange::new(r.as_bytes()));
        Ok(sa
            .as_support_bundle_storage()
            .head(
                zpool_id,
                dataset_id,
                support_bundle_id,
                range,
                SupportBundleQueryType::Index,
            )
            .await?)
    }

    async fn support_bundle_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SupportBundlePathParam>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let sa = rqctx.context();

        let SupportBundlePathParam { zpool_id, dataset_id, support_bundle_id } =
            path_params.into_inner();

        sa.as_support_bundle_storage()
            .delete(zpool_id, dataset_id, support_bundle_id)
            .await?;
        Ok(HttpResponseDeleted())
    }

    async fn zone_bundle_cleanup(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BTreeMap<Utf8PathBuf, CleanupCount>>, HttpError>
    {
        let sa = rqctx.context();
        sa.zone_bundle_cleanup()
            .await
            .map(HttpResponseOk)
            .map_err(HttpError::from)
    }

    async fn zones_list(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<String>>, HttpError> {
        let sa = rqctx.context();
        sa.zones_list().await.map(HttpResponseOk).map_err(HttpError::from)
    }

    async fn omicron_config_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<OmicronSledConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let body_args = body.into_inner();
        sa.set_omicron_config(body_args).await??;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn sled_role_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledRole>, HttpError> {
        let sa = rqctx.context();
        Ok(HttpResponseOk(sa.get_role()))
    }

    async fn vmm_register(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        body: TypedBody<InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
        let sa = rqctx.context();
        let propolis_id = path_params.into_inner().propolis_id;
        let body_args = body.into_inner();
        Ok(HttpResponseOk(
            sa.instance_ensure_registered(propolis_id, body_args).await?,
        ))
    }

    async fn vmm_unregister(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
    ) -> Result<HttpResponseOk<VmmUnregisterResponse>, HttpError> {
        let sa = rqctx.context();
        let id = path_params.into_inner().propolis_id;
        Ok(HttpResponseOk(sa.instance_ensure_unregistered(id).await?))
    }

    async fn vmm_put_state(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        body: TypedBody<VmmPutStateBody>,
    ) -> Result<HttpResponseOk<VmmPutStateResponse>, HttpError> {
        let sa = rqctx.context();
        let id = path_params.into_inner().propolis_id;
        let body_args = body.into_inner();
        Ok(HttpResponseOk(sa.instance_ensure_state(id, body_args.state).await?))
    }

    async fn vmm_get_state(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
        let sa = rqctx.context();
        let id = path_params.into_inner().propolis_id;
        Ok(HttpResponseOk(sa.instance_get_state(id).await?))
    }

    async fn vmm_put_external_ip(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        body: TypedBody<InstanceExternalIpBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let id = path_params.into_inner().propolis_id;
        let body_args = body.into_inner();
        sa.instance_put_external_ip(id, &body_args).await?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn vmm_delete_external_ip(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        body: TypedBody<InstanceExternalIpBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let id = path_params.into_inner().propolis_id;
        let body_args = body.into_inner();
        sa.instance_delete_external_ip(id, &body_args).await?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn disk_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<DiskPathParam>,
        body: TypedBody<DiskEnsureBody>,
    ) -> Result<HttpResponseOk<DiskRuntimeState>, HttpError> {
        let sa = rqctx.context();
        let disk_id = path_params.into_inner().disk_id;
        let body_args = body.into_inner();
        Ok(HttpResponseOk(
            sa.disk_ensure(
                disk_id,
                body_args.initial_runtime.clone(),
                body_args.target.clone(),
            )
            .await
            .map_err(|e| Error::from(e))?,
        ))
    }

    async fn artifact_list(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ArtifactListResponse>, HttpError> {
        Ok(HttpResponseOk(rqctx.context().artifact_store().list().await?))
    }

    async fn artifact_config_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ArtifactConfig>, HttpError> {
        match rqctx.context().artifact_store().get_config() {
            Some(config) => Ok(HttpResponseOk(config)),
            None => Err(HttpError::for_not_found(
                None,
                "No artifact configuration present".to_string(),
            )),
        }
    }

    async fn artifact_config_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<ArtifactConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        rqctx.context().artifact_store().put_config(body.into_inner()).await?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn artifact_copy_from_depot(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<ArtifactPathParam>,
        query_params: Query<ArtifactQueryParam>,
        body: TypedBody<ArtifactCopyFromDepotBody>,
    ) -> Result<HttpResponseAccepted<ArtifactCopyFromDepotResponse>, HttpError>
    {
        let sha256 = path_params.into_inner().sha256;
        let generation = query_params.into_inner().generation;
        let depot_base_url = body.into_inner().depot_base_url;
        rqctx
            .context()
            .artifact_store()
            .copy_from_depot(sha256, generation, &depot_base_url)
            .await?;
        Ok(HttpResponseAccepted(ArtifactCopyFromDepotResponse {}))
    }

    async fn artifact_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<ArtifactPathParam>,
        query_params: Query<ArtifactQueryParam>,
        body: StreamingBody,
    ) -> Result<HttpResponseOk<ArtifactPutResponse>, HttpError> {
        let sha256 = path_params.into_inner().sha256;
        let generation = query_params.into_inner().generation;
        Ok(HttpResponseOk(
            rqctx
                .context()
                .artifact_store()
                .put_body(sha256, generation, body)
                .await?,
        ))
    }

    async fn vmm_issue_disk_snapshot_request(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmIssueDiskSnapshotRequestPathParam>,
        body: TypedBody<VmmIssueDiskSnapshotRequestBody>,
    ) -> Result<HttpResponseOk<VmmIssueDiskSnapshotRequestResponse>, HttpError>
    {
        let sa = rqctx.context();
        let path_params = path_params.into_inner();
        let body = body.into_inner();

        sa.vmm_issue_disk_snapshot_request(
            path_params.propolis_id,
            path_params.disk_id,
            body.snapshot_id,
        )
        .await?;

        Ok(HttpResponseOk(VmmIssueDiskSnapshotRequestResponse {
            snapshot_id: body.snapshot_id,
        }))
    }

    async fn vpc_firewall_rules_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VpcPathParam>,
        body: TypedBody<VpcFirewallRulesEnsureBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let _vpc_id = path_params.into_inner().vpc_id;
        let body_args = body.into_inner();

        sa.firewall_rules_ensure(body_args.vni, &body_args.rules[..])
            .await
            .map_err(Error::from)?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn set_v2p(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<VirtualNetworkInterfaceHost>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let body_args = body.into_inner();

        sa.set_virtual_nic_host(&body_args).await.map_err(Error::from)?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn del_v2p(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<VirtualNetworkInterfaceHost>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let body_args = body.into_inner();

        sa.unset_virtual_nic_host(&body_args).await.map_err(Error::from)?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn list_v2p(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<VirtualNetworkInterfaceHost>>, HttpError>
    {
        let sa = rqctx.context();

        let vnics = sa.list_virtual_nics().await.map_err(Error::from)?;

        Ok(HttpResponseOk(vnics))
    }

    async fn uplink_ensure(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<SwitchPorts>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        sa.ensure_scrimlet_host_ports(body.into_inner().uplinks).await?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn read_network_bootstore_config_cache(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<EarlyNetworkConfig>, HttpError> {
        let sa = rqctx.context();
        let bs = sa.bootstore();

        let config = bs.get_network_config().await.map_err(|e| {
            HttpError::for_internal_error(format!(
                "failed to get bootstore: {e}"
            ))
        })?;

        let config = match config {
            Some(config) => EarlyNetworkConfig::deserialize_bootstore_config(
                &rqctx.log, &config,
            )
            .map_err(|e| {
                HttpError::for_internal_error(format!(
                    "deserialize early network config: {e}"
                ))
            })?,
            None => {
                return Err(HttpError::for_unavail(
                    None,
                    "early network config does not exist yet".into(),
                ));
            }
        };

        Ok(HttpResponseOk(config))
    }

    async fn write_network_bootstore_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<EarlyNetworkConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let bs = sa.bootstore();
        let config = body.into_inner();

        bs.update_network_config(NetworkConfig::from(config)).await.map_err(
            |e| {
                HttpError::for_internal_error(format!(
                    "failed to write updated config to boot store: {e}"
                ))
            },
        )?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn sled_add(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<AddSledRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let request = body.into_inner();

        // Perform some minimal validation
        if request.start_request.body.use_trust_quorum
            && !request.start_request.body.is_lrtq_learner
        {
            return Err(HttpError::for_bad_request(
                None,
                "New sleds must be LRTQ learners if trust quorum is in use"
                    .to_string(),
            ));
        }

        crate::sled_agent::sled_add(
            sa.logger().clone(),
            sa.sprockets().clone(),
            request.sled_id,
            request.start_request,
        )
        .await
        .map_err(|e| {
            let message = format!("Failed to add sled to rack cluster: {e}");
            HttpError {
                status_code: ErrorStatusCode::INTERNAL_SERVER_ERROR,
                error_code: None,
                external_message: message.clone(),
                internal_message: message,
                headers: None,
            }
        })?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn inventory(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Inventory>, HttpError> {
        let sa = request_context.context();
        Ok(HttpResponseOk(sa.inventory().await?))
    }

    async fn sled_identifiers(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledIdentifiers>, HttpError> {
        Ok(HttpResponseOk(request_context.context().sled_identifiers()))
    }

    async fn bootstore_status(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BootstoreStatus>, HttpError> {
        let sa = request_context.context();
        let bootstore = sa.bootstore();
        let status = bootstore
            .get_status()
            .await
            .map_err(|e| {
                HttpError::from(omicron_common::api::external::Error::from(e))
            })?
            .into();
        Ok(HttpResponseOk(status))
    }

    async fn list_vpc_routes(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<ResolvedVpcRouteState>>, HttpError> {
        let sa = request_context.context();
        Ok(HttpResponseOk(sa.list_vpc_routes()))
    }

    async fn set_vpc_routes(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<Vec<ResolvedVpcRouteSet>>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = request_context.context();
        sa.set_vpc_routes(body.into_inner())?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn set_eip_gateways(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<ExternalIpGatewayMap>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = request_context.context();
        sa.set_eip_gateways(body.into_inner()).await?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn support_zoneadm_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError> {
        let sa = request_context.context();
        let res = sa.support_zoneadm_info().await;
        Ok(HttpResponseOk(res.get_output()))
    }

    async fn support_ipadm_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
    {
        let sa = request_context.context();
        Ok(HttpResponseOk(
            sa.support_ipadm_info()
                .await
                .into_iter()
                .map(|cmd| cmd.get_output())
                .collect::<Vec<_>>(),
        ))
    }

    async fn support_dladm_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
    {
        let sa = request_context.context();
        Ok(HttpResponseOk(
            sa.support_dladm_info()
                .await
                .into_iter()
                .map(|cmd| cmd.get_output())
                .collect::<Vec<_>>(),
        ))
    }

    async fn support_nvmeadm_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError> {
        let sa = request_context.context();
        let res = sa.support_nvmeadm_info().await;
        Ok(HttpResponseOk(res.get_output()))
    }

    async fn support_pargs_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
    {
        let sa = request_context.context();
        Ok(HttpResponseOk(
            sa.support_pargs_info()
                .await
                .into_iter()
                .map(|cmd| cmd.get_output())
                .collect::<Vec<_>>(),
        ))
    }

    async fn support_pstack_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
    {
        let sa = request_context.context();
        Ok(HttpResponseOk(
            sa.support_pstack_info()
                .await
                .into_iter()
                .map(|cmd| cmd.get_output())
                .collect::<Vec<_>>(),
        ))
    }

    async fn support_pfiles_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
    {
        let sa = request_context.context();
        Ok(HttpResponseOk(
            sa.support_pfiles_info()
                .await
                .into_iter()
                .map(|cmd| cmd.get_output())
                .collect::<Vec<_>>(),
        ))
    }

    async fn support_zfs_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError> {
        let sa = request_context.context();
        let res = sa.support_zfs_info().await;
        Ok(HttpResponseOk(res.get_output()))
    }

    async fn support_zpool_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError> {
        let sa = request_context.context();
        let res = sa.support_zpool_info().await;
        Ok(HttpResponseOk(res.get_output()))
    }

    async fn support_health_check(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
    {
        let sa = request_context.context();
        Ok(HttpResponseOk(
            sa.support_health_check()
                .await
                .into_iter()
                .map(|cmd| cmd.get_output())
                .collect::<Vec<_>>(),
        ))
    }

    async fn support_logs(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<String>>, HttpError> {
        let sa = request_context.context();
        sa.as_support_bundle_logs()
            .zones_list()
            .await
            .map(HttpResponseOk)
            .map_err(HttpError::from)
    }

    async fn support_logs_download(
        request_context: RequestContext<Self::Context>,
        path_params: Path<SledDiagnosticsLogsDownloadPathParm>,
        query_params: Query<SledDiagnosticsLogsDownloadQueryParam>,
    ) -> Result<http::Response<dropshot::Body>, HttpError> {
        let sa = request_context.context();
        let SledDiagnosticsLogsDownloadPathParm { zone } =
            path_params.into_inner();
        let SledDiagnosticsLogsDownloadQueryParam { max_rotated } =
            query_params.into_inner();

        sa.as_support_bundle_logs()
            .get_logs_for_zone(zone, max_rotated)
            .await
            .map_err(HttpError::from)
    }

    async fn chicken_switch_destroy_orphaned_datasets_get(
        _request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ChickenSwitchDestroyOrphanedDatasets>, HttpError>
    {
        // This API has been removed, but we still provide an endpoint for
        // backwards compatibility. Only `omdb` ever called this endpoint, so we
        // could probably just always returning an error, but we can at least
        // attempt to do something reasonable. We've removed this chicken switch
        // and always attempt to destroy orphans, so we can just claim the chick
        // switch is always in that state.
        let destroy_orphans = true;
        Ok(HttpResponseOk(ChickenSwitchDestroyOrphanedDatasets {
            destroy_orphans,
        }))
    }

    async fn chicken_switch_destroy_orphaned_datasets_put(
        _request_context: RequestContext<Self::Context>,
        body: TypedBody<ChickenSwitchDestroyOrphanedDatasets>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ChickenSwitchDestroyOrphanedDatasets { destroy_orphans } =
            body.into_inner();

        // This API has been removed, but we still provide an endpoint for
        // backwards compatibility. Only `omdb` ever called this endpoint, so we
        // could probably just always returning an error, but we can at least
        // attempt to do something reasonable. We've removed this chicken switch
        // and always attempt to destroy orphans, so we can treat requests to
        // destroy orphans as successful and attempts to disable it as an error.
        if destroy_orphans {
            Ok(HttpResponseUpdatedNoContent())
        } else {
            Err(HttpError::for_bad_request(
                None,
                "orphaned dataset destruction can no longer be disabled"
                    .to_string(),
            ))
        }
    }
}
