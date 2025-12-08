// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the sled agent's exposed API

use super::collection::PokeMode;
use crate::support_bundle::storage::SupportBundleQueryType;
use camino::Utf8PathBuf;
use dropshot::ApiDescription;
use dropshot::ErrorStatusCode;
use dropshot::FreeformBody;
use dropshot::Header;
use dropshot::HttpError;
use dropshot::HttpResponseAccepted;
use dropshot::HttpResponseCreated;
use dropshot::HttpResponseDeleted;
use dropshot::HttpResponseHeaders;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::StreamingBody;
use dropshot::TypedBody;
use dropshot::endpoint;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::SledVmmState;
use omicron_common::api::internal::shared::ExternalIpGatewayMap;
use omicron_common::api::internal::shared::SledIdentifiers;
use omicron_common::api::internal::shared::VirtualNetworkInterfaceHost;
use omicron_common::api::internal::shared::{
    ResolvedVpcRouteSet, ResolvedVpcRouteState, SwitchPorts,
};
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::ZpoolUuid;
use range_requests::PotentialRange;
use sled_agent_api::*;
// Use fixed identifiers from migrations crate to match the API trait
use sled_agent_types_migrations::{v1, v3, v7, v9, v10};
use sled_diagnostics::SledDiagnosticsQueryOutput;
use std::collections::BTreeMap;
use std::sync::Arc;

use super::sled_agent::SledAgent;

type SledApiDescription = ApiDescription<Arc<SledAgent>>;

/// Returns a description of the sled agent API
pub fn api() -> SledApiDescription {
    fn register_endpoints() -> Result<SledApiDescription, anyhow::Error> {
        let mut api = sled_agent_api::sled_agent_api_mod::api_description::<
            SledAgentSimImpl,
        >()?;
        api.register(instance_poke_post)?;
        api.register(instance_poke_single_step_post)?;
        api.register(instance_post_sim_migration_source)?;
        api.register(disk_poke_post)?;
        Ok(api)
    }

    register_endpoints().expect("failed to register entrypoints")
}

enum SledAgentSimImpl {}

impl SledAgentApi for SledAgentSimImpl {
    type Context = Arc<SledAgent>;

    async fn vmm_register(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
        body: TypedBody<sled_agent_types::instance::InstanceEnsureBody>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
        let sa = rqctx.context();
        let propolis_id = path_params.into_inner().propolis_id;
        let body_args = body.into_inner();
        Ok(HttpResponseOk(sa.instance_register(propolis_id, body_args).await?))
    }

    async fn vmm_unregister(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
    ) -> Result<HttpResponseOk<v1::instance::VmmUnregisterResponse>, HttpError>
    {
        let sa = rqctx.context();
        let id = path_params.into_inner().propolis_id;
        Ok(HttpResponseOk(sa.instance_unregister(id).await?))
    }

    async fn vmm_put_state(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
        body: TypedBody<v1::instance::VmmPutStateBody>,
    ) -> Result<HttpResponseOk<v1::instance::VmmPutStateResponse>, HttpError>
    {
        let sa = rqctx.context();
        let id = path_params.into_inner().propolis_id;
        let body_args = body.into_inner();
        Ok(HttpResponseOk(sa.instance_ensure_state(id, body_args.state).await?))
    }

    async fn vmm_get_state(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
    ) -> Result<HttpResponseOk<SledVmmState>, HttpError> {
        let sa = rqctx.context();
        let id = path_params.into_inner().propolis_id;
        Ok(HttpResponseOk(sa.instance_get_state(id).await?))
    }

    async fn vmm_put_external_ip(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
        body: TypedBody<v1::instance::InstanceExternalIpBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let id = path_params.into_inner().propolis_id;
        let body_args = body.into_inner();
        sa.instance_put_external_ip(id, &body_args).await?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn vmm_delete_external_ip(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
        body: TypedBody<v1::instance::InstanceExternalIpBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let id = path_params.into_inner().propolis_id;
        let body_args = body.into_inner();
        sa.instance_delete_external_ip(id, &body_args).await?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn vmm_join_multicast_group(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
        body: TypedBody<v7::instance::InstanceMulticastBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let propolis_id = path_params.into_inner().propolis_id;
        let body_args = body.into_inner();

        match body_args {
            v7::instance::InstanceMulticastBody::Join(membership) => {
                // v7::InstanceMulticastMembership is the canonical type
                sa.instance_join_multicast_group(propolis_id, &membership)
                    .await?;
            }
            v7::instance::InstanceMulticastBody::Leave(_) => {
                // This endpoint is for joining - reject leave operations
                return Err(HttpError::for_bad_request(
                    None,
                    "Join endpoint cannot process Leave operations".to_string(),
                ));
            }
        }

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn vmm_leave_multicast_group(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VmmPathParam>,
        body: TypedBody<v7::instance::InstanceMulticastBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let propolis_id = path_params.into_inner().propolis_id;
        let body_args = body.into_inner();

        match body_args {
            v7::instance::InstanceMulticastBody::Leave(membership) => {
                // v7::InstanceMulticastMembership is the canonical type
                sa.instance_leave_multicast_group(propolis_id, &membership)
                    .await?;
            }
            v7::instance::InstanceMulticastBody::Join(_) => {
                // This endpoint is for leaving - reject join operations
                return Err(HttpError::for_bad_request(
                    None,
                    "Leave endpoint cannot process Join operations".to_string(),
                ));
            }
        }

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn disk_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::DiskPathParam>,
        body: TypedBody<v1::disk::DiskEnsureBody>,
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
            .await?,
        ))
    }

    async fn artifact_config_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v1::artifact::ArtifactConfig>, HttpError> {
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
        body: TypedBody<v1::artifact::ArtifactConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        rqctx.context().artifact_store().put_config(body.into_inner()).await?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn artifact_list(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v1::views::ArtifactListResponse>, HttpError>
    {
        Ok(HttpResponseOk(rqctx.context().artifact_store().list().await?))
    }

    async fn artifact_copy_from_depot(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::ArtifactPathParam>,
        query_params: Query<v1::params::ArtifactQueryParam>,
        body: TypedBody<v1::params::ArtifactCopyFromDepotBody>,
    ) -> Result<
        HttpResponseAccepted<v1::views::ArtifactCopyFromDepotResponse>,
        HttpError,
    > {
        let sha256 = path_params.into_inner().sha256;
        let generation = query_params.into_inner().generation;
        let depot_base_url = body.into_inner().depot_base_url;
        rqctx
            .context()
            .artifact_store()
            .copy_from_depot(sha256, generation, &depot_base_url)
            .await?;
        Ok(HttpResponseAccepted(v1::views::ArtifactCopyFromDepotResponse {}))
    }

    async fn artifact_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::ArtifactPathParam>,
        query_params: Query<v1::params::ArtifactQueryParam>,
        body: StreamingBody,
    ) -> Result<HttpResponseOk<v1::views::ArtifactPutResponse>, HttpError> {
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
        path_params: Path<v1::params::VmmIssueDiskSnapshotRequestPathParam>,
        body: TypedBody<v1::params::VmmIssueDiskSnapshotRequestBody>,
    ) -> Result<
        HttpResponseOk<v1::views::VmmIssueDiskSnapshotRequestResponse>,
        HttpError,
    > {
        let sa = rqctx.context();
        let path_params = path_params.into_inner();
        let body = body.into_inner();

        sa.instance_issue_disk_snapshot_request(
            path_params.propolis_id,
            path_params.disk_id,
            body.snapshot_id,
        )
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

        Ok(HttpResponseOk(v1::views::VmmIssueDiskSnapshotRequestResponse {
            snapshot_id: body.snapshot_id,
        }))
    }

    async fn vpc_firewall_rules_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::VpcPathParam>,
        body: TypedBody<v10::instance::VpcFirewallRulesEnsureBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let _sa = rqctx.context();
        let _vpc_id = path_params.into_inner().vpc_id;
        let _body_args = body.into_inner();

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn set_v2p(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<VirtualNetworkInterfaceHost>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let body_args = body.into_inner();

        sa.set_virtual_nic_host(&body_args)
            .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn del_v2p(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<VirtualNetworkInterfaceHost>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let body_args = body.into_inner();

        sa.unset_virtual_nic_host(&body_args)
            .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn list_v2p(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<VirtualNetworkInterfaceHost>>, HttpError>
    {
        let sa = rqctx.context();

        let vnics = sa.list_virtual_nics().map_err(HttpError::from)?;

        Ok(HttpResponseOk(vnics))
    }

    async fn uplink_ensure(
        _rqctx: RequestContext<Self::Context>,
        _body: TypedBody<SwitchPorts>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn read_network_bootstore_config_cache(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<v1::early_networking::EarlyNetworkConfig>,
        HttpError,
    > {
        let config =
            rqctx.context().bootstore_network_config.lock().unwrap().clone();
        Ok(HttpResponseOk(config))
    }

    async fn write_network_bootstore_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v1::early_networking::EarlyNetworkConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let mut config =
            rqctx.context().bootstore_network_config.lock().unwrap();
        *config = body.into_inner();
        Ok(HttpResponseUpdatedNoContent())
    }

    /// Fetch basic information about this sled
    async fn inventory(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v10::inventory::Inventory>, HttpError> {
        let sa = rqctx.context();
        Ok(HttpResponseOk(
            sa.inventory(rqctx.server.local_addr).map_err(|e| {
                HttpError::for_internal_error(format!("{:#}", e))
            })?,
        ))
    }

    async fn omicron_config_put(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<v10::inventory::OmicronSledConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        let body_args = body.into_inner();
        sa.set_omicron_config(body_args)?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn sled_add(
        _rqctx: RequestContext<Self::Context>,
        _body: TypedBody<v1::sled::AddSledRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn list_vpc_routes(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<ResolvedVpcRouteState>>, HttpError> {
        let sa = rqctx.context();
        Ok(HttpResponseOk(sa.list_vpc_routes()))
    }

    async fn set_vpc_routes(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<Vec<ResolvedVpcRouteSet>>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();
        sa.set_vpc_routes(body.into_inner());
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn support_bundle_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::SupportBundleListPathParam>,
    ) -> Result<
        HttpResponseOk<Vec<v1::support_bundle::SupportBundleMetadata>>,
        HttpError,
    > {
        let sa = rqctx.context();

        let v1::params::SupportBundleListPathParam { zpool_id, dataset_id } =
            path_params.into_inner();

        let bundles = sa.support_bundle_list(zpool_id, dataset_id).await?;
        Ok(HttpResponseOk(bundles))
    }

    async fn support_bundle_start_creation(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::SupportBundlePathParam>,
    ) -> Result<
        HttpResponseCreated<v1::support_bundle::SupportBundleMetadata>,
        HttpError,
    > {
        let sa = rqctx.context();

        let v1::params::SupportBundlePathParam {
            zpool_id,
            dataset_id,
            support_bundle_id,
        } = path_params.into_inner();

        Ok(HttpResponseCreated(
            sa.support_bundle_start_creation(
                zpool_id,
                dataset_id,
                support_bundle_id,
            )
            .await?,
        ))
    }

    async fn support_bundle_transfer(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::SupportBundlePathParam>,
        query_params: Query<v1::params::SupportBundleTransferQueryParams>,
        body: StreamingBody,
    ) -> Result<
        HttpResponseCreated<v1::support_bundle::SupportBundleMetadata>,
        HttpError,
    > {
        let sa = rqctx.context();

        let v1::params::SupportBundlePathParam {
            zpool_id,
            dataset_id,
            support_bundle_id,
        } = path_params.into_inner();
        let v1::params::SupportBundleTransferQueryParams { offset } =
            query_params.into_inner();

        Ok(HttpResponseCreated(
            sa.support_bundle_transfer(
                zpool_id,
                dataset_id,
                support_bundle_id,
                offset,
                body.into_stream(),
            )
            .await?,
        ))
    }

    async fn support_bundle_finalize(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::SupportBundlePathParam>,
        query_params: Query<v1::params::SupportBundleFinalizeQueryParams>,
    ) -> Result<
        HttpResponseCreated<v1::support_bundle::SupportBundleMetadata>,
        HttpError,
    > {
        let sa = rqctx.context();

        let v1::params::SupportBundlePathParam {
            zpool_id,
            dataset_id,
            support_bundle_id,
        } = path_params.into_inner();
        let v1::params::SupportBundleFinalizeQueryParams { hash } =
            query_params.into_inner();

        Ok(HttpResponseCreated(
            sa.support_bundle_finalize(
                zpool_id,
                dataset_id,
                support_bundle_id,
                hash,
            )
            .await?,
        ))
    }

    async fn support_bundle_download(
        rqctx: RequestContext<Self::Context>,
        headers: Header<v1::params::RangeRequestHeaders>,
        path_params: Path<v1::params::SupportBundlePathParam>,
    ) -> Result<http::Response<dropshot::Body>, HttpError> {
        let sa = rqctx.context();
        let v1::params::SupportBundlePathParam {
            zpool_id,
            dataset_id,
            support_bundle_id,
        } = path_params.into_inner();

        let range = headers
            .into_inner()
            .range
            .map(|r| PotentialRange::new(r.as_bytes()));
        sa.support_bundle_get(
            zpool_id,
            dataset_id,
            support_bundle_id,
            range,
            SupportBundleQueryType::Whole,
        )
        .await
    }

    async fn support_bundle_download_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<v1::params::RangeRequestHeaders>,
        path_params: Path<v1::params::SupportBundleFilePathParam>,
    ) -> Result<http::Response<dropshot::Body>, HttpError> {
        let sa = rqctx.context();
        let v1::params::SupportBundleFilePathParam {
            parent:
                v1::params::SupportBundlePathParam {
                    zpool_id,
                    dataset_id,
                    support_bundle_id,
                },
            file,
        } = path_params.into_inner();

        let range = headers
            .into_inner()
            .range
            .map(|r| PotentialRange::new(r.as_bytes()));
        sa.support_bundle_get(
            zpool_id,
            dataset_id,
            support_bundle_id,
            range,
            SupportBundleQueryType::Path { file_path: file },
        )
        .await
    }

    async fn support_bundle_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<v1::params::RangeRequestHeaders>,
        path_params: Path<v1::params::SupportBundlePathParam>,
    ) -> Result<http::Response<dropshot::Body>, HttpError> {
        let sa = rqctx.context();
        let v1::params::SupportBundlePathParam {
            zpool_id,
            dataset_id,
            support_bundle_id,
        } = path_params.into_inner();

        let range = headers
            .into_inner()
            .range
            .map(|r| PotentialRange::new(r.as_bytes()));
        sa.support_bundle_get(
            zpool_id,
            dataset_id,
            support_bundle_id,
            range,
            SupportBundleQueryType::Index,
        )
        .await
    }

    async fn support_bundle_head(
        rqctx: RequestContext<Self::Context>,
        headers: Header<v1::params::RangeRequestHeaders>,
        path_params: Path<v1::params::SupportBundlePathParam>,
    ) -> Result<http::Response<dropshot::Body>, HttpError> {
        let sa = rqctx.context();
        let v1::params::SupportBundlePathParam {
            zpool_id,
            dataset_id,
            support_bundle_id,
        } = path_params.into_inner();

        let range = headers
            .into_inner()
            .range
            .map(|r| PotentialRange::new(r.as_bytes()));
        sa.support_bundle_head(
            zpool_id,
            dataset_id,
            support_bundle_id,
            range,
            SupportBundleQueryType::Whole,
        )
        .await
    }

    async fn support_bundle_head_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<v1::params::RangeRequestHeaders>,
        path_params: Path<v1::params::SupportBundleFilePathParam>,
    ) -> Result<http::Response<dropshot::Body>, HttpError> {
        let sa = rqctx.context();
        let v1::params::SupportBundleFilePathParam {
            parent:
                v1::params::SupportBundlePathParam {
                    zpool_id,
                    dataset_id,
                    support_bundle_id,
                },
            file,
        } = path_params.into_inner();

        let range = headers
            .into_inner()
            .range
            .map(|r| PotentialRange::new(r.as_bytes()));
        sa.support_bundle_get(
            zpool_id,
            dataset_id,
            support_bundle_id,
            range,
            SupportBundleQueryType::Path { file_path: file },
        )
        .await
    }

    async fn support_bundle_head_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<v1::params::RangeRequestHeaders>,
        path_params: Path<v1::params::SupportBundlePathParam>,
    ) -> Result<http::Response<dropshot::Body>, HttpError> {
        let sa = rqctx.context();
        let v1::params::SupportBundlePathParam {
            zpool_id,
            dataset_id,
            support_bundle_id,
        } = path_params.into_inner();

        let range = headers
            .into_inner()
            .range
            .map(|r| PotentialRange::new(r.as_bytes()));
        sa.support_bundle_head(
            zpool_id,
            dataset_id,
            support_bundle_id,
            range,
            SupportBundleQueryType::Index,
        )
        .await
    }

    async fn support_bundle_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v1::params::SupportBundlePathParam>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let sa = rqctx.context();

        let v1::params::SupportBundlePathParam {
            zpool_id,
            dataset_id,
            support_bundle_id,
        } = path_params.into_inner();

        sa.support_bundle_delete(zpool_id, dataset_id, support_bundle_id)
            .await?;

        Ok(HttpResponseDeleted())
    }

    async fn local_storage_dataset_ensure(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v9::params::LocalStoragePathParam>,
        body: TypedBody<v9::params::LocalStorageDatasetEnsureRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();

        let v9::params::LocalStoragePathParam { zpool_id, dataset_id } =
            path_params.into_inner();

        sa.ensure_local_storage_dataset(
            zpool_id,
            dataset_id,
            body.into_inner(),
        );

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn local_storage_dataset_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<v9::params::LocalStoragePathParam>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let sa = rqctx.context();

        let v9::params::LocalStoragePathParam { zpool_id, dataset_id } =
            path_params.into_inner();

        sa.drop_dataset(
            ZpoolUuid::from_untyped_uuid(zpool_id.into_untyped_uuid()),
            dataset_id,
        );

        Ok(HttpResponseUpdatedNoContent())
    }

    // --- Unimplemented endpoints ---

    async fn set_eip_gateways(
        rqctx: RequestContext<Self::Context>,
        _body: TypedBody<ExternalIpGatewayMap>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let _sa = rqctx.context();
        // sa.set_vpc_routes(body.into_inner()).await;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn zone_bundle_list_all(
        _rqctx: RequestContext<Self::Context>,
        _query: Query<v1::params::ZoneBundleFilter>,
    ) -> Result<
        HttpResponseOk<Vec<v1::zone_bundle::ZoneBundleMetadata>>,
        HttpError,
    > {
        method_unimplemented()
    }

    async fn zone_bundle_list(
        _rqctx: RequestContext<Self::Context>,
        _params: Path<v1::params::ZonePathParam>,
    ) -> Result<
        HttpResponseOk<Vec<v1::zone_bundle::ZoneBundleMetadata>>,
        HttpError,
    > {
        method_unimplemented()
    }

    async fn zone_bundle_get(
        _rqctx: RequestContext<Self::Context>,
        _params: Path<v1::zone_bundle::ZoneBundleId>,
    ) -> Result<HttpResponseHeaders<HttpResponseOk<FreeformBody>>, HttpError>
    {
        method_unimplemented()
    }

    async fn zone_bundle_delete(
        _rqctx: RequestContext<Self::Context>,
        _params: Path<v1::zone_bundle::ZoneBundleId>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        method_unimplemented()
    }

    async fn zone_bundle_utilization(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<
            BTreeMap<Utf8PathBuf, v1::zone_bundle::BundleUtilization>,
        >,
        HttpError,
    > {
        method_unimplemented()
    }

    async fn zone_bundle_cleanup_context(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v1::zone_bundle::CleanupContext>, HttpError>
    {
        method_unimplemented()
    }

    async fn zone_bundle_cleanup_context_update(
        _rqctx: RequestContext<Self::Context>,
        _body: TypedBody<v1::params::CleanupContextUpdate>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        method_unimplemented()
    }

    async fn zone_bundle_cleanup(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<BTreeMap<Utf8PathBuf, v1::zone_bundle::CleanupCount>>,
        HttpError,
    > {
        method_unimplemented()
    }

    async fn zones_list(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<String>>, HttpError> {
        method_unimplemented()
    }

    async fn sled_role_get(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v1::inventory::SledRole>, HttpError> {
        method_unimplemented()
    }

    async fn sled_identifiers(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledIdentifiers>, HttpError> {
        method_unimplemented()
    }

    async fn bootstore_status(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v1::bootstore::BootstoreStatus>, HttpError> {
        method_unimplemented()
    }

    async fn support_zoneadm_info(
        _request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError> {
        method_unimplemented()
    }

    async fn support_ipadm_info(
        _request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
    {
        method_unimplemented()
    }

    async fn support_dladm_info(
        _request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
    {
        method_unimplemented()
    }

    async fn support_nvmeadm_info(
        _request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError> {
        method_unimplemented()
    }

    async fn support_pargs_info(
        _request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
    {
        method_unimplemented()
    }

    async fn support_pstack_info(
        _request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
    {
        method_unimplemented()
    }

    async fn support_pfiles_info(
        _request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
    {
        method_unimplemented()
    }

    async fn support_zfs_info(
        _request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError> {
        method_unimplemented()
    }

    async fn support_zpool_info(
        _request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SledDiagnosticsQueryOutput>, HttpError> {
        method_unimplemented()
    }

    async fn support_health_check(
        _request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<SledDiagnosticsQueryOutput>>, HttpError>
    {
        method_unimplemented()
    }

    async fn support_logs(
        _request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<String>>, HttpError> {
        // Return an empty zone list for testing.
        Ok(HttpResponseOk(Default::default()))
    }

    async fn support_logs_download(
        _request_context: RequestContext<Self::Context>,
        _path_params: Path<v1::params::SledDiagnosticsLogsDownloadPathParam>,
        _query_params: Query<v1::params::SledDiagnosticsLogsDownloadQueryParam>,
    ) -> Result<http::Response<dropshot::Body>, HttpError> {
        method_unimplemented()
    }

    async fn chicken_switch_destroy_orphaned_datasets_get(
        _request_context: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<v1::shared::ChickenSwitchDestroyOrphanedDatasets>,
        HttpError,
    > {
        method_unimplemented()
    }

    async fn chicken_switch_destroy_orphaned_datasets_put(
        _request_context: RequestContext<Self::Context>,
        _body: TypedBody<v1::shared::ChickenSwitchDestroyOrphanedDatasets>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        method_unimplemented()
    }

    async fn debug_operator_switch_zone_policy_get(
        _request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v3::shared::OperatorSwitchZonePolicy>, HttpError>
    {
        method_unimplemented()
    }

    async fn debug_operator_switch_zone_policy_put(
        _request_context: RequestContext<Self::Context>,
        _body: TypedBody<v3::shared::OperatorSwitchZonePolicy>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        method_unimplemented()
    }

    async fn probes_put(
        _request_context: RequestContext<Self::Context>,
        _body: TypedBody<v10::probes::ProbeSet>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        Ok(HttpResponseUpdatedNoContent())
    }
}

fn method_unimplemented<T>() -> Result<T, HttpError> {
    Err(HttpError {
        // Use a client error here (405 Method Not Allowed vs 501 Not
        // Implemented) even though it isn't strictly accurate here, so tests
        // get to see the error message.
        status_code: ErrorStatusCode::METHOD_NOT_ALLOWED,
        error_code: None,
        external_message: "Method not implemented in sled-agent-sim"
            .to_string(),
        internal_message: "Method not implemented in sled-agent-sim"
            .to_string(),
        headers: None,
    })
}

// --- Extra endpoints only available in the sim implementation ---

#[endpoint {
    method = POST,
    path = "/vmms/{propolis_id}/poke",
}]
async fn instance_poke_post(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<v1::params::VmmPathParam>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let id = path_params.into_inner().propolis_id;
    sa.vmm_poke(id, PokeMode::Drain).await;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/vmms/{propolis_id}/poke-single-step",
}]
async fn instance_poke_single_step_post(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<v1::params::VmmPathParam>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let id = path_params.into_inner().propolis_id;
    sa.vmm_poke(id, PokeMode::SingleStep).await;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/vmms/{propolis_id}/sim-migration-source",
}]
async fn instance_post_sim_migration_source(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<v1::params::VmmPathParam>,
    body: TypedBody<super::instance::SimulateMigrationSource>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let id = path_params.into_inner().propolis_id;
    sa.instance_simulate_migration_source(id, body.into_inner()).await?;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/disks/{disk_id}/poke",
}]
async fn disk_poke_post(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<v1::params::DiskPathParam>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let disk_id = path_params.into_inner().disk_id;
    sa.disk_poke(disk_id).await;
    Ok(HttpResponseUpdatedNoContent())
}
