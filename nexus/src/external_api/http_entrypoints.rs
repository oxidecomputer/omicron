// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for external HTTP APIs

use super::{
    console_api, device_auth, params,
    views::{
        self, Certificate, FloatingIp, Group, IdentityProvider, Image, IpPool,
        IpPoolRange, PhysicalDisk, Project, Rack, Role, Silo, SiloQuotas,
        SiloUtilization, Sled, Snapshot, SshKey, User, UserBuiltin,
        Utilization, Vpc, VpcRouter, VpcSubnet,
    },
};
use crate::{context::ApiContext, external_api::shared};
use dropshot::Body;
use dropshot::EmptyScanParams;
use dropshot::HttpError;
use dropshot::HttpResponseDeleted;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::PaginationOrder;
use dropshot::PaginationParams;
use dropshot::Path;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::ResultsPage;
use dropshot::TypedBody;
use dropshot::WhichPage;
use dropshot::{ApiDescription, StreamingBody};
use dropshot::{HttpResponseAccepted, HttpResponseFound, HttpResponseSeeOther};
use dropshot::{HttpResponseCreated, HttpResponseHeaders};
use dropshot::{WebsocketChannelResult, WebsocketConnection};
use http::Response;
use ipnetwork::IpNetwork;
use nexus_db_queries::authz;
use nexus_db_queries::db;
use nexus_db_queries::db::identity::Resource;
use nexus_db_queries::db::lookup::ImageLookup;
use nexus_db_queries::db::lookup::ImageParentLookup;
use nexus_db_queries::db::model::Name;
use nexus_external_api::*;
use nexus_types::{
    authn::cookies::Cookies,
    external_api::{
        params::SystemMetricsPathParam,
        shared::{BfdStatus, ProbeInfo},
    },
};
use omicron_common::api::external::http_pagination::data_page_params_for;
use omicron_common::api::external::http_pagination::marker_for_name;
use omicron_common::api::external::http_pagination::marker_for_name_or_id;
use omicron_common::api::external::http_pagination::name_or_id_pagination;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::http_pagination::PaginatedById;
use omicron_common::api::external::http_pagination::PaginatedByName;
use omicron_common::api::external::http_pagination::PaginatedByNameOrId;
use omicron_common::api::external::http_pagination::ScanById;
use omicron_common::api::external::http_pagination::ScanByName;
use omicron_common::api::external::http_pagination::ScanByNameOrId;
use omicron_common::api::external::http_pagination::ScanParams;
use omicron_common::api::external::AddressLot;
use omicron_common::api::external::AddressLotBlock;
use omicron_common::api::external::AddressLotCreateResponse;
use omicron_common::api::external::AggregateBgpMessageHistory;
use omicron_common::api::external::BgpAnnounceSet;
use omicron_common::api::external::BgpAnnouncement;
use omicron_common::api::external::BgpConfig;
use omicron_common::api::external::BgpExported;
use omicron_common::api::external::BgpImportedRouteIpv4;
use omicron_common::api::external::BgpPeerStatus;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Disk;
use omicron_common::api::external::Error;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceNetworkInterface;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::LoopbackAddress;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::Probe;
use omicron_common::api::external::RouterRoute;
use omicron_common::api::external::RouterRouteKind;
use omicron_common::api::external::SwitchPort;
use omicron_common::api::external::SwitchPortSettings;
use omicron_common::api::external::SwitchPortSettingsView;
use omicron_common::api::external::TufRepoGetResponse;
use omicron_common::api::external::TufRepoInsertResponse;
use omicron_common::api::external::VpcFirewallRuleUpdateParams;
use omicron_common::api::external::VpcFirewallRules;
use omicron_common::bail_unless;
use omicron_uuid_kinds::GenericUuid;
use propolis_client::support::tungstenite::protocol::frame::coding::CloseCode;
use propolis_client::support::tungstenite::protocol::{
    CloseFrame, Role as WebSocketRole,
};
use propolis_client::support::WebSocketStream;
use ref_cast::RefCast;

type NexusApiDescription = ApiDescription<ApiContext>;

/// Returns a description of the external nexus API
pub(crate) fn external_api() -> NexusApiDescription {
    nexus_external_api_mod::api_description::<NexusExternalApiImpl>()
        .expect("registered entrypoints")
}

enum NexusExternalApiImpl {}

impl NexusExternalApi for NexusExternalApiImpl {
    type Context = ApiContext;

    async fn system_policy_view(
        rqctx: RequestContext<ApiContext>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::FleetRole>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let policy = nexus.fleet_fetch_policy(&opctx).await?;
            Ok(HttpResponseOk(policy))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn system_policy_update(
        rqctx: RequestContext<ApiContext>,
        new_policy: TypedBody<shared::Policy<shared::FleetRole>>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::FleetRole>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let new_policy = new_policy.into_inner();
            let nasgns = new_policy.role_assignments.len();
            // This should have been validated during parsing.
            bail_unless!(nasgns <= shared::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let policy = nexus.fleet_update_policy(&opctx, &new_policy).await?;
            Ok(HttpResponseOk(policy))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn policy_view(
        rqctx: RequestContext<ApiContext>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::SiloRole>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let silo: NameOrId = opctx
                .authn
                .silo_required()
                .internal_context("loading current silo")?
                .id()
                .into();

            let silo_lookup = nexus.silo_lookup(&opctx, silo)?;
            let policy = nexus.silo_fetch_policy(&opctx, &silo_lookup).await?;
            Ok(HttpResponseOk(policy))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn policy_update(
        rqctx: RequestContext<ApiContext>,
        new_policy: TypedBody<shared::Policy<shared::SiloRole>>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::SiloRole>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let new_policy = new_policy.into_inner();
            let nasgns = new_policy.role_assignments.len();
            // This should have been validated during parsing.
            bail_unless!(nasgns <= shared::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let silo: NameOrId = opctx
                .authn
                .silo_required()
                .internal_context("loading current silo")?
                .id()
                .into();
            let silo_lookup = nexus.silo_lookup(&opctx, silo)?;
            let policy = nexus
                .silo_update_policy(&opctx, &silo_lookup, &new_policy)
                .await?;
            Ok(HttpResponseOk(policy))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn utilization_view(
        rqctx: RequestContext<ApiContext>,
    ) -> Result<HttpResponseOk<Utilization>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let silo_lookup = nexus.current_silo_lookup(&opctx)?;
            let utilization =
                nexus.silo_utilization_view(&opctx, &silo_lookup).await?;

            Ok(HttpResponseOk(utilization.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn silo_utilization_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SiloPath>,
    ) -> Result<HttpResponseOk<SiloUtilization>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;

            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let silo_lookup =
                nexus.silo_lookup(&opctx, path_params.into_inner().silo)?;
            let quotas =
                nexus.silo_utilization_view(&opctx, &silo_lookup).await?;

            Ok(HttpResponseOk(quotas.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }
    async fn silo_utilization_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<SiloUtilization>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;

            let query = query_params.into_inner();
            let pagparams = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pagparams, scan_params)?;

            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let utilization = nexus
                .silo_utilization_list(&opctx, &paginated_by)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();

            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                utilization,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn system_quotas_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<SiloQuotas>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;

            let query = query_params.into_inner();
            let pagparams = data_page_params_for(&rqctx, &query)?;

            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let quotas = nexus
                .fleet_list_quotas(&opctx, &pagparams)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();

            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                quotas,
                &|_, quota: &SiloQuotas| quota.silo_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn silo_quotas_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SiloPath>,
    ) -> Result<HttpResponseOk<SiloQuotas>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;

            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let silo_lookup =
                nexus.silo_lookup(&opctx, path_params.into_inner().silo)?;
            let quota = nexus.silo_quotas_view(&opctx, &silo_lookup).await?;
            Ok(HttpResponseOk(quota.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn silo_quotas_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SiloPath>,
        new_quota: TypedBody<params::SiloQuotasUpdate>,
    ) -> Result<HttpResponseOk<SiloQuotas>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;

            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let silo_lookup =
                nexus.silo_lookup(&opctx, path_params.into_inner().silo)?;
            let quota = nexus
                .silo_update_quota(
                    &opctx,
                    &silo_lookup,
                    &new_quota.into_inner(),
                )
                .await?;
            Ok(HttpResponseOk(quota.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn silo_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<Silo>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let silos = nexus
                .silos_list(&opctx, &paginated_by)
                .await?
                .into_iter()
                .map(|p| p.try_into())
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                silos,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn silo_create(
        rqctx: RequestContext<ApiContext>,
        new_silo_params: TypedBody<params::SiloCreate>,
    ) -> Result<HttpResponseCreated<Silo>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let silo =
                nexus.silo_create(&opctx, new_silo_params.into_inner()).await?;
            Ok(HttpResponseCreated(silo.try_into()?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn silo_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SiloPath>,
    ) -> Result<HttpResponseOk<Silo>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, path.silo)?;
            let (.., silo) = silo_lookup.fetch().await?;
            Ok(HttpResponseOk(silo.try_into()?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn silo_ip_pool_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SiloPath>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<views::SiloIpPool>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();

            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;

            let silo_lookup = nexus.silo_lookup(&opctx, path.silo)?;
            let pools = nexus
                .silo_ip_pool_list(&opctx, &silo_lookup, &paginated_by)
                .await?
                .iter()
                .map(|(pool, silo_link)| views::SiloIpPool {
                    identity: pool.identity(),
                    is_default: silo_link.is_default,
                })
                .collect();

            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                pools,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn silo_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SiloPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let params = path_params.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, params.silo)?;
            nexus.silo_delete(&opctx, &silo_lookup).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn silo_policy_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SiloPath>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::SiloRole>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, path.silo)?;
            let policy = nexus.silo_fetch_policy(&opctx, &silo_lookup).await?;
            Ok(HttpResponseOk(policy))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn silo_policy_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SiloPath>,
        new_policy: TypedBody<shared::Policy<shared::SiloRole>>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::SiloRole>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let new_policy = new_policy.into_inner();
            let nasgns = new_policy.role_assignments.len();
            // This should have been validated during parsing.
            bail_unless!(nasgns <= shared::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, path.silo)?;
            let policy = nexus
                .silo_update_policy(&opctx, &silo_lookup, &new_policy)
                .await?;
            Ok(HttpResponseOk(policy))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Silo-specific user endpoints

    async fn silo_user_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedById<params::SiloSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<User>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanById::from_query(&query)?;
            let silo_lookup =
                nexus.silo_lookup(&opctx, scan_params.selector.silo.clone())?;
            let users = nexus
                .silo_list_users(&opctx, &silo_lookup, &pag_params)
                .await?
                .into_iter()
                .map(|i| i.into())
                .collect();
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                users,
                &|_, user: &User| user.id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn silo_user_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::UserParam>,
        query_params: Query<params::SiloSelector>,
    ) -> Result<HttpResponseOk<User>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, query.silo)?;
            let user = nexus
                .silo_user_fetch(&opctx, &silo_lookup, path.user_id)
                .await?;
            Ok(HttpResponseOk(user.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Silo identity providers

    async fn silo_identity_provider_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<params::SiloSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<IdentityProvider>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let silo_lookup =
                nexus.silo_lookup(&opctx, scan_params.selector.silo.clone())?;
            let identity_providers = nexus
                .identity_provider_list(&opctx, &silo_lookup, &paginated_by)
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                identity_providers,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Silo SAML identity providers

    async fn saml_identity_provider_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::SiloSelector>,
        new_provider: TypedBody<params::SamlIdentityProviderCreate>,
    ) -> Result<HttpResponseCreated<views::SamlIdentityProvider>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, query.silo)?;
            let provider = nexus
                .saml_identity_provider_create(
                    &opctx,
                    &silo_lookup,
                    new_provider.into_inner(),
                )
                .await?;
            Ok(HttpResponseCreated(provider.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn saml_identity_provider_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::ProviderPath>,
        query_params: Query<params::SiloSelector>,
    ) -> Result<HttpResponseOk<views::SamlIdentityProvider>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let saml_identity_provider_selector =
                params::SamlIdentityProviderSelector {
                    silo: Some(query.silo),
                    saml_identity_provider: path.provider,
                };
            let (.., provider) = nexus
                .saml_identity_provider_lookup(
                    &opctx,
                    saml_identity_provider_selector,
                )?
                .fetch()
                .await?;
            Ok(HttpResponseOk(provider.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // TODO: no DELETE for identity providers?

    // "Local" Identity Provider

    async fn local_idp_user_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::SiloSelector>,
        new_user_params: TypedBody<params::UserCreate>,
    ) -> Result<HttpResponseCreated<User>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, query.silo)?;
            let user = nexus
                .local_idp_create_user(
                    &opctx,
                    &silo_lookup,
                    new_user_params.into_inner(),
                )
                .await?;
            Ok(HttpResponseCreated(user.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn local_idp_user_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::UserParam>,
        query_params: Query<params::SiloSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, query.silo)?;
            nexus
                .local_idp_delete_user(&opctx, &silo_lookup, path.user_id)
                .await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn local_idp_user_set_password(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::UserParam>,
        query_params: Query<params::SiloPath>,
        update: TypedBody<params::UserPassword>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, query.silo)?;
            nexus
                .local_idp_user_set_password(
                    &opctx,
                    &silo_lookup,
                    path.user_id,
                    update.into_inner(),
                )
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn project_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<Project>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let projects = nexus
                .project_list(&opctx, &paginated_by)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                projects,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn project_create(
        rqctx: RequestContext<ApiContext>,
        new_project: TypedBody<params::ProjectCreate>,
    ) -> Result<HttpResponseCreated<Project>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project =
                nexus.project_create(&opctx, &new_project.into_inner()).await?;
            Ok(HttpResponseCreated(project.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn project_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::ProjectPath>,
    ) -> Result<HttpResponseOk<Project>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project_selector =
                params::ProjectSelector { project: path.project };
            let (.., project) =
                nexus.project_lookup(&opctx, project_selector)?.fetch().await?;
            Ok(HttpResponseOk(project.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn project_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::ProjectPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project_selector =
                params::ProjectSelector { project: path.project };
            let project_lookup =
                nexus.project_lookup(&opctx, project_selector)?;
            nexus.project_delete(&opctx, &project_lookup).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // TODO-correctness: Is it valid for PUT to accept application/json that's a
    // subset of what the resource actually represents?  If not, is that a problem?
    // (HTTP may require that this be idempotent.)  If so, can we get around that
    // having this be a slightly different content-type (e.g.,
    // "application/json-patch")?  We should see what other APIs do.
    async fn project_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::ProjectPath>,
        updated_project: TypedBody<params::ProjectUpdate>,
    ) -> Result<HttpResponseOk<Project>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let updated_project = updated_project.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project_selector =
                params::ProjectSelector { project: path.project };
            let project_lookup =
                nexus.project_lookup(&opctx, project_selector)?;
            let project = nexus
                .project_update(&opctx, &project_lookup, &updated_project)
                .await?;
            Ok(HttpResponseOk(project.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn project_policy_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::ProjectPath>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::ProjectRole>>, HttpError>
    {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project_selector =
                params::ProjectSelector { project: path.project };
            let project_lookup =
                nexus.project_lookup(&opctx, project_selector)?;
            let policy =
                nexus.project_fetch_policy(&opctx, &project_lookup).await?;
            Ok(HttpResponseOk(policy))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn project_policy_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::ProjectPath>,
        new_policy: TypedBody<shared::Policy<shared::ProjectRole>>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::ProjectRole>>, HttpError>
    {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let new_policy = new_policy.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project_selector =
                params::ProjectSelector { project: path.project };
            let project_lookup =
                nexus.project_lookup(&opctx, project_selector)?;
            nexus
                .project_update_policy(&opctx, &project_lookup, &new_policy)
                .await?;
            Ok(HttpResponseOk(new_policy))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // IP Pools

    async fn project_ip_pool_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<views::SiloIpPool>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let pools = nexus
                .current_silo_ip_pool_list(&opctx, &paginated_by)
                .await?
                .into_iter()
                .map(|(pool, silo_link)| views::SiloIpPool {
                    identity: pool.identity(),
                    is_default: silo_link.is_default,
                })
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                pools,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn project_ip_pool_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::IpPoolPath>,
    ) -> Result<HttpResponseOk<views::SiloIpPool>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let pool_selector = path_params.into_inner().pool;
            let (.., pool, silo_link) =
                nexus.silo_ip_pool_fetch(&opctx, &pool_selector).await?;
            Ok(HttpResponseOk(views::SiloIpPool {
                identity: pool.identity(),
                is_default: silo_link.is_default,
            }))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<IpPool>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let pools = nexus
                .ip_pools_list(&opctx, &paginated_by)
                .await?
                .into_iter()
                .map(IpPool::from)
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                pools,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_create(
        rqctx: RequestContext<ApiContext>,
        pool_params: TypedBody<params::IpPoolCreate>,
    ) -> Result<HttpResponseCreated<views::IpPool>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let pool_params = pool_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let pool = nexus.ip_pool_create(&opctx, &pool_params).await?;
            Ok(HttpResponseCreated(IpPool::from(pool)))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::IpPoolPath>,
    ) -> Result<HttpResponseOk<views::IpPool>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let pool_selector = path_params.into_inner().pool;
            // We do not prevent the service pool from being fetched by name or ID
            // like we do for update, delete, associate.
            let (.., pool) =
                nexus.ip_pool_lookup(&opctx, &pool_selector)?.fetch().await?;
            Ok(HttpResponseOk(IpPool::from(pool)))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::IpPoolPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
            nexus.ip_pool_delete(&opctx, &pool_lookup).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::IpPoolPath>,
        updates: TypedBody<params::IpPoolUpdate>,
    ) -> Result<HttpResponseOk<views::IpPool>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let updates = updates.into_inner();
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
            let pool =
                nexus.ip_pool_update(&opctx, &pool_lookup, &updates).await?;
            Ok(HttpResponseOk(pool.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_utilization_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::IpPoolPath>,
    ) -> Result<HttpResponseOk<views::IpPoolUtilization>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let pool_selector = path_params.into_inner().pool;
            // We do not prevent the service pool from being fetched by name or ID
            // like we do for update, delete, associate.
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &pool_selector)?;
            let utilization =
                nexus.ip_pool_utilization_view(&opctx, &pool_lookup).await?;
            Ok(HttpResponseOk(utilization.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_silo_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::IpPoolPath>,
        // paginating by resource_id because they're unique per pool. most robust
        // option would be to paginate by a composite key representing the (pool,
        // resource_type, resource)
        query_params: Query<PaginatedById>,
        // TODO: this could just list views::Silo -- it's not like knowing silo_id
        // and nothing else is particularly useful -- except we also want to say
        // whether the pool is marked default on each silo. So one option would
        // be  to do the same as we did with SiloIpPool -- include is_default on
        // whatever the thing is. Still... all we'd have to do to make this usable
        // in both places would be to make it { ...IpPool, silo_id, silo_name,
        // is_default }
    ) -> Result<HttpResponseOk<ResultsPage<views::IpPoolSiloLink>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;

            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;

            let path = path_params.into_inner();
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;

            let assocs = nexus
                .ip_pool_silo_list(&opctx, &pool_lookup, &pag_params)
                .await?
                .into_iter()
                .map(|assoc| assoc.into())
                .collect();

            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                assocs,
                &|_, x: &views::IpPoolSiloLink| x.silo_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_silo_link(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::IpPoolPath>,
        resource_assoc: TypedBody<params::IpPoolLinkSilo>,
    ) -> Result<HttpResponseCreated<views::IpPoolSiloLink>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let resource_assoc = resource_assoc.into_inner();
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
            let assoc = nexus
                .ip_pool_link_silo(&opctx, &pool_lookup, &resource_assoc)
                .await?;
            Ok(HttpResponseCreated(assoc.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_silo_unlink(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::IpPoolSiloPath>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
            let silo_lookup = nexus.silo_lookup(&opctx, path.silo)?;
            nexus
                .ip_pool_unlink_silo(&opctx, &pool_lookup, &silo_lookup)
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_silo_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::IpPoolSiloPath>,
        update: TypedBody<params::IpPoolSiloUpdate>,
    ) -> Result<HttpResponseOk<views::IpPoolSiloLink>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let update = update.into_inner();
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
            let silo_lookup = nexus.silo_lookup(&opctx, path.silo)?;
            let assoc = nexus
                .ip_pool_silo_update(
                    &opctx,
                    &pool_lookup,
                    &silo_lookup,
                    &update,
                )
                .await?;
            Ok(HttpResponseOk(assoc.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_service_view(
        rqctx: RequestContext<ApiContext>,
    ) -> Result<HttpResponseOk<views::IpPool>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let pool = nexus.ip_pool_service_fetch(&opctx).await?;
            Ok(HttpResponseOk(IpPool::from(pool)))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_range_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::IpPoolPath>,
        query_params: Query<IpPoolRangePaginationParams>,
    ) -> Result<HttpResponseOk<ResultsPage<IpPoolRange>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let path = path_params.into_inner();
            let marker = match query.page {
                WhichPage::First(_) => None,
                WhichPage::Next(ref addr) => Some(addr),
            };
            let pag_params = DataPageParams {
                limit: rqctx.page_limit(&query)?,
                direction: PaginationOrder::Ascending,
                marker,
            };
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
            let ranges = nexus
                .ip_pool_list_ranges(&opctx, &pool_lookup, &pag_params)
                .await?
                .into_iter()
                .map(|range| range.into())
                .collect();
            Ok(HttpResponseOk(ResultsPage::new(
                ranges,
                &EmptyScanParams {},
                |range: &IpPoolRange, _| {
                    IpNetwork::from(range.range.first_address())
                },
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_range_add(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::IpPoolPath>,
        range_params: TypedBody<shared::IpRange>,
    ) -> Result<HttpResponseCreated<IpPoolRange>, HttpError> {
        let apictx = &rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let range = range_params.into_inner();
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
            let out =
                nexus.ip_pool_add_range(&opctx, &pool_lookup, &range).await?;
            Ok(HttpResponseCreated(out.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_range_remove(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::IpPoolPath>,
        range_params: TypedBody<shared::IpRange>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let range = range_params.into_inner();
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
            nexus.ip_pool_delete_range(&opctx, &pool_lookup, &range).await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_service_range_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<IpPoolRangePaginationParams>,
    ) -> Result<HttpResponseOk<ResultsPage<IpPoolRange>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let marker = match query.page {
                WhichPage::First(_) => None,
                WhichPage::Next(ref addr) => Some(addr),
            };
            let pag_params = DataPageParams {
                limit: rqctx.page_limit(&query)?,
                direction: PaginationOrder::Ascending,
                marker,
            };
            let ranges = nexus
                .ip_pool_service_list_ranges(&opctx, &pag_params)
                .await?
                .into_iter()
                .map(|range| range.into())
                .collect();
            Ok(HttpResponseOk(ResultsPage::new(
                ranges,
                &EmptyScanParams {},
                |range: &IpPoolRange, _| {
                    IpNetwork::from(range.range.first_address())
                },
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_service_range_add(
        rqctx: RequestContext<ApiContext>,
        range_params: TypedBody<shared::IpRange>,
    ) -> Result<HttpResponseCreated<IpPoolRange>, HttpError> {
        let apictx = &rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let range = range_params.into_inner();
            let out = nexus.ip_pool_service_add_range(&opctx, &range).await?;
            Ok(HttpResponseCreated(out.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn ip_pool_service_range_remove(
        rqctx: RequestContext<ApiContext>,
        range_params: TypedBody<shared::IpRange>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context();
        let nexus = &apictx.context.nexus;
        let range = range_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            nexus.ip_pool_service_delete_range(&opctx, &range).await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Floating IP Addresses

    async fn floating_ip_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<views::FloatingIp>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let project_lookup =
                nexus.project_lookup(&opctx, scan_params.selector.clone())?;
            let ips = nexus
                .floating_ips_list(&opctx, &project_lookup, &paginated_by)
                .await?;
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                ips,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn floating_ip_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::ProjectSelector>,
        floating_params: TypedBody<params::FloatingIpCreate>,
    ) -> Result<HttpResponseCreated<views::FloatingIp>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let floating_params = floating_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project_lookup =
                nexus.project_lookup(&opctx, query_params.into_inner())?;
            let ip = nexus
                .floating_ip_create(&opctx, &project_lookup, floating_params)
                .await?;
            Ok(HttpResponseCreated(ip))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn floating_ip_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::FloatingIpPath>,
        query_params: Query<params::OptionalProjectSelector>,
        updated_floating_ip: TypedBody<params::FloatingIpUpdate>,
    ) -> Result<HttpResponseOk<FloatingIp>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let updated_floating_ip_params = updated_floating_ip.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let floating_ip_selector = params::FloatingIpSelector {
                project: query.project,
                floating_ip: path.floating_ip,
            };
            let floating_ip_lookup =
                nexus.floating_ip_lookup(&opctx, floating_ip_selector)?;
            let floating_ip = nexus
                .floating_ip_update(
                    &opctx,
                    floating_ip_lookup,
                    updated_floating_ip_params,
                )
                .await?;
            Ok(HttpResponseOk(floating_ip))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn floating_ip_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::FloatingIpPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let floating_ip_selector = params::FloatingIpSelector {
                floating_ip: path.floating_ip,
                project: query.project,
            };
            let fip_lookup =
                nexus.floating_ip_lookup(&opctx, floating_ip_selector)?;

            nexus.floating_ip_delete(&opctx, fip_lookup).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn floating_ip_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::FloatingIpPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<views::FloatingIp>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let floating_ip_selector = params::FloatingIpSelector {
                floating_ip: path.floating_ip,
                project: query.project,
            };
            let (.., fip) = nexus
                .floating_ip_lookup(&opctx, floating_ip_selector)?
                .fetch()
                .await?;
            Ok(HttpResponseOk(fip.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn floating_ip_attach(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::FloatingIpPath>,
        query_params: Query<params::OptionalProjectSelector>,
        target: TypedBody<params::FloatingIpAttach>,
    ) -> Result<HttpResponseAccepted<views::FloatingIp>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let floating_ip_selector = params::FloatingIpSelector {
                floating_ip: path.floating_ip,
                project: query.project,
            };
            let ip = nexus
                .floating_ip_attach(
                    &opctx,
                    floating_ip_selector,
                    target.into_inner(),
                )
                .await?;
            Ok(HttpResponseAccepted(ip))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn floating_ip_detach(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::FloatingIpPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseAccepted<views::FloatingIp>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let floating_ip_selector = params::FloatingIpSelector {
                floating_ip: path.floating_ip,
                project: query.project,
            };
            let fip_lookup =
                nexus.floating_ip_lookup(&opctx, floating_ip_selector)?;
            let ip = nexus.floating_ip_detach(&opctx, fip_lookup).await?;
            Ok(HttpResponseAccepted(ip))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Disks

    async fn disk_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<Disk>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let project_lookup =
                nexus.project_lookup(&opctx, scan_params.selector.clone())?;
            let disks = nexus
                .disk_list(&opctx, &project_lookup, &paginated_by)
                .await?
                .into_iter()
                .map(|disk| disk.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                disks,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // TODO-correctness See note about instance create.  This should be async.
    async fn disk_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::ProjectSelector>,
        new_disk: TypedBody<params::DiskCreate>,
    ) -> Result<HttpResponseCreated<Disk>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let params = new_disk.into_inner();
            let project_lookup = nexus.project_lookup(&opctx, query)?;
            let disk = nexus
                .project_create_disk(&opctx, &project_lookup, &params)
                .await?;
            Ok(HttpResponseCreated(disk.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn disk_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::DiskPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<Disk>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let disk_selector = params::DiskSelector {
                disk: path.disk,
                project: query.project,
            };
            let (.., disk) =
                nexus.disk_lookup(&opctx, disk_selector)?.fetch().await?;
            Ok(HttpResponseOk(disk.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn disk_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::DiskPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let disk_selector = params::DiskSelector {
                disk: path.disk,
                project: query.project,
            };
            let disk_lookup = nexus.disk_lookup(&opctx, disk_selector)?;
            nexus.project_delete_disk(&opctx, &disk_lookup).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn disk_metrics_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::DiskMetricsPath>,
        query_params: Query<
            PaginationParams<params::ResourceMetrics, params::ResourceMetrics>,
        >,
        selector_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<ResultsPage<oximeter_db::Measurement>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();

            let selector = selector_params.into_inner();
            let limit = rqctx.page_limit(&query)?;
            let disk_selector = params::DiskSelector {
                disk: path.disk,
                project: selector.project,
            };
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let (.., authz_disk) = nexus
                .disk_lookup(&opctx, disk_selector)?
                .lookup_for(authz::Action::Read)
                .await?;

            let result = nexus
                .select_timeseries(
                    &format!("crucible_upstairs:{}", path.metric),
                    &[&format!("upstairs_uuid=={}", authz_disk.id())],
                    query,
                    limit,
                )
                .await?;

            Ok(HttpResponseOk(result))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn disk_bulk_write_import_start(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::DiskPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();

            let disk_selector = params::DiskSelector {
                disk: path.disk,
                project: query.project,
            };
            let disk_lookup = nexus.disk_lookup(&opctx, disk_selector)?;

            nexus.disk_manual_import_start(&opctx, &disk_lookup).await?;

            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn disk_bulk_write_import(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::DiskPath>,
        query_params: Query<params::OptionalProjectSelector>,
        import_params: TypedBody<params::ImportBlocksBulkWrite>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let params = import_params.into_inner();

            let disk_selector = params::DiskSelector {
                disk: path.disk,
                project: query.project,
            };
            let disk_lookup = nexus.disk_lookup(&opctx, disk_selector)?;

            nexus.disk_manual_import(&disk_lookup, params).await?;

            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn disk_bulk_write_import_stop(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::DiskPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();

            let disk_selector = params::DiskSelector {
                disk: path.disk,
                project: query.project,
            };
            let disk_lookup = nexus.disk_lookup(&opctx, disk_selector)?;

            nexus.disk_manual_import_stop(&opctx, &disk_lookup).await?;

            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn disk_finalize_import(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::DiskPath>,
        query_params: Query<params::OptionalProjectSelector>,
        finalize_params: TypedBody<params::FinalizeDisk>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let params = finalize_params.into_inner();
            let disk_selector = params::DiskSelector {
                disk: path.disk,
                project: query.project,
            };
            let disk_lookup = nexus.disk_lookup(&opctx, disk_selector)?;

            nexus.disk_finalize_import(&opctx, &disk_lookup, &params).await?;

            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Instances

    async fn instance_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<Instance>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project_lookup =
                nexus.project_lookup(&opctx, scan_params.selector.clone())?;
            let instances = nexus
                .instance_list(&opctx, &project_lookup, &paginated_by)
                .await?
                .into_iter()
                .map(|i| i.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                instances,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::ProjectSelector>,
        new_instance: TypedBody<params::InstanceCreate>,
    ) -> Result<HttpResponseCreated<Instance>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let project_selector = query_params.into_inner();
        let new_instance_params = &new_instance.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project_lookup =
                nexus.project_lookup(&opctx, project_selector)?;
            let instance = nexus
                .project_create_instance(
                    &opctx,
                    &project_lookup,
                    &new_instance_params,
                )
                .await?;
            Ok(HttpResponseCreated(instance.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_view(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseOk<Instance>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let instance_selector = params::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let (.., authz_instance) =
                instance_lookup.lookup_for(authz::Action::Read).await?;
            let instance_and_vmm = nexus
                .datastore()
                .instance_fetch_with_vmm(&opctx, &authz_instance)
                .await?;
            Ok(HttpResponseOk(instance_and_vmm.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_delete(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let instance_selector = params::InstanceSelector {
            project: query.project,
            instance: path.instance,
        };
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            nexus.project_destroy_instance(&opctx, &instance_lookup).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_update(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::InstancePath>,
        reconfigure_params: TypedBody<params::InstanceUpdate>,
    ) -> Result<HttpResponseOk<Instance>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let reconfigure_params = reconfigure_params.into_inner();
        let instance_selector = params::InstanceSelector {
            project: query.project,
            instance: path.instance,
        };
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let instance = nexus
                .instance_reconfigure(
                    &opctx,
                    &instance_lookup,
                    &reconfigure_params,
                )
                .await?;
            Ok(HttpResponseOk(instance.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_reboot(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseAccepted<Instance>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let instance_selector = params::InstanceSelector {
            project: query.project,
            instance: path.instance,
        };
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let instance =
                nexus.instance_reboot(&opctx, &instance_lookup).await?;
            Ok(HttpResponseAccepted(instance.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_start(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseAccepted<Instance>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let instance_selector = params::InstanceSelector {
            project: query.project,
            instance: path.instance,
        };
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let instance = nexus
                .instance_start(
                    &opctx,
                    &instance_lookup,
                    crate::app::sagas::instance_start::Reason::User,
                )
                .await?;
            Ok(HttpResponseAccepted(instance.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_stop(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseAccepted<Instance>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let instance_selector = params::InstanceSelector {
            project: query.project,
            instance: path.instance,
        };
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let instance =
                nexus.instance_stop(&opctx, &instance_lookup).await?;
            Ok(HttpResponseAccepted(instance.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_serial_console(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::InstancePath>,
        query_params: Query<params::InstanceSerialConsoleRequest>,
    ) -> Result<HttpResponseOk<params::InstanceSerialConsoleData>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let instance_selector = params::InstanceSelector {
                project: query.project.clone(),
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let data = nexus
                .instance_serial_console_data(&opctx, &instance_lookup, &query)
                .await?;
            Ok(HttpResponseOk(data))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_serial_console_stream(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::InstancePath>,
        query_params: Query<params::InstanceSerialConsoleStreamRequest>,
        conn: WebsocketConnection,
    ) -> WebsocketChannelResult {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let instance_selector = params::InstanceSelector {
            project: query.project.clone(),
            instance: path.instance,
        };
        let mut client_stream = WebSocketStream::from_raw_socket(
            conn.into_inner(),
            WebSocketRole::Server,
            None,
        )
        .await;
        match nexus.instance_lookup(&opctx, instance_selector) {
            Ok(instance_lookup) => {
                nexus
                    .instance_serial_console_stream(
                        &opctx,
                        client_stream,
                        &instance_lookup,
                        &query,
                    )
                    .await?;
                Ok(())
            }
            Err(e) => {
                let _ = client_stream
                    .close(Some(CloseFrame {
                        code: CloseCode::Error,
                        reason: e.to_string().into(),
                    }))
                    .await
                    .is_ok();
                Err(e.into())
            }
        }
    }

    async fn instance_ssh_public_key_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::InstancePath>,
        query_params: Query<
            PaginatedByNameOrId<params::OptionalProjectSelector>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<SshKey>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let instance_selector = params::InstanceSelector {
                project: scan_params.selector.project.clone(),
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let ssh_keys = nexus
                .instance_ssh_keys_list(&opctx, &instance_lookup, &paginated_by)
                .await?
                .into_iter()
                .map(|k| k.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                ssh_keys,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_disk_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<
            PaginatedByNameOrId<params::OptionalProjectSelector>,
        >,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseOk<ResultsPage<Disk>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let instance_selector = params::InstanceSelector {
                project: scan_params.selector.project.clone(),
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let disks = nexus
                .instance_list_disks(&opctx, &instance_lookup, &paginated_by)
                .await?
                .into_iter()
                .map(|d| d.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                disks,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_disk_attach(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::InstancePath>,
        query_params: Query<params::OptionalProjectSelector>,
        disk_to_attach: TypedBody<params::DiskPath>,
    ) -> Result<HttpResponseAccepted<Disk>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let disk = disk_to_attach.into_inner().disk;
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let instance_selector = params::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let disk = nexus
                .instance_attach_disk(&opctx, &instance_lookup, disk)
                .await?;
            Ok(HttpResponseAccepted(disk.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_disk_detach(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::InstancePath>,
        query_params: Query<params::OptionalProjectSelector>,
        disk_to_detach: TypedBody<params::DiskPath>,
    ) -> Result<HttpResponseAccepted<Disk>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let disk = disk_to_detach.into_inner().disk;
            let instance_selector = params::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let disk = nexus
                .instance_detach_disk(&opctx, &instance_lookup, disk)
                .await?;
            Ok(HttpResponseAccepted(disk.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Certificates

    async fn certificate_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<Certificate>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let certs = nexus
                .certificates_list(&opctx, &paginated_by)
                .await?
                .into_iter()
                .map(|d| d.try_into())
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                certs,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn certificate_create(
        rqctx: RequestContext<ApiContext>,
        new_cert: TypedBody<params::CertificateCreate>,
    ) -> Result<HttpResponseCreated<Certificate>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let new_cert_params = new_cert.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let cert =
                nexus.certificate_create(&opctx, new_cert_params).await?;
            Ok(HttpResponseCreated(cert.try_into()?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn certificate_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::CertificatePath>,
    ) -> Result<HttpResponseOk<Certificate>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let (.., cert) = nexus
                .certificate_lookup(&opctx, &path.certificate)
                .fetch()
                .await?;
            Ok(HttpResponseOk(cert.try_into()?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn certificate_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::CertificatePath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            nexus
                .certificate_delete(
                    &opctx,
                    nexus.certificate_lookup(&opctx, &path.certificate),
                )
                .await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_address_lot_create(
        rqctx: RequestContext<ApiContext>,
        new_address_lot: TypedBody<params::AddressLotCreate>,
    ) -> Result<HttpResponseCreated<AddressLotCreateResponse>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let params = new_address_lot.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let result = nexus.address_lot_create(&opctx, params).await?;

            let lot: AddressLot = result.lot.into();
            let blocks: Vec<AddressLotBlock> =
                result.blocks.iter().map(|b| b.clone().into()).collect();

            Ok(HttpResponseCreated(AddressLotCreateResponse { lot, blocks }))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_address_lot_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::AddressLotPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let address_lot_lookup =
                nexus.address_lot_lookup(&opctx, path.address_lot)?;
            nexus.address_lot_delete(&opctx, &address_lot_lookup).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_address_lot_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<AddressLot>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let lots = nexus
                .address_lot_list(&opctx, &paginated_by)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();

            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                lots,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_address_lot_block_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::AddressLotPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<AddressLotBlock>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let path = path_params.into_inner();
            let pagparams = data_page_params_for(&rqctx, &query)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let address_lot_lookup =
                nexus.address_lot_lookup(&opctx, path.address_lot)?;
            let blocks = nexus
                .address_lot_block_list(&opctx, &address_lot_lookup, &pagparams)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();

            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                blocks,
                &|_, x: &AddressLotBlock| x.id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_loopback_address_create(
        rqctx: RequestContext<ApiContext>,
        new_loopback_address: TypedBody<params::LoopbackAddressCreate>,
    ) -> Result<HttpResponseCreated<LoopbackAddress>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let params = new_loopback_address.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let result = nexus.loopback_address_create(&opctx, params).await?;

            let addr: LoopbackAddress = result.into();

            Ok(HttpResponseCreated(addr))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_loopback_address_delete(
        rqctx: RequestContext<ApiContext>,
        path: Path<params::LoopbackAddressPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let addr = match IpNetwork::new(path.address, path.subnet_mask) {
                Ok(addr) => Ok(addr),
                Err(_) => Err(HttpError::for_bad_request(
                    None,
                    "invalid ip address".into(),
                )),
            }?;
            nexus
                .loopback_address_delete(
                    &opctx,
                    path.rack_id,
                    path.switch_location.into(),
                    addr.into(),
                )
                .await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_loopback_address_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<LoopbackAddress>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pagparams = data_page_params_for(&rqctx, &query)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let addrs = nexus
                .loopback_address_list(&opctx, &pagparams)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();

            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                addrs,
                &|_, x: &LoopbackAddress| x.id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_switch_port_settings_create(
        rqctx: RequestContext<ApiContext>,
        new_settings: TypedBody<params::SwitchPortSettingsCreate>,
    ) -> Result<HttpResponseCreated<SwitchPortSettingsView>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let params = new_settings.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let result =
                nexus.switch_port_settings_post(&opctx, params).await?;

            let settings: SwitchPortSettingsView = result.into();
            Ok(HttpResponseCreated(settings))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_switch_port_settings_delete(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::SwitchPortSettingsSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let selector = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            nexus.switch_port_settings_delete(&opctx, &selector).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_switch_port_settings_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<
            PaginatedByNameOrId<params::SwitchPortSettingsSelector>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<SwitchPortSettings>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let settings = nexus
                .switch_port_settings_list(&opctx, &paginated_by)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();

            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                settings,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_switch_port_settings_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SwitchPortSettingsInfoSelector>,
    ) -> Result<HttpResponseOk<SwitchPortSettingsView>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = path_params.into_inner().port;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let settings =
                nexus.switch_port_settings_get(&opctx, &query).await?;
            Ok(HttpResponseOk(settings.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_switch_port_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedById<params::SwitchPortPageSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<SwitchPort>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pagparams = data_page_params_for(&rqctx, &query)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let addrs = nexus
                .switch_port_list(&opctx, &pagparams)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();

            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                addrs,
                &|_, x: &SwitchPort| x.id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_switch_port_status(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SwitchPortPathSelector>,
        query_params: Query<params::SwitchPortSelector>,
    ) -> Result<HttpResponseOk<shared::SwitchLinkState>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            Ok(HttpResponseOk(
                nexus
                    .switch_port_status(
                        &opctx,
                        query.switch_location,
                        path.port,
                    )
                    .await?,
            ))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_switch_port_apply_settings(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SwitchPortPathSelector>,
        query_params: Query<params::SwitchPortSelector>,
        settings_body: TypedBody<params::SwitchPortApplySettings>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let port = path_params.into_inner().port;
            let query = query_params.into_inner();
            let settings = settings_body.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            nexus
                .switch_port_apply_settings(&opctx, &port, &query, &settings)
                .await?;
            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_switch_port_clear_settings(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SwitchPortPathSelector>,
        query_params: Query<params::SwitchPortSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let port = path_params.into_inner().port;
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            nexus.switch_port_clear_settings(&opctx, &port, &query).await?;
            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_bgp_config_create(
        rqctx: RequestContext<ApiContext>,
        config: TypedBody<params::BgpConfigCreate>,
    ) -> Result<HttpResponseCreated<BgpConfig>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let config = config.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let result = nexus.bgp_config_create(&opctx, &config).await?;
            Ok(HttpResponseCreated::<BgpConfig>(result.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_bgp_config_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<BgpConfig>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let configs = nexus
                .bgp_config_list(&opctx, &paginated_by)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();

            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                configs,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    //TODO pagination? the normal by-name/by-id stuff does not work here
    async fn networking_bgp_status(
        rqctx: RequestContext<ApiContext>,
    ) -> Result<HttpResponseOk<Vec<BgpPeerStatus>>, HttpError> {
        let apictx = rqctx.context();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let handler = async {
            let nexus = &apictx.context.nexus;
            let result = nexus.bgp_peer_status(&opctx).await?;
            Ok(HttpResponseOk(result))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    //TODO pagination? the normal by-name/by-id stuff does not work here
    async fn networking_bgp_exported(
        rqctx: RequestContext<ApiContext>,
    ) -> Result<HttpResponseOk<BgpExported>, HttpError> {
        let apictx = rqctx.context();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let handler = async {
            let nexus = &apictx.context.nexus;
            let result = nexus.bgp_exported(&opctx).await?;
            Ok(HttpResponseOk(result))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_bgp_message_history(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::BgpRouteSelector>,
    ) -> Result<HttpResponseOk<AggregateBgpMessageHistory>, HttpError> {
        let apictx = rqctx.context();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let handler = async {
            let nexus = &apictx.context.nexus;
            let sel = query_params.into_inner();
            let result = nexus.bgp_message_history(&opctx, &sel).await?;
            Ok(HttpResponseOk(AggregateBgpMessageHistory::new(result)))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    //TODO pagination? the normal by-name/by-id stuff does not work here
    async fn networking_bgp_imported_routes_ipv4(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::BgpRouteSelector>,
    ) -> Result<HttpResponseOk<Vec<BgpImportedRouteIpv4>>, HttpError> {
        let apictx = rqctx.context();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let handler = async {
            let nexus = &apictx.context.nexus;
            let sel = query_params.into_inner();
            let result = nexus.bgp_imported_routes_ipv4(&opctx, &sel).await?;
            Ok(HttpResponseOk(result))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_bgp_config_delete(
        rqctx: RequestContext<ApiContext>,
        sel: Query<params::BgpConfigSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let sel = sel.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            nexus.bgp_config_delete(&opctx, &sel).await?;
            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_bgp_announce_set_update(
        rqctx: RequestContext<ApiContext>,
        config: TypedBody<params::BgpAnnounceSetCreate>,
    ) -> Result<HttpResponseCreated<BgpAnnounceSet>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let config = config.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let result = nexus.bgp_update_announce_set(&opctx, &config).await?;
            Ok(HttpResponseCreated::<BgpAnnounceSet>(result.0.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_bgp_announce_set_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<Vec<BgpAnnounceSet>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let result = nexus
                .bgp_announce_set_list(&opctx, &paginated_by)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();
            Ok(HttpResponseOk(result))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_bgp_announce_set_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::BgpAnnounceSetSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let sel = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            nexus.bgp_delete_announce_set(&opctx, &sel).await?;
            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_bgp_announcement_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::BgpAnnounceSetSelector>,
    ) -> Result<HttpResponseOk<Vec<BgpAnnouncement>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let sel = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;

            let result = nexus
                .bgp_announcement_list(&opctx, &sel)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();

            Ok(HttpResponseOk(result))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_bfd_enable(
        rqctx: RequestContext<ApiContext>,
        session: TypedBody<params::BfdSessionEnable>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
            nexus.bfd_enable(&opctx, session.into_inner()).await?;
            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_bfd_disable(
        rqctx: RequestContext<ApiContext>,
        session: TypedBody<params::BfdSessionDisable>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
            nexus.bfd_disable(&opctx, session.into_inner()).await?;
            Ok(HttpResponseUpdatedNoContent {})
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_bfd_status(
        rqctx: RequestContext<ApiContext>,
    ) -> Result<HttpResponseOk<Vec<BfdStatus>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
            let status = nexus.bfd_status(&opctx).await?;
            Ok(HttpResponseOk(status))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_allow_list_view(
        rqctx: RequestContext<ApiContext>,
    ) -> Result<HttpResponseOk<views::AllowList>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            nexus
                .allow_list_view(&opctx)
                .await
                .map(HttpResponseOk)
                .map_err(HttpError::from)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_allow_list_update(
        rqctx: RequestContext<ApiContext>,
        params: TypedBody<params::AllowListUpdate>,
    ) -> Result<HttpResponseOk<views::AllowList>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let server_kind = apictx.kind;
            let params = params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let remote_addr = rqctx.request.remote_addr().ip();
            nexus
                .allow_list_upsert(&opctx, remote_addr, server_kind, params)
                .await
                .map(HttpResponseOk)
                .map_err(HttpError::from)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Images

    async fn image_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<
            PaginatedByNameOrId<params::OptionalProjectSelector>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<Image>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let parent_lookup = match scan_params.selector.project.clone() {
                Some(project) => {
                    let project_lookup = nexus.project_lookup(
                        &opctx,
                        params::ProjectSelector { project },
                    )?;
                    ImageParentLookup::Project(project_lookup)
                }
                None => {
                    let silo_lookup = nexus.current_silo_lookup(&opctx)?;
                    ImageParentLookup::Silo(silo_lookup)
                }
            };
            let images = nexus
                .image_list(&opctx, &parent_lookup, &paginated_by)
                .await?
                .into_iter()
                .map(|d| d.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                images,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn image_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::OptionalProjectSelector>,
        new_image: TypedBody<params::ImageCreate>,
    ) -> Result<HttpResponseCreated<Image>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let params = &new_image.into_inner();
            let parent_lookup = match query.project.clone() {
                Some(project) => {
                    let project_lookup = nexus.project_lookup(
                        &opctx,
                        params::ProjectSelector { project },
                    )?;
                    ImageParentLookup::Project(project_lookup)
                }
                None => {
                    let silo_lookup = nexus.current_silo_lookup(&opctx)?;
                    ImageParentLookup::Silo(silo_lookup)
                }
            };
            let image =
                nexus.image_create(&opctx, &parent_lookup, &params).await?;
            Ok(HttpResponseCreated(image.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn image_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::ImagePath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<Image>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let image: nexus_db_model::Image = match nexus
                .image_lookup(
                    &opctx,
                    params::ImageSelector {
                        image: path.image,
                        project: query.project,
                    },
                )
                .await?
            {
                ImageLookup::ProjectImage(image) => {
                    let (.., db_image) = image.fetch().await?;
                    db_image.into()
                }
                ImageLookup::SiloImage(image) => {
                    let (.., db_image) = image.fetch().await?;
                    db_image.into()
                }
            };
            Ok(HttpResponseOk(image.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn image_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::ImagePath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let image_lookup = nexus
                .image_lookup(
                    &opctx,
                    params::ImageSelector {
                        image: path.image,
                        project: query.project,
                    },
                )
                .await?;
            nexus.image_delete(&opctx, &image_lookup).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn image_promote(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::ImagePath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseAccepted<Image>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let image_lookup = nexus
                .image_lookup(
                    &opctx,
                    params::ImageSelector {
                        image: path.image,
                        project: query.project,
                    },
                )
                .await?;
            let image = nexus.image_promote(&opctx, &image_lookup).await?;
            Ok(HttpResponseAccepted(image.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn image_demote(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::ImagePath>,
        query_params: Query<params::ProjectSelector>,
    ) -> Result<HttpResponseAccepted<Image>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let image_lookup = nexus
                .image_lookup(
                    &opctx,
                    params::ImageSelector { image: path.image, project: None },
                )
                .await?;

            let project_lookup = nexus.project_lookup(&opctx, query)?;

            let image = nexus
                .image_demote(&opctx, &image_lookup, &project_lookup)
                .await?;
            Ok(HttpResponseAccepted(image.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_network_interface_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<params::InstanceSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<InstanceNetworkInterface>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let instance_lookup =
                nexus.instance_lookup(&opctx, scan_params.selector.clone())?;
            let interfaces = nexus
                .instance_network_interface_list(
                    &opctx,
                    &instance_lookup,
                    &paginated_by,
                )
                .await?
                .into_iter()
                .map(|d| d.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                interfaces,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_network_interface_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::InstanceSelector>,
        interface_params: TypedBody<params::InstanceNetworkInterfaceCreate>,
    ) -> Result<HttpResponseCreated<InstanceNetworkInterface>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let instance_lookup = nexus.instance_lookup(&opctx, query)?;
            let iface = nexus
                .network_interface_create(
                    &opctx,
                    &instance_lookup,
                    &interface_params.into_inner(),
                )
                .await?;
            Ok(HttpResponseCreated(iface.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_network_interface_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::NetworkInterfacePath>,
        query_params: Query<params::OptionalInstanceSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let interface_selector = params::InstanceNetworkInterfaceSelector {
                project: query.project,
                instance: query.instance,
                network_interface: path.interface,
            };
            let interface_lookup = nexus.instance_network_interface_lookup(
                &opctx,
                interface_selector,
            )?;
            nexus
                .instance_network_interface_delete(&opctx, &interface_lookup)
                .await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_network_interface_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::NetworkInterfacePath>,
        query_params: Query<params::OptionalInstanceSelector>,
    ) -> Result<HttpResponseOk<InstanceNetworkInterface>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let interface_selector = params::InstanceNetworkInterfaceSelector {
                project: query.project,
                instance: query.instance,
                network_interface: path.interface,
            };
            let (.., interface) = nexus
                .instance_network_interface_lookup(&opctx, interface_selector)?
                .fetch()
                .await?;
            Ok(HttpResponseOk(interface.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_network_interface_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::NetworkInterfacePath>,
        query_params: Query<params::OptionalInstanceSelector>,
        updated_iface: TypedBody<params::InstanceNetworkInterfaceUpdate>,
    ) -> Result<HttpResponseOk<InstanceNetworkInterface>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let updated_iface = updated_iface.into_inner();
            let network_interface_selector =
                params::InstanceNetworkInterfaceSelector {
                    project: query.project,
                    instance: query.instance,
                    network_interface: path.interface,
                };
            let network_interface_lookup = nexus
                .instance_network_interface_lookup(
                    &opctx,
                    network_interface_selector,
                )?;
            let interface = nexus
                .instance_network_interface_update(
                    &opctx,
                    &network_interface_lookup,
                    updated_iface,
                )
                .await?;
            Ok(HttpResponseOk(InstanceNetworkInterface::from(interface)))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // External IP addresses for instances

    async fn instance_external_ip_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseOk<ResultsPage<views::ExternalIp>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let instance_selector = params::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let ips = nexus
                .instance_list_external_ips(&opctx, &instance_lookup)
                .await?;
            Ok(HttpResponseOk(ResultsPage { items: ips, next_page: None }))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_ephemeral_ip_attach(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::InstancePath>,
        query_params: Query<params::OptionalProjectSelector>,
        ip_to_create: TypedBody<params::EphemeralIpCreate>,
    ) -> Result<HttpResponseAccepted<views::ExternalIp>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let instance_selector = params::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let ip = nexus
                .instance_attach_ephemeral_ip(
                    &opctx,
                    &instance_lookup,
                    ip_to_create.into_inner().pool,
                )
                .await?;
            Ok(HttpResponseAccepted(ip))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_ephemeral_ip_detach(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::InstancePath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let instance_selector = params::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            nexus
                .instance_detach_external_ip(
                    &opctx,
                    &instance_lookup,
                    &params::ExternalIpDetach::Ephemeral,
                )
                .await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Snapshots

    async fn snapshot_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<Snapshot>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let project_lookup =
                nexus.project_lookup(&opctx, scan_params.selector.clone())?;
            let snapshots = nexus
                .snapshot_list(&opctx, &project_lookup, &paginated_by)
                .await?
                .into_iter()
                .map(|d| d.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                snapshots,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn snapshot_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::ProjectSelector>,
        new_snapshot: TypedBody<params::SnapshotCreate>,
    ) -> Result<HttpResponseCreated<Snapshot>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let new_snapshot_params = &new_snapshot.into_inner();
            let project_lookup = nexus.project_lookup(&opctx, query)?;
            let snapshot = nexus
                .snapshot_create(&opctx, project_lookup, &new_snapshot_params)
                .await?;
            Ok(HttpResponseCreated(snapshot.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn snapshot_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SnapshotPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<Snapshot>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let snapshot_selector = params::SnapshotSelector {
                project: query.project,
                snapshot: path.snapshot,
            };
            let (.., snapshot) = nexus
                .snapshot_lookup(&opctx, snapshot_selector)?
                .fetch()
                .await?;
            Ok(HttpResponseOk(snapshot.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn snapshot_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SnapshotPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let snapshot_selector = params::SnapshotSelector {
                project: query.project,
                snapshot: path.snapshot,
            };
            let snapshot_lookup =
                nexus.snapshot_lookup(&opctx, snapshot_selector)?;
            nexus.snapshot_delete(&opctx, &snapshot_lookup).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // VPCs

    async fn vpc_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<Vpc>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project_lookup =
                nexus.project_lookup(&opctx, scan_params.selector.clone())?;
            let vpcs = nexus
                .vpc_list(&opctx, &project_lookup, &paginated_by)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();

            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                vpcs,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::ProjectSelector>,
        body: TypedBody<params::VpcCreate>,
    ) -> Result<HttpResponseCreated<Vpc>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let query = query_params.into_inner();
        let new_vpc_params = body.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project_lookup = nexus.project_lookup(&opctx, query)?;
            let vpc = nexus
                .project_create_vpc(&opctx, &project_lookup, &new_vpc_params)
                .await?;
            Ok(HttpResponseCreated(vpc.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::VpcPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<Vpc>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let vpc_selector =
                params::VpcSelector { project: query.project, vpc: path.vpc };
            let (.., vpc) =
                nexus.vpc_lookup(&opctx, vpc_selector)?.fetch().await?;
            Ok(HttpResponseOk(vpc.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::VpcPath>,
        query_params: Query<params::OptionalProjectSelector>,
        updated_vpc: TypedBody<params::VpcUpdate>,
    ) -> Result<HttpResponseOk<Vpc>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let updated_vpc_params = &updated_vpc.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let vpc_selector =
                params::VpcSelector { project: query.project, vpc: path.vpc };
            let vpc_lookup = nexus.vpc_lookup(&opctx, vpc_selector)?;
            let vpc = nexus
                .project_update_vpc(&opctx, &vpc_lookup, &updated_vpc_params)
                .await?;
            Ok(HttpResponseOk(vpc.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::VpcPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let vpc_selector =
                params::VpcSelector { project: query.project, vpc: path.vpc };
            let vpc_lookup = nexus.vpc_lookup(&opctx, vpc_selector)?;
            nexus.project_delete_vpc(&opctx, &vpc_lookup).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_subnet_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<params::VpcSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<VpcSubnet>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let vpc_lookup =
                nexus.vpc_lookup(&opctx, scan_params.selector.clone())?;
            let subnets = nexus
                .vpc_subnet_list(&opctx, &vpc_lookup, &paginated_by)
                .await?
                .into_iter()
                .map(|vpc| vpc.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                subnets,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_subnet_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::VpcSelector>,
        create_params: TypedBody<params::VpcSubnetCreate>,
    ) -> Result<HttpResponseCreated<VpcSubnet>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let create = create_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let vpc_lookup = nexus.vpc_lookup(&opctx, query)?;
            let subnet =
                nexus.vpc_create_subnet(&opctx, &vpc_lookup, &create).await?;
            Ok(HttpResponseCreated(subnet.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_subnet_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SubnetPath>,
        query_params: Query<params::OptionalVpcSelector>,
    ) -> Result<HttpResponseOk<VpcSubnet>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let subnet_selector = params::SubnetSelector {
                project: query.project,
                vpc: query.vpc,
                subnet: path.subnet,
            };
            let (.., subnet) = nexus
                .vpc_subnet_lookup(&opctx, subnet_selector)?
                .fetch()
                .await?;
            Ok(HttpResponseOk(subnet.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_subnet_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SubnetPath>,
        query_params: Query<params::OptionalVpcSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let subnet_selector = params::SubnetSelector {
                project: query.project,
                vpc: query.vpc,
                subnet: path.subnet,
            };
            let subnet_lookup =
                nexus.vpc_subnet_lookup(&opctx, subnet_selector)?;
            nexus.vpc_delete_subnet(&opctx, &subnet_lookup).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_subnet_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SubnetPath>,
        query_params: Query<params::OptionalVpcSelector>,
        subnet_params: TypedBody<params::VpcSubnetUpdate>,
    ) -> Result<HttpResponseOk<VpcSubnet>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let subnet_params = subnet_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let subnet_selector = params::SubnetSelector {
                project: query.project,
                vpc: query.vpc,
                subnet: path.subnet,
            };
            let subnet_lookup =
                nexus.vpc_subnet_lookup(&opctx, subnet_selector)?;
            let subnet = nexus
                .vpc_update_subnet(&opctx, &subnet_lookup, &subnet_params)
                .await?;
            Ok(HttpResponseOk(subnet.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // This endpoint is likely temporary. We would rather list all IPs allocated in
    // a subnet whether they come from NICs or something else. See
    // https://github.com/oxidecomputer/omicron/issues/2476

    async fn vpc_subnet_list_network_interfaces(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SubnetPath>,
        query_params: Query<PaginatedByNameOrId<params::OptionalVpcSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<InstanceNetworkInterface>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let path = path_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let subnet_selector = params::SubnetSelector {
                project: scan_params.selector.project.clone(),
                vpc: scan_params.selector.vpc.clone(),
                subnet: path.subnet,
            };
            let subnet_lookup =
                nexus.vpc_subnet_lookup(&opctx, subnet_selector)?;
            let interfaces = nexus
                .subnet_list_instance_network_interfaces(
                    &opctx,
                    &subnet_lookup,
                    &paginated_by,
                )
                .await?
                .into_iter()
                .map(|interfaces| interfaces.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                interfaces,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // VPC Firewalls

    async fn vpc_firewall_rules_view(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::VpcSelector>,
    ) -> Result<HttpResponseOk<VpcFirewallRules>, HttpError> {
        // TODO: Check If-Match and fail if the ETag doesn't match anymore.
        // Without this check, if firewall rules change while someone is listing
        // the rules, they will see a mix of the old and new rules.
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let vpc_lookup = nexus.vpc_lookup(&opctx, query)?;
            let rules =
                nexus.vpc_list_firewall_rules(&opctx, &vpc_lookup).await?;
            Ok(HttpResponseOk(VpcFirewallRules {
                rules: rules.into_iter().map(|rule| rule.into()).collect(),
            }))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Note: the limits in the below comment come from the firewall rules model
    // file, nexus/db-model/src/vpc_firewall_rule.rs.

    async fn vpc_firewall_rules_update(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::VpcSelector>,
        router_params: TypedBody<VpcFirewallRuleUpdateParams>,
    ) -> Result<HttpResponseOk<VpcFirewallRules>, HttpError> {
        // TODO: Check If-Match and fail if the ETag doesn't match anymore.
        // TODO: limit size of the ruleset because the GET endpoint is not paginated
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let router_params = router_params.into_inner();
            let vpc_lookup = nexus.vpc_lookup(&opctx, query)?;
            let rules = nexus
                .vpc_update_firewall_rules(&opctx, &vpc_lookup, &router_params)
                .await?;
            Ok(HttpResponseOk(VpcFirewallRules {
                rules: rules.into_iter().map(|rule| rule.into()).collect(),
            }))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // VPC Routers

    async fn vpc_router_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<params::VpcSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<VpcRouter>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let vpc_lookup =
                nexus.vpc_lookup(&opctx, scan_params.selector.clone())?;
            let routers = nexus
                .vpc_router_list(&opctx, &vpc_lookup, &paginated_by)
                .await?
                .into_iter()
                .map(|s| s.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                routers,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_router_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::RouterPath>,
        query_params: Query<params::OptionalVpcSelector>,
    ) -> Result<HttpResponseOk<VpcRouter>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let router_selector = params::RouterSelector {
                project: query.project,
                vpc: query.vpc,
                router: path.router,
            };
            let (.., vpc_router) = nexus
                .vpc_router_lookup(&opctx, router_selector)?
                .fetch()
                .await?;
            Ok(HttpResponseOk(vpc_router.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_router_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::VpcSelector>,
        create_params: TypedBody<params::VpcRouterCreate>,
    ) -> Result<HttpResponseCreated<VpcRouter>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let create = create_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let vpc_lookup = nexus.vpc_lookup(&opctx, query)?;
            let router = nexus
                .vpc_create_router(
                    &opctx,
                    &vpc_lookup,
                    &db::model::VpcRouterKind::Custom,
                    &create,
                )
                .await?;
            Ok(HttpResponseCreated(router.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_router_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::RouterPath>,
        query_params: Query<params::OptionalVpcSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let router_selector = params::RouterSelector {
                project: query.project,
                vpc: query.vpc,
                router: path.router,
            };
            let router_lookup =
                nexus.vpc_router_lookup(&opctx, router_selector)?;
            nexus.vpc_delete_router(&opctx, &router_lookup).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_router_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::RouterPath>,
        query_params: Query<params::OptionalVpcSelector>,
        router_params: TypedBody<params::VpcRouterUpdate>,
    ) -> Result<HttpResponseOk<VpcRouter>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let router_params = router_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let router_selector = params::RouterSelector {
                project: query.project,
                vpc: query.vpc,
                router: path.router,
            };
            let router_lookup =
                nexus.vpc_router_lookup(&opctx, router_selector)?;
            let router = nexus
                .vpc_update_router(&opctx, &router_lookup, &router_params)
                .await?;
            Ok(HttpResponseOk(router.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_router_route_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<params::RouterSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<RouterRoute>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let router_lookup = nexus
                .vpc_router_lookup(&opctx, scan_params.selector.clone())?;
            let routes = nexus
                .vpc_router_route_list(&opctx, &router_lookup, &paginated_by)
                .await?
                .into_iter()
                .map(|route| route.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                routes,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Vpc Router Routes

    async fn vpc_router_route_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::RoutePath>,
        query_params: Query<params::RouterSelector>,
    ) -> Result<HttpResponseOk<RouterRoute>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let route_selector = params::RouteSelector {
                project: query.project,
                vpc: query.vpc,
                router: Some(query.router),
                route: path.route,
            };
            let (.., route) = nexus
                .vpc_router_route_lookup(&opctx, route_selector)?
                .fetch()
                .await?;
            Ok(HttpResponseOk(route.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_router_route_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::RouterSelector>,
        create_params: TypedBody<params::RouterRouteCreate>,
    ) -> Result<HttpResponseCreated<RouterRoute>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let create = create_params.into_inner();
            let router_lookup = nexus.vpc_router_lookup(&opctx, query)?;
            let route = nexus
                .router_create_route(
                    &opctx,
                    &router_lookup,
                    &RouterRouteKind::Custom,
                    &create,
                )
                .await?;
            Ok(HttpResponseCreated(route.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_router_route_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::RoutePath>,
        query_params: Query<params::OptionalRouterSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let route_selector = params::RouteSelector {
                project: query.project,
                vpc: query.vpc,
                router: query.router,
                route: path.route,
            };
            let route_lookup =
                nexus.vpc_router_route_lookup(&opctx, route_selector)?;
            nexus.router_delete_route(&opctx, &route_lookup).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn vpc_router_route_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::RoutePath>,
        query_params: Query<params::OptionalRouterSelector>,
        router_params: TypedBody<params::RouterRouteUpdate>,
    ) -> Result<HttpResponseOk<RouterRoute>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let router_params = router_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let route_selector = params::RouteSelector {
                project: query.project,
                vpc: query.vpc,
                router: query.router,
                route: path.route,
            };
            let route_lookup =
                nexus.vpc_router_route_lookup(&opctx, route_selector)?;
            let route = nexus
                .router_update_route(&opctx, &route_lookup, &router_params)
                .await?;
            Ok(HttpResponseOk(route.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Internet gateways

    /// List internet gateways
    async fn internet_gateway_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<params::VpcSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<views::InternetGateway>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let vpc_lookup =
                nexus.vpc_lookup(&opctx, scan_params.selector.clone())?;
            let results = nexus
                .internet_gateway_list(&opctx, &vpc_lookup, &paginated_by)
                .await?
                .into_iter()
                .map(|s| s.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                results,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    /// Fetch internet gateway
    async fn internet_gateway_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::InternetGatewayPath>,
        query_params: Query<params::OptionalVpcSelector>,
    ) -> Result<HttpResponseOk<views::InternetGateway>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let selector = params::InternetGatewaySelector {
                project: query.project,
                vpc: query.vpc,
                gateway: path.gateway,
            };
            let (.., internet_gateway) = nexus
                .internet_gateway_lookup(&opctx, selector)?
                .fetch()
                .await?;
            Ok(HttpResponseOk(internet_gateway.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    /// Create VPC internet gateway
    async fn internet_gateway_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::VpcSelector>,
        create_params: TypedBody<params::InternetGatewayCreate>,
    ) -> Result<HttpResponseCreated<views::InternetGateway>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let create = create_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let query = query_params.into_inner();
            let vpc_lookup = nexus.vpc_lookup(&opctx, query)?;
            let result = nexus
                .internet_gateway_create(&opctx, &vpc_lookup, &create)
                .await?;
            Ok(HttpResponseCreated(result.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    /// Delete internet gateway
    async fn internet_gateway_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::InternetGatewayPath>,
        query_params: Query<params::InternetGatewayDeleteSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let selector = params::InternetGatewaySelector {
                project: query.project,
                vpc: query.vpc,
                gateway: path.gateway,
            };
            let lookup = nexus.internet_gateway_lookup(&opctx, selector)?;
            nexus
                .internet_gateway_delete(&opctx, &lookup, query.cascade)
                .await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    /// List IP pools attached to an internet gateway.
    async fn internet_gateway_ip_pool_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<
            PaginatedByNameOrId<params::InternetGatewaySelector>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<views::InternetGatewayIpPool>>,
        HttpError,
    > {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let lookup = nexus.internet_gateway_lookup(
                &opctx,
                scan_params.selector.clone(),
            )?;
            let results = nexus
                .internet_gateway_ip_pool_list(&opctx, &lookup, &paginated_by)
                .await?
                .into_iter()
                .map(|route| route.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                results,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    /// Attach an IP pool to an internet gateway
    async fn internet_gateway_ip_pool_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::InternetGatewaySelector>,
        create_params: TypedBody<params::InternetGatewayIpPoolCreate>,
    ) -> Result<HttpResponseCreated<views::InternetGatewayIpPool>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let create = create_params.into_inner();
            let lookup = nexus.internet_gateway_lookup(&opctx, query)?;
            let result = nexus
                .internet_gateway_ip_pool_attach(&opctx, &lookup, &create)
                .await?;
            Ok(HttpResponseCreated(result.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    /// Detach an IP pool from an internet gateway
    async fn internet_gateway_ip_pool_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::IpPoolPath>,
        query_params: Query<params::DeleteInternetGatewayElementSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let selector = params::InternetGatewayIpPoolSelector {
                project: query.project,
                vpc: query.vpc,
                gateway: query.gateway,
                pool: path.pool,
            };
            let lookup =
                nexus.internet_gateway_ip_pool_lookup(&opctx, selector)?;
            nexus
                .internet_gateway_ip_pool_detach(&opctx, &lookup, query.cascade)
                .await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    /// List addresses attached to an internet gateway.
    async fn internet_gateway_ip_address_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<
            PaginatedByNameOrId<params::InternetGatewaySelector>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<views::InternetGatewayIpAddress>>,
        HttpError,
    > {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let lookup = nexus.internet_gateway_lookup(
                &opctx,
                scan_params.selector.clone(),
            )?;
            let results = nexus
                .internet_gateway_ip_address_list(
                    &opctx,
                    &lookup,
                    &paginated_by,
                )
                .await?
                .into_iter()
                .map(|route| route.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                results,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    /// Attach an IP address to an internet gateway
    async fn internet_gateway_ip_address_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::InternetGatewaySelector>,
        create_params: TypedBody<params::InternetGatewayIpAddressCreate>,
    ) -> Result<HttpResponseCreated<views::InternetGatewayIpAddress>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let create = create_params.into_inner();
            let lookup = nexus.internet_gateway_lookup(&opctx, query)?;
            let route = nexus
                .internet_gateway_ip_address_attach(&opctx, &lookup, &create)
                .await?;
            Ok(HttpResponseCreated(route.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    /// Detach an IP address from an internet gateway
    async fn internet_gateway_ip_address_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::IpAddressPath>,
        query_params: Query<params::DeleteInternetGatewayElementSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let selector = params::InternetGatewayIpAddressSelector {
                project: query.project,
                vpc: query.vpc,
                gateway: query.gateway,
                address: path.address,
            };
            let lookup =
                nexus.internet_gateway_ip_address_lookup(&opctx, selector)?;
            nexus
                .internet_gateway_ip_address_detach(
                    &opctx,
                    &lookup,
                    query.cascade,
                )
                .await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Racks

    async fn rack_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<Rack>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let racks = nexus
                .racks_list(&opctx, &data_page_params_for(&rqctx, &query)?)
                .await?
                .into_iter()
                .map(|r| r.into())
                .collect();
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                racks,
                &|_, rack: &Rack| rack.identity.id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn rack_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::RackPath>,
    ) -> Result<HttpResponseOk<Rack>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let rack_info = nexus.rack_lookup(&opctx, &path.rack_id).await?;
            Ok(HttpResponseOk(rack_info.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn sled_list_uninitialized(
        rqctx: RequestContext<ApiContext>,
        query: Query<PaginationParams<EmptyScanParams, String>>,
    ) -> Result<HttpResponseOk<ResultsPage<shared::UninitializedSled>>, HttpError>
    {
        let apictx = rqctx.context();
        // We don't actually support real pagination
        let pag_params = query.into_inner();
        if let dropshot::WhichPage::Next(last_seen) = &pag_params.page {
            return Err(Error::invalid_value(
                last_seen.clone(),
                "bad page token",
            )
            .into());
        }
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let sleds = nexus.sled_list_uninitialized(&opctx).await?;
            Ok(HttpResponseOk(ResultsPage { items: sleds, next_page: None }))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn sled_add(
        rqctx: RequestContext<ApiContext>,
        sled: TypedBody<params::UninitializedSledId>,
    ) -> Result<HttpResponseCreated<views::SledId>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let id = nexus
                .sled_add(&opctx, sled.into_inner())
                .await?
                .into_untyped_uuid();
            Ok(HttpResponseCreated(views::SledId { id }))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Sleds

    async fn sled_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<Sled>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let sleds = nexus
                .sled_list(&opctx, &data_page_params_for(&rqctx, &query)?)
                .await?
                .into_iter()
                .map(|s| s.into())
                .collect();
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                sleds,
                &|_, sled: &Sled| sled.identity.id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn sled_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SledPath>,
    ) -> Result<HttpResponseOk<Sled>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let (.., sled) =
                nexus.sled_lookup(&opctx, &path.sled_id)?.fetch().await?;
            Ok(HttpResponseOk(sled.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn sled_set_provision_policy(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SledPath>,
        new_provision_state: TypedBody<params::SledProvisionPolicyParams>,
    ) -> Result<HttpResponseOk<params::SledProvisionPolicyResponse>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;

            let path = path_params.into_inner();
            let new_state = new_provision_state.into_inner().state;

            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;

            let sled_lookup = nexus.sled_lookup(&opctx, &path.sled_id)?;

            let old_state = nexus
                .sled_set_provision_policy(&opctx, &sled_lookup, new_state)
                .await?;

            let response =
                params::SledProvisionPolicyResponse { old_state, new_state };

            Ok(HttpResponseOk(response))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn sled_instance_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SledPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<views::SledInstance>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let sled_lookup = nexus.sled_lookup(&opctx, &path.sled_id)?;
            let sled_instances = nexus
                .sled_instance_list(
                    &opctx,
                    &sled_lookup,
                    &data_page_params_for(&rqctx, &query)?,
                )
                .await?
                .into_iter()
                .map(|s| s.into())
                .collect();
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                sled_instances,
                &|_, sled_instance: &views::SledInstance| {
                    sled_instance.identity.id
                },
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Physical disks

    async fn physical_disk_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<PhysicalDisk>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let disks = nexus
                .physical_disk_list(
                    &opctx,
                    &data_page_params_for(&rqctx, &query)?,
                )
                .await?
                .into_iter()
                .map(|s| s.into())
                .collect();
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                disks,
                &|_, disk: &PhysicalDisk| disk.identity.id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn physical_disk_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::PhysicalDiskPath>,
    ) -> Result<HttpResponseOk<PhysicalDisk>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;

            let (.., physical_disk) =
                nexus.physical_disk_lookup(&opctx, &path)?.fetch().await?;
            Ok(HttpResponseOk(physical_disk.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Switches

    async fn switch_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<views::Switch>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let switches = nexus
                .switch_list(&opctx, &data_page_params_for(&rqctx, &query)?)
                .await?
                .into_iter()
                .map(|s| s.into())
                .collect();
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                switches,
                &|_, switch: &views::Switch| switch.identity.id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn switch_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SwitchPath>,
    ) -> Result<HttpResponseOk<views::Switch>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let (.., switch) = nexus
                .switch_lookup(
                    &opctx,
                    params::SwitchSelector { switch: path.switch_id },
                )?
                .fetch()
                .await?;
            Ok(HttpResponseOk(switch.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn sled_physical_disk_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SledPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<PhysicalDisk>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let disks = nexus
                .sled_list_physical_disks(
                    &opctx,
                    path.sled_id,
                    &data_page_params_for(&rqctx, &query)?,
                )
                .await?
                .into_iter()
                .map(|s| s.into())
                .collect();
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                disks,
                &|_, disk: &PhysicalDisk| disk.identity.id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Metrics

    async fn system_metric(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<SystemMetricsPathParam>,
        pag_params: Query<
            PaginationParams<params::ResourceMetrics, params::ResourceMetrics>,
        >,
        other_params: Query<params::OptionalSiloSelector>,
    ) -> Result<HttpResponseOk<ResultsPage<oximeter_db::Measurement>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let metric_name = path_params.into_inner().metric_name;
            let pagination = pag_params.into_inner();
            let limit = rqctx.page_limit(&pagination)?;

            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let silo_lookup = match other_params.into_inner().silo {
                Some(silo) => Some(nexus.silo_lookup(&opctx, silo)?),
                _ => None,
            };

            let result = nexus
                .system_metric_list(
                    &opctx,
                    metric_name,
                    silo_lookup,
                    pagination,
                    limit,
                )
                .await?;

            Ok(HttpResponseOk(result))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn silo_metric(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<SystemMetricsPathParam>,
        pag_params: Query<
            PaginationParams<params::ResourceMetrics, params::ResourceMetrics>,
        >,
        other_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<ResultsPage<oximeter_db::Measurement>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let metric_name = path_params.into_inner().metric_name;

            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project_lookup = match other_params.into_inner().project {
                Some(project) => {
                    let project_selector = params::ProjectSelector { project };
                    Some(nexus.project_lookup(&opctx, project_selector)?)
                }
                _ => None,
            };

            let pagination = pag_params.into_inner();
            let limit = rqctx.page_limit(&pagination)?;

            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let result = nexus
                .silo_metric_list(
                    &opctx,
                    metric_name,
                    project_lookup,
                    pagination,
                    limit,
                )
                .await?;

            Ok(HttpResponseOk(result))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn timeseries_schema_list(
        rqctx: RequestContext<ApiContext>,
        pag_params: Query<TimeseriesSchemaPaginationParams>,
    ) -> Result<
        HttpResponseOk<ResultsPage<oximeter_db::TimeseriesSchema>>,
        HttpError,
    > {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let pagination = pag_params.into_inner();
            let limit = rqctx.page_limit(&pagination)?;
            nexus
                .timeseries_schema_list(&opctx, &pagination, limit)
                .await
                .map(HttpResponseOk)
                .map_err(HttpError::from)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn timeseries_query(
        rqctx: RequestContext<ApiContext>,
        body: TypedBody<params::TimeseriesQuery>,
    ) -> Result<HttpResponseOk<views::OxqlQueryResult>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let query = body.into_inner().query;
            nexus
                .timeseries_query(&opctx, &query)
                .await
                .map(|tables| HttpResponseOk(views::OxqlQueryResult { tables }))
                .map_err(HttpError::from)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Updates

    async fn system_update_put_repository(
        rqctx: RequestContext<ApiContext>,
        query: Query<params::UpdatesPutRepositoryParams>,
        body: StreamingBody,
    ) -> Result<HttpResponseOk<TufRepoInsertResponse>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let query = query.into_inner();
            let body = body.into_stream();
            let update = nexus
                .updates_put_repository(&opctx, body, query.file_name)
                .await?;
            Ok(HttpResponseOk(update))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn system_update_get_repository(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::UpdatesGetRepositoryParams>,
    ) -> Result<HttpResponseOk<TufRepoGetResponse>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let params = path_params.into_inner();
            let description = nexus
                .updates_get_repository(&opctx, params.system_version)
                .await?;
            Ok(HttpResponseOk(TufRepoGetResponse {
                description: description.into_external(),
            }))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Silo users

    async fn user_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedById<params::OptionalGroupSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<User>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pagparams = data_page_params_for(&rqctx, &query)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let scan_params = ScanById::from_query(&query)?;

            // TODO: a valid UUID gets parsed here and will 404 if it doesn't exist
            // (as expected) but a non-UUID string just gets let through as None
            // (i.e., ignored) instead of 400ing

            let users = if let Some(group_id) = scan_params.selector.group {
                nexus
                    .current_silo_group_users_list(
                        &opctx, &pagparams, &group_id,
                    )
                    .await?
            } else {
                nexus.silo_users_list_current(&opctx, &pagparams).await?
            };

            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                users.into_iter().map(|i| i.into()).collect(),
                &|_, user: &User| user.id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Silo groups

    async fn group_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<Group>>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let query = query_params.into_inner();
        let pagparams = data_page_params_for(&rqctx, &query)?;
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let groups = nexus
                .silo_groups_list(&opctx, &pagparams)
                .await?
                .into_iter()
                .map(|i| i.into())
                .collect();
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                groups,
                &|_, group: &Group| group.id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn group_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::GroupPath>,
    ) -> Result<HttpResponseOk<Group>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let (.., group) =
                nexus.silo_group_lookup(&opctx, &path.group_id).fetch().await?;
            Ok(HttpResponseOk(group.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Built-in (system) users

    async fn user_builtin_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByName>,
    ) -> Result<HttpResponseOk<ResultsPage<UserBuiltin>>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let query = query_params.into_inner();
        let pagparams = data_page_params_for(&rqctx, &query)?
            .map_name(|n| Name::ref_cast(n));
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let users = nexus
                .users_builtin_list(&opctx, &pagparams)
                .await?
                .into_iter()
                .map(|i| i.into())
                .collect();
            Ok(HttpResponseOk(ScanByName::results_page(
                &query,
                users,
                &marker_for_name,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn user_builtin_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::UserBuiltinSelector>,
    ) -> Result<HttpResponseOk<UserBuiltin>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let user_selector = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let (.., user) = nexus
                .user_builtin_lookup(&opctx, &user_selector)?
                .fetch()
                .await?;
            Ok(HttpResponseOk(user.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Built-in roles

    async fn role_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<
            PaginationParams<EmptyScanParams, params::RolePage>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<Role>>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let query = query_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let marker = match &query.page {
                WhichPage::First(..) => None,
                WhichPage::Next(params::RolePage { last_seen }) => {
                    Some(last_seen.split_once('.').ok_or_else(|| {
                        Error::invalid_value(
                            last_seen.clone(),
                            "bad page token",
                        )
                    })?)
                    .map(|(s1, s2)| (s1.to_string(), s2.to_string()))
                }
            };
            let pagparams = DataPageParams {
                limit: rqctx.page_limit(&query)?,
                direction: PaginationOrder::Ascending,
                marker: marker.as_ref(),
            };
            let roles = nexus
                .roles_builtin_list(&opctx, &pagparams)
                .await?
                .into_iter()
                .map(|i| i.into())
                .collect();
            Ok(HttpResponseOk(dropshot::ResultsPage::new(
                roles,
                &EmptyScanParams {},
                |role: &Role, _| params::RolePage {
                    last_seen: role.name.to_string(),
                },
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn role_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::RolePath>,
    ) -> Result<HttpResponseOk<Role>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let role_name = &path.role_name;
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let role = nexus.role_builtin_fetch(&opctx, &role_name).await?;
            Ok(HttpResponseOk(role.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Current user

    async fn current_user_view(
        rqctx: RequestContext<ApiContext>,
    ) -> Result<HttpResponseOk<views::CurrentUser>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let user = nexus.silo_user_fetch_self(&opctx).await?;
            let (_, silo) = nexus.current_silo_lookup(&opctx)?.fetch().await?;
            Ok(HttpResponseOk(views::CurrentUser {
                user: user.into(),
                silo_name: silo.name().clone(),
            }))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn current_user_groups(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<views::Group>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let groups = nexus
                .silo_user_fetch_groups_for_self(
                    &opctx,
                    &data_page_params_for(&rqctx, &query)?,
                )
                .await?
                .into_iter()
                .map(|d| d.into())
                .collect();
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                groups,
                &|_, group: &views::Group| group.id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn current_user_ssh_key_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<SshKey>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let &actor = opctx
                .authn
                .actor_required()
                .internal_context("listing current user's ssh keys")?;
            let ssh_keys = nexus
                .ssh_keys_list(&opctx, actor.actor_id(), &paginated_by)
                .await?
                .into_iter()
                .map(SshKey::from)
                .collect::<Vec<SshKey>>();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                ssh_keys,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn current_user_ssh_key_create(
        rqctx: RequestContext<ApiContext>,
        new_key: TypedBody<params::SshKeyCreate>,
    ) -> Result<HttpResponseCreated<SshKey>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let &actor = opctx
                .authn
                .actor_required()
                .internal_context("creating ssh key for current user")?;
            let ssh_key = nexus
                .ssh_key_create(&opctx, actor.actor_id(), new_key.into_inner())
                .await?;
            Ok(HttpResponseCreated(ssh_key.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn current_user_ssh_key_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SshKeyPath>,
    ) -> Result<HttpResponseOk<SshKey>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let &actor = opctx
                .authn
                .actor_required()
                .internal_context("fetching one of current user's ssh keys")?;
            let ssh_key_selector = params::SshKeySelector {
                silo_user_id: actor.actor_id(),
                ssh_key: path.ssh_key,
            };
            let ssh_key_lookup =
                nexus.ssh_key_lookup(&opctx, &ssh_key_selector)?;
            let (.., silo_user, _, ssh_key) = ssh_key_lookup.fetch().await?;
            // Ensure the SSH key exists in the current silo
            assert_eq!(silo_user.id(), actor.actor_id());
            Ok(HttpResponseOk(ssh_key.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn current_user_ssh_key_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::SshKeyPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let &actor = opctx
                .authn
                .actor_required()
                .internal_context("deleting one of current user's ssh keys")?;
            let ssh_key_selector = params::SshKeySelector {
                silo_user_id: actor.actor_id(),
                ssh_key: path.ssh_key,
            };
            let ssh_key_lookup =
                nexus.ssh_key_lookup(&opctx, &ssh_key_selector)?;
            nexus
                .ssh_key_delete(&opctx, actor.actor_id(), &ssh_key_lookup)
                .await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn probe_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<ProbeInfo>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
            let project_lookup =
                nexus.project_lookup(&opctx, scan_params.selector.clone())?;

            let probes = nexus
                .probe_list(&opctx, &project_lookup, &paginated_by)
                .await?;

            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                probes,
                &|_, p: &ProbeInfo| match paginated_by {
                    PaginatedBy::Id(_) => NameOrId::Id(p.id),
                    PaginatedBy::Name(_) => NameOrId::Name(p.name.clone()),
                },
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn probe_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<params::ProbePath>,
        query_params: Query<params::ProjectSelector>,
    ) -> Result<HttpResponseOk<ProbeInfo>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let project_selector = query_params.into_inner();
            let project_lookup =
                nexus.project_lookup(&opctx, project_selector)?;
            let probe =
                nexus.probe_get(&opctx, &project_lookup, &path.probe).await?;
            Ok(HttpResponseOk(probe))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn probe_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::ProjectSelector>,
        new_probe: TypedBody<params::ProbeCreate>,
    ) -> Result<HttpResponseCreated<Probe>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

            let nexus = &apictx.context.nexus;
            let new_probe_params = &new_probe.into_inner();
            let project_selector = query_params.into_inner();
            let project_lookup =
                nexus.project_lookup(&opctx, project_selector)?;
            let probe = nexus
                .probe_create(&opctx, &project_lookup, &new_probe_params)
                .await?;
            Ok(HttpResponseCreated(probe.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn probe_delete(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<params::ProjectSelector>,
        path_params: Path<params::ProbePath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let project_selector = query_params.into_inner();
            let project_lookup =
                nexus.project_lookup(&opctx, project_selector)?;
            nexus.probe_delete(&opctx, &project_lookup, path.probe).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn login_saml_begin(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::LoginToProviderPathParam>,
        query_params: Query<params::LoginUrlQuery>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::login_saml_begin(rqctx, path_params, query_params).await
    }

    async fn login_saml_redirect(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::LoginToProviderPathParam>,
        query_params: Query<params::LoginUrlQuery>,
    ) -> Result<HttpResponseFound, HttpError> {
        console_api::login_saml_redirect(rqctx, path_params, query_params).await
    }

    async fn login_saml(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::LoginToProviderPathParam>,
        body_bytes: dropshot::UntypedBody,
    ) -> Result<HttpResponseSeeOther, HttpError> {
        console_api::login_saml(rqctx, path_params, body_bytes).await
    }

    async fn login_local_begin(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::LoginPath>,
        query_params: Query<params::LoginUrlQuery>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::login_local_begin(rqctx, path_params, query_params).await
    }

    async fn login_local(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::LoginPath>,
        credentials: TypedBody<params::UsernamePasswordCredentials>,
    ) -> Result<HttpResponseHeaders<HttpResponseUpdatedNoContent>, HttpError>
    {
        console_api::login_local(rqctx, path_params, credentials).await
    }

    async fn logout(
        rqctx: RequestContext<Self::Context>,
        cookies: Cookies,
    ) -> Result<HttpResponseHeaders<HttpResponseUpdatedNoContent>, HttpError>
    {
        console_api::logout(rqctx, cookies).await
    }

    async fn login_begin(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::LoginUrlQuery>,
    ) -> Result<HttpResponseFound, HttpError> {
        console_api::login_begin(rqctx, query_params).await
    }

    async fn console_projects(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_projects(rqctx, path_params).await
    }

    async fn console_settings_page(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_settings_page(rqctx, path_params).await
    }

    async fn console_system_page(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_system_page(rqctx, path_params).await
    }

    async fn console_lookup(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_lookup(rqctx, path_params).await
    }

    async fn console_root(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_root(rqctx).await
    }

    async fn console_projects_new(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_projects_new(rqctx).await
    }

    async fn console_silo_images(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_silo_images(rqctx).await
    }

    async fn console_silo_utilization(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_silo_utilization(rqctx).await
    }

    async fn console_silo_access(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_silo_access(rqctx).await
    }

    async fn asset(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::asset(rqctx, path_params).await
    }

    async fn device_auth_request(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<params::DeviceAuthRequest>,
    ) -> Result<Response<Body>, HttpError> {
        device_auth::device_auth_request(rqctx, params).await
    }

    async fn device_auth_verify(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        device_auth::device_auth_verify(rqctx).await
    }

    async fn device_auth_success(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        device_auth::device_auth_success(rqctx).await
    }

    async fn device_auth_confirm(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<params::DeviceAuthVerify>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        device_auth::device_auth_confirm(rqctx, params).await
    }

    async fn device_access_token(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<params::DeviceAccessTokenRequest>,
    ) -> Result<Response<Body>, HttpError> {
        device_auth::device_access_token(rqctx, params.into_inner()).await
    }
}
