// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for external HTTP APIs

use super::console_api;
use crate::app::SetTargetReleaseIntent;
use crate::app::external_endpoints::authority_for_request;
use crate::app::support_bundles::SupportBundleQueryType;
use crate::context::{ApiContext, audit_and_time};
use dropshot::Body;
use dropshot::EmptyScanParams;
use dropshot::Header;
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
use dropshot::{http_response_found, http_response_see_other};
use http::{Response, StatusCode, header};
use ipnetwork::IpNetwork;
use nexus_db_lookup::lookup::ImageLookup;
use nexus_db_lookup::lookup::ImageParentLookup;
use nexus_db_queries::authn::external::session_cookie::{self, SessionStore};
use nexus_db_queries::authz;
use nexus_db_queries::db;
use nexus_db_queries::db::identity::Resource;
use nexus_db_queries::db::model::Name;
use nexus_external_api::*;
use nexus_types::authn::cookies::Cookies;
use nexus_types::external_api::{
    affinity, alert, audit, certificate, console, device, disk, external_ip,
    external_subnet, floating_ip, hardware, identity_provider, image, instance,
    internet_gateway, ip_pool, metrics, multicast, networking, oxql,
    path_params, policy, probe, project, rack, scim, silo, sled, snapshot,
    ssh_key, subnet_pool, support_bundle, switch, system, timeseries, update,
    user, vpc,
};
// Type imports for API implementations (per RFD 619)
use nexus_types::external_api::bfd::BfdStatus;
use nexus_types::external_api::certificate::Certificate;
use nexus_types::external_api::floating_ip::FloatingIp;
use nexus_types::external_api::identity_provider::IdentityProvider;
use nexus_types::external_api::image::Image;
use nexus_types::external_api::ip_pool::{IpPool, IpPoolRange};
use nexus_types::external_api::metrics::SystemMetricsPathParam;
use nexus_types::external_api::physical_disk::PhysicalDisk;
use nexus_types::external_api::probe::ProbeInfo;
use nexus_types::external_api::project::Project;
use nexus_types::external_api::rack::{Rack, RackMembershipStatus};
use nexus_types::external_api::silo::{
    Silo, SiloQuotas, SiloUtilization, Utilization,
};
use nexus_types::external_api::sled::Sled;
use nexus_types::external_api::snapshot::Snapshot;
use nexus_types::external_api::ssh_key::SshKey;
use nexus_types::external_api::user::{Group, User, UserBuiltin};
use nexus_types::external_api::vpc::{Vpc, VpcRouter, VpcSubnet};
use nexus_types_versions::latest::headers::RangeRequest;
use nexus_types_versions::v2025_11_20_00;
use omicron_common::address::IpRange;
use omicron_common::api::external::AddressLot;
use omicron_common::api::external::AddressLotBlock;
use omicron_common::api::external::AddressLotCreateResponse;
use omicron_common::api::external::AddressLotViewResponse;
use omicron_common::api::external::AffinityGroupMember;
use omicron_common::api::external::AggregateBgpMessageHistory;
use omicron_common::api::external::AntiAffinityGroupMember;
use omicron_common::api::external::BgpAnnounceSet;
use omicron_common::api::external::BgpAnnouncement;
use omicron_common::api::external::BgpConfig;
use omicron_common::api::external::BgpExported;
use omicron_common::api::external::BgpImported;
use omicron_common::api::external::BgpPeerStatus;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Disk;
use omicron_common::api::external::Error;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceNetworkInterface;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::LldpLinkConfig;
use omicron_common::api::external::LldpNeighbor;
use omicron_common::api::external::LoopbackAddress;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::Probe;
use omicron_common::api::external::RouterRoute;
use omicron_common::api::external::RouterRouteKind;
use omicron_common::api::external::ServiceIcmpConfig;
use omicron_common::api::external::SwitchPort;
use omicron_common::api::external::SwitchPortSettings;
use omicron_common::api::external::SwitchPortSettingsIdentity;
use omicron_common::api::external::VpcFirewallRuleUpdateParams;
use omicron_common::api::external::VpcFirewallRules;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::http_pagination::PaginatedById;
use omicron_common::api::external::http_pagination::PaginatedByName;
use omicron_common::api::external::http_pagination::PaginatedByNameOrId;
use omicron_common::api::external::http_pagination::PaginatedByTimeAndId;
use omicron_common::api::external::http_pagination::PaginatedByVersion;
use omicron_common::api::external::http_pagination::ScanById;
use omicron_common::api::external::http_pagination::ScanByName;
use omicron_common::api::external::http_pagination::ScanByNameOrId;
use omicron_common::api::external::http_pagination::ScanByTimeAndId;
use omicron_common::api::external::http_pagination::ScanByVersion;
use omicron_common::api::external::http_pagination::ScanParams;
use omicron_common::api::external::http_pagination::data_page_params_for;
use omicron_common::api::external::http_pagination::marker_for_id;
use omicron_common::api::external::http_pagination::marker_for_name;
use omicron_common::api::external::http_pagination::marker_for_name_or_id;
use omicron_common::api::external::http_pagination::name_or_id_pagination;
use omicron_common::bail_unless;
use omicron_uuid_kinds::*;
use propolis_client::support::WebSocketStream;
use propolis_client::support::tungstenite::protocol::frame::coding::CloseCode;
use propolis_client::support::tungstenite::protocol::{
    CloseFrame, Role as WebSocketRole,
};
use range_requests::PotentialRange;
use ref_cast::RefCast;
use trust_quorum_types::types::Epoch;

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
    ) -> Result<HttpResponseOk<policy::Policy<policy::FleetRole>>, HttpError>
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
        new_policy: TypedBody<policy::Policy<policy::FleetRole>>,
    ) -> Result<HttpResponseOk<policy::Policy<policy::FleetRole>>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let new_policy = new_policy.into_inner();
            let nasgns = new_policy.role_assignments.len();
            // This should have been validated during parsing.
            bail_unless!(nasgns <= policy::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
            let policy = nexus.fleet_update_policy(&opctx, &new_policy).await?;
            Ok(HttpResponseOk(policy))
        })
        .await
    }

    async fn policy_view(
        rqctx: RequestContext<ApiContext>,
    ) -> Result<HttpResponseOk<policy::Policy<policy::SiloRole>>, HttpError>
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
        new_policy: TypedBody<policy::Policy<policy::SiloRole>>,
    ) -> Result<HttpResponseOk<policy::Policy<policy::SiloRole>>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let new_policy = new_policy.into_inner();
            let nasgns = new_policy.role_assignments.len();
            // This should have been validated during parsing.
            bail_unless!(nasgns <= policy::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
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
        })
        .await
    }

    async fn auth_settings_view(
        rqctx: RequestContext<ApiContext>,
    ) -> Result<HttpResponseOk<silo::SiloAuthSettings>, HttpError> {
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
            let settings =
                nexus.silo_fetch_auth_settings(&opctx, &silo_lookup).await?;
            Ok(HttpResponseOk(settings.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn auth_settings_update(
        rqctx: RequestContext<Self::Context>,
        new_settings: TypedBody<silo::SiloAuthSettingsUpdate>,
    ) -> Result<HttpResponseOk<silo::SiloAuthSettings>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let new_settings = new_settings.into_inner();
            let silo: NameOrId = opctx
                .authn
                .silo_required()
                .internal_context("loading current silo")?
                .id()
                .into();
            let silo_lookup = nexus.silo_lookup(&opctx, silo)?;
            let settings = nexus
                .silo_update_auth_settings(&opctx, &silo_lookup, &new_settings)
                .await?;
            Ok(HttpResponseOk(settings.into()))
        })
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
        path_params: Path<path_params::SiloPath>,
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
        path_params: Path<path_params::SiloPath>,
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
        path_params: Path<path_params::SiloPath>,
        new_quota: TypedBody<silo::SiloQuotasUpdate>,
    ) -> Result<HttpResponseOk<SiloQuotas>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let new_quota = new_quota.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, path.silo)?;
            let quota = nexus
                .silo_update_quota(&opctx, &silo_lookup, &new_quota)
                .await?;
            Ok(HttpResponseOk(quota.into()))
        })
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
        new_silo_params: TypedBody<silo::SiloCreate>,
    ) -> Result<HttpResponseCreated<Silo>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let new_silo_params = new_silo_params.into_inner();
            let silo = nexus.silo_create(&opctx, new_silo_params).await?;
            Ok(HttpResponseCreated(silo.try_into()?))
        })
        .await
    }

    async fn silo_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::SiloPath>,
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
        path_params: Path<path_params::SiloPath>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<ip_pool::SiloIpPool>>, HttpError>
    {
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
                .map(|(pool, silo_link)| ip_pool::SiloIpPool {
                    identity: pool.identity(),
                    is_default: silo_link.is_default,
                    ip_version: pool.ip_version.into(),
                    pool_type: pool.pool_type.into(),
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
        path_params: Path<path_params::SiloPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let params = path_params.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, params.silo)?;
            nexus.silo_delete(&opctx, &silo_lookup).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn silo_policy_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::SiloPath>,
    ) -> Result<HttpResponseOk<policy::Policy<policy::SiloRole>>, HttpError>
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
        path_params: Path<path_params::SiloPath>,
        new_policy: TypedBody<policy::Policy<policy::SiloRole>>,
    ) -> Result<HttpResponseOk<policy::Policy<policy::SiloRole>>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let new_policy = new_policy.into_inner();
            let nasgns = new_policy.role_assignments.len();
            // This should have been validated during parsing.
            bail_unless!(nasgns <= policy::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
            let silo_lookup = nexus.silo_lookup(&opctx, path.silo)?;
            let policy = nexus
                .silo_update_policy(&opctx, &silo_lookup, &new_policy)
                .await?;
            Ok(HttpResponseOk(policy))
        })
        .await
    }

    // Silo-specific user endpoints

    async fn silo_user_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedById<silo::SiloSelector>>,
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
                &|_, user: &User| user.id.into_untyped_uuid(),
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
        path_params: Path<user::UserParam>,
        query_params: Query<silo::SiloSelector>,
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
        query_params: Query<PaginatedByNameOrId<silo::SiloSelector>>,
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
        query_params: Query<silo::SiloSelector>,
        new_provider: TypedBody<identity_provider::SamlIdentityProviderCreate>,
    ) -> Result<
        HttpResponseCreated<identity_provider::SamlIdentityProvider>,
        HttpError,
    > {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let new_provider = new_provider.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, query.silo)?;
            let provider = nexus
                .saml_identity_provider_create(
                    &opctx,
                    &silo_lookup,
                    new_provider,
                )
                .await?;
            Ok(HttpResponseCreated(provider.into()))
        })
        .await
    }

    async fn saml_identity_provider_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::ProviderPath>,
        query_params: Query<silo::OptionalSiloSelector>,
    ) -> Result<
        HttpResponseOk<identity_provider::SamlIdentityProvider>,
        HttpError,
    > {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let saml_identity_provider_selector =
                identity_provider::SamlIdentityProviderSelector {
                    silo: query.silo,
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
        query_params: Query<silo::SiloSelector>,
        new_user_params: TypedBody<user::UserCreate>,
    ) -> Result<HttpResponseCreated<User>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let new_user_params = new_user_params.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, query.silo)?;
            let user = nexus
                .local_idp_create_user(&opctx, &silo_lookup, new_user_params)
                .await?;
            Ok(HttpResponseCreated(user.into()))
        })
        .await
    }

    async fn local_idp_user_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<user::UserParam>,
        query_params: Query<silo::SiloSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, query.silo)?;
            nexus
                .local_idp_delete_user(&opctx, &silo_lookup, path.user_id)
                .await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn local_idp_user_set_password(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<user::UserParam>,
        query_params: Query<path_params::SiloPath>,
        update: TypedBody<user::UserPassword>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let update = update.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, query.silo)?;
            nexus
                .local_idp_user_set_password(
                    &opctx,
                    &silo_lookup,
                    path.user_id,
                    update,
                )
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        })
        .await
    }

    async fn scim_token_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<silo::SiloSelector>,
    ) -> Result<HttpResponseOk<Vec<scim::ScimClientBearerToken>>, HttpError>
    {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let query = query_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let silo_lookup = nexus.silo_lookup(&opctx, query.silo)?;
            let tokens =
                nexus.scim_idp_get_tokens(&opctx, &silo_lookup).await?;
            Ok(HttpResponseOk(tokens))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn scim_token_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<silo::SiloSelector>,
    ) -> Result<HttpResponseCreated<scim::ScimClientBearerTokenValue>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, query.silo)?;
            let token =
                nexus.scim_idp_create_token(&opctx, &silo_lookup).await?;
            Ok(HttpResponseCreated(token))
        })
        .await
    }

    async fn scim_token_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<scim::ScimV2TokenPathParam>,
        query_params: Query<silo::SiloSelector>,
    ) -> Result<HttpResponseOk<scim::ScimClientBearerToken>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let query = query_params.into_inner();
        let path = path_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let silo_lookup = nexus.silo_lookup(&opctx, query.silo)?;
            let token = nexus
                .scim_idp_get_token_by_id(&opctx, &silo_lookup, path.token_id)
                .await?;
            Ok(HttpResponseOk(token))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn scim_token_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<scim::ScimV2TokenPathParam>,
        query_params: Query<silo::SiloSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let path = path_params.into_inner();
            let silo_lookup = nexus.silo_lookup(&opctx, query.silo)?;
            nexus
                .scim_idp_delete_token_by_id(
                    &opctx,
                    &silo_lookup,
                    path.token_id,
                )
                .await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn scim_v2_list_users(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<scim2_rs::QueryParams>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            nexus.scim_v2_list_users(&opctx, query).await
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn scim_v2_get_user(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<scim::ScimV2UserPathParam>,
        query_params: Query<scim2_rs::QueryParams>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            nexus.scim_v2_get_user_by_id(&opctx, query, path.user_id).await
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn scim_v2_create_user(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<scim2_rs::CreateUserRequest>,
    ) -> Result<Response<Body>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let body = body.into_inner();
            nexus.scim_v2_create_user(&opctx, body).await
        })
        .await
    }

    async fn scim_v2_put_user(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<scim::ScimV2UserPathParam>,
        body: TypedBody<scim2_rs::CreateUserRequest>,
    ) -> Result<Response<Body>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let body = body.into_inner();
            nexus.scim_v2_replace_user(&opctx, path.user_id, body).await
        })
        .await
    }

    async fn scim_v2_patch_user(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<scim::ScimV2UserPathParam>,
        body: TypedBody<scim2_rs::PatchRequest>,
    ) -> Result<Response<Body>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let body = body.into_inner();
            nexus.scim_v2_patch_user(&opctx, path.user_id, body).await
        })
        .await
    }

    async fn scim_v2_delete_user(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<scim::ScimV2UserPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            nexus.scim_v2_delete_user(&opctx, path.user_id).await
        })
        .await
    }

    async fn scim_v2_list_groups(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<scim2_rs::QueryParams>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            nexus.scim_v2_list_groups(&opctx, query).await
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn scim_v2_get_group(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<scim::ScimV2GroupPathParam>,
        query_params: Query<scim2_rs::QueryParams>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            nexus.scim_v2_get_group_by_id(&opctx, query, path.group_id).await
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn scim_v2_create_group(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<scim2_rs::CreateGroupRequest>,
    ) -> Result<Response<Body>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let body = body.into_inner();
            nexus.scim_v2_create_group(&opctx, body).await
        })
        .await
    }

    async fn scim_v2_put_group(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<scim::ScimV2GroupPathParam>,
        body: TypedBody<scim2_rs::CreateGroupRequest>,
    ) -> Result<Response<Body>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let body = body.into_inner();
            nexus.scim_v2_replace_group(&opctx, path.group_id, body).await
        })
        .await
    }

    async fn scim_v2_patch_group(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<scim::ScimV2GroupPathParam>,
        body: TypedBody<scim2_rs::PatchRequest>,
    ) -> Result<Response<Body>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let body = body.into_inner();
            nexus.scim_v2_patch_group(&opctx, path.group_id, body).await
        })
        .await
    }

    async fn scim_v2_delete_group(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<scim::ScimV2GroupPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            nexus.scim_v2_delete_group(&opctx, path.group_id).await
        })
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
        new_project: TypedBody<project::ProjectCreate>,
    ) -> Result<HttpResponseCreated<Project>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let new_project = new_project.into_inner();
            let project = nexus.project_create(&opctx, &new_project).await?;
            Ok(HttpResponseCreated(project.into()))
        })
        .await
    }

    async fn project_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::ProjectPath>,
    ) -> Result<HttpResponseOk<Project>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project_selector =
                project::ProjectSelector { project: path.project };
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
        path_params: Path<path_params::ProjectPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let project_selector =
                project::ProjectSelector { project: path.project };
            let project_lookup =
                nexus.project_lookup(&opctx, project_selector)?;
            nexus.project_delete(&opctx, &project_lookup).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    // TODO-correctness: Is it valid for PUT to accept application/json that's a
    // subset of what the resource actually represents?  If not, is that a problem?
    // (HTTP may require that this be idempotent.)  If so, can we get around that
    // having this be a slightly different content-type (e.g.,
    // "application/json-patch")?  We should see what other APIs do.
    async fn project_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::ProjectPath>,
        updated_project: TypedBody<project::ProjectUpdate>,
    ) -> Result<HttpResponseOk<Project>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let updated_project = updated_project.into_inner();
            let project_selector =
                project::ProjectSelector { project: path.project };
            let project_lookup =
                nexus.project_lookup(&opctx, project_selector)?;
            let project = nexus
                .project_update(&opctx, &project_lookup, &updated_project)
                .await?;
            Ok(HttpResponseOk(project.into()))
        })
        .await
    }

    async fn project_policy_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::ProjectPath>,
    ) -> Result<HttpResponseOk<policy::Policy<policy::ProjectRole>>, HttpError>
    {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project_selector =
                project::ProjectSelector { project: path.project };
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
        path_params: Path<path_params::ProjectPath>,
        new_policy: TypedBody<policy::Policy<policy::ProjectRole>>,
    ) -> Result<HttpResponseOk<policy::Policy<policy::ProjectRole>>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let new_policy = new_policy.into_inner();
            let project_selector =
                project::ProjectSelector { project: path.project };
            let project_lookup =
                nexus.project_lookup(&opctx, project_selector)?;
            nexus
                .project_update_policy(&opctx, &project_lookup, &new_policy)
                .await?;
            Ok(HttpResponseOk(new_policy))
        })
        .await
    }

    // IP Pools

    async fn ip_pool_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<ip_pool::SiloIpPool>>, HttpError>
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
            let pools = nexus
                .current_silo_ip_pool_list(&opctx, &paginated_by)
                .await?
                .into_iter()
                .map(|(pool, silo_link)| ip_pool::SiloIpPool {
                    identity: pool.identity(),
                    is_default: silo_link.is_default,
                    ip_version: pool.ip_version.into(),
                    pool_type: pool.pool_type.into(),
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

    async fn ip_pool_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::IpPoolPath>,
    ) -> Result<HttpResponseOk<ip_pool::SiloIpPool>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let pool_selector = path_params.into_inner().pool;
            let (.., pool, silo_link) =
                nexus.silo_ip_pool_fetch(&opctx, &pool_selector).await?;
            Ok(HttpResponseOk(ip_pool::SiloIpPool {
                identity: pool.identity(),
                is_default: silo_link.is_default,
                ip_version: pool.ip_version.into(),
                pool_type: pool.pool_type.into(),
            }))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn system_ip_pool_list(
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

    async fn system_ip_pool_create(
        rqctx: RequestContext<ApiContext>,
        pool_params: TypedBody<ip_pool::IpPoolCreate>,
    ) -> Result<HttpResponseCreated<IpPool>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let pool_params = pool_params.into_inner();
            let pool = nexus.ip_pool_create(&opctx, &pool_params).await?;
            Ok(HttpResponseCreated(pool.into()))
        })
        .await
    }

    async fn system_ip_pool_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::IpPoolPath>,
    ) -> Result<HttpResponseOk<ip_pool::IpPool>, HttpError> {
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
            Ok(HttpResponseOk(pool.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn system_ip_pool_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::IpPoolPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
            nexus.ip_pool_delete(&opctx, &pool_lookup).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn system_ip_pool_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::IpPoolPath>,
        updates: TypedBody<ip_pool::IpPoolUpdate>,
    ) -> Result<HttpResponseOk<IpPool>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let updates = updates.into_inner();
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
            let pool =
                nexus.ip_pool_update(&opctx, &pool_lookup, &updates).await?;
            Ok(HttpResponseOk(pool.into()))
        })
        .await
    }

    async fn system_ip_pool_utilization_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::IpPoolPath>,
    ) -> Result<HttpResponseOk<ip_pool::IpPoolUtilization>, HttpError> {
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

    async fn system_ip_pool_silo_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::IpPoolPath>,
        // paginating by resource_id because they're unique per pool. most robust
        // option would be to paginate by a composite key representing the (pool,
        // resource_type, resource)
        query_params: Query<PaginatedById>,
        // TODO: this could just list silo::Silo -- it's not like knowing silo_id
        // and nothing else is particularly useful -- except we also want to say
        // whether the pool is marked default on each silo. So one option would
        // be  to do the same as we did with SiloIpPool -- include is_default on
        // whatever the thing is. Still... all we'd have to do to make this usable
        // in both places would be to make it { ...IpPool, silo_id, silo_name,
        // is_default }
    ) -> Result<HttpResponseOk<ResultsPage<ip_pool::IpPoolSiloLink>>, HttpError>
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
                &|_, x: &ip_pool::IpPoolSiloLink| x.silo_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn system_ip_pool_silo_link(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::IpPoolPath>,
        resource_assoc: TypedBody<ip_pool::IpPoolLinkSilo>,
    ) -> Result<HttpResponseCreated<ip_pool::IpPoolSiloLink>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let resource_assoc = resource_assoc.into_inner();
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
            let assoc = nexus
                .ip_pool_link_silo(&opctx, &pool_lookup, &resource_assoc)
                .await?;
            Ok(HttpResponseCreated(assoc.into()))
        })
        .await
    }

    async fn system_ip_pool_silo_unlink(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<ip_pool::IpPoolSiloPath>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
            let silo_lookup = nexus.silo_lookup(&opctx, path.silo)?;
            nexus
                .ip_pool_unlink_silo(&opctx, &pool_lookup, &silo_lookup)
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        })
        .await
    }

    async fn system_ip_pool_silo_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<ip_pool::IpPoolSiloPath>,
        update: TypedBody<ip_pool::IpPoolSiloUpdate>,
    ) -> Result<HttpResponseOk<ip_pool::IpPoolSiloLink>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
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
        })
        .await
    }

    async fn system_ip_pool_service_view(
        rqctx: RequestContext<ApiContext>,
    ) -> Result<HttpResponseOk<ip_pool::IpPool>, HttpError> {
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

    async fn system_ip_pool_range_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::IpPoolPath>,
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
                .map(|range| range.try_into())
                .collect::<Result<Vec<_>, _>>()?;
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

    async fn system_ip_pool_range_add(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::IpPoolPath>,
        range_params: TypedBody<IpRange>,
    ) -> Result<HttpResponseCreated<IpPoolRange>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let range = range_params.into_inner();
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
            let out =
                nexus.ip_pool_add_range(&opctx, &pool_lookup, &range).await?;
            Ok(HttpResponseCreated(out.try_into()?))
        })
        .await
    }

    async fn system_ip_pool_range_remove(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::IpPoolPath>,
        range_params: TypedBody<IpRange>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let range = range_params.into_inner();
            let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
            nexus.ip_pool_delete_range(&opctx, &pool_lookup, &range).await?;
            Ok(HttpResponseUpdatedNoContent())
        })
        .await
    }

    async fn system_ip_pool_service_range_list(
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
                .map(|range| range.try_into())
                .collect::<Result<Vec<_>, _>>()?;
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

    async fn system_ip_pool_service_range_add(
        rqctx: RequestContext<ApiContext>,
        range_params: TypedBody<IpRange>,
    ) -> Result<HttpResponseCreated<IpPoolRange>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let range = range_params.into_inner();
            let out = nexus.ip_pool_service_add_range(&opctx, &range).await?;
            Ok(HttpResponseCreated(out.try_into()?))
        })
        .await
    }

    async fn system_ip_pool_service_range_remove(
        rqctx: RequestContext<ApiContext>,
        range_params: TypedBody<IpRange>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let range = range_params.into_inner();
            nexus.ip_pool_service_delete_range(&opctx, &range).await?;
            Ok(HttpResponseUpdatedNoContent())
        })
        .await
    }

    // Subnet Pools

    async fn system_subnet_pool_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<subnet_pool::SubnetPool>>, HttpError>
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
            let pools = nexus.subnet_pool_list(&opctx, &paginated_by).await?;
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

    async fn system_subnet_pool_create(
        rqctx: RequestContext<ApiContext>,
        pool_params: TypedBody<subnet_pool::SubnetPoolCreate>,
    ) -> Result<HttpResponseCreated<subnet_pool::SubnetPool>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let pool_params = pool_params.into_inner();
            let pool = nexus.subnet_pool_create(&opctx, pool_params).await?;
            Ok(HttpResponseCreated(pool))
        })
        .await
    }

    async fn system_subnet_pool_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<subnet_pool::SubnetPoolPath>,
    ) -> Result<HttpResponseOk<subnet_pool::SubnetPool>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let pool_selector = path_params.into_inner().pool;
            let pool = nexus.subnet_pool_view(&opctx, &pool_selector).await?;
            Ok(HttpResponseOk(pool))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn system_subnet_pool_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<subnet_pool::SubnetPoolPath>,
        updates: TypedBody<subnet_pool::SubnetPoolUpdate>,
    ) -> Result<HttpResponseOk<subnet_pool::SubnetPool>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let updates = updates.into_inner();
            let pool =
                nexus.subnet_pool_update(&opctx, &path.pool, updates).await?;
            Ok(HttpResponseOk(pool))
        })
        .await
    }

    async fn system_subnet_pool_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<subnet_pool::SubnetPoolPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            nexus.subnet_pool_delete(&opctx, &path.pool).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn system_subnet_pool_member_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<subnet_pool::SubnetPoolPath>,
        query_params: Query<SubnetPoolMemberPaginationParams>,
    ) -> Result<
        HttpResponseOk<ResultsPage<subnet_pool::SubnetPoolMember>>,
        HttpError,
    > {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let marker = match query.page {
                WhichPage::First(_) => None,
                WhichPage::Next(ref net) => Some(net),
            };
            let pag_params = DataPageParams {
                limit: rqctx.page_limit(&query)?,
                direction: PaginationOrder::Ascending,
                marker,
            };
            let path = path_params.into_inner();
            nexus
                .subnet_pool_member_list(&opctx, &path.pool, &pag_params)
                .await
                .map_err(HttpError::from)
                .and_then(|items| {
                    ResultsPage::new(
                        items,
                        &EmptyScanParams {},
                        |member: &subnet_pool::SubnetPoolMember, _| {
                            member.subnet
                        },
                    )
                })
                .map(HttpResponseOk)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn system_subnet_pool_member_add(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<subnet_pool::SubnetPoolPath>,
        subnet_params: TypedBody<subnet_pool::SubnetPoolMemberAdd>,
    ) -> Result<HttpResponseCreated<subnet_pool::SubnetPoolMember>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let subnet_params = subnet_params.into_inner();
            let member = nexus
                .subnet_pool_member_add(&opctx, &path.pool, &subnet_params)
                .await?;
            Ok(HttpResponseCreated(member))
        })
        .await
    }

    async fn system_subnet_pool_member_remove(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<subnet_pool::SubnetPoolPath>,
        subnet_params: TypedBody<subnet_pool::SubnetPoolMemberRemove>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let subnet_params = subnet_params.into_inner();
            nexus
                .subnet_pool_member_remove(&opctx, &path.pool, &subnet_params)
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        })
        .await
    }

    async fn system_subnet_pool_silo_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<subnet_pool::SubnetPoolPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<ResultsPage<subnet_pool::SubnetPoolSiloLink>>,
        HttpError,
    > {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let path = path_params.into_inner();
            let links = nexus
                .subnet_pool_silo_list(&opctx, &path.pool, &pag_params)
                .await?;
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                links,
                &|_, x: &subnet_pool::SubnetPoolSiloLink| x.silo_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn silo_subnet_pool_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::SiloPath>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<subnet_pool::SiloSubnetPool>>,
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
            let path = path_params.into_inner();

            let silo_lookup = nexus.silo_lookup(&opctx, path.silo)?;
            let pools = nexus
                .silo_subnet_pool_list(&opctx, &silo_lookup, &paginated_by)
                .await?
                .into_iter()
                .map(|(pool, silo_link)| subnet_pool::SiloSubnetPool {
                    identity: pool.identity(),
                    is_default: silo_link.is_default,
                    ip_version: pool.ip_version.into(),
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

    async fn subnet_pool_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<subnet_pool::SiloSubnetPool>>,
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
            let pools = nexus
                .current_silo_subnet_pool_list(&opctx, &paginated_by)
                .await?
                .into_iter()
                .map(|(pool, silo_link)| subnet_pool::SiloSubnetPool {
                    identity: pool.identity(),
                    is_default: silo_link.is_default,
                    ip_version: pool.ip_version.into(),
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

    async fn subnet_pool_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<subnet_pool::SubnetPoolPath>,
    ) -> Result<HttpResponseOk<subnet_pool::SiloSubnetPool>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let pool_selector = path_params.into_inner().pool;
            let (pool, silo_link) =
                nexus.silo_subnet_pool_fetch(&opctx, &pool_selector).await?;
            Ok(HttpResponseOk(subnet_pool::SiloSubnetPool {
                identity: pool.identity(),
                is_default: silo_link.is_default,
                ip_version: pool.ip_version.into(),
            }))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn system_subnet_pool_silo_link(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<subnet_pool::SubnetPoolPath>,
        silo_link: TypedBody<subnet_pool::SubnetPoolLinkSilo>,
    ) -> Result<HttpResponseCreated<subnet_pool::SubnetPoolSiloLink>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let silo_link = silo_link.into_inner();
            let link = nexus
                .subnet_pool_silo_link(&opctx, &path.pool, silo_link)
                .await?;
            Ok(HttpResponseCreated(link))
        })
        .await
    }

    async fn system_subnet_pool_silo_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<subnet_pool::SubnetPoolSiloPath>,
        update: TypedBody<subnet_pool::SubnetPoolSiloUpdate>,
    ) -> Result<HttpResponseOk<subnet_pool::SubnetPoolSiloLink>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let update = update.into_inner();
            let link = nexus
                .subnet_pool_silo_update(&opctx, path.pool, path.silo, update)
                .await?;
            Ok(HttpResponseOk(link))
        })
        .await
    }

    async fn system_subnet_pool_silo_unlink(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<subnet_pool::SubnetPoolSiloPath>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            nexus.subnet_pool_silo_unlink(&opctx, path.pool, path.silo).await?;
            Ok(HttpResponseUpdatedNoContent())
        })
        .await
    }

    async fn system_subnet_pool_utilization_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<subnet_pool::SubnetPoolPath>,
    ) -> Result<HttpResponseOk<subnet_pool::SubnetPoolUtilization>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let utilization =
                nexus.subnet_pool_utilization_view(&opctx, &path.pool).await?;
            Ok(HttpResponseOk(utilization))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // External Subnets

    async fn external_subnet_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<project::ProjectSelector>>,
    ) -> Result<
        HttpResponseOk<ResultsPage<external_subnet::ExternalSubnet>>,
        HttpError,
    > {
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
            let subnets = nexus
                .external_subnet_list(&opctx, &project_lookup, &paginated_by)
                .await?;
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

    async fn external_subnet_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::ProjectSelector>,
        subnet_params: TypedBody<external_subnet::ExternalSubnetCreate>,
    ) -> Result<HttpResponseCreated<external_subnet::ExternalSubnet>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let params = subnet_params.into_inner();
            let project_lookup = nexus.project_lookup(&opctx, query)?;
            let subnet = nexus
                .external_subnet_create(&opctx, &project_lookup, params)
                .await?;
            Ok(HttpResponseCreated(subnet))
        })
        .await
    }

    async fn external_subnet_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<external_subnet::ExternalSubnetPath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<external_subnet::ExternalSubnet>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let selector = external_subnet::ExternalSubnetSelector {
                external_subnet: path.external_subnet,
                project: query.project,
            };
            let subnet = nexus.external_subnet_view(&opctx, selector).await?;
            Ok(HttpResponseOk(subnet))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn external_subnet_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<external_subnet::ExternalSubnetPath>,
        query_params: Query<project::OptionalProjectSelector>,
        subnet_params: TypedBody<external_subnet::ExternalSubnetUpdate>,
    ) -> Result<HttpResponseOk<external_subnet::ExternalSubnet>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let params = subnet_params.into_inner();
            let selector = external_subnet::ExternalSubnetSelector {
                external_subnet: path.external_subnet,
                project: query.project,
            };
            let subnet =
                nexus.external_subnet_update(&opctx, selector, params).await?;
            Ok(HttpResponseOk(subnet))
        })
        .await
    }

    async fn external_subnet_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<external_subnet::ExternalSubnetPath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let selector = external_subnet::ExternalSubnetSelector {
                external_subnet: path.external_subnet,
                project: query.project,
            };
            nexus.external_subnet_delete(&opctx, selector).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn external_subnet_attach(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<external_subnet::ExternalSubnetPath>,
        query_params: Query<project::OptionalProjectSelector>,
        attach_params: TypedBody<external_subnet::ExternalSubnetAttach>,
    ) -> Result<HttpResponseAccepted<external_subnet::ExternalSubnet>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let attach = attach_params.into_inner();
            let selector = external_subnet::ExternalSubnetSelector {
                external_subnet: path.external_subnet,
                project: query.project,
            };
            let subnet =
                nexus.external_subnet_attach(&opctx, selector, attach).await?;
            Ok(HttpResponseAccepted(subnet))
        })
        .await
    }

    async fn external_subnet_detach(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<external_subnet::ExternalSubnetPath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseAccepted<external_subnet::ExternalSubnet>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let selector = external_subnet::ExternalSubnetSelector {
                external_subnet: path.external_subnet,
                project: query.project,
            };
            let subnet = nexus.external_subnet_detach(&opctx, selector).await?;
            Ok(HttpResponseAccepted(subnet))
        })
        .await
    }

    // Floating IP Addresses

    async fn floating_ip_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<project::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<floating_ip::FloatingIp>>, HttpError>
    {
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
        query_params: Query<project::ProjectSelector>,
        floating_params: TypedBody<floating_ip::FloatingIpCreate>,
    ) -> Result<HttpResponseCreated<FloatingIp>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let project_selector = query_params.into_inner();
            let floating_params = floating_params.into_inner();
            let project_lookup =
                nexus.project_lookup(&opctx, project_selector)?;
            let ip = nexus
                .floating_ip_create(&opctx, &project_lookup, floating_params)
                .await?;
            Ok(HttpResponseCreated(ip))
        })
        .await
    }

    async fn floating_ip_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::FloatingIpPath>,
        query_params: Query<project::OptionalProjectSelector>,
        updated_floating_ip: TypedBody<floating_ip::FloatingIpUpdate>,
    ) -> Result<HttpResponseOk<FloatingIp>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let updated_floating_ip_params = updated_floating_ip.into_inner();
            let floating_ip_selector = floating_ip::FloatingIpSelector {
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
        })
        .await
    }

    async fn floating_ip_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::FloatingIpPath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let floating_ip_selector = floating_ip::FloatingIpSelector {
                floating_ip: path.floating_ip,
                project: query.project,
            };
            let fip_lookup =
                nexus.floating_ip_lookup(&opctx, floating_ip_selector)?;
            nexus.floating_ip_delete(&opctx, fip_lookup).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn floating_ip_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::FloatingIpPath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<floating_ip::FloatingIp>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let floating_ip_selector = floating_ip::FloatingIpSelector {
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
        path_params: Path<path_params::FloatingIpPath>,
        query_params: Query<project::OptionalProjectSelector>,
        target: TypedBody<floating_ip::FloatingIpAttach>,
    ) -> Result<HttpResponseAccepted<FloatingIp>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let target = target.into_inner();
            let floating_ip_selector = floating_ip::FloatingIpSelector {
                floating_ip: path.floating_ip,
                project: query.project,
            };
            let ip = nexus
                .floating_ip_attach(&opctx, floating_ip_selector, target)
                .await?;
            Ok(HttpResponseAccepted(ip))
        })
        .await
    }

    async fn floating_ip_detach(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::FloatingIpPath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseAccepted<FloatingIp>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let floating_ip_selector = floating_ip::FloatingIpSelector {
                floating_ip: path.floating_ip,
                project: query.project,
            };
            let fip_lookup =
                nexus.floating_ip_lookup(&opctx, floating_ip_selector)?;
            let ip = nexus.floating_ip_detach(&opctx, fip_lookup).await?;
            Ok(HttpResponseAccepted(ip))
        })
        .await
    }

    // Multicast Groups

    async fn multicast_group_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<multicast::MulticastGroup>>, HttpError>
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
            let groups =
                nexus.multicast_groups_list(&opctx, &paginated_by).await?;
            let results_page = ScanByNameOrId::results_page(
                &query,
                groups,
                &marker_for_name_or_id,
            )?;
            Ok(HttpResponseOk(results_page))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn multicast_group_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<multicast::MulticastGroupPath>,
    ) -> Result<HttpResponseOk<multicast::MulticastGroup>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let group_selector = multicast::MulticastGroupSelector {
                multicast_group: path.multicast_group,
            };
            let group = apictx
                .context
                .nexus
                .multicast_group_view(&opctx, &group_selector)
                .await?;
            Ok(HttpResponseOk(group))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Multicast Group Member Management

    async fn multicast_group_member_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<multicast::MulticastGroupPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<
        HttpResponseOk<ResultsPage<multicast::MulticastGroupMember>>,
        HttpError,
    > {
        let apictx = rqctx.context();
        let handler = async {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let group_selector = multicast::MulticastGroupSelector {
                multicast_group: path.multicast_group,
            };
            let group_lookup = apictx
                .context
                .nexus
                .multicast_group_lookup(&opctx, &group_selector)
                .await?;
            let members = apictx
                .context
                .nexus
                .multicast_group_members_list(
                    &opctx,
                    &group_lookup,
                    &pag_params,
                )
                .await?;
            let results = members
                .into_iter()
                .map(multicast::MulticastGroupMember::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                results,
                &|_, member: &multicast::MulticastGroupMember| {
                    member.identity.id
                },
            )?))
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
        query_params: Query<PaginatedByNameOrId<project::ProjectSelector>>,
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
        query_params: Query<project::ProjectSelector>,
        new_disk: TypedBody<disk::DiskCreate>,
    ) -> Result<HttpResponseCreated<Disk>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let params = new_disk.into_inner();
            let project_lookup = nexus.project_lookup(&opctx, query)?;
            let disk = nexus
                .project_create_disk(&opctx, &project_lookup, &params)
                .await?;
            Ok(HttpResponseCreated(disk.into()))
        })
        .await
    }

    async fn disk_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::DiskPath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<Disk>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let disk_selector =
                disk::DiskSelector { disk: path.disk, project: query.project };
            let disk = nexus.disk_get(&opctx, disk_selector).await?;
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
        path_params: Path<path_params::DiskPath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let disk_selector =
                disk::DiskSelector { disk: path.disk, project: query.project };
            let disk_lookup = nexus.disk_lookup(&opctx, disk_selector)?;
            nexus.project_delete_disk(&opctx, &disk_lookup).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn disk_bulk_write_import_start(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::DiskPath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let disk_selector =
                disk::DiskSelector { disk: path.disk, project: query.project };
            let disk_lookup = nexus.disk_lookup(&opctx, disk_selector)?;
            nexus.disk_manual_import_start(&opctx, &disk_lookup).await?;
            Ok(HttpResponseUpdatedNoContent())
        })
        .await
    }

    async fn disk_bulk_write_import(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::DiskPath>,
        query_params: Query<project::OptionalProjectSelector>,
        import_params: TypedBody<disk::ImportBlocksBulkWrite>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let import_params = import_params.into_inner();
            let disk_selector =
                disk::DiskSelector { disk: path.disk, project: query.project };
            let disk_lookup = nexus.disk_lookup(&opctx, disk_selector)?;
            nexus
                .disk_manual_import(&opctx, &disk_lookup, import_params)
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        })
        .await
    }

    async fn disk_bulk_write_import_stop(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::DiskPath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let disk_selector =
                disk::DiskSelector { disk: path.disk, project: query.project };
            let disk_lookup = nexus.disk_lookup(&opctx, disk_selector)?;
            nexus.disk_manual_import_stop(&opctx, &disk_lookup).await?;
            Ok(HttpResponseUpdatedNoContent())
        })
        .await
    }

    async fn disk_finalize_import(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::DiskPath>,
        query_params: Query<project::OptionalProjectSelector>,
        finalize_params: TypedBody<disk::FinalizeDisk>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let finalize_params = finalize_params.into_inner();
            let disk_selector =
                disk::DiskSelector { disk: path.disk, project: query.project };
            let disk_lookup = nexus.disk_lookup(&opctx, disk_selector)?;
            nexus
                .disk_finalize_import(&opctx, &disk_lookup, &finalize_params)
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        })
        .await
    }

    // Instances

    async fn instance_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<project::ProjectSelector>>,
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
        query_params: Query<project::ProjectSelector>,
        new_instance: TypedBody<instance::InstanceCreate>,
    ) -> Result<HttpResponseCreated<Instance>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let project_selector = query_params.into_inner();
            let new_instance_params = new_instance.into_inner();
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
        })
        .await
    }

    async fn instance_view(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<path_params::InstancePath>,
    ) -> Result<HttpResponseOk<Instance>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let instance_selector = instance::InstanceSelector {
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
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<path_params::InstancePath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let instance_selector = instance::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            nexus.project_destroy_instance(&opctx, &instance_lookup).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn instance_update(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<path_params::InstancePath>,
        instance_config: TypedBody<instance::InstanceUpdate>,
    ) -> Result<HttpResponseOk<Instance>, HttpError> {
        let query = query_params.into_inner();
        let path = path_params.into_inner();
        let instance_config = instance_config.into_inner();
        let instance_selector = instance::InstanceSelector {
            project: query.project,
            instance: path.instance,
        };
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let instance = nexus
                .instance_reconfigure(
                    &opctx,
                    &instance_lookup,
                    &instance_config,
                )
                .await?;
            Ok(HttpResponseOk(instance.into()))
        })
        .await
    }

    async fn instance_reboot(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<path_params::InstancePath>,
    ) -> Result<HttpResponseAccepted<Instance>, HttpError> {
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let instance_selector = instance::InstanceSelector {
            project: query.project,
            instance: path.instance,
        };
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let instance =
                nexus.instance_reboot(&opctx, &instance_lookup).await?;
            Ok(HttpResponseAccepted(instance.into()))
        })
        .await
    }

    async fn instance_start(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<path_params::InstancePath>,
    ) -> Result<HttpResponseAccepted<Instance>, HttpError> {
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let instance_selector = instance::InstanceSelector {
            project: query.project,
            instance: path.instance,
        };
        audit_and_time(&rqctx, |opctx, nexus| async move {
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
        })
        .await
    }

    async fn instance_stop(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<path_params::InstancePath>,
    ) -> Result<HttpResponseAccepted<Instance>, HttpError> {
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let instance_selector = instance::InstanceSelector {
            project: query.project,
            instance: path.instance,
        };
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let instance =
                nexus.instance_stop(&opctx, &instance_lookup).await?;
            Ok(HttpResponseAccepted(instance.into()))
        })
        .await
    }

    async fn instance_serial_console(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::InstancePath>,
        query_params: Query<instance::InstanceSerialConsoleRequest>,
    ) -> Result<HttpResponseOk<instance::InstanceSerialConsoleData>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let instance_selector = instance::InstanceSelector {
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
        path_params: Path<path_params::InstancePath>,
        query_params: Query<instance::InstanceSerialConsoleStreamRequest>,
        conn: WebsocketConnection,
    ) -> WebsocketChannelResult {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let instance_selector = instance::InstanceSelector {
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
        path_params: Path<path_params::InstancePath>,
        query_params: Query<
            PaginatedByNameOrId<project::OptionalProjectSelector>,
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
            let instance_selector = instance::InstanceSelector {
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
            PaginatedByNameOrId<project::OptionalProjectSelector>,
        >,
        path_params: Path<path_params::InstancePath>,
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
            let instance_selector = instance::InstanceSelector {
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
        path_params: Path<path_params::InstancePath>,
        query_params: Query<project::OptionalProjectSelector>,
        disk_to_attach: TypedBody<path_params::DiskPath>,
    ) -> Result<HttpResponseAccepted<Disk>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let disk = disk_to_attach.into_inner().disk;
            let instance_selector = instance::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let disk = nexus
                .instance_attach_disk(&opctx, &instance_lookup, disk)
                .await?;
            Ok(HttpResponseAccepted(disk.into()))
        })
        .await
    }

    async fn instance_disk_detach(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::InstancePath>,
        query_params: Query<project::OptionalProjectSelector>,
        disk_to_detach: TypedBody<path_params::DiskPath>,
    ) -> Result<HttpResponseAccepted<Disk>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let disk = disk_to_detach.into_inner().disk;
            let instance_selector = instance::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let disk = nexus
                .instance_detach_disk(&opctx, &instance_lookup, disk)
                .await?;
            Ok(HttpResponseAccepted(disk.into()))
        })
        .await
    }

    async fn instance_affinity_group_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<
            PaginatedByNameOrId<project::OptionalProjectSelector>,
        >,
        path_params: Path<path_params::InstancePath>,
    ) -> Result<HttpResponseOk<ResultsPage<affinity::AffinityGroup>>, HttpError>
    {
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
            let instance_selector = instance::InstanceSelector {
                project: scan_params.selector.project.clone(),
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let groups = nexus
                .instance_list_affinity_groups(
                    &opctx,
                    &instance_lookup,
                    &paginated_by,
                )
                .await?
                .into_iter()
                .map(|g| g.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                groups,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_anti_affinity_group_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<
            PaginatedByNameOrId<project::OptionalProjectSelector>,
        >,
        path_params: Path<path_params::InstancePath>,
    ) -> Result<
        HttpResponseOk<ResultsPage<affinity::AntiAffinityGroup>>,
        HttpError,
    > {
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
            let instance_selector = instance::InstanceSelector {
                project: scan_params.selector.project.clone(),
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let groups = nexus
                .instance_list_anti_affinity_groups(
                    &opctx,
                    &instance_lookup,
                    &paginated_by,
                )
                .await?
                .into_iter()
                .map(|g| g.into())
                .collect();
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                groups,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Affinity Groups

    async fn affinity_group_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<project::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<affinity::AffinityGroup>>, HttpError>
    {
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
            let groups = nexus
                .affinity_group_list(&opctx, &project_lookup, &paginated_by)
                .await?;
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                groups,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn affinity_group_view(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<path_params::AffinityGroupPath>,
    ) -> Result<HttpResponseOk<affinity::AffinityGroup>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let query = query_params.into_inner();

            let group_selector = affinity::AffinityGroupSelector {
                affinity_group: path.affinity_group,
                project: query.project.clone(),
            };

            let (.., group) = nexus
                .affinity_group_lookup(&opctx, group_selector)?
                .fetch()
                .await?;

            Ok(HttpResponseOk(group.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn affinity_group_member_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<
            PaginatedByNameOrId<project::OptionalProjectSelector>,
        >,
        path_params: Path<path_params::AffinityGroupPath>,
    ) -> Result<HttpResponseOk<ResultsPage<AffinityGroupMember>>, HttpError>
    {
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

            let group_selector = affinity::AffinityGroupSelector {
                project: scan_params.selector.project.clone(),
                affinity_group: path.affinity_group,
            };
            let group_lookup =
                nexus.affinity_group_lookup(&opctx, group_selector)?;
            let affinity_group_member_instances = nexus
                .affinity_group_member_list(
                    &opctx,
                    &group_lookup,
                    &paginated_by,
                )
                .await?;
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                affinity_group_member_instances,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn affinity_group_member_instance_view(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<affinity::AffinityInstanceGroupMemberPath>,
    ) -> Result<HttpResponseOk<AffinityGroupMember>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let query = query_params.into_inner();

            // Select group
            let group_selector = affinity::AffinityGroupSelector {
                affinity_group: path.affinity_group,
                project: query.project.clone(),
            };
            let group_lookup =
                nexus.affinity_group_lookup(&opctx, group_selector)?;

            // Select instance
            let instance_selector = instance::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;

            let group = nexus
                .affinity_group_member_view(
                    &opctx,
                    &group_lookup,
                    &instance_lookup,
                )
                .await?;

            Ok(HttpResponseOk(group))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn affinity_group_member_instance_add(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<affinity::AffinityInstanceGroupMemberPath>,
    ) -> Result<HttpResponseCreated<AffinityGroupMember>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let group_selector = affinity::AffinityGroupSelector {
                affinity_group: path.affinity_group,
                project: query.project.clone(),
            };
            let group_lookup =
                nexus.affinity_group_lookup(&opctx, group_selector)?;

            let instance_selector = instance::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;

            let member = nexus
                .affinity_group_member_add(
                    &opctx,
                    &group_lookup,
                    &instance_lookup,
                )
                .await?;
            Ok(HttpResponseCreated(member))
        })
        .await
    }

    async fn affinity_group_member_instance_delete(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<affinity::AffinityInstanceGroupMemberPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let group_selector = affinity::AffinityGroupSelector {
                affinity_group: path.affinity_group,
                project: query.project.clone(),
            };
            let group_lookup =
                nexus.affinity_group_lookup(&opctx, group_selector)?;

            let instance_selector = instance::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            nexus
                .affinity_group_member_delete(
                    &opctx,
                    &group_lookup,
                    &instance_lookup,
                )
                .await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn affinity_group_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::ProjectSelector>,
        new_affinity_group_params: TypedBody<affinity::AffinityGroupCreate>,
    ) -> Result<HttpResponseCreated<affinity::AffinityGroup>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let new_affinity_group = new_affinity_group_params.into_inner();
            let project_lookup = nexus.project_lookup(&opctx, query)?;
            let affinity_group = nexus
                .affinity_group_create(
                    &opctx,
                    &project_lookup,
                    new_affinity_group,
                )
                .await?;
            Ok(HttpResponseCreated(affinity_group))
        })
        .await
    }

    async fn affinity_group_update(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<path_params::AffinityGroupPath>,
        updated_group: TypedBody<affinity::AffinityGroupUpdate>,
    ) -> Result<HttpResponseOk<affinity::AffinityGroup>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let updates = updated_group.into_inner();
            let group_selector = affinity::AffinityGroupSelector {
                project: query.project,
                affinity_group: path.affinity_group,
            };
            let group_lookup =
                nexus.affinity_group_lookup(&opctx, group_selector)?;
            let affinity_group = nexus
                .affinity_group_update(&opctx, &group_lookup, &updates)
                .await?;
            Ok(HttpResponseOk(affinity_group))
        })
        .await
    }

    async fn affinity_group_delete(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<path_params::AffinityGroupPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let group_selector = affinity::AffinityGroupSelector {
                project: query.project,
                affinity_group: path.affinity_group,
            };
            let group_lookup =
                nexus.affinity_group_lookup(&opctx, group_selector)?;
            nexus.affinity_group_delete(&opctx, &group_lookup).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn anti_affinity_group_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<project::ProjectSelector>>,
    ) -> Result<
        HttpResponseOk<ResultsPage<affinity::AntiAffinityGroup>>,
        HttpError,
    > {
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
            let groups = nexus
                .anti_affinity_group_list(
                    &opctx,
                    &project_lookup,
                    &paginated_by,
                )
                .await?;
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                groups,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn anti_affinity_group_view(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<path_params::AntiAffinityGroupPath>,
    ) -> Result<HttpResponseOk<affinity::AntiAffinityGroup>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let query = query_params.into_inner();

            let group_selector = affinity::AntiAffinityGroupSelector {
                anti_affinity_group: path.anti_affinity_group,
                project: query.project.clone(),
            };

            let (.., group) = nexus
                .anti_affinity_group_lookup(&opctx, group_selector)?
                .fetch()
                .await?;

            Ok(HttpResponseOk(group.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn anti_affinity_group_member_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<
            PaginatedByNameOrId<project::OptionalProjectSelector>,
        >,
        path_params: Path<path_params::AntiAffinityGroupPath>,
    ) -> Result<HttpResponseOk<ResultsPage<AntiAffinityGroupMember>>, HttpError>
    {
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

            let group_selector = affinity::AntiAffinityGroupSelector {
                project: scan_params.selector.project.clone(),
                anti_affinity_group: path.anti_affinity_group,
            };
            let group_lookup =
                nexus.anti_affinity_group_lookup(&opctx, group_selector)?;
            let group_members = nexus
                .anti_affinity_group_member_list(
                    &opctx,
                    &group_lookup,
                    &paginated_by,
                )
                .await?;
            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                group_members,
                &marker_for_name_or_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn anti_affinity_group_member_instance_view(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<affinity::AntiAffinityInstanceGroupMemberPath>,
    ) -> Result<HttpResponseOk<AntiAffinityGroupMember>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let query = query_params.into_inner();

            // Select group
            let group_selector = affinity::AntiAffinityGroupSelector {
                anti_affinity_group: path.anti_affinity_group,
                project: query.project.clone(),
            };
            let group_lookup =
                nexus.anti_affinity_group_lookup(&opctx, group_selector)?;

            // Select instance
            let instance_selector = instance::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;

            let group = nexus
                .anti_affinity_group_member_instance_view(
                    &opctx,
                    &group_lookup,
                    &instance_lookup,
                )
                .await?;

            Ok(HttpResponseOk(group))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn anti_affinity_group_member_instance_add(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<affinity::AntiAffinityInstanceGroupMemberPath>,
    ) -> Result<HttpResponseCreated<AntiAffinityGroupMember>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let group_selector = affinity::AntiAffinityGroupSelector {
                anti_affinity_group: path.anti_affinity_group,
                project: query.project.clone(),
            };
            let group_lookup =
                nexus.anti_affinity_group_lookup(&opctx, group_selector)?;

            let instance_selector = instance::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;

            let member = nexus
                .anti_affinity_group_member_instance_add(
                    &opctx,
                    &group_lookup,
                    &instance_lookup,
                )
                .await?;
            Ok(HttpResponseCreated(member))
        })
        .await
    }

    async fn anti_affinity_group_member_instance_delete(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<affinity::AntiAffinityInstanceGroupMemberPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let group_selector = affinity::AntiAffinityGroupSelector {
                anti_affinity_group: path.anti_affinity_group,
                project: query.project.clone(),
            };
            let group_lookup =
                nexus.anti_affinity_group_lookup(&opctx, group_selector)?;

            let instance_selector = instance::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;

            nexus
                .anti_affinity_group_member_instance_delete(
                    &opctx,
                    &group_lookup,
                    &instance_lookup,
                )
                .await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn anti_affinity_group_create(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::ProjectSelector>,
        new_anti_affinity_group_params: TypedBody<
            affinity::AntiAffinityGroupCreate,
        >,
    ) -> Result<HttpResponseCreated<affinity::AntiAffinityGroup>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let new_anti_affinity_group =
                new_anti_affinity_group_params.into_inner();
            let project_lookup = nexus.project_lookup(&opctx, query)?;
            let anti_affinity_group = nexus
                .anti_affinity_group_create(
                    &opctx,
                    &project_lookup,
                    new_anti_affinity_group,
                )
                .await?;
            Ok(HttpResponseCreated(anti_affinity_group))
        })
        .await
    }

    async fn anti_affinity_group_update(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<path_params::AntiAffinityGroupPath>,
        updated_group: TypedBody<affinity::AntiAffinityGroupUpdate>,
    ) -> Result<HttpResponseOk<affinity::AntiAffinityGroup>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let updates = updated_group.into_inner();
            let group_selector = affinity::AntiAffinityGroupSelector {
                project: query.project,
                anti_affinity_group: path.anti_affinity_group,
            };
            let group_lookup =
                nexus.anti_affinity_group_lookup(&opctx, group_selector)?;
            let anti_affinity_group = nexus
                .anti_affinity_group_update(&opctx, &group_lookup, &updates)
                .await?;
            Ok(HttpResponseOk(anti_affinity_group))
        })
        .await
    }

    async fn anti_affinity_group_delete(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<path_params::AntiAffinityGroupPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let group_selector = affinity::AntiAffinityGroupSelector {
                project: query.project,
                anti_affinity_group: path.anti_affinity_group,
            };
            let group_lookup =
                nexus.anti_affinity_group_lookup(&opctx, group_selector)?;
            nexus.anti_affinity_group_delete(&opctx, &group_lookup).await?;
            Ok(HttpResponseDeleted())
        })
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
        new_cert: TypedBody<certificate::CertificateCreate>,
    ) -> Result<HttpResponseCreated<Certificate>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let new_cert_params = new_cert.into_inner();
            let cert =
                nexus.certificate_create(&opctx, new_cert_params).await?;
            Ok(HttpResponseCreated(cert.try_into()?))
        })
        .await
    }

    async fn certificate_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::CertificatePath>,
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
        path_params: Path<path_params::CertificatePath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            nexus
                .certificate_delete(
                    &opctx,
                    nexus.certificate_lookup(&opctx, &path.certificate),
                )
                .await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn networking_address_lot_create(
        rqctx: RequestContext<ApiContext>,
        new_address_lot: TypedBody<networking::AddressLotCreate>,
    ) -> Result<HttpResponseCreated<AddressLotCreateResponse>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let params = new_address_lot.into_inner();
            let result = nexus.address_lot_create(&opctx, params).await?;
            let lot: AddressLot = result.lot.into();
            let blocks: Vec<AddressLotBlock> =
                result.blocks.iter().map(|b| b.clone().into()).collect();
            Ok(HttpResponseCreated(AddressLotCreateResponse { lot, blocks }))
        })
        .await
    }

    async fn networking_address_lot_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::AddressLotPath>,
    ) -> Result<HttpResponseOk<AddressLotViewResponse>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let lookup = nexus.address_lot_lookup(&opctx, path.address_lot)?;
            let (.., lot) = lookup.fetch().await?;
            let blocks = nexus
                .address_lot_block_list(&opctx, &lookup, None)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();
            Ok(HttpResponseOk(AddressLotViewResponse {
                lot: lot.into(),
                blocks,
            }))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_address_lot_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::AddressLotPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let address_lot_lookup =
                nexus.address_lot_lookup(&opctx, path.address_lot)?;
            nexus.address_lot_delete(&opctx, &address_lot_lookup).await?;
            Ok(HttpResponseDeleted())
        })
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
        path_params: Path<path_params::AddressLotPath>,
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
                .address_lot_block_list(
                    &opctx,
                    &address_lot_lookup,
                    Some(&pagparams),
                )
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
        new_loopback_address: TypedBody<networking::LoopbackAddressCreate>,
    ) -> Result<HttpResponseCreated<LoopbackAddress>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let params = new_loopback_address.into_inner();
            let result = nexus.loopback_address_create(&opctx, params).await?;
            let addr: LoopbackAddress = result.into();
            Ok(HttpResponseCreated(addr))
        })
        .await
    }

    async fn networking_loopback_address_delete(
        rqctx: RequestContext<ApiContext>,
        path: Path<networking::LoopbackAddressPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let path = path.into_inner();
        let addr = match IpNetwork::new(path.address, path.subnet_mask) {
            Ok(addr) => Ok(addr),
            Err(_) => Err(HttpError::for_bad_request(
                None,
                "invalid ip address".into(),
            )),
        }?;
        audit_and_time(&rqctx, |opctx, nexus| async move {
            nexus
                .loopback_address_delete(
                    &opctx,
                    path.rack_id,
                    path.switch_location.into(),
                    addr.into(),
                )
                .await?;
            Ok(HttpResponseDeleted())
        })
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
        new_settings: TypedBody<networking::SwitchPortSettingsCreate>,
    ) -> Result<HttpResponseCreated<SwitchPortSettings>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let params = new_settings.into_inner();
            let result =
                nexus.switch_port_settings_post(&opctx, params).await?;
            let settings: SwitchPortSettings = result.into();
            Ok(HttpResponseCreated(settings))
        })
        .await
    }

    async fn networking_switch_port_settings_delete(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<networking::SwitchPortSettingsSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let selector = query_params.into_inner();
            nexus.switch_port_settings_delete(&opctx, &selector).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn networking_switch_port_settings_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<
            PaginatedByNameOrId<networking::SwitchPortSettingsSelector>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<SwitchPortSettingsIdentity>>,
        HttpError,
    > {
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
        path_params: Path<networking::SwitchPortSettingsInfoSelector>,
    ) -> Result<HttpResponseOk<SwitchPortSettings>, HttpError> {
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
        query_params: Query<PaginatedById<networking::SwitchPortPageSelector>>,
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
        path_params: Path<networking::SwitchPortPathSelector>,
        query_params: Query<networking::SwitchPortSelector>,
    ) -> Result<HttpResponseOk<switch::SwitchLinkState>, HttpError> {
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
        path_params: Path<networking::SwitchPortPathSelector>,
        query_params: Query<networking::SwitchPortSelector>,
        settings_body: TypedBody<networking::SwitchPortApplySettings>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let port = path_params.into_inner().port;
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let settings = settings_body.into_inner();
            nexus
                .switch_port_apply_settings(&opctx, &port, &query, &settings)
                .await?;
            Ok(HttpResponseUpdatedNoContent {})
        })
        .await
    }

    async fn networking_switch_port_clear_settings(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<networking::SwitchPortPathSelector>,
        query_params: Query<networking::SwitchPortSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let port = path_params.into_inner().port;
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            nexus.switch_port_clear_settings(&opctx, &port, &query).await?;
            Ok(HttpResponseUpdatedNoContent {})
        })
        .await
    }

    async fn networking_switch_port_lldp_config_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<networking::SwitchPortPathSelector>,
        query_params: Query<networking::SwitchPortSelector>,
    ) -> Result<HttpResponseOk<LldpLinkConfig>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let settings = nexus
                .lldp_config_get(
                    &opctx,
                    query.rack_id,
                    query.switch_location,
                    path.port,
                )
                .await?;
            Ok(HttpResponseOk(settings))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_switch_port_lldp_config_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<networking::SwitchPortPathSelector>,
        query_params: Query<networking::SwitchPortSelector>,
        config: TypedBody<LldpLinkConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let path = path_params.into_inner();
            let config = config.into_inner();
            nexus
                .lldp_config_update(
                    &opctx,
                    query.rack_id,
                    query.switch_location,
                    path.port,
                    config,
                )
                .await?;
            Ok(HttpResponseUpdatedNoContent {})
        })
        .await
    }

    async fn networking_switch_port_lldp_neighbors(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<networking::LldpPortPathSelector>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<LldpNeighbor>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let query = query_params.into_inner();
            let path = path_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let limit = pag_params.limit.into();
            let prev = pag_params.marker.cloned();

            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let neighbors = nexus
                .lldp_neighbors_get(
                    &opctx,
                    &prev,
                    limit,
                    path.rack_id,
                    &path.switch_location,
                    &path.port,
                )
                .await?;

            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                neighbors,
                &marker_for_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_bgp_config_create(
        rqctx: RequestContext<ApiContext>,
        config: TypedBody<networking::BgpConfigCreate>,
    ) -> Result<HttpResponseCreated<BgpConfig>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let config = config.into_inner();
            let result = nexus.bgp_config_create(&opctx, &config).await?;
            Ok(HttpResponseCreated::<BgpConfig>(result.try_into()?))
        })
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
                .map(|p| p.try_into())
                .collect::<Result<Vec<_>, _>>()?;

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

    async fn networking_bgp_exported(
        rqctx: RequestContext<ApiContext>,
    ) -> Result<HttpResponseOk<Vec<BgpExported>>, HttpError> {
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
        query_params: Query<networking::BgpRouteSelector>,
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
        query_params: Query<networking::BgpRouteSelector>,
    ) -> Result<
        HttpResponseOk<Vec<v2025_11_20_00::networking::BgpImportedRouteIpv4>>,
        HttpError,
    > {
        let apictx = rqctx.context();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let handler = async {
            let nexus = &apictx.context.nexus;
            let sel = query_params.into_inner();
            let all_routes = nexus.bgp_imported_routes(&opctx, &sel).await?;
            let result: Vec<v2025_11_20_00::networking::BgpImportedRouteIpv4> =
                all_routes.into_iter().flat_map(|r| r.try_into().ok()).collect();
            Ok(HttpResponseOk(result))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn networking_bgp_imported(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<networking::BgpRouteSelector>,
    ) -> Result<HttpResponseOk<Vec<BgpImported>>, HttpError> {
        let apictx = rqctx.context();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let handler = async {
            let nexus = &apictx.context.nexus;
            let sel = query_params.into_inner();
            let result = nexus.bgp_imported_routes(&opctx, &sel).await?;
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
        sel: Query<networking::BgpConfigSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let sel = sel.into_inner();
            nexus.bgp_config_delete(&opctx, &sel).await?;
            Ok(HttpResponseUpdatedNoContent {})
        })
        .await
    }

    async fn networking_bgp_announce_set_update(
        rqctx: RequestContext<ApiContext>,
        config: TypedBody<networking::BgpAnnounceSetCreate>,
    ) -> Result<HttpResponseOk<BgpAnnounceSet>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let config = config.into_inner();
            let result = nexus.bgp_update_announce_set(&opctx, &config).await?;
            Ok(HttpResponseOk::<BgpAnnounceSet>(result.0.into()))
        })
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
        path_params: Path<networking::BgpAnnounceSetSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let sel = path_params.into_inner();
            nexus.bgp_delete_announce_set(&opctx, &sel).await?;
            Ok(HttpResponseUpdatedNoContent {})
        })
        .await
    }

    async fn networking_bgp_announcement_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<networking::BgpAnnounceSetSelector>,
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
        session: TypedBody<networking::BfdSessionEnable>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let session = session.into_inner();
            opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
            nexus.bfd_enable(&opctx, session).await?;
            Ok(HttpResponseUpdatedNoContent {})
        })
        .await
    }

    async fn networking_bfd_disable(
        rqctx: RequestContext<ApiContext>,
        session: TypedBody<networking::BfdSessionDisable>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let session = session.into_inner();
            opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
            nexus.bfd_disable(&opctx, session).await?;
            Ok(HttpResponseUpdatedNoContent {})
        })
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
    ) -> Result<HttpResponseOk<system::AllowList>, HttpError> {
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
        params: TypedBody<system::AllowListUpdate>,
    ) -> Result<HttpResponseOk<system::AllowList>, HttpError> {
        let server_kind = rqctx.context().kind;
        let remote_addr = rqctx.request.remote_addr().ip();
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let params = params.into_inner();
            nexus
                .allow_list_upsert(&opctx, remote_addr, server_kind, params)
                .await
                .map(HttpResponseOk)
                .map_err(HttpError::from)
        })
        .await
    }

    async fn networking_inbound_icmp_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ServiceIcmpConfig>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            nexus
                .nexus_firewall_inbound_icmp_view(&opctx)
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

    async fn networking_inbound_icmp_update(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<ServiceIcmpConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let params = params.into_inner();
            nexus
                .nexus_firewall_inbound_icmp_update(&opctx, params)
                .await
                .map(|_| HttpResponseUpdatedNoContent())
                .map_err(HttpError::from)
        })
        .await
    }

    // Images

    async fn image_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<
            PaginatedByNameOrId<project::OptionalProjectSelector>,
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
                        project::ProjectSelector { project },
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
        query_params: Query<project::OptionalProjectSelector>,
        new_image: TypedBody<image::ImageCreate>,
    ) -> Result<HttpResponseCreated<Image>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let params = new_image.into_inner();
            let parent_lookup = match query.project.clone() {
                Some(project) => {
                    let project_lookup = nexus.project_lookup(
                        &opctx,
                        project::ProjectSelector { project },
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
        })
        .await
    }

    async fn image_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::ImagePath>,
        query_params: Query<project::OptionalProjectSelector>,
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
                    image::ImageSelector {
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
        path_params: Path<path_params::ImagePath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let image_lookup = nexus
                .image_lookup(
                    &opctx,
                    image::ImageSelector {
                        image: path.image,
                        project: query.project,
                    },
                )
                .await?;
            nexus.image_delete(&opctx, &image_lookup).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn image_promote(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::ImagePath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseAccepted<Image>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let image_lookup = nexus
                .image_lookup(
                    &opctx,
                    image::ImageSelector {
                        image: path.image,
                        project: query.project,
                    },
                )
                .await?;
            let image = nexus.image_promote(&opctx, &image_lookup).await?;
            Ok(HttpResponseAccepted(image.into()))
        })
        .await
    }

    async fn image_demote(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::ImagePath>,
        query_params: Query<project::ProjectSelector>,
    ) -> Result<HttpResponseAccepted<Image>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let image_lookup = nexus
                .image_lookup(
                    &opctx,
                    image::ImageSelector { image: path.image, project: None },
                )
                .await?;
            let project_lookup = nexus.project_lookup(&opctx, query)?;
            let image = nexus
                .image_demote(&opctx, &image_lookup, &project_lookup)
                .await?;
            Ok(HttpResponseAccepted(image.into()))
        })
        .await
    }

    async fn instance_network_interface_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<instance::InstanceSelector>>,
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
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?;
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
        query_params: Query<instance::InstanceSelector>,
        interface_params: TypedBody<instance::InstanceNetworkInterfaceCreate>,
    ) -> Result<HttpResponseCreated<InstanceNetworkInterface>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let interface_params = interface_params.into_inner();
            let instance_lookup = nexus.instance_lookup(&opctx, query)?;
            let iface = nexus
                .network_interface_create(
                    &opctx,
                    &instance_lookup,
                    &interface_params,
                )
                .await?;
            iface.try_into().map(HttpResponseCreated).map_err(HttpError::from)
        })
        .await
    }

    async fn instance_network_interface_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::NetworkInterfacePath>,
        query_params: Query<instance::OptionalInstanceSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let interface_selector =
                instance::InstanceNetworkInterfaceSelector {
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
        })
        .await
    }

    async fn instance_network_interface_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::NetworkInterfacePath>,
        query_params: Query<instance::OptionalInstanceSelector>,
    ) -> Result<HttpResponseOk<InstanceNetworkInterface>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let interface_selector =
                instance::InstanceNetworkInterfaceSelector {
                    project: query.project,
                    instance: query.instance,
                    network_interface: path.interface,
                };
            let (.., interface) = nexus
                .instance_network_interface_lookup(&opctx, interface_selector)?
                .fetch()
                .await?;
            interface.try_into().map(HttpResponseOk).map_err(HttpError::from)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_network_interface_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::NetworkInterfacePath>,
        query_params: Query<instance::OptionalInstanceSelector>,
        updated_iface: TypedBody<instance::InstanceNetworkInterfaceUpdate>,
    ) -> Result<HttpResponseOk<InstanceNetworkInterface>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let updated_iface = updated_iface.into_inner();
            let network_interface_selector =
                instance::InstanceNetworkInterfaceSelector {
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
            interface.try_into().map(HttpResponseOk).map_err(HttpError::from)
        })
        .await
    }

    // External IP addresses for instances

    async fn instance_external_ip_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<path_params::InstancePath>,
    ) -> Result<HttpResponseOk<ResultsPage<external_ip::ExternalIp>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let instance_selector = instance::InstanceSelector {
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
        path_params: Path<path_params::InstancePath>,
        query_params: Query<project::OptionalProjectSelector>,
        ip_to_create: TypedBody<instance::EphemeralIpCreate>,
    ) -> Result<HttpResponseAccepted<external_ip::ExternalIp>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let instance::EphemeralIpCreate { pool_selector } =
                ip_to_create.into_inner();
            let instance_selector = instance::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let (pool, ip_version) = match pool_selector {
                ip_pool::PoolSelector::Explicit { pool } => (Some(pool), None),
                ip_pool::PoolSelector::Auto { ip_version } => {
                    (None, ip_version)
                }
            };
            let ip = nexus
                .instance_attach_ephemeral_ip(
                    &opctx,
                    &instance_lookup,
                    pool,
                    ip_version,
                )
                .await?;
            Ok(HttpResponseAccepted(ip))
        })
        .await
    }

    async fn instance_ephemeral_ip_detach(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::InstancePath>,
        query_params: Query<instance::EphemeralIpDetachSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let instance_selector = instance::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            nexus
                .instance_detach_external_ip(
                    &opctx,
                    &instance_lookup,
                    &instance::ExternalIpDetach::Ephemeral {
                        ip_version: query.ip_version,
                    },
                )
                .await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    // Instance External Subnets

    async fn instance_external_subnet_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::OptionalProjectSelector>,
        path_params: Path<path_params::InstancePath>,
    ) -> Result<
        HttpResponseOk<ResultsPage<external_subnet::ExternalSubnet>>,
        HttpError,
    > {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let instance_selector = instance::InstanceSelector {
                project: query.project,
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let subnets = nexus
                .instance_list_external_subnets(&opctx, &instance_lookup)
                .await?;
            // The number of external subnets attached to an instance is capped
            // by MAX_ATTACHED_SUBNETS_PER_INSTANCE, so pagination is not
            // implemented.
            Ok(HttpResponseOk(ResultsPage { items: subnets, next_page: None }))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Instance Multicast Groups

    async fn instance_multicast_group_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedById<project::OptionalProjectSelector>>,
        path_params: Path<path_params::InstancePath>,
    ) -> Result<
        HttpResponseOk<ResultsPage<multicast::MulticastGroupMember>>,
        HttpError,
    > {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanById::from_query(&query)?;

            // Note: When instance is specified by UUID, project should be `None`
            // (UUIDs are globally unique). Project is only needed for name-based lookup.
            let instance_selector = instance::InstanceSelector {
                project: match &path.instance {
                    NameOrId::Name(_) => scan_params.selector.project.clone(),
                    NameOrId::Id(_) => None,
                },
                instance: path.instance,
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let members = nexus
                .instance_list_multicast_groups(
                    &opctx,
                    &instance_lookup,
                    &pag_params,
                )
                .await?;
            let results = members
                .into_iter()
                .map(multicast::MulticastGroupMember::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                results,
                &|_, member: &multicast::MulticastGroupMember| {
                    member.identity.id
                },
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn instance_multicast_group_join(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<multicast::InstanceMulticastGroupPath>,
        query_params: Query<project::OptionalProjectSelector>,
        body_params: TypedBody<multicast::InstanceMulticastGroupJoin>,
    ) -> Result<HttpResponseCreated<multicast::MulticastGroupMember>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let body = body_params.into_inner();
            let instance_selector = instance::InstanceSelector {
                project: match &path.instance {
                    NameOrId::Name(_) => query.project.clone(),
                    NameOrId::Id(_) => None,
                },
                instance: path.instance.clone(),
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let result = nexus
                .instance_join_multicast_group(
                    &opctx,
                    &path.multicast_group,
                    &instance_lookup,
                    body.source_ips.as_deref(),
                    body.ip_version,
                )
                .await?;
            Ok(HttpResponseCreated(multicast::MulticastGroupMember::try_from(
                result,
            )?))
        })
        .await
    }

    async fn instance_multicast_group_leave(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<multicast::InstanceMulticastGroupPath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let instance_selector = instance::InstanceSelector {
                project: match &path.instance {
                    NameOrId::Name(_) => query.project.clone(),
                    NameOrId::Id(_) => None,
                },
                instance: path.instance.clone(),
            };
            let instance_lookup =
                nexus.instance_lookup(&opctx, instance_selector)?;
            let group_selector = multicast::MulticastGroupSelector {
                multicast_group: path.multicast_group,
            };
            let group_lookup =
                nexus.multicast_group_lookup(&opctx, &group_selector).await?;
            nexus
                .instance_leave_multicast_group(
                    &opctx,
                    &group_lookup,
                    &instance_lookup,
                )
                .await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    // Cannot delegate to lib.rs: old API version has no body parameter, but the
    // new `instance_multicast_group_join` requires `TypedBody<InstanceMulticastGroupJoin>`.
    // TypedBody has no public constructor, so we can't create a default body for delegation.
    async fn instance_multicast_group_join_v2025_11_20_00(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<
            v2025_11_20_00::multicast::InstanceMulticastGroupPath,
        >,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<
        HttpResponseCreated<v2025_11_20_00::multicast::MulticastGroupMember>,
        HttpError,
    > {
        let apictx = rqctx.context();
        let handler = async {
            let path: multicast::InstanceMulticastGroupPath =
                path_params.into_inner().into();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let instance_selector = instance::InstanceSelector {
                project: match &path.instance {
                    NameOrId::Name(_) => query.project.clone(),
                    NameOrId::Id(_) => None,
                },
                instance: path.instance.clone(),
            };
            let instance_lookup = apictx
                .context
                .nexus
                .instance_lookup(&opctx, instance_selector)?;
            let result = apictx
                .context
                .nexus
                .instance_join_multicast_group(
                    &opctx,
                    &path.multicast_group,
                    &instance_lookup,
                    None, // Old API version doesn't support source_ips
                    None, // Old API version doesn't support ip_version
                )
                .await?;
            let member = multicast::MulticastGroupMember::try_from(result)?;
            Ok(HttpResponseCreated(member.into()))
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
        query_params: Query<PaginatedByNameOrId<project::ProjectSelector>>,
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
        query_params: Query<project::ProjectSelector>,
        new_snapshot: TypedBody<snapshot::SnapshotCreate>,
    ) -> Result<HttpResponseCreated<Snapshot>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let new_snapshot_params = new_snapshot.into_inner();
            let project_lookup = nexus.project_lookup(&opctx, query)?;
            let snapshot = nexus
                .snapshot_create(&opctx, project_lookup, &new_snapshot_params)
                .await?;
            Ok(HttpResponseCreated(snapshot.into()))
        })
        .await
    }

    async fn snapshot_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::SnapshotPath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<Snapshot>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let snapshot_selector = snapshot::SnapshotSelector {
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
        path_params: Path<path_params::SnapshotPath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let snapshot_selector = snapshot::SnapshotSelector {
                project: query.project,
                snapshot: path.snapshot,
            };
            let snapshot_lookup =
                nexus.snapshot_lookup(&opctx, snapshot_selector)?;
            nexus.snapshot_delete(&opctx, &snapshot_lookup).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    // VPCs

    async fn vpc_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<project::ProjectSelector>>,
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
        query_params: Query<project::ProjectSelector>,
        body: TypedBody<vpc::VpcCreate>,
    ) -> Result<HttpResponseCreated<Vpc>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let new_vpc_params = body.into_inner();
            let project_lookup = nexus.project_lookup(&opctx, query)?;
            let vpc = nexus
                .project_create_vpc(&opctx, &project_lookup, &new_vpc_params)
                .await?;
            Ok(HttpResponseCreated(vpc.into()))
        })
        .await
    }

    async fn vpc_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::VpcPath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<Vpc>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let vpc_selector =
                vpc::VpcSelector { project: query.project, vpc: path.vpc };
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
        path_params: Path<path_params::VpcPath>,
        query_params: Query<project::OptionalProjectSelector>,
        updated_vpc: TypedBody<vpc::VpcUpdate>,
    ) -> Result<HttpResponseOk<Vpc>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let updated_vpc_params = updated_vpc.into_inner();
            let vpc_selector =
                vpc::VpcSelector { project: query.project, vpc: path.vpc };
            let vpc_lookup = nexus.vpc_lookup(&opctx, vpc_selector)?;
            let vpc = nexus
                .project_update_vpc(&opctx, &vpc_lookup, &updated_vpc_params)
                .await?;
            Ok(HttpResponseOk(vpc.into()))
        })
        .await
    }

    async fn vpc_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::VpcPath>,
        query_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let vpc_selector =
                vpc::VpcSelector { project: query.project, vpc: path.vpc };
            let vpc_lookup = nexus.vpc_lookup(&opctx, vpc_selector)?;
            nexus.project_delete_vpc(&opctx, &vpc_lookup).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn vpc_subnet_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<vpc::VpcSelector>>,
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
        query_params: Query<vpc::VpcSelector>,
        create_params: TypedBody<vpc::VpcSubnetCreate>,
    ) -> Result<HttpResponseCreated<VpcSubnet>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let create = create_params.into_inner();
            let vpc_lookup = nexus.vpc_lookup(&opctx, query)?;
            let subnet =
                nexus.vpc_create_subnet(&opctx, &vpc_lookup, &create).await?;
            Ok(HttpResponseCreated(subnet.into()))
        })
        .await
    }

    async fn vpc_subnet_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::SubnetPath>,
        query_params: Query<vpc::OptionalVpcSelector>,
    ) -> Result<HttpResponseOk<VpcSubnet>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let subnet_selector = vpc::SubnetSelector {
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
        path_params: Path<path_params::SubnetPath>,
        query_params: Query<vpc::OptionalVpcSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let subnet_selector = vpc::SubnetSelector {
                project: query.project,
                vpc: query.vpc,
                subnet: path.subnet,
            };
            let subnet_lookup =
                nexus.vpc_subnet_lookup(&opctx, subnet_selector)?;
            nexus.vpc_delete_subnet(&opctx, &subnet_lookup).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn vpc_subnet_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::SubnetPath>,
        query_params: Query<vpc::OptionalVpcSelector>,
        subnet_params: TypedBody<vpc::VpcSubnetUpdate>,
    ) -> Result<HttpResponseOk<VpcSubnet>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let subnet_params = subnet_params.into_inner();
            let subnet_selector = vpc::SubnetSelector {
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
        })
        .await
    }

    // This endpoint is likely temporary. We would rather list all IPs allocated in
    // a subnet whether they come from NICs or something else. See
    // https://github.com/oxidecomputer/omicron/issues/2476

    async fn vpc_subnet_list_network_interfaces(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::SubnetPath>,
        query_params: Query<PaginatedByNameOrId<vpc::OptionalVpcSelector>>,
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
            let subnet_selector = vpc::SubnetSelector {
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
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?;
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
        query_params: Query<vpc::VpcSelector>,
    ) -> Result<HttpResponseOk<VpcFirewallRules>, HttpError> {
        // TODO: Check If-Match and fail if the ETag doesn't match anymore.
        // Without this check, if firewall rules change while someone is listing
        // the rules, they will see a mix of the old and new rules.
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let query = query_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
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
        query_params: Query<vpc::VpcSelector>,
        router_params: TypedBody<VpcFirewallRuleUpdateParams>,
    ) -> Result<HttpResponseOk<VpcFirewallRules>, HttpError> {
        // TODO: Check If-Match and fail if the ETag doesn't match anymore.
        // TODO: limit size of the ruleset because the GET endpoint is not paginated
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let router_params = router_params.into_inner();
            let vpc_lookup = nexus.vpc_lookup(&opctx, query)?;
            let rules = nexus
                .vpc_update_firewall_rules(&opctx, &vpc_lookup, &router_params)
                .await?;
            Ok(HttpResponseOk(VpcFirewallRules {
                rules: rules.into_iter().map(|rule| rule.into()).collect(),
            }))
        })
        .await
    }

    // VPC Routers

    async fn vpc_router_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<vpc::VpcSelector>>,
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
        path_params: Path<path_params::RouterPath>,
        query_params: Query<vpc::OptionalVpcSelector>,
    ) -> Result<HttpResponseOk<VpcRouter>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let router_selector = vpc::RouterSelector {
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
        query_params: Query<vpc::VpcSelector>,
        create_params: TypedBody<vpc::VpcRouterCreate>,
    ) -> Result<HttpResponseCreated<VpcRouter>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let create = create_params.into_inner();
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
        })
        .await
    }

    async fn vpc_router_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::RouterPath>,
        query_params: Query<vpc::OptionalVpcSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let router_selector = vpc::RouterSelector {
                project: query.project,
                vpc: query.vpc,
                router: path.router,
            };
            let router_lookup =
                nexus.vpc_router_lookup(&opctx, router_selector)?;
            nexus.vpc_delete_router(&opctx, &router_lookup).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn vpc_router_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::RouterPath>,
        query_params: Query<vpc::OptionalVpcSelector>,
        router_params: TypedBody<vpc::VpcRouterUpdate>,
    ) -> Result<HttpResponseOk<VpcRouter>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let router_params = router_params.into_inner();
            let router_selector = vpc::RouterSelector {
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
        })
        .await
    }

    async fn vpc_router_route_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<vpc::RouterSelector>>,
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
        path_params: Path<path_params::RoutePath>,
        query_params: Query<vpc::OptionalRouterSelector>,
    ) -> Result<HttpResponseOk<RouterRoute>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let route_selector = vpc::RouteSelector {
                project: query.project,
                vpc: query.vpc,
                router: query.router,
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
        query_params: Query<vpc::RouterSelector>,
        create_params: TypedBody<vpc::RouterRouteCreate>,
    ) -> Result<HttpResponseCreated<RouterRoute>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
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
        })
        .await
    }

    async fn vpc_router_route_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::RoutePath>,
        query_params: Query<vpc::OptionalRouterSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let route_selector = vpc::RouteSelector {
                project: query.project,
                vpc: query.vpc,
                router: query.router,
                route: path.route,
            };
            let route_lookup =
                nexus.vpc_router_route_lookup(&opctx, route_selector)?;
            nexus.router_delete_route(&opctx, &route_lookup).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn vpc_router_route_update(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::RoutePath>,
        query_params: Query<vpc::OptionalRouterSelector>,
        router_params: TypedBody<vpc::RouterRouteUpdate>,
    ) -> Result<HttpResponseOk<RouterRoute>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let router_params = router_params.into_inner();
            let route_selector = vpc::RouteSelector {
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
        })
        .await
    }

    // Internet gateways

    /// List internet gateways
    async fn internet_gateway_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<vpc::VpcSelector>>,
    ) -> Result<
        HttpResponseOk<ResultsPage<internet_gateway::InternetGateway>>,
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
        path_params: Path<path_params::InternetGatewayPath>,
        query_params: Query<vpc::OptionalVpcSelector>,
    ) -> Result<HttpResponseOk<internet_gateway::InternetGateway>, HttpError>
    {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let selector = internet_gateway::InternetGatewaySelector {
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
        query_params: Query<vpc::VpcSelector>,
        create_params: TypedBody<internet_gateway::InternetGatewayCreate>,
    ) -> Result<HttpResponseCreated<internet_gateway::InternetGateway>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let create = create_params.into_inner();
            let vpc_lookup = nexus.vpc_lookup(&opctx, query)?;
            let result = nexus
                .internet_gateway_create(&opctx, &vpc_lookup, &create)
                .await?;
            Ok(HttpResponseCreated(result.into()))
        })
        .await
    }

    /// Delete internet gateway
    async fn internet_gateway_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::InternetGatewayPath>,
        query_params: Query<internet_gateway::InternetGatewayDeleteSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let selector = internet_gateway::InternetGatewaySelector {
                project: query.project,
                vpc: query.vpc,
                gateway: path.gateway,
            };
            let lookup = nexus.internet_gateway_lookup(&opctx, selector)?;
            nexus
                .internet_gateway_delete(&opctx, &lookup, query.cascade)
                .await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    /// List IP pools attached to an internet gateway.
    async fn internet_gateway_ip_pool_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<
            PaginatedByNameOrId<internet_gateway::InternetGatewaySelector>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<internet_gateway::InternetGatewayIpPool>>,
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
        query_params: Query<internet_gateway::InternetGatewaySelector>,
        create_params: TypedBody<internet_gateway::InternetGatewayIpPoolCreate>,
    ) -> Result<
        HttpResponseCreated<internet_gateway::InternetGatewayIpPool>,
        HttpError,
    > {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let create = create_params.into_inner();
            let lookup = nexus.internet_gateway_lookup(&opctx, query)?;
            let result = nexus
                .internet_gateway_ip_pool_attach(&opctx, &lookup, &create)
                .await?;
            Ok(HttpResponseCreated(result.into()))
        })
        .await
    }

    /// Detach an IP pool from an internet gateway
    async fn internet_gateway_ip_pool_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::IpPoolPath>,
        query_params: Query<
            internet_gateway::DeleteInternetGatewayElementSelector,
        >,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let selector = internet_gateway::InternetGatewayIpPoolSelector {
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
        })
        .await
    }

    /// List addresses attached to an internet gateway.
    async fn internet_gateway_ip_address_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<
            PaginatedByNameOrId<internet_gateway::InternetGatewaySelector>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<internet_gateway::InternetGatewayIpAddress>>,
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
        query_params: Query<internet_gateway::InternetGatewaySelector>,
        create_params: TypedBody<
            internet_gateway::InternetGatewayIpAddressCreate,
        >,
    ) -> Result<
        HttpResponseCreated<internet_gateway::InternetGatewayIpAddress>,
        HttpError,
    > {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query_params.into_inner();
            let create = create_params.into_inner();
            let lookup = nexus.internet_gateway_lookup(&opctx, query)?;
            let route = nexus
                .internet_gateway_ip_address_attach(&opctx, &lookup, &create)
                .await?;
            Ok(HttpResponseCreated(route.into()))
        })
        .await
    }

    /// Detach an IP address from an internet gateway
    async fn internet_gateway_ip_address_delete(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::IpAddressPath>,
        query_params: Query<
            internet_gateway::DeleteInternetGatewayElementSelector,
        >,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let selector = internet_gateway::InternetGatewayIpAddressSelector {
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
        })
        .await
    }

    //
    // Trust Quorum
    //

    async fn rack_membership_add_sleds(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<path_params::RackPath>,
        req: TypedBody<rack::RackMembershipAddSledsRequest>,
    ) -> Result<HttpResponseOk<RackMembershipStatus>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let req = req.into_inner();
            let rack_id =
                RackUuid::from_untyped_uuid(path_params.into_inner().rack_id);
            let status =
                nexus.tq_add_sleds(&opctx, rack_id, req.sled_ids).await?;
            Ok(HttpResponseOk(status.into()))
        })
        .await
    }

    async fn rack_membership_abort(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<path_params::RackPath>,
    ) -> Result<HttpResponseOk<RackMembershipStatus>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let rack_id =
                RackUuid::from_untyped_uuid(path_params.into_inner().rack_id);
            let status = nexus.tq_abort_latest_config(&opctx, rack_id).await?;
            Ok(HttpResponseOk(status.into()))
        })
        .await
    }

    async fn rack_membership_status(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<rack::RackMembershipConfigPathParams>,
        query_params: Query<rack::RackMembershipVersionParam>,
    ) -> Result<HttpResponseOk<RackMembershipStatus>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path_params = path_params.into_inner();
        let rack_id = RackUuid::from_untyped_uuid(path_params.rack_id);
        let version = query_params.into_inner().version;
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let status = if let Some(version) = version {
                let epoch = Epoch(version.0);
                nexus.datastore().tq_get_config(&opctx, rack_id, epoch).await?
            } else {
                nexus.datastore().tq_get_latest_config(&opctx, rack_id).await?
            };
            if let Some(status) = status {
                Ok(HttpResponseOk(status.into()))
            } else {
                Err(Error::non_resourcetype_not_found(
                    "could not find rack membership",
                ))?
            }
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
        path_params: Path<path_params::RackPath>,
    ) -> Result<HttpResponseOk<Rack>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let handler = async {
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
    ) -> Result<
        HttpResponseOk<ResultsPage<hardware::UninitializedSled>>,
        HttpError,
    > {
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
        sled: TypedBody<hardware::UninitializedSledId>,
    ) -> Result<HttpResponseCreated<sled::SledId>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let sled = sled.into_inner();
            let id = nexus.sled_add(&opctx, sled).await?;
            Ok(HttpResponseCreated(sled::SledId { id }))
        })
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
        path_params: Path<path_params::SledPath>,
    ) -> Result<HttpResponseOk<Sled>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let handler = async {
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
        path_params: Path<path_params::SledPath>,
        new_provision_state: TypedBody<sled::SledProvisionPolicyParams>,
    ) -> Result<HttpResponseOk<sled::SledProvisionPolicyResponse>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let new_state = new_provision_state.into_inner().state;
            let sled_lookup = nexus.sled_lookup(&opctx, &path.sled_id)?;
            let old_state = nexus
                .sled_set_provision_policy(&opctx, &sled_lookup, new_state)
                .await?;
            let response =
                sled::SledProvisionPolicyResponse { old_state, new_state };
            Ok(HttpResponseOk(response))
        })
        .await
    }

    async fn sled_instance_list(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::SledPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<sled::SledInstance>>, HttpError>
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
                &|_, sled_instance: &sled::SledInstance| {
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
        path_params: Path<path_params::PhysicalDiskPath>,
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
    ) -> Result<HttpResponseOk<ResultsPage<switch::Switch>>, HttpError> {
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
                &|_, switch: &switch::Switch| switch.identity.id,
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
        path_params: Path<path_params::SwitchPath>,
    ) -> Result<HttpResponseOk<switch::Switch>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let path = path_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let (.., switch) = nexus
                .switch_lookup(
                    &opctx,
                    sled::SwitchSelector { switch: path.switch_id },
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
        path_params: Path<path_params::SledPath>,
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
            PaginationParams<
                metrics::ResourceMetrics,
                metrics::ResourceMetrics,
            >,
        >,
        other_params: Query<silo::OptionalSiloSelector>,
    ) -> Result<HttpResponseOk<ResultsPage<oximeter_db::Measurement>>, HttpError>
    {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let pagination = pag_params.into_inner();
        let limit = rqctx.page_limit(&pagination)?;
        let metric_name = path_params.into_inner().metric_name;
        let silo_selector = other_params.into_inner().silo;
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let silo_lookup = match silo_selector {
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
            PaginationParams<
                metrics::ResourceMetrics,
                metrics::ResourceMetrics,
            >,
        >,
        other_params: Query<project::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<ResultsPage<oximeter_db::Measurement>>, HttpError>
    {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let pagination = pag_params.into_inner();
        let limit = rqctx.page_limit(&pagination)?;
        let metric_name = path_params.into_inner().metric_name;
        let project_selector = other_params.into_inner().project;
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project_lookup = match project_selector {
                Some(project) => {
                    let project_selector = project::ProjectSelector { project };
                    Some(nexus.project_lookup(&opctx, project_selector)?)
                }
                _ => None,
            };

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

    async fn system_timeseries_schema_list(
        rqctx: RequestContext<ApiContext>,
        pag_params: Query<TimeseriesSchemaPaginationParams>,
    ) -> Result<
        HttpResponseOk<ResultsPage<oximeter_db::TimeseriesSchema>>,
        HttpError,
    > {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let pagination = pag_params.into_inner();
        let limit = rqctx.page_limit(&pagination)?;
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
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

    async fn system_timeseries_query(
        rqctx: RequestContext<ApiContext>,
        body: TypedBody<timeseries::TimeseriesQuery>,
    ) -> Result<HttpResponseOk<oxql::OxqlQueryResult>, HttpError> {
        // Not audited: this is a read-only query that uses POST only because
        // the query is too large to fit in a URL.
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let body_params = body.into_inner();
            let query = body_params.query;
            let include_summaries = body_params.include_summaries;
            nexus
                .timeseries_query(&opctx, &query)
                .await
                .map(|result| {
                    HttpResponseOk(oxql::OxqlQueryResult {
                        tables: result
                            .tables
                            .into_iter()
                            .map(Into::into)
                            .collect(),
                        query_summaries: include_summaries.then(|| {
                            result
                                .query_summaries
                                .into_iter()
                                .map(Into::into)
                                .collect()
                        }),
                    })
                })
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
        query_params: Query<project::ProjectSelector>,
        body: TypedBody<timeseries::TimeseriesQuery>,
    ) -> Result<HttpResponseOk<oxql::OxqlQueryResult>, HttpError> {
        // Not audited: this is a read-only query that uses POST only because
        // the query is too large to fit in a URL.
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let project_selector = query_params.into_inner();
            let body_params = body.into_inner();
            let query = body_params.query;
            let include_summaries = body_params.include_summaries;
            let project_lookup =
                nexus.project_lookup(&opctx, project_selector)?;
            nexus
                .timeseries_query_project(&opctx, &project_lookup, &query)
                .await
                .map(|result| {
                    HttpResponseOk(oxql::OxqlQueryResult {
                        tables: result
                            .tables
                            .into_iter()
                            .map(Into::into)
                            .collect(),
                        query_summaries: include_summaries.then(|| {
                            result
                                .query_summaries
                                .into_iter()
                                .map(Into::into)
                                .collect()
                        }),
                    })
                })
                .map_err(HttpError::from)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Updates

    async fn system_update_repository_upload(
        rqctx: RequestContext<ApiContext>,
        query: Query<update::UpdatesPutRepositoryParams>,
        body: StreamingBody,
    ) -> Result<HttpResponseOk<update::TufRepoUpload>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let query = query.into_inner();
            let body = body.into_stream();
            let update = nexus
                .updates_put_repository(&opctx, body, query.file_name)
                .await?;
            Ok(HttpResponseOk(update.into()))
        })
        .await
    }

    async fn system_update_repository_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<update::UpdatesGetRepositoryParams>,
    ) -> Result<HttpResponseOk<update::TufRepo>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let params = path_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let repo = nexus
                .updates_get_repository(&opctx, params.system_version)
                .await?;
            Ok(HttpResponseOk(repo.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn system_update_repository_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByVersion>,
    ) -> Result<HttpResponseOk<ResultsPage<update::TufRepo>>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let query = query_params.into_inner();
            let pagparams = data_page_params_for(&rqctx, &query)?;
            let repos =
                nexus.updates_list_repositories(&opctx, &pagparams).await?;

            let responses: Vec<update::TufRepo> =
                repos.into_iter().map(Into::into).collect();

            Ok(HttpResponseOk(ScanByVersion::results_page(
                &query,
                responses,
                &|_scan_params, item: &update::TufRepo| {
                    item.system_version.clone()
                },
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn system_update_trust_root_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<update::UpdatesTrustRoot>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;

            let query = query_params.into_inner();
            let pagparams = data_page_params_for(&rqctx, &query)?;

            let trust_roots = nexus
                .updates_list_trust_roots(&opctx, &pagparams)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();

            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                trust_roots,
                &|_, trust_root: &update::UpdatesTrustRoot| {
                    trust_root.id.into_untyped_uuid()
                },
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn system_update_trust_root_create(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<update::TufSignedRootRole>,
    ) -> Result<HttpResponseCreated<update::UpdatesTrustRoot>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let body = body.into_inner();
            Ok(HttpResponseCreated(
                nexus.updates_add_trust_root(&opctx, body).await?.into(),
            ))
        })
        .await
    }

    async fn system_update_trust_root_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<path_params::TufTrustRootPath>,
    ) -> Result<HttpResponseOk<update::UpdatesTrustRoot>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;

            let id = TufTrustRootUuid::from_untyped_uuid(
                path_params.into_inner().trust_root_id,
            );

            Ok(HttpResponseOk(
                nexus.updates_get_trust_root(&opctx, id).await?.into(),
            ))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn system_update_trust_root_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<path_params::TufTrustRootPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let id = TufTrustRootUuid::from_untyped_uuid(
            path_params.into_inner().trust_root_id,
        );
        audit_and_time(&rqctx, |opctx, nexus| async move {
            nexus.updates_delete_trust_root(&opctx, id).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn target_release_update(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<update::SetTargetReleaseParams>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            nexus
                .target_release_update(
                    &opctx,
                    body.into_inner(),
                    SetTargetReleaseIntent::Update,
                )
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        })
        .await
    }

    async fn target_release_update_recovery(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<update::SetTargetReleaseParams>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            nexus
                .target_release_update(
                    &opctx,
                    body.into_inner(),
                    SetTargetReleaseIntent::RecoverFromMupdate,
                )
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        })
        .await
    }

    async fn system_update_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<update::UpdateStatus>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let status = nexus.update_status_external(&opctx).await?;
            Ok(HttpResponseOk(status))
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
        query_params: Query<PaginatedById<user::OptionalGroupSelector>>,
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
                &|_, user: &User| user.id.into_untyped_uuid(),
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn user_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<path_params::UserPath>,
    ) -> Result<HttpResponseOk<user::User>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let (.., user) =
                nexus.current_silo_user_lookup(&opctx, path.user_id).await?;
            Ok(HttpResponseOk(user.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn user_token_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<path_params::UserPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<device::DeviceAccessToken>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let tokens = nexus
                .silo_user_token_list(&opctx, path.user_id, &pag_params)
                .await?
                .into_iter()
                .map(device::DeviceAccessToken::from)
                .collect();
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                tokens,
                &marker_for_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn user_session_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<path_params::UserPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<device::ConsoleSession>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;

            let sessions = nexus
                .silo_user_session_list(
                    &opctx,
                    path.user_id,
                    &pag_params,
                    // TODO: https://github.com/oxidecomputer/omicron/issues/8625
                    apictx.context.console_config.session_idle_timeout,
                    apictx.context.console_config.session_absolute_timeout,
                )
                .await?
                .into_iter()
                .map(device::ConsoleSession::from)
                .collect();
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                sessions,
                &marker_for_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn user_logout(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<path_params::UserPath>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            nexus.current_silo_user_logout(&opctx, path.user_id).await?;
            Ok(HttpResponseUpdatedNoContent())
        })
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
                &|_, group: &Group| group.id.into_untyped_uuid(),
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
        path_params: Path<path_params::GroupPath>,
    ) -> Result<HttpResponseOk<Group>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let group = nexus.silo_group_lookup(&opctx, &path.group_id).await?;
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
        path_params: Path<user::UserBuiltinSelector>,
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

    // Current user

    async fn current_user_view(
        rqctx: RequestContext<ApiContext>,
    ) -> Result<HttpResponseOk<user::CurrentUser>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let user = nexus.silo_user_fetch_self(&opctx).await?;
            let (authz_silo, silo) =
                nexus.current_silo_lookup(&opctx)?.fetch().await?;

            // only eat Forbidden errors indicating lack of perms. other errors
            // blow up normally
            let fleet_viewer =
                match opctx.authorize(authz::Action::Read, &authz::FLEET).await
                {
                    Ok(()) => true,
                    Err(Error::Forbidden) => false,
                    Err(e) => return Err(e.into()),
                };
            let silo_admin =
                match opctx.authorize(authz::Action::Modify, &authz_silo).await
                {
                    Ok(()) => true,
                    Err(Error::Forbidden) => false,
                    Err(e) => return Err(e.into()),
                };

            Ok(HttpResponseOk(user::CurrentUser {
                user: user.into(),
                silo_name: silo.name().clone(),
                fleet_viewer,
                silo_admin,
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
    ) -> Result<HttpResponseOk<ResultsPage<user::Group>>, HttpError> {
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
                &|_, group: &user::Group| group.id.into_untyped_uuid(),
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

            let silo_user_id = match actor.silo_user_id() {
                Some(silo_user_id) => silo_user_id,
                None => {
                    return Err(Error::non_resourcetype_not_found(
                        "could not find silo user",
                    ))?;
                }
            };

            let ssh_keys = nexus
                .ssh_keys_list(&opctx, silo_user_id, &paginated_by)
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
        new_key: TypedBody<ssh_key::SshKeyCreate>,
    ) -> Result<HttpResponseCreated<SshKey>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let new_key = new_key.into_inner();
            let &actor = opctx
                .authn
                .actor_required()
                .internal_context("creating ssh key for current user")?;

            let silo_user_id = match actor.silo_user_id() {
                Some(silo_user_id) => silo_user_id,
                None => {
                    return Err(Error::non_resourcetype_not_found(
                        "could not find silo user",
                    ))?;
                }
            };

            let ssh_key =
                nexus.ssh_key_create(&opctx, silo_user_id, new_key).await?;
            Ok(HttpResponseCreated(ssh_key.into()))
        })
        .await
    }

    async fn current_user_ssh_key_view(
        rqctx: RequestContext<ApiContext>,
        path_params: Path<path_params::SshKeyPath>,
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

            let silo_user_id = match actor.silo_user_id() {
                Some(silo_user_id) => silo_user_id,
                None => {
                    return Err(Error::non_resourcetype_not_found(
                        "could not find silo user",
                    ))?;
                }
            };

            let ssh_key_selector =
                ssh_key::SshKeySelector { silo_user_id, ssh_key: path.ssh_key };

            let ssh_key_lookup =
                nexus.ssh_key_lookup(&opctx, &ssh_key_selector)?;

            let (.., ssh_key) = ssh_key_lookup.fetch().await?;

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
        path_params: Path<path_params::SshKeyPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let &actor = opctx
                .authn
                .actor_required()
                .internal_context("deleting one of current user's ssh keys")?;

            let silo_user_id = match actor.silo_user_id() {
                Some(silo_user_id) => silo_user_id,
                None => {
                    return Err(Error::non_resourcetype_not_found(
                        "could not find silo user",
                    ))?;
                }
            };

            let ssh_key_selector =
                ssh_key::SshKeySelector { silo_user_id, ssh_key: path.ssh_key };
            let ssh_key_lookup =
                nexus.ssh_key_lookup(&opctx, &ssh_key_selector)?;
            nexus.ssh_key_delete(&opctx, silo_user_id, &ssh_key_lookup).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn current_user_access_token_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<device::DeviceAccessToken>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let tokens = nexus
                .current_user_token_list(&opctx, &pag_params)
                .await?
                .into_iter()
                .map(device::DeviceAccessToken::from)
                .collect();
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                tokens,
                &marker_for_id,
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn current_user_access_token_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<path_params::TokenPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            nexus.current_user_token_delete(&opctx, path.token_id).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn support_bundle_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByTimeAndId>,
    ) -> Result<
        HttpResponseOk<ResultsPage<support_bundle::SupportBundleInfo>>,
        HttpError,
    > {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;

            let query = query_params.into_inner();
            let pagparams = data_page_params_for(&rqctx, &query)?;

            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;

            let bundles = nexus
                .support_bundle_list(&opctx, &pagparams)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();

            Ok(HttpResponseOk(ScanByTimeAndId::results_page(
                &query,
                bundles,
                &|_, bundle: &support_bundle::SupportBundleInfo| {
                    (bundle.time_created, bundle.id.into_untyped_uuid())
                },
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<support_bundle::SupportBundlePath>,
    ) -> Result<HttpResponseOk<support_bundle::SupportBundleInfo>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();

            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;

            let bundle = nexus
                .support_bundle_view(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle_id),
                )
                .await?;

            Ok(HttpResponseOk(bundle.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<support_bundle::SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;

            let head = false;
            let range = headers
                .into_inner()
                .range
                .map(|r| PotentialRange::new(r.as_bytes()));

            let body = nexus
                .support_bundle_download(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle_id),
                    SupportBundleQueryType::Index,
                    head,
                    range,
                )
                .await?;
            Ok(body)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_download(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<support_bundle::SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;

            let head = false;
            let range = headers
                .into_inner()
                .range
                .map(|r| PotentialRange::new(r.as_bytes()));

            let body = nexus
                .support_bundle_download(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle_id),
                    SupportBundleQueryType::Whole,
                    head,
                    range,
                )
                .await?;
            Ok(body)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_download_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<support_bundle::SupportBundleFilePath>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let head = false;
            let range = headers
                .into_inner()
                .range
                .map(|r| PotentialRange::new(r.as_bytes()));

            let body = nexus
                .support_bundle_download(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle.bundle_id),
                    SupportBundleQueryType::Path { file_path: path.file },
                    head,
                    range,
                )
                .await?;
            Ok(body)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_head(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<support_bundle::SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let head = true;
            let range = headers
                .into_inner()
                .range
                .map(|r| PotentialRange::new(r.as_bytes()));

            let body = nexus
                .support_bundle_download(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle_id),
                    SupportBundleQueryType::Whole,
                    head,
                    range,
                )
                .await?;
            Ok(body)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_head_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<support_bundle::SupportBundleFilePath>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let head = true;
            let range = headers
                .into_inner()
                .range
                .map(|r| PotentialRange::new(r.as_bytes()));

            let body = nexus
                .support_bundle_download(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle.bundle_id),
                    SupportBundleQueryType::Path { file_path: path.file },
                    head,
                    range,
                )
                .await?;
            Ok(body)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_create(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<support_bundle::SupportBundleCreate>,
    ) -> Result<HttpResponseCreated<support_bundle::SupportBundleInfo>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let create_params = body.into_inner();
            let bundle = nexus
                .support_bundle_create(
                    &opctx,
                    "Created by external API",
                    create_params.user_comment,
                )
                .await?;
            Ok(HttpResponseCreated(bundle.into()))
        })
        .await
    }

    async fn support_bundle_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<support_bundle::SupportBundlePath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            nexus
                .support_bundle_delete(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle_id),
                )
                .await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn support_bundle_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<support_bundle::SupportBundlePath>,
        body: TypedBody<support_bundle::SupportBundleUpdate>,
    ) -> Result<HttpResponseOk<support_bundle::SupportBundleInfo>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let path = path_params.into_inner();
            let update = body.into_inner();
            let bundle = nexus
                .support_bundle_update_user_comment(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle_id),
                    update.user_comment,
                )
                .await?;
            Ok(HttpResponseOk(bundle.into()))
        })
        .await
    }

    async fn probe_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByNameOrId<project::ProjectSelector>>,
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
        path_params: Path<path_params::ProbePath>,
        query_params: Query<project::ProjectSelector>,
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
        query_params: Query<project::ProjectSelector>,
        new_probe: TypedBody<probe::ProbeCreate>,
    ) -> Result<HttpResponseCreated<Probe>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
            let new_probe_params = new_probe.into_inner();
            let project_selector = query_params.into_inner();
            let project_lookup =
                nexus.project_lookup(&opctx, project_selector)?;
            let probe = nexus
                .probe_create(&opctx, &project_lookup, &new_probe_params)
                .await?;
            Ok(HttpResponseCreated(probe.into()))
        })
        .await
    }

    async fn probe_delete(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<project::ProjectSelector>,
        path_params: Path<path_params::ProbePath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
            let path = path_params.into_inner();
            let project_selector = query_params.into_inner();
            let project_lookup =
                nexus.project_lookup(&opctx, project_selector)?;
            nexus.probe_delete(&opctx, &project_lookup, path.probe).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn audit_log_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByTimeAndId<audit::AuditLogParams>>,
    ) -> Result<HttpResponseOk<ResultsPage<audit::AuditLogEntry>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;

            let nexus = &apictx.context.nexus;
            let query = query_params.into_inner();
            let scan_params = ScanByTimeAndId::from_query(&query)?;
            let pag_params = data_page_params_for(&rqctx, &query)?;

            let log_entries = nexus
                .audit_log_list(
                    &opctx,
                    &pag_params,
                    scan_params.selector.start_time,
                    scan_params.selector.end_time,
                )
                .await?;
            Ok(HttpResponseOk(ScanByTimeAndId::results_page(
                &query,
                log_entries
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
                &|_, entry: &audit::AuditLogEntry| {
                    (entry.time_completed, entry.id)
                },
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn login_saml_begin(
        rqctx: RequestContext<Self::Context>,
        _path_params: Path<console::LoginToProviderPathParam>,
        _query_params: Query<console::LoginUrlQuery>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async { console_api::serve_console_index(&rqctx).await };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn login_saml_redirect(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<console::LoginToProviderPathParam>,
        query_params: Query<console::LoginUrlQuery>,
    ) -> Result<HttpResponseFound, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path_params = path_params.into_inner();
            let query_params = query_params.into_inner();

            // Use opctx_external_authn because this request will be
            // unauthenticated.
            let opctx = nexus.opctx_external_authn();

            nexus
                .login_saml_redirect(
                    &opctx,
                    &path_params.silo_name.into(),
                    &path_params.provider_name.into(),
                    query_params.redirect_uri,
                )
                .await
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn login_saml(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<console::LoginToProviderPathParam>,
        body_bytes: dropshot::UntypedBody,
    ) -> Result<HttpResponseSeeOther, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;

            // By definition, this request is not authenticated. These
            // operations happen using the Nexus "external authentication"
            // context, which we keep specifically for this purpose.
            let opctx = nexus.opctx_external_authn();
            let audit =
                nexus.audit_log_entry_init_unauthed(opctx, &rqctx).await?;

            let result = async {
                let path_params = path_params.into_inner();
                let (session, next_url) = nexus
                    .login_saml(
                        opctx,
                        body_bytes,
                        &path_params.silo_name.into(),
                        &path_params.provider_name.into(),
                    )
                    .await?;

                let mut response = http_response_see_other(next_url)?;
                {
                    let headers = response.headers_mut();
                    // Use absolute timeout even though session might idle out
                    // first. Browser expiration is mostly for convenience, as
                    // the API will reject requests with an expired session
                    // regardless.
                    let cookie = session_cookie::session_cookie_header_value(
                        &session.token,
                        apictx.context.session_absolute_timeout(),
                        apictx.context.external_tls_enabled,
                    )?;
                    headers.append(header::SET_COOKIE, cookie);
                }
                Ok(response)
            }
            .await;

            let _ =
                nexus.audit_log_entry_complete(opctx, &audit, &result).await;
            result
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn login_local_begin(
        rqctx: RequestContext<Self::Context>,
        _path_params: Path<console::LoginPath>,
        _query_params: Query<console::LoginUrlQuery>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async { console_api::serve_console_index(&rqctx).await };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn login_local(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<console::LoginPath>,
        credentials: TypedBody<user::UsernamePasswordCredentials>,
    ) -> Result<HttpResponseHeaders<HttpResponseUpdatedNoContent>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;

            // By definition, this request is not authenticated. These
            // operations happen using the Nexus "external authentication"
            // context, which we keep specifically for this purpose.
            let opctx = nexus.opctx_external_authn();
            let audit =
                nexus.audit_log_entry_init_unauthed(opctx, &rqctx).await?;

            let result = async {
                let path = path_params.into_inner();
                let credentials = credentials.into_inner();
                let silo = path.silo_name.into();

                let silo_lookup = nexus.silo_lookup(opctx, silo)?;
                let user =
                    nexus.login_local(opctx, &silo_lookup, credentials).await?;

                let session = nexus.session_create(opctx, &user).await?;
                let mut response = HttpResponseHeaders::new_unnamed(
                    HttpResponseUpdatedNoContent(),
                );

                {
                    let headers = response.headers_mut();
                    // Use absolute timeout even though session might idle out
                    // first. Browser expiration is mostly for convenience, as
                    // the API will reject requests with an expired session
                    // regardless.
                    let cookie = session_cookie::session_cookie_header_value(
                        &session.token,
                        apictx.context.session_absolute_timeout(),
                        apictx.context.external_tls_enabled,
                    )?;
                    headers.append(header::SET_COOKIE, cookie);
                }
                Ok(response)
            }
            .await;

            let _ =
                nexus.audit_log_entry_complete(opctx, &audit, &result).await;
            result
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn logout(
        rqctx: RequestContext<Self::Context>,
        cookies: Cookies,
    ) -> Result<HttpResponseHeaders<HttpResponseUpdatedNoContent>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            // this is kind of a weird one, but we're only doing things here
            // that are authorized directly by the possession of the token,
            // which makes it somewhat like a login
            let opctx = nexus.opctx_external_authn();
            let session_cookie =
                cookies.get(session_cookie::SESSION_COOKIE_COOKIE_NAME);

            // Look up session and delete it if present
            if let Some(cookie) = session_cookie {
                let token = cookie.value().to_string();
                nexus.session_hard_delete_by_token(&opctx, token).await?;
            }

            // If user's session was already expired, they fail auth and their
            // session is automatically deleted by the auth scheme. If they
            // have no session at all (because, e.g., they cleared their cookies
            // while sitting on the page) they will also fail auth, but nothing
            // is deleted and the above lookup by token fails (but doesn't early
            // return).

            // Even if the user failed auth, we don't want to send them back a 401
            // like we would for a normal request. They are in fact logged out like
            // they intended, and we should send the standard success response.

            let mut response = HttpResponseHeaders::new_unnamed(
                HttpResponseUpdatedNoContent(),
            );
            {
                let headers = response.headers_mut();
                headers.append(
                    header::SET_COOKIE,
                    session_cookie::clear_session_cookie_header_value(
                        apictx.context.external_tls_enabled,
                    )?,
                );
            };

            Ok(response)
        };

        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn login_begin(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<console::LoginUrlQuery>,
    ) -> Result<HttpResponseFound, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let query = query_params.into_inner();
            let login_url =
                console_api::get_login_url(&rqctx, query.redirect_uri).await?;
            http_response_found(login_url)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn console_projects(
        rqctx: RequestContext<Self::Context>,
        _path_params: Path<console::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_index_or_login_redirect(rqctx).await
    }

    async fn console_settings_page(
        rqctx: RequestContext<Self::Context>,
        _path_params: Path<console::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_index_or_login_redirect(rqctx).await
    }

    async fn console_system_page(
        rqctx: RequestContext<Self::Context>,
        _path_params: Path<console::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_index_or_login_redirect(rqctx).await
    }

    async fn console_lookup(
        rqctx: RequestContext<Self::Context>,
        _path_params: Path<console::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_index_or_login_redirect(rqctx).await
    }

    async fn console_root(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_index_or_login_redirect(rqctx).await
    }

    async fn console_projects_new(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_index_or_login_redirect(rqctx).await
    }

    async fn console_silo_images(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_index_or_login_redirect(rqctx).await
    }

    async fn console_silo_utilization(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_index_or_login_redirect(rqctx).await
    }

    async fn console_silo_access(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_index_or_login_redirect(rqctx).await
    }

    async fn asset(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<console::RestPathParam>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async { console_api::asset(&rqctx, path_params).await };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Entrypoints for the OAuth 2.0 Device Authorization Grant flow.
    //
    // These are endpoints used by the API client per se (e.g., the CLI),
    // *not* the user of that client (e.g., an Oxide rack operator). They
    // are for requesting access tokens that will be managed and used by
    // the client to make other API requests.

    async fn device_auth_request(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<device::DeviceAuthRequest>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let nexus = &apictx.context.nexus;
        let params = params.into_inner();
        let handler = async {
            let opctx = nexus.opctx_external_authn();
            let authority = authority_for_request(&rqctx.request);
            let host = match &authority {
                Ok(host) => host.as_str(),
                Err(error) => {
                    return nexus.build_oauth_response(
                        StatusCode::BAD_REQUEST,
                        &serde_json::json!({
                            "error": "invalid_request",
                            "error_description": error,
                        }),
                    );
                }
            };

            let model =
                nexus.device_auth_request_create(&opctx, params).await?;
            nexus.build_oauth_response(
                StatusCode::OK,
                &model.into_response(rqctx.server.using_tls(), host),
            )
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn device_auth_verify(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_index_or_login_redirect(rqctx).await
    }

    async fn device_auth_success(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        console_api::console_index_or_login_redirect(rqctx).await
    }

    // Note that the audit logging for the device auth token flow is done in
    // `device_auth_confirm` because that is where both authentication (using
    // a web session) and token creation actually happen. This is the endpoint
    // hit by the web console with an existing session and the device code.
    // We create the token in the DB but do not put it in the response (it's
    // a 204).
    //
    // In theory we could log the `device_access_token` endpoint as well, but
    // there are a ton of polling calls, all but one of which return 400s with
    // a "pending" state. At present I do not think logging when the client
    // _receives_ the token adds useful information on top of knowing when it
    // was created. They will virtually always happen at the same time anyway.
    //
    // Audit logging in `device_auth_request` would be even more pointless
    // because at that point we do not know who the user is and nothing
    // requiring authentication has happened yet -- anybody could spam those
    // requests and nothing would happen.

    // This is an authenticated request, so we know who the user is. In that
    // respect it's more like a regular resource create operation and not like
    // the true login endpoints `login_local` and `login_saml`.
    async fn device_auth_confirm(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<device::DeviceAuthVerify>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let params = params.into_inner();
            let &actor = opctx.authn.actor_required().internal_context(
                "creating new device auth session for current user",
            )?;

            let silo_user_id = match actor.silo_user_id() {
                Some(silo_user_id) => silo_user_id,
                None => {
                    return Err(Error::non_resourcetype_not_found(
                        "could not find silo user",
                    ))?;
                }
            };

            let _token = nexus
                .device_auth_request_verify(
                    &opctx,
                    params.user_code,
                    silo_user_id,
                )
                .await?;

            Ok(HttpResponseUpdatedNoContent())
        })
        .await
    }

    async fn device_access_token(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<device::DeviceAccessTokenRequest>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx = nexus.opctx_external_authn();
            let params = params.into_inner();
            nexus.device_access_token(&opctx, params).await
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn alert_class_list(
        rqctx: RequestContext<Self::Context>,
        pag_params: Query<
            PaginationParams<EmptyScanParams, alert::AlertClassPage>,
        >,
        filter: Query<alert::AlertClassFilter>,
    ) -> Result<HttpResponseOk<ResultsPage<alert::AlertClass>>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;

            let query = pag_params.into_inner();
            let filter = filter.into_inner();
            let marker = match query.page {
                WhichPage::First(_) => None,
                WhichPage::Next(ref addr) => Some(addr),
            };
            let pag_params = DataPageParams {
                limit: rqctx.page_limit(&query)?,
                direction: PaginationOrder::Ascending,
                marker,
            };
            let alert_classes =
                nexus.alert_class_list(&opctx, filter, pag_params).await?;
            Ok(HttpResponseOk(ResultsPage::new(
                alert_classes,
                &EmptyScanParams {},
                |class: &alert::AlertClass, _| alert::AlertClassPage {
                    last_seen: class.name.clone(),
                },
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn alert_receiver_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<alert::AlertReceiver>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;

            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;

            let query = query_params.into_inner();
            let pagparams = data_page_params_for(&rqctx, &query)?;
            let scan_params = ScanByNameOrId::from_query(&query)?;
            let paginated_by = name_or_id_pagination(&pagparams, scan_params)?;

            let rxs = nexus
                .alert_receiver_list(&opctx, &paginated_by)
                .await?
                .into_iter()
                .map(|webhook| {
                    alert::WebhookReceiver::try_from(webhook)
                        .map(alert::AlertReceiver::from)
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(HttpResponseOk(ScanByNameOrId::results_page(
                &query,
                rxs,
                &marker_for_name_or_id,
            )?))
        };

        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn alert_receiver_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<alert::AlertReceiverSelector>,
    ) -> Result<HttpResponseOk<alert::AlertReceiver>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;

            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;
            let webhook_selector = path_params.into_inner();
            let rx = nexus.alert_receiver_lookup(&opctx, webhook_selector)?;
            let webhook = nexus.alert_receiver_config_fetch(&opctx, rx).await?;
            Ok(HttpResponseOk(
                alert::WebhookReceiver::try_from(webhook)?.into(),
            ))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn webhook_receiver_create(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<alert::WebhookCreate>,
    ) -> Result<HttpResponseCreated<alert::WebhookReceiver>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let params = params.into_inner();
            let receiver =
                nexus.webhook_receiver_create(&opctx, params).await?;
            Ok(HttpResponseCreated(alert::WebhookReceiver::try_from(receiver)?))
        })
        .await
    }

    async fn webhook_receiver_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<alert::AlertReceiverSelector>,
        params: TypedBody<alert::WebhookReceiverUpdate>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let webhook_selector = path_params.into_inner();
            let params = params.into_inner();
            let rx = nexus.alert_receiver_lookup(&opctx, webhook_selector)?;
            nexus.webhook_receiver_update(&opctx, rx, params).await?;
            Ok(HttpResponseUpdatedNoContent())
        })
        .await
    }

    async fn alert_receiver_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<alert::AlertReceiverSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let webhook_selector = path_params.into_inner();
            let rx = nexus.alert_receiver_lookup(&opctx, webhook_selector)?;
            nexus.webhook_receiver_delete(&opctx, rx).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn alert_receiver_subscription_add(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<alert::AlertReceiverSelector>,
        params: TypedBody<alert::AlertSubscriptionCreate>,
    ) -> Result<HttpResponseCreated<alert::AlertSubscriptionCreated>, HttpError>
    {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let webhook_selector = path_params.into_inner();
            let subscription = params.into_inner();
            let rx = nexus.alert_receiver_lookup(&opctx, webhook_selector)?;
            let subscription = nexus
                .alert_receiver_subscription_add(&opctx, rx, subscription)
                .await?;
            Ok(HttpResponseCreated(subscription))
        })
        .await
    }

    async fn alert_receiver_subscription_remove(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<alert::AlertSubscriptionSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let alert::AlertSubscriptionSelector { receiver, subscription } =
                path_params.into_inner();
            let rx = nexus.alert_receiver_lookup(&opctx, receiver)?;
            nexus
                .alert_receiver_subscription_remove(&opctx, rx, subscription)
                .await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn alert_receiver_probe(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<alert::AlertReceiverSelector>,
        query_params: Query<alert::AlertReceiverProbe>,
    ) -> Result<HttpResponseOk<alert::AlertProbeResult>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let webhook_selector = path_params.into_inner();
            let probe_params = query_params.into_inner();
            let rx = nexus.alert_receiver_lookup(&opctx, webhook_selector)?;
            let result =
                nexus.webhook_receiver_probe(&opctx, rx, probe_params).await?;
            Ok(HttpResponseOk(result))
        })
        .await
    }

    async fn webhook_secrets_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<alert::AlertReceiverSelector>,
    ) -> Result<HttpResponseOk<alert::WebhookSecrets>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;

            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;

            let webhook_selector = query_params.into_inner();
            let rx = nexus.alert_receiver_lookup(&opctx, webhook_selector)?;
            let secrets = nexus
                .webhook_receiver_secrets_list(&opctx, rx)
                .await?
                .into_iter()
                .map(Into::into)
                .collect();

            Ok(HttpResponseOk(alert::WebhookSecrets { secrets }))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    /// Add a secret to a webhook.
    async fn webhook_secrets_add(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<alert::AlertReceiverSelector>,
        params: TypedBody<alert::WebhookSecretCreate>,
    ) -> Result<HttpResponseCreated<alert::WebhookSecret>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let alert::WebhookSecretCreate { secret } = params.into_inner();
            let webhook_selector = query_params.into_inner();
            let rx = nexus.alert_receiver_lookup(&opctx, webhook_selector)?;
            let secret =
                nexus.webhook_receiver_secret_add(&opctx, rx, secret).await?;
            Ok(HttpResponseCreated(secret))
        })
        .await
    }

    /// Delete a secret from a webhook receiver.
    async fn webhook_secrets_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<alert::WebhookSecretSelector>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let secret_selector = path_params.into_inner();
            let secret =
                nexus.webhook_secret_lookup(&opctx, secret_selector)?;
            nexus.webhook_receiver_secret_delete(&opctx, secret).await?;
            Ok(HttpResponseDeleted())
        })
        .await
    }

    async fn alert_delivery_list(
        rqctx: RequestContext<Self::Context>,
        receiver: Path<alert::AlertReceiverSelector>,
        filter: Query<alert::AlertDeliveryStateFilter>,
        query: Query<PaginatedByTimeAndId>,
    ) -> Result<HttpResponseOk<ResultsPage<alert::AlertDelivery>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;

            let opctx =
                crate::context::op_context_for_external_api(&rqctx).await?;

            let webhook_selector = receiver.into_inner();
            let filter = filter.into_inner();
            let query = query.into_inner();
            let pag_params = data_page_params_for(&rqctx, &query)?;
            let rx = nexus.alert_receiver_lookup(&opctx, webhook_selector)?;
            let deliveries = nexus
                .alert_receiver_delivery_list(&opctx, rx, filter, &pag_params)
                .await?;

            Ok(HttpResponseOk(ScanByTimeAndId::results_page(
                &query,
                deliveries,
                &|_, d| (d.time_started, d.id),
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn alert_delivery_resend(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<alert::AlertSelector>,
        receiver: Query<alert::AlertReceiverSelector>,
    ) -> Result<HttpResponseCreated<alert::AlertDeliveryId>, HttpError> {
        audit_and_time(&rqctx, |opctx, nexus| async move {
            let event_selector = path_params.into_inner();
            let webhook_selector = receiver.into_inner();
            let event = nexus.alert_lookup(&opctx, event_selector)?;
            let rx = nexus.alert_receiver_lookup(&opctx, webhook_selector)?;
            let delivery_id =
                nexus.alert_receiver_resend(&opctx, rx, event).await?;
            Ok(HttpResponseCreated(alert::AlertDeliveryId {
                delivery_id: delivery_id.into_untyped_uuid(),
            }))
        })
        .await
    }
}
