use std::collections::BTreeMap;

use anyhow::anyhow;
use dropshot::Body;
use dropshot::{
    EmptyScanParams, EndpointTagPolicy, Header, HttpError,
    HttpResponseAccepted, HttpResponseCreated, HttpResponseDeleted,
    HttpResponseFound, HttpResponseHeaders, HttpResponseOk,
    HttpResponseSeeOther, HttpResponseUpdatedNoContent, PaginationParams, Path,
    Query, RequestContext, ResultsPage, StreamingBody, TypedBody,
    WebsocketChannelResult, WebsocketConnection,
};
use http::Response;
use ipnetwork::IpNetwork;
use nexus_types::{
    authn::cookies::Cookies,
    external_api::{headers, params, shared, views},
};
use omicron_common::api::external::{
    http_pagination::{
        PaginatedById, PaginatedByName, PaginatedByNameOrId,
        PaginatedByTimeAndId,
    },
    *,
};
use openapi_manager_types::ValidationContext;
use openapiv3::OpenAPI;

pub const API_VERSION: &str = "20250604.0.0";

const MIB: usize = 1024 * 1024;
const GIB: usize = 1024 * MIB;
const DISK_BULK_WRITE_MAX_BYTES: usize = 8 * MIB;
// Full release repositories are currently (Dec 2024) 1.8 GiB and are likely to
// continue growing.
const PUT_UPDATE_REPOSITORY_MAX_BYTES: usize = 4 * GIB;

// API ENDPOINT FUNCTION NAMING CONVENTIONS
//
// Generally, HTTP resources are grouped within some collection. For a
// relatively simple example:
//
//   GET    v1/projects                (list the projects in the collection)
//   POST   v1/projects                (create a project in the collection)
//   GET    v1/projects/{project}      (look up a project in the collection)
//   DELETE v1/projects/{project}      (delete a project in the collection)
//   PUT    v1/projects/{project}      (update a project in the collection)
//
// We pick a name for the function that implements a given API entrypoint
// based on how we expect it to appear in the CLI subcommand hierarchy. For
// example:
//
//   GET    v1/projects                 -> project_list()
//   POST   v1/projects                 -> project_create()
//   GET    v1/projects/{project}       -> project_view()
//   DELETE v1/projects/{project}       -> project_delete()
//   PUT    v1/projects/{project}       -> project_update()
//
// Note that the path typically uses the entity's plural form while the
// function name uses its singular.
//
// Operations beyond list, create, view, delete, and update should use a
// descriptive noun or verb, again bearing in mind that this will be
// transcribed into the CLI and SDKs:
//
//   POST   -> instance_reboot
//   POST   -> instance_stop
//   GET    -> instance_serial_console
//
// Note that these function names end up in generated OpenAPI spec as the
// operationId for each endpoint, and therefore represent a contract with
// clients. Client generators use operationId to name API methods, so changing
// a function name is a breaking change from a client perspective.

#[dropshot::api_description {
    tag_config = {
        allow_other_tags = false,
        policy = EndpointTagPolicy::ExactlyOne,
        tags = {
            "affinity" = {
                description = "Anti-affinity groups give control over instance placement.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/affinity"
                }

            },
            "disks" = {
                description = "Virtual disks are used to store instance-local data which includes the operating system.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/disks"
                }
            },
            "floating-ips" = {
                description = "Floating IPs allow a project to allocate well-known IPs to instances.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/floating-ips"
                }
            },
            "hidden" = {
                description = "TODO operations that will not ship to customers",
                external_docs = {
                    url = "http://docs.oxide.computer/api"
                }
            },
            "images" = {
                description = "Images are read-only virtual disks that may be used to boot virtual machines.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/images"
                }
            },
            "instances" = {
                description = "Virtual machine instances are the basic unit of computation. These operations are used for provisioning, controlling, and destroying instances.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/instances"
                }
            },
            "login" = {
                description = "Authentication endpoints",
                external_docs = {
                    url = "http://docs.oxide.computer/api/login"
                }
            },
            "metrics" = {
                description = "Silo-scoped metrics",
                external_docs = {
                    url = "http://docs.oxide.computer/api/metrics"
                }
            },
            "policy" = {
                description = "System-wide IAM policy",
                external_docs = {
                    url = "http://docs.oxide.computer/api/policy"
                }
            },
            "projects" = {
                description = "Projects are a grouping of associated resources such as instances and disks within a silo for purposes of billing and access control.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/projects"
                }
            },
            "roles" = {
                description = "Roles are a component of Identity and Access Management (IAM) that allow a user or agent account access to additional permissions.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/roles"
                }
            },
            "session" = {
                description = "Information pertaining to the current session.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/session"
                }
            },
            "silos" = {
                description = "Silos represent a logical partition of users and resources.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/silos"
                }
            },
            "snapshots" = {
                description = "Snapshots of virtual disks at a particular point in time.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/snapshots"
                }
            },
            "tokens" = {
                description = "API clients use device access tokens for authentication.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/tokens"
                }
            },
            "vpcs" = {
                description = "Virtual Private Clouds (VPCs) provide isolated network environments for managing and deploying services.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/vpcs"
                }
            },
            "system/alerts" = {
                description = "Alerts deliver notifications for events that occur on the Oxide rack",
                external_docs = {
                    url = "http://docs.oxide.computer/api/alerts"
                }
            },
            "system/probes" = {
                description = "Probes for testing network connectivity",
                external_docs = {
                    url = "http://docs.oxide.computer/api/probes"
                }
            },
            "system/status" = {
                description = "Endpoints related to system health",
                external_docs = {
                    url = "http://docs.oxide.computer/api/system-status"
                }
            },
            "system/hardware" = {
                description = "These operations pertain to hardware inventory and management. Racks are the unit of expansion of an Oxide deployment. Racks are in turn composed of sleds, switches, power supplies, and a cabled backplane.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/system-hardware"
                }
            },
            "system/metrics" = {
                description = "Metrics provide insight into the operation of the Oxide deployment. These include telemetry on hardware and software components that can be used to understand the current state as well as to diagnose issues.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/system-metrics"
                }
            },
            "system/ip-pools" = {
                description = "IP pools are collections of external IPs that can be assigned to silos. When a pool is linked to a silo, users in that silo can allocate IPs from the pool for their instances.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/system-ip-pools"
                }
            },
            "system/networking" = {
                description = "This provides rack-level network configuration.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/system-networking"
                }
            },
            "system/silos" = {
                description = "Silos represent a logical partition of users and resources.",
                external_docs = {
                    url = "http://docs.oxide.computer/api/system-silos"
                }
            }
        }
    }
}]
pub trait NexusExternalApi {
    type Context;

    /// Ping API
    ///
    /// Always responds with Ok if it responds at all.
    #[endpoint {
        method = GET,
        path = "/v1/ping",
        tags = ["system/status"],
    }]
    async fn ping(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<views::Ping>, HttpError> {
        Ok(HttpResponseOk(views::Ping { status: views::PingStatus::Ok }))
    }

    /// Fetch top-level IAM policy
    #[endpoint {
        method = GET,
        path = "/v1/system/policy",
        tags = ["policy"],
    }]
    async fn system_policy_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::FleetRole>>, HttpError>;

    /// Update top-level IAM policy
    #[endpoint {
        method = PUT,
        path = "/v1/system/policy",
        tags = ["policy"],
    }]
    async fn system_policy_update(
        rqctx: RequestContext<Self::Context>,
        new_policy: TypedBody<shared::Policy<shared::FleetRole>>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::FleetRole>>, HttpError>;

    /// Fetch current silo's IAM policy
    #[endpoint {
        method = GET,
        path = "/v1/policy",
        tags = ["silos"],
    }]
    async fn policy_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::SiloRole>>, HttpError>;

    /// Update current silo's IAM policy
    #[endpoint {
        method = PUT,
        path = "/v1/policy",
        tags = ["silos"],
    }]
    async fn policy_update(
        rqctx: RequestContext<Self::Context>,
        new_policy: TypedBody<shared::Policy<shared::SiloRole>>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::SiloRole>>, HttpError>;

    /// Fetch current silo's auth settings
    #[endpoint {
        method = GET,
        path = "/v1/auth-settings",
        tags = ["silos"],
    }]
    async fn auth_settings_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<views::SiloAuthSettings>, HttpError>;

    /// Update current silo's auth settings
    #[endpoint {
        method = PUT,
        path = "/v1/auth-settings",
        tags = ["silos"],
    }]
    async fn auth_settings_update(
        rqctx: RequestContext<Self::Context>,
        new_settings: TypedBody<params::SiloAuthSettingsUpdate>,
    ) -> Result<HttpResponseOk<views::SiloAuthSettings>, HttpError>;

    /// Fetch resource utilization for user's current silo
    #[endpoint {
        method = GET,
        path = "/v1/utilization",
        tags = ["silos"],
    }]
    async fn utilization_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<views::Utilization>, HttpError>;

    /// Fetch current utilization for given silo
    #[endpoint {
        method = GET,
        path = "/v1/system/utilization/silos/{silo}",
        tags = ["system/silos"],
    }]
    async fn silo_utilization_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SiloPath>,
    ) -> Result<HttpResponseOk<views::SiloUtilization>, HttpError>;

    /// List current utilization state for all silos
    #[endpoint {
        method = GET,
        path = "/v1/system/utilization/silos",
        tags = ["system/silos"],
    }]
    async fn silo_utilization_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<views::SiloUtilization>>, HttpError>;

    /// Lists resource quotas for all silos
    #[endpoint {
        method = GET,
        path = "/v1/system/silo-quotas",
        tags = ["system/silos"],
    }]
    async fn system_quotas_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<views::SiloQuotas>>, HttpError>;

    /// Fetch resource quotas for silo
    #[endpoint {
        method = GET,
        path = "/v1/system/silos/{silo}/quotas",
        tags = ["system/silos"],
    }]
    async fn silo_quotas_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SiloPath>,
    ) -> Result<HttpResponseOk<views::SiloQuotas>, HttpError>;

    /// Update resource quotas for silo
    ///
    /// If a quota value is not specified, it will remain unchanged.
    #[endpoint {
        method = PUT,
        path = "/v1/system/silos/{silo}/quotas",
        tags = ["system/silos"],
    }]
    async fn silo_quotas_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SiloPath>,
        new_quota: TypedBody<params::SiloQuotasUpdate>,
    ) -> Result<HttpResponseOk<views::SiloQuotas>, HttpError>;

    /// List silos
    ///
    /// Lists silos that are discoverable based on the current permissions.
    #[endpoint {
        method = GET,
        path = "/v1/system/silos",
        tags = ["system/silos"],
    }]
    async fn silo_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<views::Silo>>, HttpError>;

    /// Create a silo
    #[endpoint {
        method = POST,
        path = "/v1/system/silos",
        tags = ["system/silos"],
    }]
    async fn silo_create(
        rqctx: RequestContext<Self::Context>,
        new_silo_params: TypedBody<params::SiloCreate>,
    ) -> Result<HttpResponseCreated<views::Silo>, HttpError>;

    /// Fetch silo
    ///
    /// Fetch silo by name or ID.
    #[endpoint {
        method = GET,
        path = "/v1/system/silos/{silo}",
        tags = ["system/silos"],
    }]
    async fn silo_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SiloPath>,
    ) -> Result<HttpResponseOk<views::Silo>, HttpError>;

    /// List IP pools linked to silo
    ///
    /// Linked IP pools are available to users in the specified silo. A silo
    /// can have at most one default pool. IPs are allocated from the default
    /// pool when users ask for one without specifying a pool.
    #[endpoint {
        method = GET,
        path = "/v1/system/silos/{silo}/ip-pools",
        tags = ["system/silos"],
    }]
    async fn silo_ip_pool_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SiloPath>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<views::SiloIpPool>>, HttpError>;

    /// Delete a silo
    ///
    /// Delete a silo by name or ID.
    #[endpoint {
        method = DELETE,
        path = "/v1/system/silos/{silo}",
        tags = ["system/silos"],
    }]
    async fn silo_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SiloPath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Fetch silo IAM policy
    #[endpoint {
        method = GET,
        path = "/v1/system/silos/{silo}/policy",
        tags = ["system/silos"],
    }]
    async fn silo_policy_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SiloPath>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::SiloRole>>, HttpError>;

    /// Update silo IAM policy
    #[endpoint {
        method = PUT,
        path = "/v1/system/silos/{silo}/policy",
        tags = ["system/silos"],
    }]
    async fn silo_policy_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SiloPath>,
        new_policy: TypedBody<shared::Policy<shared::SiloRole>>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::SiloRole>>, HttpError>;

    // Silo-specific user endpoints

    /// List built-in (system) users in silo
    #[endpoint {
        method = GET,
        path = "/v1/system/users",
        tags = ["system/silos"],
    }]
    async fn silo_user_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById<params::SiloSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<views::User>>, HttpError>;

    /// Fetch built-in (system) user
    #[endpoint {
        method = GET,
        path = "/v1/system/users/{user_id}",
        tags = ["system/silos"],
    }]
    async fn silo_user_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::UserParam>,
        query_params: Query<params::SiloSelector>,
    ) -> Result<HttpResponseOk<views::User>, HttpError>;

    // Silo identity providers

    /// List identity providers for silo
    ///
    /// List identity providers for silo by silo name or ID.
    #[endpoint {
        method = GET,
        path = "/v1/system/identity-providers",
        tags = ["system/silos"],
    }]
    async fn silo_identity_provider_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<params::SiloSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<views::IdentityProvider>>, HttpError>;

    // Silo SAML identity providers

    /// Create SAML identity provider
    #[endpoint {
        method = POST,
        path = "/v1/system/identity-providers/saml",
        tags = ["system/silos"],
    }]
    async fn saml_identity_provider_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::SiloSelector>,
        new_provider: TypedBody<params::SamlIdentityProviderCreate>,
    ) -> Result<HttpResponseCreated<views::SamlIdentityProvider>, HttpError>;

    /// Fetch SAML identity provider
    #[endpoint {
        method = GET,
        path = "/v1/system/identity-providers/saml/{provider}",
        tags = ["system/silos"],
    }]
    async fn saml_identity_provider_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::ProviderPath>,
        query_params: Query<params::OptionalSiloSelector>,
    ) -> Result<HttpResponseOk<views::SamlIdentityProvider>, HttpError>;

    // TODO: no DELETE for identity providers?

    // "Local" Identity Provider

    /// Create user
    ///
    /// Users can only be created in Silos with `provision_type` == `Fixed`.
    /// Otherwise, Silo users are just-in-time (JIT) provisioned when a user
    /// first logs in using an external Identity Provider.
    #[endpoint {
        method = POST,
        path = "/v1/system/identity-providers/local/users",
        tags = ["system/silos"],
    }]
    async fn local_idp_user_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::SiloSelector>,
        new_user_params: TypedBody<params::UserCreate>,
    ) -> Result<HttpResponseCreated<views::User>, HttpError>;

    /// Delete user
    #[endpoint {
        method = DELETE,
        path = "/v1/system/identity-providers/local/users/{user_id}",
        tags = ["system/silos"],
    }]
    async fn local_idp_user_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::UserParam>,
        query_params: Query<params::SiloSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Set or invalidate user's password
    ///
    /// Passwords can only be updated for users in Silos with identity mode
    /// `LocalOnly`.
    #[endpoint {
        method = POST,
        path = "/v1/system/identity-providers/local/users/{user_id}/set-password",
        tags = ["system/silos"],
    }]
    async fn local_idp_user_set_password(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::UserParam>,
        query_params: Query<params::SiloPath>,
        update: TypedBody<params::UserPassword>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// List projects
    #[endpoint {
        method = GET,
        path = "/v1/projects",
        tags = ["projects"],
    }]
    async fn project_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<views::Project>>, HttpError>;

    /// Create project
    #[endpoint {
        method = POST,
        path = "/v1/projects",
        tags = ["projects"],
    }]
    async fn project_create(
        rqctx: RequestContext<Self::Context>,
        new_project: TypedBody<params::ProjectCreate>,
    ) -> Result<HttpResponseCreated<views::Project>, HttpError>;

    /// Fetch project
    #[endpoint {
        method = GET,
        path = "/v1/projects/{project}",
        tags = ["projects"],
    }]
    async fn project_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::ProjectPath>,
    ) -> Result<HttpResponseOk<views::Project>, HttpError>;

    /// Delete project
    #[endpoint {
        method = DELETE,
        path = "/v1/projects/{project}",
        tags = ["projects"],
    }]
    async fn project_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::ProjectPath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    // TODO-correctness: Is it valid for PUT to accept application/json that's
    // a subset of what the resource actually represents?  If not, is that a
    // problem? (HTTP may require that this be idempotent.)  If so, can we get
    // around that having this be a slightly different content-type (e.g.,
    // "application/json-patch")?  We should see what other APIs do.
    /// Update a project
    #[endpoint {
        method = PUT,
        path = "/v1/projects/{project}",
        tags = ["projects"],
    }]
    async fn project_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::ProjectPath>,
        updated_project: TypedBody<params::ProjectUpdate>,
    ) -> Result<HttpResponseOk<views::Project>, HttpError>;

    /// Fetch project's IAM policy
    #[endpoint {
        method = GET,
        path = "/v1/projects/{project}/policy",
        tags = ["projects"],
    }]
    async fn project_policy_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::ProjectPath>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::ProjectRole>>, HttpError>;

    /// Update project's IAM policy
    #[endpoint {
        method = PUT,
        path = "/v1/projects/{project}/policy",
        tags = ["projects"],
    }]
    async fn project_policy_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::ProjectPath>,
        new_policy: TypedBody<shared::Policy<shared::ProjectRole>>,
    ) -> Result<HttpResponseOk<shared::Policy<shared::ProjectRole>>, HttpError>;

    // IP Pools

    /// List IP pools
    #[endpoint {
        method = GET,
        path = "/v1/ip-pools",
        tags = ["projects"],
    }]
    async fn project_ip_pool_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<views::SiloIpPool>>, HttpError>;

    /// Fetch IP pool
    #[endpoint {
        method = GET,
        path = "/v1/ip-pools/{pool}",
        tags = ["projects"],
    }]
    async fn project_ip_pool_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::IpPoolPath>,
    ) -> Result<HttpResponseOk<views::SiloIpPool>, HttpError>;

    /// List IP pools
    #[endpoint {
        method = GET,
        path = "/v1/system/ip-pools",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<views::IpPool>>, HttpError>;

    /// Create IP pool
    #[endpoint {
        method = POST,
        path = "/v1/system/ip-pools",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_create(
        rqctx: RequestContext<Self::Context>,
        pool_params: TypedBody<params::IpPoolCreate>,
    ) -> Result<HttpResponseCreated<views::IpPool>, HttpError>;

    /// Fetch IP pool
    #[endpoint {
        method = GET,
        path = "/v1/system/ip-pools/{pool}",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::IpPoolPath>,
    ) -> Result<HttpResponseOk<views::IpPool>, HttpError>;

    /// Delete IP pool
    #[endpoint {
        method = DELETE,
        path = "/v1/system/ip-pools/{pool}",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::IpPoolPath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Update IP pool
    #[endpoint {
        method = PUT,
        path = "/v1/system/ip-pools/{pool}",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::IpPoolPath>,
        updates: TypedBody<params::IpPoolUpdate>,
    ) -> Result<HttpResponseOk<views::IpPool>, HttpError>;

    /// Fetch IP pool utilization
    #[endpoint {
        method = GET,
        path = "/v1/system/ip-pools/{pool}/utilization",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_utilization_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::IpPoolPath>,
    ) -> Result<HttpResponseOk<views::IpPoolUtilization>, HttpError>;

    /// List IP pool's linked silos
    #[endpoint {
        method = GET,
        path = "/v1/system/ip-pools/{pool}/silos",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_silo_list(
        rqctx: RequestContext<Self::Context>,
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
    ) -> Result<HttpResponseOk<ResultsPage<views::IpPoolSiloLink>>, HttpError>;

    /// Link IP pool to silo
    ///
    /// Users in linked silos can allocate external IPs from this pool for their
    /// instances. A silo can have at most one default pool. IPs are allocated from
    /// the default pool when users ask for one without specifying a pool.
    #[endpoint {
        method = POST,
        path = "/v1/system/ip-pools/{pool}/silos",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_silo_link(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::IpPoolPath>,
        resource_assoc: TypedBody<params::IpPoolLinkSilo>,
    ) -> Result<HttpResponseCreated<views::IpPoolSiloLink>, HttpError>;

    /// Unlink IP pool from silo
    ///
    /// Will fail if there are any outstanding IPs allocated in the silo.
    #[endpoint {
        method = DELETE,
        path = "/v1/system/ip-pools/{pool}/silos/{silo}",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_silo_unlink(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::IpPoolSiloPath>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Make IP pool default for silo
    ///
    /// When a user asks for an IP (e.g., at instance create time) without
    /// specifying a pool, the IP comes from the default pool if a default is
    /// configured. When a pool is made the default for a silo, any existing
    /// default will remain linked to the silo, but will no longer be the
    /// default.
    #[endpoint {
        method = PUT,
        path = "/v1/system/ip-pools/{pool}/silos/{silo}",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_silo_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::IpPoolSiloPath>,
        update: TypedBody<params::IpPoolSiloUpdate>,
    ) -> Result<HttpResponseOk<views::IpPoolSiloLink>, HttpError>;

    /// Fetch Oxide service IP pool
    #[endpoint {
        method = GET,
        path = "/v1/system/ip-pools-service",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_service_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<views::IpPool>, HttpError>;

    /// List ranges for IP pool
    ///
    /// Ranges are ordered by their first address.
    #[endpoint {
        method = GET,
        path = "/v1/system/ip-pools/{pool}/ranges",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_range_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::IpPoolPath>,
        query_params: Query<IpPoolRangePaginationParams>,
    ) -> Result<HttpResponseOk<ResultsPage<views::IpPoolRange>>, HttpError>;

    /// Add range to IP pool
    ///
    /// IPv6 ranges are not allowed yet.
    #[endpoint {
        method = POST,
        path = "/v1/system/ip-pools/{pool}/ranges/add",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_range_add(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::IpPoolPath>,
        range_params: TypedBody<shared::IpRange>,
    ) -> Result<HttpResponseCreated<views::IpPoolRange>, HttpError>;

    /// Remove range from IP pool
    #[endpoint {
        method = POST,
        path = "/v1/system/ip-pools/{pool}/ranges/remove",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_range_remove(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::IpPoolPath>,
        range_params: TypedBody<shared::IpRange>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// List IP ranges for the Oxide service pool
    ///
    /// Ranges are ordered by their first address.
    #[endpoint {
        method = GET,
        path = "/v1/system/ip-pools-service/ranges",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_service_range_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<IpPoolRangePaginationParams>,
    ) -> Result<HttpResponseOk<ResultsPage<views::IpPoolRange>>, HttpError>;

    /// Add IP range to Oxide service pool
    ///
    /// IPv6 ranges are not allowed yet.
    #[endpoint {
        method = POST,
        path = "/v1/system/ip-pools-service/ranges/add",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_service_range_add(
        rqctx: RequestContext<Self::Context>,
        range_params: TypedBody<shared::IpRange>,
    ) -> Result<HttpResponseCreated<views::IpPoolRange>, HttpError>;

    /// Remove IP range from Oxide service pool
    #[endpoint {
        method = POST,
        path = "/v1/system/ip-pools-service/ranges/remove",
        tags = ["system/ip-pools"],
    }]
    async fn ip_pool_service_range_remove(
        rqctx: RequestContext<Self::Context>,
        range_params: TypedBody<shared::IpRange>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    // Floating IP Addresses

    /// List floating IPs
    #[endpoint {
        method = GET,
        path = "/v1/floating-ips",
        tags = ["floating-ips"],
    }]
    async fn floating_ip_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<views::FloatingIp>>, HttpError>;

    /// Create floating IP
    #[endpoint {
        method = POST,
        path = "/v1/floating-ips",
        tags = ["floating-ips"],
    }]
    async fn floating_ip_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::ProjectSelector>,
        floating_params: TypedBody<params::FloatingIpCreate>,
    ) -> Result<HttpResponseCreated<views::FloatingIp>, HttpError>;

    /// Update floating IP
    #[endpoint {
        method = PUT,
        path = "/v1/floating-ips/{floating_ip}",
        tags = ["floating-ips"],
    }]
    async fn floating_ip_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::FloatingIpPath>,
        query_params: Query<params::OptionalProjectSelector>,
        updated_floating_ip: TypedBody<params::FloatingIpUpdate>,
    ) -> Result<HttpResponseOk<views::FloatingIp>, HttpError>;

    /// Delete floating IP
    #[endpoint {
        method = DELETE,
        path = "/v1/floating-ips/{floating_ip}",
        tags = ["floating-ips"],
    }]
    async fn floating_ip_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::FloatingIpPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Fetch floating IP
    #[endpoint {
        method = GET,
        path = "/v1/floating-ips/{floating_ip}",
        tags = ["floating-ips"]
    }]
    async fn floating_ip_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::FloatingIpPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<views::FloatingIp>, HttpError>;

    /// Attach floating IP
    ///
    /// Attach floating IP to an instance or other resource.
    #[endpoint {
        method = POST,
        path = "/v1/floating-ips/{floating_ip}/attach",
        tags = ["floating-ips"],
    }]
    async fn floating_ip_attach(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::FloatingIpPath>,
        query_params: Query<params::OptionalProjectSelector>,
        target: TypedBody<params::FloatingIpAttach>,
    ) -> Result<HttpResponseAccepted<views::FloatingIp>, HttpError>;

    /// Detach floating IP
    ///
    // Detach floating IP from instance or other resource.
    #[endpoint {
        method = POST,
        path = "/v1/floating-ips/{floating_ip}/detach",
        tags = ["floating-ips"],
    }]
    async fn floating_ip_detach(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::FloatingIpPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseAccepted<views::FloatingIp>, HttpError>;

    // Disks

    /// List disks
    #[endpoint {
        method = GET,
        path = "/v1/disks",
        tags = ["disks"],
    }]
    async fn disk_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<Disk>>, HttpError>;

    // TODO-correctness See note about instance create.  This should be async.
    /// Create a disk
    #[endpoint {
        method = POST,
        path = "/v1/disks",
        tags = ["disks"]
    }]
    async fn disk_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::ProjectSelector>,
        new_disk: TypedBody<params::DiskCreate>,
    ) -> Result<HttpResponseCreated<Disk>, HttpError>;

    /// Fetch disk
    #[endpoint {
        method = GET,
        path = "/v1/disks/{disk}",
        tags = ["disks"]
    }]
    async fn disk_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::DiskPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<Disk>, HttpError>;

    /// Delete disk
    #[endpoint {
        method = DELETE,
        path = "/v1/disks/{disk}",
        tags = ["disks"],
    }]
    async fn disk_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::DiskPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Fetch disk metrics
    #[endpoint {
        method = GET,
        path = "/v1/disks/{disk}/metrics/{metric}",
        tags = ["disks"],
    }]
    async fn disk_metrics_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::DiskMetricsPath>,
        query_params: Query<
            PaginationParams<params::ResourceMetrics, params::ResourceMetrics>,
        >,
        selector_params: Query<params::OptionalProjectSelector>,
    ) -> Result<
        HttpResponseOk<ResultsPage<oximeter_types::Measurement>>,
        HttpError,
    >;

    /// Start importing blocks into disk
    ///
    /// Start the process of importing blocks into a disk
    #[endpoint {
        method = POST,
        path = "/v1/disks/{disk}/bulk-write-start",
        tags = ["disks"],
    }]
    async fn disk_bulk_write_import_start(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::DiskPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Import blocks into disk
    #[endpoint {
        method = POST,
        path = "/v1/disks/{disk}/bulk-write",
        tags = ["disks"],
        request_body_max_bytes = DISK_BULK_WRITE_MAX_BYTES,
    }]
    async fn disk_bulk_write_import(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::DiskPath>,
        query_params: Query<params::OptionalProjectSelector>,
        import_params: TypedBody<params::ImportBlocksBulkWrite>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Stop importing blocks into disk
    ///
    /// Stop the process of importing blocks into a disk
    #[endpoint {
        method = POST,
        path = "/v1/disks/{disk}/bulk-write-stop",
        tags = ["disks"],
    }]
    async fn disk_bulk_write_import_stop(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::DiskPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Confirm disk block import completion
    #[endpoint {
        method = POST,
        path = "/v1/disks/{disk}/finalize",
        tags = ["disks"],
    }]
    async fn disk_finalize_import(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::DiskPath>,
        query_params: Query<params::OptionalProjectSelector>,
        finalize_params: TypedBody<params::FinalizeDisk>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    // Instances

    /// List instances
    #[endpoint {
        method = GET,
        path = "/v1/instances",
        tags = ["instances"],
    }]
    async fn instance_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<Instance>>, HttpError>;

    /// Create instance
    #[endpoint {
        method = POST,
        path = "/v1/instances",
        tags = ["instances"],
    }]
    async fn instance_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::ProjectSelector>,
        new_instance: TypedBody<params::InstanceCreate>,
    ) -> Result<HttpResponseCreated<Instance>, HttpError>;

    /// Fetch instance
    #[endpoint {
        method = GET,
        path = "/v1/instances/{instance}",
        tags = ["instances"],
    }]
    async fn instance_view(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseOk<Instance>, HttpError>;

    /// Delete instance
    #[endpoint {
        method = DELETE,
        path = "/v1/instances/{instance}",
        tags = ["instances"],
    }]
    async fn instance_delete(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Update instance
    #[endpoint {
        method = PUT,
        path = "/v1/instances/{instance}",
        tags = ["instances"],
    }]
    async fn instance_update(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::InstancePath>,
        instance_config: TypedBody<params::InstanceUpdate>,
    ) -> Result<HttpResponseOk<Instance>, HttpError>;

    /// Reboot an instance
    #[endpoint {
        method = POST,
        path = "/v1/instances/{instance}/reboot",
        tags = ["instances"],
    }]
    async fn instance_reboot(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseAccepted<Instance>, HttpError>;

    /// Boot instance
    #[endpoint {
        method = POST,
        path = "/v1/instances/{instance}/start",
        tags = ["instances"],
    }]
    async fn instance_start(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseAccepted<Instance>, HttpError>;

    /// Stop instance
    #[endpoint {
        method = POST,
        path = "/v1/instances/{instance}/stop",
        tags = ["instances"],
    }]
    async fn instance_stop(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseAccepted<Instance>, HttpError>;

    /// Fetch instance serial console
    #[endpoint {
        method = GET,
        path = "/v1/instances/{instance}/serial-console",
        tags = ["instances"],
    }]
    async fn instance_serial_console(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::InstancePath>,
        query_params: Query<params::InstanceSerialConsoleRequest>,
    ) -> Result<HttpResponseOk<params::InstanceSerialConsoleData>, HttpError>;

    /// Stream instance serial console
    #[channel {
        protocol = WEBSOCKETS,
        path = "/v1/instances/{instance}/serial-console/stream",
        tags = ["instances"],
    }]
    async fn instance_serial_console_stream(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::InstancePath>,
        query_params: Query<params::InstanceSerialConsoleStreamRequest>,
        conn: WebsocketConnection,
    ) -> WebsocketChannelResult;

    /// List SSH public keys for instance
    ///
    /// List SSH public keys injected via cloud-init during instance creation.
    /// Note that this list is a snapshot in time and will not reflect updates
    /// made after the instance is created.
    #[endpoint {
        method = GET,
        path = "/v1/instances/{instance}/ssh-public-keys",
        tags = ["instances"],
    }]
    async fn instance_ssh_public_key_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::InstancePath>,
        query_params: Query<
            PaginatedByNameOrId<params::OptionalProjectSelector>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<views::SshKey>>, HttpError>;

    /// List disks for instance
    #[endpoint {
        method = GET,
        path = "/v1/instances/{instance}/disks",
        tags = ["instances"],
    }]
    async fn instance_disk_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<params::OptionalProjectSelector>,
        >,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseOk<ResultsPage<Disk>>, HttpError>;

    /// Attach disk to instance
    #[endpoint {
        method = POST,
        path = "/v1/instances/{instance}/disks/attach",
        tags = ["instances"],
    }]
    async fn instance_disk_attach(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::InstancePath>,
        query_params: Query<params::OptionalProjectSelector>,
        disk_to_attach: TypedBody<params::DiskPath>,
    ) -> Result<HttpResponseAccepted<Disk>, HttpError>;

    /// Detach disk from instance
    #[endpoint {
        method = POST,
        path = "/v1/instances/{instance}/disks/detach",
        tags = ["instances"],
    }]
    async fn instance_disk_detach(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::InstancePath>,
        query_params: Query<params::OptionalProjectSelector>,
        disk_to_detach: TypedBody<params::DiskPath>,
    ) -> Result<HttpResponseAccepted<Disk>, HttpError>;

    /// List affinity groups containing instance
    #[endpoint {
        method = GET,
        path = "/v1/instances/{instance}/affinity-groups",
        tags = ["hidden"],
    }]
    async fn instance_affinity_group_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<params::OptionalProjectSelector>,
        >,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseOk<ResultsPage<views::AffinityGroup>>, HttpError>;

    /// List anti-affinity groups containing instance
    #[endpoint {
        method = GET,
        path = "/v1/instances/{instance}/anti-affinity-groups",
        tags = ["instances"],
    }]
    async fn instance_anti_affinity_group_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<params::OptionalProjectSelector>,
        >,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseOk<ResultsPage<views::AntiAffinityGroup>>, HttpError>;

    // Affinity Groups

    /// List affinity groups
    #[endpoint {
        method = GET,
        path = "/v1/affinity-groups",
        tags = ["hidden"],
    }]
    async fn affinity_group_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<views::AffinityGroup>>, HttpError>;

    /// Fetch affinity group
    #[endpoint {
        method = GET,
        path = "/v1/affinity-groups/{affinity_group}",
        tags = ["hidden"],
    }]
    async fn affinity_group_view(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::AffinityGroupPath>,
    ) -> Result<HttpResponseOk<views::AffinityGroup>, HttpError>;

    /// List affinity group members
    #[endpoint {
        method = GET,
        path = "/v1/affinity-groups/{affinity_group}/members",
        tags = ["hidden"],
    }]
    async fn affinity_group_member_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<params::OptionalProjectSelector>,
        >,
        path_params: Path<params::AffinityGroupPath>,
    ) -> Result<HttpResponseOk<ResultsPage<AffinityGroupMember>>, HttpError>;

    /// Fetch affinity group member
    #[endpoint {
        method = GET,
        path = "/v1/affinity-groups/{affinity_group}/members/instance/{instance}",
        tags = ["hidden"],
    }]
    async fn affinity_group_member_instance_view(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::AffinityInstanceGroupMemberPath>,
    ) -> Result<HttpResponseOk<AffinityGroupMember>, HttpError>;

    /// Add member to affinity group
    #[endpoint {
        method = POST,
        path = "/v1/affinity-groups/{affinity_group}/members/instance/{instance}",
        tags = ["hidden"],
    }]
    async fn affinity_group_member_instance_add(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::AffinityInstanceGroupMemberPath>,
    ) -> Result<HttpResponseCreated<AffinityGroupMember>, HttpError>;

    /// Remove member from affinity group
    #[endpoint {
        method = DELETE,
        path = "/v1/affinity-groups/{affinity_group}/members/instance/{instance}",
        tags = ["hidden"],
    }]
    async fn affinity_group_member_instance_delete(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::AffinityInstanceGroupMemberPath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Create affinity group
    #[endpoint {
        method = POST,
        path = "/v1/affinity-groups",
        tags = ["hidden"],
    }]
    async fn affinity_group_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::ProjectSelector>,
        new_affinity_group_params: TypedBody<params::AffinityGroupCreate>,
    ) -> Result<HttpResponseCreated<views::AffinityGroup>, HttpError>;

    /// Update affinity group
    #[endpoint {
        method = PUT,
        path = "/v1/affinity-groups/{affinity_group}",
        tags = ["hidden"],
    }]
    async fn affinity_group_update(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::AffinityGroupPath>,
        updated_group: TypedBody<params::AffinityGroupUpdate>,
    ) -> Result<HttpResponseOk<views::AffinityGroup>, HttpError>;

    /// Delete affinity group
    #[endpoint {
        method = DELETE,
        path = "/v1/affinity-groups/{affinity_group}",
        tags = ["hidden"],
    }]
    async fn affinity_group_delete(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::AffinityGroupPath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// List anti-affinity groups
    #[endpoint {
        method = GET,
        path = "/v1/anti-affinity-groups",
        tags = ["affinity"],
    }]
    async fn anti_affinity_group_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<views::AntiAffinityGroup>>, HttpError>;

    /// Fetch anti-affinity group
    #[endpoint {
        method = GET,
        path = "/v1/anti-affinity-groups/{anti_affinity_group}",
        tags = ["affinity"],
    }]
    async fn anti_affinity_group_view(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::AntiAffinityGroupPath>,
    ) -> Result<HttpResponseOk<views::AntiAffinityGroup>, HttpError>;

    /// List anti-affinity group members
    #[endpoint {
        method = GET,
        path = "/v1/anti-affinity-groups/{anti_affinity_group}/members",
        tags = ["affinity"],
    }]
    async fn anti_affinity_group_member_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<params::OptionalProjectSelector>,
        >,
        path_params: Path<params::AntiAffinityGroupPath>,
    ) -> Result<HttpResponseOk<ResultsPage<AntiAffinityGroupMember>>, HttpError>;

    /// Fetch anti-affinity group member
    #[endpoint {
        method = GET,
        path = "/v1/anti-affinity-groups/{anti_affinity_group}/members/instance/{instance}",
        tags = ["affinity"],
    }]
    async fn anti_affinity_group_member_instance_view(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::AntiAffinityInstanceGroupMemberPath>,
    ) -> Result<HttpResponseOk<AntiAffinityGroupMember>, HttpError>;

    /// Add member to anti-affinity group
    #[endpoint {
        method = POST,
        path = "/v1/anti-affinity-groups/{anti_affinity_group}/members/instance/{instance}",
        tags = ["affinity"],
    }]
    async fn anti_affinity_group_member_instance_add(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::AntiAffinityInstanceGroupMemberPath>,
    ) -> Result<HttpResponseCreated<AntiAffinityGroupMember>, HttpError>;

    /// Remove member from anti-affinity group
    #[endpoint {
        method = DELETE,
        path = "/v1/anti-affinity-groups/{anti_affinity_group}/members/instance/{instance}",
        tags = ["affinity"],
    }]
    async fn anti_affinity_group_member_instance_delete(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::AntiAffinityInstanceGroupMemberPath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Create anti-affinity group
    #[endpoint {
        method = POST,
        path = "/v1/anti-affinity-groups",
        tags = ["affinity"],
    }]
    async fn anti_affinity_group_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::ProjectSelector>,
        new_affinity_group_params: TypedBody<params::AntiAffinityGroupCreate>,
    ) -> Result<HttpResponseCreated<views::AntiAffinityGroup>, HttpError>;

    /// Update anti-affinity group
    #[endpoint {
        method = PUT,
        path = "/v1/anti-affinity-groups/{anti_affinity_group}",
        tags = ["affinity"],
    }]
    async fn anti_affinity_group_update(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::AntiAffinityGroupPath>,
        updated_group: TypedBody<params::AntiAffinityGroupUpdate>,
    ) -> Result<HttpResponseOk<views::AntiAffinityGroup>, HttpError>;

    /// Delete anti-affinity group
    #[endpoint {
        method = DELETE,
        path = "/v1/anti-affinity-groups/{anti_affinity_group}",
        tags = ["affinity"],
    }]
    async fn anti_affinity_group_delete(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::AntiAffinityGroupPath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    // Certificates

    /// List certificates for external endpoints
    ///
    /// Returns a list of TLS certificates used for the external API (for the
    /// current Silo).  These are sorted by creation date, with the most recent
    /// certificates appearing first.
    #[endpoint {
        method = GET,
        path = "/v1/certificates",
        tags = ["silos"],
    }]
    async fn certificate_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<views::Certificate>>, HttpError>;

    /// Create new system-wide x.509 certificate
    ///
    /// This certificate is automatically used by the Oxide Control plane to serve
    /// external connections.
    #[endpoint {
        method = POST,
        path = "/v1/certificates",
        tags = ["silos"]
    }]
    async fn certificate_create(
        rqctx: RequestContext<Self::Context>,
        new_cert: TypedBody<params::CertificateCreate>,
    ) -> Result<HttpResponseCreated<views::Certificate>, HttpError>;

    /// Fetch certificate
    ///
    /// Returns the details of a specific certificate
    #[endpoint {
        method = GET,
        path = "/v1/certificates/{certificate}",
        tags = ["silos"],
    }]
    async fn certificate_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::CertificatePath>,
    ) -> Result<HttpResponseOk<views::Certificate>, HttpError>;

    /// Delete certificate
    ///
    /// Permanently delete a certificate. This operation cannot be undone.
    #[endpoint {
        method = DELETE,
        path = "/v1/certificates/{certificate}",
        tags = ["silos"],
    }]
    async fn certificate_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::CertificatePath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Create address lot
    #[endpoint {
        method = POST,
        path = "/v1/system/networking/address-lot",
        tags = ["system/networking"],
    }]
    async fn networking_address_lot_create(
        rqctx: RequestContext<Self::Context>,
        new_address_lot: TypedBody<params::AddressLotCreate>,
    ) -> Result<HttpResponseCreated<AddressLotCreateResponse>, HttpError>;

    /// Delete address lot
    #[endpoint {
        method = DELETE,
        path = "/v1/system/networking/address-lot/{address_lot}",
        tags = ["system/networking"],
    }]
    async fn networking_address_lot_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::AddressLotPath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// List address lots
    #[endpoint {
        method = GET,
        path = "/v1/system/networking/address-lot",
        tags = ["system/networking"],
    }]
    async fn networking_address_lot_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<AddressLot>>, HttpError>;

    /// List blocks in address lot
    #[endpoint {
        method = GET,
        path = "/v1/system/networking/address-lot/{address_lot}/blocks",
        tags = ["system/networking"],
    }]
    async fn networking_address_lot_block_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::AddressLotPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<AddressLotBlock>>, HttpError>;

    /// Create loopback address
    #[endpoint {
        method = POST,
        path = "/v1/system/networking/loopback-address",
        tags = ["system/networking"],
    }]
    async fn networking_loopback_address_create(
        rqctx: RequestContext<Self::Context>,
        new_loopback_address: TypedBody<params::LoopbackAddressCreate>,
    ) -> Result<HttpResponseCreated<LoopbackAddress>, HttpError>;

    /// Delete loopback address
    #[endpoint {
        method = DELETE,
        path = "/v1/system/networking/loopback-address/{rack_id}/{switch_location}/{address}/{subnet_mask}",
        tags = ["system/networking"],
    }]
    async fn networking_loopback_address_delete(
        rqctx: RequestContext<Self::Context>,
        path: Path<params::LoopbackAddressPath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// List loopback addresses
    #[endpoint {
        method = GET,
        path = "/v1/system/networking/loopback-address",
        tags = ["system/networking"],
    }]
    async fn networking_loopback_address_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<LoopbackAddress>>, HttpError>;

    /// Create switch port settings
    #[endpoint {
        method = POST,
        path = "/v1/system/networking/switch-port-settings",
        tags = ["system/networking"],
    }]
    async fn networking_switch_port_settings_create(
        rqctx: RequestContext<Self::Context>,
        new_settings: TypedBody<params::SwitchPortSettingsCreate>,
    ) -> Result<HttpResponseCreated<SwitchPortSettings>, HttpError>;

    /// Delete switch port settings
    #[endpoint {
        method = DELETE,
        path = "/v1/system/networking/switch-port-settings",
        tags = ["system/networking"],
    }]
    async fn networking_switch_port_settings_delete(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::SwitchPortSettingsSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// List switch port settings
    #[endpoint {
        method = GET,
        path = "/v1/system/networking/switch-port-settings",
        tags = ["system/networking"],
    }]
    async fn networking_switch_port_settings_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<params::SwitchPortSettingsSelector>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<SwitchPortSettingsIdentity>>,
        HttpError,
    >;

    /// Get information about switch port
    #[endpoint {
        method = GET,
        path = "/v1/system/networking/switch-port-settings/{port}",
        tags = ["system/networking"],
    }]
    async fn networking_switch_port_settings_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SwitchPortSettingsInfoSelector>,
    ) -> Result<HttpResponseOk<SwitchPortSettings>, HttpError>;

    /// List switch ports
    #[endpoint {
        method = GET,
        path = "/v1/system/hardware/switch-port",
        tags = ["system/hardware"],
    }]
    async fn networking_switch_port_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById<params::SwitchPortPageSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<SwitchPort>>, HttpError>;

    /// Get switch port status
    #[endpoint {
        method = GET,
        path = "/v1/system/hardware/switch-port/{port}/status",
        tags = ["system/hardware"],
    }]
    async fn networking_switch_port_status(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SwitchPortPathSelector>,
        query_params: Query<params::SwitchPortSelector>,
    ) -> Result<HttpResponseOk<shared::SwitchLinkState>, HttpError>;

    /// Apply switch port settings
    #[endpoint {
        method = POST,
        path = "/v1/system/hardware/switch-port/{port}/settings",
        tags = ["system/hardware"],
    }]
    async fn networking_switch_port_apply_settings(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SwitchPortPathSelector>,
        query_params: Query<params::SwitchPortSelector>,
        settings_body: TypedBody<params::SwitchPortApplySettings>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Clear switch port settings
    #[endpoint {
        method = DELETE,
        path = "/v1/system/hardware/switch-port/{port}/settings",
        tags = ["system/hardware"],
    }]
    async fn networking_switch_port_clear_settings(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SwitchPortPathSelector>,
        query_params: Query<params::SwitchPortSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Fetch the LLDP configuration for a switch port
    #[endpoint {
        method = GET,
        path = "/v1/system/hardware/switch-port/{port}/lldp/config",
        tags = ["system/networking"],
    }]
    async fn networking_switch_port_lldp_config_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SwitchPortPathSelector>,
        query_params: Query<params::SwitchPortSelector>,
    ) -> Result<HttpResponseOk<LldpLinkConfig>, HttpError>;

    /// Update the LLDP configuration for a switch port
    #[endpoint {
        method = POST,
        path = "/v1/system/hardware/switch-port/{port}/lldp/config",
        tags = ["system/networking"],
    }]
    async fn networking_switch_port_lldp_config_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SwitchPortPathSelector>,
        query_params: Query<params::SwitchPortSelector>,
        config: TypedBody<LldpLinkConfig>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Fetch the LLDP neighbors seen on a switch port
    #[endpoint {
        method = GET,
        path = "/v1/system/hardware/rack-switch-port/{rack_id}/{switch_location}/{port}/lldp/neighbors",
        tags = ["system/networking"],
    }]
    async fn networking_switch_port_lldp_neighbors(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::LldpPortPathSelector>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<LldpNeighbor>>, HttpError>;

    /// Create new BGP configuration
    #[endpoint {
        method = POST,
        path = "/v1/system/networking/bgp",
        tags = ["system/networking"],
    }]
    async fn networking_bgp_config_create(
        rqctx: RequestContext<Self::Context>,
        config: TypedBody<params::BgpConfigCreate>,
    ) -> Result<HttpResponseCreated<BgpConfig>, HttpError>;

    /// List BGP configurations
    #[endpoint {
        method = GET,
        path = "/v1/system/networking/bgp",
        tags = ["system/networking"],
    }]
    async fn networking_bgp_config_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<BgpConfig>>, HttpError>;

    //TODO pagination? the normal by-name/by-id stuff does not work here
    /// Get BGP peer status
    #[endpoint {
        method = GET,
        path = "/v1/system/networking/bgp-status",
        tags = ["system/networking"],
    }]
    async fn networking_bgp_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<BgpPeerStatus>>, HttpError>;

    //TODO pagination? the normal by-name/by-id stuff does not work here
    /// Get BGP exported routes
    #[endpoint {
        method = GET,
        path = "/v1/system/networking/bgp-exported",
        tags = ["system/networking"],
    }]
    async fn networking_bgp_exported(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BgpExported>, HttpError>;

    /// Get BGP router message history
    #[endpoint {
        method = GET,
        path = "/v1/system/networking/bgp-message-history",
        tags = ["system/networking"],
    }]
    async fn networking_bgp_message_history(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::BgpRouteSelector>,
    ) -> Result<HttpResponseOk<AggregateBgpMessageHistory>, HttpError>;

    //TODO pagination? the normal by-name/by-id stuff does not work here
    /// Get imported IPv4 BGP routes
    #[endpoint {
        method = GET,
        path = "/v1/system/networking/bgp-routes-ipv4",
        tags = ["system/networking"],
    }]
    async fn networking_bgp_imported_routes_ipv4(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::BgpRouteSelector>,
    ) -> Result<HttpResponseOk<Vec<BgpImportedRouteIpv4>>, HttpError>;

    /// Delete BGP configuration
    #[endpoint {
        method = DELETE,
        path = "/v1/system/networking/bgp",
        tags = ["system/networking"],
    }]
    async fn networking_bgp_config_delete(
        rqctx: RequestContext<Self::Context>,
        sel: Query<params::BgpConfigSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Update BGP announce set
    ///
    /// If the announce set exists, this endpoint replaces the existing announce
    /// set with the one specified.
    #[endpoint {
        method = PUT,
        path = "/v1/system/networking/bgp-announce-set",
        tags = ["system/networking"],
    }]
    async fn networking_bgp_announce_set_update(
        rqctx: RequestContext<Self::Context>,
        config: TypedBody<params::BgpAnnounceSetCreate>,
    ) -> Result<HttpResponseOk<BgpAnnounceSet>, HttpError>;

    /// List BGP announce sets
    #[endpoint {
        method = GET,
        path = "/v1/system/networking/bgp-announce-set",
        tags = ["system/networking"],
    }]
    async fn networking_bgp_announce_set_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<Vec<BgpAnnounceSet>>, HttpError>;

    /// Delete BGP announce set
    #[endpoint {
        method = DELETE,
        path = "/v1/system/networking/bgp-announce-set/{announce_set}",
        tags = ["system/networking"],
    }]
    async fn networking_bgp_announce_set_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::BgpAnnounceSetSelector>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    // TODO: is pagination necessary here? How large do we expect the list of
    // announcements to become in real usage?
    /// Get originated routes for a specified BGP announce set
    #[endpoint {
        method = GET,
        path = "/v1/system/networking/bgp-announce-set/{announce_set}/announcement",
        tags = ["system/networking"],
    }]
    async fn networking_bgp_announcement_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::BgpAnnounceSetSelector>,
    ) -> Result<HttpResponseOk<Vec<BgpAnnouncement>>, HttpError>;

    /// Enable a BFD session
    #[endpoint {
        method = POST,
        path = "/v1/system/networking/bfd-enable",
        tags = ["system/networking"],
    }]
    async fn networking_bfd_enable(
        rqctx: RequestContext<Self::Context>,
        session: TypedBody<params::BfdSessionEnable>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Disable a BFD session
    #[endpoint {
        method = POST,
        path = "/v1/system/networking/bfd-disable",
        tags = ["system/networking"],
    }]
    async fn networking_bfd_disable(
        rqctx: RequestContext<Self::Context>,
        session: TypedBody<params::BfdSessionDisable>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Get BFD status
    #[endpoint {
        method = GET,
        path = "/v1/system/networking/bfd-status",
        tags = ["system/networking"],
    }]
    async fn networking_bfd_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<shared::BfdStatus>>, HttpError>;

    /// Get user-facing services IP allowlist
    #[endpoint {
        method = GET,
        path = "/v1/system/networking/allow-list",
        tags = ["system/networking"],
    }]
    async fn networking_allow_list_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<views::AllowList>, HttpError>;

    /// Update user-facing services IP allowlist
    #[endpoint {
        method = PUT,
        path = "/v1/system/networking/allow-list",
        tags = ["system/networking"],
    }]
    async fn networking_allow_list_update(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<params::AllowListUpdate>,
    ) -> Result<HttpResponseOk<views::AllowList>, HttpError>;

    // Images

    /// List images
    ///
    /// List images which are global or scoped to the specified project. The images
    /// are returned sorted by creation date, with the most recent images appearing first.
    #[endpoint {
        method = GET,
        path = "/v1/images",
        tags = ["images"],
    }]
    async fn image_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<params::OptionalProjectSelector>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<views::Image>>, HttpError>;

    /// Create image
    ///
    /// Create a new image in a project.
    #[endpoint {
        method = POST,
        path = "/v1/images",
        tags = ["images"]
    }]
    async fn image_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        new_image: TypedBody<params::ImageCreate>,
    ) -> Result<HttpResponseCreated<views::Image>, HttpError>;

    /// Fetch image
    ///
    /// Fetch the details for a specific image in a project.
    #[endpoint {
        method = GET,
        path = "/v1/images/{image}",
        tags = ["images"],
    }]
    async fn image_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::ImagePath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<views::Image>, HttpError>;

    /// Delete image
    ///
    /// Permanently delete an image from a project. This operation cannot be undone.
    /// Any instances in the project using the image will continue to run, however
    /// new instances can not be created with this image.
    #[endpoint {
        method = DELETE,
        path = "/v1/images/{image}",
        tags = ["images"],
    }]
    async fn image_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::ImagePath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Promote project image
    ///
    /// Promote project image to be visible to all projects in the silo
    #[endpoint {
        method = POST,
        path = "/v1/images/{image}/promote",
        tags = ["images"]
    }]
    async fn image_promote(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::ImagePath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseAccepted<views::Image>, HttpError>;

    /// Demote silo image
    ///
    /// Demote silo image to be visible only to a specified project
    #[endpoint {
        method = POST,
        path = "/v1/images/{image}/demote",
        tags = ["images"]
    }]
    async fn image_demote(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::ImagePath>,
        query_params: Query<params::ProjectSelector>,
    ) -> Result<HttpResponseAccepted<views::Image>, HttpError>;

    /// List network interfaces
    #[endpoint {
        method = GET,
        path = "/v1/network-interfaces",
        tags = ["instances"],
    }]
    async fn instance_network_interface_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<params::InstanceSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<InstanceNetworkInterface>>, HttpError>;

    /// Create network interface
    #[endpoint {
        method = POST,
        path = "/v1/network-interfaces",
        tags = ["instances"],
    }]
    async fn instance_network_interface_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::InstanceSelector>,
        interface_params: TypedBody<params::InstanceNetworkInterfaceCreate>,
    ) -> Result<HttpResponseCreated<InstanceNetworkInterface>, HttpError>;

    /// Delete network interface
    ///
    /// Note that the primary interface for an instance cannot be deleted if there
    /// are any secondary interfaces. A new primary interface must be designated
    /// first. The primary interface can be deleted if there are no secondary
    /// interfaces.
    #[endpoint {
        method = DELETE,
        path = "/v1/network-interfaces/{interface}",
        tags = ["instances"],
    }]
    async fn instance_network_interface_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::NetworkInterfacePath>,
        query_params: Query<params::OptionalInstanceSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Fetch network interface
    #[endpoint {
        method = GET,
        path = "/v1/network-interfaces/{interface}",
        tags = ["instances"],
    }]
    async fn instance_network_interface_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::NetworkInterfacePath>,
        query_params: Query<params::OptionalInstanceSelector>,
    ) -> Result<HttpResponseOk<InstanceNetworkInterface>, HttpError>;

    /// Update network interface
    #[endpoint {
        method = PUT,
        path = "/v1/network-interfaces/{interface}",
        tags = ["instances"],
    }]
    async fn instance_network_interface_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::NetworkInterfacePath>,
        query_params: Query<params::OptionalInstanceSelector>,
        updated_iface: TypedBody<params::InstanceNetworkInterfaceUpdate>,
    ) -> Result<HttpResponseOk<InstanceNetworkInterface>, HttpError>;

    // External IP addresses for instances

    /// List external IP addresses
    #[endpoint {
        method = GET,
        path = "/v1/instances/{instance}/external-ips",
        tags = ["instances"],
    }]
    async fn instance_external_ip_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::OptionalProjectSelector>,
        path_params: Path<params::InstancePath>,
    ) -> Result<HttpResponseOk<ResultsPage<views::ExternalIp>>, HttpError>;

    /// Allocate and attach ephemeral IP to instance
    #[endpoint {
        method = POST,
        path = "/v1/instances/{instance}/external-ips/ephemeral",
        tags = ["instances"],
    }]
    async fn instance_ephemeral_ip_attach(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::InstancePath>,
        query_params: Query<params::OptionalProjectSelector>,
        ip_to_create: TypedBody<params::EphemeralIpCreate>,
    ) -> Result<HttpResponseAccepted<views::ExternalIp>, HttpError>;

    /// Detach and deallocate ephemeral IP from instance
    #[endpoint {
        method = DELETE,
        path = "/v1/instances/{instance}/external-ips/ephemeral",
        tags = ["instances"],
    }]
    async fn instance_ephemeral_ip_detach(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::InstancePath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    // Snapshots

    /// List snapshots
    #[endpoint {
        method = GET,
        path = "/v1/snapshots",
        tags = ["snapshots"],
    }]
    async fn snapshot_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<views::Snapshot>>, HttpError>;

    /// Create snapshot
    ///
    /// Creates a point-in-time snapshot from a disk.
    #[endpoint {
        method = POST,
        path = "/v1/snapshots",
        tags = ["snapshots"],
    }]
    async fn snapshot_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::ProjectSelector>,
        new_snapshot: TypedBody<params::SnapshotCreate>,
    ) -> Result<HttpResponseCreated<views::Snapshot>, HttpError>;

    /// Fetch snapshot
    #[endpoint {
        method = GET,
        path = "/v1/snapshots/{snapshot}",
        tags = ["snapshots"],
    }]
    async fn snapshot_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SnapshotPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<views::Snapshot>, HttpError>;

    /// Delete snapshot
    #[endpoint {
        method = DELETE,
        path = "/v1/snapshots/{snapshot}",
        tags = ["snapshots"],
    }]
    async fn snapshot_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SnapshotPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    // VPCs

    /// List VPCs
    #[endpoint {
        method = GET,
        path = "/v1/vpcs",
        tags = ["vpcs"],
    }]
    async fn vpc_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<views::Vpc>>, HttpError>;

    /// Create VPC
    #[endpoint {
        method = POST,
        path = "/v1/vpcs",
        tags = ["vpcs"],
    }]
    async fn vpc_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::ProjectSelector>,
        body: TypedBody<params::VpcCreate>,
    ) -> Result<HttpResponseCreated<views::Vpc>, HttpError>;

    /// Fetch VPC
    #[endpoint {
        method = GET,
        path = "/v1/vpcs/{vpc}",
        tags = ["vpcs"],
    }]
    async fn vpc_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::VpcPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseOk<views::Vpc>, HttpError>;

    /// Update a VPC
    #[endpoint {
        method = PUT,
        path = "/v1/vpcs/{vpc}",
        tags = ["vpcs"],
    }]
    async fn vpc_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::VpcPath>,
        query_params: Query<params::OptionalProjectSelector>,
        updated_vpc: TypedBody<params::VpcUpdate>,
    ) -> Result<HttpResponseOk<views::Vpc>, HttpError>;

    /// Delete VPC
    #[endpoint {
        method = DELETE,
        path = "/v1/vpcs/{vpc}",
        tags = ["vpcs"],
    }]
    async fn vpc_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::VpcPath>,
        query_params: Query<params::OptionalProjectSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// List subnets
    #[endpoint {
        method = GET,
        path = "/v1/vpc-subnets",
        tags = ["vpcs"],
    }]
    async fn vpc_subnet_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<params::VpcSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<views::VpcSubnet>>, HttpError>;

    /// Create subnet
    #[endpoint {
        method = POST,
        path = "/v1/vpc-subnets",
        tags = ["vpcs"],
    }]
    async fn vpc_subnet_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::VpcSelector>,
        create_params: TypedBody<params::VpcSubnetCreate>,
    ) -> Result<HttpResponseCreated<views::VpcSubnet>, HttpError>;

    /// Fetch subnet
    #[endpoint {
        method = GET,
        path = "/v1/vpc-subnets/{subnet}",
        tags = ["vpcs"],
    }]
    async fn vpc_subnet_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SubnetPath>,
        query_params: Query<params::OptionalVpcSelector>,
    ) -> Result<HttpResponseOk<views::VpcSubnet>, HttpError>;

    /// Delete subnet
    #[endpoint {
        method = DELETE,
        path = "/v1/vpc-subnets/{subnet}",
        tags = ["vpcs"],
    }]
    async fn vpc_subnet_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SubnetPath>,
        query_params: Query<params::OptionalVpcSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Update subnet
    #[endpoint {
        method = PUT,
        path = "/v1/vpc-subnets/{subnet}",
        tags = ["vpcs"],
    }]
    async fn vpc_subnet_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SubnetPath>,
        query_params: Query<params::OptionalVpcSelector>,
        subnet_params: TypedBody<params::VpcSubnetUpdate>,
    ) -> Result<HttpResponseOk<views::VpcSubnet>, HttpError>;

    // This endpoint is likely temporary. We would rather list all IPs allocated in
    // a subnet whether they come from NICs or something else. See
    // https://github.com/oxidecomputer/omicron/issues/2476

    /// List network interfaces
    #[endpoint {
        method = GET,
        path = "/v1/vpc-subnets/{subnet}/network-interfaces",
        tags = ["vpcs"],
    }]
    async fn vpc_subnet_list_network_interfaces(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SubnetPath>,
        query_params: Query<PaginatedByNameOrId<params::OptionalVpcSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<InstanceNetworkInterface>>, HttpError>;

    // VPC Firewalls

    /// List firewall rules
    #[endpoint {
        method = GET,
        path = "/v1/vpc-firewall-rules",
        tags = ["vpcs"],
    }]
    async fn vpc_firewall_rules_view(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::VpcSelector>,
    ) -> Result<HttpResponseOk<VpcFirewallRules>, HttpError>;

    // Note: the limits in the below comment come from the firewall rules model
    // file, nexus/db-model/src/vpc_firewall_rule.rs.

    /// Replace firewall rules
    ///
    /// The maximum number of rules per VPC is 1024.
    ///
    /// Targets are used to specify the set of instances to which a firewall rule
    /// applies. You can target instances directly by name, or specify a VPC, VPC
    /// subnet, IP, or IP subnet, which will apply the rule to traffic going to
    /// all matching instances. Targets are additive: the rule applies to instances
    /// matching ANY target. The maximum number of targets is 256.
    ///
    /// Filters reduce the scope of a firewall rule. Without filters, the rule
    /// applies to all packets to the targets (or from the targets, if it's an
    /// outbound rule). With multiple filters, the rule applies only to packets
    /// matching ALL filters. The maximum number of each type of filter is 256.
    #[endpoint {
        method = PUT,
        path = "/v1/vpc-firewall-rules",
        tags = ["vpcs"],
    }]
    async fn vpc_firewall_rules_update(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::VpcSelector>,
        router_params: TypedBody<VpcFirewallRuleUpdateParams>,
    ) -> Result<HttpResponseOk<VpcFirewallRules>, HttpError>;

    // VPC Routers

    /// List routers
    #[endpoint {
        method = GET,
        path = "/v1/vpc-routers",
        tags = ["vpcs"],
    }]
    async fn vpc_router_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<params::VpcSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<views::VpcRouter>>, HttpError>;

    /// Fetch router
    #[endpoint {
        method = GET,
        path = "/v1/vpc-routers/{router}",
        tags = ["vpcs"],
    }]
    async fn vpc_router_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RouterPath>,
        query_params: Query<params::OptionalVpcSelector>,
    ) -> Result<HttpResponseOk<views::VpcRouter>, HttpError>;

    /// Create VPC router
    #[endpoint {
        method = POST,
        path = "/v1/vpc-routers",
        tags = ["vpcs"],
    }]
    async fn vpc_router_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::VpcSelector>,
        create_params: TypedBody<params::VpcRouterCreate>,
    ) -> Result<HttpResponseCreated<views::VpcRouter>, HttpError>;

    /// Delete router
    #[endpoint {
        method = DELETE,
        path = "/v1/vpc-routers/{router}",
        tags = ["vpcs"],
    }]
    async fn vpc_router_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RouterPath>,
        query_params: Query<params::OptionalVpcSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Update router
    #[endpoint {
        method = PUT,
        path = "/v1/vpc-routers/{router}",
        tags = ["vpcs"],
    }]
    async fn vpc_router_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RouterPath>,
        query_params: Query<params::OptionalVpcSelector>,
        router_params: TypedBody<params::VpcRouterUpdate>,
    ) -> Result<HttpResponseOk<views::VpcRouter>, HttpError>;

    /// List routes
    ///
    /// List the routes associated with a router in a particular VPC.
    #[endpoint {
        method = GET,
        path = "/v1/vpc-router-routes",
        tags = ["vpcs"],
    }]
    async fn vpc_router_route_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<params::RouterSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<RouterRoute>>, HttpError>;

    // Vpc Router Routes

    /// Fetch route
    #[endpoint {
        method = GET,
        path = "/v1/vpc-router-routes/{route}",
        tags = ["vpcs"],
    }]
    async fn vpc_router_route_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RoutePath>,
        query_params: Query<params::OptionalRouterSelector>,
    ) -> Result<HttpResponseOk<RouterRoute>, HttpError>;

    /// Create route
    #[endpoint {
        method = POST,
        path = "/v1/vpc-router-routes",
        tags = ["vpcs"],
    }]
    async fn vpc_router_route_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::RouterSelector>,
        create_params: TypedBody<params::RouterRouteCreate>,
    ) -> Result<HttpResponseCreated<RouterRoute>, HttpError>;

    /// Delete route
    #[endpoint {
        method = DELETE,
        path = "/v1/vpc-router-routes/{route}",
        tags = ["vpcs"],
    }]
    async fn vpc_router_route_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RoutePath>,
        query_params: Query<params::OptionalRouterSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Update route
    #[endpoint {
        method = PUT,
        path = "/v1/vpc-router-routes/{route}",
        tags = ["vpcs"],
    }]
    async fn vpc_router_route_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RoutePath>,
        query_params: Query<params::OptionalRouterSelector>,
        router_params: TypedBody<params::RouterRouteUpdate>,
    ) -> Result<HttpResponseOk<RouterRoute>, HttpError>;

    // Internet gateways

    /// List internet gateways
    #[endpoint {
        method = GET,
        path = "/v1/internet-gateways",
        tags = ["vpcs"],
    }]
    async fn internet_gateway_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<params::VpcSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<views::InternetGateway>>, HttpError>;

    /// Fetch internet gateway
    #[endpoint {
        method = GET,
        path = "/v1/internet-gateways/{gateway}",
        tags = ["vpcs"],
    }]
    async fn internet_gateway_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::InternetGatewayPath>,
        query_params: Query<params::OptionalVpcSelector>,
    ) -> Result<HttpResponseOk<views::InternetGateway>, HttpError>;

    /// Create VPC internet gateway
    #[endpoint {
        method = POST,
        path = "/v1/internet-gateways",
        tags = ["vpcs"],
    }]
    async fn internet_gateway_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::VpcSelector>,
        create_params: TypedBody<params::InternetGatewayCreate>,
    ) -> Result<HttpResponseCreated<views::InternetGateway>, HttpError>;

    /// Delete internet gateway
    #[endpoint {
        method = DELETE,
        path = "/v1/internet-gateways/{gateway}",
        tags = ["vpcs"],
    }]
    async fn internet_gateway_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::InternetGatewayPath>,
        query_params: Query<params::InternetGatewayDeleteSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// List IP pools attached to internet gateway
    #[endpoint {
        method = GET,
        path = "/v1/internet-gateway-ip-pools",
        tags = ["vpcs"],
    }]
    async fn internet_gateway_ip_pool_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<params::InternetGatewaySelector>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<views::InternetGatewayIpPool>>,
        HttpError,
    >;

    /// Attach IP pool to internet gateway
    #[endpoint {
        method = POST,
        path = "/v1/internet-gateway-ip-pools",
        tags = ["vpcs"],
    }]
    async fn internet_gateway_ip_pool_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::InternetGatewaySelector>,
        create_params: TypedBody<params::InternetGatewayIpPoolCreate>,
    ) -> Result<HttpResponseCreated<views::InternetGatewayIpPool>, HttpError>;

    /// Detach IP pool from internet gateway
    #[endpoint {
        method = DELETE,
        path = "/v1/internet-gateway-ip-pools/{pool}",
        tags = ["vpcs"],
    }]
    async fn internet_gateway_ip_pool_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::IpPoolPath>,
        query_params: Query<params::DeleteInternetGatewayElementSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// List IP addresses attached to internet gateway
    #[endpoint {
        method = GET,
        path = "/v1/internet-gateway-ip-addresses",
        tags = ["vpcs"],
    }]
    async fn internet_gateway_ip_address_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginatedByNameOrId<params::InternetGatewaySelector>,
        >,
    ) -> Result<
        HttpResponseOk<ResultsPage<views::InternetGatewayIpAddress>>,
        HttpError,
    >;

    /// Attach IP address to internet gateway
    #[endpoint {
        method = POST,
        path = "/v1/internet-gateway-ip-addresses",
        tags = ["vpcs"],
    }]
    async fn internet_gateway_ip_address_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::InternetGatewaySelector>,
        create_params: TypedBody<params::InternetGatewayIpAddressCreate>,
    ) -> Result<HttpResponseCreated<views::InternetGatewayIpAddress>, HttpError>;

    /// Detach IP address from internet gateway
    #[endpoint {
        method = DELETE,
        path = "/v1/internet-gateway-ip-addresses/{address}",
        tags = ["vpcs"],
    }]
    async fn internet_gateway_ip_address_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::IpAddressPath>,
        query_params: Query<params::DeleteInternetGatewayElementSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    // Racks

    /// List racks
    #[endpoint {
        method = GET,
        path = "/v1/system/hardware/racks",
        tags = ["system/hardware"],
    }]
    async fn rack_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<views::Rack>>, HttpError>;

    /// Fetch rack
    #[endpoint {
        method = GET,
        path = "/v1/system/hardware/racks/{rack_id}",
        tags = ["system/hardware"],
    }]
    async fn rack_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RackPath>,
    ) -> Result<HttpResponseOk<views::Rack>, HttpError>;

    /// List uninitialized sleds
    #[endpoint {
        method = GET,
        path = "/v1/system/hardware/sleds-uninitialized",
        tags = ["system/hardware"]
    }]
    async fn sled_list_uninitialized(
        rqctx: RequestContext<Self::Context>,
        query: Query<PaginationParams<EmptyScanParams, String>>,
    ) -> Result<HttpResponseOk<ResultsPage<shared::UninitializedSled>>, HttpError>;

    /// Add sled to initialized rack
    //
    // TODO: In the future this should really be a PUT request, once we resolve
    // https://github.com/oxidecomputer/omicron/issues/4494. It should also
    // explicitly be tied to a rack via a `rack_id` path param. For now we assume
    // we are only operating on single rack systems.
    #[endpoint {
        method = POST,
        path = "/v1/system/hardware/sleds",
        tags = ["system/hardware"]
    }]
    async fn sled_add(
        rqctx: RequestContext<Self::Context>,
        sled: TypedBody<params::UninitializedSledId>,
    ) -> Result<HttpResponseCreated<views::SledId>, HttpError>;

    // Sleds

    /// List sleds
    #[endpoint {
        method = GET,
        path = "/v1/system/hardware/sleds",
        tags = ["system/hardware"],
    }]
    async fn sled_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<views::Sled>>, HttpError>;

    /// Fetch sled
    #[endpoint {
        method = GET,
        path = "/v1/system/hardware/sleds/{sled_id}",
        tags = ["system/hardware"],
    }]
    async fn sled_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SledPath>,
    ) -> Result<HttpResponseOk<views::Sled>, HttpError>;

    /// Set sled provision policy
    #[endpoint {
        method = PUT,
        path = "/v1/system/hardware/sleds/{sled_id}/provision-policy",
        tags = ["system/hardware"],
    }]
    async fn sled_set_provision_policy(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SledPath>,
        new_provision_state: TypedBody<params::SledProvisionPolicyParams>,
    ) -> Result<HttpResponseOk<params::SledProvisionPolicyResponse>, HttpError>;

    /// List instances running on given sled
    #[endpoint {
        method = GET,
        path = "/v1/system/hardware/sleds/{sled_id}/instances",
        tags = ["system/hardware"],
    }]
    async fn sled_instance_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SledPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<views::SledInstance>>, HttpError>;

    // Physical disks

    /// List physical disks
    #[endpoint {
        method = GET,
        path = "/v1/system/hardware/disks",
        tags = ["system/hardware"],
    }]
    async fn physical_disk_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<views::PhysicalDisk>>, HttpError>;

    /// Get a physical disk
    #[endpoint {
        method = GET,
        path = "/v1/system/hardware/disks/{disk_id}",
        tags = ["system/hardware"],
    }]
    async fn physical_disk_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::PhysicalDiskPath>,
    ) -> Result<HttpResponseOk<views::PhysicalDisk>, HttpError>;

    // Switches

    /// List switches
    #[endpoint {
        method = GET,
        path = "/v1/system/hardware/switches",
        tags = ["system/hardware"],
    }]
    async fn switch_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<views::Switch>>, HttpError>;

    /// Fetch switch
    #[endpoint {
        method = GET,
        path = "/v1/system/hardware/switches/{switch_id}",
        tags = ["system/hardware"],
    }]
    async fn switch_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SwitchPath>,
    ) -> Result<HttpResponseOk<views::Switch>, HttpError>;

    /// List physical disks attached to sleds
    #[endpoint {
        method = GET,
        path = "/v1/system/hardware/sleds/{sled_id}/disks",
        tags = ["system/hardware"],
    }]
    async fn sled_physical_disk_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SledPath>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<views::PhysicalDisk>>, HttpError>;

    // Metrics

    /// View metrics
    ///
    /// View CPU, memory, or storage utilization metrics at the fleet or silo level.
    #[endpoint {
        method = GET,
        path = "/v1/system/metrics/{metric_name}",
        tags = ["system/metrics"],
    }]
    async fn system_metric(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SystemMetricsPathParam>,
        pag_params: Query<
            PaginationParams<params::ResourceMetrics, params::ResourceMetrics>,
        >,
        other_params: Query<params::OptionalSiloSelector>,
    ) -> Result<
        HttpResponseOk<ResultsPage<oximeter_types::Measurement>>,
        HttpError,
    >;

    /// View metrics
    ///
    /// View CPU, memory, or storage utilization metrics at the silo or project level.
    #[endpoint {
        method = GET,
        path = "/v1/metrics/{metric_name}",
        tags = ["metrics"],
    }]
    async fn silo_metric(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SystemMetricsPathParam>,
        pag_params: Query<
            PaginationParams<params::ResourceMetrics, params::ResourceMetrics>,
        >,
        other_params: Query<params::OptionalProjectSelector>,
    ) -> Result<
        HttpResponseOk<ResultsPage<oximeter_types::Measurement>>,
        HttpError,
    >;

    /// List timeseries schemas
    #[endpoint {
        method = GET,
        path = "/v1/system/timeseries/schemas",
        tags = ["system/metrics"],
    }]
    async fn system_timeseries_schema_list(
        rqctx: RequestContext<Self::Context>,
        pag_params: Query<TimeseriesSchemaPaginationParams>,
    ) -> Result<
        HttpResponseOk<ResultsPage<oximeter_types::TimeseriesSchema>>,
        HttpError,
    >;

    // TODO: can we link to an OxQL reference? Do we have one? Can we even do links?

    /// Run timeseries query
    ///
    /// Queries are written in OxQL.
    #[endpoint {
        method = POST,
        path = "/v1/system/timeseries/query",
        tags = ["system/metrics"],
    }]
    async fn system_timeseries_query(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<params::TimeseriesQuery>,
    ) -> Result<HttpResponseOk<views::OxqlQueryResult>, HttpError>;

    // TODO: list endpoint for project-scoped schemas is blocked on
    // https://github.com/oxidecomputer/omicron/issues/5942: the authz scope for
    // each schema is not stored in Clickhouse yet.

    /// Run project-scoped timeseries query
    ///
    /// Queries are written in OxQL. Project must be specified by name or ID in
    /// URL query parameter. The OxQL query will only return timeseries data
    /// from the specified project.
    #[endpoint {
        method = POST,
        path = "/v1/timeseries/query",
        tags = ["hidden"],
    }]
    async fn timeseries_query(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::ProjectSelector>,
        body: TypedBody<params::TimeseriesQuery>,
    ) -> Result<HttpResponseOk<views::OxqlQueryResult>, HttpError>;

    // Updates

    /// Upload TUF repository
    #[endpoint {
        method = PUT,
        path = "/v1/system/update/repository",
        tags = ["system/update"],
        unpublished = true,
        request_body_max_bytes = PUT_UPDATE_REPOSITORY_MAX_BYTES,
    }]
    async fn system_update_put_repository(
        rqctx: RequestContext<Self::Context>,
        query: Query<params::UpdatesPutRepositoryParams>,
        body: StreamingBody,
    ) -> Result<HttpResponseOk<TufRepoInsertResponse>, HttpError>;

    /// Fetch TUF repository description
    ///
    /// Fetch description of TUF repository by system version.
    #[endpoint {
        method = GET,
        path = "/v1/system/update/repository/{system_version}",
        tags = ["system/update"],
        unpublished = true,
    }]
    async fn system_update_get_repository(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::UpdatesGetRepositoryParams>,
    ) -> Result<HttpResponseOk<TufRepoGetResponse>, HttpError>;

    /// Get the current target release of the rack's system software
    ///
    /// This may not correspond to the actual software running on the rack
    /// at the time of request; it is instead the release that the rack
    /// reconfigurator should be moving towards as a goal state. After some
    /// number of planning and execution phases, the software running on the
    /// rack should eventually correspond to the release described here.
    #[endpoint {
        method = GET,
        path = "/v1/system/update/target-release",
        tags = ["hidden"], // "system/update"
    }]
    async fn target_release_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<views::TargetRelease>, HttpError>;

    /// Set the current target release of the rack's system software
    ///
    /// The rack reconfigurator will treat the software specified here as
    /// a goal state for the rack's software, and attempt to asynchronously
    /// update to that release.
    #[endpoint {
        method = PUT,
        path = "/v1/system/update/target-release",
        tags = ["hidden"], // "system/update"
    }]
    async fn target_release_update(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<params::SetTargetReleaseParams>,
    ) -> Result<HttpResponseCreated<views::TargetRelease>, HttpError>;

    // Silo users

    /// List users
    #[endpoint {
        method = GET,
        path = "/v1/users",
        tags = ["silos"],
    }]
    async fn user_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById<params::OptionalGroupSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<views::User>>, HttpError>;

    // Silo groups

    /// List groups
    #[endpoint {
        method = GET,
        path = "/v1/groups",
        tags = ["silos"],
    }]
    async fn group_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<views::Group>>, HttpError>;

    /// Fetch group
    #[endpoint {
        method = GET,
        path = "/v1/groups/{group_id}",
        tags = ["silos"],
    }]
    async fn group_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::GroupPath>,
    ) -> Result<HttpResponseOk<views::Group>, HttpError>;

    // Built-in (system) users

    /// List built-in users
    #[endpoint {
        method = GET,
        path = "/v1/system/users-builtin",
        tags = ["system/silos"],
    }]
    async fn user_builtin_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByName>,
    ) -> Result<HttpResponseOk<ResultsPage<views::UserBuiltin>>, HttpError>;

    /// Fetch built-in user
    #[endpoint {
        method = GET,
        path = "/v1/system/users-builtin/{user}",
        tags = ["system/silos"],
    }]
    async fn user_builtin_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::UserBuiltinSelector>,
    ) -> Result<HttpResponseOk<views::UserBuiltin>, HttpError>;

    // Built-in roles

    /// List built-in roles
    #[endpoint {
        method = GET,
        path = "/v1/system/roles",
        tags = ["roles"],
    }]
    async fn role_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<
            PaginationParams<EmptyScanParams, params::RolePage>,
        >,
    ) -> Result<HttpResponseOk<ResultsPage<views::Role>>, HttpError>;

    /// Fetch built-in role
    #[endpoint {
        method = GET,
        path = "/v1/system/roles/{role_name}",
        tags = ["roles"],
    }]
    async fn role_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RolePath>,
    ) -> Result<HttpResponseOk<views::Role>, HttpError>;

    // Current user

    /// Fetch user for current session
    #[endpoint {
        method = GET,
        path = "/v1/me",
        tags = ["session"],
    }]
    async fn current_user_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<views::CurrentUser>, HttpError>;

    /// Fetch current user's groups
    #[endpoint {
        method = GET,
        path = "/v1/me/groups",
        tags = ["session"],
    }]
    async fn current_user_groups(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<views::Group>>, HttpError>;

    // Per-user SSH public keys

    /// List SSH public keys
    ///
    /// Lists SSH public keys for the currently authenticated user.
    #[endpoint {
        method = GET,
        path = "/v1/me/ssh-keys",
        tags = ["session"],
    }]
    async fn current_user_ssh_key_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<views::SshKey>>, HttpError>;

    /// Create SSH public key
    ///
    /// Create an SSH public key for the currently authenticated user.
    #[endpoint {
        method = POST,
        path = "/v1/me/ssh-keys",
        tags = ["session"],
    }]
    async fn current_user_ssh_key_create(
        rqctx: RequestContext<Self::Context>,
        new_key: TypedBody<params::SshKeyCreate>,
    ) -> Result<HttpResponseCreated<views::SshKey>, HttpError>;

    /// Fetch SSH public key
    ///
    /// Fetch SSH public key associated with the currently authenticated user.
    #[endpoint {
        method = GET,
        path = "/v1/me/ssh-keys/{ssh_key}",
        tags = ["session"],
    }]
    async fn current_user_ssh_key_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SshKeyPath>,
    ) -> Result<HttpResponseOk<views::SshKey>, HttpError>;

    /// Delete SSH public key
    ///
    /// Delete an SSH public key associated with the currently authenticated user.
    #[endpoint {
        method = DELETE,
        path = "/v1/me/ssh-keys/{ssh_key}",
        tags = ["session"],
    }]
    async fn current_user_ssh_key_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SshKeyPath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// List access tokens
    ///
    /// List device access tokens for the currently authenticated user.
    #[endpoint {
        method = GET,
        path = "/v1/me/access-tokens",
        tags = ["tokens"],
    }]
    async fn current_user_access_token_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<views::DeviceAccessToken>>, HttpError>;

    /// Delete access token
    ///
    /// Delete a device access token for the currently authenticated user.
    #[endpoint {
        method = DELETE,
        path = "/v1/me/access-tokens/{token_id}",
        tags = ["tokens"],
    }]
    async fn current_user_access_token_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::TokenPath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    // Support bundles (experimental)

    /// List all support bundles
    #[endpoint {
        method = GET,
        path = "/experimental/v1/system/support-bundles",
        tags = ["hidden"], // system/support-bundles: only one tag is allowed
    }]
    async fn support_bundle_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<shared::SupportBundleInfo>>, HttpError>;

    /// View a support bundle
    #[endpoint {
        method = GET,
        path = "/experimental/v1/system/support-bundles/{bundle_id}",
        tags = ["hidden"], // system/support-bundles: only one tag is allowed
    }]
    async fn support_bundle_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SupportBundlePath>,
    ) -> Result<HttpResponseOk<shared::SupportBundleInfo>, HttpError>;

    /// Download the index of a support bundle
    #[endpoint {
        method = GET,
        path = "/experimental/v1/system/support-bundles/{bundle_id}/index",
        tags = ["hidden"], // system/support-bundles: only one tag is allowed
    }]
    async fn support_bundle_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<headers::RangeRequest>,
        path_params: Path<params::SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError>;

    /// Download the contents of a support bundle
    #[endpoint {
        method = GET,
        path = "/experimental/v1/system/support-bundles/{bundle_id}/download",
        tags = ["hidden"], // system/support-bundles: only one tag is allowed
    }]
    async fn support_bundle_download(
        rqctx: RequestContext<Self::Context>,
        headers: Header<headers::RangeRequest>,
        path_params: Path<params::SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError>;

    /// Download a file within a support bundle
    #[endpoint {
        method = GET,
        path = "/experimental/v1/system/support-bundles/{bundle_id}/download/{file}",
        tags = ["hidden"], // system/support-bundles: only one tag is allowed
    }]
    async fn support_bundle_download_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<headers::RangeRequest>,
        path_params: Path<params::SupportBundleFilePath>,
    ) -> Result<Response<Body>, HttpError>;

    /// Download the metadata of a support bundle
    #[endpoint {
        method = HEAD,
        path = "/experimental/v1/system/support-bundles/{bundle_id}/download",
        tags = ["hidden"], // system/support-bundles: only one tag is allowed
    }]
    async fn support_bundle_head(
        rqctx: RequestContext<Self::Context>,
        headers: Header<headers::RangeRequest>,
        path_params: Path<params::SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError>;

    /// Download the metadata of a file within the support bundle
    #[endpoint {
        method = HEAD,
        path = "/experimental/v1/system/support-bundles/{bundle_id}/download/{file}",
        tags = ["hidden"], // system/support-bundles: only one tag is allowed
    }]
    async fn support_bundle_head_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<headers::RangeRequest>,
        path_params: Path<params::SupportBundleFilePath>,
    ) -> Result<Response<Body>, HttpError>;

    /// Create a new support bundle
    #[endpoint {
        method = POST,
        path = "/experimental/v1/system/support-bundles",
        tags = ["hidden"], // system/support-bundles: only one tag is allowed
    }]
    async fn support_bundle_create(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseCreated<shared::SupportBundleInfo>, HttpError>;

    /// Delete an existing support bundle
    ///
    /// May also be used to cancel a support bundle which is currently being
    /// collected, or to remove metadata for a support bundle that has failed.
    #[endpoint {
        method = DELETE,
        path = "/experimental/v1/system/support-bundles/{bundle_id}",
        tags = ["hidden"], // system/support-bundles: only one tag is allowed
    }]
    async fn support_bundle_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::SupportBundlePath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    // Probes (experimental)

    /// List instrumentation probes
    #[endpoint {
        method = GET,
        path = "/experimental/v1/probes",
        tags = ["hidden"], // system/probes: only one tag is allowed
    }]
    async fn probe_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
    ) -> Result<HttpResponseOk<ResultsPage<shared::ProbeInfo>>, HttpError>;

    /// View instrumentation probe
    #[endpoint {
        method = GET,
        path = "/experimental/v1/probes/{probe}",
        tags = ["hidden"], // system/probes: only one tag is allowed
    }]
    async fn probe_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::ProbePath>,
        query_params: Query<params::ProjectSelector>,
    ) -> Result<HttpResponseOk<shared::ProbeInfo>, HttpError>;

    /// Create instrumentation probe
    #[endpoint {
        method = POST,
        path = "/experimental/v1/probes",
        tags = ["hidden"], // system/probes: only one tag is allowed
    }]
    async fn probe_create(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::ProjectSelector>,
        new_probe: TypedBody<params::ProbeCreate>,
    ) -> Result<HttpResponseCreated<Probe>, HttpError>;

    /// Delete instrumentation probe
    #[endpoint {
        method = DELETE,
        path = "/experimental/v1/probes/{probe}",
        tags = ["hidden"], // system/probes: only one tag is allowed
    }]
    async fn probe_delete(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::ProjectSelector>,
        path_params: Path<params::ProbePath>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    // Console API: logins

    /// SAML login console page (just a link to the IdP)
    #[endpoint {
        method = GET,
        path = "/login/{silo_name}/saml/{provider_name}",
        tags = ["login"],
        unpublished = true,
    }]
    async fn login_saml_begin(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::LoginToProviderPathParam>,
        query_params: Query<params::LoginUrlQuery>,
    ) -> Result<Response<Body>, HttpError>;

    /// Get a redirect straight to the IdP
    ///
    /// Console uses this to avoid having to ask the API anything about the IdP. It
    /// already knows the IdP name from the path, so it can just link to this path
    /// and rely on Nexus to redirect to the actual IdP.
    #[endpoint {
        method = GET,
        path = "/login/{silo_name}/saml/{provider_name}/redirect",
        tags = ["login"],
        unpublished = true,
    }]
    async fn login_saml_redirect(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::LoginToProviderPathParam>,
        query_params: Query<params::LoginUrlQuery>,
    ) -> Result<HttpResponseFound, HttpError>;

    /// Authenticate a user via SAML
    #[endpoint {
        method = POST,
        path = "/login/{silo_name}/saml/{provider_name}",
        tags = ["login"],
    }]
    async fn login_saml(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::LoginToProviderPathParam>,
        body_bytes: dropshot::UntypedBody,
    ) -> Result<HttpResponseSeeOther, HttpError>;

    #[endpoint {
        method = GET,
        path = "/login/{silo_name}/local",
        tags = ["login"],
        unpublished = true,
    }]
    async fn login_local_begin(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::LoginPath>,
        query_params: Query<params::LoginUrlQuery>,
    ) -> Result<Response<Body>, HttpError>;

    /// Authenticate a user via username and password
    #[endpoint {
        method = POST,
        path = "/v1/login/{silo_name}/local",
        tags = ["login"],
    }]
    async fn login_local(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::LoginPath>,
        credentials: TypedBody<params::UsernamePasswordCredentials>,
    ) -> Result<HttpResponseHeaders<HttpResponseUpdatedNoContent>, HttpError>;

    /// Log user out of web console by deleting session on client and server
    #[endpoint {
        // important for security that this be a POST despite the empty req body
        method = POST,
        path = "/v1/logout",
        tags = ["hidden"],
    }]
    async fn logout(
        rqctx: RequestContext<Self::Context>,
        cookies: Cookies,
    ) -> Result<HttpResponseHeaders<HttpResponseUpdatedNoContent>, HttpError>;

    /// Redirect to a login page for the current Silo (if that can be determined)
    #[endpoint {
        method = GET,
        path = "/login",
        unpublished = true,
    }]
    async fn login_begin(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::LoginUrlQuery>,
    ) -> Result<HttpResponseFound, HttpError>;

    // Console API: Pages
    //
    // Dropshot does not have route match ranking and does not allow overlapping
    // route definitions, so we cannot use a catchall `/*` route for console pages
    // because it would overlap with the API routes definitions. So instead we have
    // to manually define more specific routes.

    #[endpoint {
        method = GET,
        path = "/projects/{path:.*}",
        unpublished = true,
    }]
    async fn console_projects(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RestPathParam>,
    ) -> Result<Response<Body>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/settings/{path:.*}",
        unpublished = true,
    }]
    async fn console_settings_page(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RestPathParam>,
    ) -> Result<Response<Body>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/system/{path:.*}",
        unpublished = true,
    }]
    async fn console_system_page(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RestPathParam>,
    ) -> Result<Response<Body>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/lookup/{path:.*}",
        unpublished = true,
    }]
    async fn console_lookup(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RestPathParam>,
    ) -> Result<Response<Body>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/",
        unpublished = true,
    }]
    async fn console_root(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/projects-new",
        unpublished = true,
    }]
    async fn console_projects_new(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/images",
        unpublished = true,
    }]
    async fn console_silo_images(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/utilization",
        unpublished = true,
    }]
    async fn console_silo_utilization(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/access",
        unpublished = true,
    }]
    async fn console_silo_access(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError>;

    /// Serve a static asset
    #[endpoint {
        method = GET,
        path = "/assets/{path:.*}",
        unpublished = true,
    }]
    async fn asset(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::RestPathParam>,
    ) -> Result<Response<Body>, HttpError>;

    /// Start an OAuth 2.0 Device Authorization Grant
    ///
    /// This endpoint is designed to be accessed from an *unauthenticated*
    /// API client. It generates and records a `device_code` and `user_code`
    /// which must be verified and confirmed prior to a token being granted.
    #[endpoint {
        method = POST,
        path = "/device/auth",
        content_type = "application/x-www-form-urlencoded",
        tags = ["hidden"], // "token"
    }]
    async fn device_auth_request(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<params::DeviceAuthRequest>,
    ) -> Result<Response<Body>, HttpError>;

    /// Verify an OAuth 2.0 Device Authorization Grant
    ///
    /// This endpoint should be accessed in a full user agent (e.g.,
    /// a browser). If the user is not logged in, we redirect them to
    /// the login page and use the `state` parameter to get them back
    /// here on completion. If they are logged in, serve up the console
    /// verification page so they can verify the user code.
    #[endpoint {
        method = GET,
        path = "/device/verify",
        unpublished = true,
    }]
    async fn device_auth_verify(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError>;

    #[endpoint {
        method = GET,
        path = "/device/success",
        unpublished = true,
    }]
    async fn device_auth_success(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError>;

    /// Confirm an OAuth 2.0 Device Authorization Grant
    ///
    /// This endpoint is designed to be accessed by the user agent (browser),
    /// not the client requesting the token. So we do not actually return the
    /// token here; it will be returned in response to the poll on `/device/token`.
    #[endpoint {
        method = POST,
        path = "/device/confirm",
        tags = ["hidden"], // "token"
    }]
    async fn device_auth_confirm(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<params::DeviceAuthVerify>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Request a device access token
    ///
    /// This endpoint should be polled by the client until the user code
    /// is verified and the grant is confirmed.
    #[endpoint {
        method = POST,
        path = "/device/token",
        content_type = "application/x-www-form-urlencoded",
        tags = ["hidden"], // "token"
    }]
    async fn device_access_token(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<params::DeviceAccessTokenRequest>,
    ) -> Result<Response<Body>, HttpError>;

    // Alerts

    /// List alert classes
    #[endpoint {
        method = GET,
        path = "/v1/alert-classes",
        tags = ["system/alerts"],
    }]
    async fn alert_class_list(
        rqctx: RequestContext<Self::Context>,
        pag_params: Query<
            PaginationParams<EmptyScanParams, params::AlertClassPage>,
        >,
        filter: Query<params::AlertClassFilter>,
    ) -> Result<HttpResponseOk<ResultsPage<views::AlertClass>>, HttpError>;

    /// List alert receivers
    #[endpoint {
        method = GET,
        path = "/v1/alert-receivers",
        tags = ["system/alerts"],
    }]
    async fn alert_receiver_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByNameOrId>,
    ) -> Result<HttpResponseOk<ResultsPage<views::AlertReceiver>>, HttpError>;

    /// Fetch alert receiver
    #[endpoint {
        method = GET,
        path = "/v1/alert-receivers/{receiver}",
        tags = ["system/alerts"],
    }]
    async fn alert_receiver_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::AlertReceiverSelector>,
    ) -> Result<HttpResponseOk<views::AlertReceiver>, HttpError>;

    /// Delete alert receiver
    #[endpoint {
        method = DELETE,
        path = "/v1/alert-receivers/{receiver}",
        tags = ["system/alerts"],
    }]
    async fn alert_receiver_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::AlertReceiverSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Add alert receiver subscription
    #[endpoint {
        method = POST,
        path = "/v1/alert-receivers/{receiver}/subscriptions",
        tags = ["system/alerts"],
    }]
    async fn alert_receiver_subscription_add(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::AlertReceiverSelector>,
        params: TypedBody<params::AlertSubscriptionCreate>,
    ) -> Result<HttpResponseCreated<views::AlertSubscriptionCreated>, HttpError>;

    /// Remove alert receiver subscription
    #[endpoint {
        method = DELETE,
        path = "/v1/alert-receivers/{receiver}/subscriptions/{subscription}",
        tags = ["system/alerts"],
    }]
    async fn alert_receiver_subscription_remove(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::AlertSubscriptionSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// List delivery attempts to alert receiver
    ///
    /// Optional query parameters to this endpoint may be used to filter
    /// deliveries by state. If none of the `failed`, `pending` or `delivered`
    /// query parameters are present, all deliveries are returned. If one or
    /// more of these parameters are provided, only those which are set to
    /// "true" are included in the response.
    #[endpoint {
        method = GET,
        path = "/v1/alert-receivers/{receiver}/deliveries",
        tags = ["system/alerts"],
    }]
    async fn alert_delivery_list(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::AlertReceiverSelector>,
        state_filter: Query<params::AlertDeliveryStateFilter>,
        pagination: Query<PaginatedByTimeAndId>,
    ) -> Result<HttpResponseOk<ResultsPage<views::AlertDelivery>>, HttpError>;

    /// Send liveness probe to alert receiver
    ///
    /// This endpoint synchronously sends a liveness probe to the selected alert
    /// receiver. The response message describes the outcome of the probe:
    /// either the successful response (as appropriate), or indication of why
    /// the probe failed.
    ///
    /// The result of the probe is represented as an `AlertDelivery` model.
    /// Details relating to the status of the probe depend on the alert delivery
    /// mechanism, and are included in the `AlertDeliveryAttempts` model. For
    /// example, webhook receiver liveness probes include the HTTP status code
    /// returned by the receiver endpoint.
    ///
    /// Note that the response status is `200 OK` as long as a probe request was
    /// able to be sent to the receiver endpoint. If an HTTP-based receiver,
    /// such as a webhook, responds to the another status code, including an
    /// error, this will be indicated by the response body, *not* the status of
    /// the response.
    ///
    /// The `resend` query parameter can be used to request re-delivery of
    /// failed events if the liveness probe succeeds. If it is set to true and
    /// the liveness probe succeeds, any alerts for which delivery to this
    /// receiver has failed will be queued for re-delivery.
    #[endpoint {
        method = POST,
        path = "/v1/alert-receivers/{receiver}/probe",
        tags = ["system/alerts"],
    }]
    async fn alert_receiver_probe(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::AlertReceiverSelector>,
        query_params: Query<params::AlertReceiverProbe>,
    ) -> Result<HttpResponseOk<views::AlertProbeResult>, HttpError>;

    /// Request re-delivery of alert
    #[endpoint {
        method = POST,
        path = "/v1/alerts/{alert_id}/resend",
        tags = ["system/alerts"],
    }]
    async fn alert_delivery_resend(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::AlertSelector>,
        receiver: Query<params::AlertReceiverSelector>,
    ) -> Result<HttpResponseCreated<views::AlertDeliveryId>, HttpError>;

    // ALERTS: WEBHOOKS

    /// Create webhook receiver
    #[endpoint {
        method = POST,
        path = "/v1/webhook-receivers",
        tags = ["system/alerts"],
    }]
    async fn webhook_receiver_create(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<params::WebhookCreate>,
    ) -> Result<HttpResponseCreated<views::WebhookReceiver>, HttpError>;

    /// Update webhook receiver
    ///
    /// Note that receiver secrets are NOT added or removed using this endpoint.
    /// Instead, use the `/v1/webhooks/{secrets}/?receiver={receiver}` endpoint
    /// to add and remove secrets.
    #[endpoint {
        method = PUT,
        path = "/v1/webhook-receivers/{receiver}",
        tags = ["system/alerts"],
    }]
    async fn webhook_receiver_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::AlertReceiverSelector>,
        params: TypedBody<params::WebhookReceiverUpdate>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// List webhook receiver secret IDs
    #[endpoint {
        method = GET,
        path = "/v1/webhook-secrets",
        tags = ["system/alerts"],
    }]
    async fn webhook_secrets_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::AlertReceiverSelector>,
    ) -> Result<HttpResponseOk<views::WebhookSecrets>, HttpError>;

    /// Add secret to webhook receiver
    #[endpoint {
        method = POST,
        path = "/v1/webhook-secrets",
        tags = ["system/alerts"],
    }]
    async fn webhook_secrets_add(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<params::AlertReceiverSelector>,
        params: TypedBody<params::WebhookSecretCreate>,
    ) -> Result<HttpResponseCreated<views::WebhookSecret>, HttpError>;

    /// Remove secret from webhook receiver
    #[endpoint {
        method = DELETE,
        path = "/v1/webhook-secrets/{secret_id}",
        tags = ["system/alerts"],
    }]
    async fn webhook_secrets_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<params::WebhookSecretSelector>,
    ) -> Result<HttpResponseDeleted, HttpError>;
}

/// Perform extra validations on the OpenAPI spec.
pub fn validate_api(spec: &OpenAPI, mut cx: ValidationContext<'_>) {
    if spec.openapi != "3.0.3" {
        cx.report_error(anyhow!(
            "Expected OpenAPI version to be 3.0.3, found {}",
            spec.openapi,
        ));
    }
    if spec.info.title != "Oxide Region API" {
        cx.report_error(anyhow!(
            "Expected OpenAPI version to be 'Oxide Region API', found '{}'",
            spec.info.title,
        ));
    }
    if spec.info.version != API_VERSION {
        cx.report_error(anyhow!(
            "Expected OpenAPI version to be '{}', found '{}'",
            API_VERSION,
            spec.info.version,
        ));
    }

    // Spot check a couple of items.
    if spec.paths.paths.is_empty() {
        cx.report_error(anyhow!("Expected at least one path in the spec"));
    }
    if spec.paths.paths.get("/v1/projects").is_none() {
        cx.report_error(anyhow!("Expected a path for /v1/projects"));
    }

    // Construct a string that helps us identify the organization of tags and
    // operations.
    let mut ops_by_tag =
        BTreeMap::<String, Vec<(String, String, String)>>::new();

    let mut ops_by_tag_valid = true;
    for (path, method, op) in spec.operations() {
        // Make sure each operation has exactly one tag. Note, we intentionally
        // do this before validating the OpenAPI output as fixing an error here
        // would necessitate refreshing the spec file again.
        if op.tags.len() != 1 {
            cx.report_error(anyhow!(
                "operation '{}' has {} tags rather than 1",
                op.operation_id.as_ref().unwrap(),
                op.tags.len()
            ));
            ops_by_tag_valid = false;
            continue;
        }

        // Every non-hidden endpoint must have a summary
        if op.tags.contains(&"hidden".to_string()) && op.summary.is_none() {
            cx.report_error(anyhow!(
                "operation '{}' is missing a summary doc comment",
                op.operation_id.as_ref().unwrap()
            ));
            // This error does not prevent `ops_by_tag` from being populated
            // correctly, so we can continue.
        }

        ops_by_tag
            .entry(op.tags.first().unwrap().to_string())
            .or_default()
            .push((
                op.operation_id.as_ref().unwrap().to_string(),
                method.to_string().to_uppercase(),
                path.to_string(),
            ));
    }

    if ops_by_tag_valid {
        let mut tags = String::new();
        for (tag, mut ops) in ops_by_tag {
            ops.sort();
            tags.push_str(&format!(
                r#"API operations found with tag "{}""#,
                tag
            ));
            tags.push_str(&format!(
                "\n{:40} {:8} {}\n",
                "OPERATION ID", "METHOD", "URL PATH"
            ));
            for (operation_id, method, path) in ops {
                tags.push_str(&format!(
                    "{:40} {:8} {}\n",
                    operation_id, method, path
                ));
            }
            tags.push('\n');
        }

        // When this fails, verify that operations on which you're adding,
        // renaming, or changing the tags are what you intend.
        cx.record_file_contents(
            "nexus/external-api/output/nexus_tags.txt",
            tags.into_bytes(),
        );
    }
}

pub type IpPoolRangePaginationParams =
    PaginationParams<EmptyScanParams, IpNetwork>;

/// Type used to paginate request to list timeseries schema
pub type TimeseriesSchemaPaginationParams =
    PaginationParams<EmptyScanParams, oximeter_types::TimeseriesName>;
