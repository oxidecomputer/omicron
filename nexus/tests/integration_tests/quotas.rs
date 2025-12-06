use anyhow::Error;
use dropshot::HttpErrorResponseBody;
use dropshot::test_util::ClientTestContext;
use http::Method;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::http_testing::TestResponse;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils::resource_helpers::create_ip_pool;
use nexus_test_utils::resource_helpers::create_local_user;
use nexus_test_utils::resource_helpers::grant_iam;
use nexus_test_utils::resource_helpers::link_ip_pool;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils::resource_helpers::object_create_error;
use nexus_test_utils::resource_helpers::test_params;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::shared;
use nexus_types::external_api::shared::SiloRole;
use nexus_types::external_api::views::{Silo, SiloQuotas};
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::InstanceCpuCount;
use serde_json::json;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

struct ResourceAllocator {
    auth: AuthnMode,
}

impl ResourceAllocator {
    fn new(auth: AuthnMode) -> Self {
        Self { auth }
    }

    async fn set_quotas(
        &self,
        client: &ClientTestContext,
        quotas: params::SiloQuotasUpdate,
    ) -> Result<TestResponse, Error> {
        NexusRequest::object_put(
            client,
            "/v1/system/silos/quota-test-silo/quotas",
            Some(&quotas),
        )
        .authn_as(self.auth.clone())
        .execute()
        .await
    }

    async fn set_quotas_expect_error(
        &self,
        client: &ClientTestContext,
        quotas: params::SiloQuotasUpdate,
        code: http::StatusCode,
    ) -> HttpErrorResponseBody {
        NexusRequest::expect_failure_with_body(
            client,
            code,
            http::Method::PUT,
            "/v1/system/silos/quota-test-silo/quotas",
            &Some(&quotas),
        )
        .authn_as(self.auth.clone())
        .execute()
        .await
        .expect("Expected failure updating quotas")
        .parsed_body::<HttpErrorResponseBody>()
        .expect("Failed to read response after setting quotas")
    }

    async fn get_quotas(&self, client: &ClientTestContext) -> SiloQuotas {
        NexusRequest::object_get(
            client,
            "/v1/system/silos/quota-test-silo/quotas",
        )
        .authn_as(self.auth.clone())
        .execute()
        .await
        .expect("failed to fetch quotas")
        .parsed_body()
        .expect("failed to parse quotas")
    }

    async fn provision_instance(
        &self,
        client: &ClientTestContext,
        name: &str,
        cpus: u16,
        memory: u32,
    ) -> Result<TestResponse, Error> {
        NexusRequest::objects_post(
            client,
            "/v1/instances?project=project",
            &params::InstanceCreate {
                identity: IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: "".into(),
                },
                ncpus: InstanceCpuCount(cpus),
                memory: ByteCount::from_gibibytes_u32(memory),
                hostname: "host".parse().unwrap(),
                user_data: b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                    .to_vec(),
                ssh_public_keys:  Some(Vec::new()),
                network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
                external_ips: Vec::<params::ExternalIpCreate>::new(),
                disks: Vec::<params::InstanceDiskAttachment>::new(),
                boot_disk: None,
                cpu_platform: None,
                start: false,
                auto_restart_policy: Default::default(),
                anti_affinity_groups: Vec::new(),
                multicast_groups: Vec::new(),
            },
        )
        .authn_as(self.auth.clone())
        .execute()
        .await
        .expect("Instance should be created regardless of quotas");

        NexusRequest::new(
            RequestBuilder::new(
                client,
                Method::POST,
                format!("/v1/instances/{}/start?project=project", name)
                    .as_str(),
            )
            .body(None as Option<&serde_json::Value>),
        )
        .authn_as(self.auth.clone())
        .execute()
        .await
    }

    async fn cleanup_instance(
        &self,
        client: &ClientTestContext,
        name: &str,
    ) -> TestResponse {
        // Try to stop the instance
        NexusRequest::new(
            RequestBuilder::new(
                client,
                Method::POST,
                format!("/v1/instances/{}/stop?project=project", name).as_str(),
            )
            .body(None as Option<&serde_json::Value>),
        )
        .authn_as(self.auth.clone())
        .execute()
        .await
        .expect("failed to stop instance");

        NexusRequest::object_delete(
            client,
            format!("/v1/instances/{}?project=project", name).as_str(),
        )
        .authn_as(self.auth.clone())
        .execute()
        .await
        .expect("failed to delete instance")
    }

    async fn provision_disk(
        &self,
        client: &ClientTestContext,
        name: &str,
        size: u32,
    ) -> Result<TestResponse, Error> {
        NexusRequest::new(
            RequestBuilder::new(
                client,
                Method::POST,
                "/v1/disks?project=project",
            )
            .body(Some(&params::DiskCreate {
                identity: IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: "".into(),
                },
                size: ByteCount::from_gibibytes_u32(size),
                disk_backend: params::DiskBackend::Distributed {
                    disk_source: params::DiskSource::Blank {
                        block_size: params::BlockSize::try_from(512).unwrap(),
                    },
                },
            })),
        )
        .authn_as(self.auth.clone())
        .execute()
        .await
    }
}

async fn setup_silo_with_quota(
    client: &ClientTestContext,
    silo_name: &str,
    quotas: params::SiloQuotasCreate,
) -> ResourceAllocator {
    let silo: Silo = object_create(
        client,
        "/v1/system/silos",
        &params::SiloCreate {
            identity: IdentityMetadataCreateParams {
                name: silo_name.parse().unwrap(),
                description: "".into(),
            },
            quotas,
            discoverable: true,
            identity_mode: shared::SiloIdentityMode::LocalOnly,
            admin_group_name: None,
            tls_certificates: vec![],
            mapped_fleet_roles: Default::default(),
        },
    )
    .await;

    // create default pool and link to this silo. can't use
    // create_default_ip_pool because that links to the default silo
    create_ip_pool(&client, "default", None).await;
    link_ip_pool(&client, "default", &silo.identity.id, true).await;

    // Create a silo user
    let user = create_local_user(
        client,
        &silo,
        &"user".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;

    // Make silo admin
    grant_iam(
        client,
        format!("/v1/system/silos/{}", silo_name).as_str(),
        SiloRole::Admin,
        user.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    let auth_mode = AuthnMode::SiloUser(user.id);

    NexusRequest::objects_post(
        client,
        "/v1/projects",
        &params::ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: "project".parse().unwrap(),
                description: "".into(),
            },
        },
    )
    .authn_as(auth_mode.clone())
    .execute()
    .await
    .unwrap();

    ResourceAllocator::new(auth_mode)
}

#[nexus_test]
async fn test_quotas(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Simulate space for disks
    DiskTest::new(&cptestctx).await;

    let system = setup_silo_with_quota(
        &client,
        "quota-test-silo",
        params::SiloQuotasCreate::empty(),
    )
    .await;

    // Ensure trying to provision an instance with empty quotas fails
    let err = system
        .provision_instance(client, "instance", 1, 1)
        .await
        .unwrap()
        .parsed_body::<HttpErrorResponseBody>()
        .expect("failed to parse error body");
    assert!(
        err.message.contains("vCPU Limit Exceeded"),
        "Unexpected error: {0}",
        err.message
    );
    system.cleanup_instance(client, "instance").await;

    // Up the CPU, memory quotas
    system
        .set_quotas(
            client,
            params::SiloQuotasUpdate {
                cpus: Some(4),
                memory: Some(ByteCount::from_gibibytes_u32(15)),
                storage: Some(ByteCount::from_gibibytes_u32(2)),
            },
        )
        .await
        .expect("failed to set quotas");

    let quotas = system.get_quotas(client).await;
    assert_eq!(quotas.limits.cpus, 4);
    assert_eq!(quotas.limits.memory, ByteCount::from_gibibytes_u32(15));
    assert_eq!(quotas.limits.storage, ByteCount::from_gibibytes_u32(2));

    // Ensure memory quota is enforced
    let err = system
        .provision_instance(client, "instance", 1, 16)
        .await
        .unwrap()
        .parsed_body::<HttpErrorResponseBody>()
        .expect("failed to parse error body");
    assert!(
        err.message.contains("Memory Limit Exceeded"),
        "Unexpected error: {0}",
        err.message
    );
    system.cleanup_instance(client, "instance").await;

    // Allocating instance should now succeed
    system
        .provision_instance(client, "instance", 2, 10)
        .await
        .expect("Instance should've had enough resources to be provisioned");

    let err = system
        .provision_disk(client, "disk", 3)
        .await
        .unwrap()
        .parsed_body::<HttpErrorResponseBody>()
        .expect("failed to parse error body");
    assert!(
        err.message.contains("Storage Limit Exceeded"),
        "Unexpected error: {0}",
        err.message
    );

    system
        .provision_disk(client, "disk", 1)
        .await
        .expect("Disk should be provisioned");
}

#[nexus_test]
async fn test_quota_limits(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let system = setup_silo_with_quota(
        &client,
        "quota-test-silo",
        params::SiloQuotasCreate::empty(),
    )
    .await;

    // Maximal legal limits should be allowed.
    let quota_limit = params::SiloQuotasUpdate {
        cpus: Some(i64::MAX),
        memory: Some(i64::MAX.try_into().unwrap()),
        storage: Some(i64::MAX.try_into().unwrap()),
    };
    system
        .set_quotas(client, quota_limit.clone())
        .await
        .expect("set max quotas");
    let quotas = system.get_quotas(client).await;
    assert_eq!(quotas.limits.cpus, quota_limit.cpus.unwrap());
    assert_eq!(quotas.limits.memory, quota_limit.memory.unwrap());
    assert_eq!(quotas.limits.storage, quota_limit.storage.unwrap());

    // Construct a value that fits in a u64 but not an i64.
    let out_of_bounds = u64::try_from(i64::MAX).unwrap() + 1;

    for key in ["cpus", "memory", "storage"] {
        // We can't construct a `SiloQuotasUpdate` with higher-than-maximal
        // values, but we can construct the equivalent JSON blob of such a
        // request.
        let request = json!({ key: out_of_bounds });

        let err = NexusRequest::expect_failure_with_body(
            client,
            http::StatusCode::BAD_REQUEST,
            http::Method::PUT,
            "/v1/system/silos/quota-test-silo/quotas",
            &request,
        )
        .authn_as(system.auth.clone())
        .execute()
        .await
        .expect("sent quota update")
        .parsed_body::<HttpErrorResponseBody>()
        .expect("parsed error body");
        assert!(
            err.message.contains(key)
                && (err.message.contains("invalid value")
                    || err
                        .message
                        .contains("value is too large for a byte count")),
            "Unexpected error: {0}",
            err.message
        );

        // The quota limits we set above should be unchanged.
        let quotas = system.get_quotas(client).await;
        assert_eq!(quotas.limits.cpus, quota_limit.cpus.unwrap());
        assert_eq!(quotas.limits.memory, quota_limit.memory.unwrap());
        assert_eq!(quotas.limits.storage, quota_limit.storage.unwrap());
    }
}

#[nexus_test]
async fn test_negative_quota(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Can't make a silo with a negative quota
    let mut quotas = params::SiloQuotasCreate::empty();
    quotas.cpus = -1;
    let response = object_create_error(
        client,
        "/v1/system/silos",
        &params::SiloCreate {
            identity: IdentityMetadataCreateParams {
                name: "negative-cpus-not-allowed".parse().unwrap(),
                description: "".into(),
            },
            quotas,
            discoverable: true,
            identity_mode: shared::SiloIdentityMode::LocalOnly,
            admin_group_name: None,
            tls_certificates: vec![],
            mapped_fleet_roles: Default::default(),
        },
        http::StatusCode::BAD_REQUEST,
    )
    .await;

    assert!(
        response.message.contains(
            "Cannot create silo quota: CPU quota must not be negative"
        ),
        "Unexpected response: {}",
        response.message
    );

    // Make the silo with an empty quota
    let system = setup_silo_with_quota(
        &client,
        "quota-test-silo",
        params::SiloQuotasCreate::empty(),
    )
    .await;

    // Can't update a silo with a negative quota
    let quota_limit = params::SiloQuotasUpdate {
        cpus: Some(-1),
        memory: Some(0_u64.try_into().unwrap()),
        storage: Some(0_u64.try_into().unwrap()),
    };
    let response = system
        .set_quotas_expect_error(
            client,
            quota_limit.clone(),
            http::StatusCode::BAD_REQUEST,
        )
        .await;

    assert!(
        response.message.contains(
            "Cannot update silo quota: CPU quota must not be negative"
        ),
        "Unexpected response: {}",
        response.message
    );
}
