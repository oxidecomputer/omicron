use anyhow::Error;
use dropshot::test_util::ClientTestContext;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::TestResponse;
use nexus_test_utils::resource_helpers::create_local_user;
use nexus_test_utils::resource_helpers::grant_iam;
use nexus_test_utils::resource_helpers::object_create;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::shared;
use nexus_types::external_api::shared::SiloRole;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::InstanceCpuCount;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

struct ResourceAllocator {
    auth: AuthnMode,
}

impl ResourceAllocator {
    fn new(auth: AuthnMode) -> Self {
        Self { auth }
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
                hostname: "host".to_string(),
                user_data: b"#cloud-config\nsystem_info:\n  default_user:\n    name: oxide"
                    .to_vec(),
                network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
                external_ips: Vec::<params::ExternalIpCreate>::new(),
                disks: Vec::<params::InstanceDiskAttachment>::new(),
                start: true,
            },
        )
        .authn_as(self.auth.clone())
        .execute()
        .await
    }

    async fn provision_disk(
        &self,
        client: &ClientTestContext,
        name: &str,
        size: u32,
    ) -> Result<TestResponse, Error> {
        NexusRequest::objects_post(
            client,
            "/v1/disks?project=project",
            &params::DiskCreate {
                identity: IdentityMetadataCreateParams {
                    name: name.parse().unwrap(),
                    description: "".into(),
                },
                size: ByteCount::from_gibibytes_u32(size),
                disk_source: params::DiskSource::Blank {
                    block_size: params::BlockSize::try_from(512).unwrap(),
                },
            },
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
    let silo = object_create(
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

    // Create a silo user
    let user = create_local_user(
        client,
        &silo,
        &"user".parse().unwrap(),
        params::UserPassword::LoginDisallowed,
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
    .await?;

    ResourceAllocator::new(auth_mode)
}

#[nexus_test]
async fn test_quotas(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    // let nexus = &cptestctx.server.apictx().nexus;

    let system = setup_silo_with_quota(
        &client,
        "rationed_silo",
        params::SiloQuotasCreate::empty(),
    )
    .await;
    system.provision_instance(client, "instance", 1, 1).await;
}
