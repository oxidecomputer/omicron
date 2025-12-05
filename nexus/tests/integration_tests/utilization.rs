use dropshot::test_util::ClientTestContext;
use http::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_instance;
use nexus_test_utils::resource_helpers::create_local_user;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::grant_iam;
use nexus_test_utils::resource_helpers::link_ip_pool;
use nexus_test_utils::resource_helpers::object_get;
use nexus_test_utils::resource_helpers::object_put;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::resource_helpers::test_params;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::params::SiloQuotasCreate;
use nexus_types::external_api::views::Silo;
use nexus_types::external_api::views::SiloQuotas;
use nexus_types::external_api::views::SiloUtilization;
use nexus_types::external_api::views::Utilization;
use nexus_types::external_api::views::VirtualResourceCounts;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::InstanceCpuCount;
use oxide_client::types::SiloRole;

static PROJECT_NAME: &str = "utilization-test-project";
static INSTANCE_NAME: &str = "utilization-test-instance";

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_utilization_list(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;

    // default-silo has quotas, but is explicitly filtered out by ID in the
    // DB query to avoid user confusion. test-suite-silo also exists, but is
    // filtered out because it has no quotas, so list is empty
    assert!(util_list(client).await.is_empty());

    // setting quotas will make test-suite-silo show up in the list
    let quotas_url = "/v1/system/silos/test-suite-silo/quotas";
    let _: SiloQuotas = object_put(
        client,
        quotas_url,
        &params::SiloQuotasCreate::arbitrarily_high_default(),
    )
    .await;

    // now test-suite-silo shows up in the list
    let current_util = util_list(client).await;
    assert_eq!(current_util.len(), 1);
    assert_eq!(current_util[0].silo_name, "test-suite-silo");
    // it's empty because it has no resources
    assert_eq!(current_util[0].provisioned, SiloQuotasCreate::empty().into());
    assert_eq!(
        current_util[0].allocated,
        SiloQuotasCreate::arbitrarily_high_default().into()
    );

    // create the resources that should change the utilization
    create_resources_in_test_suite_silo(client).await;

    // list response shows provisioned resources
    let current_util = util_list(client).await;
    assert_eq!(current_util.len(), 1);
    assert_eq!(current_util[0].silo_name, "test-suite-silo");
    assert_eq!(
        current_util[0].provisioned,
        VirtualResourceCounts {
            cpus: 2,
            memory: ByteCount::from_gibibytes_u32(4),
            storage: ByteCount::from(0)
        }
    );
    assert_eq!(
        current_util[0].allocated,
        SiloQuotasCreate::arbitrarily_high_default().into()
    );

    // now we take the quota back off of test-suite-silo and end up empty again
    let _: SiloQuotas =
        object_put(client, quotas_url, &params::SiloQuotasCreate::empty())
            .await;

    assert!(util_list(client).await.is_empty());
}

// Even though default silo is filtered out of the list view, you can still
// fetch utilization for it individiually, so we test that here
#[nexus_test]
async fn test_utilization_view(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;

    let _ = create_project(&client, &PROJECT_NAME).await;
    let _ = create_instance(client, &PROJECT_NAME, &INSTANCE_NAME).await;

    let instance_start_url = format!(
        "/v1/instances/{}/start?project={}",
        &INSTANCE_NAME, &PROJECT_NAME
    );

    // Start instance
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &instance_start_url)
            .body(None as Option<&serde_json::Value>)
            .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to start instance");

    // get utilization for just the default silo
    let default_silo_util: SiloUtilization =
        object_get(client, "/v1/system/utilization/silos/default-silo").await;

    assert_eq!(
        default_silo_util.provisioned,
        VirtualResourceCounts {
            cpus: 4,
            memory: ByteCount::from_gibibytes_u32(1),
            storage: ByteCount::from(0)
        }
    );

    // Simulate space for disks
    DiskTest::new(&cptestctx).await;

    let disk_url = format!("/v1/disks?project={}", &PROJECT_NAME);
    // provision disk
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, &disk_url)
            .body(Some(&params::DiskCreate {
                identity: IdentityMetadataCreateParams {
                    name: "test-disk".parse().unwrap(),
                    description: "".into(),
                },
                size: ByteCount::from_gibibytes_u32(2),
                disk_backend: params::DiskBackend::Crucible {
                    disk_source: params::DiskSource::Blank {
                        block_size: params::BlockSize::try_from(512).unwrap(),
                    },
                },
            }))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("disk failed to create");

    // Get the silo but this time using the silo admin view
    let default_silo_util: Utilization =
        object_get(client, "/v1/utilization").await;

    assert_eq!(
        default_silo_util.provisioned,
        VirtualResourceCounts {
            cpus: 4,
            memory: ByteCount::from_gibibytes_u32(1),
            storage: ByteCount::from_gibibytes_u32(2)
        }
    );
}

async fn util_list(client: &ClientTestContext) -> Vec<SiloUtilization> {
    objects_list_page_authz(client, "/v1/system/utilization/silos").await.items
}

/// Could be inlined, but pulling it out makes the test much clearer
async fn create_resources_in_test_suite_silo(client: &ClientTestContext) {
    // in order to create resources in test-suite-silo, we have to create a user
    // with the right perms so we have a user ID on hand to use in the authn_as
    let silo_url = "/v1/system/silos/test-suite-silo";
    let test_suite_silo: Silo = object_get(client, silo_url).await;
    link_ip_pool(client, "default", &test_suite_silo.identity.id, true).await;
    let user1 = create_local_user(
        client,
        &test_suite_silo,
        &"user1".parse().unwrap(),
        test_params::UserPassword::LoginDisallowed,
    )
    .await;
    grant_iam(
        client,
        silo_url,
        SiloRole::Collaborator,
        user1.id,
        AuthnMode::PrivilegedUser,
    )
    .await;

    let test_project_name = "test-suite-project";

    // Create project in test-suite-silo as the test user
    NexusRequest::objects_post(
        client,
        "/v1/projects",
        &params::ProjectCreate {
            identity: IdentityMetadataCreateParams {
                name: test_project_name.parse().unwrap(),
                description: String::new(),
            },
        },
    )
    .authn_as(AuthnMode::SiloUser(user1.id))
    .execute()
    .await
    .expect("failed to create project in test-suite-silo");

    // Create instance in test-suite-silo as the test user
    let instance_params = params::InstanceCreate {
        identity: IdentityMetadataCreateParams {
            name: "test-inst".parse().unwrap(),
            description: "test instance in test-suite-silo".to_string(),
        },
        ncpus: InstanceCpuCount::try_from(2).unwrap(),
        memory: ByteCount::from_gibibytes_u32(4),
        hostname: "test-inst".parse().unwrap(),
        user_data: vec![],
        ssh_public_keys: None,
        network_interfaces: params::InstanceNetworkInterfaceAttachment::Default,
        external_ips: vec![],
        disks: vec![],
        boot_disk: None,
        cpu_platform: None,
        start: true,
        auto_restart_policy: Default::default(),
        anti_affinity_groups: Vec::new(),
        multicast_groups: Vec::new(),
    };

    NexusRequest::objects_post(
        client,
        &format!("/v1/instances?project={}", test_project_name),
        &instance_params,
    )
    .authn_as(AuthnMode::SiloUser(user1.id))
    .execute()
    .await
    .expect("failed to create instance in test-suite-silo");
}
