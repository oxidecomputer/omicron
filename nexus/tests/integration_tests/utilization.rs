use dropshot::test_util::ClientTestContext;
use http::Method;
use http::StatusCode;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils::resource_helpers::create_default_ip_pool;
use nexus_test_utils::resource_helpers::create_instance;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params;
use nexus_types::external_api::params::SiloQuotasCreate;
use nexus_types::external_api::views::SiloUtilization;
use nexus_types::external_api::views::Utilization;
use nexus_types::external_api::views::VirtualResourceCounts;
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::IdentityMetadataCreateParams;

static PROJECT_NAME: &str = "utilization-test-project";
static INSTANCE_NAME: &str = "utilization-test-instance";

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_utilization(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    create_default_ip_pool(&client).await;

    // default-silo has quotas, but is explicitly filtered out by ID in the DB
    // query to avoid user confusion. so list is empty
    let current_util = fetch_util(client).await;
    assert!(current_util.is_empty());

    // set high quota for test silo
    let _ = NexusRequest::object_put(
        client,
        "/v1/system/silos/test-suite-silo/quotas",
        Some(&params::SiloQuotasCreate::arbitrarily_high_default()),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await;

    // now test-suite-silo shows up
    let current_util = fetch_util(client).await;
    assert_eq!(current_util.len(), 1);
    assert_eq!(current_util[0].silo_name, "test-suite-silo");
    assert_eq!(current_util[0].provisioned, SiloQuotasCreate::empty().into());
    assert_eq!(
        current_util[0].allocated,
        SiloQuotasCreate::arbitrarily_high_default().into()
    );

    // now we take the quota back off of test-suite-silo and end up empty again
    let _ = NexusRequest::object_put(
        client,
        "/v1/system/silos/test-suite-silo/quotas",
        Some(&params::SiloQuotasCreate::empty()),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await;

    let current_util = fetch_util(client).await;

    assert!(current_util.is_empty());

    // you can still fetch utilization for the default silo by name if you want,
    // so we test that below

    let _ = create_project(&client, &PROJECT_NAME).await;
    let _ = create_instance(client, &PROJECT_NAME, &INSTANCE_NAME).await;

    // Start instance
    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            format!(
                "/v1/instances/{}/start?project={}",
                &INSTANCE_NAME, &PROJECT_NAME
            )
            .as_str(),
        )
        .body(None as Option<&serde_json::Value>)
        .expect_status(Some(StatusCode::ACCEPTED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to start instance");

    // get utilization for just the default silo
    let default_silo_util = NexusRequest::object_get(
        client,
        "/v1/system/utilization/silos/default-silo",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SiloUtilization>()
    .await;

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

    // provision disk
    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            format!("/v1/disks?project={}", &PROJECT_NAME).as_str(),
        )
        .body(Some(&params::DiskCreate {
            identity: IdentityMetadataCreateParams {
                name: "test-disk".parse().unwrap(),
                description: "".into(),
            },
            size: ByteCount::from_gibibytes_u32(2),
            disk_source: params::DiskSource::Blank {
                block_size: params::BlockSize::try_from(512).unwrap(),
            },
        }))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("disk failed to create");

    // Get the silo but this time using the silo admin view
    let default_silo_util = NexusRequest::object_get(client, "/v1/utilization")
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to fetch utilization for current (default) silo")
        .parsed_body::<Utilization>()
        .unwrap();

    assert_eq!(
        default_silo_util.provisioned,
        VirtualResourceCounts {
            cpus: 4,
            memory: ByteCount::from_gibibytes_u32(1),
            storage: ByteCount::from_gibibytes_u32(2)
        }
    );
}

async fn fetch_util(client: &ClientTestContext) -> Vec<SiloUtilization> {
    objects_list_page_authz(client, "/v1/system/utilization/silos").await.items
}
