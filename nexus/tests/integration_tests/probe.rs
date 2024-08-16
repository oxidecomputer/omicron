use dropshot::HttpErrorResponseBody;
use http::{Method, StatusCode};
use nexus_test_utils::{
    http_testing::{AuthnMode, NexusRequest},
    resource_helpers::{create_default_ip_pool, create_project},
    SLED_AGENT_UUID,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::{params::ProbeCreate, shared::ProbeInfo};
use omicron_common::api::external::{IdentityMetadataCreateParams, Probe};

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_probe_basic_crud(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;

    create_default_ip_pool(&client).await;
    create_project(&client, "nebula").await;

    let probes = NexusRequest::iter_collection_authn::<ProbeInfo>(
        client,
        "/experimental/v1/probes?project=nebula",
        "",
        None,
    )
    .await
    .expect("Failed to list probes")
    .all_items;

    assert_eq!(probes.len(), 0, "Expected zero probes");

    let params = ProbeCreate {
        identity: IdentityMetadataCreateParams {
            name: "class1".parse().unwrap(),
            description: "subspace relay probe".to_owned(),
        },
        ip_pool: None,
        sled: SLED_AGENT_UUID.parse().unwrap(),
    };

    let created: Probe = NexusRequest::objects_post(
        client,
        "/experimental/v1/probes?project=nebula",
        &params,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    let probes = NexusRequest::iter_collection_authn::<ProbeInfo>(
        client,
        "/experimental/v1/probes?project=nebula",
        "",
        None,
    )
    .await
    .expect("Failed to list probes")
    .all_items;

    assert_eq!(probes.len(), 1, "Expected one probe");
    assert_eq!(probes[0].id, created.identity.id);

    let error: HttpErrorResponseBody = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        Method::GET,
        "/experimental/v1/probes/class2?project=nebula",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    assert_eq!(error.message, "not found: probe with name \"class2\"");

    NexusRequest::object_get(
        client,
        "/experimental/v1/probes/class1?project=nebula",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect("failed to view probe")
    .parsed_body::<ProbeInfo>()
    .expect("failed to parse probe info");

    let fetched: ProbeInfo = NexusRequest::object_get(
        client,
        "/experimental/v1/probes/class1?project=nebula",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(fetched.id, created.identity.id);

    NexusRequest::object_delete(
        client,
        "/experimental/v1/probes/class1?project=nebula",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    let probes = NexusRequest::iter_collection_authn::<ProbeInfo>(
        client,
        "/experimental/v1/probes?project=nebula",
        "",
        None,
    )
    .await
    .expect("Failed to list probes after delete")
    .all_items;

    assert_eq!(probes.len(), 0, "Expected zero probes");
}
