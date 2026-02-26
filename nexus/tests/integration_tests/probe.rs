use dropshot::HttpErrorResponseBody;
use http::{Method, StatusCode};
use nexus_test_utils::{
    SLED_AGENT_UUID,
    http_testing::{AuthnMode, NexusRequest},
    resource_helpers::{create_default_ip_pools, create_project},
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::ip_pool::PoolSelector;
use nexus_types::external_api::probe::{ProbeCreate, ProbeInfo};
use omicron_common::api::external::{
    IdentityMetadataCreateParams, IpVersion, Probe,
};

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_probe_basic_crud(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;

    let (_v4_pool, v6_pool) = create_default_ip_pools(&client).await;
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
        pool_selector: PoolSelector::Explicit {
            pool: v6_pool.identity.name.clone().into(),
        },
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

/// Test that probes can use PoolSelector::Default with `ip_version` to
/// disambiguate between IPv4 and IPv6 default pools in a dual-stack setup.
#[nexus_test]
async fn test_probe_pool_selector_ip_version(ctx: &ControlPlaneTestContext) {
    let client = &ctx.external_client;

    // Create both IPv4 and IPv6 default pools (dual-stack setup)
    create_default_ip_pools(&client).await;
    create_project(&client, "nebula").await;

    // Create probe using default pool with IPv6 preference
    let params_v6 = ProbeCreate {
        identity: IdentityMetadataCreateParams {
            name: "probe-v6".parse().unwrap(),
            description: "IPv6 probe".to_owned(),
        },
        pool_selector: PoolSelector::Auto { ip_version: Some(IpVersion::V6) },
        sled: SLED_AGENT_UUID.parse().unwrap(),
    };

    let created_v6: Probe = NexusRequest::objects_post(
        client,
        "/experimental/v1/probes?project=nebula",
        &params_v6,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Verify the probe was created
    let probe_info: ProbeInfo = NexusRequest::object_get(
        client,
        "/experimental/v1/probes/probe-v6?project=nebula",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(probe_info.id, created_v6.identity.id);
    // Verify the probe got an IPv6 address
    assert!(
        !probe_info.external_ips.is_empty(),
        "Probe should have an external IP"
    );
    assert!(
        probe_info.external_ips[0].ip.is_ipv6(),
        "Probe should have an IPv6 address, got {:?}",
        probe_info.external_ips[0].ip
    );

    // Create probe using default pool with IPv4 preference
    let params_v4 = ProbeCreate {
        identity: IdentityMetadataCreateParams {
            name: "probe-v4".parse().unwrap(),
            description: "IPv4 probe".to_owned(),
        },
        pool_selector: PoolSelector::Auto { ip_version: Some(IpVersion::V4) },
        sled: SLED_AGENT_UUID.parse().unwrap(),
    };

    let created_v4: Probe = NexusRequest::objects_post(
        client,
        "/experimental/v1/probes?project=nebula",
        &params_v4,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Verify the probe was created with IPv4
    let probe_info_v4: ProbeInfo = NexusRequest::object_get(
        client,
        "/experimental/v1/probes/probe-v4?project=nebula",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    assert_eq!(probe_info_v4.id, created_v4.identity.id);
    assert!(
        !probe_info_v4.external_ips.is_empty(),
        "Probe should have an external IP"
    );
    assert!(
        probe_info_v4.external_ips[0].ip.is_ipv4(),
        "Probe should have an IPv4 address, got {:?}",
        probe_info_v4.external_ips[0].ip
    );

    // Cleanup
    NexusRequest::object_delete(
        client,
        "/experimental/v1/probes/probe-v6?project=nebula",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    NexusRequest::object_delete(
        client,
        "/experimental/v1/probes/probe-v4?project=nebula",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();
}
