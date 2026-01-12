// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for multicast IP pool selection.
//!
//! These tests verify pool selection behavior when joining multicast groups:
//! - SSM→ASM fallback when `has_sources=true` but no SSM pool is available
//! - IP version disambiguation when both IPv4 and IPv6 pools exist

use http::StatusCode;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::InstanceMulticastGroupJoin;
use nexus_types::external_api::views::{IpVersion, MulticastGroupMember};
use std::net::IpAddr;

use nexus_test_utils::resource_helpers::{
    create_default_ip_pools, create_project,
};

use super::*;

const PROJECT_NAME: &str = "pool-selection-project";

/// Test SSM→ASM fallback when joining with sources but only ASM pool exists.
///
/// When `has_sources=true` and no SSM pool is linked to the silo, the system
/// should fall back to using an ASM pool. This is valid because source filtering
/// still works on ASM addresses via IGMPv3/MLDv2, just without SSM's network-level
/// source guarantees.
#[nexus_test]
async fn test_ssm_to_asm_fallback_with_sources(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Setup: create only ASM pool (no SSM pool)
    ops::join3(
        create_default_ip_pools(client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(client, "asm-only-pool"),
    )
    .await;
    ensure_multicast_test_ready(cptestctx).await;

    // Create an instance
    let instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "fallback-instance",
        false, // don't start
        &[],
    )
    .await;

    // Join a group BY NAME with sources:
    // - has_sources=true (sources provided)
    // - Pool selection will try SSM first
    // - No SSM pool → should fall back to ASM pool
    // - Group will be created with ASM IP (224.x.x.x)
    let join_url = format!(
        "/v1/instances/{}/multicast-groups/asm-fallback-group?project={PROJECT_NAME}",
        instance.identity.id
    );

    let member: MulticastGroupMember = put_upsert(
        client,
        &join_url,
        &InstanceMulticastGroupJoin {
            source_ips: Some(vec![
                "10.0.0.1".parse::<IpAddr>().unwrap(),
                "10.0.0.2".parse::<IpAddr>().unwrap(),
            ]),
            ip_version: None,
        },
    )
    .await;

    // Verify member was created
    assert_eq!(member.instance_id, instance.identity.id);

    // Activate reconciler to process the new group ("Creating" → "Active")
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Wait for group to become "Active" (reconciler runs DPD ensure saga)
    let group = wait_for_group_active(client, "asm-fallback-group").await;

    // Verify the group got an ASM IP (224.x.x.x) from the fallback
    let ip = group.multicast_ip;
    match ip {
        IpAddr::V4(v4) => {
            assert!(
                v4.octets()[0] == 224,
                "Expected ASM IP (224.x.x.x) from fallback, got {ip}"
            );
        }
        IpAddr::V6(_) => {
            panic!("Expected IPv4 ASM address, got IPv6: {ip}");
        }
    }

    // Verify group is Active
    assert_eq!(group.state, "Active");
}

/// Test that SSM pool is preferred when both ASM and SSM pools exist.
#[nexus_test]
async fn test_ssm_pool_preferred_with_sources(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Setup: create both ASM and SSM pools
    ops::join4(
        create_default_ip_pools(client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(client, "asm-pool"),
        create_multicast_ip_pool_with_range(
            client,
            "ssm-pool",
            (232, 1, 0, 0),
            (232, 1, 0, 255),
        ),
    )
    .await;
    ensure_multicast_test_ready(cptestctx).await;

    // Create an instance
    let instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "ssm-prefer-instance",
        false,
        &[],
    )
    .await;

    // Join with sources - should use SSM pool (not ASM)
    let join_url = format!(
        "/v1/instances/{}/multicast-groups/ssm-preferred-group?project={PROJECT_NAME}",
        instance.identity.id
    );

    put_upsert::<_, MulticastGroupMember>(
        client,
        &join_url,
        &InstanceMulticastGroupJoin {
            source_ips: Some(vec!["10.0.0.1".parse::<IpAddr>().unwrap()]),
            ip_version: None,
        },
    )
    .await;

    // Activate reconciler to process the new group ("Creating" → "Active")
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Wait for group to become Active
    let group = wait_for_group_active(client, "ssm-preferred-group").await;

    // Verify the group got an SSM IP (232.x.x.x)
    let ip = group.multicast_ip;
    match ip {
        IpAddr::V4(v4) => {
            assert!(
                v4.octets()[0] == 232,
                "Expected SSM IP (232.x.x.x), got {ip}"
            );
        }
        IpAddr::V6(_) => {
            panic!("Expected IPv4 SSM address, got IPv6: {ip}");
        }
    }

    assert_eq!(group.state, "Active");
}

/// Test that ASM pool is used directly when no sources provided.
#[nexus_test]
async fn test_asm_pool_used_without_sources(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Setup: create both ASM and SSM pools
    ops::join4(
        create_default_ip_pools(client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(client, "asm-pool"),
        create_multicast_ip_pool_with_range(
            client,
            "ssm-pool",
            (232, 1, 0, 0),
            (232, 1, 0, 255),
        ),
    )
    .await;
    ensure_multicast_test_ready(cptestctx).await;

    // Create an instance
    let instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "asm-direct-instance",
        false,
        &[],
    )
    .await;

    // Join without sources - should use ASM pool directly (skip SSM)
    let join_url = format!(
        "/v1/instances/{}/multicast-groups/asm-direct-group?project={PROJECT_NAME}",
        instance.identity.id
    );

    put_upsert::<_, MulticastGroupMember>(
        client,
        &join_url,
        &InstanceMulticastGroupJoin { source_ips: None, ip_version: None },
    )
    .await;

    // Activate reconciler to process the new group ("Creating" → "Active")
    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;

    // Wait for group to become Active
    let group = wait_for_group_active(client, "asm-direct-group").await;

    // Verify the group got an ASM IP (224.x.x.x)
    let ip = group.multicast_ip;
    match ip {
        IpAddr::V4(v4) => {
            assert!(
                v4.octets()[0] == 224,
                "Expected ASM IP (224.x.x.x), got {ip}"
            );
        }
        IpAddr::V6(_) => {
            panic!("Expected IPv4 ASM address, got IPv6: {ip}");
        }
    }

    assert_eq!(group.state, "Active");
}

/// Test IP version disambiguation when both IPv4 and IPv6 multicast pools exist.
///
/// This verifies that:
/// 1. Joining by name without `ip_version` returns an error when both pools exist
/// 2. Specifying `ip_version=V4` selects the IPv4 pool
/// 3. Specifying `ip_version=V6` selects the IPv6 pool
#[nexus_test]
async fn test_ip_version_disambiguation(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Setup: create both IPv4 and IPv6 multicast pools
    ops::join4(
        create_default_ip_pools(client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(client, "ipv4-mcast-pool"),
        create_multicast_ip_pool_v6(client, "ipv6-mcast-pool"),
    )
    .await;
    ensure_multicast_test_ready(cptestctx).await;

    // Create an instance
    let instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "disambiguation-instance",
        false,
        &[],
    )
    .await;

    // Attempting to join by name without ip_version should fail
    // because we can't determine which pool to use
    let join_url = format!(
        "/v1/instances/{}/multicast-groups/ambiguous-group?project={PROJECT_NAME}",
        instance.identity.id
    );

    let error: dropshot::HttpErrorResponseBody = NexusRequest::new(
        RequestBuilder::new(client, http::Method::PUT, &join_url)
            .body(Some(&InstanceMulticastGroupJoin {
                source_ips: None,
                ip_version: None,
            }))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();

    // Should get InvalidRequest error due to ambiguous IP version
    assert_eq!(
        error.error_code,
        Some("InvalidRequest".to_string()),
        "Expected InvalidRequest error for ambiguous IP version, got: {:?} (message: {})",
        error.error_code,
        error.message
    );

    // Join with ip_version=V4 - should succeed and get IPv4 address
    let v4_join_url = format!(
        "/v1/instances/{}/multicast-groups/ipv4-group?project={PROJECT_NAME}",
        instance.identity.id
    );

    let v4_member: MulticastGroupMember = put_upsert(
        client,
        &v4_join_url,
        &InstanceMulticastGroupJoin {
            source_ips: None,
            ip_version: Some(IpVersion::V4),
        },
    )
    .await;

    assert_eq!(v4_member.instance_id, instance.identity.id);

    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;
    let v4_group = wait_for_group_active(client, "ipv4-group").await;

    match v4_group.multicast_ip {
        IpAddr::V4(v4) => {
            assert!(
                v4.octets()[0] == 224,
                "Expected IPv4 multicast address (224.x.x.x), got {}",
                v4_group.multicast_ip
            );
        }
        IpAddr::V6(_) => {
            panic!(
                "Expected IPv4 address when ip_version=V4, got {}",
                v4_group.multicast_ip
            );
        }
    }

    // Join with ip_version=V6 - should succeed and get IPv6 address
    let v6_join_url = format!(
        "/v1/instances/{}/multicast-groups/ipv6-group?project={PROJECT_NAME}",
        instance.identity.id
    );

    let v6_member: MulticastGroupMember = put_upsert(
        client,
        &v6_join_url,
        &InstanceMulticastGroupJoin {
            source_ips: None,
            ip_version: Some(IpVersion::V6),
        },
    )
    .await;

    assert_eq!(v6_member.instance_id, instance.identity.id);

    wait_for_multicast_reconciler(&cptestctx.lockstep_client).await;
    let v6_group = wait_for_group_active(client, "ipv6-group").await;

    match v6_group.multicast_ip {
        IpAddr::V4(_) => {
            panic!(
                "Expected IPv6 address when ip_version=V6, got {}",
                v6_group.multicast_ip
            );
        }
        IpAddr::V6(v6) => {
            // Global scope multicast: ff0e::/16
            assert!(
                v6.segments()[0] == 0xff0e,
                "Expected IPv6 global multicast (ff0e::), got {}",
                v6_group.multicast_ip
            );
        }
    }
}
