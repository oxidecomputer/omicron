// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for omdb multicast commands with real data.
//!
//! These tests verify that omdb correctly formats multicast data by creating
//! actual multicast pools, groups, and members, then running omdb commands
//! and checking the output.

use futures::future::join3;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::params::MulticastGroupMemberAdd;
use nexus_types::external_api::views::{MulticastGroup, MulticastGroupMember};
use omicron_common::api::external::NameOrId;
use std::net::IpAddr;
use std::path::PathBuf;
use std::process::Command;

use super::{
    ControlPlaneTestContext, create_multicast_ip_pool,
    create_multicast_ip_pool_with_range, instance_for_multicast_groups,
    mcast_group_url, wait_for_group_active, wait_for_member_state,
};
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_project, object_create,
};

const PROJECT_NAME: &str = "omdb-test-project";
const TARGET_DIR: &str = "target";
const OMDB_BIN: &str = "omdb";
const BUILD_PROFILES: &[&str] = &["debug", "release"];

/// Find the omdb binary path.
///
/// Since omdb is not built as part of omicron-nexus tests, we look for it
/// in the target directory relative to CARGO_MANIFEST_DIR.
fn find_omdb() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir.parent().unwrap();

    for profile in BUILD_PROFILES {
        let omdb_path =
            workspace_root.join(TARGET_DIR).join(profile).join(OMDB_BIN);
        if omdb_path.exists() {
            return omdb_path;
        }
    }

    PathBuf::from(OMDB_BIN)
}

/// Run an omdb command and return its stdout.
fn run_omdb(db_url: &str, args: &[&str]) -> String {
    let cmd_path = find_omdb();
    let output = Command::new(&cmd_path)
        .env("OMDB_DB_URL", db_url)
        .args(args)
        .output()
        .expect(
            "failed to execute `omdb` - ensure `cargo build -p omicron-omdb` has been run",
        );

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("omdb command failed with args {args:?}:\nstderr: {stderr}");
    }

    String::from_utf8_lossy(&output.stdout).to_string()
}

/// Test omdb multicast pools command.
#[nexus_test]
async fn test_omdb_multicast_pools(cptestctx: &ControlPlaneTestContext) {
    let db_url = cptestctx.database.listen_url().to_string();
    let client = &cptestctx.external_client;

    // Before creating any pools, should show "no multicast IP pools found"
    let output = run_omdb(&db_url, &["db", "multicast", "pools"]);
    assert!(
        output.contains("no multicast IP pools found"),
        "Expected empty pool message, got: {output}"
    );

    // Create a multicast pool
    create_multicast_ip_pool(client, "test-mcast-pool").await;

    // Now should show the pool
    let output = run_omdb(&db_url, &["db", "multicast", "pools"]);
    assert!(
        output.contains("test-mcast-pool"),
        "Expected pool name in output, got: {output}"
    );
    assert!(
        output.contains("224.2.0.0"),
        "Expected pool range start in output, got: {output}"
    );
}

/// Test omdb multicast groups, members, and info commands.
///
/// This consolidated test verifies all multicast commands work with actual data.
#[nexus_test]
async fn test_omdb_multicast_commands(cptestctx: &ControlPlaneTestContext) {
    let db_url = cptestctx.database.listen_url().to_string();
    let client = &cptestctx.external_client;

    // Setup: create pools and project
    let (_, _, _) = join3(
        create_default_ip_pool(client),
        create_project(client, PROJECT_NAME),
        create_multicast_ip_pool(client, "test-mcast-pool"),
    )
    .await;

    // Create an instance without multicast groups first
    let instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "test-instance",
        false, // don't start
        &[],   // no multicast groups yet
    )
    .await;

    // Add a multicast member via API (this implicitly creates the group)
    let member_add_url = format!(
        "/v1/multicast-groups/test-mcast-group/members?project={PROJECT_NAME}"
    );

    object_create::<_, MulticastGroupMember>(
        client,
        &member_add_url,
        &MulticastGroupMemberAdd {
            instance: NameOrId::Id(instance.identity.id),
            source_ips: None, // ASM (Any-Source Multicast)
        },
    )
    .await;

    // Wait for the group to become "Active"
    wait_for_group_active(client, "test-mcast-group").await;

    // Get the group details for later tests
    let group_url = mcast_group_url("test-mcast-group");
    let group: MulticastGroup = NexusRequest::object_get(client, &group_url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("failed to get group")
        .parsed_body()
        .expect("failed to parse group");

    // Test: omdb db multicast groups
    let output = run_omdb(&db_url, &["db", "multicast", "groups"]);
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name in groups output, got: {output}"
    );

    // Verify ASM (any-source multicast) is shown since we didn't specify source_ips
    assert!(
        output.contains("ASM"),
        "Expected ASM in sources column, got: {output}"
    );

    // Test: omdb db multicast groups --state active
    let output =
        run_omdb(&db_url, &["db", "multicast", "groups", "--state", "active"]);
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name with state filter, got: {output}"
    );

    // Test: omdb db multicast groups --pool
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "groups", "--pool", "test-mcast-pool"],
    );
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name with pool filter, got: {output}"
    );

    // Test: omdb db multicast members
    let output = run_omdb(&db_url, &["db", "multicast", "members"]);
    assert!(
        output.contains(&instance.identity.id.to_string()),
        "Expected instance ID in members output, got: {output}"
    );

    // Verify the IP column shows the group's allocated IP
    let group_ip = group.multicast_ip.to_string();
    assert!(
        output.contains(&group_ip),
        "Expected multicast IP {group_ip} in members output, got: {output}"
    );

    // Test: omdb db multicast members --group-name
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--group-name", "test-mcast-group"],
    );
    assert!(
        output.contains(&instance.identity.id.to_string()),
        "Expected instance ID with group-name filter, got: {output}"
    );

    // Test: omdb db multicast members --group-ip (reuses group_ip from above)
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--group-ip", &group_ip],
    );
    assert!(
        output.contains(&instance.identity.id.to_string()),
        "Expected instance ID with group-ip filter, got: {output}"
    );

    // Test: omdb db multicast members --group-id
    let group_id = group.identity.id.to_string();
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--group-id", &group_id],
    );
    assert!(
        output.contains(&instance.identity.id.to_string()),
        "Expected instance ID with group-id filter, got: {output}"
    );

    // Test: omdb db multicast members --state left
    // Wait for the RPW reconciler to transition member to "Left" state
    // (instance isn't running, so no sled_id assignment)
    wait_for_member_state(
        cptestctx,
        "test-mcast-group",
        instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Left,
    )
    .await;
    let output =
        run_omdb(&db_url, &["db", "multicast", "members", "--state", "left"]);
    assert!(
        output.contains(&instance.identity.id.to_string()),
        "Expected instance ID with state=left filter, got: {output}"
    );

    // Test: omdb db multicast members --sled-id
    // Create a started instance so the member gets a sled_id
    let started_instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "started-instance",
        true, // start the instance
        &[],
    )
    .await;

    // Add member to a new group for the started instance
    let sled_member_url = format!(
        "/v1/multicast-groups/sled-test-group/members?project={PROJECT_NAME}"
    );
    object_create::<_, MulticastGroupMember>(
        client,
        &sled_member_url,
        &MulticastGroupMemberAdd {
            instance: NameOrId::Id(started_instance.identity.id),
            source_ips: None,
        },
    )
    .await;

    wait_for_group_active(client, "sled-test-group").await;

    // Query members by sled_id - the started instance should be on first_sled
    let sled_id = cptestctx.first_sled_id().to_string();
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--sled-id", &sled_id],
    );
    assert!(
        output.contains(&started_instance.identity.id.to_string()),
        "Expected started instance ID with sled-id filter, got: {output}"
    );

    // Test: omdb db multicast members --state joined
    // Wait for the started instance's member to reach "Joined" state
    wait_for_member_state(
        cptestctx,
        "sled-test-group",
        started_instance.identity.id,
        nexus_db_model::MulticastGroupMemberState::Joined,
    )
    .await;

    // Now test the --state joined filter
    let output_joined =
        run_omdb(&db_url, &["db", "multicast", "members", "--state", "joined"]);
    assert!(
        output_joined.contains(&started_instance.identity.id.to_string()),
        "Expected started instance in joined state, got: {output_joined}"
    );

    // Verify started instance is NOT in left state
    let output_left =
        run_omdb(&db_url, &["db", "multicast", "members", "--state", "left"]);
    assert!(
        !output_left.contains(&started_instance.identity.id.to_string()),
        "Started instance should NOT be in left state, got: {output_left}"
    );

    // Test: combined filters (--group-name + --state)
    // The started instance's member should appear when filtering by both
    let output_combined = run_omdb(
        &db_url,
        &[
            "db",
            "multicast",
            "members",
            "--group-name",
            "sled-test-group",
            "--state",
            "joined",
        ],
    );
    assert!(
        output_combined.contains(&started_instance.identity.id.to_string()),
        "Expected started instance with combined filters, got: {output_combined}"
    );

    // Test: combined filters that should return empty (wrong group + state)
    let output_combined_empty = run_omdb(
        &db_url,
        &[
            "db",
            "multicast",
            "members",
            "--group-name",
            "test-mcast-group",
            "--state",
            "joined",
        ],
    );
    // test-mcast-group has a non-started instance, so it should NOT be in joined state
    assert!(
        !output_combined_empty
            .contains(&started_instance.identity.id.to_string()),
        "Started instance should NOT appear in wrong group filter, got: {output_combined_empty}"
    );

    // Test: omdb db multicast info --name
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "info", "--name", "test-mcast-group"],
    );
    assert!(
        output.contains("MULTICAST GROUP"),
        "Expected group header in info output, got: {output}"
    );
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name in info output, got: {output}"
    );
    assert!(
        output.contains("test-mcast-pool"),
        "Expected pool name in info output, got: {output}"
    );
    assert!(
        output.contains("MEMBERS"),
        "Expected members section in info output, got: {output}"
    );

    // Test: omdb db multicast info --ip
    let output =
        run_omdb(&db_url, &["db", "multicast", "info", "--ip", &group_ip]);
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name when querying by IP, got: {output}"
    );

    // Test: omdb db multicast info --group-id (reuses group_id from members test)
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "info", "--group-id", &group_id],
    );
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name when querying by ID, got: {output}"
    );

    // Test SSM (Source-Specific Multicast) - create a group with source IPs
    // SSM requires IPs from the 232.x.x.x range, so create an SSM pool first
    create_multicast_ip_pool_with_range(
        client,
        "test-ssm-pool",
        (232, 1, 0, 0),   // SSM range start
        (232, 1, 0, 255), // SSM range end
    )
    .await;

    let ssm_instance = instance_for_multicast_groups(
        cptestctx,
        PROJECT_NAME,
        "ssm-instance",
        false,
        &[],
    )
    .await;

    let ssm_member_url = format!(
        "/v1/multicast-groups/ssm-group/members?project={PROJECT_NAME}"
    );
    object_create::<_, MulticastGroupMember>(
        client,
        &ssm_member_url,
        &MulticastGroupMemberAdd {
            instance: NameOrId::Id(ssm_instance.identity.id),
            source_ips: Some(vec![
                "10.0.0.1".parse::<IpAddr>().unwrap(),
                "10.0.0.2".parse::<IpAddr>().unwrap(),
            ]),
        },
    )
    .await;

    wait_for_group_active(client, "ssm-group").await;

    // Verify SSM sources show in groups list
    let output = run_omdb(&db_url, &["db", "multicast", "groups"]);
    assert!(
        output.contains("ssm-group"),
        "Expected SSM group in output, got: {output}"
    );
    assert!(
        output.contains("10.0.0.1") && output.contains("10.0.0.2"),
        "Expected SSM source IPs in output, got: {output}"
    );

    // Verify SSM sources show in info command
    let output =
        run_omdb(&db_url, &["db", "multicast", "info", "--name", "ssm-group"]);
    assert!(
        output.contains("10.0.0.1") || output.contains("10.0.0.2"),
        "Expected SSM source IPs in info output, got: {output}"
    );
}
