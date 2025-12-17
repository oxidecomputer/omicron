// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for omdb multicast commands with real data.
//!
//! These tests verify that omdb correctly formats multicast data by creating
//! actual multicast pools, groups, and members, then running omdb commands
//! and checking the output.

use std::net::IpAddr;
use std::path::PathBuf;
use std::process::Command;

use futures::future::join3;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
use nexus_test_utils::resource_helpers::{
    create_default_ip_pool, create_project,
};
use nexus_test_utils_macros::nexus_test;

use super::*;

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

    // Now should show the pool with all columns
    let output = run_omdb(&db_url, &["db", "multicast", "pools"]);
    // pool name
    assert!(
        output.contains("test-mcast-pool"),
        "Expected pool name in output, got: {output}"
    );
    // first address
    assert!(
        output.contains("224.2.0.0"),
        "Expected first address in output, got: {output}"
    );
    // last address
    assert!(
        output.contains("224.2.255.255"),
        "Expected last address in output, got: {output}"
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
    // Use instance-centric join endpoint: POST /v1/instances/{instance}/multicast-groups/{group}
    let join_url = format!(
        "/v1/instances/{}/multicast-groups/test-mcast-group?project={PROJECT_NAME}",
        instance.identity.id
    );

    put_upsert::<_, MulticastGroupMember>(
        client,
        &join_url,
        &InstanceMulticastGroupJoin {
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
    // group id
    assert!(
        output.contains(&group.identity.id.to_string()),
        "Expected group id in output, got: {output}"
    );
    // group name
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name in output, got: {output}"
    );
    // state
    assert!(
        output.contains("Active"),
        "Expected state 'Active' in output, got: {output}"
    );
    // multicast ip
    assert!(
        output.contains(&group.multicast_ip.to_string()),
        "Expected multicast ip in output, got: {output}"
    );
    // range (ASM for 224.x.x.x)
    assert!(
        output.contains("ASM"),
        "Expected range 'ASM' in output, got: {output}"
    );
    // vni (column exists but VNI is internal, not exposed in the external API)
    assert!(
        output.contains("VNI"),
        "Expected VNI column in output, got: {output}"
    );
    // members (instance@sled format, "-" when not started)
    assert!(
        output.contains("test-instance@-"),
        "Expected member 'test-instance@-' in output, got: {output}"
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
    // group name
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name in members output, got: {output}"
    );
    // parent id (instance id)
    assert!(
        output.contains(&instance.identity.id.to_string()),
        "Expected parent id in members output, got: {output}"
    );
    // multicast ip
    let group_ip = group.multicast_ip.to_string();
    assert!(
        output.contains(&group_ip),
        "Expected multicast ip in members output, got: {output}"
    );
    // sources ("-" for ASM)
    assert!(
        output.contains("-"),
        "Expected sources '-' for ASM in members output, got: {output}"
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
    let sled_join_url = format!(
        "/v1/instances/{}/multicast-groups/sled-test-group?project={PROJECT_NAME}",
        started_instance.identity.id
    );
    put_upsert::<_, MulticastGroupMember>(
        client,
        &sled_join_url,
        &InstanceMulticastGroupJoin { source_ips: None },
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
    // state column shows "Joined"
    assert!(
        output_joined.contains("Joined"),
        "Expected 'Joined' state in members output, got: {output_joined}"
    );
    // sled_id column shows the sled UUID
    assert!(
        output_joined.contains(&sled_id),
        "Expected sled_id in members output, got: {output_joined}"
    );

    // Verify info for started instance shows sled serial (not "-")
    let output_info = run_omdb(
        &db_url,
        &["db", "multicast", "info", "--name", "sled-test-group"],
    );
    // member instance name
    assert!(
        output_info.contains("started-instance"),
        "Expected 'started-instance' in info members, got: {output_info}"
    );
    // underlay group should be present for active group
    assert!(
        output_info.contains("UNDERLAY GROUP"),
        "Expected 'UNDERLAY GROUP' section in info output, got: {output_info}"
    );

    // Verify groups output shows started instance with sled serial (not "-")
    let output_groups = run_omdb(&db_url, &["db", "multicast", "groups"]);
    // sled-test-group should show "started-instance@<serial>" not "started-instance@-"
    assert!(
        output_groups.contains("started-instance@"),
        "Expected 'started-instance@' in groups members column, got: {output_groups}"
    );
    // The sled serial should appear (not just "-")
    // Note: test sled serial is typically "serial0" or similar
    assert!(
        !output_groups.contains("started-instance@-"),
        "Started instance should have sled serial, not '-', got: {output_groups}"
    );

    // Verify underlay_ip column shows an IP for active groups
    // Active groups with joined members should have an underlay group assigned
    // The underlay IP is in the ff04::/64 range (admin-scoped IPv6 multicast)
    assert!(
        output_groups.contains("ff04:"),
        "Expected underlay_ip (ff04:*) for active group in groups output, got: {output_groups}"
    );

    // Verify started instance is not in "Left" state
    let output_left =
        run_omdb(&db_url, &["db", "multicast", "members", "--state", "left"]);
    assert!(
        !output_left.contains(&started_instance.identity.id.to_string()),
        "Started instance should not be in 'Left' state, got: {output_left}"
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
    // test-mcast-group has a non-started instance, so it should not be in "Joined" state
    assert!(
        !output_combined_empty
            .contains(&started_instance.identity.id.to_string()),
        "Started instance should not appear in wrong group filter, got: {output_combined_empty}"
    );

    // Test: omdb db multicast info --name
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "info", "--name", "test-mcast-group"],
    );
    // section header
    assert!(
        output.contains("MULTICAST GROUP"),
        "Expected 'MULTICAST GROUP' header in info output, got: {output}"
    );
    // id
    assert!(
        output.contains(&group.identity.id.to_string()),
        "Expected group id in info output, got: {output}"
    );
    // name
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name in info output, got: {output}"
    );
    // state
    assert!(
        output.contains("Active"),
        "Expected state 'Active' in info output, got: {output}"
    );
    // multicast ip
    assert!(
        output.contains(&group.multicast_ip.to_string()),
        "Expected multicast ip in info output, got: {output}"
    );
    // vni (field exists but VNI is internal, not exposed in the external API)
    assert!(
        output.contains("vni:"),
        "Expected vni field in info output, got: {output}"
    );
    // ip pool
    assert!(
        output.contains("test-mcast-pool"),
        "Expected pool name in info output, got: {output}"
    );
    // members section
    assert!(
        output.contains("MEMBERS"),
        "Expected 'MEMBERS' section in info output, got: {output}"
    );
    // member instance name
    assert!(
        output.contains("test-instance"),
        "Expected instance name in info members, got: {output}"
    );
    // member sled ("-" when not started)
    // Note: info shows sled serial, not sled_id UUID
    assert!(
        output.contains("-"),
        "Expected sled '-' for non-started instance in info members, got: {output}"
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

    // Test SSM (Source-Specific Multicast) - group in 232/8 range
    // SSM range is 232.0.0.0/8 for IPv4, ff3x::/32 for IPv6
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

    let ssm_join_url = format!(
        "/v1/instances/{}/multicast-groups/ssm-group?project={PROJECT_NAME}",
        ssm_instance.identity.id
    );
    put_upsert::<_, MulticastGroupMember>(
        client,
        &ssm_join_url,
        &InstanceMulticastGroupJoin {
            source_ips: Some(vec![
                "10.0.0.1".parse::<IpAddr>().unwrap(),
                "10.0.0.2".parse::<IpAddr>().unwrap(),
            ]),
        },
    )
    .await;

    wait_for_group_active(client, "ssm-group").await;

    // Verify SSM group shows in groups list with sources
    let output = run_omdb(&db_url, &["db", "multicast", "groups"]);
    assert!(
        output.contains("ssm-group"),
        "Expected SSM group in output, got: {output}"
    );
    // Verify SSM is shown in RANGE column (232.x.x.x = SSM range)
    assert!(
        output.contains("SSM"),
        "Expected SSM in range column, got: {output}"
    );
    // Verify ASM is shown for 224.x.x.x range (test-mcast-group)
    assert!(
        output.contains("ASM"),
        "Expected ASM in range column, got: {output}"
    );
    // Verify SSM source IPs
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

    // Test: omdb db multicast members shows sources per member
    let output = run_omdb(&db_url, &["db", "multicast", "members"]);
    // SSM member should show its sources
    assert!(
        output.contains("10.0.0.1") || output.contains("10.0.0.2"),
        "Expected SSM member sources in members output, got: {output}"
    );

    // Test: omdb db multicast members --source-ip
    // Filter by SSM source IP - should find SSM member
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--source-ip", "10.0.0.1"],
    );
    assert!(
        output.contains(&ssm_instance.identity.id.to_string()),
        "Expected SSM instance with source-ip filter, got: {output}"
    );
    // Members without sources should not appear for any source-ip filter
    assert!(
        !output.contains(&instance.identity.id.to_string()),
        "Member without sources should not appear, got: {output}"
    );

    // Test: --source-ip with non-existent IP returns no members
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--source-ip", "10.99.99.99"],
    );
    assert!(
        !output.contains(&ssm_instance.identity.id.to_string()),
        "No members should match non-existent source IP, got: {output}"
    );
}
