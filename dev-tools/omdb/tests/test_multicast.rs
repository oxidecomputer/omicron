// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db multicast` integration tests.
//!
//! These tests run omdb against records created through the external API
//! or `DataStore` methods. Deleted records are tested without a reconciler
//! pass.

use std::net::IpAddr;
use std::time::Duration;

use dropshot::test_util::ClientTestContext;
use futures::future::join3;
use http::{Method, StatusCode};
use nexus_db_model::MemberParentRef;
use nexus_db_queries::db::fixed_data::silo::DEFAULT_SILO;
use nexus_db_queries::db::pub_test_utils::TestDatabase;
use nexus_db_queries::db::pub_test_utils::helpers::create_stopped_instance_record;
use nexus_db_queries::db::pub_test_utils::multicast;
use nexus_test_utils::background::activate_background_task;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::{
    create_default_ip_pools, create_instance_with, create_multicast_ip_pool,
    create_project, link_ip_pool, object_delete, objects_list_page_authz,
};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::instance::Instance;
use nexus_types::external_api::instance::InstanceNetworkInterfaceAttachment;
use nexus_types::external_api::multicast::{
    InstanceMulticastGroupJoin, MulticastGroup, MulticastGroupMember,
};
use nexus_types::identity::Resource;
use omicron_common::address::{IpRange, Ipv4Range};
use omicron_test_utils::dev;
use omicron_test_utils::dev::poll::{self, CondCheckError, wait_for_condition};
use omicron_test_utils::dev::test_cmds::{
    EXIT_FAILURE, EXIT_SUCCESS, assert_exit_code, path_to_executable,
    run_command,
};
use omicron_uuid_kinds::{GenericUuid, MulticastGroupUuid};
use subprocess::Exec;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

const PROJECT_NAME: &str = "omdb-test-project";

/// Path to the omdb binary built by Cargo.
const CMD_OMDB: &str = env!("CARGO_BIN_EXE_omdb");

const POLL_INTERVAL: Duration = Duration::from_millis(80);
const MULTICAST_OPERATION_TIMEOUT: Duration = Duration::from_secs(120);

/// Run an omdb command and return its stdout.
async fn run_omdb(db_url: &str, args: &[&str]) -> String {
    run_omdb_expect_exit(db_url, args, EXIT_SUCCESS).await.0
}

async fn run_omdb_expect_exit(
    db_url: &str,
    args: &[&str],
    expected_exit: u32,
) -> (String, String) {
    let db_url = db_url.to_owned();
    let args: Vec<_> = args.iter().map(|arg| arg.to_string()).collect();
    let (status, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_command(
            Exec::cmd(path_to_executable(CMD_OMDB))
                .env("OMDB_DB_URL", db_url)
                .env("RUST_BACKTRACE", "1")
                .env("RUST_LIB_BACKTRACE", "0")
                .args(&args),
        )
    })
    .await
    .unwrap();
    assert_exit_code(status, expected_exit, &stderr);
    (stdout, stderr)
}

/// Build URL for a specific multicast group by name.
fn mcast_group_url(group_name: &str) -> String {
    format!("/v1/multicast-groups/{group_name}")
}

/// Create a multicast IP pool and link it to the default silo.
async fn create_multicast_pool_linked(
    client: &ClientTestContext,
    pool_name: &str,
    ip_range: Option<IpRange>,
) {
    create_multicast_ip_pool(client, pool_name, ip_range).await;
    link_ip_pool(
        client,
        pool_name,
        &DEFAULT_SILO.id().into_untyped_uuid(),
        false,
    )
    .await;
}

async fn put_upsert<InputType, OutputType>(
    client: &ClientTestContext,
    path: &str,
    input: &InputType,
) -> OutputType
where
    InputType: serde::Serialize,
    OutputType: serde::de::DeserializeOwned,
{
    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, path)
            .body(Some(input))
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap_or_else(|e| panic!("failed to make PUT request to {path}: {e}"))
    .parsed_body()
    .unwrap()
}

/// Wait for a group to become "Active".
async fn wait_for_group_active(
    client: &ClientTestContext,
    group_name: &str,
) -> MulticastGroup {
    match wait_for_condition(
        || async {
            let group: MulticastGroup =
                NexusRequest::object_get(client, &mcast_group_url(group_name))
                    .authn_as(AuthnMode::PrivilegedUser)
                    .execute_and_parse_unwrap()
                    .await;
            if group.state == "Active" {
                Ok(group)
            } else {
                Err(CondCheckError::<()>::NotYet { status: None })
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    {
        Ok(group) => group,
        Err(poll::Error::TimedOut { elapsed, .. }) => {
            panic!(
                "group {group_name} did not reach state 'Active' within {elapsed:?}"
            );
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!(
                "failed waiting for group {group_name} to reach 'Active': {err:?}"
            );
        }
    }
}

/// Wait for a specific member to reach the expected state.
async fn wait_for_member_state(
    cptestctx: &ControlPlaneTestContext,
    group_name: &str,
    instance_id: uuid::Uuid,
    expected_state: nexus_db_model::MulticastGroupMemberState,
) -> MulticastGroupMember {
    let client = &cptestctx.external_client;
    let expected_state_str = expected_state.to_string();

    match wait_for_condition(
        || async {
            activate_background_task(
                &cptestctx.lockstep_client,
                "multicast_reconciler",
            )
            .await;
            let url = format!("/v1/multicast-groups/{group_name}/members");
            let members =
                objects_list_page_authz::<MulticastGroupMember>(client, &url)
                    .await
                    .items;
            if let Some(member) =
                members.iter().find(|m| m.parent_id == instance_id)
            {
                if member.state == expected_state_str {
                    Ok(member.clone())
                } else {
                    Err(CondCheckError::<()>::NotYet { status: None })
                }
            } else {
                Err(CondCheckError::<()>::NotYet { status: None })
            }
        },
        &POLL_INTERVAL,
        &MULTICAST_OPERATION_TIMEOUT,
    )
    .await
    {
        Ok(member) => member,
        Err(poll::Error::TimedOut { elapsed, .. }) => {
            panic!(
                "member {instance_id} in group {group_name} did not reach state '{expected_state_str}' within {elapsed:?}"
            );
        }
        Err(poll::Error::PermanentError(err)) => {
            panic!(
                "failed waiting for member {instance_id} in group {group_name}: {err:?}"
            );
        }
    }
}

/// Create an instance for multicast testing.
async fn create_test_instance(
    client: &ClientTestContext,
    project_name: &str,
    instance_name: &str,
    start: bool,
) -> Instance {
    create_instance_with(
        client,
        project_name,
        instance_name,
        &InstanceNetworkInterfaceAttachment::DefaultIpv4,
        vec![],
        vec![],
        start,
        None,
        None,
        vec![],
    )
    .await
}

/// Test omdb multicast pools command.
#[nexus_test]
async fn test_omdb_multicast_pools(cptestctx: &ControlPlaneTestContext) {
    let db_url = cptestctx.database.listen_url().to_string();
    let client = &cptestctx.external_client;

    // Before creating any pools, this should show "no multicast IP pools found"
    let output = run_omdb(&db_url, &["db", "multicast", "pools"]).await;
    assert!(
        output.contains("no multicast IP pools found"),
        "Expected empty pool message, received: {output}"
    );

    // Create a multicast pool (no silo linking needed for pools-only test)
    create_multicast_ip_pool(client, "test-mcast-pool", None).await;

    let output = run_omdb(&db_url, &["db", "multicast", "pools"]).await;
    assert!(
        output.contains("test-mcast-pool"),
        "Expected pool name in output, received: {output}"
    );
    // Default range from test-utils: 224.1.0.0 - 224.1.255.255.
    assert!(
        output.contains("224.1.0.0"),
        "Expected first address in output, received: {output}"
    );
    assert!(
        output.contains("224.1.255.255"),
        "Expected last address in output, received: {output}"
    );
}

/// Test deleted group and member queries without a running reconciler.
#[tokio::test]
async fn test_omdb_multicast_deleted_records() {
    let logctx = dev::test_setup_log("test_omdb_multicast_deleted_records");
    let db = TestDatabase::new_with_datastore(&logctx.log).await;
    let db_url = db.crdb().listen_url().to_string();
    let (opctx, datastore) = (db.opctx(), db.datastore());
    let setup = multicast::create_test_setup(
        opctx,
        datastore,
        "test-mcast-pool",
        PROJECT_NAME,
    )
    .await;
    let live_group = multicast::create_test_group(
        opctx,
        datastore,
        "live-group",
        "224.10.1.1",
    )
    .await;
    let del_group = multicast::create_test_group(
        opctx,
        datastore,
        "deleted-group",
        "224.10.1.2",
    )
    .await;

    let live_group_id = MulticastGroupUuid::from_untyped_uuid(live_group.id());
    let del_group_id = MulticastGroupUuid::from_untyped_uuid(del_group.id());
    let live_instance = create_stopped_instance_record(
        opctx,
        datastore,
        &setup.authz_project,
        "live-instance",
    )
    .await;
    let del_instance = create_stopped_instance_record(
        opctx,
        datastore,
        &setup.authz_project,
        "deleted-instance",
    )
    .await;
    let sources = ["10.0.0.1".parse().unwrap()];
    let live_member = datastore
        .multicast_group_member_attach(
            opctx,
            live_group_id,
            MemberParentRef::Instance(live_instance),
            Some(&sources),
        )
        .await
        .unwrap()
        .member;
    let del_member = datastore
        .multicast_group_member_attach(
            opctx,
            live_group_id,
            MemberParentRef::Instance(del_instance),
            None,
        )
        .await
        .unwrap()
        .member;
    let del_group_member = datastore
        .multicast_group_member_attach(
            opctx,
            del_group_id,
            MemberParentRef::Instance(del_instance),
            None,
        )
        .await
        .unwrap()
        .member;
    for mem_id in [del_member.id, del_group_member.id] {
        datastore
            .multicast_group_member_delete_by_id(opctx, mem_id)
            .await
            .unwrap();
    }
    assert!(
        datastore
            .multicast_group_mark_removal_if_empty(opctx, del_group_id,)
            .await
            .unwrap()
    );

    let live_group_id = live_group_id.to_string();
    let del_group_id = del_group_id.to_string();
    let live_mem_id = live_member.id.to_string();
    let del_mem_id = del_member.id.to_string();
    let del_group_mem_id = del_group_member.id.to_string();

    for include_deleted in [false, true] {
        let mut args = vec!["db", "multicast", "groups"];
        if include_deleted {
            args.push("--include-deleted");
        }
        let output = run_omdb(&db_url, &args).await;
        let live_row = output
            .lines()
            .find(|line| line.contains(&live_group_id))
            .unwrap_or_else(|| panic!("live group missing: {output}"));
        assert!(
            live_row.split_whitespace().any(|cell| cell == "10.0.0.1"),
            "received: {output}"
        );
        assert_eq!(
            live_row.contains("deleted-instance@- (deleted)"),
            include_deleted,
            "received: {output}"
        );
        assert_eq!(
            output.contains(&del_group_id),
            include_deleted,
            "received: {output}"
        );

        if include_deleted {
            assert_eq!(
                live_row.split_whitespace().last(),
                Some("-"),
                "received: {output}"
            );
            let del_row = output
                .lines()
                .find(|line| line.contains(&del_group_id))
                .unwrap_or_else(|| panic!("deleted group missing: {output}"));
            assert_ne!(
                del_row.split_whitespace().last(),
                Some("-"),
                "received: {output}"
            );
        }

        let mut args = vec!["db", "multicast", "members"];
        if include_deleted {
            args.push("--include-deleted");
        }
        let output = run_omdb(&db_url, &args).await;
        assert!(output.contains(&live_mem_id), "received: {output}");
        if include_deleted {
            let live_row = output
                .lines()
                .find(|line| line.contains(&live_mem_id))
                .unwrap_or_else(|| panic!("live member missing: {output}"));
            assert_eq!(
                live_row.split_whitespace().last(),
                Some("-"),
                "received: {output}"
            );
        }
        for mem_id in [&del_mem_id, &del_group_mem_id] {
            assert_eq!(
                output.contains(mem_id),
                include_deleted,
                "received: {output}"
            );
            if include_deleted {
                let del_row = output
                    .lines()
                    .find(|line| line.contains(mem_id.as_str()))
                    .unwrap_or_else(|| panic!("member missing: {output}"));
                assert_ne!(
                    del_row.split_whitespace().last(),
                    Some("-"),
                    "received: {output}"
                );
            }
        }

        let mut args = vec!["db", "multicast", "info", "--name", "live-group"];
        if include_deleted {
            args.push("--include-deleted");
        }
        let output = run_omdb(&db_url, &args).await;
        assert!(output.contains(&live_mem_id), "received: {output}");
        assert_eq!(
            output.contains(&del_mem_id),
            include_deleted,
            "received: {output}"
        );
        assert!(
            output.lines().any(|line| line.trim() == "source_ips: 10.0.0.1"),
            "received: {output}"
        );
    }

    let output = run_omdb(
        &db_url,
        &["db", "multicast", "groups", "--state", "deleting"],
    )
    .await;
    assert!(output.contains(&del_group_id), "received: {output}");
    assert!(!output.contains(&live_group_id), "received: {output}");

    for (info_selector, member_selector, value) in [
        ("--name", "--group-name", "deleted-group"),
        ("--ip", "--group-ip", "224.10.1.2"),
        ("--group-id", "--group-id", del_group_id.as_str()),
    ] {
        let (_, stderr) = run_omdb_expect_exit(
            &db_url,
            &["db", "multicast", "info", info_selector, value],
            EXIT_FAILURE,
        )
        .await;
        assert!(
            stderr.contains("no multicast group found")
                && stderr.contains("--include-deleted"),
            "received: {stderr}"
        );

        let output = run_omdb(
            &db_url,
            &[
                "db",
                "multicast",
                "info",
                "--include-deleted",
                info_selector,
                value,
            ],
        )
        .await;
        assert!(output.contains(&del_group_id), "received: {output}");
        assert!(output.contains(&del_group_mem_id), "received: {output}");
        assert!(output.contains("Deleting"), "received: {output}");
        assert!(
            output
                .lines()
                .any(|line| line.trim_start().starts_with("deleted:")),
            "received: {output}"
        );

        let output = run_omdb(
            &db_url,
            &[
                "db",
                "multicast",
                "members",
                "--include-deleted",
                member_selector,
                value,
            ],
        )
        .await;
        assert!(output.contains(&del_group_mem_id), "received: {output}");
        assert!(!output.contains(&live_mem_id), "received: {output}");
        assert!(!output.contains(&del_mem_id), "received: {output}");
    }

    let (_, stderr) = run_omdb_expect_exit(
        &db_url,
        &["db", "multicast", "info", "--include-deleted", "--name", "nope"],
        EXIT_FAILURE,
    )
    .await;
    assert!(stderr.contains("no multicast group found"), "received: {stderr}");
    assert!(!stderr.contains("use --include-deleted"), "received: {stderr}");

    db.terminate().await;
    logctx.cleanup_successful();
}

/// Test omdb multicast commands.
#[nexus_test]
async fn test_omdb_multicast_commands(cptestctx: &ControlPlaneTestContext) {
    let db_url = cptestctx.database.listen_url().to_string();
    let client = &cptestctx.external_client;

    // Setup: create pools and project
    join3(
        create_default_ip_pools(client),
        create_project(client, PROJECT_NAME),
        create_multicast_pool_linked(client, "test-mcast-pool", None),
    )
    .await;

    // Create an instance without multicast groups first
    let instance = create_test_instance(
        client,
        PROJECT_NAME,
        "test-instance",
        false, // don't start
    )
    .await;
    let instance_id = instance.identity.id.to_string();

    // Add a multicast member via API (this implicitly creates the group)
    // Use the instance-centric join endpoint:
    // PUT /v1/instances/{instance}/multicast-groups/{group}
    let join_url = format!(
        "/v1/instances/{}/multicast-groups/test-mcast-group?project={PROJECT_NAME}",
        instance.identity.id
    );

    put_upsert::<_, MulticastGroupMember>(
        client,
        &join_url,
        &InstanceMulticastGroupJoin {
            source_ips: None, // ASM (Any-Source Multicast)
            ip_version: None,
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
    let output = run_omdb(&db_url, &["db", "multicast", "groups"]).await;
    assert!(
        output.contains(&group.identity.id.to_string()),
        "Expected group id in output, received: {output}"
    );
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name in output, received: {output}"
    );
    assert!(
        output.contains("Active"),
        "Expected state 'Active' in output, received: {output}"
    );
    assert!(
        output.contains(&group.multicast_ip.to_string()),
        "Expected multicast ip in output, received: {output}"
    );
    // range (ASM for 224.x.x.x)
    assert!(
        output.contains("ASM"),
        "Expected range 'ASM' in output, received: {output}"
    );
    // vni (column exists but VNI is internal, not exposed in the external API)
    assert!(
        output.contains("VNI"),
        "Expected VNI column in output, received: {output}"
    );
    // members (parent@sled format, "-" when not started)
    assert!(
        output.contains("test-instance@-"),
        "Expected member 'test-instance@-' in output, received: {output}"
    );

    // Test: omdb db multicast groups --state active
    let output =
        run_omdb(&db_url, &["db", "multicast", "groups", "--state", "active"])
            .await;
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name with state filter, received: {output}"
    );

    // Test: omdb db multicast groups --pool
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "groups", "--pool", "test-mcast-pool"],
    )
    .await;
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name with pool filter, received: {output}"
    );

    // Test: omdb db multicast members
    let output = run_omdb(&db_url, &["db", "multicast", "members"]).await;
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name in members output, received: {output}"
    );
    assert!(
        output.contains("PARENT_KIND"),
        "Expected PARENT_KIND column in members output, received: {output}"
    );
    // parent id (instance id) with parent kind "instance" on the same row
    assert!(
        output.contains(&instance_id),
        "Expected parent id in members output, received: {output}"
    );
    let has_instance_kind = output
        .lines()
        .any(|line| line.contains(&instance_id) && line.contains(" instance "));
    assert!(
        has_instance_kind,
        "Expected parent kind 'instance' for instance member, received: {output}"
    );
    let group_ip = group.multicast_ip.to_string();
    assert!(
        output.contains(&group_ip),
        "Expected multicast ip in members output, received: {output}"
    );
    // sources ("-" = any-source member)
    let has_any_source = output
        .lines()
        .any(|line| line.contains(&instance_id) && line.contains(" - "));
    assert!(
        has_any_source,
        "Expected '-' for any-source member, received: {output}"
    );

    // Test: omdb db multicast members --parent-kind
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--parent-kind", "instance"],
    )
    .await;
    assert!(
        output.contains(&instance_id),
        "Expected instance ID with parent-kind=instance filter, received: {output}"
    );
    // There are no probe members.
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--parent-kind", "probe"],
    )
    .await;
    assert!(
        !output.contains(&instance_id),
        "Instance member should not appear with parent-kind=probe filter, received: {output}"
    );

    // Test: omdb db multicast members --group-name
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--group-name", "test-mcast-group"],
    )
    .await;
    assert!(
        output.contains(&instance_id),
        "Expected instance ID with group-name filter, received: {output}"
    );

    // Test: omdb db multicast members --group-ip
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--group-ip", &group_ip],
    )
    .await;
    assert!(
        output.contains(&instance_id),
        "Expected instance ID with group-ip filter, received: {output}"
    );

    // Test: omdb db multicast members --group-id
    let group_id = group.identity.id.to_string();
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--group-id", &group_id],
    )
    .await;
    assert!(
        output.contains(&instance_id),
        "Expected instance ID with group-id filter, received: {output}"
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
        run_omdb(&db_url, &["db", "multicast", "members", "--state", "left"])
            .await;
    assert!(
        output.contains(&instance_id),
        "Expected instance ID with state=left filter, received: {output}"
    );

    // Test: omdb db multicast members --sled-id
    // Create a started instance so the member gets a sled_id
    let started_instance = create_test_instance(
        client,
        PROJECT_NAME,
        "started-instance",
        true, // start the instance
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
        &InstanceMulticastGroupJoin { source_ips: None, ip_version: None },
    )
    .await;

    wait_for_group_active(client, "sled-test-group").await;

    // Query members by sled_id - the started instance should be on first_sled
    let sled_id = cptestctx.first_sled_id().to_string();
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--sled-id", &sled_id],
    )
    .await;
    assert!(
        output.contains(&started_instance.identity.id.to_string()),
        "Expected started instance ID with sled-id filter, received: {output}"
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

    let output_joined =
        run_omdb(&db_url, &["db", "multicast", "members", "--state", "joined"])
            .await;
    assert!(
        output_joined.contains(&started_instance.identity.id.to_string()),
        "Expected started instance in 'Joined' state, received: {output_joined}"
    );
    assert!(
        output_joined.contains("Joined"),
        "Expected 'Joined' state in members output, received: {output_joined}"
    );
    assert!(
        output_joined.contains(&sled_id),
        "Expected sled_id in members output, received: {output_joined}"
    );

    // Verify info for started instance shows sled serial (not "-")
    let output_info = run_omdb(
        &db_url,
        &["db", "multicast", "info", "--name", "sled-test-group"],
    )
    .await;
    assert!(
        output_info.contains("started-instance"),
        "Expected 'started-instance' in info members, received: {output_info}"
    );
    // underlay group should be present for active group
    assert!(
        output_info.contains("UNDERLAY GROUP"),
        "Expected 'UNDERLAY GROUP' section in info output, received: {output_info}"
    );

    // Verify groups output shows started instance with sled serial (not "-")
    let output_groups = run_omdb(&db_url, &["db", "multicast", "groups"]).await;
    // sled-test-group should show "started-instance@<serial>", not
    // "started-instance@-"
    assert!(
        output_groups.contains("started-instance@"),
        "Expected 'started-instance@' in groups members column, received: {output_groups}"
    );
    assert!(
        !output_groups.contains("started-instance@-"),
        "Started instance should have sled serial, not '-', received: {output_groups}"
    );

    // Active groups with joined members use the ff04::/64 underlay subnet.
    assert!(
        output_groups.contains("ff04:"),
        "Expected underlay_ip (ff04:*) for active group in groups output, received: {output_groups}"
    );

    // Verify started instance is not in "Left" state
    let output_left =
        run_omdb(&db_url, &["db", "multicast", "members", "--state", "left"])
            .await;
    assert!(
        !output_left.contains(&started_instance.identity.id.to_string()),
        "Started instance should not be in 'Left' state, received: {output_left}"
    );

    // Test: combined filters (--group-name + --state)
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
    )
    .await;
    assert!(
        output_combined.contains(&started_instance.identity.id.to_string()),
        "Expected started instance with combined filters, received: {output_combined}"
    );

    // test-instance is in test-mcast-group but is in the "Left" state, meaning
    // that `--state` joined must keep it hidden
    let output = run_omdb(
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
    )
    .await;
    assert!(
        !output.contains(&instance_id),
        "'Left' member should not match --state joined, received: {output}"
    );

    // Each group filter should show only its own group's member.
    let started_id = started_instance.identity.id.to_string();

    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--group-name", "sled-test-group"],
    )
    .await;
    assert!(output.contains(&started_id), "received: {output}");
    assert!(!output.contains(&instance_id), "received: {output}");

    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--group-ip", &group_ip],
    )
    .await;
    assert!(output.contains(&instance_id), "received: {output}");
    assert!(!output.contains(&started_id), "received: {output}");

    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--group-id", &group_id],
    )
    .await;
    assert!(output.contains(&instance_id), "received: {output}");
    assert!(!output.contains(&started_id), "received: {output}");

    // Both groups are "Active", so --state creating should show neither.
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "groups", "--state", "creating"],
    )
    .await;
    assert!(!output.contains("test-mcast-group"), "received: {output}");
    assert!(!output.contains("sled-test-group"), "received: {output}");

    // Test: omdb db multicast info --name
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "info", "--name", "test-mcast-group"],
    )
    .await;
    assert!(
        output.contains("MULTICAST GROUP"),
        "Expected 'MULTICAST GROUP' header in info output, received: {output}"
    );
    assert!(
        output.contains(&group.identity.id.to_string()),
        "Expected group id in info output, received: {output}"
    );
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name in info output, received: {output}"
    );
    assert!(
        output.contains("Active"),
        "Expected state 'Active' in info output, received: {output}"
    );
    assert!(
        output.contains(&group.multicast_ip.to_string()),
        "Expected multicast ip in info output, received: {output}"
    );
    // vni (field exists but VNI is internal, not exposed in the external API)
    assert!(
        output.contains("vni:"),
        "Expected vni field in info output, received: {output}"
    );
    assert!(
        output.contains("test-mcast-pool"),
        "Expected pool name in info output, received: {output}"
    );
    assert!(
        output.contains("MEMBERS"),
        "Expected 'MEMBERS' section in info output, received: {output}"
    );
    assert!(
        output.contains("PARENT_KIND") && output.contains("PARENT "),
        "Expected PARENT_KIND and PARENT columns in info members, received: {output}"
    );
    // member parent name, with parent kind "instance" on the same row
    assert!(
        output.contains("test-instance"),
        "Expected instance name in info members, received: {output}"
    );
    let has_instance_kind = output.lines().any(|line| {
        line.contains("test-instance") && line.contains(" instance ")
    });
    assert!(
        has_instance_kind,
        "Expected parent kind 'instance' on same line as instance name, received: {output}"
    );
    // Match the member row; "-" also occurs in UUIDs.
    let has_sled_dash = output
        .lines()
        .any(|line| line.contains("test-instance") && line.contains(" - "));
    assert!(
        has_sled_dash,
        "Expected sled '-' for non-started instance on same line as instance name, received: {output}"
    );

    // Test: omdb db multicast info --ip
    let output =
        run_omdb(&db_url, &["db", "multicast", "info", "--ip", &group_ip])
            .await;
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name when querying by IP, received: {output}"
    );

    // Test: omdb db multicast info --group-id
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "info", "--group-id", &group_id],
    )
    .await;
    assert!(
        output.contains("test-mcast-group"),
        "Expected group name when querying by ID, received: {output}"
    );

    // Use an IPv4 pool in the SSM range (232.0.0.0/8).
    let ssm_range = IpRange::V4(
        Ipv4Range::new(
            std::net::Ipv4Addr::new(232, 1, 0, 0),
            std::net::Ipv4Addr::new(232, 1, 0, 255),
        )
        .unwrap(),
    );
    create_multicast_pool_linked(client, "test-ssm-pool", Some(ssm_range))
        .await;

    let ssm_instance =
        create_test_instance(client, PROJECT_NAME, "ssm-instance", false).await;

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
            ip_version: None,
        },
    )
    .await;

    wait_for_group_active(client, "ssm-group").await;

    // Verify SSM group shows in groups list with sources
    let output = run_omdb(&db_url, &["db", "multicast", "groups"]).await;
    assert!(
        output.contains("ssm-group"),
        "Expected SSM group in output, received: {output}"
    );
    // Verify SSM is shown in RANGE column (232.x.x.x = SSM range)
    assert!(
        output.contains("SSM"),
        "Expected SSM in range column, received: {output}"
    );
    // Verify ASM is shown for 224.x.x.x range (test-mcast-group)
    assert!(
        output.contains("ASM"),
        "Expected ASM in range column, received: {output}"
    );
    assert!(
        output.contains("10.0.0.1") && output.contains("10.0.0.2"),
        "Expected SSM source IPs in output, received: {output}"
    );

    // Verify SSM sources show in info command
    let output =
        run_omdb(&db_url, &["db", "multicast", "info", "--name", "ssm-group"])
            .await;
    assert!(
        output.contains("10.0.0.1") && output.contains("10.0.0.2"),
        "Expected SSM source IPs in info output, received: {output}"
    );

    // Test: omdb db multicast members shows sources per member
    let output = run_omdb(&db_url, &["db", "multicast", "members"]).await;
    assert!(
        output.contains("10.0.0.1") && output.contains("10.0.0.2"),
        "Expected SSM member sources in members output, received: {output}"
    );

    // Test: omdb db multicast members --source-ip
    // Filter by SSM source IP - should find SSM member
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--source-ip", "10.0.0.1"],
    )
    .await;
    assert!(
        output.contains(&ssm_instance.identity.id.to_string()),
        "Expected SSM instance with source-ip filter, received: {output}"
    );
    // Members without sources should not appear for any source-ip filter
    assert!(
        !output.contains(&instance_id),
        "Member without sources should not appear, received: {output}"
    );

    // Test: --source-ip with non-existent IP returns no members
    let output = run_omdb(
        &db_url,
        &["db", "multicast", "members", "--source-ip", "10.99.99.99"],
    )
    .await;
    assert!(
        !output.contains(&ssm_instance.identity.id.to_string()),
        "No members should match non-existent source IP, received: {output}"
    );

    for (command, expected) in [
        ("groups", "ssm-group"),
        ("members", "ssm-group"),
        ("pools", "test-ssm-pool"),
    ] {
        let output = run_omdb(
            &db_url,
            &["db", "multicast", command, "--include-deleted"],
        )
        .await;
        assert!(output.contains(expected), "received: {output}");
    }

    for selector in [
        ["--name", "test-mcast-group"],
        ["--ip", group_ip.as_str()],
        ["--group-id", group_id.as_str()],
    ] {
        let output = run_omdb(
            &db_url,
            &[
                "db",
                "multicast",
                "info",
                "--include-deleted",
                selector[0],
                selector[1],
            ],
        )
        .await;
        assert!(output.contains(&group_id), "received: {output}");
    }

    object_delete(client, &ssm_join_url).await;
    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::POST,
            "/v1/system/ip-pools/test-ssm-pool/ranges/remove",
        )
        .body(Some(&ssm_range))
        .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    let ssm_instance_id = ssm_instance.identity.id.to_string();
    let output = run_omdb(&db_url, &["db", "multicast", "members"]).await;
    assert!(!output.contains(&ssm_instance_id), "received: {output}");

    let output = run_omdb(&db_url, &["db", "multicast", "pools"]).await;
    let pool_line = output
        .lines()
        .find(|line| line.contains("test-ssm-pool"))
        .unwrap_or_else(|| panic!("pool without ranges missing: {output}"));
    let cols: Vec<&str> = pool_line.split_whitespace().collect();
    assert_eq!(cols[2..4], ["-", "-"], "received: {output}");

    object_delete(client, "/v1/system/ip-pools/test-ssm-pool").await;

    let output = run_omdb(&db_url, &["db", "multicast", "pools"]).await;
    assert!(!output.contains("test-ssm-pool"), "received: {output}");

    let output =
        run_omdb(&db_url, &["db", "multicast", "pools", "--include-deleted"])
            .await;
    assert!(output.contains("TIME_DELETED"), "received: {output}");
    let del_range_line = output
        .lines()
        .find(|line| line.contains("test-ssm-pool"))
        .unwrap_or_else(|| panic!("deleted pool missing: {output}"));
    let cols: Vec<&str> = del_range_line.split_whitespace().collect();
    assert_eq!(cols[2..4], ["232.1.0.0", "232.1.0.255"], "received: {output}");
    assert_ne!(cols[5], "-", "received: {output}");

    create_multicast_pool_linked(client, "test-ssm-pool", Some(ssm_range))
        .await;
    let replacement_join_url = format!(
        "/v1/instances/{}/multicast-groups/replacement-group?project={PROJECT_NAME}",
        ssm_instance.identity.id
    );
    put_upsert::<_, MulticastGroupMember>(
        client,
        &replacement_join_url,
        &InstanceMulticastGroupJoin {
            source_ips: Some(vec!["10.0.0.1".parse().unwrap()]),
            ip_version: None,
        },
    )
    .await;

    let output = run_omdb(
        &db_url,
        &["db", "multicast", "groups", "--pool", "test-ssm-pool"],
    )
    .await;
    assert!(output.contains("replacement-group"), "received: {output}");
    assert!(!output.contains("test-mcast-group"), "received: {output}");

    let output = run_omdb(
        &db_url,
        &["db", "multicast", "groups", "--pool", "test-mcast-pool"],
    )
    .await;
    assert!(output.contains("test-mcast-group"), "received: {output}");
    assert!(!output.contains("replacement-group"), "received: {output}");

    let second_sled_join_url = format!(
        "/v1/instances/{}/multicast-groups/sled-test-group?project={PROJECT_NAME}",
        instance.identity.id
    );
    put_upsert::<_, MulticastGroupMember>(
        client,
        &second_sled_join_url,
        &InstanceMulticastGroupJoin { source_ips: None, ip_version: None },
    )
    .await;

    let output = run_omdb(
        &db_url,
        &[
            "db",
            "multicast",
            "groups",
            "--pool",
            "test-mcast-pool",
            "--fetch-limit",
            "2",
        ],
    )
    .await;
    assert!(output.contains("test-mcast-group"), "received: {output}");
    assert!(output.contains("sled-test-group"), "received: {output}");
    let group_lines = |output: &str| {
        output
            .lines()
            .filter(|line| {
                line.contains("test-mcast-group")
                    || line.contains("sled-test-group")
            })
            .map(String::from)
            .collect::<Vec<_>>()
    };
    assert!(
        group_lines(&output).iter().any(|line| line.contains("...")),
        "received: {output}"
    );

    let output = run_omdb(
        &db_url,
        &[
            "db",
            "multicast",
            "groups",
            "--pool",
            "test-mcast-pool",
            "--fetch-limit",
            "3",
        ],
    )
    .await;
    assert_eq!(group_lines(&output).len(), 2, "received: {output}");
    assert!(!output.contains("..."), "received: {output}");

    let asm_ip = "224.1.5.5";
    let asm_join_url = |inst: &Instance| {
        format!(
            "/v1/instances/{}/multicast-groups/{asm_ip}?project={PROJECT_NAME}",
            inst.identity.id
        )
    };
    let asm_a =
        create_test_instance(client, PROJECT_NAME, "asm-src-a", false).await;
    let asm_b =
        create_test_instance(client, PROJECT_NAME, "asm-src-b", false).await;
    for (inst, source) in [(&asm_a, "10.1.0.1"), (&asm_b, "10.1.0.2")] {
        put_upsert::<_, MulticastGroupMember>(
            client,
            &asm_join_url(inst),
            &InstanceMulticastGroupJoin {
                source_ips: Some(vec![source.parse().unwrap()]),
                ip_version: None,
            },
        )
        .await;
    }
    wait_for_group_active(client, asm_ip).await;

    let asm_group_cols = |output: &str| {
        output
            .lines()
            .find(|line| line.contains(asm_ip))
            .unwrap_or_else(|| panic!("ASM group missing: {output}"))
            .split_whitespace()
            .map(String::from)
            .collect::<Vec<_>>()
    };
    let output = run_omdb(&db_url, &["db", "multicast", "groups"]).await;
    let cols = asm_group_cols(&output);
    assert_eq!(cols[4], "ASM", "received: {output}");
    assert!(cols[5].starts_with("ff04:"), "received: {output}");
    assert_eq!(cols[6], "10.1.0.1,10.1.0.2", "received: {output}");

    let output =
        run_omdb(&db_url, &["db", "multicast", "info", "--ip", asm_ip]).await;
    assert!(output.contains("10.1.0.1,10.1.0.2"), "received: {output}");

    let asm_any =
        create_test_instance(client, PROJECT_NAME, "asm-any", false).await;
    put_upsert::<_, MulticastGroupMember>(
        client,
        &asm_join_url(&asm_any),
        &InstanceMulticastGroupJoin { source_ips: None, ip_version: None },
    )
    .await;

    let output = run_omdb(&db_url, &["db", "multicast", "groups"]).await;
    let cols = asm_group_cols(&output);
    assert!(cols[5].starts_with("ff04:"), "received: {output}");
    assert_eq!(cols[6], "-", "received: {output}");
}
