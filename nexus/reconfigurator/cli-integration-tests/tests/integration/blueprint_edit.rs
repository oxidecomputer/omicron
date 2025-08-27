// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use camino::Utf8Path;
use nexus_db_queries::authn;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_test_utils::SLED_AGENT_UUID;
use nexus_test_utils::resource_helpers::DiskTest;
use nexus_test_utils_macros::nexus_test;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::UnstableReconfiguratorState;
use omicron_common::api::external::Error;
use omicron_test_utils::dev::poll::CondCheckError;
use omicron_test_utils::dev::poll::wait_for_condition;
use omicron_test_utils::dev::test_cmds::EXIT_SUCCESS;
use omicron_test_utils::dev::test_cmds::assert_exit_code;
use omicron_test_utils::dev::test_cmds::path_to_executable;
use omicron_test_utils::dev::test_cmds::run_command;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use slog::debug;
use std::io::BufReader;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use subprocess::Exec;
use swrite::SWrite;
use swrite::swriteln;

fn path_to_cli() -> PathBuf {
    path_to_executable(env!("CARGO_BIN_EXE_reconfigurator-cli-dup"))
}

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// Tests a round trip of blueprint editing: start with the blueprint that's
// present in a running system, fetch it with the rest of the reconfigurator
// state, load it into reconfigurator-cli, edit it, save that to a file, then
// import it back.
#[nexus_test]
async fn test_blueprint_edit(cptestctx: &ControlPlaneTestContext) {
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();

    let log = &cptestctx.logctx.log;
    let opctx = OpContext::for_background(
        log.clone(),
        Arc::new(authz::Authz::new(log)),
        authn::Context::internal_api(),
        datastore.clone(),
    );

    // Setup
    //
    // For all the disks our blueprint says each sled should have, actually
    // insert them into the DB. This is working around nexus-test-utils's setup
    // being a little scattershot and spread out; tests are supposed to do their
    // own disk setup.
    let (_blueprint_target, initial_blueprint) = datastore
        .blueprint_target_get_current_full(&opctx)
        .await
        .expect("failed to read current target blueprint");
    let mut disk_test = DiskTest::new(&cptestctx).await;
    disk_test.add_blueprint_disks(&initial_blueprint).await;

    let tmpdir = camino_tempfile::tempdir().expect("failed to create tmpdir");
    // Save the path and prevent the temporary directory from being cleaned up
    // automatically.  We want to be preserve the contents if this test fails.
    let tmpdir_path = tmpdir.keep();
    let saved_state1_path = tmpdir_path.join("reconfigurator-state1.json");
    let saved_state2_path = tmpdir_path.join("reconfigurator-state2.json");
    let script1_path = tmpdir_path.join("cmds1");
    let script2_path = tmpdir_path.join("cmds2");
    let new_blueprint_path = tmpdir_path.join("new_blueprint.json");

    println!("temporary directory: {}", tmpdir_path);

    // Wait until Nexus has successfully completed an inventory collection.
    // We don't need it directly but we want it to be present in the saved
    // reconfigurator state.
    let collection = wait_for_condition(
        || async {
            let result =
                datastore.inventory_get_latest_collection(&opctx).await;
            let log_result = match &result {
                Ok(Some(_)) => Ok("found"),
                Ok(None) => Ok("not found"),
                Err(error) => Err(error),
            };
            debug!(
                log,
                "attempt to fetch latest inventory collection";
                "result" => ?log_result,
            );

            match result {
                Ok(None) => Err(CondCheckError::NotYet),
                Ok(Some(c)) => Ok(c),
                Err(Error::ServiceUnavailable { .. }) => {
                    Err(CondCheckError::NotYet)
                }
                Err(error) => Err(CondCheckError::Failed(error)),
            }
        },
        &Duration::from_millis(50),
        &Duration::from_secs(30),
    )
    .await
    .expect("took too long to find first inventory collection");

    // Assemble state that we can load into reconfigurator-cli.
    let state1 = nexus_reconfigurator_preparation::reconfigurator_state_load(
        &opctx, datastore, None,
    )
    .await
    .expect("failed to assemble reconfigurator state");

    // Smoke check the initial state.
    let sled_id: SledUuid = SLED_AGENT_UUID.parse().unwrap();
    state1
        .planning_input
        .sled_lookup(SledFilter::Commissioned, sled_id)
        .expect("state1 has initial sled");
    assert!(!state1.planning_input.service_ip_pool_ranges().is_empty());
    assert!(!state1.silo_names.is_empty());
    assert!(!state1.external_dns_zone_names.is_empty());
    // We waited for the first inventory collection already.
    assert!(state1.collections.iter().any(|c| c.id == collection.id));
    assert!(!state1.collections.is_empty());
    // Test suite setup establishes the initial blueprint.
    assert!(!state1.blueprints.is_empty());
    // Setup requires that internal and external DNS be configured so we should
    // have at least the current DNS generations here.
    assert!(!state1.internal_dns.is_empty());
    assert!(!state1.external_dns.is_empty());

    // unwrap: we checked above that this list was non-empty.
    let blueprint = state1.blueprints.first().unwrap();

    // Write a reconfigurator-cli script to load the file, edit the
    // blueprint, and save the entire state to a new file.
    let mut s = String::new();
    swriteln!(s, "load {} {}", saved_state1_path, collection.id);
    swriteln!(s, "blueprint-edit {} add-nexus {}", blueprint.id, sled_id);
    swriteln!(s, "save {}", saved_state2_path);
    std::fs::write(&script1_path, &s)
        .with_context(|| format!("write {}", &script1_path))
        .unwrap();

    // Run this reconfigurator-cli invocation.
    write_json(&saved_state1_path, &state1).unwrap();
    let exec = Exec::cmd(path_to_cli()).arg(&script1_path);
    let (exit_status, _, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);

    // Load the new file and find the new blueprint name.
    let state2: UnstableReconfiguratorState =
        read_json(&saved_state2_path).unwrap();
    assert_eq!(state2.blueprints.len(), state1.blueprints.len() + 1);
    let new_blueprint = state2.blueprints.into_iter().rev().next().unwrap();
    assert_ne!(new_blueprint.id, blueprint.id);

    // While we're at it, smoke check the new blueprint.
    assert_eq!(new_blueprint.parent_blueprint_id, Some(blueprint.id));
    assert_eq!(new_blueprint.creator, "reconfigurator-cli");

    // Now run reconfigurator-cli again just to save the new blueprint.  This is
    // a little unfortunate but it's hard to avoid if we want to test that
    // blueprint-save works.
    let mut s = String::new();
    swriteln!(s, "load {} {}", saved_state2_path, collection.id);
    swriteln!(s, "blueprint-save {} {}", new_blueprint.id, new_blueprint_path);
    std::fs::write(&script2_path, &s)
        .with_context(|| format!("write {}", &script2_path))
        .unwrap();
    let exec = Exec::cmd(path_to_cli()).arg(&script2_path);
    let (exit_status, _, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);

    // Load the blueprint we just wrote.
    let new_blueprint2: Blueprint = read_json(&new_blueprint_path).unwrap();
    assert_eq!(new_blueprint, new_blueprint2);

    // Import the new blueprint.
    let nexus_internal_url =
        format!("http://{}/", cptestctx.internal_client.bind_address);
    let nexus_client =
        nexus_client::Client::new(&nexus_internal_url, log.clone());
    nexus_client
        .blueprint_import(&new_blueprint)
        .await
        .expect("failed to import new blueprint");

    let found_blueprint = nexus_client
        .blueprint_view(new_blueprint.id.as_untyped_uuid())
        .await
        .expect("failed to find imported blueprint in Nexus")
        .into_inner();
    assert_eq!(found_blueprint, new_blueprint2);

    // Set the blueprint as the (disabled) target.
    nexus_client
        .blueprint_target_set(&nexus_client::types::BlueprintTargetSet {
            target_id: new_blueprint.id,
            enabled: false,
        })
        .await
        .context("setting target blueprint")
        .unwrap();

    // Read that back.
    let target = nexus_client
        .blueprint_target_view()
        .await
        .context("fetching target blueprint")
        .unwrap();
    assert_eq!(target.target_id, new_blueprint.id);

    // Now clean up the temporary directory.
    for path in [
        saved_state1_path,
        saved_state2_path,
        script1_path,
        script2_path,
        new_blueprint_path,
    ] {
        std::fs::remove_file(&path)
            .with_context(|| format!("remove {}", path))
            .unwrap();
    }

    std::fs::remove_dir(&tmpdir_path)
        .with_context(|| format!("remove {}", tmpdir_path))
        .unwrap();
}

fn read_json<T: for<'a> serde::Deserialize<'a>>(
    path: &Utf8Path,
) -> Result<T, anyhow::Error> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("open {:?}", path))?;
    let bufread = BufReader::new(file);
    serde_json::from_reader(bufread).with_context(|| format!("read {:?}", path))
}

fn write_json<T: serde::Serialize>(
    path: &Utf8Path,
    obj: &T,
) -> Result<(), anyhow::Error> {
    let file = std::fs::File::create(path)
        .with_context(|| format!("create {:?}", path))?;
    let bufwrite = BufWriter::new(file);
    serde_json::to_writer_pretty(bufwrite, obj)
        .with_context(|| format!("write {:?}", path))?;
    Ok(())
}
