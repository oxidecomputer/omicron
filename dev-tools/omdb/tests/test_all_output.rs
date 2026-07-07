// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Smoke tests for the `omdb` tool
//!
//! Feel free to change the tool's output.  This test just makes it easy to make
//! sure you're only breaking what you intend.

use dropshot::Method;
use expectorate::assert_contents;
use gateway_client::ClientInfo as _;
use http::StatusCode;
use nexus_test_utils::background::activate_background_task;
use nexus_test_utils::wait_for_producer;
use nexus_test_utils::{OXIMETER_UUID, PRODUCER_UUID};
use nexus_test_utils_macros::nexus_test;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::UnstableReconfiguratorState;
use omicron_test_utils::dev::test_cmds::Redactor;
use omicron_test_utils::dev::test_cmds::path_to_executable;
use omicron_test_utils::dev::test_cmds::run_command;
use sled_agent_types::early_networking::SwitchSlot;
use slog_error_chain::InlineErrorChain;
use std::fmt::Write;
use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;
use subprocess::Exec;
use uuid::Uuid;

/// name of the "omdb" executable
const CMD_OMDB: &str = env!("CARGO_BIN_EXE_omdb");

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

/// The `oximeter` list-producers command output is not easy to compare as a
/// string directly because the timing of registrations with both our test
/// producer and the one nexus registers. But, let's find our test producer
/// in the list.
fn assert_oximeter_list_producers_output(
    output: &str,
    ox_url: &str,
    test_producer: IpAddr,
) {
    assert!(
        output.contains(format!("Collector ID: {}", OXIMETER_UUID).as_str())
    );
    assert!(output.contains(ox_url));

    let found = output.lines().any(|line| {
        line.contains(PRODUCER_UUID)
            && line.contains(&test_producer.to_string())
    });

    assert!(
        found,
        "test producer {} and producer UUID {} not found on line together",
        test_producer, PRODUCER_UUID
    );
}

#[tokio::test]
async fn test_omdb_usage_errors() {
    clear_omdb_env();
    let cmd_path = path_to_executable(CMD_OMDB);
    let mut output = String::new();
    let invocations: &[&[&'static str]] = &[
        // No arguments
        &[],
        // Help output
        &["--help"],
        // Bogus command
        &["not-a-command"],
        // Bogus argument
        &["--not-a-command"],
        // Command help output
        &["db"],
        &["db", "--help"],
        &["db", "alert"],
        &["db", "alert", "list", "--help"],
        // Nonexistent alert class
        &["db", "alert", "list", "--classes", "test.foo.bar", "test.foo.box"],
        &["db", "disks"],
        &["db", "dns"],
        &["db", "dns", "diff"],
        &["db", "dns", "names"],
        &["db", "inventory", "--help"],
        &["db", "inventory", "collections", "--help"],
        &["db", "inventory", "collections", "show"],
        &["db", "inventory", "collections", "show", "--help"],
        &["db", "inventory", "collections", "show", "all", "--help"],
        &["db", "inventory", "collections", "show", "sp", "--help"],
        &["db", "ereport"],
        &["db", "ereport", "list", "--help"],
        &["db", "ereport", "reporters", "--help"],
        &["db", "ereport", "info", "--help"],
        &["db", "sleds", "--help"],
        &["db", "sitrep", "--help"],
        &["db", "saga"],
        &["db", "snapshots"],
        &["db", "network"],
        &["mgs"],
        &["nexus"],
        &["nexus", "background-tasks"],
        &["nexus", "background-tasks", "show", "--help"],
        &["nexus", "blueprints"],
        &["nexus", "sagas"],
        // Missing "--destructive" flag.  The URL is bogus but just ensures that
        // we get far enough to hit the error we care about.
        &[
            "nexus",
            "--nexus-internal-url",
            "http://[::1]:111",
            "sagas",
            "demo-create",
        ],
        &["nexus", "sleds"],
        &["sled-agent"],
        &["sled-agent", "zones"],
        &["oximeter", "--help"],
        &["oxql", "--help"],
        // Mispelled argument
        &["oxql", "--summarizes"],
        &["reconfigurator"],
        &["reconfigurator", "export"],
        &["reconfigurator", "archive"],
    ];

    for args in invocations {
        do_run(&mut output, |exec| exec, &cmd_path, args).await;
    }

    assert_contents("tests/usage_errors.out", &output);
}

#[tokio::test]
async fn test_omdb_success_cases() {
    // Use a custom ControlPlaneBuilder so we can enable background tasks
    // which might otherwise be disabled.
    // We want it enabled here so the omdb is realistic.
    let cptestctx =
        nexus_test_utils::ControlPlaneBuilder::new("test_omdb_success_cases")
            .with_extra_sled_agents(1)
            .customize_nexus_config(&|config| {
                config.pkg.background_tasks.sp_ereport_ingester.disable = false;
            })
            .start::<omicron_nexus::Server>()
            .await;
    clear_omdb_env();

    let cmd_path = path_to_executable(CMD_OMDB);

    let postgres_url = cptestctx.database.listen_url().to_string();
    let nexus_lockstep_url =
        format!("http://{}/", cptestctx.lockstep_client.bind_address);
    let mgs_url = cptestctx
        .gateway
        .get(&SwitchSlot::Switch0)
        .expect("nexus_test always sets up MGS on switch 0")
        .client
        .baseurl();
    let ox_url = format!("http://{}/", cptestctx.oximeter.server_address());
    let ox_test_producer = cptestctx.producer.address().ip();
    let ch_url = format!("http://{}/", cptestctx.clickhouse.http_address());

    let tmpdir = camino_tempfile::tempdir()
        .expect("failed to create temporary directory");
    let tmppath = tmpdir.path().join("reconfigurator-export.out");
    let initial_blueprint_id = cptestctx.initial_blueprint_id.to_string();

    // Get the CockroachDB metadata from the blueprint so we can redact it
    let initial_blueprint: Blueprint = dropshot::test_util::read_json(
        &mut cptestctx
            .lockstep_client
            .make_request_no_body(
                Method::GET,
                &format!("/deployment/blueprints/all/{initial_blueprint_id}"),
                StatusCode::OK,
            )
            .await
            .unwrap(),
    )
    .await;

    // Wait for Nexus to have gathered at least one inventory collection. (We'll
    // check below that `reconfigurator export` contains at least one, so have
    // to wait until there's one to export.)
    cptestctx
        .wait_for_at_least_one_inventory_collection(Duration::from_secs(60))
        .await;

    // Drive the FM task pipeline to its steady state, so that the omdb
    // snapshot of each task's last completed activation is deterministic
    // rather than racing the tasks' watch-channel triggers:
    //
    // 1. `fm_analysis` commits the first sitrep (unless its natural cadence
    //    already has). This run reports "committed new sitrep".
    // 2. `fm_sitrep_loader` loads that sitrep and publishes it on the sitrep
    //    watch channel.
    // 3. `fm_analysis` re-runs with the loaded sitrep as its parent and
    //    reports "no changes" -- the steady-state output asserted below.
    //    (No later activation ever commits another sitrep here: this
    //    environment has no in-service control plane disks and no
    //    consumable ereports, so every post-load analysis is a no-op.)
    // 4. `fm_rendezvous` runs against the loaded sitrep, so its status shows
    //    the executed operations rather than "no FM situation report loaded".
    let lockstep_client = &cptestctx.lockstep_client;
    activate_background_task(lockstep_client, "fm_analysis").await;
    activate_background_task(lockstep_client, "fm_sitrep_loader").await;
    activate_background_task(lockstep_client, "fm_analysis").await;
    activate_background_task(lockstep_client, "fm_rendezvous").await;

    let mut output = String::new();

    let invocations: &[&[&str]] = &[
        &["db", "db-metadata", "ls-nexus"],
        // We expect this operation to fail (the nexus generation is the same
        // as the one in the target blueprint - it shouldn't be trying to
        // quiesce yet).
        //
        // We test a version of this command which sets this record to quiesced
        // anyway as the final invocation.
        &[
            "--destructive",
            "db",
            "db-metadata",
            "force-mark-nexus-quiesced",
            "--skip-confirmation",
            &cptestctx.server.server_context().nexus.id().to_string(),
        ],
        &["db", "disks", "list"],
        &["db", "dns", "show"],
        &["db", "dns", "diff", "external", "2"],
        &["db", "dns", "names", "external", "2"],
        &["db", "instances"],
        &["db", "sleds"],
        &["db", "sleds", "-F", "discretionary"],
        &["mgs", "inventory"],
        &["nexus", "background-tasks", "doc"],
        // Hide "currently executing" to avoid a test flake in case a task is
        // running while this command is run. But note that there are other
        // output lines (particularly "last completed activation") which can
        // potentially be flaky. We haven't seen "last completed activation"
        // actually being flaky yet, though.
        &["nexus", "background-tasks", "show", "--no-executing-info"],
        // background tasks: test picking out specific names
        &[
            "nexus",
            "background-tasks",
            "show",
            "saga_recovery",
            "--no-executing-info",
        ],
        &[
            "nexus",
            "background-tasks",
            "show",
            "blueprint_loader",
            "blueprint_executor",
            "--no-executing-info",
        ],
        // background tasks: test recognized group names
        &[
            "nexus",
            "background-tasks",
            "show",
            "dns_internal",
            "--no-executing-info",
        ],
        &[
            "nexus",
            "background-tasks",
            "show",
            "dns_external",
            "--no-executing-info",
        ],
        &["nexus", "background-tasks", "show", "all", "--no-executing-info"],
        &["nexus", "sagas", "list"],
        &["--destructive", "nexus", "sagas", "demo-create"],
        &["nexus", "sagas", "list"],
        &[
            "--destructive",
            "nexus",
            "background-tasks",
            "activate",
            "inventory_collection",
        ],
        &["nexus", "blueprints", "list"],
        &["nexus", "blueprints", "show", &initial_blueprint_id],
        &["nexus", "blueprints", "show", "current-target"],
        &[
            "nexus",
            "blueprints",
            "diff",
            &initial_blueprint_id,
            "current-target",
        ],
        // This one should fail because it has no parent.
        &["nexus", "blueprints", "diff", &initial_blueprint_id],
        // reconfigurator config: show and set
        &["nexus", "reconfigurator-config", "show", "current"],
        &["nexus", "update-status"],
        &["nexus", "update-status", "--details"],
        // NOTE: Enabling the planner here _may_ cause Nexus to start creating
        // new blueprints; any commands whose output is only stable if the set
        // of blueprints is stable must come before this command to avoid being
        // racy.
        &[
            "-w",
            "nexus",
            "reconfigurator-config",
            "set",
            "--planner-enabled",
            "true",
        ],
        &[
            "-w",
            "nexus",
            "reconfigurator-config",
            "set",
            "--disruption-policy",
            "migrate-or-terminate",
        ],
        &["nexus", "reconfigurator-config", "show", "current"],
        &["reconfigurator", "export", tmppath.as_str()],
        // We can't easily test the sled agent output because that's only
        // provided by a real sled agent, which is not available in the
        // ControlPlaneTestContext.

        // Test the whatis command with two known UUIDs
        &[
            "db",
            "whatis",
            "001de000-5110-4000-8000-000000000000",
            "001de000-05e4-4000-8000-000000004007",
        ],
        // The alert list command should accept multiple case IDs and multiple
        // alert classes. Note that this command shouldn't actually print any
        // alerts, which is fine; this is a test for the argument parsing.
        &[
            "db",
            "alert",
            "list",
            "--cases",
            "001de000-05e4-4000-8000-000000004007",
            "98b11fa2-3437-4704-83f5-e4aa5163e672",
        ],
        &["db", "alert", "list", "--classes", "test.foo.bar", "test.foo.baz"],
        // This operation will set the "db_metadata_nexus" state to quiesced.
        //
        // This would normally only be set by a Nexus as it shuts itself down;
        // save it for last to avoid causing a weird state while testing other
        // commands.
        &[
            "--destructive",
            "db",
            "db-metadata",
            "force-mark-nexus-quiesced",
            "--skip-confirmation",
            "--skip-blueprint-validation",
            &cptestctx.server.server_context().nexus.id().to_string(),
        ],
    ];

    let mut redactor = Redactor::default();
    redactor
        .extra_variable_length("tmp_path", tmppath.as_str())
        .extra_fixed_length("blueprint_id", &initial_blueprint_id)
        .extra_variable_length(
            "cockroachdb_fingerprint",
            &initial_blueprint.cockroachdb_fingerprint,
        )
        // Error numbers vary between operating systems.
        .field("os error", r"\d+");

    let crdb_version =
        initial_blueprint.cockroachdb_setting_preserve_downgrade.to_string();
    if initial_blueprint.cockroachdb_setting_preserve_downgrade.is_set() {
        redactor.extra_variable_length("cockroachdb_version", &crdb_version);
    }

    // The `reconfigurator_config_watcher` task's output depends on
    // whether it has had time to complete an activation.
    redactor.field("config updated:", r"\w+");

    // The `tuf_artifact_replication` task's output depends on how
    // many sleds happened to register with Nexus before its first
    // execution. These redactions work around the issue described in
    // https://github.com/oxidecomputer/omicron/issues/7417.
    redactor
        .field("put config ok:", r"\d+")
        .field("list ok:", r"\d+")
        .field("triggered by", r"[\w ]+")
        .section(&["task: \"tuf_artifact_replication\"", "request ringbuf:"]);

    // The `fm_analysis` task's input report includes a line comparing the
    // current inventory collection against the parent sitrep's collection,
    // which can be either "same" or "different" depending on whether a new
    // inventory was collected between sitreps. Collapse both forms.
    redactor.variable_regex(
        "fm_input_inv_comparison",
        r" --> (same collection as parent sitrep|different from parent sitrep \(collection [-a-f0-9]+\))",
    );

    // The `fm_sitrep_gc` task's orphan counts are racy. When two `fm_analysis`
    // activations overlap (e.g. the boot-time activation and the explicit drive
    // above), both can insert a first sitrep before either is made current; the
    // loser's sitrep and its stashed analysis report are inserted but orphaned
    // (see `DataStore::fm_sitrep_insert`'s `ParentNotCurrent` path), and a
    // later GC pass deletes them. Whether that race happened before this
    // snapshot is timing-dependent, so redact both counts. Other child tables
    // stay zero: with no faults, orphaned sitreps carry no cases, facts,
    // ereports, or bundles.
    redactor
        .field("orphaned sitreps deleted:", r"\d+")
        .field("orphaned fm_sitrep_analysis_report rows deleted:", r"\d+");

    // The `sp_ereport_ingester` task's output depends on how many simulated
    // sled agents ahppen to register with Nexus before its first execution.
    // These redactions work around the issue described in
    // https://github.com/oxidecomputer/omicron/issues/8979
    redactor
        .field("total ereports received:", r"\d+")
        .field("new ereports ingested:", r"\d+")
        .field("total HTTP requests sent:", r"\d+")
        .field("total collection errors:", r"\d+")
        .field("total reporters:", r"\d+")
        .field("contacted successfully:", r"\d+")
        .field("with ereports:", r"\d+")
        .field("without ereports:", r"\d+")
        .field("with collection errors:", r"\d+")
        .totally_annihilate_section(&[
            "task: \"sp_ereport_ingester\"",
            "errors listing reporters:",
        ])
        .totally_annihilate_section(&[
            "task: \"sp_ereport_ingester\"",
            "service processors:",
        ]);

    for args in invocations {
        println!("running commands with args: {:?}", args);
        let p = postgres_url.clone();
        let u = nexus_lockstep_url.clone();
        let g = mgs_url.to_owned();
        let ox = ox_url.clone();
        let ch = ch_url.clone();
        do_run_extra(
            &mut output,
            move |exec| {
                exec.env("OMDB_DB_URL", &p)
                    .env("OMDB_NEXUS_URL", &u)
                    .env("OMDB_MGS_URL", &g)
                    .env("OMDB_OXIMETER_URL", &ox)
                    .env("OMDB_CLICKHOUSE_URL", &ch)
            },
            &cmd_path,
            args,
            &redactor,
        )
        .await;
    }

    assert_contents("tests/successes.out", &output);

    // The `reconfigurator-save` output is not easy to compare as a string.  But
    // let's make sure we can at least parse it and that it looks broadly like
    // what we'd expect.
    let generated = std::fs::read_to_string(&tmppath).unwrap_or_else(|error| {
        panic!(
            "failed to read temporary file containing reconfigurator-save \
            output: {:?}: {}",
            tmppath,
            InlineErrorChain::new(&error),
        )
    });
    let parsed: UnstableReconfiguratorState = serde_json::from_str(&generated)
        .unwrap_or_else(|error| {
            panic!(
                "failed to parse reconfigurator-save output (path {}): {}",
                tmppath,
                InlineErrorChain::new(&error),
            )
        });
    // Did we find at least one sled in the planning input, and at least one
    // collection?
    assert!(
        parsed
            .planning_input
            .all_sled_ids(SledFilter::Commissioned)
            .next()
            .is_some()
    );
    assert!(!parsed.collections.is_empty());

    // Exercise `omdb support-bundle collect` end-to-end. We don't add this
    // to the `successes.out` snapshot because the output includes a
    // randomly-generated bundle UUID, timing-dependent step durations,
    // and per-sled step names that would all need redaction. Instead we
    // run the command and verify the resulting zip is well-formed and
    // contains the expected metadata files.
    let bundle_path = tmpdir.path().join("bundle.zip");
    let bundle_args: &[&str] = &[
        "-w",
        "support-bundle",
        "collect",
        "--output",
        bundle_path.as_str(),
        "--tempdir",
        tmpdir.path().as_str(),
        "--reason",
        "integration test",
    ];
    let mut bundle_output = String::new();
    let p = postgres_url.clone();
    let dns = cptestctx.internal_dns.dns_server.local_address().to_string();
    do_run_no_redactions(
        &mut bundle_output,
        move |exec| exec.env("OMDB_DB_URL", &p).env("OMDB_DNS_SERVER", &dns),
        &cmd_path,
        bundle_args,
    )
    .await;
    let zip_file = std::fs::File::open(&bundle_path).unwrap_or_else(|err| {
        panic!(
            "bundle zip not produced at {bundle_path}: {}\n\
             omdb output was:\n{bundle_output}",
            InlineErrorChain::new(&err),
        )
    });
    let mut archive =
        zip::ZipArchive::new(zip_file).expect("bundle is a valid zip archive");
    for required in [
        "bundle_id.txt",
        "meta/reason_for_creation.txt",
        "meta/trace.json",
        "sp_task_dumps/sled_0/dump-0.zip",
        "sp_task_dumps/sled_1/dump-0.zip",
        "sp_task_dumps/switch_0/dump-0.zip",
        "sp_task_dumps/switch_1/dump-0.zip",
    ] {
        assert!(
            archive.by_name(required).is_ok(),
            "bundle zip is missing expected entry {required}",
        );
    }

    // Now exercise the stdout-streaming path: omit `--output` and
    // capture stdout as bytes. Verifies the data-descriptor zip variant
    // produced by `bundle_to_stream` is well-formed and contains the
    // expected metadata.
    let stdout_path = tmpdir.path().join("bundle-stdout.zip");
    let stdout_file =
        std::fs::File::create(&stdout_path).expect("create stdout capture");
    let cmd_path_owned = cmd_path.to_path_buf();
    let p = postgres_url.clone();
    let dns = cptestctx.internal_dns.dns_server.local_address().to_string();
    let stream_tempdir = tmpdir.path().to_owned();
    let exit_status = tokio::task::spawn_blocking(move || {
        Exec::cmd(&cmd_path_owned)
            .env("OMDB_DB_URL", &p)
            .env("OMDB_DNS_SERVER", &dns)
            .env("RUST_BACKTRACE", "1")
            .env("RUST_LIB_BACKTRACE", "0")
            .args(&[
                "-w",
                "support-bundle",
                "collect",
                "--tempdir",
                stream_tempdir.as_str(),
                "--reason",
                "integration test (stdout)",
            ])
            .stdout(subprocess::Redirection::File(stdout_file))
            .join()
            .expect("running omdb to stream a bundle")
    })
    .await
    .expect("spawn_blocking");
    assert!(exit_status.success(), "stdout streaming failed: {exit_status:?}");
    let zip_file = std::fs::File::open(&stdout_path)
        .expect("captured stdout file should exist");
    let mut archive = zip::ZipArchive::new(zip_file)
        .expect("streamed bundle is a valid zip archive");
    for required in [
        "bundle_id.txt",
        "meta/reason_for_creation.txt",
        "meta/trace.json",
        "sp_task_dumps/sled_0/dump-0.zip",
        "sp_task_dumps/sled_1/dump-0.zip",
        "sp_task_dumps/switch_0/dump-0.zip",
        "sp_task_dumps/switch_1/dump-0.zip",
    ] {
        assert!(
            archive.by_name(required).is_ok(),
            "streamed bundle zip is missing expected entry {required}",
        );
    }

    let ox_invocation = &["oximeter", "list-producers"];
    let mut ox_output = String::new();
    let ox = ox_url.clone();

    wait_for_producer(
        &cptestctx.oximeter,
        PRODUCER_UUID.parse::<Uuid>().unwrap(),
    )
    .await;
    do_run_no_redactions(
        &mut ox_output,
        move |exec| exec.env("OMDB_OXIMETER_URL", &ox),
        &cmd_path,
        ox_invocation,
    )
    .await;
    assert_oximeter_list_producers_output(
        &ox_output,
        &ox_url,
        ox_test_producer,
    );

    cptestctx.teardown().await;
}

/// Verify that we properly deal with cases where:
///
/// - a URL is specified on the command line
/// - a URL is specified in both places
///
/// for both of the URLs that we accept.  We don't need to check the cases where
/// (1) no URL is specified in either place because that's covered by the usage
/// test above, nor (2) the URL is specified only in the environment because
/// that's covered by the success tests above.
#[nexus_test(extra_sled_agents = 1)]
async fn test_omdb_env_settings(cptestctx: &ControlPlaneTestContext) {
    clear_omdb_env();

    let cmd_path = path_to_executable(CMD_OMDB);
    let postgres_url = cptestctx.database.listen_url().to_string();
    let nexus_lockstep_url =
        format!("http://{}", cptestctx.lockstep_client.bind_address);
    let ox_url = format!("http://{}/", cptestctx.oximeter.server_address());
    let ox_test_producer = cptestctx.producer.address().ip();
    let ch_url = format!("http://{}/", cptestctx.clickhouse.http_address());
    let dns_sockaddr = cptestctx.internal_dns.dns_server.local_address();
    let mut output = String::new();

    // Database URL
    // Case 1: specified on the command line
    let args = &["db", "--db-url", &postgres_url, "sleds"];
    do_run(&mut output, |exec| exec, &cmd_path, args).await;

    // Case 2: specified in multiple places (command-line argument wins)
    let args = &["db", "--db-url", "junk", "sleds"];
    let p = postgres_url.clone();
    do_run(
        &mut output,
        move |exec| exec.env("OMDB_DB_URL", &p),
        &cmd_path,
        args,
    )
    .await;

    // Nexus URL
    // Case 1: specified on the command line
    let args = &[
        "nexus",
        "--nexus-internal-url",
        &nexus_lockstep_url.clone(),
        "background-tasks",
        "doc",
    ];
    do_run(&mut output, |exec| exec, &cmd_path.clone(), args).await;

    // Case 2: specified in multiple places (command-line argument wins)
    let args =
        &["nexus", "--nexus-internal-url", "junk", "background-tasks", "doc"];
    let n = nexus_lockstep_url.clone();
    do_run(
        &mut output,
        move |exec| exec.env("OMDB_NEXUS_URL", &n),
        &cmd_path,
        args,
    )
    .await;

    // Verify that if you provide a working internal DNS server, you can omit
    // the URLs.  That's true regardless of whether you pass it on the command
    // line or via an environment variable.
    let args = &["nexus", "background-tasks", "doc"];
    do_run(
        &mut output,
        move |exec| exec.env("OMDB_DNS_SERVER", dns_sockaddr.to_string()),
        &cmd_path,
        args,
    )
    .await;

    let args = &[
        "--dns-server",
        &dns_sockaddr.to_string(),
        "nexus",
        "background-tasks",
        "doc",
    ];
    do_run(&mut output, move |exec| exec, &cmd_path, args).await;

    let args = &["db", "sleds"];
    do_run(
        &mut output,
        move |exec| exec.env("OMDB_DNS_SERVER", dns_sockaddr.to_string()),
        &cmd_path,
        args,
    )
    .await;

    let args = &["--dns-server", &dns_sockaddr.to_string(), "db", "sleds"];
    do_run(&mut output, move |exec| exec, &cmd_path, args).await;

    // That said, the "sagas" command prints an extra warning in this case.
    let args = &["nexus", "sagas", "list"];
    do_run(
        &mut output,
        move |exec| exec.env("OMDB_DNS_SERVER", &dns_sockaddr.to_string()),
        &cmd_path,
        args,
    )
    .await;

    // Case: specified in multiple places (command-line argument wins)
    let args = &["oximeter", "--oximeter-url", "junk", "list-producers"];
    let ox = ox_url.clone();
    do_run(
        &mut output,
        move |exec| exec.env("OMDB_OXIMETER_URL", &ox),
        &cmd_path,
        args,
    )
    .await;

    // Case: specified in multiple places (command-line argument wins)
    let args = &["oxql", "--clickhouse-url", "junk"];
    do_run(
        &mut output,
        move |exec| exec.env("OMDB_CLICKHOUSE_URL", &ch_url),
        &cmd_path,
        args,
    )
    .await;

    assert_contents("tests/env.out", &output);

    // Oximeter URL
    // Case 1: specified on the command line.
    // Case 2: is covered by the success tests above.
    let ox_args1 = &["oximeter", "--oximeter-url", &ox_url, "list-producers"];
    let mut ox_output1 = String::new();
    wait_for_producer(
        &cptestctx.oximeter,
        PRODUCER_UUID.parse::<Uuid>().unwrap(),
    )
    .await;
    do_run_no_redactions(
        &mut ox_output1,
        move |exec| exec,
        &cmd_path,
        ox_args1,
    )
    .await;
    assert_oximeter_list_producers_output(
        &ox_output1,
        &ox_url,
        ox_test_producer,
    );
}

async fn do_run<F>(
    output: &mut String,
    modexec: F,
    cmd_path: &Path,
    args: &[&str],
) where
    F: FnOnce(Exec) -> Exec + Send + 'static,
{
    do_run_extra(output, modexec, cmd_path, args, &Redactor::default()).await;
}

async fn do_run_no_redactions<F>(
    output: &mut String,
    modexec: F,
    cmd_path: &Path,
    args: &[&str],
) where
    F: FnOnce(Exec) -> Exec + Send + 'static,
{
    do_run_extra(output, modexec, cmd_path, args, &Redactor::noop()).await;
}

async fn do_run_extra<F>(
    output: &mut String,
    modexec: F,
    cmd_path: &Path,
    args: &[&str],
    redactor: &Redactor<'_>,
) where
    F: FnOnce(Exec) -> Exec + Send + 'static,
{
    write!(
        output,
        "EXECUTING COMMAND: {} {:?}\n",
        cmd_path.file_name().expect("missing command").to_string_lossy(),
        args.iter().map(|r| redactor.do_redact(r)).collect::<Vec<_>>()
    )
    .unwrap();

    // Using `subprocess`, the child process will be spawned synchronously.  In
    // some cases it then tries to make an HTTP request back into this process.
    // But if the executor is blocked on the child process, the HTTP server
    // acceptor won't run and we'll deadlock.  So we use spawn_blocking() to run
    // the child process from a different thread.  tokio requires that these be
    // 'static (it does not know that we're going to wait synchronously for the
    // task) so we need to create owned versions of these arguments.
    let cmd_path = cmd_path.to_owned();
    let owned_args: Vec<_> = args.into_iter().map(|s| s.to_string()).collect();
    let (exit_status, stdout_text, stderr_text) =
        tokio::task::spawn_blocking(move || {
            let exec = modexec(
                Exec::cmd(cmd_path)
                    // Set RUST_BACKTRACE explicitly for consistency between CI
                    // and developers' local runs.  We set it to 1 so that in
                    // the event of a panic, particularly in CI, we have more
                    // information about what went wrong.  But we set
                    // RUST_LIB_BACKTRACE=1 so that we don't get a dump to
                    // stderr for all the Errors that get created and handled
                    // gracefully.
                    .env("RUST_BACKTRACE", "1")
                    .env("RUST_LIB_BACKTRACE", "0")
                    .args(&owned_args),
            );
            run_command(exec)
        })
        .await
        .unwrap();

    write!(output, "termination: {:?}\n", exit_status).unwrap();
    write!(output, "---------------------------------------------\n").unwrap();
    write!(output, "stdout:\n").unwrap();
    output.push_str(&redactor.do_redact(&stdout_text));

    write!(output, "---------------------------------------------\n").unwrap();
    write!(output, "stderr:\n").unwrap();
    output.push_str(&redactor.do_redact(&stderr_text));

    write!(output, "=============================================\n").unwrap();
}

// We're testing behavior that can be affected by OMDB-related environment
// variables.  Clear all of them from the current process so that all child
// processes don't have them.  OMDB environment variables can affect even the
// help output provided by clap.  See clap-rs/clap#5673 for an example.
fn clear_omdb_env() {
    // Rust documents that it's not safe to manipulate the environment in a
    // multi-threaded process outside of Windows because it's possible that
    // other threads are reading or writing the environment and most systems do
    // not support this.  On illumos, the underlying interfaces are broadly
    // thread-safe.  Further, Omicron only supports running tests under `cargo
    // nextest`, in which case there are no threads running concurrently here
    // that may be reading or modifying the environment.
    for (env_var, _) in std::env::vars().filter(|(k, _)| k.starts_with("OMDB_"))
    {
        eprintln!("removing {:?} from environment", env_var);
        // SAFETY: https://nexte.st/docs/configuration/env-vars/#altering-the-environment-within-tests
        unsafe { std::env::remove_var(env_var) };
    }
}
