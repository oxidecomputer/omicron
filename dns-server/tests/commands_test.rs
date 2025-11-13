// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino_tempfile::Utf8TempDir;
use dns_server::storage::Store;
use omicron_test_utils::dev::test_cmds::EXIT_SUCCESS;
use omicron_test_utils::dev::test_cmds::assert_exit_code;
use omicron_test_utils::dev::test_cmds::path_to_executable;
use omicron_test_utils::dev::test_cmds::run_command;
use omicron_test_utils::dev::test_setup_log;
use std::net::SocketAddr;

const CMD_DNSADM: &str = env!("CARGO_BIN_EXE_dnsadm");

#[tokio::test]
async fn test_dnsadm() {
    // Start a DNS server with some sample data.
    let logctx = test_setup_log("test_dnsadm");
    let tmpdir = Utf8TempDir::with_prefix("test_dnsadm")
        .expect("failed to create tmp directory for test");
    let storage_path = tmpdir.path().to_path_buf();

    let store = Store::new(
        logctx.log.clone(),
        &dns_server::storage::Config { storage_path, keep_old_generations: 3 },
    )
    .expect("failed to create test Store");
    assert!(store.is_new());

    let (_dns_server, dropshot_server) = dns_server::start_servers(
        logctx.log.clone(),
        store,
        &dns_server::dns::server::Config {
            bind_address: "[::1]:0".parse().unwrap(),
            ..Default::default()
        },
        &dropshot::ConfigDropshot {
            bind_address: "[::1]:0".parse().unwrap(),
            ..Default::default()
        },
    )
    .await
    .expect("starting servers");

    let config_addr = dropshot_server.local_addr();

    let h1 = tokio::task::spawn_blocking(move || {
        // Now run a sequence of `dnsadm` commands against that server, put the
        // results together, and check against expected output.
        let mut buffer = String::new();

        run(&mut buffer, config_addr, &["list-records"]);
        run(
            &mut buffer,
            config_addr,
            &["add-aaaa", "z1.oxide.test", "host1", "fe80::2:1"],
        );
        run(&mut buffer, config_addr, &["list-records"]);
        run(
            &mut buffer,
            config_addr,
            &["add-aaaa", "z1.oxide.test", "host1", "fe80::2:2"],
        );
        run(&mut buffer, config_addr, &["list-records"]);
        run(
            &mut buffer,
            config_addr,
            &["add-aaaa", "z1.oxide.test", "host2", "fe80::2:3"],
        );
        run(
            &mut buffer,
            config_addr,
            &["add-aaaa", "z2.oxide.test", "host1", "fe80::3:1"],
        );
        run(&mut buffer, config_addr, &["list-records"]);
        run(
            &mut buffer,
            config_addr,
            &[
                "add-srv",
                "z1.oxide.test",
                "s1.services",
                "0",
                "0",
                "12345",
                "host1.z1.oxide.test",
            ],
        );
        run(
            &mut buffer,
            config_addr,
            &[
                "add-srv",
                "z1.oxide.test",
                "s1.services",
                "0",
                "0",
                "12345",
                "host2.z1.oxide.test",
            ],
        );
        run(&mut buffer, config_addr, &["list-records"]);
        run(
            &mut buffer,
            config_addr,
            &["delete-record", "z1.oxide.test", "host1"],
        );
        run(&mut buffer, config_addr, &["list-records"]);
        // This one should do nothing.
        run(
            &mut buffer,
            config_addr,
            &["delete-record", "z1.oxide.test", "host1"],
        );
        run(&mut buffer, config_addr, &["list-records"]);
        buffer
    });

    let output = h1.await.expect("command task panicked");
    let _ = dropshot_server.close().await;

    expectorate::assert_contents("tests/output/test_dnsadm.txt", &output);
    logctx.cleanup_successful();
}

fn run(s: &mut String, config_addr: SocketAddr, args: &[&str]) {
    let path = path_to_executable(CMD_DNSADM);
    let mut exec = subprocess::Exec::cmd(&path)
        .arg("--address")
        .arg(config_addr.to_string());

    // Redact the TCP port number because it changes with each invocation.
    let mut cmdstr_redacted = "dnsadm --address REDACTED".to_string();

    for arg in args {
        exec = exec.arg(arg);
        cmdstr_redacted = cmdstr_redacted + " " + arg;
    }

    println!("running command: {:?}", exec);
    let (exit_status, stdout, stderr) = run_command(exec);
    println!("command exited with status: {:?}", exit_status);
    println!("stderr:\n{}\n", stderr);
    println!("stdout:\n{}\n", stdout);

    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr);

    // Append the output to the expected output string.  Redact the timestamps,
    // since those will change with each invocation.
    s.push_str("----------------------\n");
    s.push_str(&format!("command: {}\n", cmdstr_redacted));
    s.push_str("----------------------\n");
    s.push_str(
        &stdout
            .lines()
            .map(|l| {
                if l.starts_with("    created ") && l.ends_with("UTC") {
                    "    created <REDACTED>"
                } else if l.starts_with("    applied ") && l.ends_with("UTC") {
                    "    applied <REDACTED>"
                } else {
                    l
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
    );
    s.push_str("\n\n");
}
