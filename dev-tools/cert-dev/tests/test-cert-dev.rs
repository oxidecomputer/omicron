// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for cert-dev.

use std::path::PathBuf;

use anyhow::Context;
use omicron_test_utils::dev::test_cmds::{
    assert_exit_code, path_to_executable, run_command, EXIT_SUCCESS,
};
use subprocess::Exec;

const CMD_CERT_DEV: &str = env!("CARGO_BIN_EXE_cert-dev");

fn path_to_cert_dev() -> PathBuf {
    path_to_executable(CMD_CERT_DEV)
}

#[test]
fn test_cert_create() {
    let tmpdir = camino_tempfile::tempdir().unwrap();
    println!("tmpdir: {}", tmpdir.path());
    let output_base = format!("{}/test-", tmpdir.path());
    let exec = Exec::cmd(path_to_cert_dev())
        .arg("create")
        .arg(output_base)
        .arg("foo.example")
        .arg("bar.example");
    let (exit_status, _, stderr_text) = run_command(exec);
    assert_exit_code(exit_status, EXIT_SUCCESS, &stderr_text);
    let cert_path = tmpdir.path().join("test-cert.pem");
    let key_path = tmpdir.path().join("test-key.pem");
    let cert_contents = std::fs::read(&cert_path)
        .with_context(|| format!("reading certificate path {:?}", cert_path))
        .unwrap();
    let key_contents = std::fs::read(&key_path)
        .with_context(|| format!("reading private key path: {:?}", key_path))
        .unwrap();
    let certs_pem = openssl::x509::X509::stack_from_pem(&cert_contents)
        .context("parsing certificate")
        .unwrap();
    let private_key = openssl::pkey::PKey::private_key_from_pem(&key_contents)
        .context("parsing private key")
        .unwrap();
    assert!(certs_pem
        .iter()
        .last()
        .unwrap()
        .public_key()
        .unwrap()
        .public_eq(&private_key));
}
