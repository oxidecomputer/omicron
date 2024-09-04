// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask live-tests

use crate::common::run_subcmd;
use anyhow::{bail, Context, Result};
use clap::Parser;
use std::process::Command;

#[derive(Parser)]
pub struct Args {}

pub fn run_cmd(_args: Args) -> Result<()> {
    const NAME: &str = "live-tests-archive";

    // The live tests operate in deployed environments, which always run
    // illumos.  Bail out quickly if someone tries to run this on a system whose
    // binaries won't be usable.  (We could compile this subcommand out
    // altogether on non-illumos systems, but it seems more confusing to be
    // silently missing something you might expect to be there.  Plus, you can
    // still check and even build *this* code on non-illumos systems.)
    if cfg!(not(target_os = "illumos")) {
        bail!("live-tests archive can only be built on illumos systems");
    }

    let tmpdir_root =
        camino_tempfile::tempdir().context("creating temporary directory")?;
    let final_tarball = camino::Utf8PathBuf::try_from(
        std::env::current_dir()
            .map(|d| d.join("target"))
            .context("getting current directory")?,
    )
    .context("non-UTF-8 current directory")?
    .join(format!("{}.tgz", NAME));
    let proto_root = tmpdir_root.path().join(NAME);
    let nextest_archive_file = proto_root.join("omicron-live-tests.tar.zst");

    eprintln!("using temporary directory: {}", tmpdir_root.path());
    eprintln!("will create archive file:  {}", nextest_archive_file);
    eprintln!("output tarball:            {}", final_tarball);
    eprintln!();

    std::fs::create_dir(&proto_root)
        .with_context(|| format!("mkdir {:?}", &proto_root))?;

    let cargo =
        std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
    let mut command = Command::new(&cargo);

    command.arg("nextest");
    command.arg("archive");
    command.arg("--package");
    command.arg("omicron-live-tests");
    command.arg("--archive-file");
    command.arg(&nextest_archive_file);
    run_subcmd(command)?;

    // Using nextest archives requires that the source be separately transmitted
    // to the system where the tests will be run.  We're trying to automate
    // that.  So let's bundle up the source and the nextest archive into one big
    // tarball.  But which source files do we bundle?  We need:
    //
    // - Cargo.toml (nextest expects to find this)
    // - .config/nextest.toml (nextest's configuration, which is used while
    //   running the tests)
    // - live-tests (this is where the tests live, and they might expect stuff
    //   that exists in here like expectorate files)
    //
    // plus the nextext archive file.
    //
    // To avoid creating a tarbomb, we want all the files prefixed with
    // "live-tests-archive/".  There's no great way to do this with the illumos
    // tar(1) except to create a temporary directory called "live-tests-archive"
    // that contains the files and then tar'ing up that.
    //
    // Ironically, an easy way to construct that directory is with tar(1).
    let mut command = Command::new("bash");
    command.arg("-c");
    command.arg(format!(
        "tar cf - Cargo.toml .config/nextest.toml live-tests | \
         tar xf - -C {:?}",
        &proto_root
    ));
    run_subcmd(command)?;

    let mut command = Command::new("tar");
    command.arg("cf");
    command.arg(&final_tarball);
    command.arg("-C");
    command.arg(tmpdir_root.path());
    command.arg(NAME);
    run_subcmd(command)?;

    drop(tmpdir_root);

    eprint!("created: ");
    println!("{}", &final_tarball);
    eprintln!("\nTo use this:\n");
    eprintln!(
        "1. Copy the tarball to the switch zone in a deployed Omicron system.\n"
    );
    let raw = &[
        "scp \\",
        &format!("{} \\", &final_tarball),
        "root@YOUR_SCRIMLET_GZ_IP:/zone/oxz_switch/root/root",
    ]
    .join("\n");
    let text = textwrap::wrap(
        &raw,
        textwrap::Options::new(160)
            .initial_indent("     e.g., ")
            .subsequent_indent("              "),
    );
    eprintln!("{}\n", text.join("\n"));
    eprintln!("2. Copy the `cargo-nextest` binary to the same place.\n");
    let raw = &[
        "scp \\",
        "$(which cargo-nextest) \\",
        "root@YOUR_SCRIMLET_GZ_IP:/zone/oxz_switch/root/root",
    ]
    .join("\n");
    let text = textwrap::wrap(
        &raw,
        textwrap::Options::new(160)
            .initial_indent("     e.g., ")
            .subsequent_indent("              "),
    );
    eprintln!("{}\n", text.join("\n"));
    eprintln!("3. On that system, unpack the tarball with:\n");
    eprintln!("     tar xzf {}\n", final_tarball.file_name().unwrap());
    eprintln!("4. On that system, run tests with:\n");
    // TMPDIR=/var/tmp puts stuff on disk, cached as needed, rather than the
    // default /tmp which requires that stuff be in-memory.  That can lead to
    // great sadness if the tests wind up writing a lot of data.
    //
    // nextest configuration for these tests is specified in the "live-tests"
    // profile.
    let raw = &[
        "TMPDIR=/var/tmp ./cargo-nextest nextest run --profile=live-tests \\",
        &format!(
            "--archive-file {}/{} \\",
            NAME,
            nextest_archive_file.file_name().unwrap()
        ),
        &format!("--workspace-remap {}", NAME),
    ]
    .join("\n");
    let text = textwrap::wrap(
        &raw,
        textwrap::Options::new(160)
            .initial_indent("     ")
            .subsequent_indent("         "),
    );
    eprintln!("{}\n", text.join("\n"));

    Ok(())
}
