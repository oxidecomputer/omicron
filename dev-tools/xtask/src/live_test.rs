// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask live-test

use anyhow::{bail, Context, Result};
use clap::Parser;
use std::process::Command;

// XXX-dap This should probably bail out if !illumos

#[derive(Parser)]
pub struct Args {}

pub fn run_cmd(_args: Args) -> Result<()> {
    let tmpdir =
        camino_tempfile::tempdir().context("creating temporary directory")?;
    let tarball = camino::Utf8PathBuf::try_from(
        std::env::current_dir()
            .map(|d| d.join("target"))
            .context("getting current directory")?,
    )
    .context("non-UTF-8 current directory")?
    .join("live-test-archive.tgz");
    let contents = tmpdir.path().join("live-test-archive");
    let archive_file = contents.join("omicron-live-tests.tar.zst");

    eprintln!("using temporary directory: {}", tmpdir.path());
    eprintln!("will create archive file:  {}", archive_file);
    eprintln!("output tarball:            {}", tarball);
    eprintln!();

    std::fs::create_dir(&contents)
        .with_context(|| format!("mkdir {:?}", &contents))?;

    let cargo =
        std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
    let mut command = Command::new(&cargo);

    command.arg("nextest");
    command.arg("archive");
    command.arg("--package");
    command.arg("omicron-live-tests");
    command.arg("--archive-file");
    command.arg(&archive_file);
    run_subcmd(command)?;

    // Bundle up the source.
    // XXX-dap using git here is debatable.
    // XXX-dap consider adding nextest binary
    let mut command = Command::new("git");
    command.arg("archive");
    command.arg("--format=tar.gz");
    command.arg("--prefix=live-test-archive/");
    command.arg(format!("--add-file={}", &archive_file));
    command.arg("HEAD");
    let archive_stream = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tarball)
        .with_context(|| format!("open {:?}", tarball))?;
    command.stdout(archive_stream);

    run_subcmd(command)?;

    drop(tmpdir);

    eprint!("created: ");
    println!("{}", &tarball);
    eprintln!("\nTo use this:\n");
    eprintln!(
        "1. Copy the tarball to the switch zone in a deployed Omicron system.\n"
    );
    let raw = &[
        "scp \\",
        &format!("{} \\", &tarball),
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
    eprintln!("     tar xzf {}\n", tarball.file_name().unwrap());
    eprintln!("4. On that system, run tests with:\n");
    let raw = &[
        "./cargo-nextest nextest run \\",
        &format!(
            "--archive-file {}/{} \\",
            contents.file_name().unwrap(),
            archive_file.file_name().unwrap()
        ),
        &format!("--workspace-remap {}", contents.file_name().unwrap()),
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

// XXX-dap commonize with clippy
fn run_subcmd(mut command: Command) -> Result<()> {
    eprintln!(
        "running: {} {}",
        command.get_program().to_str().unwrap(),
        command
            .get_args()
            .map(|arg| format!("{:?}", arg.to_str().unwrap()))
            .collect::<Vec<_>>()
            .join(" ")
    );

    let exit_status = command
        .spawn()
        .context("failed to spawn child process")?
        .wait()
        .context("failed to wait for child process")?;

    if !exit_status.success() {
        bail!("failed: {}", exit_status);
    }

    Ok(())
}
