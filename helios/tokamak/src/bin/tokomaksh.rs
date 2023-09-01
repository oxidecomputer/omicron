// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A shell-based interface to tokamak

use anyhow::anyhow;
use camino::Utf8PathBuf;
use clap::{Parser, ValueEnum};
use helios_fusion::Host;
use helios_fusion::Input;
use helios_tokamak::FakeHost;
use slog::Drain;
use slog::Level;
use slog::LevelFilter;
use slog::Logger;
use slog_term::FullFormat;
use slog_term::TermDecorator;
use std::process::Command;

#[derive(Clone, Debug, ValueEnum)]
#[clap(rename_all = "kebab_case")]
enum MachineMode {
    /// The machine exists with no hardware
    Empty,
    /// The machine is pre-populated with some disk devices
    Disks,
    /// The machine is populated with a variety of running interfaces.
    Populate,
}

fn parse_log_level(s: &str) -> anyhow::Result<Level> {
    s.parse().map_err(|_| anyhow!("Invalid log level"))
}

#[derive(Debug, Parser)]
struct Args {
    /// Describes how to pre-populate the fake machine
    #[clap(long = "machine-mode", default_value = "empty")]
    machine_mode: MachineMode,

    /// The log level for the command.
    #[arg(long, value_parser = parse_log_level, default_value_t = Level::Warning)]
    log_level: Level,
}

fn run_command_during_setup(host: &FakeHost, command: &mut Command) {
    println!("[POPULATING] $ {}", Input::from(&*command));
    let output = host.executor().execute(command).expect("Failed during setup");

    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    let decorator = TermDecorator::new().build();
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let drain = LevelFilter::new(drain, args.log_level).fuse();
    let log = Logger::root(drain, slog::o!("unit" => "zone-bundle"));

    let config = rustyline::Config::builder().auto_add_history(true).build();
    let mut rl = rustyline::Editor::<(), _>::with_history(
        config,
        rustyline::history::MemHistory::new(),
    )?;

    let host = FakeHost::new(log);

    let add_vdevs = || {
        let vdevs = vec![
            Utf8PathBuf::from("/unreal/block/a"),
            Utf8PathBuf::from("/unreal/block/b"),
            Utf8PathBuf::from("/unreal/block/c"),
        ];

        for vdev in &vdevs {
            println!("[POPULATING] Adding virtual device: {vdev}");
        }

        host.add_devices(&vdevs);
    };

    match args.machine_mode {
        MachineMode::Empty => (),
        MachineMode::Disks => {
            add_vdevs();
        }
        MachineMode::Populate => {
            add_vdevs();
            run_command_during_setup(
                &host,
                Command::new(helios_fusion::ZPOOL).args([
                    "create",
                    "oxp_2f11d4e8-fa31-4230-a781-e800a51404e7",
                    "/unreal/block/a",
                ]),
            );
            run_command_during_setup(
                &host,
                Command::new(helios_fusion::ZFS)
                    .args(["create", "oxp_2f11d4e8-fa31-4230-a781-e800a51404e7/nested_filesystem"])
            );
        }
    }

    const DEFAULT: &str = "🍩 ";
    const OK: &str = "✅ ";
    const ERR: &str = "❌ ";
    let mut prompt = DEFAULT;

    while let Ok(line) = rl.readline(prompt) {
        let Some(args) = shlex::split(&line) else {
            eprintln!("Couldn't parse that, try again ");
            continue;
        };
        if args.is_empty() {
            prompt = DEFAULT;
            continue;
        }
        let program = helios_fusion::which_binary(&args[0]);
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(&args[1..]);
        match host.executor().execute_async(&mut cmd).await {
            Ok(output) => {
                print!("{}", String::from_utf8_lossy(&output.stdout));
                prompt = OK;
            }
            Err(err) => {
                match err {
                    helios_fusion::ExecutionError::CommandFailure(info) => {
                        eprintln!("{}", info.stderr);
                    }
                    _ => eprintln!("{}", err),
                }
                prompt = ERR;
            }
        }
    }
    Ok(())
}
