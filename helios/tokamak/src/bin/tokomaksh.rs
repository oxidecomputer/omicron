// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A shell-based interface to tokamak

use anyhow::anyhow;
use camino::Utf8PathBuf;
use clap::{Parser, ValueEnum};
use helios_fusion::Host;
use slog::Drain;
use slog::Level;
use slog::LevelFilter;
use slog::Logger;
use slog_term::FullFormat;
use slog_term::TermDecorator;

#[derive(Clone, Debug, ValueEnum)]
#[clap(rename_all = "kebab_case")]
enum MachineMode {
    /// The machine exists with no hardware
    Empty,
    /// The machine is pre-populated with some disk devices
    Disks,
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

    let host = helios_tokamak::FakeHost::new(log);

    match args.machine_mode {
        MachineMode::Disks => {
            let vdevs = vec![
                Utf8PathBuf::from("/unreal/block/a"),
                Utf8PathBuf::from("/unreal/block/b"),
                Utf8PathBuf::from("/unreal/block/c"),
            ];

            for vdev in &vdevs {
                println!("Adding virtual device: {vdev}");
            }

            host.add_devices(&vdevs);
        }
        MachineMode::Empty => (),
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
