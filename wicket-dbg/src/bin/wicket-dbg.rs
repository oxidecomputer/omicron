// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use reedline::{DefaultPrompt, Reedline, Signal};
use std::iter;
use wicket_dbg::Client;

fn main() -> anyhow::Result<()> {
    let mut client = Client::connect("::1:9010")?;

    let mut rl = Reedline::create();
    let prompt = DefaultPrompt::default();

    loop {
        let sig = rl.read_line(&prompt);
        match sig {
            Ok(Signal::Success(line)) => {
                let output = process(&mut client, line);
                println!("{}", output);
            }
            Ok(Signal::CtrlD) | Ok(Signal::CtrlC) => {
                println!("\nAborted!");
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

/// Process a line from a user at the terminal
fn process(client: &mut Client, line: String) -> String {
    // we pretend there is no "command", since we are embedding clap
    // inside reedline
    let args = iter::once("").chain(line.split_ascii_whitespace());
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(e) => {
            return format!("{}", e);
        }
    };

    match cli.cmd {
        Cmds::Load { path } => {
            let rsp: Result<Result<(), String>> =
                client.send(&wicket_dbg::Cmd::Load(path));
            format!("{:?}", rsp)
        }
        Cmds::Reset => {
            let rsp: Result<Result<(), String>> =
                client.send(&wicket_dbg::Cmd::Reset);
            format!("{:?}", rsp)
        }
        Cmds::Run => {
            let rsp: Result<Result<(), String>> = client
                .send(&wicket_dbg::Cmd::Run { speedup: None, slowdown: None });
            format!("{:?}", rsp)
        }
    }
}

#[derive(Parser)]
#[command(name = "")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmds,
}

#[derive(Subcommand)]
enum Cmds {
    /// Load a wicket recording on the debug server
    Load { path: Utf8PathBuf },

    /// Reset the debugger to the start of the recording
    Reset,

    /// Play the recording
    // TODO: speedup/slowdown
    Run,
}
