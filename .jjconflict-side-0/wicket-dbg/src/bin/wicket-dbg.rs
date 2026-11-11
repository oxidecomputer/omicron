// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use reedline::{
    DefaultPrompt, DefaultPromptSegment, FileBackedHistory, Reedline, Signal,
};
use std::{iter, path::PathBuf};
use wicket_dbg::Client;

fn main() -> anyhow::Result<()> {
    // TODO: Configuration
    let mut client = Client::connect("::1:9010")?;

    // TODO: Configuration
    let mut path = PathBuf::from(std::env::var("HOME").unwrap());
    path.push(".wicket-dbg.history.txt");

    let history = Box::new(
        FileBackedHistory::with_file(1024, path)
            .expect("Error configuring history"),
    );

    let mut rl = Reedline::create().with_history(history);
    let prompt = DefaultPrompt::new(
        DefaultPromptSegment::WorkingDirectory,
        DefaultPromptSegment::Empty,
    );

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

    let res = match cli.cmd {
        Cmds::Load { path } => client.send(&wicket_dbg::Cmd::Load(path)),
        Cmds::Reset => client.send(&wicket_dbg::Cmd::Reset),
        Cmds::Run { slowdown, speedup } => {
            client.send(&wicket_dbg::Cmd::Run { speedup, slowdown })
        }
        Cmds::Pause => client.send(&wicket_dbg::Cmd::Pause),
        Cmds::Resume => client.send(&wicket_dbg::Cmd::Resume),
        Cmds::GetState => client.send(&wicket_dbg::Cmd::GetState),
        Cmds::Break { event } => client.send(&wicket_dbg::Cmd::Break { event }),
        Cmds::GetEvent => client.send(&wicket_dbg::Cmd::GetEvent),
        Cmds::Step => client.send(&wicket_dbg::Cmd::Step),
        Cmds::Jump => client.send(&wicket_dbg::Cmd::Jump),
        Cmds::DebugState => client.send(&wicket_dbg::Cmd::DebugState),
    };
    match res {
        Ok(rpy) => format!("{:#?}", rpy),
        Err(e) => format!("{:?}", e),
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
    ///
    /// We can speedup or slowdown the recording by dividing or multiplying
    /// the tick duration respectively. `speedup` and `slowdown` are mutually
    /// exclusive factors that affect tick duration.
    Run {
        #[clap(long, conflicts_with = "slowdown")]
        speedup: Option<u32>,

        #[clap(long, conflicts_with = "speedup")]
        slowdown: Option<u32>,
    },

    /// Pause a running recording
    Pause,

    /// Resume a running recording
    Resume,

    /// Return the current state
    GetState,

    /// Set a break point at the given event index
    Break { event: u32 },

    /// Get the event index of the current event at the server
    GetEvent,

    /// Execute the next event
    Step,

    /// Execute all events including the next event that is not a `Tick`
    Jump,

    /// Return information about the state of the debugger
    DebugState,
}
