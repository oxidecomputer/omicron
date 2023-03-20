// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use reedline::{DefaultPrompt, Reedline, Signal};
use wicket_dbg::Client;

fn main() -> Result<()> {
    let mut client = Client::connect("::1:9010")?;

    let mut rl = Reedline::create();
    let prompt = DefaultPrompt::default();

    loop {
        let sig = rl.read_line(&prompt);
        match sig {
            Ok(Signal::Success(line)) => {
                let output = process(&mut client, line)?;
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
fn process(client: &mut Client, line: String) -> anyhow::Result<String> {
    let mut args = line.split_ascii_whitespace();

    let cmd = args.next();
    if cmd.is_none() {
        return Ok("\n".to_string());
    }

    match cmd.unwrap() {
        "load" | "l" => {
            let rsp = client.send::<Result<(), String>>(
                &wicket_dbg::Cmd::Load("SOME RECORDING".into()),
            )?;
            Ok(format!("{:?}", rsp))
        }
        _ => Ok("Error: Unknown Command".to_string()),
    }
}
