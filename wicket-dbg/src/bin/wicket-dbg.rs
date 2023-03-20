// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use rustyline::{error::ReadlineError, DefaultEditor};
use wicket_dbg::Client;

fn main() -> Result<()> {
    let mut client = Client::connect("::1:9010")?;

    let mut rl = DefaultEditor::new()?;

    //    loop {
    let readline = rl.readline(">> ");
    match readline {
        Ok(line) => {
            //            rl.add_history_entry(line.as_str())?;
            let output = process(&mut client, line)?;
            println!("{}", output);
            //                client.send(&wicket_dbg::Cmd::Load("SOME RECORDING".into()))?;
        }
        Err(ReadlineError::Interrupted) => {
            println!("CTRL-C");
            //      break;
        }
        Err(ReadlineError::Eof) => {
            println!("CTRL-D");
            //    break;
        }
        Err(err) => {
            println!("Error: {:?}", err);
            //                break;
        }
    }
    //  }

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
        "load" | "l" => Ok("LOAD".to_string()),
        _ => Ok("Error: Unknown Command".to_string()),
    }
}
