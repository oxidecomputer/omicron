// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{Cmd, Runner};
use std::fs::File;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use wicket::Snapshot;

/// The server used to handle wicket-dbg commands, run them, and return the
/// response to the client.
///
/// We only allow one client to connect at a time because:
///   1. The client is a human trying to debug
///   2. There is only one screen to display the wicket UI on
pub struct Server {
    recording_path: Option<PathBuf>,
    runner: Runner,
}

impl Server {
    pub fn run<A: ToSocketAddrs>(
        addr: A,
        runner: Runner,
    ) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr)?;

        let mut server = Server { recording_path: None, runner };

        // accept connections and process them serially
        for stream in listener.incoming() {
            server.handle_client(stream?)?;
        }
        Ok(())
    }

    fn handle_client(&mut self, mut stream: TcpStream) -> anyhow::Result<()> {
        loop {
            let cmd: Cmd = ciborium::de::from_reader(&mut stream)?;
            self.handle(cmd, &mut stream)?;
        }
    }

    fn handle(
        &mut self,
        cmd: Cmd,
        stream: &mut TcpStream,
    ) -> anyhow::Result<()> {
        match cmd {
            Cmd::Load(path) => match File::open(&path) {
                Ok(file) => match ciborium::de::from_reader(&file) {
                    Ok(snapshot) => {
                        self.recording_path = Some(path);
                        self.runner.load_snapshot(snapshot);
                        ok_reply(stream)?;
                    }
                    Err(e) => {
                        let res: Result<(), String> = Err(format!(
                            "Failed to deserialize recording: {}",
                            e.to_string()
                        ));
                        ciborium::ser::into_writer(&res, stream)?;
                    }
                },
                Err(e) => {
                    let res: Result<(), String> =
                        Err(format!("{e}: {}", path.display()));
                    ciborium::ser::into_writer(&res, stream)?;
                }
            },
            Cmd::Reset => {
                // We can't really tolerate render failures
                self.runner.restart().unwrap();
                ok_reply(stream)?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

fn ok_reply(stream: &mut TcpStream) -> anyhow::Result<()> {
    let res: Result<(), String> = Ok(());
    ciborium::ser::into_writer(&res, stream)?;
    Ok(())
}
