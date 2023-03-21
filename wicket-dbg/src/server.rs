// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::Cmd;
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
#[derive(Debug, Default)]
pub struct Server {
    recording_path: Option<PathBuf>,
    snapshot: Option<Snapshot>,
}

impl Server {
    pub fn run<A: ToSocketAddrs>(addr: A) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr)?;

        let mut server = Server::default();

        // accept connections and process them serially
        for stream in listener.incoming() {
            server.handle_client(stream?)?;
        }
        Ok(())
    }

    fn handle_client(&mut self, mut stream: TcpStream) -> anyhow::Result<()> {
        loop {
            let cmd: Cmd = ciborium::de::from_reader(&mut stream)?;
            println!("{:#?}", cmd);
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
                        self.snapshot = Some(snapshot);
                        println!("{:#?}", self.snapshot);
                        let res: Result<(), String> = Ok(());
                        ciborium::ser::into_writer(&res, stream)?;
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
            _ => unreachable!(),
        }
        Ok(())
    }
}
