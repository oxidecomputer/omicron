// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::Cmd;
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
        let cmd: Cmd = bincode::deserialize_from(&mut stream)?;
        println!("{:#?}", cmd);
        self.handle(cmd, &mut stream)?;
        Ok(())
    }

    fn handle(
        &mut self,
        cmd: Cmd,
        stream: &mut TcpStream,
    ) -> anyhow::Result<()> {
        match cmd {
            Cmd::Load(file) => {
                self.recording_path = Some(file);
                let res: Result<(), String> = Ok(());
                bincode::serialize_into(stream, &res)?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}
