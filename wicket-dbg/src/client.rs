// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::Cmd;
use serde::de::DeserializeOwned;
use std::net::{TcpStream, ToSocketAddrs};

/// A client driven from the wicket-dbg CLI
///
/// All commands are issued in a blocking mannner, and the server only expects
/// one client at a time.
pub struct Client {
    sock: TcpStream,
}

impl Client {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> anyhow::Result<Client> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        Ok(Client { sock: stream })
    }

    /// Serialize and send a `Cmd` to the server
    ///
    /// Return any deserialized responses to the caller.
    pub fn send<T: DeserializeOwned>(
        &mut self,
        cmd: &Cmd,
    ) -> anyhow::Result<T> {
        bincode::serialize_into(&mut self.sock, cmd)?;
        let val = bincode::deserialize_from(&mut self.sock)?;
        Ok(val)
    }
}
