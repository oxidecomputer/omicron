// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::Cmd;
use serde::de::DeserializeOwned;
use std::io::{Read, Write};
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
        // write 4 byte le size header, followed by data
        let size: u32 = bincode::serialized_size(cmd)?.try_into()?;
        self.sock.write_all(&size.to_le_bytes())?;
        let buf = bincode::serialize(cmd)?;
        self.sock.write_all(&buf)?;

        // Read 4 byte le size header, followed by data
        let mut size_buf = [0u8; 4];
        self.sock.read_exact(&mut size_buf[..])?;
        let size = u32::from_le_bytes(size_buf) as usize;
        let mut buf = vec![0; size];
        self.sock.read_exact(&mut buf)?;
        let val = bincode::deserialize(&buf)?;

        Ok(val)
    }
}
