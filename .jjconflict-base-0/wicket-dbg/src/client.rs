// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{Cmd, Rpy};
use anyhow::bail;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};

const SMALL_BUF_SIZE: usize = 256;

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
    pub fn send(&mut self, cmd: &Cmd) -> anyhow::Result<Rpy> {
        // Requests are always small
        let mut buf = Vec::with_capacity(SMALL_BUF_SIZE);
        ciborium::ser::into_writer(cmd, &mut buf)?;
        let frame_size = u32::try_from(buf.len()).unwrap();

        // Write the 4-byte size header and serialized data
        self.sock.write_all(&frame_size.to_le_bytes()[..])?;
        self.sock.write_all(&buf[..])?;

        // Reuse buf. Most replies are small.
        buf.resize(SMALL_BUF_SIZE, 0);

        // Read the 4-byte size header for the response and serialized data
        // We loop, rather than reading exactly the header size of bytes to
        // minimize the number of read syscalls, and also potentially the
        // number of allocations. This is especially useful if the data is
        // small.
        let mut n = 0;

        // Read at least the header
        while n < 4 {
            n += self.sock.read(&mut buf[n..])?;
        }
        let frame_size =
            u32::from_le_bytes(<[u8; 4]>::try_from(&buf[..4]).unwrap())
                as usize;

        let total_size = frame_size + 4;
        if total_size > SMALL_BUF_SIZE {
            buf.resize(total_size, 0);
        }

        while n < total_size {
            n += self.sock.read(&mut buf[n..])?;
        }

        if n > total_size {
            bail!("Data size exceeded frame + header size");
        }

        return Ok(ciborium::de::from_reader(&buf[4..])?);
    }
}
