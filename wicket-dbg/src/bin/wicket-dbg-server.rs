// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{bail, Result};
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wicket_dbg::{Cmd, Runner, RunnerHandle};

#[tokio::main]
async fn main() -> Result<()> {
    let drain = slog::Discard;
    let root = slog::Logger::root(drain, slog::o!());
    let (mut runner, handle) = Runner::new(root);

    tokio::spawn(async move {
        runner.run().await;
    });

    // TODO: Allow port configuration
    let listener = TcpListener::bind("::1:9010").await?;

    // Accept connections serially. We only want one client at a time.
    loop {
        let (sock, _) = listener.accept().await?;
        process(sock, &handle).await?;
    }
}

async fn process(mut sock: TcpStream, handle: &RunnerHandle) -> Result<()> {
    // Reuse allocations across commands
    let mut buf = BytesMut::with_capacity(4096);
    let mut out = Vec::with_capacity(4096);
    loop {
        loop {
            sock.read_buf(&mut buf).await?;
            if buf.len() >= 4 {
                // We at least have the 4-byte size header
                let frame_size =
                    u32::from_le_bytes(<[u8; 4]>::try_from(&buf[..4]).unwrap())
                        as usize;
                if buf.len() == frame_size + 4 {
                    break;
                }
                if buf.len() > frame_size + 4 {
                    bail!("Data size exceeded frame + header size");
                }
            }
        }

        // We've read a complete frame
        let cmd: Cmd = ciborium::de::from_reader(&buf[4..])?;
        let rpy = handle.send(cmd).await;

        // Serialize the reply
        ciborium::ser::into_writer(&rpy, &mut out)?;

        // Write a 4-byte frame header along with the reply
        let frame_size = u32::try_from(out.len()).unwrap();
        sock.write_all(&frame_size.to_le_bytes()[..]).await?;
        sock.write_all(&out[..]).await?;

        // Clear the buffers so we can re-use them
        buf.clear();
        out.clear();
    }
}
