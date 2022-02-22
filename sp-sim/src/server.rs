// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::Config;
use anyhow::{Context, Result, bail};
use gateway_messages::{Request, SerializedSize};
use slog::{debug, error, Logger};
use std::net::SocketAddr;
use tokio::net::UdpSocket;

/// Thin wrapper pairing a [`UdpSocket`] with a buffer sized for [`Request`]s.
pub(crate) struct UdpServer {
    sock: UdpSocket,
    buf: [u8; Request::MAX_SIZE],
}

impl UdpServer {
    pub(crate) async fn new(config: &Config) -> Result<Self> {
        let sock =
            UdpSocket::bind(config.bind_address).await.with_context(|| {
                format!("failed to bind to {}", config.bind_address)
            })?;

        Ok(Self { sock, buf: [0; Request::MAX_SIZE] })
    }

    pub(crate) async fn recv_from(&mut self) -> Result<(&[u8], SocketAddr)> {
        let (len, addr) = self
            .sock
            .recv_from(&mut self.buf)
            .await
            .with_context(|| "recv_from failed")?;
        Ok((&self.buf[..len], addr))
    }

    pub async fn send_to(&self, buf: &[u8], addr: SocketAddr) -> Result<()> {
        self.sock.send_to(buf, addr).await.with_context(|| "send_to failed")?;
        Ok(())
    }
}

pub(crate) fn logger(config: &Config, name: &str) -> Result<Logger> {
    use slog::Drain;
    let (drain, registration) = slog_dtrace::with_drain(
        config
            .log
            .to_logger(name)
            .with_context(|| "initializing logger")?
    );
    let log = slog::Logger::root(drain.fuse(), slog::o!());
    if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
        let msg = format!("failed to register DTrace probes: {}", e);
        error!(log, "{}", msg);
        bail!("{}", msg);
    } else {
        debug!(log, "registered DTrace probes");
    }
    Ok(log)
}
