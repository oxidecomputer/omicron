// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::io;
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

use slog::Logger;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use vsss_rs::Share;

use super::msgs::Response;
use crate::bootstrap::{agent::BootstrapError, spdm};

// TODO: Get port from config
// TODO: Get IpAddr from local router:
//   See https://github.com/oxidecomputer/omicron/issues/443
pub const PORT: u16 = 12347;

/// A TCP server over which a secure SPDM channel will be established and an
/// application level trust protocol will run.
pub struct Server {
    log: Logger,
    share: Share,
    listener: TcpListener,
}

impl Server {
    pub fn new(log: &Logger, share: Share) -> io::Result<Self> {
        let addr = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, PORT, 0, 0);
        let sock = socket2::Socket::new(
            socket2::Domain::IPV6,
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )?;
        sock.set_only_v6(true)?;

        // Allow rebinding during TIME_WAIT
        sock.set_reuse_address(true)?;

        sock.bind(&addr.into())?;
        sock.listen(5)?;
        sock.set_nonblocking(true)?;

        Ok(Server {
            log: log.clone(),
            share,
            listener: TcpListener::from_std(sock.into())?,
        })
    }

    pub async fn run(&mut self) -> Result<(), BootstrapError> {
        loop {
            // TODO: Track the returned handles in a FuturesUnordered and log any errors?
            // Alternatively, maintain some shared state across all
            // responders that is accessable to the Server.
            // See https://github.com/oxidecomputer/omicron/issues/517
            let _ = self.accept().await?;
        }
    }

    async fn accept(
        &mut self,
    ) -> Result<JoinHandle<Result<(), BootstrapError>>, BootstrapError> {
        let (sock, addr) = self.listener.accept().await?;
        debug!(self.log, "Accepted connection from {}", addr);
        let share = self.share.clone();
        let log = self.log.clone();

        Ok(tokio::spawn(
            async move { run_responder(log, addr, sock, share).await },
        ))
    }
}

async fn run_responder(
    log: Logger,
    addr: SocketAddr,
    sock: TcpStream,
    share: Share,
) -> Result<(), BootstrapError> {
    let transport = spdm::Transport::new(sock, log.clone());

    // TODO: Future code will return a secure SPDM session. For now, we just
    // return the framed transport so we can send unencrypted messages.
    let mut transport = spdm::responder::run(log.clone(), transport).await?;

    info!(log, "Sending share to {}", addr);

    let req = transport.recv().await?;

    // There's only one possible request
    let _ = bincode::deserialize(&req)?;

    let rsp = Response::Share(share);
    let rsp = bincode::serialize(&rsp)?;
    transport.send(&rsp).await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::super::client::Client;
    use super::super::rack_secret::RackSecret;
    use super::*;

    #[tokio::test]
    async fn send_share() {
        // Create a rack secret and some shares
        let secret = RackSecret::new();
        let (shares, verifier) = secret.split(2, 2).unwrap();

        // Start a trust quorum server, but only accept one connection
        let log =
            omicron_test_utils::dev::test_setup_log("trust_quorum::send_share")
                .log;
        let mut server = Server::new(&log, shares[0].clone()).unwrap();
        let join_handle = tokio::spawn(async move { server.accept().await });

        let client =
            Client::new(&log, verifier, "[::1]:12347".parse().unwrap());
        let share = client.get_share().await.unwrap();
        assert_eq!(share, shares[0]);

        join_handle.await.unwrap().unwrap();
    }
}
