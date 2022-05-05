// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::{SocketAddr, SocketAddrV6};

use slog::Logger;
use tokio::net::TcpStream;
use vsss_rs::Share;

use super::msgs::{Request, Response};
use super::rack_secret::Verifier;
use super::TrustQuorumError;
use crate::bootstrap::spdm;

pub struct Client {
    log: Logger,
    verifier: Verifier,
    addr: SocketAddrV6,
}

impl Client {
    pub fn new(log: &Logger, verifier: Verifier, addr: SocketAddrV6) -> Client {
        Client { log: log.clone(), verifier, addr }
    }

    pub fn addr(&self) -> &SocketAddrV6 {
        &self.addr
    }

    // Connect to a trust quorum server, establish an SPDM channel, and retrieve
    // a share.
    pub async fn get_share(&self) -> Result<Share, TrustQuorumError> {
        let sock = TcpStream::connect(&self.addr).await.map_err(|err| {
            TrustQuorumError::Io {
                message: format!("Connecting to {}", self.addr),
                err,
            }
        })?;
        let transport = spdm::Transport::new(sock, self.log.clone());

        // Complete SPDM negotiation and return a secure transport
        let mut transport =
            spdm::requester::run(self.log.clone(), transport).await?;

        // Request a share and receive it, validating it's what we expect.
        let req = bincode::serialize(&Request::Share)?;
        transport.send(&req).await?;

        let rsp = transport.recv().await?;
        let rsp: Response = bincode::deserialize(&rsp)?;

        let Response::Share(share) = rsp;
        if self.verifier.verify(&share) {
            Ok(share)
        } else {
            Err(TrustQuorumError::InvalidShare(SocketAddr::V6(self.addr)))
        }
    }
}
