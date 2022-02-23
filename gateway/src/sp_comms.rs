// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO: This may need to move in part or in its entirety to a separate crate
// for reuse by RSS, once that exists.

//! Inteface for communicating with SPs over UDP on the management network.

use crate::config::KnownSps;
use gateway_messages::{
    HubpackError, IgnitionState, Request, RequestKind, Response, ResponseKind,
    SerializedSize,
};
use slog::{debug, error, info, o, Logger};
use std::{io, net::SocketAddr, sync::atomic::AtomicU32};
use thiserror::Error;
use tokio::net::UdpSocket;

#[derive(Debug, Error)]
pub enum Error {
    #[error("error binding to UDP address {addr}: {err}")]
    UdpBind { addr: SocketAddr, err: io::Error },
    #[error("error sending to UDP address {addr}: {err}")]
    UdpSend { addr: SocketAddr, err: io::Error },
    #[error("error receiving on UDP socket: {err}")]
    UdpRecv { err: io::Error },
    #[error("malformed response packet from {addr}: {err}")]
    MalformedResponse { addr: SocketAddr, err: HubpackError },
}

#[derive(Debug)]
pub struct SpCommunicator {
    log: Logger,
    socket: UdpSocket,
    known_sps: KnownSps,
    request_id: AtomicU32,
    buf: [u8; Request::MAX_SIZE],
}

impl SpCommunicator {
    pub async fn new(
        bind_addr: SocketAddr,
        known_sps: KnownSps,
        log: &Logger,
    ) -> Result<Self, Error> {
        let socket = UdpSocket::bind(bind_addr)
            .await
            .map_err(|err| Error::UdpBind { addr: bind_addr, err })?;
        let log = log.new(o!("local_addr" => bind_addr));
        info!(&log, "started sp-server");
        Ok(Self {
            log,
            socket,
            known_sps,
            request_id: AtomicU32::new(0),
            buf: [0; Request::MAX_SIZE],
        })
    }

    pub fn placeholder_known_sps(&self) -> &KnownSps {
        &self.known_sps
    }

    // TODO: How do we want to describe ignition targets? Ultimately want to
    // send a u8 in the UDP message, so just take that for now.
    pub async fn ignition_get(
        &self,
        target: u8,
    ) -> Result<IgnitionState, Error> {
        // XXX We currently assume we're know which ignition controller is our
        // local one, and only use it for ignition interactions.
        let controller = self.known_sps.ignition_controller;

        let request = Request {
            request_id: self
                .request_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            kind: RequestKind::IgnitionState { target },
        };

        // We know `buf` is large enough for any `Request`, so unwrapping
        // here is fine.
        let mut buf = [0; Request::MAX_SIZE];
        let n = gateway_messages::serialize(&mut buf, &request).unwrap();

        let serialized_request = &buf[..n];
        debug!(
            &self.log,
            "sending {:?} to igntition controller {}", request, controller
        );
        self.socket
            .send_to(serialized_request, controller)
            .await
            .map_err(|err| Error::UdpSend { addr: controller, err })?;

        // TODO Move receiving onto a background task? Mecahnics TBD
        let mut resp_buf = [0; Response::MAX_SIZE];
        let (n, addr) = self
            .socket
            .recv_from(&mut resp_buf)
            .await
            .map_err(|err| Error::UdpRecv { err })?;

        // TODO should we check that `extra` is empty?
        let (resp, _extra) =
            gateway_messages::deserialize::<Response>(&resp_buf[..n])
                .map_err(|err| Error::MalformedResponse { addr, err })?;

        // TODO TODO TODO
        assert_eq!(resp.request_id, request.request_id);
        match resp.kind {
            ResponseKind::IgnitionState(state) => Ok(state),
            other => panic!("bogus response kind {:?}", other),
        }
    }
}
